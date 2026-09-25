"""Silver: conformed, deduplicated, contract-checked entities, published via WAP.

For each entity:
  1. Take bronze rows landed since the last silver run (by _ingested_at).
  2. Deduplicate to the latest version per primary key.
  3. Apply the data contract row by row. Violating rows go to ops.quarantine with
     the reason; they are never silently dropped.
  4. MERGE the good rows onto a WAP branch, audit the branch, then publish.
  5. Halt the pipeline (exit 3) if batch-level rules break, e.g. too many rows
     quarantined. That usually means an upstream bug, and publishing partial data
     would mislead agents and colleagues.

Brand is denormalised onto every customer-scoped table so the row-level security
filter in OPA is a cheap, prunable predicate.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import (
    Check,
    ensure_ops_tables,
    entrypoint,
    halt,
    latest_snapshot_id,
    new_run_id,
    record_checks,
    record_run,
    silver_lease_holder,
    spark_session,
    table_exists,
    write_audit_publish,
)

BRANDS = ["Meridian", "Northgate", "Isle"]
CURRENCIES = ["GBP", "EUR", "USD"]


@dataclass
class Entity:
    name: str
    pk: str
    columns: list[str]
    # contract: reason -> condition that must be TRUE for a valid row
    contract: dict[str, Column] = field(default_factory=dict)
    brand_from: str | None = None  # how to resolve brand: via customers or accounts


def entities() -> list[Entity]:
    """Built lazily: column expressions need an active Spark session."""
    return [
        Entity(
            "customers",
            "customer_id",
            [
                "customer_id",
                "brand",
                "first_name",
                "last_name",
                "date_of_birth",
                "email",
                "phone",
                "postcode",
                "region",
                "segment",
                "vulnerability_flag",
                "created_at",
                "updated_at",
            ],
            {
                "brand_unknown": F.col("brand").isin(BRANDS),
                "email_invalid": F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
                "dob_in_future": F.col("date_of_birth") < F.current_date(),
            },
        ),
        Entity(
            "accounts",
            "account_id",
            [
                "account_id",
                "customer_id",
                "brand",
                "product",
                "iban",
                "status",
                "balance",
                "currency",
                "opened_at",
                "updated_at",
            ],
            {
                "orphan_customer": F.col("brand").isNotNull(),
                "currency_unknown": F.col("currency").isin(CURRENCIES),
            },
            brand_from="customers",
        ),
        Entity(
            "transactions",
            "txn_id",
            [
                "txn_id",
                "account_id",
                "customer_id",
                "brand",
                "txn_ts",
                "amount",
                "currency",
                "merchant",
                "category",
                "channel",
                "status",
                "updated_at",
            ],
            {
                "orphan_account": F.col("brand").isNotNull(),
                "currency_unknown": F.col("currency").isin(CURRENCIES),
                "timestamp_in_future": F.col("txn_ts")
                <= F.current_timestamp() + F.expr("INTERVAL 1 HOUR"),
                "zero_amount": F.col("amount") != 0,
            },
            brand_from="accounts",
        ),
        Entity(
            "complaints",
            "complaint_id",
            [
                "complaint_id",
                "customer_id",
                "brand",
                "opened_at",
                "channel",
                "category",
                "summary",
                "status",
                "resolution",
                "updated_at",
            ],
            {
                "orphan_customer": F.col("brand").isNotNull(),
                "status_unknown": F.col("status").isin(
                    "open", "investigating", "resolved", "referred_to_fos"
                ),
            },
            brand_from="customers",
        ),
    ]

MAX_QUARANTINE_RATE = 0.01  # >1% bad rows in a batch = upstream incident, halt


def ensure_quarantine(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ops.quarantine (
            run_id STRING, entity STRING, record_key STRING, reasons ARRAY<STRING>,
            payload STRING, quarantined_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (entity, days(quarantined_at))
    """)


def pending_batch(spark: SparkSession, e: Entity) -> DataFrame:
    bronze = spark.table(f"bronze.{e.name}")
    target = f"silver.{e.name}"
    if table_exists(spark, target):
        hw = spark.table(target).agg(F.max("_bronze_ingested_at")).collect()[0][0]
        if hw is not None:
            bronze = bronze.where(F.col("_ingested_at") > F.lit(hw))
    latest = Window.partitionBy(e.pk).orderBy(
        F.col("updated_at").desc(), F.col("_ingested_at").desc()
    )
    return (
        bronze.withColumn("_rn", F.row_number().over(latest))
        .where("_rn = 1")
        .drop("_rn")
        .withColumnRenamed("_ingested_at", "_bronze_ingested_at")
    )


def with_brand(spark: SparkSession, e: Entity, df: DataFrame) -> DataFrame:
    if e.brand_from == "customers":
        ref = spark.table("silver.customers").select("customer_id", "brand")
        return df.join(ref, "customer_id", "left")
    if e.brand_from == "accounts":
        ref = spark.table("silver.accounts").select(
            "account_id", "customer_id", "brand"
        )
        return df.join(ref, "account_id", "left")
    return df


def apply_contract(e: Entity, df: DataFrame) -> DataFrame:
    reasons = F.array_compact(
        F.array(
            *[
                F.when(~F.coalesce(cond, F.lit(False)), F.lit(name))
                for name, cond in e.contract.items()
            ]
        )
    )
    return df.withColumn("_violations", reasons)


def create_silver(spark: SparkSession, e: Entity, sample: DataFrame) -> None:
    target = f"silver.{e.name}"
    if table_exists(spark, target):
        return
    writer = (
        sample.limit(0)
        .writeTo(target)
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        # MERGE rewrites only touched rows (merge-on-read), compaction folds deletes later.
        .tableProperty("write.merge.mode", "merge-on-read")
        .tableProperty("write.update.mode", "merge-on-read")
        .tableProperty("write.delete.mode", "merge-on-read")
    )
    if e.name == "transactions":
        writer = writer.partitionedBy(F.months("txn_ts"))
    elif e.name != "customers":
        writer = writer.partitionedBy(F.col("brand"))
    writer.create()


def audit_checks(e: Entity, rate: float, accept_quarantine: bool):
    """The WAP gate for one silver table: key integrity, brand resolved, quarantine rate."""

    def audit(staged: DataFrame) -> list[Check]:
        dupes = staged.groupBy(e.pk).count().where("count > 1").count()
        null_pk = staged.where(F.col(e.pk).isNull()).count()
        null_brand = staged.where(F.col("brand").isNull()).count()
        return [
            Check("pk_unique", dupes == 0, dupes, "== 0"),
            Check("pk_not_null", null_pk == 0, null_pk, "== 0"),
            Check("brand_not_null", null_brand == 0, null_brand, "== 0"),
            Check(
                "quarantine_rate",
                rate <= MAX_QUARANTINE_RATE or accept_quarantine,
                rate,
                f"<= {MAX_QUARANTINE_RATE}",
            ),
        ]

    return audit


def process(
    spark: SparkSession, e: Entity, run_id: str, accept_quarantine: bool
) -> None:
    started = datetime.now(timezone.utc)
    target = f"silver.{e.name}"
    batch = apply_contract(e, with_brand(spark, e, pending_batch(spark, e))).cache()
    total = batch.count()
    if total == 0:
        # Nothing new is still a successful, recorded run. The gate is re-evaluated on
        # the current table, so the orchestrator sees current check results, not stale ones.
        print(f"[silver] {target}: nothing new")
        if table_exists(spark, target):
            record_checks(spark, run_id, target, audit_checks(e, 0.0, False)(spark.table(target)))
            record_run(
                spark, run_id=run_id, step="silver", table_name=target, status="ok", rows_in=0,
                rows_published=0, rows_quarantined=0, snapshot_id=latest_snapshot_id(spark, target),
                started_at=started, finished_at=datetime.now(timezone.utc), detail="no new data",
            )
        return

    bad = batch.where(F.size("_violations") > 0)
    good = batch.where(F.size("_violations") == 0).select(
        *e.columns, "_bronze_ingested_at"
    )
    n_bad = bad.count()
    rate = n_bad / total

    if n_bad:
        (
            bad.select(
                F.lit(run_id).alias("run_id"),
                F.lit(e.name).alias("entity"),
                F.col(e.pk).cast("string").alias("record_key"),
                F.col("_violations").alias("reasons"),
                F.to_json(F.struct(*e.columns)).alias("payload"),
                F.current_timestamp().alias("quarantined_at"),
            )
            .writeTo("ops.quarantine")
            .append()
        )
        print(f"[silver] {target}: quarantined {n_bad:,}/{total:,} rows ({rate:.2%})")

    if rate > MAX_QUARANTINE_RATE and not accept_quarantine:
        record_run(
            spark,
            run_id=run_id,
            step="silver",
            table_name=target,
            status="halted",
            rows_in=total,
            rows_published=0,
            rows_quarantined=n_bad,
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            detail=f"quarantine rate {rate:.2%} > {MAX_QUARANTINE_RATE:.0%}",
        )
        halt(
            f"{target}: {rate:.2%} of the batch violates the contract (limit {MAX_QUARANTINE_RATE:.0%}). "
            "main is untouched. Inspect ops.quarantine, fix upstream, or re-run with --accept-quarantine."
        )

    create_silver(spark, e, good)
    good.createOrReplaceTempView("src")
    n_good = total - n_bad

    def write() -> None:
        cols = e.columns + ["_bronze_ingested_at"]
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols)
        spark.sql(f"""
            MERGE INTO {target} t USING src s ON t.{e.pk} = s.{e.pk}
            WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)

    audit = audit_checks(e, rate, accept_quarantine)

    published, checks = write_audit_publish(spark, target, run_id, write, audit)
    status = "ok" if published else "halted"
    record_run(
        spark,
        run_id=run_id,
        step="silver",
        table_name=target,
        status=status,
        rows_in=total,
        rows_published=n_good if published else 0,
        rows_quarantined=n_bad,
        snapshot_id=latest_snapshot_id(spark, target),
        started_at=started,
        finished_at=datetime.now(timezone.utc),
        detail="; ".join(
            f"{c.name}={'ok' if c.passed else 'FAIL'}({c.observed})" for c in checks
        ),
    )
    if not published:
        failed = [c.name for c in checks if not c.passed]
        halt(
            f"{target}: audit failed {failed}; branch wap_{run_id} kept for inspection, main untouched"
        )
    print(f"[silver] {target}: published {n_good:,} rows (WAP ok)")
    batch.unpersist()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--accept-quarantine",
        action="store_true",
        help="publish the good rows even if the quarantine rate is above the limit",
    )
    args = ap.parse_args()
    spark = spark_session("silver-conform")
    holder = silver_lease_holder(spark)
    if holder:
        print(f"[silver] skipped: silver is owned by '{holder}' (live lease). Batch silver is for "
              "backfills while the stream is stopped; bronze keeps everything meanwhile.")
        return
    ensure_ops_tables(spark)
    ensure_quarantine(spark)
    run_id = new_run_id()
    for e in entities():  # order matters: brand resolves from customers, then accounts
        process(spark, e, run_id, args.accept_quarantine)


if __name__ == "__main__":
    entrypoint(main)
