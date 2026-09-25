"""Gold: customer_360, the data product agents and colleagues use during a live call.

One row per customer: identity and contact (masked by policy at query time),
holdings, balances, 30-day activity, and complaint history. It is built for
point lookups by customer_id: sorted and bucketed so Trino reads only a few KB per
lookup, even at hundreds of millions of customers.

Published via WAP like silver, with product-level checks (row count must match
silver.customers, no negative counts). This is the data product's contract.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import (
    Check,
    ensure_ops_tables,
    entrypoint,
    halt,
    latest_snapshot_id,
    new_run_id,
    record_run,
    spark_session,
    table_exists,
    write_audit_publish,
)

TARGET = "gold.customer_360"


def build(spark: SparkSession) -> DataFrame:
    customers = spark.table("silver.customers")
    accounts = spark.table("silver.accounts")
    txns = spark.table("silver.transactions")
    complaints = spark.table("silver.complaints")

    holdings = accounts.groupBy("customer_id").agg(
        F.sort_array(
            F.collect_set(F.when(F.col("status") == "open", F.col("product")))
        ).alias("products_held"),
        F.sum(F.when(F.col("status") == "open", 1).otherwise(0)).alias("open_accounts"),
        F.sum(
            F.when(
                (F.col("status") == "open")
                & (F.col("product").isin("current_account", "savings")),
                F.col("balance"),
            ).otherwise(0)
        )
        .cast("decimal(18,2)")
        .alias("deposit_balance"),
        F.sum(
            F.when(
                (F.col("status") == "open") & (F.col("balance") < 0), -F.col("balance")
            ).otherwise(0)
        )
        .cast("decimal(18,2)")
        .alias("lending_balance"),
        F.max(F.when(F.col("status") == "frozen", True).otherwise(False)).alias(
            "has_frozen_account"
        ),
    )

    recent = txns.where(
        F.col("txn_ts") >= F.current_timestamp() - F.expr("INTERVAL 30 DAYS")
    )
    activity = recent.groupBy("customer_id").agg(
        F.count("*").alias("txn_count_30d"),
        F.sum(F.when(F.col("amount") < 0, -F.col("amount")).otherwise(0))
        .cast("decimal(18,2)")
        .alias("spend_30d"),
        F.sum(F.when(F.col("status") == "reversed", 1).otherwise(0)).alias(
            "reversed_txn_30d"
        ),
        F.max("txn_ts").alias("last_txn_ts"),
    )

    by_recency = complaints.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("opened_at").desc())
        ),
    )
    history = complaints.groupBy("customer_id").agg(
        F.sum(
            F.when(F.col("status").isin("open", "investigating"), 1).otherwise(0)
        ).alias("open_complaints"),
        F.count("*").alias("total_complaints"),
        F.sum(F.when(F.col("status") == "referred_to_fos", 1).otherwise(0)).alias(
            "fos_referrals"
        ),
    )
    last = by_recency.where("_rank = 1").select(
        "customer_id",
        F.col("category").alias("last_complaint_category"),
        F.col("summary").alias("last_complaint_summary"),
        F.col("status").alias("last_complaint_status"),
        F.col("opened_at").alias("last_complaint_opened_at"),
    )

    return (
        customers.select(
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
            F.col("created_at").alias("customer_since"),
        )
        .join(holdings, "customer_id", "left")
        .join(activity, "customer_id", "left")
        .join(history, "customer_id", "left")
        .join(last, "customer_id", "left")
        .fillna(
            0,
            [
                "open_accounts",
                "txn_count_30d",
                "reversed_txn_30d",
                "open_complaints",
                "total_complaints",
                "fos_referrals",
            ],
        )
        .withColumn("refreshed_at", F.current_timestamp())
    )


def main() -> None:
    spark = spark_session("gold-customer-360")
    ensure_ops_tables(spark)
    run_id = new_run_id()
    started = datetime.now(timezone.utc)
    df = build(spark)

    if not table_exists(spark, TARGET):
        (
            df.limit(0)
            .writeTo(TARGET)
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            # Point lookups by customer_id: bucket + sort so min/max stats prune to one small file range.
            .partitionedBy(F.bucket(16, "customer_id"))
            .create()
        )
        spark.sql(f"ALTER TABLE {TARGET} WRITE ORDERED BY customer_id")

    expected = spark.table("silver.customers").count()

    def write() -> None:
        df.writeTo(TARGET).overwritePartitions()

    def audit(staged: DataFrame) -> list[Check]:
        n = staged.count()
        neg = staged.where("open_accounts < 0 OR open_complaints < 0").count()
        null_brand = staged.where(F.col("brand").isNull()).count()
        return [
            Check("row_count_matches_customers", n == expected, n, f"== {expected}"),
            Check("no_negative_counts", neg == 0, neg, "== 0"),
            Check("brand_not_null", null_brand == 0, null_brand, "== 0"),
        ]

    published, checks = write_audit_publish(spark, TARGET, run_id, write, audit)
    record_run(
        spark,
        run_id=run_id,
        step="gold",
        table_name=TARGET,
        status="ok" if published else "halted",
        rows_in=expected,
        rows_published=expected if published else 0,
        rows_quarantined=0,
        snapshot_id=latest_snapshot_id(spark, TARGET),
        started_at=started,
        finished_at=datetime.now(timezone.utc),
        detail="; ".join(
            f"{c.name}={'ok' if c.passed else 'FAIL'}({c.observed})" for c in checks
        ),
    )
    if not published:
        halt(
            f"{TARGET}: product checks failed; consumers keep reading the previous good snapshot"
        )
    print(f"[gold] {TARGET}: published {expected:,} rows (WAP ok)")


if __name__ == "__main__":
    entrypoint(main)
