"""Streaming CDC: Debezium change events (Kafka) -> bronze changelog + silver upserts, in seconds.

Every micro-batch (default 10 s):
  1. BRONZE  appends the raw change events to `bronze.cdc_events`, the immutable
             changelog (replayable, and the audit trail of what the source said).
  2. SILVER  per entity, in dependency order (customers -> accounts -> transactions
             -> complaints, because brand resolves through customers and accounts):
             keep the latest change per key (by source LSN), apply the *same* data
             contract as batch silver, quarantine violations with reasons, and MERGE
             the rest: inserts, updates (newer wins) and deletes.

Delivery guarantees:
  * Kafka offsets live in the Spark checkpoint and only advance after the batch
    function returns, so a crash replays the last batch (at-least-once input).
  * Bronze writes are idempotent: each commit carries `cdc.batch-id` in its snapshot
    summary and a replayed batch is skipped.
  * Silver MERGEs are idempotent by construction (upsert by key, newer wins), so
    at-least-once input gives effectively-once results.

Differences from batch silver, on purpose: a streaming job can't stop the world
the way batch WAP can. Row-level contracts still apply (bad rows never reach
silver), and the quarantine rate is exported as a metric with an alert, instead of
halting freshness for every other customer.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import (
    ensure_ops_tables,
    renew_silver_lease,
    spark_session,
    table_exists,
)
from silver import (
    apply_contract,
    create_silver,
    ensure_quarantine,
    entities,
    with_brand,
)

STREAM_ID = "corebank-cdc-v1"  # stable across restarts: part of the idempotency key
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = r"corebank\.core\.(customers|accounts|transactions|complaints)"
CHECKPOINT = os.environ.get("CDC_CHECKPOINT", "/checkpoints/corebank-cdc")
TRIGGER = os.environ.get("CDC_TRIGGER", "10 seconds")
MAX_OFFSETS = int(os.environ.get("CDC_MAX_OFFSETS_PER_TRIGGER", 100_000))
ORDER = ["customers", "accounts", "transactions", "complaints"]

# Debezium payload fields -> Spark types, per entity. The JSON is the source's
# contract with us; anything else in it is ignored.
SOURCE_SCHEMA = {
    "customers": "customer_id string, brand string, first_name string, last_name string, "
    "date_of_birth int, email string, phone string, postcode string, region string, segment string, "
    "vulnerability_flag boolean, created_at string, updated_at string",
    "accounts": "account_id string, customer_id string, product string, iban string, status string, "
    "balance string, currency string, opened_at string, updated_at string",
    "transactions": "txn_id string, account_id string, txn_ts string, amount string, currency string, "
    "merchant string, category string, channel string, status string, updated_at string",
    "complaints": "complaint_id string, customer_id string, opened_at string, channel string, "
    "category string, summary string, status string, resolution string, updated_at string",
}

# ---------------------------------------------------------------- metrics
EVENTS = Counter("cdc_events_total", "Change events processed", ["entity", "op"])
QUARANTINED = Counter(
    "cdc_quarantined_total", "Change events quarantined by contract", ["entity"]
)
BATCH_SECONDS = Histogram(
    "cdc_batch_duration_seconds",
    "Micro-batch processing time",
    buckets=(1, 2, 5, 10, 20, 40, 80, 160),
)
LAG = Gauge(
    "cdc_end_to_end_lag_seconds",
    "Source commit -> silver commit, oldest event in the last batch",
)
LAST_BATCH = Gauge(
    "cdc_last_batch_completed_timestamp_seconds", "When the last micro-batch finished"
)
PROGRESS = Gauge(
    "cdc_last_progress_timestamp_seconds", "Last stream progress report (idle streams report too)"
)
QUARANTINE_RATE = Gauge(
    "cdc_quarantine_rate", "Share of the last batch quarantined", ["entity"]
)


def ensure_bronze_changelog(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.cdc_events (
            entity STRING, op STRING, record_key STRING, payload STRING, source_lsn BIGINT,
            source_ts TIMESTAMP, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
            ingested_at TIMESTAMP, batch_id BIGINT
        ) USING iceberg PARTITIONED BY (entity, days(ingested_at))
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """)


def already_committed(spark: SparkSession, key: str) -> bool:
    return bool(
        spark.sql(
            f"SELECT 1 FROM bronze.cdc_events.snapshots WHERE summary['cdc.batch-id'] = '{key}' LIMIT 1"
        ).collect()
    )


def parse(raw: DataFrame) -> DataFrame:
    value = F.col("value").cast("string")
    op = F.get_json_object(value, "$.op")
    return raw.select(
        F.regexp_extract("topic", r"\.([a-z_]+)$", 1).alias("entity"),
        op.alias("op"),
        F.when(op == "d", F.get_json_object(value, "$.before"))
        .otherwise(F.get_json_object(value, "$.after"))
        .alias("payload"),
        F.get_json_object(value, "$.source.lsn").cast("bigint").alias("source_lsn"),
        (F.get_json_object(value, "$.source.ts_ms").cast("double") / 1000)
        .cast("timestamp")
        .alias("source_ts"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).where(F.col("op").isNotNull())


def to_silver_shape(spark: SparkSession, entity: str, changes: DataFrame) -> DataFrame:
    """Debezium JSON -> typed rows with the silver columns (brand resolved later)."""
    rows = changes.select(
        "op",
        "source_lsn",
        "kafka_offset",
        F.from_json("payload", SOURCE_SCHEMA[entity]).alias("r"),
    ).select("op", "source_lsn", "kafka_offset", "r.*")
    for c, t in rows.dtypes:
        if c.endswith("_at") or c == "txn_ts":
            rows = rows.withColumn(c, F.col(c).cast("timestamp"))
        elif c in ("balance", "amount"):
            rows = rows.withColumn(c, F.col(c).cast("decimal(18,2)"))
        elif c == "date_of_birth" and t == "int":
            rows = rows.withColumn(
                c, F.date_add(F.lit("1970-01-01").cast("date"), F.col(c))
            )
    return rows


def upsert_entity(
    spark: SparkSession, entity, changes: DataFrame, batch_key: str
) -> tuple[int, int]:
    pk = entity.pk
    rows = to_silver_shape(spark, entity.name, changes)
    # The latest change per key wins: source LSN order is commit order.
    latest = Window.partitionBy(pk).orderBy(
        F.col("source_lsn").desc(), F.col("kafka_offset").desc()
    )
    rows = (
        rows.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
    )

    deletes = rows.where("op = 'd'").select(pk)
    upserts = rows.where("op <> 'd'")
    if "brand" in upserts.columns and entity.brand_from:
        upserts = upserts.drop("brand")
    if "customer_id" in upserts.columns and entity.brand_from == "accounts":
        upserts = upserts.drop(
            "customer_id"
        )  # resolved through the account, as in batch
    checked = apply_contract(entity, with_brand(spark, entity, upserts)).cache()
    total = checked.count()
    bad = checked.where(F.size("_violations") > 0)
    n_bad = bad.count() if total else 0

    if n_bad:
        (
            bad.select(
                F.lit(batch_key).alias("run_id"),
                F.lit(entity.name).alias("entity"),
                F.col(pk).cast("string").alias("record_key"),
                F.col("_violations").alias("reasons"),
                F.to_json(
                    F.struct(*[c for c in entity.columns if c in bad.columns])
                ).alias("payload"),
                F.current_timestamp().alias("quarantined_at"),
            )
            .writeTo("ops.quarantine")
            .append()
        )
    QUARANTINED.labels(entity.name).inc(n_bad)
    QUARANTINE_RATE.labels(entity.name).set(n_bad / total if total else 0.0)

    now = F.current_timestamp()
    good = checked.where(F.size("_violations") == 0).select(
        *entity.columns, now.alias("_bronze_ingested_at"), F.lit("u").alias("_op")
    )
    target = f"silver.{entity.name}"
    if not table_exists(spark, target):
        create_silver(spark, entity, good.drop("_op"))
    target_cols = [f.name for f in spark.table(target).schema.fields]
    null_row = [
        F.lit(None).cast(dict(good.dtypes)[c]).alias(c) for c in target_cols if c != pk
    ]
    src = good.select(*target_cols, "_op").unionByName(
        deletes.select(F.col(pk), *null_row, F.lit("d").alias("_op"))
    )
    src.createOrReplaceTempView("cdc_src")
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in target_cols)
    insert_cols = ", ".join(target_cols)
    insert_vals = ", ".join(f"s.{c}" for c in target_cols)
    merge = f"""
        MERGE INTO {target} t USING cdc_src s ON t.{pk} = s.{pk}
        WHEN MATCHED AND s._op = 'd' THEN DELETE
        WHEN MATCHED AND s._op <> 'd' AND s.updated_at > t.updated_at THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    # A concurrent writer (compaction, say) can make a commit fail validation. The
    # MERGE is idempotent, so retrying it is always safe.
    for attempt in range(1, 5):
        try:
            # foreachBatch hands us DataFrames bound to a per-batch session clone; the
            # temp view lives there, so the MERGE must run in that same session.
            src.sparkSession.sql(merge)
            break
        except Exception as exc:  # noqa: BLE001 - classify by message: py4j wraps the JVM type
            msg = str(exc)
            if attempt == 4 or not any(
                s in msg for s in ("ValidationException", "CommitFailedException")
            ):
                raise
            print(
                f"[cdc] {target}: commit conflict, retry {attempt}: {msg.splitlines()[0][:160]}"
            )
            time.sleep(attempt * 2)
    checked.unpersist()
    return total - n_bad, n_bad


def make_batch_fn(spark: SparkSession):
    ents = {e.name: e for e in entities()}

    def process(raw: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        changes = parse(raw).cache()
        if changes.isEmpty():
            LAST_BATCH.set(time.time())
            changes.unpersist()
            return
        key = f"{STREAM_ID}:{batch_id}"
        oldest = changes.agg(F.min("source_ts")).collect()[0][0]

        if not already_committed(spark, key):
            record_key = F.coalesce(
                *[
                    F.when(F.col("entity") == e.name, F.get_json_object("payload", f"$.{e.pk}"))
                    for e in ents.values()
                ]
            )
            (
                changes.select(
                    "entity",
                    "op",
                    record_key.alias("record_key"),
                    "payload",
                    "source_lsn",
                    "source_ts",
                    "kafka_topic",
                    "kafka_partition",
                    "kafka_offset",
                    F.current_timestamp().alias("ingested_at"),
                    F.lit(batch_id).cast("bigint").alias("batch_id"),
                )
                .writeTo("bronze.cdc_events")
                .option("snapshot-property.cdc.batch-id", key)
                .append()
            )

        present = set()
        for r in changes.groupBy("entity", "op").count().collect():
            EVENTS.labels(r["entity"], r["op"]).inc(r["count"])
            present.add(r["entity"])
        summary = []
        for name in ORDER:
            if name in present:
                good, bad = upsert_entity(
                    spark, ents[name], changes.where(F.col("entity") == name), key
                )
                summary.append(
                    f"{name}={good}" + (f" (+{bad} quarantined)" if bad else "")
                )
        changes.unpersist()

        now = datetime.now(timezone.utc)
        lag = (
            (now - oldest.replace(tzinfo=timezone.utc)).total_seconds()
            if oldest
            else 0.0
        )
        LAG.set(lag)
        LAST_BATCH.set(time.time())
        BATCH_SECONDS.observe(time.monotonic() - started)
        print(
            f"[cdc] batch {batch_id}: {', '.join(summary)}; lag {lag:.1f}s", flush=True
        )

    return process


def main() -> None:
    start_http_server(int(os.environ.get("METRICS_PORT", 9108)))
    spark = spark_session("cdc-stream")
    ensure_ops_tables(spark)
    ensure_quarantine(spark)
    ensure_bronze_changelog(spark)
    renew_silver_lease(spark, STREAM_ID)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPICS)
        # Pick up newly created topics in seconds, not the 5-minute client default.
        .option("kafka.metadata.max.age.ms", "30000")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        # Losing events silently is never acceptable: fail loudly if retention outran us.
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        raw.writeStream.queryName(STREAM_ID)
        .foreachBatch(make_batch_fn(spark))
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"[cdc] streaming {TOPICS} -> bronze.cdc_events + silver.* every {TRIGGER}", flush=True)
    # Liveness = progress. Spark reports progress every trigger, even when idle, so a
    # stalled batch shows up as silence and the healthcheck fails.
    PROGRESS.set(time.time())
    last_lease = time.monotonic()
    while query.isActive:
        # Renew on a clock, not per batch: an idle stream still owns silver.
        if time.monotonic() - last_lease > 60:
            renew_silver_lease(spark, STREAM_ID)
            last_lease = time.monotonic()
        progress = query.lastProgress
        if progress:
            ts = datetime.fromisoformat(progress["timestamp"].replace("Z", "+00:00"))
            PROGRESS.set(ts.timestamp())
        query.awaitTermination(5)
    if query.exception():
        raise RuntimeError(f"stream stopped: {query.exception()}")


if __name__ == "__main__":
    main()
