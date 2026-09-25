"""Bronze: land source rows as-is, incrementally, with lineage columns.

Delivery semantics: at-least-once. The watermark is derived from what bronze already
holds (max source updated_at), minus a small overlap to catch late-committing source
transactions. Re-reading a row twice is harmless: silver merges on the primary key
and keeps the latest version, so the pipeline is effectively-once end to end.

Next step (streaming profile): replace this JDBC pull with Debezium CDC -> Kafka ->
Iceberg, which captures deletes and removes load from the source database.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import (
    ensure_ops_tables,
    entrypoint,
    max_or_default,
    new_run_id,
    record_run,
    spark_session,
    table_exists,
)

SOURCES = ["customers", "accounts", "transactions", "complaints"]
OVERLAP = timedelta(minutes=10)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

JDBC_URL = os.environ.get(
    "COREBANK_JDBC_URL", "jdbc:postgresql://postgres:5432/corebank"
)


def main() -> None:
    spark = spark_session("bronze-ingest")
    ensure_ops_tables(spark)
    run_id = new_run_id()

    for table in SOURCES:
        started = datetime.now(timezone.utc)
        target = f"bronze.{table}"
        watermark = EPOCH
        if table_exists(spark, target):
            watermark = max_or_default(spark.table(target), "updated_at", EPOCH)
            if watermark.tzinfo is None:
                watermark = watermark.replace(tzinfo=timezone.utc)
            watermark = max(EPOCH, watermark - OVERLAP)

        query = (
            f"(SELECT * FROM core.{table} WHERE updated_at > "
            f"'{watermark.isoformat()}'::timestamptz) AS src"
        )
        src = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", query)
            .option("user", "corebank_reader")  # read-only source identity
            .option("password", os.environ["COREBANK_READER_PASSWORD"])
            .option("fetchsize", "10000")
            .load()
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_source", F.lit(f"corebank.core.{table}"))
        )

        if not table_exists(spark, target):
            (
                src.limit(0)
                .writeTo(target)
                .tableProperty("format-version", "2")
                .tableProperty("write.parquet.compression-codec", "zstd")
                .partitionedBy(F.days("_ingested_at"))
                .create()
            )

        src.writeTo(target).append()
        rows = spark.sql(
            f"SELECT count(*) FROM {target} WHERE _run_id = '{run_id}'"
        ).collect()[0][0]
        print(f"[bronze] {target}: +{rows:,} rows since {watermark.isoformat()}")
        record_run(
            spark,
            run_id=run_id,
            step="bronze",
            table_name=target,
            status="ok",
            rows_in=rows,
            rows_published=rows,
            rows_quarantined=0,
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            detail=f"watermark={watermark.isoformat()}",
        )


if __name__ == "__main__":
    entrypoint(main)
