"""Table maintenance: the difference between a lakehouse that stays fast and one that rots.

For every table in silver/gold/ops (bronze is append-only raw; it gets expiry only):
  * rewrite_data_files       bin-pack small files; folds merge-on-read delete files in
  * rewrite_position_delete_files  compact delete files left by MERGE
  * rewrite_manifests        keep query planning O(manifests), not O(commits)
  * expire_snapshots         bound metadata and storage growth (keeps 7 days of time travel)
  * remove_orphan_files      reclaim files left by failed writes (older than 3 days only,
                             so an in-flight commit is never touched). Uses Iceberg's own
                             S3FileIO prefix listing: no Hadoop S3 keys anywhere.
  * drop stale WAP branches  audit branches kept after a halted run, older than 7 days

Each call is idempotent and safe to re-run. At scale this runs per table on a
schedule driven by table health metrics (file count, delete ratio), or it is
delegated to a table-maintenance service.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from common import CATALOG, ensure_ops_tables, entrypoint, new_run_id, record_run, spark_session

RETAIN_DAYS = 7
ORPHAN_AGE_DAYS = 3


def tables(spark: SparkSession, namespace: str) -> list[str]:
    return [
        f"{namespace}.{r.tableName}"
        for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{namespace}").collect()
    ]


def call(spark: SparkSession, proc: str, args: str) -> str:
    rows = spark.sql(f"CALL {CATALOG}.system.{proc}({args})").collect()
    return (
        ", ".join(f"{k}={v}" for k, v in rows[0].asDict().items()) if rows else "no-op"
    )


def drop_stale_wap_branches(
    spark: SparkSession, table: str, older_than: datetime
) -> int:
    refs = spark.sql(
        f"SELECT name, snapshot_id FROM {table}.refs WHERE type = 'BRANCH' AND name LIKE 'wap_%'"
    )
    dropped = 0
    for ref in refs.collect():
        committed = spark.sql(
            f"SELECT committed_at FROM {table}.snapshots WHERE snapshot_id = {ref.snapshot_id}"
        ).collect()
        if committed and committed[0][0].replace(tzinfo=timezone.utc) < older_than:
            spark.sql(f"ALTER TABLE {table} DROP BRANCH `{ref.name}`")
            dropped += 1
    return dropped


def main() -> None:
    spark = spark_session("table-maintenance")
    ensure_ops_tables(spark)
    run_id = new_run_id()
    now = datetime.now(timezone.utc)
    expire_before = (now - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (now - timedelta(days=ORPHAN_AGE_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    for ns in ["bronze", "silver", "gold", "ops"]:
        for table in tables(spark, ns):
            started = datetime.now(timezone.utc)
            notes = []
            if ns != "bronze":
                notes.append(
                    "files: "
                    + call(
                        spark,
                        "rewrite_data_files",
                        f"table => '{table}', strategy => 'binpack', "
                        "options => map('min-input-files','5','rewrite-all','false')",
                    )
                )
                notes.append(
                    "deletes: "
                    + call(
                        spark, "rewrite_position_delete_files", f"table => '{table}'"
                    )
                )
                notes.append(
                    "manifests: "
                    + call(spark, "rewrite_manifests", f"table => '{table}'")
                )
                notes.append(
                    f"wap_branches_dropped={drop_stale_wap_branches(spark, table, now - timedelta(days=7))}"
                )
            notes.append(
                "expire: "
                + call(
                    spark,
                    "expire_snapshots",
                    f"table => '{table}', older_than => TIMESTAMP '{expire_before}', retain_last => 20",
                )
            )
            notes.append(
                "orphans: "
                + call(
                    spark,
                    "remove_orphan_files",
                    f"table => '{table}', older_than => TIMESTAMP '{orphan_before}', prefix_listing => true",
                )
            )
            print(f"[maintenance] {table}: " + " | ".join(n[:120] for n in notes))
            record_run(
                spark,
                run_id=run_id,
                step="maintenance",
                table_name=table,
                status="ok",
                started_at=started,
                finished_at=datetime.now(timezone.utc),
                detail=" | ".join(notes)[:2000],
            )


if __name__ == "__main__":
    entrypoint(main)
