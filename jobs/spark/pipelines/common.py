"""Shared plumbing for the Spark pipelines: session, Write-Audit-Publish, DQ recording."""

from __future__ import annotations

import os
import socket
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CATALOG = "lakehouse"
DQ_HALT_EXIT_CODE = 3

# Dagster Pipes: when an orchestrator launched this job, results flow back to it
# (materializations with row counts and snapshot ids, and every data-quality check).
# Run by hand or from `make`, the same code runs with no orchestrator at all.
_pipes: Any = None


def entrypoint(main: Callable[[], None]) -> None:
    global _pipes
    if "DAGSTER_PIPES_CONTEXT" not in os.environ:
        main()
        return
    # Launched by the orchestrator: a missing Pipes library must fail loudly, not
    # silently drop every check result the orchestrator is waiting for.
    from dagster_pipes import open_dagster_pipes

    with open_dagster_pipes() as ctx:
        _pipes = ctx
        main()


def _asset_key(table: str) -> str:
    return table.replace(".", "/")


def _reachable(url: str) -> bool:
    host, _, port = url.split("://", 1)[-1].split("/", 1)[0].partition(":")
    try:
        with socket.create_connection((host, int(port or 80)), timeout=0.5):
            return True
    except OSError:
        return False


def _lineage(builder: SparkSession.Builder, app: str) -> SparkSession.Builder:
    """Column-level lineage for every job via the OpenLineage listener, when a backend is up.

    Lineage is observability, never a dependency: if the lineage service is down the
    job runs anyway (and says so) rather than failing a data delivery over metadata.
    """
    url = os.environ.get("OPENLINEAGE_URL", "")
    if not url:
        return builder
    if not _reachable(url):
        print(f"[lineage] {url} unreachable; running without lineage events", file=sys.stderr)
        return builder
    return (
        builder.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", url)
        .config("spark.openlineage.namespace", os.environ.get("OPENLINEAGE_NAMESPACE", "open-lakehouse"))
        .config("spark.openlineage.parentJobName", app)
    )


def spark_session(app: str) -> SparkSession:
    c = f"spark.sql.catalog.{CATALOG}"
    spark = (
        _lineage(SparkSession.builder.appName(app), app)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "rest")
        .config(
            f"{c}.uri", os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog")
        )
        .config(f"{c}.warehouse", CATALOG)
        .config(
            f"{c}.credential",
            f"{os.environ['SPARK_POLARIS_CLIENT_ID']}:{os.environ['SPARK_POLARIS_CLIENT_SECRET']}",
        )
        .config(f"{c}.scope", "PRINCIPAL_ROLE:ALL")
        # Explicit auth settings: Iceberg is removing the implicit token-endpoint fallback.
        .config(f"{c}.rest.auth.type", "oauth2")
        .config(f"{c}.oauth2-server-uri",
                os.environ.get("POLARIS_TOKEN_URI", "http://polaris:8181/api/catalog/v1/oauth/tokens"))
        .config(f"{c}.token-refresh-enabled", "true")
        # No storage keys in Spark: Polaris vends short-lived, table-scoped credentials.
        .config(f"{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.path-style-access", "true")
        # Read your own writes. Each foreachBatch runs in a session clone with its own
        # catalog, so a cached table (30 s by default) hides the clone's commits from the
        # parent session: the stream would see accounts' new customers as missing.
        .config(f"{c}.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]


def table_exists(spark: SparkSession, ident: str) -> bool:
    return spark.catalog.tableExists(ident)


@dataclass
class Check:
    name: str
    passed: bool
    observed: float
    threshold: str


def ensure_ops_tables(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ops.dq_results (
            run_id STRING, table_name STRING, check_name STRING, passed BOOLEAN,
            observed DOUBLE, threshold STRING, checked_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(checked_at))
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            run_id STRING, step STRING, table_name STRING, status STRING, rows_in BIGINT,
            rows_published BIGINT, rows_quarantined BIGINT, snapshot_id BIGINT,
            started_at TIMESTAMP, finished_at TIMESTAMP, detail STRING
        ) USING iceberg PARTITIONED BY (days(started_at))
    """)


def record_checks(
    spark: SparkSession, run_id: str, table: str, checks: list[Check]
) -> None:
    now = datetime.now(timezone.utc)
    rows = [
        (run_id, table, c.name, c.passed, float(c.observed), c.threshold, now)
        for c in checks
    ]
    spark.createDataFrame(
        rows,
        "run_id string, table_name string, check_name string, passed boolean, "
        "observed double, threshold string, checked_at timestamp",
    ).writeTo("ops.dq_results").append()
    if _pipes is not None:
        for c in checks:
            _pipes.report_asset_check(
                check_name=c.name,
                passed=bool(c.passed),
                asset_key=_asset_key(table),
                metadata={"observed": float(c.observed), "threshold": c.threshold, "run_id": run_id},
            )


def record_run(spark: SparkSession, **kw) -> None:
    cols = [
        "run_id",
        "step",
        "table_name",
        "status",
        "rows_in",
        "rows_published",
        "rows_quarantined",
        "snapshot_id",
        "started_at",
        "finished_at",
        "detail",
    ]
    row = tuple(kw.get(c) for c in cols)
    spark.createDataFrame(
        [row],
        "run_id string, step string, table_name string, status string, "
        "rows_in bigint, rows_published bigint, rows_quarantined bigint, "
        "snapshot_id bigint, started_at timestamp, finished_at timestamp, "
        "detail string",
    ).writeTo("ops.pipeline_runs").append()
    if _pipes is not None and kw.get("status") == "ok" and kw.get("table_name"):
        _pipes.report_asset_materialization(
            asset_key=_asset_key(kw["table_name"]),
            metadata={
                "rows_in": kw.get("rows_in"),
                "rows_published": kw.get("rows_published"),
                "rows_quarantined": kw.get("rows_quarantined"),
                "iceberg_snapshot_id": str(kw.get("snapshot_id")),
                "run_id": kw.get("run_id"),
            },
        )


def write_audit_publish(
    spark: SparkSession,
    table: str,
    run_id: str,
    write: Callable[[], None],
    audit: Callable[[DataFrame], list[Check]],
) -> tuple[bool, list[Check]]:
    """Write-Audit-Publish on an Iceberg branch.

    1. WRITE: `write()` runs with spark.wap.branch set, so every commit lands on a
       private branch; readers of `main` see nothing.
    2. AUDIT: `audit()` gets the branch's contents and returns checks.
    3. PUBLISH: only if every check passes, `main` fast-forwards to the branch in
       one atomic metadata commit. On failure the branch is kept for forensics.
    """
    branch = f"wap_{run_id}"
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS `{branch}`")
    spark.conf.set("spark.wap.branch", branch)
    try:
        write()
    finally:
        spark.conf.unset("spark.wap.branch")

    staged = spark.read.option("branch", branch).table(table)
    checks = audit(staged)
    record_checks(spark, run_id, table, checks)

    if all(c.passed for c in checks):
        spark.sql(f"CALL {CATALOG}.system.fast_forward('{table}', 'main', '{branch}')")
        spark.sql(f"ALTER TABLE {table} DROP BRANCH `{branch}`")
        return True, checks
    return False, checks


# Single-writer rule for silver. While the CDC stream owns silver, it renews a lease
# on the namespace (a catalog property, so no table commits). Batch silver refuses to
# run under a live lease: two writers MERGEing the same table would fight over
# commits, and WAP fast-forward needs main not to move underneath it.
LEASE_PROPERTY = "writer.lease"
LEASE_TTL_S = 300


def renew_silver_lease(spark: SparkSession, holder: str) -> None:
    spark.sql(f"ALTER NAMESPACE silver SET PROPERTIES ('{LEASE_PROPERTY}' = '{holder}@{int(time.time())}')")


def _silver_property(spark: SparkSession, key: str) -> str | None:
    rows = spark.sql("DESCRIBE NAMESPACE EXTENDED silver").collect()
    props = next((r[1] for r in rows if r[0].lower() == "properties"), "") or ""
    marker = f"{key},"
    for part in props.strip("()").split("), ("):
        if part.startswith(marker):
            return part[len(marker):]
    return None


def silver_lease_holder(spark: SparkSession) -> str | None:
    """The live lease holder, or None if the lease is absent or expired."""
    holder, _, ts = (_silver_property(spark, LEASE_PROPERTY) or "").rpartition("@")
    if ts.isdigit() and time.time() - int(ts) < LEASE_TTL_S:
        return holder
    return None


# The stream records when it last had nothing left to read: everything in Kafka at
# that moment is in silver. Readers that need a complete silver (gold) wait for a
# drain that started after they did, instead of building on a stream mid-catch-up.
DRAINED_PROPERTY = "writer.drained-at"


def mark_stream_drained(spark: SparkSession, at: float) -> None:
    spark.sql(f"ALTER NAMESPACE silver SET PROPERTIES ('{DRAINED_PROPERTY}' = '{int(at)}')")


def wait_for_stream_drain(spark: SparkSession, timeout_s: int = 900) -> None:
    """If a stream owns silver, block until it has drained everything published before now."""
    holder = silver_lease_holder(spark)
    if not holder:
        return
    since, deadline = time.time(), time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        at = _silver_property(spark, DRAINED_PROPERTY) or ""
        if at.isdigit() and int(at) >= since:
            print(f"[drain] '{holder}' caught up at {int(at)}; silver is complete", flush=True)
            return
        print(f"[drain] waiting for '{holder}' to catch up (drained-at={at or 'never'}, need >= {int(since)})",
              flush=True)
        time.sleep(10)
    raise RuntimeError(
        f"stream '{holder}' did not catch up within {timeout_s}s; refusing to build on a partial silver"
    )


def halt(message: str) -> None:
    print(f"DQ HALT: {message}", file=sys.stderr)
    sys.exit(DQ_HALT_EXIT_CODE)


def latest_snapshot_id(spark: SparkSession, table: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return rows[0][0] if rows else None


def max_or_default(df: DataFrame, col: str, default):
    value = df.agg(F.max(col)).collect()[0][0]
    return value if value is not None else default
