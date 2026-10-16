"""The bank's pipelines as Airflow DAGs (ADR 16, slice 1a): the same Spark jobs as Dagster's
`lakehouse_orchestration`, run from /opt/pipelines/run.sh in this image.

    bank_bronze (02:00) -> asset bronze -> bank_silver -> asset silver -> bank_gold_customer_360
    bank_gold_customer_360 also runs every 30 minutes (the stream keeps silver current)
    bank_table_maintenance (03:00)

Failure semantics match run.sh: Airflow owns retries (2, exponential backoff), so the runner
gets MAX_ATTEMPTS=1. A data-quality halt (exit 3) fails without retry: bad data needs a human.
All DAGs are created paused while Dagster still runs the same jobs (two schedulers must not race on
silver/gold): unpause in the UI to try one. Remove PAUSED when Dagster is deleted.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import DAG, Asset, AssetOrTimeSchedule, CronTriggerTimetable, task
from lakehouse_common import (
    RUNNER,
    classify_exit,
    silver_decision,
    silver_lease_holder,
    spark_env,
)

ENTITIES = ["customers", "accounts", "transactions", "complaints"]
BRONZE = Asset("lakehouse://bronze", extra={"tables": ENTITIES})
SILVER = Asset("lakehouse://silver", extra={"tables": ENTITIES})
GOLD = Asset("lakehouse://gold/customer_360")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}
COMMON = {
    "start_date": datetime(2026, 10, 1),
    "catchup": False,
    "max_active_runs": 1,
    "default_args": DEFAULT_ARGS,
}
PAUSED = {"is_paused_upon_creation": True}


def run_spark(step: str) -> None:
    proc = subprocess.run([RUNNER, step], env=spark_env(dict(os.environ)), check=False)  # noqa: S603
    match classify_exit(proc.returncode):
        case "ok":
            return
        case "dq_halt":
            raise AirflowFailException(
                f"{step}: data-quality halt. Readers keep the last good snapshot. "
                "Inspect ops.quarantine and ops.dq_results; fix upstream or re-run with --accept-quarantine."
            )
        case _:
            raise RuntimeError(f"{step} failed (exit {proc.returncode})")


with DAG(
    "bank_bronze",
    schedule="0 2 * * *",
    description="JDBC ingest into bronze (backfill path; CDC is the live path)",
    tags=["bank", "spark"],
    **COMMON,
    **PAUSED,
):

    @task(outlets=[BRONZE])
    def ingest_bronze():
        run_spark("ingest_bronze")

    ingest_bronze()

with DAG(
    "bank_silver",
    schedule=[BRONZE],
    description="Batch silver (WAP). Skipped while the CDC stream owns it",
    tags=["bank", "spark"],
    **COMMON,
    **PAUSED,
):

    @task(outlets=[SILVER])
    def silver(dag_run=None):
        holder = silver_lease_holder()
        run_type = (
            str(getattr(dag_run.run_type, "value", dag_run.run_type))
            if dag_run
            else "manual"
        )
        match silver_decision(holder, run_type):
            case "skip":
                # No asset event is emitted, so gold is not triggered; its 30-minute run rebuilds it.
                raise AirflowSkipException(
                    f"silver skipped: '{holder}' holds the writer lease (stream keeps it current)"
                )
            case "refuse":
                raise AirflowFailException(
                    f"silver is owned by '{holder}' (live lease). Stop the CDC stream to run a batch backfill."
                )
        run_spark("silver")

    silver()

with DAG(
    "bank_gold_customer_360",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/30 * * * *", timezone="UTC"), assets=SILVER
    ),
    description="Rebuild the customer_360 data product (waits inside the job for the stream to drain)",
    tags=["bank", "spark"],
    **COMMON,
    **PAUSED,
):

    @task(outlets=[GOLD])
    def gold():
        run_spark("gold")

    gold()

with DAG(
    "bank_table_maintenance",
    schedule="0 3 * * *",
    description="Compaction, manifest rewrite, snapshot expiry, orphan cleanup",
    tags=["platform", "spark"],
    **COMMON,
    **PAUSED,
):

    @task
    def maintenance():
        run_spark("maintenance")

    maintenance()
