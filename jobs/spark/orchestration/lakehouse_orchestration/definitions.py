"""Dagster definitions: the lakehouse as software-defined assets.

Every table is an asset, from the core-banking source through bronze, silver and the
gold `customer_360` data product. Each Spark job runs unchanged under Dagster Pipes
and reports back:
  * materializations with row counts, quarantine counts and the Iceberg snapshot id;
  * the WAP audit checks (the same checks that gate publishing) as asset checks.

Only what a job explicitly reports counts: no implicit materializations. A table
with nothing new, or silver while the CDC stream owns it, is simply not
materialized in that run, and it's never shown as fresh when it isn't.

Failure semantics match `run.sh`: transient failures retry with exponential backoff
and jitter. A data-quality halt (exit 3) fails *without* retry: bad data needs a
human decision, not another attempt.

Freshness is declared on the data product, so staleness is visible in the UI and
alertable, not discovered by a colleague on a call.
"""

import json
import time
import urllib.parse
import urllib.request
from datetime import timedelta
from pathlib import Path

import dagster as dg
from dagster._core.errors import (
    DagsterPipesExecutionError,
)  # not re-exported at top level

RUNNER = "/opt/pipelines/run.sh"
ENTITIES = ["customers", "accounts", "transactions", "complaints"]
SILVER_CHECKS = ["pk_unique", "pk_not_null", "brand_not_null", "quarantine_rate"]
GOLD_CHECKS = ["row_count_matches_customers", "no_negative_counts", "brand_not_null"]
OWNERS = ["team:data-platform"]
DQ_HALT_EXIT_CODE = 3

POLARIS = "http://polaris:8181/api/catalog"
SECRETS = Path("/run/platform-secrets/spark_etl.env")
LEASE_TTL_S = 300  # must match pipelines/common.py

RETRY = dg.RetryPolicy(
    max_retries=2, delay=30, backoff=dg.Backoff.EXPONENTIAL, jitter=dg.Jitter.PLUS_MINUS
)


def spark_step(
    context: dg.AssetExecutionContext | dg.OpExecutionContext,
    pipes: dg.PipesSubprocessClient,
    step: str,
    *args: str,
) -> dg.PipesClientCompletedInvocation:
    try:
        # MAX_ATTEMPTS=1: Dagster owns retries, so the runner must not retry as well.
        return pipes.run(
            command=[RUNNER, step, *args], context=context, env={"MAX_ATTEMPTS": "1"}
        )
    except DagsterPipesExecutionError as exc:
        if f"code {DQ_HALT_EXIT_CODE}" in str(exc):
            raise dg.Failure(
                description=(
                    f"{step}: data-quality halt. Readers keep the last good snapshot. "
                    "Inspect ops.quarantine and ops.dq_results; fix upstream or re-run with --accept-quarantine."
                ),
                allow_retries=False,
            ) from exc
        raise


def silver_lease_holder() -> str | None:
    """Who holds the silver writer lease (a Polaris namespace property), if it's live.

    Checked *before* launching Spark: when the CDC stream owns silver, a batch run
    would only start a JVM to decide to do nothing.
    """
    env = dict(
        line.split("=", 1) for line in SECRETS.read_text().split() if "=" in line
    )
    form = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "scope": "PRINCIPAL_ROLE:ALL",
            "client_id": env["SPARK_POLARIS_CLIENT_ID"],
            "client_secret": env["SPARK_POLARIS_CLIENT_SECRET"],
        }
    ).encode()
    with urllib.request.urlopen(
        f"{POLARIS}/v1/oauth/tokens", data=form, timeout=10
    ) as r:  # noqa: S310
        token = json.load(r)["access_token"]
    req = urllib.request.Request(
        f"{POLARIS}/v1/lakehouse/namespaces/silver",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310 - fixed internal URL
        lease = json.load(r).get("properties", {}).get("writer.lease", "")
    holder, _, ts = lease.rpartition("@")
    if ts.isdigit() and time.time() - int(ts) < LEASE_TTL_S:
        return holder
    return None


# ------------------------------------------------------------------ sources
sources = [
    dg.AssetSpec(
        ["corebank", t],
        group_name="sources",
        kinds={"postgres"},
        owners=["team:core-banking"],
        description=f"core.{t} in the core-banking system (CDC via Debezium; JDBC for backfills)",
    )
    for t in ENTITIES
]


# ------------------------------------------------------------------- bronze
@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            ["bronze", t],
            deps=[["corebank", t]],
            group_name="bronze",
            kinds={"spark", "iceberg"},
            owners=OWNERS,
            skippable=True,
            description=f"Raw, append-only core.{t} with ingest metadata. Not queryable by colleagues.",
        )
        for t in ENTITIES
    ],
    retry_policy=RETRY,
    op_tags={"lakehouse/writer": "bronze"},
)
def bronze(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    """Incremental JDBC ingest (backfill path; the CDC stream is the live path)."""
    yield from spark_step(context, pipes, "ingest_bronze").get_results(
        implicit_materializations=False
    )


# ------------------------------------------------------------------- silver
SILVER_KEYS = {dg.AssetKey(["silver", t]) for t in ENTITIES}


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            ["silver", t],
            # Order between silver tables (brand resolves via customers) is handled inside
            # the job; Dagster only sees each table's bronze input.
            deps=[["bronze", t]],
            group_name="silver",
            kinds={"spark", "iceberg"},
            owners=OWNERS,
            skippable=True,
            tags={"contract": "odcs"},
            description=f"Conformed, deduplicated {t}: contract-checked, quarantined, published via WAP.",
        )
        for t in ENTITIES
    ],
    check_specs=[
        dg.AssetCheckSpec(
            name,
            asset=["silver", t],
            description="WAP audit gate: publish only if this passes",
        )
        for t in ENTITIES
        for name in SILVER_CHECKS
    ],
    retry_policy=RETRY,
    op_tags={"lakehouse/writer": "silver"},
    # Subsettable only so its outputs (assets and WAP checks) are optional: a scheduled
    # run that finds the stream in charge emits nothing rather than fake check results.
    # The Spark job still writes silver as one unit; the guard below enforces that.
    can_subset=True,
)
def silver(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    """Batch silver. Skipped while the CDC stream holds the silver writer lease."""
    if set(context.selected_asset_keys) != SILVER_KEYS:
        raise dg.Failure(
            description="silver is built as one unit (brand resolves via customers); select all silver assets.",
            allow_retries=False,
        )
    holder = silver_lease_holder()
    if holder and context.run.tags.get("dagster/schedule_name"):
        # The schedule checks the lease when it plans the run, but the stream can take it
        # back before this step starts (e.g. both resume after the host sleeps). Silver is
        # then already current, so skip rather than fail: gold (downstream) is skipped too,
        # and gold_refresh rebuilds it from streamed silver within 30 minutes.
        context.log.warning(
            f"silver skipped: '{holder}' took the writer lease after this run was planned. "
            "The stream keeps silver current; gold_refresh rebuilds gold from it."
        )
        return
    if holder:
        # Explicit, not a silent skip: an operator asked for a batch write that would race
        # the live stream.
        raise dg.Failure(
            description=f"silver is owned by '{holder}' (live lease). Stop the CDC stream to run a batch "
            "backfill; while it runs, the stream enforces the same contract per micro-batch.",
            allow_retries=False,
        )
    yield from spark_step(context, pipes, "silver").get_results(
        implicit_materializations=False
    )


# --------------------------------------------------------------------- gold
@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            ["gold", "customer_360"],
            deps=[["silver", t] for t in ENTITIES],
            group_name="data_products",
            kinds={"spark", "iceberg"},
            owners=OWNERS,
            tags={"data_product": "customer-360", "tier": "gold"},
            description=(
                "One row per customer for live-call agents and colleagues. Contract: "
                "contracts/customer_360.odcs.yaml. Point lookups by customer_id."
            ),
            freshness_policy=dg.FreshnessPolicy.time_window(
                fail_window=timedelta(hours=1), warn_window=timedelta(minutes=30)
            ),
        )
    ],
    check_specs=[
        dg.AssetCheckSpec(
            name, asset=["gold", "customer_360"], description="Data product audit gate"
        )
        for name in GOLD_CHECKS
    ],
    retry_policy=RETRY,
    op_tags={"lakehouse/writer": "gold"},
)
def gold_customer_360(
    context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient
):
    yield from spark_step(context, pipes, "gold").get_results(
        implicit_materializations=False
    )


# -------------------------------------------------------------- maintenance
@dg.op(retry_policy=RETRY, tags={"lakehouse/writer": "maintenance"})
def table_maintenance(context: dg.OpExecutionContext, pipes: dg.PipesSubprocessClient):
    """Compaction, delete-file rewrite, manifest rewrite, snapshot expiry, orphan cleanup."""
    spark_step(context, pipes, "maintenance")


@dg.job(description="Keep planning time and storage flat as commits accumulate")
def maintenance_job():
    table_maintenance()


# ---------------------------------------------------------- jobs & schedules
batch_refresh = dg.define_asset_job(
    "batch_refresh",
    selection=dg.AssetSelection.groups("bronze", "silver", "data_products"),
    description="Backfill path: JDBC bronze -> silver (skipped while the stream owns it) -> gold",
)
gold_refresh = dg.define_asset_job(
    "gold_refresh",
    selection=dg.AssetSelection.assets(["gold", "customer_360"]),
    description="Rebuild the customer_360 data product from (streamed) silver",
)

BRONZE_AND_GOLD = dg.AssetSelection.groups("bronze", "data_products")


@dg.schedule(
    cron_schedule="0 2 * * *",
    target=dg.AssetSelection.groups("bronze", "silver", "data_products"),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def nightly_refresh(context: dg.ScheduleEvaluationContext):
    """Backfill bronze, then silver unless the stream owns it, then rebuild gold."""
    holder = silver_lease_holder()
    if holder:
        return dg.RunRequest(
            asset_selection=list(
                BRONZE_AND_GOLD.resolve(context.repository_def.asset_graph)
            ),
            tags={"lakehouse/silver": f"owned-by-{holder}"},
        )
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[*sources, bronze, silver, gold_customer_360],
    jobs=[batch_refresh, gold_refresh, maintenance_job],
    schedules=[
        dg.ScheduleDefinition(
            job=gold_refresh,
            cron_schedule="*/30 * * * *",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        ),
        nightly_refresh,
        dg.ScheduleDefinition(
            job=maintenance_job,
            cron_schedule="0 3 * * *",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        ),
    ],
    resources={"pipes": dg.PipesSubprocessClient()},
)
