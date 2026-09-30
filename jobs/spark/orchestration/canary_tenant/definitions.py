"""The canary tenant's code location (ADR 14): a tiny synthetic team the platform onboards
through the real path (tenants/canary.yaml), so its own checks have data to run against.

It runs in a generated code server (`tenant-canary-code`) and writes with the canary's own
Polaris principal, read from the one credentials file its mount holds. It never sees the
platform's credentials: that is part of what it proves.
"""

import os
from pathlib import Path

import dagster as dg

SEED = Path(__file__).with_name("seed.py")
SELF_SERVICE = Path(__file__).with_name("self_service.py")


def tenant_credentials() -> dict[str, str]:
    path = Path(os.environ["POLARIS_ENV_FILE"])
    env = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
    return {
        "SPARK_POLARIS_CLIENT_ID": env["POLARIS_CLIENT_ID"],
        "SPARK_POLARIS_CLIENT_SECRET": env["POLARIS_CLIENT_SECRET"],
    }


@dg.asset(
    key=["canary_data", "people"],
    owners=["team:canary"],
    kinds={"spark", "iceberg"},
    description="Five synthetic people with a PII-tagged email column.",
)
def people(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    return pipes.run(
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", "local[1]",
            "--driver-memory", "512m",
            "--conf", "spark.ui.showConsoleProgress=false",
            "--py-files", "/opt/pipelines/common.py",
            str(SEED),
        ],
        context=context,
        env=tenant_credentials(),
    ).get_materialize_result()


@dg.asset(
    key=["canary_data", "self_service"],
    owners=["team:canary"],
    kinds={"spark", "iceberg"},
    description="Proves the tenant can create, rename and drop tables and views in its own namespace "
    "(scratch objects, removed at the end).",
)
def self_service(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    return pipes.run(
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", "local[1]",
            "--driver-memory", "512m",
            "--conf", "spark.ui.showConsoleProgress=false",
            "--py-files", "/opt/pipelines/common.py",
            str(SELF_SERVICE),
        ],
        context=context,
        env=tenant_credentials(),
    ).get_materialize_result()


defs = dg.Definitions(assets=[people, self_service], resources={"pipes": dg.PipesSubprocessClient()})
