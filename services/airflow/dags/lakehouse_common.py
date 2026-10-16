"""Pure logic shared by the bank DAGs, with no Airflow import so it is unit-testable.

Mirrors jobs/spark/orchestration/lakehouse_orchestration/definitions.py (Dagster):
  * exit code 3 from run.sh is a data-quality halt: fail WITHOUT retry;
  * silver is skipped by a scheduled run while the CDC stream holds the writer lease
    (a Polaris namespace property), and refused for a manual run;
  * gold's wait for `writer.drained-at` happens inside the Spark job itself (common.py).
"""

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

RUNNER = "/opt/pipelines/run.sh"
DQ_HALT_EXIT_CODE = 3
LEASE_TTL_S = 300  # must match jobs/spark/pipelines/common.py
LEASE_PROPERTY = "writer.lease"
POLARIS = "http://polaris:8181/api/catalog"
AIRFLOW_VENV_BIN = "/opt/airflow-venv/bin"
SECRETS = Path("/run/platform-secrets/spark_etl.env")

# Runs that nobody asked for by hand: the schedule, or an upstream asset update.
AUTOMATIC_RUN_TYPES = {"scheduled", "asset_triggered"}


def classify_exit(returncode: int) -> str:
    """'ok', 'dq_halt' (do not retry) or 'error' (transient: Airflow retries)."""
    if returncode == 0:
        return "ok"
    return "dq_halt" if returncode == DQ_HALT_EXIT_CODE else "error"


def live_lease_holder(lease: str, now: float | None = None) -> str | None:
    """The holder of a 'holder@epoch' lease value if it is still within its TTL, else None."""
    holder, _, ts = (lease or "").rpartition("@")
    if ts.isdigit() and (time.time() if now is None else now) - int(ts) < LEASE_TTL_S:
        return holder
    return None


def silver_decision(holder: str | None, run_type: str) -> str:
    """'run', 'skip' (automatic run; the stream keeps silver current) or 'refuse' (manual run)."""
    if not holder:
        return "run"
    return "skip" if run_type in AUTOMATIC_RUN_TYPES else "refuse"


def spark_env(environ: dict[str, str]) -> dict[str, str]:
    """The environment for run.sh: no Airflow configuration (DB URL, keys) and no Airflow venv.

    spark-submit must use the image's system Python (the one with the pipeline dependencies),
    not the venv Airflow runs in, so PATH, PYTHONPATH and the PySpark interpreters are reset.
    """
    env = {k: v for k, v in environ.items() if not k.startswith("AIRFLOW")}
    path = [p for p in env.get("PATH", "").split(":") if p and p != AIRFLOW_VENV_BIN]
    return env | {
        "PATH": ":".join(path),
        "PYTHONPATH": "/opt/orchestration",
        "PYSPARK_PYTHON": "/usr/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
        "MAX_ATTEMPTS": "1",
    }


def silver_lease_holder() -> str | None:
    """Who holds the silver writer lease, if live. Checked before launching Spark: when the
    CDC stream owns silver, a batch run would only start a JVM to decide to do nothing."""
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
        lease = json.load(r).get("properties", {}).get(LEASE_PROPERTY, "")
    return live_lease_holder(lease)
