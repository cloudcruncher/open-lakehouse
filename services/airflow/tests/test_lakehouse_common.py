import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import lakehouse_common as c  # noqa: E402


def test_exit_codes():
    assert c.classify_exit(0) == "ok"
    assert c.classify_exit(3) == "dq_halt"
    assert c.classify_exit(1) == "error"
    assert c.classify_exit(137) == "error"


def test_lease_live_and_stale():
    assert (
        c.live_lease_holder("corebank-cdc-v1@1000", now=1000 + c.LEASE_TTL_S - 1)
        == "corebank-cdc-v1"
    )
    assert c.live_lease_holder("corebank-cdc-v1@1000", now=1000 + c.LEASE_TTL_S) is None
    assert c.live_lease_holder("", now=1) is None
    assert c.live_lease_holder("garbage", now=1) is None


def test_silver_decision():
    assert c.silver_decision(None, "scheduled") == "run"
    assert c.silver_decision(None, "manual") == "run"
    assert c.silver_decision("s", "scheduled") == "skip"
    assert c.silver_decision("s", "asset_triggered") == "skip"
    assert c.silver_decision("s", "manual") == "refuse"


def test_spark_env_hides_airflow_and_uses_system_python():
    env = c.spark_env(
        {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "x",
            "AIRFLOW_HOME": "y",
            "PATH": "/opt/airflow-venv/bin:/usr/bin:/bin",
            "PYTHONPATH": "/opt/airflow/dags",
            "MAX_ATTEMPTS": "3",
            "SPARK_DRIVER_MEMORY": "1280m",
        }
    )
    assert env == {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": "/opt/orchestration",
        "PYSPARK_PYTHON": "/usr/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
        "MAX_ATTEMPTS": "1",
        "SPARK_DRIVER_MEMORY": "1280m",
    }
