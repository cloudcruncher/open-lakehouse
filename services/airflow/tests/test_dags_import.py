"""The bank DAG file imports with the Airflow task SDK (skipped where the SDK is not installed)."""

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))


def test_bank_dags_import():
    import bank_pipelines

    assert bank_pipelines.PAUSED == {"is_paused_upon_creation": True}
