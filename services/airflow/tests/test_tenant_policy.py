"""The tenant DAG processor's cluster policy pauses and labels tenant DAGs and fails closed on anything else."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowClusterPolicyViolation

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "config"))

import airflow_local_settings as policy  # noqa: E402

LOC = "/tmp/airflow/dag_bundles/tenant-markets-data/tracking_repo/dags/x.py"


def dag(fileloc, dag_id="markets-data__ingest"):
    return SimpleNamespace(fileloc=fileloc, dag_id=dag_id, is_paused_upon_creation=None, tags=set())


def test_a_tenant_bundle_dag_is_paused_and_labelled():
    d = dag(LOC)
    policy.dag_policy(d)
    assert d.is_paused_upon_creation is True
    assert d.tags == {"tenant:markets-data", "not-executable"}


def test_a_versioned_tenant_checkout_is_recognised_too():
    d = dag("/tmp/airflow/dag_bundles/tenant-canary/versions/abc123/dags/x.py", "canary__x")
    policy.dag_policy(d)
    assert d.is_paused_upon_creation is True


@pytest.mark.parametrize("dag_id", ["bank_silver", "bank_table_maintenance", "other-tenant__x", "markets-data_x", ""])
def test_a_dag_id_outside_the_tenants_prefix_is_refused(dag_id):
    with pytest.raises(AirflowClusterPolicyViolation):
        policy.dag_policy(dag(LOC, dag_id))


@pytest.mark.parametrize(
    "loc",
    ["", "/opt/airflow/dags/bank_pipelines.py", "/tmp/airflow/dag_bundles/dags-folder/x.py", None],
)
def test_an_undeterminable_tenant_is_refused_not_ignored(loc):
    with pytest.raises(AirflowClusterPolicyViolation):
        policy.dag_policy(dag(loc, "bank_silver"))
