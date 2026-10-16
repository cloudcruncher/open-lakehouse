"""Airflow cluster policy for the tenant DAG processor (ADR 16, tenant DAG bundles).

Tenant DAG files are third-party code. Only the tenant DAG processor loads this file (it is mounted
there and nowhere else). What it enforces:

  * every DAG from a tenant bundle (`tenant-<name>`) is created paused, so nothing runs by accident
  * it is tagged `tenant:<name>` and `not-executable`, so the UI says why it cannot run

The hard guard is not here: the scheduler, which would execute a task with the platform's
credentials, is not configured with the tenant bundles (tenants/render.py), so a tenant task cannot
start there. Unpausing a tenant DAG makes its runs fail, it does not run tenant code on the platform.

Fail closed: only the tenant DAG processor loads this file, so every DAG it sees is a tenant DAG. If the
tenant cannot be derived from the file location, or the dag_id is not prefixed `<tenant>__`, parsing
raises AirflowClusterPolicyViolation (the DAG is not loaded). That also stops a tenant shadowing a bank
DAG id such as `bank_silver`.

Limit: this runs in the same process as the tenant's DAG code, which can monkeypatch it. It is a
guard against mistakes, not against hostile code. A DB-level guard (a limited DB role that cannot write
other tenants' or the bank's rows) is also required: see docs/next-steps.md, "Tenant DAG bundles: blockers".
"""

from __future__ import annotations

import re

from airflow.exceptions import AirflowClusterPolicyViolation

# Airflow keeps each bundle under <tmp>/airflow/dag_bundles/<bundle name>/...
_TENANT_BUNDLE = re.compile(r"/dag_bundles/tenant-([a-z][a-z0-9-]*)/")


def tenant_of(fileloc: str) -> str | None:
    match = _TENANT_BUNDLE.search(fileloc or "")
    return match.group(1) if match else None


def dag_policy(dag) -> None:
    tenant = tenant_of(dag.fileloc)
    if tenant is None:
        raise AirflowClusterPolicyViolation(
            f"DAG {dag.dag_id!r}: cannot tell which tenant bundle {dag.fileloc!r} belongs to"
        )
    if not str(dag.dag_id).startswith(f"{tenant}__"):
        raise AirflowClusterPolicyViolation(
            f"DAG {dag.dag_id!r} must be named '{tenant}__<name>' (tenant {tenant})"
        )
    dag.is_paused_upon_creation = True
    dag.tags.update({f"tenant:{tenant}", "not-executable"})
