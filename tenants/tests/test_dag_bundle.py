"""Tenant Airflow DAG bundles (ADR 16): a pinned remote repo, parsed apart from the platform's own DAGs."""

import copy
import json
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import check  # noqa: E402
import render  # noqa: E402


@pytest.fixture(autouse=True)
def bundles_enabled(monkeypatch):
    """Rendering is exercised with the switch on; the shipped value is False (see the tests at the end)."""
    monkeypatch.setattr(check, "TENANT_BUNDLES_ENABLED", True)


GOOD = yaml.safe_load((check.TENANTS / "markets-data.yaml").read_text())
GOOD.pop("dagBundle", None)
SHA = "0123456789abcdef0123456789abcdef01234567"
BUNDLE = {
    "repo": "https://github.com/cloudcruncher/lakehouse-markets-data",
    "ref": "v1.2.3",
}


def bundle_errors(tmp_path: Path, bundle: dict) -> list[str]:
    t = copy.deepcopy(GOOD)
    t["dagBundle"] = bundle
    (tmp_path / f"{t['name']}.yaml").write_text(yaml.safe_dump(t))
    return check.check(tmp_path)


@pytest.mark.parametrize(
    "bundle",
    [
        BUNDLE,
        {**BUNDLE, "ref": "1.2.3"},
        {**BUNDLE, "ref": "v1.2.3-rc.1"},
        {**BUNDLE, "ref": SHA},
        {**BUNDLE, "subdir": "airflow/dags"},
        {**BUNDLE, "repo": "https://gitlab.example.com/team/sub/repo"},
        {**BUNDLE, "subdir": "a.b/c_d"},
    ],
)
def test_a_pinned_dag_bundle_is_valid(tmp_path, bundle):
    assert bundle_errors(tmp_path, bundle) == []


@pytest.mark.parametrize(
    "ref",
    [
        "main",
        "master",
        "HEAD",
        "refs/heads/main",
        "release/1.2",
        "1.x",
        "v1",
        "v1.2",
        "latest",
        SHA[:39],
        SHA.upper(),
        "",
    ],
)
def test_a_dag_bundle_ref_must_be_a_tag_or_full_sha_never_a_branch(tmp_path, ref):
    assert any(
        "ref" in e and "does not match" in e
        for e in bundle_errors(tmp_path, {**BUNDLE, "ref": ref})
    )


@pytest.mark.parametrize(
    "repo",
    [
        "http://github.com/x/y",
        "ssh://host/x/y",
        "file:///srv/repo",
        "/srv/repo",
        "https://user:token@github.com/x/y",
        "https://github.com/x",
        "https://github.com/x/y z",
        "https://127.0.0.1/x/y",
        "https://10.0.0.5:8080/x/y",
        "https://[::1]/x/y",
        "https://localhost/x/y",
        "https://intranet/x/y",
    ],
)
def test_a_dag_bundle_repo_must_be_plain_https(tmp_path, repo):
    assert any(
        "repo" in e and "does not match" in e
        for e in bundle_errors(tmp_path, {**BUNDLE, "repo": repo})
    )


@pytest.mark.parametrize("subdir", ["../x", "a/../b", "/abs", ".hidden", "", "a/.git", "a/.hidden/b", "dags/", "a//b"])
def test_a_dag_bundle_subdir_stays_inside_the_repo(tmp_path, subdir):
    assert any(
        "subdir" in e for e in bundle_errors(tmp_path, {**BUNDLE, "subdir": subdir})
    )


def test_a_dag_bundle_rejects_unknown_keys_and_needs_repo_and_ref(tmp_path):
    assert bundle_errors(tmp_path, {**BUNDLE, "branch": "main"})
    assert bundle_errors(tmp_path, {"repo": BUNDLE["repo"]})
    assert bundle_errors(tmp_path, {"ref": "v1.2.3"})


def bundled(**bundle):
    t = copy.deepcopy(GOOD)
    t["dagBundle"] = {**BUNDLE, **bundle}
    return t


def airflow_services(ts):
    """The whole generated compose file as YAML (anchors only resolve in the full file)."""
    return yaml.safe_load(render.render_compose(render.COMPOSE.read_text(), ts))[
        "services"
    ]


def bundle_env(svc):
    return json.loads(
        svc["environment"]["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"]
    )


def names(svc):
    return [b["name"] for b in bundle_env(svc)]


def test_a_tenant_dag_bundle_is_a_pinned_repo_bundle_named_after_the_tenant():
    lists = render.bundle_lists([bundled(subdir="airflow/dags")])
    assert [b["name"] for b in lists["tenants"]] == ["tenant-markets-data"]
    assert lists["tenants"][0]["kwargs"] == {
        "git_conn_id": None,
        "tracking_ref": "v1.2.3",
        "subdir": "airflow/dags",
        "repo_url": BUNDLE["repo"],
        "refresh_interval": render.BUNDLE_REFRESH_SECONDS,
    }
    assert lists["tenants"][0]["classpath"].endswith(".GitDagBundle")
    assert render.bundle_lists([bundled()])["tenants"][0]["kwargs"]["subdir"] == "dags"


def test_only_the_tenant_processor_and_the_api_server_learn_tenant_bundles():
    svc = airflow_services([bundled()])
    assert names(svc["airflow-tenant-dag-processor"]) == ["tenant-markets-data"]
    assert names(svc["airflow-api-server"]) == ["dags-folder", "tenant-markets-data"]
    # The scheduler runs tasks with the platform's credentials: a tenant task must not resolve there.
    for platform_side in ("airflow-scheduler", "airflow-dag-processor", "airflow-init"):
        assert names(svc[platform_side]) == ["dags-folder"], platform_side
    assert svc["airflow-dag-processor"]["command"] == [
        "dag-processor",
        "--bundle-name",
        "dags-folder",
    ]
    assert svc["airflow-tenant-dag-processor"]["command"] == [
        "dag-processor",
        "--bundle-name",
        "tenant-markets-data",
    ]


def test_the_tenant_dag_processor_holds_no_platform_secrets_or_spark_credentials():
    p = airflow_services([bundled()])["airflow-tenant-dag-processor"]
    assert [v.split(":")[0] for v in p["volumes"]] == [
        "./services/airflow/config",
        "airflow-tenant-logs",
    ]
    assert p["networks"] == ["meta"]
    # No API/JWT secret and no COREBANK_READER_PASSWORD, but this env is NOT safe: the DB login and the
    # Fernet key let tenant parse-time code read every Airflow secret and rewrite any DAG row. Do not
    # enable bundles until a dedicated limited DB role, no Fernet key and a dedicated network exist.
    assert set(p["environment"]) == {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__CORE__LOAD_EXAMPLES",
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST",
        "AIRFLOW__USAGE_DATA_COLLECTION__ENABLED",
        "AIRFLOW__OPENLINEAGE__DISABLED",
    }
    assert p["profiles"] == ["airflow"]


def test_no_dag_bundle_means_no_tenant_dag_processor():
    svc = airflow_services([GOOD])
    assert "airflow-tenant-dag-processor" not in svc
    assert names(svc["airflow-api-server"]) == ["dags-folder"]


def test_the_processor_counts_against_the_memory_budget():
    with_bundle = render.workloads([bundled()])
    assert ("airflow tenant dag-processor", 512) in with_bundle
    assert ("airflow tenant dag-processor", 512) not in render.workloads([GOOD])


def test_the_bundle_git_connection_is_explicitly_none():
    assert render.bundle_lists([bundled()])["tenants"][0]["kwargs"]["git_conn_id"] is None


def test_bundles_are_refused_for_real_tenants_while_the_blockers_stand(tmp_path, monkeypatch):
    monkeypatch.setattr(check, "TENANT_BUNDLES_ENABLED", False)  # the shipped value
    errors = bundle_errors(tmp_path, BUNDLE)
    assert any("dagBundle is not allowed yet" in e and "next-steps.md" in e for e in errors)
    # render.py goes through the same check, so `make tenants-render` exits non-zero too.
    real_check, real_load = check.check, check.load
    monkeypatch.setattr(check, "check", lambda: real_check(tmp_path))
    monkeypatch.setattr(check, "load", lambda *a: real_load(tmp_path))
    with pytest.raises(SystemExit, match="dagBundle is not allowed yet"):
        render.tenants()

