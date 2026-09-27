"""What the platform runs for a tenant is generated: check the shape of what it generates."""

import copy
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import render  # noqa: E402

GOOD = yaml.safe_load((render.check.TENANTS / "markets-data.yaml").read_text())


def deployed(**loc):
    t = copy.deepcopy(GOOD)
    t["codeLocation"].update(deploy=True, **loc)
    return t


def code_server(t):
    """The service as YAML; the shared `<<: *service` anchor only resolves inside compose.yaml."""
    return yaml.safe_load(render.service(t).replace("    <<: *service\n", ""))[
        "tenant-markets-data-code"
    ]


def test_committed_files_are_current():
    tenants = render.deployed()
    assert render.WORKSPACE.read_text() == render.render_workspace(tenants)
    assert render.COMPOSE.read_text() == render.render_compose(
        render.COMPOSE.read_text(), tenants
    )


def test_undeployed_tenant_gets_no_code_server():
    # markets-data has no image yet; the canary runs on the platform's own build.
    assert [t["name"] for t in render.deployed()] == ["canary"]
    ws = yaml.safe_load(render.render_workspace([]))
    assert [loc["grpc_server"]["location_name"] for loc in ws["load_from"]] == [
        "lakehouse"
    ]


def test_deployed_tenant_gets_a_location_and_a_code_server():
    t = deployed(memoryMb=1024)
    ws = yaml.safe_load(render.render_workspace([t]))
    assert ws["load_from"][1]["grpc_server"] == {
        "host": "tenant-markets-data-code",
        "port": 4000,
        "location_name": "markets-data",
    }
    svc = code_server(t)
    assert svc["image"] == "ghcr.io/cloudcruncher/lakehouse-markets-data:0.1.0"
    assert svc["entrypoint"][-1] == "markets_data.definitions"
    assert svc["profiles"] == ["tenant-code"]
    assert svc["mem_limit"] == "1024m"


def test_code_server_mounts_only_its_own_secrets():
    svc = code_server(deployed())
    mounts = [v for v in svc["volumes"] if isinstance(v, dict)]
    assert mounts == [
        {
            "type": "volume",
            "source": "platform-secrets",
            "target": "/run/tenant-secrets",
            "read_only": True,
            "volume": {"subpath": "tenants/markets-data"},
        }
    ]
    assert not any(
        "platform-secrets:" in v for v in svc["volumes"] if isinstance(v, str)
    )


def named(name: str, **loc):
    t = deployed(**loc)
    t["name"] = name
    return t


def test_tenants_within_the_memory_budget_pass():
    assert render.over_budget([named("a"), named("b")]) is None  # 2 x 1536 = 3072


def test_tenants_over_the_memory_budget_fail():
    problem = render.over_budget([named("a"), named("b"), named("c", memoryMb=512)])
    assert "3584 MB (a 1536, b 1536, c 512), over the 3072 MB budget" in problem
