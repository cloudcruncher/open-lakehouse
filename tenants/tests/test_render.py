"""What the platform runs for a tenant is generated: check the shape of what it generates."""

import copy
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import render  # noqa: E402

GOOD = yaml.safe_load((render.check.TENANTS / "markets-data.yaml").read_text())
GOOD.pop("services", None)  # each test declares the services it needs


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
    assert [t["name"] for t in render.deployed()] == ["canary", "markets-data"]
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
    assert svc["image"] == GOOD["codeLocation"]["image"]
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


STREAM = {
    "name": "trades-stream",
    "description": "Kafka to markets_bronze.trades, continuously.",
    "image": "ghcr.io/cloudcruncher/lakehouse-markets-data:0.2.0",
    "command": ["python3", "-m", "markets_data.streams.trades"],
    "memoryMb": 1280,
    "stateVolume": True,
    "deploy": True,
}


def test_tenants_within_the_memory_budget_pass():
    a = named("a")
    a["services"] = [STREAM]
    assert render.over_budget([a, named("b")]) is None  # 1536 + 1280 + 1536 = 4352


def test_tenants_over_the_memory_budget_fail():
    a = named("a")
    a["services"] = [dict(STREAM, memoryMb=512)]
    problem = render.over_budget([a, named("b"), named("c")])
    assert (
        "5120 MB (a code 1536, a trades-stream 512, b code 1536, c code 1536), over the 4608 MB budget"
        in problem
    )


def rendered_workload(s: dict) -> tuple[dict, str]:
    t = deployed()
    t["services"] = [s]
    compose = render.render_compose(render.COMPOSE.read_text(), [t])
    block = render.workload(t, s).replace("    <<: *service\n", "")
    return yaml.safe_load(block)[f"tenant-markets-data-{s['name']}"], compose


def test_service_runs_its_command_as_the_tenant_on_data_and_kafka_only():
    svc, _ = rendered_workload(STREAM)
    assert svc["entrypoint"] == ["python3", "-m", "markets_data.streams.trades"]
    assert (
        svc["command"] == []
    )  # the image's default (the code server) must not be appended
    assert svc["networks"] == [
        "data",
        "stream",
    ]  # no meta: no Dagster, source or audit database
    assert svc["environment"]["POLARIS_ENV_FILE"] == "/run/tenant-secrets/polaris.env"
    assert svc["mem_limit"] == "1280m"
    assert {
        "type": "volume",
        "source": "platform-secrets",
        "target": "/run/tenant-secrets",
        "read_only": True,
        "volume": {"subpath": "tenants/markets-data"},
    } in svc["volumes"]


def test_service_state_volume_is_declared_and_mounted():
    svc, compose = rendered_workload(STREAM)
    assert "tenant-markets-data-trades-stream-state:/state" in svc["volumes"]
    volumes = compose.split(render.BEGIN_VOLUMES, 1)[1].split(render.END_VOLUMES, 1)[0]
    assert volumes == "  tenant-markets-data-trades-stream-state:\n"


def test_undeployed_service_is_not_rendered():
    t = deployed()
    t["services"] = [dict(STREAM, deploy=False)]
    compose = render.render_compose(render.COMPOSE.read_text(), [t])
    assert "tenant-markets-data-trades-stream" not in compose
