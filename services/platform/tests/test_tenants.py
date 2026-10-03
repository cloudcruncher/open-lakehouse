import asyncio
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from lakehouse_platform.bootstrap.tenants import (
    TopicSpec,
    TopicState,
    desired_acls,
    kafka_state,
    load_tenants,
    orphan_namespaces,
    orphan_topics,
    plan_topics,
)

REPO_TENANTS = Path(__file__).resolve().parents[3] / "tenants"
WEEK = {"retention.ms": "604800000", "cleanup.policy": "delete"}


def spec(name="markets.trades", partitions=3, configs=WEEK):
    return TopicSpec(name, partitions, dict(configs))


def markets_data():
    return next(t for t in load_tenants(REPO_TENANTS) if t.name == "markets-data")


def test_loads_the_committed_tenants():
    assert [t.name for t in load_tenants(REPO_TENANTS)] == ["canary", "markets-data"]
    t = markets_data()
    assert (t.name, t.ident, t.group) == ("markets-data", "tenant_markets_data", "tenant-markets-data")
    assert t.namespaces == ("markets_bronze", "markets_silver", "markets_gold")
    fx = next(s for s in t.topics if s.name == "markets.reference.fx-rates")
    assert fx.configs == {"retention.ms": str(720 * 3_600_000), "cleanup.policy": "compact"}


def test_an_invalid_file_stops_everything(tmp_path):
    shutil.copytree(REPO_TENANTS, tmp_path, dirs_exist_ok=True)
    bad = yaml.safe_load((tmp_path / "markets-data.yaml").read_text())
    bad["topics"][0]["name"] = "corebank.core.customers"
    (tmp_path / "markets-data.yaml").write_text(yaml.safe_dump(bad))
    with pytest.raises(SystemExit, match="nothing applied"):
        load_tenants(tmp_path)


def test_missing_topic_is_created():
    plan = plan_topics([spec()], {})
    assert [s.name for s in plan.create] == ["markets.trades"] and plan.changes == 1


def test_in_sync_topic_is_left_alone():
    plan = plan_topics([spec()], {"markets.trades": TopicState(3, WEEK)})
    assert plan.changes == 0 and not plan.refused


def test_partitions_are_only_added():
    plan = plan_topics([spec(partitions=6)], {"markets.trades": TopicState(3, WEEK)})
    assert plan.add_partitions == {"markets.trades": 6}


def test_shrinking_partitions_is_refused_not_attempted():
    plan = plan_topics([spec(partitions=1)], {"markets.trades": TopicState(3, WEEK)})
    assert plan.changes == 0
    assert "can't remove partitions" in plan.refused[0]


def test_config_drift_sends_the_full_desired_set():
    drifted = {"retention.ms": "3600000", "cleanup.policy": "delete"}
    plan = plan_topics([spec()], {"markets.trades": TopicState(3, drifted)})
    assert plan.set_configs == {"markets.trades": WEEK}


def test_orphans_ignore_platform_and_internal_topics():
    t = markets_data()
    existing = [
        "markets.coinbase.trades",
        "corebank.core.customers",
        "contact-centre.transcripts",
        "_connect_offsets",
        "__consumer_offsets",
        "oldteam.feed",
    ]
    assert orphan_topics([t], existing) == ["oldteam.feed"]
    assert orphan_namespaces([t], ["bronze", "markets_gold", "oldteam_gold"], {"bronze"}) == ["oldteam_gold"]


class FakeAdmin:
    """Answers the three metadata calls kafka_state makes, in aiokafka's response shapes."""

    def __init__(self, topics: dict[str, tuple[int, dict[str, str]]]) -> None:
        self.topics = topics

    async def list_topics(self):
        return list(self.topics)

    async def describe_topics(self, names):
        return [{"topic": n, "partitions": [{}] * self.topics[n][0]} for n in names]

    async def describe_configs(self, resources):
        rows = [
            (0, None, 2, r.name, [(k, v, False, False, False) for k, v in self.topics[r.name][1].items()])
            for r in resources
        ]
        return [SimpleNamespace(resources=rows)]


def test_kafka_state_reads_partitions_and_configs():
    admin = FakeAdmin({"markets.trades": (3, WEEK), "other": (1, {})})
    state = asyncio.run(kafka_state(admin, ["markets.trades", "markets.missing"]))
    assert state == {"markets.trades": TopicState(3, WEEK)}


def test_tenant_writer_can_create_rename_and_drop_in_its_own_namespaces():
    from lakehouse_platform.bootstrap.tenants import TENANT_WRITER_PRIVILEGES as granted

    # Tables and views: create, rename, drop (what a Kappa replay swap and a scratch table need).
    assert {"TABLE_FULL_METADATA", "VIEW_CREATE", "VIEW_FULL_METADATA"} <= set(granted)
    # Never the namespace itself: dropping it stays a platform decision.
    assert "NAMESPACE_FULL_METADATA" not in granted
    assert "CATALOG_MANAGE_CONTENT" not in granted


def test_a_tenant_kafka_principal_gets_its_own_topics_and_group_prefix_only():
    tenant = markets_data()
    acls = desired_acls(tenant)
    topics = {name for kind, name, _, _ in acls if kind == "TOPIC"}
    assert topics == {t.name for t in tenant.topics}
    topic_ops = {op for kind, _, _, op in acls if kind == "TOPIC"}
    assert topic_ops == {"READ", "WRITE", "DESCRIBE", "DESCRIBE_CONFIGS"}
    # Never a wildcard, never CREATE/DELETE/ALTER: topics are declared in the tenant file.
    assert all(name != "*" and pattern in ("LITERAL", "PREFIXED") for _, name, pattern, _ in acls)
    assert {op for _, _, _, op in acls}.isdisjoint({"CREATE", "DELETE", "ALTER", "ALL"})
    assert ("GROUP", "markets-data-", "PREFIXED", "READ") in acls
    assert tenant.kafka_user == "tenant-markets-data"


def test_opa_lets_each_tenant_identity_read_exactly_its_namespaces():
    import json

    ents = json.loads((REPO_TENANTS.parent / "infra/opa/data/entitlements.json").read_text())["entitlements"]
    tenants = load_tenants(REPO_TENANTS)
    assert {t.trino_user: sorted(t.namespaces) for t in tenants} == {
        user: sorted(e["schemas"]) for user, e in ents["tenants"].items()
    }
