import asyncio
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from lakehouse_platform.bootstrap.tenants import (
    TopicSpec,
    TopicState,
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


def test_loads_the_committed_tenants():
    (t,) = load_tenants(REPO_TENANTS)
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
    (t,) = load_tenants(REPO_TENANTS)
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
