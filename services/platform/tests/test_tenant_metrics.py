from prometheus_client.core import REGISTRY  # noqa: F401  (imported to fail fast if the client is missing)

from lakehouse_platform import tenant_metrics as tm


def metadata(records=100, ts_ms=1_700_000_000_000, current=2):
    return {
        "current-snapshot-id": current,
        "snapshots": [
            {"snapshot-id": 1, "timestamp-ms": ts_ms - 60_000, "summary": {"total-records": "10"}},
            {"snapshot-id": 2, "timestamp-ms": ts_ms, "summary": {"total-records": str(records)}},
        ],
    }


def test_snapshot_facts_read_the_current_snapshot_not_the_latest_listed():
    assert tm.snapshot_facts(metadata(current=1)) == (1_699_999_940.0, 10)
    assert tm.snapshot_facts(metadata()) == (1_700_000_000.0, 100)


def test_a_table_never_committed_to_has_no_facts():
    assert tm.snapshot_facts({"snapshots": []}) == (None, None)


def test_lag_is_topic_records_minus_table_records_negative_means_duplicates():
    s = tm.Sample("t", "ns_a.b", "topic", True, 1.0, records=90, topic_records=100)
    assert s.lag == 10
    assert tm.Sample("t", "ns_a.b", "topic", True, 1.0, records=120, topic_records=100).lag == -20
    assert tm.Sample("t", "ns_a.b", None, True, 1.0, records=90).lag is None


def test_collect_reads_each_table_and_asks_kafka_once_per_topic():
    watched = [
        tm.Observed("markets-data", "markets_bronze.trades", "markets.coinbase.trades"),
        tm.Observed("markets-data", "markets_silver.trades"),
        tm.Observed("markets-data", "markets_bronze.trades_copy", "markets.coinbase.trades"),
    ]
    asked = []

    def topic(name):
        asked.append(name)
        return 150

    out = tm.collect(watched, lambda table: metadata(), topic)
    assert [s.lag for s in out] == [50, None, 50]
    assert asked == ["markets.coinbase.trades"]


def test_a_missing_table_is_unobserved_and_does_not_blank_the_others():
    watched = [tm.Observed("t", "ns_a.missing"), tm.Observed("t", "ns_a.there")]
    out = tm.collect(watched, lambda table: None if table.endswith("missing") else metadata(), lambda t: None)
    assert [s.observed for s in out] == [False, True]
    assert [s.exists for s in out] == [False, True]  # not created yet is not the same as unreadable
    assert out[1].records == 100


def test_a_failing_catalog_or_topic_degrades_one_sample_not_the_cycle():
    def boom(_):
        raise RuntimeError("polaris down")

    out = tm.collect([tm.Observed("t", "ns_a.b", "x.y")], boom, lambda t: 1)
    assert out[0].observed is False and out[0].exists is True  # it is there; we could not read it

    out = tm.collect([tm.Observed("t", "ns_a.b", "x.y")], lambda t: metadata(), boom)
    assert out[0].observed is True and out[0].topic_records is None and out[0].lag is None


def test_collector_publishes_the_documented_series():
    c = tm.Collector()
    c.samples = [
        tm.Sample(
            "markets-data", "markets_bronze.trades", "markets.coinbase.trades", True, 1_700_000_000.0, 90, 100
        ),
        tm.Sample("markets-data", "markets_silver.trades", None, True, 1_700_000_001.0, 80),
        tm.Sample("markets-data", "markets_gold.nope", None, False),
    ]
    series = {(m.name, tuple(s.labels.values())): s.value for m in c.collect() for s in m.samples}
    assert (
        series[
            ("tenant_table_lag_records", ("markets-data", "markets_bronze.trades", "markets.coinbase.trades"))
        ]
        == 10
    )
    assert series[("tenant_topic_records", ("markets-data", "markets.coinbase.trades"))] == 100
    assert (
        series[("tenant_table_last_commit_timestamp_seconds", ("markets-data", "markets_silver.trades"))]
        == 1_700_000_001.0
    )
    assert series[("tenant_table_observed", ("markets-data", "markets_gold.nope"))] == 0
    assert (
        series[("tenant_table_exists", ("markets-data", "markets_gold.nope"))] == 1
    )  # unreadable, not absent
    assert ("tenant_table_records", ("markets-data", "markets_gold.nope")) not in series


def test_observed_tables_come_from_the_committed_tenant_files():
    from pathlib import Path

    watched = tm.observed_tables(Path(__file__).resolve().parents[3] / "tenants")
    assert tm.Observed("markets-data", "markets_bronze.trades", "markets.coinbase.trades") in watched
    assert tm.Observed("canary", "canary_data.people") in watched


def test_a_table_the_catalog_does_not_have_yet_is_reported_absent_not_broken():
    c = tm.Collector()
    c.samples = tm.collect([tm.Observed("t", "ns_a.later")], lambda table: None, lambda t: None)
    series = {(m.name, tuple(s.labels.values())): s.value for m in c.collect() for s in m.samples}
    assert series[("tenant_table_exists", ("t", "ns_a.later"))] == 0
    assert series[("tenant_table_observed", ("t", "ns_a.later"))] == 0
