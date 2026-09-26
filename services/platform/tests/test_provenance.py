import asyncio
import json
from datetime import UTC, date, datetime

import httpx
import pytest
import yaml

from lakehouse_platform.call_assist.evals.run import HERE, run_call
from lakehouse_platform.call_assist.signals import RulesExtractor
from lakehouse_platform.mcp_server.provenance import PolicyExplainer, describe_snapshot, snapshot_sql

PROV = {
    "table": "lakehouse.silver.transactions",
    "snapshot": {"id": "8812345678901234567", "committed_seconds_before_query": 4.2},
    "query": {"engine": "Trino", "query_id": "20260926_000000_00001_abcde", "as_user": "alice"},
    "policy": {"row_filter": "brand IN ('Meridian')", "masked_columns": {}},
    "audit": {"seq": 4242, "row_hash": "0123456789abcdef"},
}


def test_snapshot_sql_reads_main_ref_and_refuses_unknown_tables():
    sql = snapshot_sql("silver.accounts")
    assert '"accounts$refs"' in sql and '"accounts$snapshots"' in sql and "r.name = 'main'" in sql
    with pytest.raises(ValueError):
        snapshot_sql('silver.accounts"; DROP TABLE x; --')


def test_describe_snapshot_keeps_exact_id_and_age():
    row = {
        "snapshot_id": 8812345678901234567,  # beyond float precision: must stay a string
        "committed_at": datetime(2026, 9, 26, 10, 0, 0, tzinfo=UTC),
        "operation": "overwrite",
        "summary": {
            "added-records": "3",
            "total-records": "90000",
            "spark.app.id": "local-1",
            "app-name": "cdc-stream",
        },
    }
    d = describe_snapshot(row, datetime(2026, 9, 26, 10, 0, 7, tzinfo=UTC))
    assert d["id"] == "8812345678901234567"
    assert d["committed_by"].startswith("CDC stream")
    assert d["committed_seconds_before_query"] == 7.0
    assert d["summary"] == {"added-records": "3", "total-records": "90000"}
    assert describe_snapshot(None, datetime.now(UTC)) is None


def fake_opa(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content)["input"]
    if request.url.path.endswith("/rowFilters"):
        assert body["context"]["identity"]["user"] == "alice"
        return httpx.Response(200, json={"result": [{"expression": "brand IN ('Meridian')"}]})
    cols = [r["column"]["columnName"] for r in body["action"]["filterResources"]]
    return httpx.Response(
        200,
        json={
            "result": [
                {"index": cols.index("phone"), "viewExpression": {"expression": "concat('*******', ...)"}}
            ]
        },
    )


def test_policy_explainer_reports_opa_decision_for_the_colleague():
    ex = PolicyExplainer("http://opa:8181")
    ex._http = httpx.Client(transport=httpx.MockTransport(fake_opa))
    out = ex.explain("alice", "gold.customer_360", [("customer_id", "varchar"), ("phone", "varchar")])
    assert out["row_filter"] == "brand IN ('Meridian')"
    assert list(out["masked_columns"]) == ["phone"]


def test_policy_explainer_never_breaks_the_answer():
    ex = PolicyExplainer("http://opa:8181")
    ex._http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    assert ex.explain("alice", "silver.accounts", [("iban", "varchar")]) == {
        "engine": "OPA",
        "unavailable": True,
    }


def test_cards_carry_provenance_but_evidence_stays_clean():
    spec = yaml.safe_load((HERE / "calls.yaml").read_text())
    case = next(c for c in spec["cases"] if "get_recent_transactions" in c.get("tools", {}))
    for tool, fx in case["tools"].items():
        for f in fx if isinstance(fx, list) else [fx]:
            if "error" not in f:
                f["provenance"] = {**PROV, "table": f"lakehouse.{tool}"}

    events: list[dict] = []
    result = asyncio.run(run_call(case, date.fromisoformat(spec["today"]), RulesExtractor(), sink=events))
    assert result["passed"], result["failures"]  # grounding unaffected by provenance

    cards = [e["card"] for e in events if e["type"] == "card"]
    with_tools = [c for c in cards if any("tool" in e for e in c["evidence"] if isinstance(e, dict))]
    assert with_tools, "expected cards backed by tool data"
    for c in with_tools:
        assert c["xray"] and all(x["audit"]["seq"] == 4242 for x in c["xray"])
    assert "provenance" not in json.dumps([c["evidence"] for c in cards])
