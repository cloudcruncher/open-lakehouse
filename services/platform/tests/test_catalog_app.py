from pathlib import Path

import pytest
import yaml
from starlette.testclient import TestClient

from lakehouse_platform.catalog import app as catalog
from lakehouse_platform.catalog.sources import Build, CheckResult, Freshness

ROOT = Path(__file__).resolve().parents[3]


class FakeSources:
    def __init__(self, fresh=None, build=None):
        self._fresh, self._build = fresh or {}, build

    def freshness(self):
        return self._fresh

    def build(self, namespace, table):
        return self._build if table == "candles" else None


def contract(tmp_path, description="Candles for everyone", upstream="markets_silver.trades"):
    folder = tmp_path / "markets-data"
    folder.mkdir(exist_ok=True)
    (folder / "gold.odcs.yaml").write_text(
        yaml.safe_dump(
            {
                "kind": "DataContract",
                "name": "markets_gold",
                "version": "0.2.0",
                "status": "active",
                "tenant": "markets-data",
                "domain": "markets",
                "description": {"purpose": description, "usage": "u", "limitations": "l"},
                "slaProperties": [{"property": "freshness", "value": 70, "unit": "m", "element": "candles"}],
                "schema": [
                    {
                        "name": "candles",
                        "physicalName": "lakehouse.markets_gold.candles",
                        "description": description,
                        "dataGranularityDescription": "one row per product and minute",
                        "customProperties": [{"property": "upstream", "value": upstream}],
                        "authoritativeDefinitions": [
                            {
                                "url": "javascript:alert(1)",
                                "type": "transformationImplementation",
                                "description": "bad",
                            },
                            {
                                "url": "https://example.test/gold.py",
                                "type": "transformationImplementation",
                                "description": "gold.py",
                            },
                        ],
                        "quality": [
                            {
                                "name": "candles_are_consistent",
                                "description": "high >= low",
                                "dimension": "consistency",
                            }
                        ],
                        "properties": [{"name": "product_id", "physicalType": "varchar", "primaryKey": True}],
                    },
                    {"name": "trades", "physicalName": "lakehouse.markets_silver.trades", "properties": []},
                ],
            }
        )
    )
    return tmp_path


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "CONTRACT_DIRS", [ROOT / "contracts", contract(tmp_path)])
    monkeypatch.setattr(catalog, "TENANTS_DIR", ROOT / "tenants")
    monkeypatch.setattr(catalog, "ENTITLEMENTS", ROOT / "infra/opa/data/entitlements.json")
    fresh = {"markets_gold.candles": Freshness(600, 1684), "markets_silver.trades": Freshness(30, 166000)}
    build = Build("gold", ("spark",), (), 1790803227.0, (CheckResult("candles_are_consistent", "SUCCEEDED"),))
    monkeypatch.setattr(catalog, "sources", FakeSources(fresh, build))
    return TestClient(catalog.app)


def test_index_lists_every_product_with_its_promise(client):
    page = client.get("/").text
    assert "/products/markets-data/markets_gold/candles" in page
    assert "/products/canary/canary_data/people" in page
    assert "promise 70 min" in page and "Fresh" in page


def test_product_page_answers_what_how_trust_and_access(client):
    page = client.get("/products/markets-data/markets_gold/candles").text
    assert "One row per product and minute" in page  # what a row is
    assert "gold.py" in page and "markets_silver.trades" in page  # how it is built, from where
    assert "1 of 1 passing" in page and "candles_are_consistent" in page  # trust
    assert "1,684" in page and "at most 70 min old" in page
    assert "Who can see what" in page and "analyst" in page  # access
    assert "FROM lakehouse.markets_gold.candles" in page  # use it
    assert "Markets Data Engineering" in page  # owner, from the tenant file


def test_lineage_links_to_known_products_and_reverse_shows_used_by(client):
    assert (
        'href="/products/markets-data/markets_silver/trades"'
        in client.get("/products/markets-data/markets_gold/candles").text
    )
    assert "Used by" in client.get("/products/markets-data/markets_silver/trades").text


def test_contract_text_is_escaped_and_unsafe_links_are_not_followed(tmp_path, monkeypatch, client):
    monkeypatch.setattr(
        catalog, "CONTRACT_DIRS", [contract(tmp_path, description="<script>alert(1)</script>")]
    )
    page = client.get("/products/markets-data/markets_gold/candles").text
    assert "<script>alert(1)</script>" not in page and "&lt;script&gt;" in page
    assert "javascript:alert" not in page


def test_unknown_product_is_a_404(client):
    assert client.get("/products/markets-data/markets_gold/nope").status_code == 404


def test_contracts_are_downloadable_but_only_contracts(client):
    ok = client.get("/contracts/markets-data/markets-data/gold.odcs.yaml")
    assert ok.status_code == 200 and "markets_gold" in ok.text
    assert client.get("/contracts/markets-data/..%2F..%2Fetc%2Fpasswd").status_code == 404
    assert client.get("/contracts/markets-data/markets-data/gold.odcs.yaml%00.txt").status_code == 404
    assert client.get("/contracts/wrong-tenant/markets-data/gold.odcs.yaml").status_code == 404


def test_json_api_carries_live_state_for_verification(client):
    rows = {r["key"]: r for r in client.get("/api/products.json").json()}
    candles = rows["markets_gold.candles"]
    assert candles["freshness"] == {"state": "fresh", "age_seconds": 600, "promise_seconds": 4200}
    assert candles["checks"] == {"candles_are_consistent": "SUCCEEDED"}
    assert candles["declared_checks"] == ["candles_are_consistent"] and candles["has_owner"] is True


def test_an_unreachable_source_shows_as_not_measured_not_as_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "CONTRACT_DIRS", [contract(tmp_path)])
    monkeypatch.setattr(catalog, "TENANTS_DIR", ROOT / "tenants")
    monkeypatch.setattr(catalog, "ENTITLEMENTS", ROOT / "infra/opa/data/entitlements.json")
    monkeypatch.setattr(catalog, "sources", FakeSources())
    page = TestClient(catalog.app).get("/products/markets-data/markets_gold/candles").text
    assert "not measured" in page.lower()


def test_wording_for_grain_and_for_nothing_measured_or_promised():
    from lakehouse_platform.catalog.render import freshness_note

    assert freshness_note(300, 4200) == "5 min ago, promise 70 min"
    assert freshness_note(None, None) == "not measured"
    assert freshness_note(300, None) == "5 min ago"


def test_grain_reads_as_a_sentence(client):
    page = client.get("/products/markets-data/markets_gold/candles").text
    assert "<b>Grain.</b> One row per product and minute." in page
