from pathlib import Path

from lakehouse_platform.catalog import model as m

CONTRACT = {
    "kind": "DataContract",
    "name": "markets_gold",
    "version": "0.2.0",
    "status": "active",
    "tenant": "markets-data",
    "domain": "markets",
    "tags": ["gold"],
    "description": {"purpose": "p", "usage": "u", "limitations": "l"},
    "slaProperties": [
        {"property": "freshness", "value": 70, "unit": "m", "element": "candles"},
        {"property": "freshness", "value": 1, "unit": "d", "element": "markets_gold.daily.day"},
        {"property": "latency", "value": 5, "unit": "m"},
    ],
    "authoritativeDefinitions": [
        {"url": "https://example.test/repo", "type": "implementation", "description": "all"}
    ],
    "schema": [
        {
            "name": "candles",
            "physicalName": "lakehouse.markets_gold.candles",
            "description": "One-minute candles",
            "dataGranularityDescription": "one row per product and minute",
            "customProperties": [
                {"property": "upstream", "value": "markets_silver.trades, markets_bronze.fx_rates"}
            ],
            "authoritativeDefinitions": [
                {"url": "https://example.test/gold.py", "type": "transformationImplementation"}
            ],
            "quality": [{"name": "candles_are_consistent", "dimension": "consistency", "severity": "error"}],
            "properties": [
                {
                    "name": "product_id",
                    "physicalType": "varchar",
                    "primaryKey": True,
                    "required": True,
                    "classification": "public",
                },
                {
                    "name": "email",
                    "physicalType": "varchar",
                    "tags": ["pii.contact"],
                    "description": "contact",
                },
                {"name": "flag", "physicalType": "boolean", "tags": ["special_category"]},
            ],
        },
        {"name": "daily", "physicalName": "lakehouse.markets_gold.daily", "properties": []},
        {"name": "untimed", "physicalName": "lakehouse.markets_silver.untimed", "properties": []},
    ],
}
ENTITLEMENTS = {
    "users": {"alice": {"persona": "partial"}, "carol": {"persona": "none"}, "bob": {"persona": "full"}},
    "personas": {
        "partial": {"schemas": ["markets_gold"], "pii": "partial", "special_category": True},
        "none": {"schemas": ["markets_gold"], "pii": "none", "special_category": False},
        "full": {"schemas": ["markets_gold"], "pii": "full", "special_category": True},
        "locked": {"schemas": ["gold"], "pii": "full"},
    },
    "column_tags": {"markets_gold.daily": {}},
}


def products():
    return {p.table: p for p in m.products_in(CONTRACT, "gold.odcs.yaml")}


def test_one_product_per_table_with_its_layer_and_key():
    p = products()
    assert set(p) == {"candles", "daily", "untimed"}
    assert (p["candles"].namespace, p["candles"].layer, p["candles"].key) == (
        "markets_gold",
        "gold",
        "markets_gold.candles",
    )
    assert p["untimed"].layer == "silver"
    assert p["candles"].path == "/products/markets-data/markets_gold/candles"


def test_the_promise_and_the_how_come_from_standard_odcs_fields():
    c = products()["candles"]
    assert c.granularity == "one row per product and minute"
    assert c.upstream == ("markets_silver.trades", "markets_bronze.fx_rates")
    assert [link.type for link in c.links] == ["transformationImplementation", "implementation"]  # own first
    assert [(k.name, k.severity) for k in c.checks] == [("candles_are_consistent", "error")]
    assert [(col.name, col.primary_key) for col in c.columns][:1] == [("product_id", True)]


def test_freshness_slo_matches_by_element_and_ignores_other_properties():
    p = products()
    assert p["candles"].freshness_slo_seconds == 70 * 60  # element "candles"
    assert p["daily"].freshness_slo_seconds == 86400  # element "namespace.table.column"
    assert p["untimed"].freshness_slo_seconds is None  # latency is not freshness; elements name others


def test_an_sla_without_an_element_covers_the_whole_contract():
    contract = {"slaProperties": [{"property": "Freshness", "value": 2, "unit": "hours"}]}
    assert m.freshness_slo(contract, "anything", "ns_gold") == 7200


def test_freshness_state_is_relative_to_the_promise():
    assert m.freshness_state(60, 3600) == "fresh"
    assert m.freshness_state(5000, 3600) == "late"
    assert m.freshness_state(9000, 3600) == "stale"
    assert m.freshness_state(None, 3600) == "unknown"
    assert m.freshness_state(60, None) == "unknown"


def test_human_units():
    assert (
        m.human_age(30) == "30 s ago" and m.human_age(600) == "10 min ago" and m.human_age(7200) == "2 h ago"
    )
    assert m.human_age(None) == "not measured"
    assert (
        m.human_duration(4200) == "70 min"
        and m.human_duration(86400) == "1 d"
        and m.human_duration(None) == "no promise"
    )


def test_access_follows_the_entitlements_opa_enforces():
    access = {a.persona: a for a in m.access_for(products()["candles"], ENTITLEMENTS)}
    assert access["locked"].can_read is False and access["locked"].handling == ()
    assert access["partial"].users == ("alice",)
    assert dict(access["partial"].handling) == {"email": "masked", "flag": "visible"}
    assert dict(access["none"].handling) == {"email": "hidden (NULL)", "flag": "hidden (NULL)"}
    assert dict(access["full"].handling) == {"email": "visible", "flag": "visible"}


def test_opa_column_tags_count_too():
    ent = {**ENTITLEMENTS, "column_tags": {"markets_gold.candles": {"product_id": "pii.name"}}}
    access = {a.persona: a for a in m.access_for(products()["candles"], ent)}
    assert ("product_id", "masked") in access["partial"].handling


def test_the_repos_own_contracts_load_as_products():
    found = m.load_products([Path(__file__).resolve().parents[3] / "contracts"])
    keys = {p.key for p in found}
    assert {"gold.customer_360", "canary_data.people", "silver.customers"} <= keys
    assert all(p.tenant for p in found)
    assert m.load_products([Path("/nonexistent")]) == []


def test_owners_come_from_the_tenant_files():
    owners = m.owners(Path(__file__).resolve().parents[3] / "tenants")
    assert owners["markets-data"]["team"] == "Markets Data Engineering"


def test_dagster_answer_becomes_a_build():
    from lakehouse_platform.catalog.sources import parse_asset

    node = {
        "__typename": "AssetNode",
        "groupName": "gold",
        "kinds": ["iceberg", "spark"],
        "dependencyKeys": [{"path": ["markets_silver", "trades"]}],
        "assetMaterializations": [{"timestamp": "1790803227887", "runId": "r"}],
        "assetChecksOrError": {
            "__typename": "AssetChecks",
            "checks": [
                {
                    "name": "ok",
                    "executionForLatestMaterialization": {"status": "SUCCEEDED", "timestamp": 1.0},
                },
                {"name": "never_ran", "executionForLatestMaterialization": None},
            ],
        },
    }
    b = parse_asset(node)
    assert b.upstream == ("markets_silver.trades",)
    assert b.last_build == 1790803227.887
    assert [(c.name, c.status) for c in b.checks] == [("ok", "SUCCEEDED"), ("never_ran", "NOT RUN")]
    assert parse_asset({"__typename": "AssetNotFoundError"}) is None
    assert parse_asset({"__typename": "AssetNode"}).checks == ()


def test_an_upstream_entry_keeps_its_spaces_only_commas_separate_entries():
    contract = dict(CONTRACT)
    contract["schema"] = [
        {
            "name": "t",
            "physicalName": "lakehouse.markets_gold.t",
            "customProperties": [
                {
                    "property": "upstream",
                    "value": "frankfurter.app (ECB reference rates), markets_silver.trades",
                }
            ],
        }
    ]
    assert m.products_in(contract)[0].upstream == (
        "frankfurter.app (ECB reference rates)",
        "markets_silver.trades",
    )
