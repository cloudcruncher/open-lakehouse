"""The contracts check a tenant repo's CI runs (ADR 14): scoped to the tenant's own namespaces."""

import importlib.util
from pathlib import Path

import yaml

# Loaded by path: tenants/check.py is also a module called `check` in the same pytest run.
_spec = importlib.util.spec_from_file_location(
    "contracts_check", Path(__file__).resolve().parents[1] / "check.py"
)
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)

MARKETS = check.tenant_namespaces("markets-data")


def contract(physical: str, **prop) -> dict:
    return {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": "urn:lakehouse-markets-data:contract:trades",
        "version": "1.0.0",
        "status": "active",
        "schema": [
            {
                "name": "trades",
                "physicalName": physical,
                "physicalType": "table",
                "properties": [
                    {
                        "name": "trade_id",
                        "logicalType": "string",
                        "physicalType": "varchar",
                        **prop,
                    }
                ],
            }
        ],
    }


def write(tmp_path: Path, c: dict) -> Path:
    (tmp_path / "trades.odcs.yaml").write_text(yaml.safe_dump(c))
    return tmp_path


def test_platform_contracts_pass():
    assert check.check_static() == []


def test_tenant_contract_in_its_own_namespace_passes(tmp_path):
    folder = write(tmp_path, contract("lakehouse.markets_silver.trades"))
    assert check.check_static(folder, MARKETS) == []


def test_tenant_contract_outside_its_namespaces_fails(tmp_path):
    folder = write(tmp_path, contract("lakehouse.silver.trades"))
    [err] = check.check_static(folder, MARKETS)
    assert "silver.trades is outside this tenant's namespaces" in err


def test_tenant_pii_column_needs_a_platform_mask(tmp_path):
    folder = write(
        tmp_path, contract("lakehouse.markets_silver.trades", tags=["pii.direct"])
    )
    [err] = check.check_static(folder, MARKETS)
    assert (
        "markets_silver.trades.trade_id is pii.direct in the contract but OPA has None"
        in err
    )


def test_tenant_check_ignores_platform_masks(tmp_path):
    # OPA masks silver.customers etc.; a tenant isn't asked to declare the platform's tables.
    folder = write(tmp_path, contract("lakehouse.markets_bronze.trades"))
    assert check.check_static(folder, MARKETS) == []


def test_tenant_with_no_contracts_fails(tmp_path):
    [err] = check.check_static(tmp_path, MARKETS)
    assert "no *.odcs.yaml contracts found" in err
