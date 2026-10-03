"""`make up USE=...` resolves tenants and presets to blueprints (scripts/use.py)."""

import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("use", Path(__file__).parents[2] / "scripts" / "use.py")
use = importlib.util.module_from_spec(spec)
spec.loader.exec_module(use)


def test_tenant_declares_its_blueprints_and_is_a_tenant():
    profiles, chosen = use.resolve(["markets-data"])
    assert profiles == ["streaming", "orchestration", "tenants"]
    assert chosen == ["markets-data"]


def test_bank_preset_adds_core_banking_without_tenants():
    assert use.resolve(["bank"]) == (["streaming", "corebank", "orchestration"], [])


def test_union_has_no_duplicates():
    profiles, chosen = use.resolve(["bank", "markets-data"])
    assert profiles == ["streaming", "corebank", "orchestration", "tenants"]
    assert chosen == ["markets-data"]


def test_unknown_name_exits():
    with pytest.raises(SystemExit, match="unknown USE name"):
        use.resolve(["nope"])
