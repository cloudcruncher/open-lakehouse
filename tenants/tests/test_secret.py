"""`make tenant-secret`: only declared slots, env files only, and values never printed."""

import copy
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import secret  # noqa: E402

LICENCE = """# from shadowtraffic.io
LICENSE_ID=abc-123
LICENSE_EMAIL=someone@example.com
LICENSE_EXPIRATION=2026-10-27
LICENSE_SIGNATURE=c2VjcmV0Cg==
"""


def test_keys_are_listed_values_are_not():
    assert secret.keys(LICENCE) == ["LICENSE_ID", "LICENSE_EMAIL", "LICENSE_EXPIRATION", "LICENSE_SIGNATURE"]


def test_a_bad_line_is_named_by_number_not_content():
    with pytest.raises(SystemExit, match=r"^FAIL line 2 is not KEY=VALUE") as e:
        secret.keys("A=1\nsk-live-leaked-token\n")
    assert "leaked" not in str(e.value)


def test_an_empty_file_is_refused():
    with pytest.raises(SystemExit, match="no KEY=VALUE"):
        secret.keys("# nothing\n\n")


def test_expiry_is_reported_for_the_renewal_runbook():
    assert secret.expiry(LICENCE) == "2026-10-27"
    assert secret.expiry("TOKEN=x\n") is None


def test_only_a_declared_slot_is_accepted():
    with pytest.raises(SystemExit, match="declares no secret 'openai'"):
        secret.find("markets-data", "openai")
    with pytest.raises(SystemExit, match="no tenant 'nobody'"):
        secret.find("nobody", "shadowtraffic")
    assert secret.find("markets-data", "shadowtraffic")["name"] == "markets-data"


def test_services_restarted_are_the_deployed_ones_using_it():
    t = yaml.safe_load((secret.check.TENANTS / "markets-data.yaml").read_text())
    t = copy.deepcopy(t)
    t["services"] = [
        {"name": "card-auths", "secrets": ["shadowtraffic"], "deploy": True},
        {"name": "idle", "secrets": ["shadowtraffic"], "deploy": False},
        {"name": "feed", "deploy": True},
    ]
    assert secret.users(t, "shadowtraffic") == ["tenant-markets-data-card-auths"]
