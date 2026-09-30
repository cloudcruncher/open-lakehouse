"""A tenant file is a request for platform resources: every way it can be wrong must fail CI."""

import copy
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import check  # noqa: E402

GOOD = yaml.safe_load((check.TENANTS / "markets-data.yaml").read_text())


def write(dir: Path, t: dict, filename: str | None = None) -> None:
    (dir / (filename or f"{t['name']}.yaml")).write_text(yaml.safe_dump(t))


def errors_for(tmp_path: Path, mutate) -> list[str]:
    t = copy.deepcopy(GOOD)
    mutate(t)
    write(tmp_path, t)
    return check.check(tmp_path)


def test_committed_tenants_are_valid():
    assert check.check() == []


@pytest.mark.parametrize(
    "mutate, expected",
    [
        (lambda t: t.pop("owner"), "'owner' is a required property"),
        (lambda t: t.update(surprise=1), "Additional properties"),
        (lambda t: t.update(name="Markets"), "does not match"),
        (lambda t: t["topics"][0].update(partitions=64), "greater than the maximum"),
        (lambda t: t["topics"][0].update(retentionHours=0), "less than the minimum"),
        (
            lambda t: t["codeLocation"].update(image="ghcr.io/x/y:latest"),
            "does not match",
        ),
        (lambda t: t["codeLocation"].update(image="ghcr.io/x/y"), "does not match"),
        (lambda t: t["owner"].update(repo="git@github.com:x/y.git"), "does not match"),
    ],
    ids=[
        "no-owner",
        "unknown-key",
        "bad-name",
        "too-many-partitions",
        "zero-retention",
        "latest-tag",
        "untagged-image",
        "non-https-repo",
    ],
)
def test_schema_rejects(tmp_path, mutate, expected):
    errs = errors_for(tmp_path, mutate)
    assert any(expected in e for e in errs), errs


def test_topic_outside_domain(tmp_path):
    errs = errors_for(
        tmp_path, lambda t: t["topics"][0].update(name="corebank.core.customers")
    )
    assert any("outside the domain 'markets.'" in e for e in errs), errs


def test_namespace_outside_domain(tmp_path):
    errs = errors_for(tmp_path, lambda t: t["namespaces"].append("payments_gold"))
    assert any("payments_gold is outside" in e for e in errs), errs


def test_platform_namespace_refused(tmp_path):
    # 'silver' fails the pattern too; the domain check is the backstop if the pattern loosens.
    errs = errors_for(tmp_path, lambda t: t["namespaces"].append("silver"))
    assert errs


def test_reserved_domain(tmp_path):
    def take_corebank(t):
        t["domain"] = "corebank"
        t["topics"] = [dict(t["topics"][0], name="corebank.trades")]
        t["namespaces"] = ["corebank_gold"]

    errs = errors_for(tmp_path, take_corebank)
    assert any("domain 'corebank' is reserved" in e for e in errs), errs


def test_file_named_after_tenant(tmp_path):
    write(tmp_path, GOOD, "markets.yaml")
    assert any("must be named markets-data.yaml" in e for e in check.check(tmp_path))


def test_two_tenants_cannot_share_a_domain(tmp_path):
    write(tmp_path, GOOD)
    write(tmp_path, dict(copy.deepcopy(GOOD), name="markets-copy"))
    assert any("domain 'markets' is already taken" in e for e in check.check(tmp_path))


def test_empty_file(tmp_path):
    (tmp_path / "empty.yaml").write_text("")
    assert check.check(tmp_path)


SVC = {
    "name": "feed",
    "description": "A producer for live trades.",
    "image": "x/y:1.0",
    "memoryMb": 128,
}


def test_service_names_are_unique_and_not_code(tmp_path):
    errors = errors_for(
        tmp_path,
        lambda t: t.update(services=[dict(SVC, name="code"), dict(SVC, name="code")]),
    )
    assert any("service 'code' is declared twice" in e for e in errors)
    assert any("'code' is taken by the code server" in e for e in errors)


def test_service_image_must_be_pinned(tmp_path):
    errors = errors_for(
        tmp_path, lambda t: t.update(services=[dict(SVC, image="x/y:latest")])
    )
    assert any("services/0/image" in e for e in errors)


def test_a_service_may_only_use_declared_secrets(tmp_path):
    errors = errors_for(
        tmp_path,
        lambda t: t.update(secrets=[], services=[dict(SVC, secrets=["shadowtraffic"])]),
    )
    assert any("uses secret 'shadowtraffic', which the tenant does not declare" in e for e in errors)


def test_the_polaris_secret_name_is_the_platforms(tmp_path):
    errors = errors_for(
        tmp_path,
        lambda t: t.update(secrets=[{"name": "polaris", "description": "Try to overwrite it."}]),
    )
    assert any("'polaris' is taken" in e for e in errors)


def test_observed_table_must_be_in_the_tenants_namespaces(tmp_path):
    errs = errors_for(tmp_path, lambda t: t.update(observe=[{"table": "payments_gold.sales"}]))
    assert any("payments_gold.sales is not in one of the tenant's namespaces" in e for e in errs), errs


def test_observed_topic_must_be_the_tenants_own(tmp_path):
    errs = errors_for(
        tmp_path, lambda t: t.update(observe=[{"table": "markets_bronze.trades", "topic": "corebank.core.customers"}])
    )
    assert any("names topic corebank.core.customers" in e for e in errs), errs


def test_observe_entry_needs_a_qualified_table(tmp_path):
    errs = errors_for(tmp_path, lambda t: t.update(observe=[{"table": "trades"}]))
    assert errs, "a table without its namespace must fail the schema"
