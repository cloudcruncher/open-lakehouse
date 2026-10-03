"""The image bump edits lines in place: only the named image ref moves, comments and layout stay."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "tenant-image-bump.py"
spec = importlib.util.spec_from_file_location("tenant_image_bump", SCRIPT)
ib = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ib)

REF = "ghcr.io/cloudcruncher/lakehouse-markets-data"
FILE = f"""# top comment
name: markets-data
codeLocation:
  image: {REF}:0.12.0
  deploy: true    # v0.12.0 published by the tenant
services:
  - name: feed
    image: {REF}:0.12.0   # trailing comment
    memoryMb: 128
  - name: card-auths
    image: ghcr.io/cloudcruncher/lakehouse-card-auths:card-auths-0.7.0
  - name: other
    image: ghcr.io/cloudcruncher/something-else:0.12.0
  - name: local
    image: open-lakehouse/spark:dev
"""


def test_bumps_exactly_the_matching_lines():
    new, changes = ib.bump(FILE, REF, "0.13.0")
    assert [n for n, _, _ in changes] == [4, 8]
    old_lines, new_lines = FILE.splitlines(), new.splitlines()
    diff = [
        i + 1
        for i, (a, b) in enumerate(zip(old_lines, new_lines, strict=True))
        if a != b
    ]
    assert diff == [4, 8]
    assert f"image: {REF}:0.13.0   # trailing comment" in new
    assert (
        "deploy: true    # v0.12.0 published by the tenant" in new
    )  # comment untouched
    assert new.startswith("# top comment\n")


def test_other_refs_and_tags_untouched():
    new, _ = ib.bump(FILE, REF, "0.13.0")
    assert "image: ghcr.io/cloudcruncher/lakehouse-card-auths:card-auths-0.7.0" in new
    assert "image: ghcr.io/cloudcruncher/something-else:0.12.0" in new
    assert "image: open-lakehouse/spark:dev" in new


def test_same_ref_with_another_tag_scheme_is_untouched():
    text = f"services:\n  - name: a\n    image: {REF}:card-auths-0.7.0\n  - name: b\n    image: {REF}:0.12.0\n"
    new, changes = ib.bump(text, REF, "0.13.0")
    assert f"image: {REF}:card-auths-0.7.0" in new and len(changes) == 1


def test_ref_must_match_exactly():
    with pytest.raises(ValueError):
        ib.bump(FILE, "ghcr.io/cloudcruncher/lakehouse-markets", "1.0.0")


def test_idempotent():
    once, _ = ib.bump(FILE, REF, "0.13.0")
    twice, changes = ib.bump(once, REF, "0.13.0")
    assert twice == once and changes == []


def run(tmp_path, version, ref=REF, tenant="t"):
    return ib.main(
        [
            "--tenant", tenant,
            "--image-ref", ref,
            "--version", version,
            "--tenants-dir", str(tmp_path),
            "--summary", str(tmp_path / "pr.md"),
        ]
    )  # fmt: skip


def test_cli_writes_file_and_summary_then_is_a_noop(tmp_path):
    f = tmp_path / "t.yaml"
    f.write_text(FILE)
    assert run(tmp_path, "v0.13.0") == 0  # leading v dropped
    assert f.read_text().count(f"{REF}:0.13.0") == 2
    body = (tmp_path / "pr.md").read_text()
    assert "line 4" in body and "line 8" in body and "card-auths" not in body
    (tmp_path / "pr.md").unlink()
    snapshot = f.read_text()
    assert run(tmp_path, "0.13.0") == 0
    assert f.read_text() == snapshot and not (tmp_path / "pr.md").exists()


def test_cli_fails_on_unknown_ref_tenant_or_version(tmp_path):
    (tmp_path / "t.yaml").write_text(FILE)
    assert run(tmp_path, "1.0.0", ref="ghcr.io/x/y") == 1
    assert run(tmp_path, "1.0.0", tenant="nope") == 1
    assert run(tmp_path, "1.0; rm -rf") == 1
