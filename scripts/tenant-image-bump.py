#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
"""Point a tenant's images at a new release (tenant contract: the tenant's release opens this PR).

Rewrites every `image: <image-ref>:<tag>` in tenants/<tenant>.yaml to the new version by editing those
lines in place, so comments and layout survive. Images of any other ref, and tags that are not plain
versions (card-auths-0.7.0 on the same ref), are left alone. Idempotent:
the same version changes nothing.

  uv run scripts/tenant-image-bump.py --tenant markets-data \
      --image-ref ghcr.io/cloudcruncher/lakehouse-markets-data --version 0.13.0 --summary changes.md

Exit 0 whether or not anything changed (the summary file is written only on a change); exit 1 when no
line matches the image ref at all, or the tenant file is missing.
"""

import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
TENANT = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def bump(
    text: str, image_ref: str, version: str
) -> tuple[str, list[tuple[int, str, str]]]:
    """Return the new text and (line number, old line, new line) for each line that changed."""
    line_re = re.compile(
        rf"^(\s*(?:-\s+)?image:\s*)({re.escape(image_ref)}):(v?\d[^\s#]*)(.*)$"
    )  # only plain version tags (0.12.0): a scheme like card-auths-0.7.0 is another release train
    out, changes, matched = [], [], 0
    for n, line in enumerate(text.splitlines(keepends=True), 1):
        body = line.rstrip("\r\n")
        m = line_re.match(body)
        if m:
            matched += 1
            new = f"{m[1]}{m[2]}:{version}{m[4]}"
            if new != body:
                changes.append((n, body, new))
                line = new + line[len(body) :]
        out.append(line)
    if not matched:
        raise ValueError(f"no `image: {image_ref}:<tag>` line found")
    return "".join(out), changes


def summary(
    tenant: str, image_ref: str, version: str, changes: list[tuple[int, str, str]]
) -> str:
    lines = [
        f"Bump `{tenant}` to `{image_ref}:{version}`. Lines changed in `tenants/{tenant}.yaml`:",
        "",
    ]
    lines += [
        f"- line {n}: `{old.strip()}` -> `{new.strip()}`" for n, old, new in changes
    ]
    lines += [
        "",
        "Images of any other ref (and other tag schemes) in the file are untouched.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--tenant", required=True)
    p.add_argument(
        "--image-ref",
        required=True,
        help="e.g. ghcr.io/cloudcruncher/lakehouse-markets-data",
    )
    p.add_argument(
        "--version",
        required=True,
        help="the new tag; a leading v is dropped (v0.13.0 -> 0.13.0)",
    )
    p.add_argument("--tenants-dir", type=Path, default=ROOT / "tenants")
    p.add_argument(
        "--summary", type=Path, help="write the PR body here when something changed"
    )
    a = p.parse_args(argv)

    version = a.version.removeprefix("v")
    if not VERSION.match(version) or not TENANT.match(a.tenant):
        print(
            f"FAIL bad tenant or version: {a.tenant!r} {a.version!r}", file=sys.stderr
        )
        return 1
    path = a.tenants_dir / f"{a.tenant}.yaml"
    if not path.is_file():
        print(f"FAIL {path} does not exist", file=sys.stderr)
        return 1
    try:
        new, changes = bump(path.read_text(), a.image_ref, version)
    except ValueError as e:
        print(f"FAIL {path.name}: {e}", file=sys.stderr)
        return 1
    yaml.safe_load(new)  # still parses
    if not changes:
        print(f"no change: {path.name} already at {a.image_ref}:{version}")
        return 0
    path.write_text(new)
    body = summary(a.tenant, a.image_ref, version, changes)
    if a.summary:
        a.summary.write_text(body)
    print(body, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
