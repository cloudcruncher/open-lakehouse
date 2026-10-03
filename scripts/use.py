#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml"]
# ///
"""Resolve `make up USE=<names>`: print the blueprints to start (or, with --tenants, the tenants named).

A name is a tenant (tenants/<name>.yaml, its `blueprints:`) or the preset `bank` (core banking,
CDC and the Dagster bank schedules). The result is the union, plus `tenants` when a tenant is named.
Exits non-zero on an unknown name so a typo never starts the wrong stack.
"""

import sys
from pathlib import Path

import yaml

PRESETS = {"bank": ["streaming", "corebank", "orchestration"]}
DEFAULT = ["streaming", "orchestration"]
ROOT = Path(__file__).resolve().parent.parent / "tenants"


def resolve(names: list[str]) -> tuple[list[str], list[str]]:
    tenants = {p.stem for p in ROOT.glob("*.yaml")}
    unknown = [n for n in names if n not in tenants and n not in PRESETS]
    if unknown:
        raise SystemExit(f"unknown USE name(s): {', '.join(unknown)}. Known: {', '.join(sorted(tenants | set(PRESETS)))}")
    blueprints: list[str] = []
    for n in names:
        wanted = PRESETS.get(n) or yaml.safe_load((ROOT / f"{n}.yaml").read_text()).get("blueprints", DEFAULT)
        blueprints += [b for b in wanted if b not in blueprints]
    chosen = [n for n in names if n in tenants]
    return blueprints + (["tenants"] if chosen else []), chosen


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--tenants"]
    if not args:
        sys.exit("usage: use.py [--tenants] <tenant|bank>...")
    profiles, chosen = resolve(args)
    print(" ".join(chosen if "--tenants" in sys.argv else profiles))
