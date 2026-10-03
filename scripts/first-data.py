#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml"]
# ///
"""Time `make up USE=<tenant>` until the tenant's gold tables return rows, then print memory.

The time-to-first-data target of ADR 15: how long a use case takes from nothing to queryable data
products. Gold tables are the `observe:` entries of tenants/<tenant>.yaml in a `*_gold` namespace.
Usage: scripts/first-data.py <tenant> [--timeout SECONDS]   (the stack may already be running)
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def rows(table: str) -> int:
    """Row count as ops_admin, or 0 while the table or the stack is not there yet."""
    out = subprocess.run(
        [ROOT / "scripts/trino-sql.sh", "ops_admin", f"SELECT count(*) FROM lakehouse.{table}"],
        capture_output=True, text=True, cwd=ROOT,
    )
    digits = out.stdout.strip().split()[-1:] if out.returncode == 0 else []
    return int(digits[0]) if digits and digits[0].isdigit() else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("tenant")
    ap.add_argument("--timeout", type=int, default=1800)
    args = ap.parse_args()
    tenant = yaml.safe_load((ROOT / "tenants" / f"{args.tenant}.yaml").read_text())
    gold = [o["table"] for o in tenant.get("observe", []) if "_gold." in o["table"]]
    if not gold:
        sys.exit(f"{args.tenant}: no `observe:` table in a *_gold namespace to wait for")

    t0 = time.monotonic()
    subprocess.run(["make", "up", f"USE={args.tenant}"], check=True, cwd=ROOT)
    up = time.monotonic() - t0
    print(f"[first-data] stack up in {up:.0f}s; waiting for {', '.join(gold)}", flush=True)

    ready: dict[str, float] = {}
    while len(ready) < len(gold):
        if time.monotonic() - t0 > args.timeout:
            missing = [t for t in gold if t not in ready]
            print(f"[first-data] timed out after {args.timeout}s without rows in {', '.join(missing)}", file=sys.stderr)
            return 1
        for table in gold:
            if table not in ready and rows(table):
                ready[table] = time.monotonic() - t0
                print(f"[first-data] {table} has rows after {ready[table]:.0f}s", flush=True)
        time.sleep(15)

    print(f"[first-data] {args.tenant}: queryable gold after {max(ready.values()):.0f}s (stack up {up:.0f}s)")
    subprocess.run(["make", "--no-print-directory", "mem"], cwd=ROOT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
