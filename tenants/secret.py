# /// script
# requires-python = ">=3.12"
# dependencies = ["jsonschema>=4.23", "pyyaml>=6"]
# ///
"""Put a tenant's secret (a vendor licence, an API key) where its services can read it (ADR 14).

The tenant file declares the slot by name (`secrets:`), reviewed like any request; the value never
enters git. This copies an env file (KEY=VALUE lines) into the tenant's own folder of the
platform's secrets volume, next to its Polaris credentials, as tenants/<tenant>/<name>.env. Only
that tenant's containers mount the folder. Values are never printed, only key names (and an
expiry date, if the file has one), and services using the secret are restarted to read it.

Usage: uv run tenants/secret.py TENANT NAME FILE   (or: make tenant-secret TENANT=... NAME=... FILE=...)
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import check  # tenants/check.py, next to this file

KEY = re.compile(r"^[A-Z_][A-Z0-9_]*$")
# `make tenant-secret` passes the Makefile's Compose command (with every profile the stack runs):
# `run` needs the reconciler's dependencies to be defined, even though it doesn't start them.
DEFAULT_COMPOSE = "docker compose --profile *"  # every blueprint; no shell, so * is literal
COMPOSE = [*os.environ.get("COMPOSE", DEFAULT_COMPOSE).split(), "--profile", "tenant-code"]


def keys(text: str) -> list[str]:
    """Key names of an env file; SystemExit naming the line number (never its content) if invalid."""
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, _ = line.partition("=")
        if not sep or not KEY.match(key.strip()):
            raise SystemExit(
                f"FAIL line {n} is not KEY=VALUE (the file must be an env file)"
            )
        out.append(key.strip())
    if not out:
        raise SystemExit("FAIL the file has no KEY=VALUE lines")
    return out


def expiry(text: str) -> str | None:
    for line in text.splitlines():
        key, _, value = line.strip().partition("=")
        if key.strip().endswith("EXPIRATION") or key.strip().endswith("EXPIRES_AT"):
            return value.strip().strip("\"'")
    return None


def users(tenant: dict, name: str) -> list[str]:
    """The tenant's deployed services that use the secret, as Compose service names."""
    return [
        f"tenant-{tenant['name']}-{s['name']}"
        for s in tenant.get("services", [])
        if name in s.get("secrets", []) and s.get("deploy", False)
    ]


def find(tenant_name: str, name: str) -> dict:
    tenants = {t["name"]: t for _, t in check.load()}
    if tenant_name not in tenants:
        raise SystemExit(f"FAIL no tenant '{tenant_name}' (tenants/*.yaml)")
    t = tenants[tenant_name]
    declared = [s["name"] for s in t.get("secrets", [])]
    if name not in declared:
        raise SystemExit(
            f"FAIL tenant '{tenant_name}' declares no secret '{name}' (declared: {', '.join(declared) or 'none'}); "
            "add it under secrets: in its tenant file first"
        )
    return t


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit(__doc__.split("Usage: ", 1)[1].strip())
    tenant_name, name, file = sys.argv[1:]
    t = find(tenant_name, name)
    text = Path(file).expanduser().read_text()
    names = keys(text)
    target = f"/run/platform-secrets/tenants/{tenant_name}/{name}.env"
    # The reconciler's container is the one that writes tenant credentials: root, secrets volume.
    stored = subprocess.run(
        [*COMPOSE, "run", "--rm", "-T", "--no-deps", "--entrypoint", "sh", "tenant-reconcile", "-c",
         f"mkdir -p $(dirname {target}) && cat > {target}.tmp"
         f" && chmod 0644 {target}.tmp && mv {target}.tmp {target}"],
        input=text.encode(), capture_output=True, check=False,
    )  # fmt: skip
    if stored.returncode:
        # Compose's own output only: the file went to stdin, never to a command line or a log.
        raise SystemExit(f"FAIL could not store it:\n{stored.stderr.decode()[-2000:]}")
    print(f"OK   {tenant_name}/{name}: {len(names)} key(s) stored ({', '.join(names)})")
    if when := expiry(text):
        print(f"     expires {when}: run this again with the renewed file before then")
    services = users(t, name)
    running = (
        subprocess.run(
            [*COMPOSE, "ps", "--status", "running", "--format", "{{.Service}}", *services],
            capture_output=True, text=True, check=False,
        ).stdout.split()
        if services
        else []
    )  # fmt: skip
    if running:
        subprocess.run([*COMPOSE, "restart", *running], check=True, capture_output=True)
        print(f"     restarted {', '.join(running)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
