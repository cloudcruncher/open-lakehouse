# /// script
# requires-python = ">=3.12"
# dependencies = ["jsonschema>=4.23", "pyyaml>=6"]
# ///
"""Restart one of a tenant's own services, or its code server (ADR 14), without a Docker socket for tenants.

A tenant can't stop or restart its own containers; the platform does it on request, but only for what the
tenant file declares: `code` is the code server, every other name must be a `services:` entry. A service
the tenant file does not deploy is refused rather than started.

Usage: uv run tenants/restart.py TENANT SERVICE   (or: make tenant-restart T=<tenant> S=<service>)
"""

from __future__ import annotations

import os
import subprocess
import sys

import check  # tenants/check.py, next to this file

DEFAULT_COMPOSE = "docker compose --profile *"  # every blueprint; no shell, so * is literal
COMPOSE = [*os.environ.get("COMPOSE", DEFAULT_COMPOSE).split(), "--profile", "tenant-code"]


def target(tenants: dict[str, dict], tenant: str, service: str) -> str:
    """The Compose service for a tenant's `code` server or one of its deployed `services:`."""
    if tenant not in tenants:
        raise SystemExit(f"FAIL no tenant '{tenant}' (tenants/*.yaml)")
    t = tenants[tenant]
    if service == "code":
        if not t.get("codeLocation", {}).get("deploy", False):
            raise SystemExit(f"FAIL tenant '{tenant}' does not deploy a code server (codeLocation.deploy)")
        return f"tenant-{tenant}-code"
    services = {s["name"]: s for s in t.get("services", [])}
    if service not in services:
        declared = ", ".join(["code", *services])
        raise SystemExit(f"FAIL tenant '{tenant}' has no service '{service}' (declared: {declared})")
    if not services[service].get("deploy", False):
        raise SystemExit(f"FAIL service '{service}' of '{tenant}' is not deployed (deploy: true in its tenant file)")
    return f"tenant-{tenant}-{service}"


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__.split("Usage: ", 1)[1].strip())
    tenant, service = sys.argv[1:]
    name = target({t["name"]: t for _, t in check.load()}, tenant, service)
    running = subprocess.run(
        [*COMPOSE, "ps", "-a", "--format", "{{.Service}}", name], capture_output=True, text=True, check=False
    ).stdout.split()
    if name not in running:
        raise SystemExit(f"FAIL {name} has no container on this stack (not started by `make up`?)")
    subprocess.run([*COMPOSE, "restart", name], check=True, capture_output=True)
    print(f"OK   restarted {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
