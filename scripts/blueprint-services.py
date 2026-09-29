#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Print the services of the given blueprints (Compose profiles), one per line.

`tenants` means the tenant-code profile. Reads the whole Compose config, so a blueprint whose
services depend on another profile (observability reads Kafka) resolves. Exits non-zero on an
unknown name or a Compose error, so `make stop` never reports success for a typo.
"""

import json
import subprocess
import sys

ALIASES = {"tenants": "tenant-code"}

wanted = {ALIASES.get(a, a) for a in sys.argv[1:]}
if not wanted:
    sys.exit("usage: blueprint-services.py <blueprint>...")
config = subprocess.run(
    ["docker", "compose", "--profile", "*", "config", "--format", "json"],
    check=True, capture_output=True, text=True,
).stdout
services = json.loads(config)["services"]
known = {p for s in services.values() for p in s.get("profiles", [])}
if unknown := wanted - known:
    sys.exit(f"unknown blueprint(s): {', '.join(sorted(unknown))}. Known: {', '.join(sorted(known))}")
print("\n".join(sorted(n for n, s in services.items() if wanted & set(s.get("profiles", [])))))
