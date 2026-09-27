# /// script
# requires-python = ">=3.12"
# dependencies = ["jsonschema>=4.23", "pyyaml>=6"]
# ///
"""Tenant registry checks: onboarding is a PR, so a bad tenant file must fail CI here.

  1. Every tenants/*.yaml validates against tenants/schema/tenant.schema.json.
  2. The file is named after the tenant, and topics and namespaces sit inside the
     tenant's own domain prefix.
  3. No tenant takes a platform name or another tenant's name or domain.

Usage: uv run tenants/check.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import jsonschema
import yaml

# Relative to this file, so the platform's reconciler can load it from a mounted copy.
TENANTS = Path(__file__).resolve().parent
SCHEMA = json.loads((TENANTS / "schema/tenant.schema.json").read_text())

# Names the platform already uses: Polaris namespaces (bootstrap/polaris.py), Kafka topic
# prefixes (kafka-init and the CDC connector in compose.yaml), and roles it keeps for itself.
PLATFORM_NAMESPACES = {"bronze", "silver", "gold", "ops"}
RESERVED_DOMAINS = {
    "corebank",
    "core",
    "contact",
    "platform",
    "ops",
    "audit",
    "connect",
    "kafka",
}


def load(dir: Path = TENANTS) -> list[tuple[Path, dict]]:
    return [(p, yaml.safe_load(p.read_text())) for p in sorted(dir.glob("*.yaml"))]


def check_one(path: Path, t: dict) -> list[str]:
    errors = [
        f"{path.name}: schema: {'/'.join(map(str, e.absolute_path)) or '(root)'}: {e.message[:160]}"
        for e in jsonschema.Draft202012Validator(SCHEMA).iter_errors(t)
    ]
    if errors or not isinstance(t, dict):
        return errors or [f"{path.name}: not a mapping"]
    name, domain = t["name"], t["domain"]
    if path.stem != name:
        errors.append(f"{path.name}: file must be named {name}.yaml")
    if domain in RESERVED_DOMAINS:
        errors.append(f"{path.name}: domain '{domain}' is reserved by the platform")
    for topic in t["topics"]:
        if not topic["name"].startswith(f"{domain}."):
            errors.append(
                f"{path.name}: topic {topic['name']} is outside the domain '{domain}.'"
            )
    for ns in t["namespaces"]:
        if ns in PLATFORM_NAMESPACES or not ns.startswith(f"{domain}_"):
            errors.append(
                f"{path.name}: namespace {ns} is outside the domain '{domain}_'"
            )
    names = [svc["name"] for svc in t.get("services", [])]
    for dup in sorted({n for n in names if names.count(n) > 1}):
        errors.append(f"{path.name}: service '{dup}' is declared twice")
    if "code" in names:
        errors.append(f"{path.name}: service name 'code' is taken by the code server")
    secrets = [x["name"] for x in t.get("secrets", [])]
    for dup in sorted({n for n in secrets if secrets.count(n) > 1}):
        errors.append(f"{path.name}: secret '{dup}' is declared twice")
    if "polaris" in secrets:
        errors.append(f"{path.name}: secret name 'polaris' is taken by the platform's credentials")
    for svc in t.get("services", []):
        for name in svc.get("secrets", []):
            if name not in secrets:
                errors.append(
                    f"{path.name}: service '{svc['name']}' uses secret '{name}', "
                    "which the tenant does not declare under secrets"
                )
    return errors


def check(dir: Path = TENANTS) -> list[str]:
    errors: list[str] = []
    seen: dict[str, dict[str, str]] = {"name": {}, "domain": {}}
    for path, t in load(dir):
        own = check_one(path, t)
        errors += own
        if own:
            continue
        for key in seen:
            other = seen[key].setdefault(t[key], path.name)
            if other != path.name:
                errors.append(
                    f"{path.name}: {key} '{t[key]}' is already taken by {other}"
                )
    return errors


def main() -> int:
    errors = check()
    for e in errors:
        print(f"FAIL {e}")
    if not errors:
        print(f"OK   {len(load())} tenant(s) valid")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
