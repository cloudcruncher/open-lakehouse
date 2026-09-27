# /// script
# requires-python = ">=3.12"
# dependencies = ["jsonschema>=4.23", "pyyaml>=6", "trino>=0.340", "httpx>=0.28"]
# ///
"""Contract checks: governance as code, enforced in CI and against the live platform.

  1. Every contract validates against the official ODCS JSON schema (v3.2.0, vendored).
  2. Policy tags agree with the masking policy, in both directions. Every pii.* or
     special_category column in a contract is masked by OPA with that tag, and every
     column OPA masks is declared in a contract. A new PII column can't ship unmasked,
     and a mask can't silently drift from its classification.
  3. With --tenant NAME (a tenant repo's CI, ADR 14): only tables in that tenant's own
     namespaces (tenants/NAME.yaml) may appear, and only masks on those tables are compared.
  4. With --live: the running tables match the contract (no missing or undeclared
     columns, same physical types). Schema drift fails `make verify`.

Usage: uv run contracts/check.py [--live]
       uv run contracts/check.py --tenant NAME --dir PATH   (the tenant-contracts workflow)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = json.loads(
    (ROOT / "contracts/schema/odcs-json-schema-v3.2.0.json").read_text()
)
ENTITLEMENTS = json.loads((ROOT / "infra/opa/data/entitlements.json").read_text())[
    "entitlements"
]


def policy_tag(tags: list[str]) -> str | None:
    found = [t for t in tags or [] if t.startswith("pii.") or t == "special_category"]
    return found[0] if found else None


def contracts(folder: Path = ROOT / "contracts") -> list[tuple[Path, dict]]:
    return [
        (p, yaml.safe_load(p.read_text())) for p in sorted(folder.glob("*.odcs.yaml"))
    ]


def tenant_namespaces(name: str) -> set[str]:
    path = ROOT / "tenants" / f"{name}.yaml"
    if not path.exists():
        raise SystemExit(f"no tenant {name!r}: onboard it first (tenants/README.md)")
    return set(yaml.safe_load(path.read_text())["namespaces"])


def check_static(
    folder: Path = ROOT / "contracts", namespaces: set[str] | None = None
) -> list[str]:
    """namespaces=None checks the platform's own contracts; a set scopes the check to a tenant."""
    errors = []
    declared: dict[str, dict[str, str]] = {}
    found = contracts(folder)
    if namespaces is not None and not found:
        errors.append(f"{folder}: no *.odcs.yaml contracts found")
    for path, c in found:
        for e in jsonschema.Draft201909Validator(SCHEMA).iter_errors(c):
            errors.append(
                f"{path.name}: schema: {'/'.join(map(str, e.absolute_path))}: {e.message[:160]}"
            )
        for obj in c.get("schema", []):
            table = obj.get("physicalName", "").removeprefix("lakehouse.")
            if namespaces is not None and table.split(".")[0] not in namespaces:
                errors.append(
                    f"{path.name}: {table or '(no physicalName)'} is outside this tenant's "
                    f"namespaces {sorted(namespaces)}"
                )
                continue
            for prop in obj.get("properties", []):
                tag = policy_tag(prop.get("tags", []))
                if tag:
                    declared.setdefault(table, {})[prop["name"]] = tag

    masked = {
        t: cols
        for t, cols in ENTITLEMENTS["column_tags"].items()
        if namespaces is None or t.split(".")[0] in namespaces
    }
    for table, cols in declared.items():
        for col, tag in cols.items():
            have = masked.get(table, {}).get(col)
            if have != tag:
                errors.append(
                    f"policy drift: {table}.{col} is {tag} in the contract but OPA has {have!r}"
                )
    for table, cols in masked.items():
        for col, tag in cols.items():
            if declared.get(table, {}).get(col) != tag:
                errors.append(
                    f"policy drift: OPA masks {table}.{col} as {tag} but no contract declares it"
                )
    return errors


def check_live() -> list[str]:
    import httpx
    import trino
    from trino.auth import JWTAuthentication

    env = dict(
        line.split("=", 1)
        for line in (ROOT / ".env").read_text().splitlines()
        if "=" in line
    )
    tok = httpx.post(
        "http://localhost:8280/realms/bank/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "trino-cli",
            "username": "ops_admin",
            "password": env["DEMO_USER_PASSWORD"],
        },
        timeout=10,
    ).json()["access_token"]
    cur = trino.dbapi.connect(
        host="localhost",
        port=8443,
        http_scheme="https",
        auth=JWTAuthentication(tok),
        verify=str(ROOT / ".secrets/ca.pem"),
        catalog="lakehouse",
    ).cursor()
    errors = []
    for path, c in contracts():
        for obj in c.get("schema", []):
            schema, table = obj["physicalName"].split(".")[1:]
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            )
            live = {n: t for n, t in cur.fetchall() if not n.startswith("_")}
            want = {p["name"]: p["physicalType"] for p in obj["properties"]}
            for col in want.keys() - live.keys():
                errors.append(
                    f"{path.name}: {schema}.{table}.{col} declared but missing from the table"
                )
            for col in live.keys() - want.keys():
                errors.append(
                    f"{path.name}: {schema}.{table}.{col} exists but is not in the contract"
                )
            for col in want.keys() & live.keys():
                if want[col] != live[col]:
                    errors.append(
                        f"{path.name}: {schema}.{table}.{col} type {live[col]} != contract {want[col]}"
                    )
    return errors


def arg(flag: str) -> str | None:
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv[:-1] else None


def main() -> int:
    tenant = arg("--tenant")
    folder = Path(arg("--dir") or ROOT / "contracts").resolve()
    namespaces = tenant_namespaces(tenant) if tenant else None
    errors = check_static(folder, namespaces)
    n = len(contracts(folder))
    if "--live" in sys.argv:
        errors += check_live()
    for e in errors:
        print(f"  FAIL {e}")
    mode = (
        "schema + policy tags + live tables"
        if "--live" in sys.argv
        else "schema + policy tags"
    )
    if tenant:
        mode += f", tenant {tenant}"
    print(
        f"contracts: {n} checked ({mode}): {'OK' if not errors else f'{len(errors)} problem(s)'}"
    )
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
