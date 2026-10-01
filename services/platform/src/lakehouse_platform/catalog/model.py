"""The data product catalog's model: what a consumer needs to know before using a table.

A product is one table in a data contract (ODCS v3.2) with everything around it: who owns it, what it
promises (purpose, limits, freshness), how it is built (upstream tables, implementation link), which
checks guard it, what each column means, and who may read what. No network here: live state (freshness,
check results) joins in `render`, so all of this is unit-tested in milliseconds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

UNIT_SECONDS = {
    "s": 1, "sec": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}  # fmt: skip
LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str
    classification: str
    tags: tuple[str, ...]
    primary_key: bool
    required: bool


@dataclass(frozen=True)
class Check:
    name: str
    description: str
    dimension: str
    severity: str


@dataclass(frozen=True)
class Link:
    url: str
    type: str
    description: str


@dataclass(frozen=True)
class Product:
    tenant: str
    domain: str
    contract: str
    version: str
    status: str
    tags: tuple[str, ...]
    purpose: str
    usage: str
    limitations: str
    namespace: str
    table: str
    description: str
    granularity: str
    columns: tuple[Column, ...]
    checks: tuple[Check, ...]
    upstream: tuple[str, ...]
    links: tuple[Link, ...]
    freshness_slo_seconds: int | None
    source_file: str = ""
    extra: dict = field(default_factory=dict, compare=False)

    @property
    def key(self) -> str:
        """`namespace.table`, the label the platform's metrics use."""
        return f"{self.namespace}.{self.table}"

    @property
    def layer(self) -> str:
        suffix = self.namespace.rsplit("_", 1)[-1]
        return suffix if suffix in LAYERS else "other"

    @property
    def path(self) -> str:
        return f"/products/{self.tenant}/{self.namespace}/{self.table}"


def seconds(value: float, unit: str) -> int | None:
    factor = UNIT_SECONDS.get(unit.strip().lower())
    return int(value * factor) if factor else None


def freshness_slo(contract: dict, table: str, namespace: str) -> int | None:
    """The promised maximum age of the table's newest commit, from ODCS `slaProperties`.

    An item applies to a table when its `element` names it (`table`, `namespace.table`, or
    `table.column`), or names nothing (the whole contract). The first matching item wins.
    """
    for sla in contract.get("slaProperties") or []:
        if str(sla.get("property", "")).lower() != "freshness":
            continue
        element = str(sla.get("element") or "")
        head = element.split(".")
        names = {table, f"{namespace}.{table}"}
        if element and not ({head[0], ".".join(head[:2])} & names):
            continue
        if isinstance(sla.get("value"), int | float) and sla.get("unit"):
            return seconds(sla["value"], str(sla["unit"]))
    return None


def _custom(items: list | None, name: str) -> str | None:
    for item in items or []:
        if item.get("property") == name:
            return str(item.get("value"))
    return None


def _column(p: dict) -> Column:
    return Column(
        name=p["name"],
        type=str(p.get("physicalType") or p.get("logicalType") or ""),
        description=str(p.get("description") or ""),
        classification=str(p.get("classification") or ""),
        tags=tuple(p.get("tags") or ()),
        primary_key=bool(p.get("primaryKey")),
        required=bool(p.get("required")),
    )


def products_in(contract: dict, source_file: str = "") -> list[Product]:
    """One product per table in a contract. Contract-level links apply to every table, and a table's
    own links come first."""
    desc = contract.get("description") or {}
    contract_links = [
        Link(d["url"], d["type"], d.get("description", ""))
        for d in contract.get("authoritativeDefinitions") or []
    ]
    found = []
    for obj in contract.get("schema") or []:
        physical = str(obj.get("physicalName") or obj["name"])
        parts = physical.split(".")
        namespace, table = (parts[-2], parts[-1]) if len(parts) >= 2 else ("", parts[-1])
        own = [
            Link(d["url"], d["type"], d.get("description", ""))
            for d in obj.get("authoritativeDefinitions") or []
        ]
        upstream = _custom(obj.get("customProperties"), "upstream") or ""
        found.append(
            Product(
                tenant=str(contract.get("tenant", "")),
                domain=str(contract.get("domain", "")),
                contract=str(contract.get("name", "")),
                version=str(contract.get("version", "")),
                status=str(contract.get("status", "")),
                tags=tuple(contract.get("tags") or ()) + tuple(obj.get("tags") or ()),
                purpose=str(desc.get("purpose", "")),
                usage=str(desc.get("usage", "")),
                limitations=str(desc.get("limitations", "")),
                namespace=namespace,
                table=table,
                description=str(obj.get("description") or ""),
                granularity=str(obj.get("dataGranularityDescription") or ""),
                columns=tuple(_column(p) for p in obj.get("properties") or []),
                checks=tuple(
                    Check(
                        str(q.get("name", "")),
                        str(q.get("description", "")),
                        str(q.get("dimension", "")),
                        str(q.get("severity", "")),
                    )
                    for q in obj.get("quality") or []
                ),
                upstream=tuple(u.strip() for u in upstream.split(",") if u.strip()),
                links=tuple(own + contract_links),
                freshness_slo_seconds=freshness_slo(contract, table, namespace),
                source_file=source_file,
            )
        )
    return found


def load_products(dirs: list[Path]) -> list[Product]:
    """Every product in every `*.odcs.yaml` under the given folders (a missing folder is skipped: the
    tenant-shipped contracts only exist after `make catalog-sync`)."""
    products: list[Product] = []
    for folder in dirs:
        for path in sorted(folder.rglob("*.odcs.yaml")) if folder.is_dir() else []:
            contract = yaml.safe_load(path.read_text())
            if isinstance(contract, dict) and contract.get("kind") == "DataContract":
                products += products_in(contract, str(path.relative_to(folder)))
    return sorted(
        products, key=lambda p: (p.tenant, LAYERS.index(p.layer) if p.layer in LAYERS else 9, p.key)
    )


def owners(tenants_dir: Path) -> dict[str, dict]:
    """tenant name -> its owner block (team, email, repo) from the tenant files."""
    found = {}
    for path in sorted(tenants_dir.glob("*.yaml")):
        t = yaml.safe_load(path.read_text())
        found[t["name"]] = t.get("owner", {})
    return found


# ------------------------------------------------------------------ live state, as data


def freshness_state(age_seconds: float | None, slo_seconds: int | None) -> str:
    """fresh: within the promise; late: within twice it; stale: beyond; unknown: nothing measured/promised."""
    if age_seconds is None or slo_seconds is None:
        return "unknown"
    if age_seconds <= slo_seconds:
        return "fresh"
    return "late" if age_seconds <= 2 * slo_seconds else "stale"


def human_age(seconds_: float | None) -> str:
    if seconds_ is None:
        return "not measured"
    s = int(seconds_)
    if s < 90:
        return f"{s} s ago"
    if s < 5400:
        return f"{round(s / 60)} min ago"
    if s < 129600:
        return f"{round(s / 3600)} h ago"
    return f"{round(s / 86400)} days ago"


def human_duration(seconds_: int | None) -> str:
    if seconds_ is None:
        return "no promise"
    if seconds_ % 86400 == 0:
        return f"{seconds_ // 86400} d"
    if seconds_ % 3600 == 0:
        return f"{seconds_ // 3600} h"
    return f"{seconds_ // 60} min"


# ------------------------------------------------------------------ who can see what


@dataclass(frozen=True)
class Access:
    persona: str
    users: tuple[str, ...]
    can_read: bool
    handling: tuple[tuple[str, str], ...]  # (column, "visible" | "masked" | "hidden (NULL)")


def _handle(tag: str, persona: dict) -> str | None:
    if tag == "special_category":
        return "visible" if persona.get("special_category") else "hidden (NULL)"
    if tag.startswith("pii."):
        return {"full": "visible", "partial": "masked"}.get(persona.get("pii", "none"), "hidden (NULL)")
    return None


def access_for(product: Product, entitlements: dict) -> list[Access]:
    """Each persona's access to the product, from the same entitlements OPA enforces: may they read the
    schema, and what happens to each personal-data column (tagged in the contract or in OPA's column tags)."""
    tags = entitlements.get("column_tags", {}).get(product.key, {})
    users: dict[str, list[str]] = {}
    for name, u in entitlements.get("users", {}).items():
        users.setdefault(u["persona"], []).append(name)
    out = []
    for name, persona in entitlements.get("personas", {}).items():
        can = product.namespace in persona.get("schemas", [])
        handling = []
        if can:
            for col in product.columns:
                for tag in (*col.tags, *([tags[col.name]] if col.name in tags else [])):
                    if (h := _handle(tag, persona)) is not None:
                        handling.append((col.name, h))
        out.append(Access(name, tuple(users.get(name, ())), can, tuple(handling)))
    return out
