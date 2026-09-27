"""Declarative, idempotent tenant reconciliation: tenants/*.yaml -> Kafka, Polaris, Keycloak (ADR 14).

Onboarding a team is a reviewed PR that adds `tenants/<name>.yaml`. This reconciler makes the
running platform match those files, the same way `polaris.py` and `keycloak.py` do for the
platform's own resources:

  * Kafka      topics created; partitions only ever added (Kafka can't remove them, and adding
               moves keys, so a shrink is refused loudly); retention and cleanup policy fixed.
  * Polaris    the tenant's namespaces; an identity `tenant_<name>` whose writer role holds
               table rights in those namespaces only; read rights for the query engine.
               OPA still decides which colleague sees what, so new data starts closed.
  * Keycloak   a group `tenant-<name>`.

It never deletes. A topic or namespace that no tenant declares any more is reported as an orphan;
dropping data is a deliberate, manual step. Files are validated with the same checker CI runs
(`tenants/check.py`), and nothing is applied if any file is invalid.
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

log = logging.getLogger("tenant-reconcile")

TENANTS_DIR = Path(os.environ.get("TENANTS_DIR", "/etc/tenants"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "bank")
PLATFORM_TOPIC_PREFIXES = ("corebank.", "contact-centre.")

# Inside its own namespaces a tenant manages tables and namespace properties (leases such as
# the silver writer lease). NAMESPACE_FULL_METADATA is withheld: it includes dropping the
# namespace, which stays a platform decision.
TENANT_WRITER_PRIVILEGES = [
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "NAMESPACE_WRITE_PROPERTIES",
    "TABLE_FULL_METADATA",
    "TABLE_READ_DATA",
    "TABLE_WRITE_DATA",
    "VIEW_LIST",
    "VIEW_READ_PROPERTIES",
]


# ------------------------------------------------------------------ model


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    configs: dict[str, str]


@dataclass(frozen=True)
class Tenant:
    name: str
    domain: str
    owner_email: str
    repo: str
    topics: tuple[TopicSpec, ...]
    namespaces: tuple[str, ...]

    @property
    def ident(self) -> str:
        """Polaris entity names can't carry '-'."""
        return f"tenant_{self.name.replace('-', '_')}"

    @property
    def group(self) -> str:
        return f"tenant-{self.name}"

    @property
    def secrets_subdir(self) -> str:
        """Inside the platform secrets volume; the tenant's code server mounts only this folder."""
        return f"tenants/{self.name}"


def topic_spec(t: dict[str, Any]) -> TopicSpec:
    return TopicSpec(
        name=t["name"],
        partitions=t["partitions"],
        configs={
            "retention.ms": str(t["retentionHours"] * 3_600_000),
            "cleanup.policy": t.get("cleanupPolicy", "delete"),
        },
    )


def load_checker(dir: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("tenant_check", dir / "check.py")
    if spec is None or spec.loader is None:
        raise SystemExit(f"no tenant checker at {dir / 'check.py'}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_tenants(dir: Path) -> list[Tenant]:
    check = load_checker(dir)
    errors = check.check(dir)
    if errors:
        for e in errors:
            log.error("invalid tenant file: %s", e)
        raise SystemExit(f"{len(errors)} tenant file error(s); nothing applied")
    return [
        Tenant(
            name=t["name"],
            domain=t["domain"],
            owner_email=t["owner"]["email"],
            repo=t["owner"]["repo"],
            topics=tuple(topic_spec(x) for x in t["topics"]),
            namespaces=tuple(t["namespaces"]),
        )
        for _, t in check.load(dir)
    ]


# ------------------------------------------------------------------ Kafka


@dataclass(frozen=True)
class TopicState:
    partitions: int
    configs: dict[str, str]


@dataclass
class TopicPlan:
    create: list[TopicSpec] = field(default_factory=list)
    add_partitions: dict[str, int] = field(default_factory=dict)
    set_configs: dict[str, dict[str, str]] = field(default_factory=dict)
    refused: list[str] = field(default_factory=list)

    @property
    def changes(self) -> int:
        return len(self.create) + len(self.add_partitions) + len(self.set_configs)


def plan_topics(desired: list[TopicSpec], existing: dict[str, TopicState]) -> TopicPlan:
    plan = TopicPlan()
    for spec in desired:
        have = existing.get(spec.name)
        if have is None:
            plan.create.append(spec)
            continue
        if spec.partitions > have.partitions:
            plan.add_partitions[spec.name] = spec.partitions
        elif spec.partitions < have.partitions:
            plan.refused.append(
                f"{spec.name}: asks for {spec.partitions} partitions but has {have.partitions}; "
                "Kafka can't remove partitions (recreate the topic deliberately if needed)"
            )
        if any(have.configs.get(k) != v for k, v in spec.configs.items()):
            plan.set_configs[spec.name] = spec.configs
    return plan


def orphan_topics(tenants: list[Tenant], existing: list[str]) -> list[str]:
    declared = {t.name for tenant in tenants for t in tenant.topics}
    return sorted(
        name
        for name in existing
        if "." in name and not name.startswith(("_", *PLATFORM_TOPIC_PREFIXES)) and name not in declared
    )


async def kafka_state(admin: Any, names: list[str]) -> dict[str, TopicState]:
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

    present = [n for n in names if n in set(await admin.list_topics())]
    if not present:
        return {}
    partitions = {t["topic"]: len(t["partitions"]) for t in await admin.describe_topics(present)}
    configs: dict[str, dict[str, str]] = {}
    responses = await admin.describe_configs(
        [
            ConfigResource(ConfigResourceType.TOPIC, n, {"retention.ms": None, "cleanup.policy": None})
            for n in present
        ]
    )
    for resp in responses:
        for resource in resp.resources:
            # (error_code, error_message, resource_type, resource_name, config_entries)
            configs[resource[3]] = {entry[0]: entry[1] for entry in resource[4]}
    return {n: TopicState(partitions[n], configs.get(n, {})) for n in present}


async def reconcile_kafka(tenants: list[Tenant], bootstrap: str) -> tuple[int, list[str]]:
    from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

    desired = [spec for tenant in tenants for spec in tenant.topics]
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        plan = plan_topics(desired, await kafka_state(admin, [s.name for s in desired]))
        if plan.create:
            resp = await admin.create_topics(
                [NewTopic(s.name, s.partitions, 1, topic_configs=s.configs) for s in plan.create]
            )
            for topic, code, *_ in resp.topic_errors:
                if code not in (0, 36):  # 36: TOPIC_ALREADY_EXISTS (a concurrent run won the race)
                    raise RuntimeError(f"create topic {topic}: Kafka error code {code}")
            log.info("kafka: created %s", ", ".join(s.name for s in plan.create))
        if plan.add_partitions:
            await admin.create_partitions({n: NewPartitions(c) for n, c in plan.add_partitions.items()})
            log.info("kafka: added partitions %s", plan.add_partitions)
        if plan.set_configs:
            # Legacy AlterConfigs replaces every non-default setting, so the full desired set is sent.
            await admin.alter_configs(
                [ConfigResource(ConfigResourceType.TOPIC, n, c) for n, c in plan.set_configs.items()]
            )
            log.info("kafka: configs fixed on %s", ", ".join(plan.set_configs))
        for message in plan.refused:
            log.error("kafka: refused %s", message)
        for name in orphan_topics(tenants, await admin.list_topics()):
            log.warning("kafka: orphan topic %s (no tenant declares it; not deleted)", name)
        if not plan.changes and not plan.refused:
            log.info("kafka: %d topic(s) in sync", len(desired))
        return plan.changes, plan.refused
    finally:
        await admin.close()


# ------------------------------------------------------------------ Polaris


def orphan_namespaces(tenants: list[Tenant], existing: list[str], platform: set[str]) -> list[str]:
    declared = {ns for tenant in tenants for ns in tenant.namespaces}
    return sorted(set(existing) - declared - platform)


def reconcile_polaris(tenants: list[Tenant]) -> int:
    # Imported here: the module reads the Polaris root secret from the environment at import.
    from lakehouse_platform.bootstrap import polaris as pb

    p = pb.Polaris(pb.POLARIS_URL, pb.REALM)
    p.login()
    base = f"/api/management/v1/catalogs/{pb.CATALOG}"
    existing = [
        ns[0] for ns in p.call("GET", f"/api/catalog/v1/{pb.CATALOG}/namespaces").get("namespaces", [])
    ]
    created = 0
    for tenant in tenants:
        for ns in tenant.namespaces:
            if ns not in existing:
                p.call(
                    "POST",
                    f"/api/catalog/v1/{pb.CATALOG}/namespaces",
                    {
                        "namespace": [ns],
                        "properties": {
                            "owner.tenant": tenant.name,
                            "owner.email": tenant.owner_email,
                            "owner.repo": tenant.repo,
                        },
                    },
                )
                created += 1
                log.info("polaris: created namespace %s for %s", ns, tenant.name)

        engine = pb.Engine(tenant.ident, tenant.ident, f"{tenant.ident}_writer", "POLARIS")
        p.call("POST", f"{base}/catalog-roles", {"catalogRole": {"name": engine.catalog_role}})
        p.call(
            "POST", "/api/management/v1/principal-roles", {"principalRole": {"name": engine.principal_role}}
        )
        p.call(
            "PUT",
            f"/api/management/v1/principal-roles/{engine.principal_role}/catalog-roles/{pb.CATALOG}",
            {"catalogRole": {"name": engine.catalog_role}},
        )
        # Namespace names only, as for the query engine; needed to find its own namespaces.
        pb.grant(p, engine.catalog_role, {"type": "catalog", "privilege": "NAMESPACE_LIST"})
        for ns in tenant.namespaces:
            for privilege in TENANT_WRITER_PRIVILEGES:
                pb.grant(
                    p, engine.catalog_role, {"type": "namespace", "namespace": [ns], "privilege": privilege}
                )
            for privilege in pb.READER_PRIVILEGES:
                pb.grant(
                    p, "lakehouse_reader", {"type": "namespace", "namespace": [ns], "privilege": privilege}
                )
        # Credentials land in the secrets volume under tenants/<name>/, rotated if they stop working.
        pb.ensure_principal(p, engine, pb.SECRETS_DIR / tenant.secrets_subdir / "polaris.env")
        log.info("polaris: %s can write %s only", engine.principal, ", ".join(tenant.namespaces))

    for ns in orphan_namespaces(tenants, existing, set(pb.NAMESPACES)):
        log.warning("polaris: orphan namespace %s (no tenant declares it; not dropped)", ns)
    return created


# ------------------------------------------------------------------ Keycloak


def reconcile_keycloak(tenants: list[Tenant]) -> int:
    from lakehouse_platform.bootstrap.keycloak import Admin

    kc = Admin(KEYCLOAK_URL, KEYCLOAK_REALM)
    kc.login(os.environ.get("KEYCLOAK_ADMIN_USER", "admin"), os.environ["KEYCLOAK_ADMIN_PASSWORD"])
    created = 0
    for tenant in tenants:
        if any(g["name"] == tenant.group for g in kc.get("/groups", search=tenant.group, exact="true")):
            continue
        kc.send(
            "POST",
            "/groups",
            {
                "name": tenant.group,
                "attributes": {"tenant": [tenant.name], "owner": [tenant.owner_email], "repo": [tenant.repo]},
            },
        )
        created += 1
        log.info("keycloak: created group %s", tenant.group)
    return created


# ------------------------------------------------------------------ main


PROBE_TABLE = "verify_probe"
PROBE_SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}],
}


def probe(tenant: Tenant) -> int:
    """Sign in as the tenant's own identity: its namespace must accept a table, a platform one must not."""
    from lakehouse_platform.bootstrap import polaris as pb

    creds = pb.read_env_file(pb.SECRETS_DIR / tenant.secrets_subdir / "polaris.env")
    p = pb.Polaris(pb.POLARIS_URL, pb.REALM)
    token = p.token(creds["POLARIS_CLIENT_ID"], creds["POLARIS_CLIENT_SECRET"])
    p.headers["Authorization"] = f"Bearer {token}"
    base = f"{p.base}/api/catalog/v1/{pb.CATALOG}/namespaces"

    def create(ns: str) -> int:
        body = {"name": PROBE_TABLE, "schema": PROBE_SCHEMA}
        return p.http.post(f"{base}/{ns}/tables", headers=p.headers, json=body).status_code

    own = tenant.namespaces[0]
    own_status = create(own)
    if own_status == 200:
        p.http.delete(f"{base}/{own}/tables/{PROBE_TABLE}", headers=p.headers)
    platform_status = create("silver")
    print(f"probe {tenant.ident}: create in {own} -> {own_status}; create in silver -> {platform_status}")
    return 0 if own_status == 200 and platform_status == 403 else 1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    tenants = load_tenants(TENANTS_DIR)
    if sys.argv[1:2] == ["--probe"]:
        return sum(probe(t) for t in tenants)
    changes = reconcile_polaris(tenants)
    changes += reconcile_keycloak(tenants)
    kafka_changes, refused = asyncio.run(reconcile_kafka(tenants, KAFKA_BOOTSTRAP))
    changes += kafka_changes
    log.info(
        "tenants reconciled: %d tenant(s), %d change(s), %d refused", len(tenants), changes, len(refused)
    )
    return 1 if refused else 0


if __name__ == "__main__":
    sys.exit(main())
