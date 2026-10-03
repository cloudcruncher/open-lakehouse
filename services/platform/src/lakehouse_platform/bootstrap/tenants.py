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
KAFKA_BOOTSTRAP_SASL = os.environ.get("KAFKA_SASL_BOOTSTRAP", "kafka:9094")
KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "bank")
PLATFORM_TOPIC_PREFIXES = ("corebank.", "contact-centre.")

# Inside its own namespaces a tenant manages tables, views and namespace properties (leases such as
# the silver writer lease): create, rename, drop, without asking the platform. Views written by
# Spark use Spark's SQL dialect, which Trino refuses to read, so a tenant's shared interface is a
# table (a replay swap is two renames). NAMESPACE_FULL_METADATA is withheld: it includes dropping
# the namespace, which stays a platform decision.
TENANT_WRITER_PRIVILEGES = [
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "NAMESPACE_WRITE_PROPERTIES",
    "TABLE_FULL_METADATA",
    "TABLE_READ_DATA",
    "TABLE_WRITE_DATA",
    "VIEW_CREATE",
    "VIEW_FULL_METADATA",
    "VIEW_LIST",
    "VIEW_READ_PROPERTIES",
]

# Granted to the query engine on tenant namespaces only; Trino's OPA policy limits who uses them.
TRINO_VIEW_PRIVILEGES = ["VIEW_CREATE", "VIEW_FULL_METADATA"]


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
    def kafka_user(self) -> str:
        """SCRAM user on the SASL listener; ACLs name it as User:<kafka_user>."""
        return f"tenant-{self.name}"

    @property
    def kafka_group_prefix(self) -> str:
        """Consumer groups a tenant may use on the SASL listener (e.g. Spark's groupIdPrefix)."""
        return f"{self.name}-"

    @property
    def trino_client(self) -> str:
        """Keycloak client whose service account is the tenant's Trino identity."""
        return f"tenant-{self.name}"

    @property
    def trino_user(self) -> str:
        """Trino's principal (preferred_username) for that service account; OPA keys on it."""
        return f"service-account-{self.trino_client}"

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


# ------------------------------------------------------------------ Kafka access (SCRAM + ACLs)

KAFKA_SASL = KAFKA_BOOTSTRAP_SASL
SCRAM_MECHANISM = "SCRAM-SHA-512"


def desired_acls(tenant: Tenant) -> list[tuple[str, str, str, str]]:
    """(resource type, name, pattern, operation) a tenant's principal may use: its topics and group prefix."""
    acls = [("TOPIC", t.name, "LITERAL", op) for t in tenant.topics for op in TOPIC_OPERATIONS]
    acls += [("GROUP", tenant.kafka_group_prefix, "PREFIXED", op) for op in GROUP_OPERATIONS]
    return acls


TOPIC_OPERATIONS = ("READ", "WRITE", "DESCRIBE", "DESCRIBE_CONFIGS")
GROUP_OPERATIONS = ("READ", "DESCRIBE")


def reconcile_kafka_access(tenants: list[Tenant], bootstrap: str) -> int:
    """A SCRAM user per tenant (password in its own secrets folder as kafka.env) and ACLs on its own topics.

    Additive: stale ACLs are not removed. The trusted listener (platform clients) is untouched.
    """
    import secrets as pysecrets

    from confluent_kafka.admin import (
        AclBinding,
        AclOperation,
        AclPermissionType,
        AdminClient,
        ResourcePatternType,
        ResourceType,
        ScramCredentialInfo,
        ScramMechanism,
        UserScramCredentialUpsertion,
    )

    from lakehouse_platform.bootstrap import polaris as pb

    admin = AdminClient({"bootstrap.servers": bootstrap})
    have = admin.describe_user_scram_credentials([t.kafka_user for t in tenants])
    changes = 0
    for tenant in tenants:
        file = pb.SECRETS_DIR / tenant.secrets_subdir / "kafka.env"
        stored = pb.read_env_file(file).get("KAFKA_PASSWORD")
        try:
            known = bool(have[tenant.kafka_user].result().scram_credential_infos)
        except Exception:  # noqa: BLE001  (a user with no credentials yet raises; that is the "create" case)
            known = False
        if not (stored and known):
            password = stored or pysecrets.token_urlsafe(32)
            upsert = UserScramCredentialUpsertion(
                tenant.kafka_user, ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), password.encode()
            )
            for future in admin.alter_user_scram_credentials([upsert]).values():
                future.result()
            pb.write_env_file(
                file,
                {
                    "KAFKA_BOOTSTRAP": KAFKA_SASL,
                    "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
                    "KAFKA_SASL_MECHANISM": SCRAM_MECHANISM,
                    "KAFKA_USERNAME": tenant.kafka_user,
                    "KAFKA_PASSWORD": password,
                },
            )
            changes += 1
            log.info("kafka: SCRAM user %s %s", tenant.kafka_user, "created" if not stored else "re-created")
        bindings = [
            AclBinding(
                ResourceType[kind],
                name,
                ResourcePatternType[pattern],
                f"User:{tenant.kafka_user}",
                "*",
                AclOperation[op],
                AclPermissionType.ALLOW,
            )
            for kind, name, pattern, op in desired_acls(tenant)
        ]
        for future in admin.create_acls(bindings).values():  # idempotent: re-creating an ACL is a no-op
            future.result()
        names = ", ".join(t.name for t in tenant.topics) or "no topics"
        log.info("kafka: %s may use %s", tenant.kafka_user, names)
    return changes


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
            # Trino creates a tenant's views as the query engine; OPA decides who may (own namespaces only).
            for privilege in (*pb.READER_PRIVILEGES, *TRINO_VIEW_PRIVILEGES):
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
    created += sum(ensure_trino_identity(kc, t) for t in tenants)
    return created


def ensure_trino_identity(kc: Any, tenant: Tenant) -> int:
    """A client-credentials client per tenant, audience `trino`; its secret lands in the tenant's folder."""
    from lakehouse_platform.bootstrap import polaris as pb
    from lakehouse_platform.bootstrap.keycloak import reconcile_client

    existed = bool(kc.get("/clients", clientId=tenant.trino_client))
    reconcile_client(kc, trino_client_spec(tenant))
    uuid = kc.get("/clients", clientId=tenant.trino_client)[0]["id"]
    secret = kc.get(f"/clients/{uuid}/client-secret")["value"]
    file = pb.SECRETS_DIR / tenant.secrets_subdir / "trino.env"
    values = {"TRINO_CLIENT_ID": tenant.trino_client, "TRINO_CLIENT_SECRET": secret}
    if pb.read_env_file(file) != values:
        pb.write_env_file(file, values)
    return 0 if existed else 1


def trino_client_spec(tenant: Tenant) -> dict[str, Any]:
    return {
        "clientId": tenant.trino_client,
        "name": f"Trino identity of tenant {tenant.name} (own namespaces only, see OPA)",
        "publicClient": False,
        "standardFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": True,
        "protocolMappers": [
            {
                "name": "audience-trino",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.client.audience": "trino",
                    "access.token.claim": "true",
                    "id.token.claim": "false",
                },
            }
        ],
    }


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


def sasl_config(tenant: Tenant) -> dict[str, Any]:
    from lakehouse_platform.bootstrap import polaris as pb

    creds = pb.read_env_file(pb.SECRETS_DIR / tenant.secrets_subdir / "kafka.env")
    return {
        "bootstrap.servers": creds["KAFKA_BOOTSTRAP"],
        "security.protocol": creds["KAFKA_SECURITY_PROTOCOL"],
        "sasl.mechanism": creds["KAFKA_SASL_MECHANISM"],
        "sasl.username": creds["KAFKA_USERNAME"],
        "sasl.password": creds["KAFKA_PASSWORD"],
    }


def visible_topics(tenant: Tenant) -> set[str]:
    """Topics the tenant may DESCRIBE on the SASL listener (Kafka hides the rest from the metadata)."""
    from confluent_kafka.admin import AdminClient

    return set(AdminClient(sasl_config(tenant)).list_topics(timeout=10).topics)


def refused_write(tenant: Tenant, topic: str) -> str:
    """Try to produce to `topic` as the tenant; Kafka's error name. A refused write leaves nothing behind."""
    from confluent_kafka import Producer

    result: list[str] = []
    producer = Producer({**sasl_config(tenant), "message.timeout.ms": 8000})
    producer.produce(topic, b"probe", callback=lambda err, _msg: result.append(err.name() if err else "ok"))
    producer.flush(10)
    return result[0] if result else "timeout"


def probe_kafka(tenants: list[Tenant]) -> int:
    """On the SASL listener each tenant sees its own topics only, and is refused writing another tenant's.

    The allowed side is checked by visibility, never by writing: a probe record would land in a real topic.
    """
    failures = 0
    for tenant in tenants:
        own = {t.name for t in tenant.topics}
        others = sorted(t.name for o in tenants if o is not tenant for t in o.topics)
        seen = visible_topics(tenant) - {"__consumer_offsets"}
        refused = refused_write(tenant, others[0]) if others else "TOPIC_AUTHORIZATION_FAILED"
        print(f"probe-kafka {tenant.kafka_user}: sees {sorted(seen)}; write {others[:1]} -> {refused}")
        failures += not (seen == own and refused == "TOPIC_AUTHORIZATION_FAILED")
    return failures


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    tenants = load_tenants(TENANTS_DIR)
    if sys.argv[1:2] == ["--probe"]:
        return sum(probe(t) for t in tenants)
    if sys.argv[1:2] == ["--probe-kafka"]:
        return probe_kafka(tenants)
    changes = reconcile_polaris(tenants)
    changes += reconcile_keycloak(tenants)
    kafka_changes, refused = asyncio.run(reconcile_kafka(tenants, KAFKA_BOOTSTRAP))
    changes += kafka_changes
    changes += reconcile_kafka_access(tenants, KAFKA_BOOTSTRAP)
    log.info(
        "tenants reconciled: %d tenant(s), %d change(s), %d refused", len(tenants), changes, len(refused)
    )
    return 1 if refused else 0


if __name__ == "__main__":
    sys.exit(main())
