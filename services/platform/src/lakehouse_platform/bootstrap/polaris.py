"""Declarative, idempotent Polaris setup: catalog, namespaces, identities and grants.

Safe to run on every `make up`. Each resource is created only if missing; engine
credentials are reused while they still work and reset if they don't, so a lost
secrets volume heals itself on the next run.

Security model (two layers):
  * Polaris (this file) decides which *engine* may touch which *namespace*, and
    vends short-lived storage credentials scoped to those tables.
  * OPA (infra/opa) decides which *colleague* sees which rows and columns inside
    the engine.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential_jitter

log = logging.getLogger("polaris-bootstrap")

POLARIS_URL = os.environ.get("POLARIS_URL", "http://polaris:8181")
REALM = os.environ.get("POLARIS_REALM", "bank")
ROOT_CLIENT_ID = os.environ.get("POLARIS_ROOT_CLIENT_ID", "root")
ROOT_CLIENT_SECRET = os.environ["POLARIS_ROOT_CLIENT_SECRET"]
CATALOG = os.environ.get("POLARIS_CATALOG", "lakehouse")
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
KMS_UNAVAILABLE = os.environ.get("S3_KMS_AVAILABLE", "false").lower() != "true"
STS_ROLE_ARN = os.environ.get("STS_ROLE_ARN", "arn:aws:iam::000000000000:role/lakehouse-vended")
SECRETS_DIR = Path(os.environ.get("PLATFORM_SECRETS_DIR", "/run/platform-secrets"))

NAMESPACES = ["bronze", "silver", "gold", "ops"]

# Read access for the query engine: data + listing. OPA decides which colleague sees what.
READER_PRIVILEGES = [
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_LIST",
    "TABLE_READ_PROPERTIES",
    "TABLE_READ_DATA",
    # Trino's SHOW TABLES and information_schema list views alongside tables.
    "VIEW_LIST",
    "VIEW_READ_PROPERTIES",
]


@dataclass(frozen=True)
class Engine:
    principal: str
    principal_role: str
    catalog_role: str
    env_prefix: str


ENGINES = [
    Engine("spark_etl", "etl_pipelines", "lakehouse_writer", "SPARK_POLARIS"),
    Engine("trino_query_engine", "query_engines", "lakehouse_reader", "TRINO_POLARIS"),
]


class Polaris:
    def __init__(self, base_url: str, realm: str) -> None:
        self.base = base_url.rstrip("/")
        self.headers = {"Polaris-Realm": realm}
        self.http = httpx.Client(timeout=15.0)

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_delay(180),
        reraise=True,
    )
    def token(self, client_id: str, client_secret: str) -> str:
        resp = self.http.post(
            f"{self.base}/api/catalog/v1/oauth/tokens",
            headers=self.headers,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def login(self) -> None:
        self.headers["Authorization"] = f"Bearer {self.token(ROOT_CLIENT_ID, ROOT_CLIENT_SECRET)}"

    def credentials_work(self, client_id: str, client_secret: str) -> bool:
        resp = self.http.post(
            f"{self.base}/api/catalog/v1/oauth/tokens",
            headers={"Polaris-Realm": self.headers["Polaris-Realm"]},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        )
        return resp.status_code == 200

    def call(self, method: str, path: str, body: dict | None = None, ok_if_exists: bool = True) -> dict:
        resp = self.http.request(method, f"{self.base}{path}", headers=self.headers, json=body)
        if resp.status_code == 409 and ok_if_exists:
            return {}
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text}")
        return resp.json() if resp.content else {}

    def exists(self, path: str) -> bool:
        return self.http.get(f"{self.base}{path}", headers=self.headers).status_code == 200


def desired_storage_config() -> dict:
    base = f"s3://{BUCKET}/warehouse"
    return {
        "storageType": "S3",
        "allowedLocations": [base],
        "roleArn": STS_ROLE_ARN,
        "region": S3_REGION,
        "endpoint": S3_ENDPOINT,
        "stsEndpoint": S3_ENDPOINT,
        "pathStyleAccess": True,
        # S3-compatible stores without KMS reject session policies that mention KMS.
        # On AWS set this to False and configure currentKmsKey for SSE-KMS.
        "kmsUnavailable": KMS_UNAVAILABLE,
    }


def ensure_catalog(p: Polaris) -> None:
    """Create the catalog, or reconcile its storage config if it has drifted."""
    desired = desired_storage_config()
    path = f"/api/management/v1/catalogs/{CATALOG}"
    if not p.exists(path):
        base = desired["allowedLocations"][0]
        p.call("POST", "/api/management/v1/catalogs", {"catalog": {
            "name": CATALOG,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {"default-base-location": base},
            "storageConfigInfo": desired,
        }})
        log.info("created catalog %s at %s", CATALOG, base)
        return

    current = p.call("GET", path)
    actual = current.get("storageConfigInfo", {})
    drift = {k: (actual.get(k), v) for k, v in desired.items() if actual.get(k) != v}
    if not drift:
        log.info("catalog %s exists and matches desired state", CATALOG)
        return
    p.call("PUT", path, {
        "currentEntityVersion": current["entityVersion"],
        "storageConfigInfo": desired,
    })
    log.warning("catalog %s storage config reconciled: %s", CATALOG, sorted(drift))


def ensure_namespaces(p: Polaris) -> None:
    for ns in NAMESPACES:
        p.call("POST", f"/api/catalog/v1/{CATALOG}/namespaces", {"namespace": [ns], "properties": {}})
    log.info("namespaces ensured: %s", ", ".join(NAMESPACES))


def grant(p: Polaris, catalog_role: str, grant_body: dict) -> None:
    p.call(
        "PUT",
        f"/api/management/v1/catalogs/{CATALOG}/catalog-roles/{catalog_role}/grants",
        {"grant": grant_body},
    )


def ensure_roles_and_grants(p: Polaris) -> None:
    for engine in ENGINES:
        p.call(
            "POST",
            f"/api/management/v1/catalogs/{CATALOG}/catalog-roles",
            {"catalogRole": {"name": engine.catalog_role}},
        )
        p.call(
            "POST", "/api/management/v1/principal-roles", {"principalRole": {"name": engine.principal_role}}
        )
        p.call(
            "PUT",
            f"/api/management/v1/principal-roles/{engine.principal_role}/catalog-roles/{CATALOG}",
            {"catalogRole": {"name": engine.catalog_role}},
        )

    # ETL owns content (tables, branches, maintenance) across the catalog.
    grant(p, "lakehouse_writer", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})

    # Listing top-level namespaces needs a catalog-level grant. It exposes namespace
    # *names* only.
    grant(p, "lakehouse_reader", {"type": "catalog", "privilege": "NAMESPACE_LIST"})
    # Bronze is readable by the engine so platform admins can debug ingestion; OPA keeps it
    # from everyone else and nulls raw record payloads (tag pii.raw_record).
    for ns in ["bronze", "silver", "gold", "ops"]:
        for privilege in READER_PRIVILEGES:
            grant(p, "lakehouse_reader", {"type": "namespace", "namespace": [ns], "privilege": privilege})
    log.info("catalog roles and grants ensured")


def read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
    return {k: v for k, v in pairs}


def write_env_file(path: Path, values: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(f"{k}={v}\n" for k, v in values.items()))
    tmp.chmod(0o640)
    tmp.replace(path)  # atomic: readers never see a half-written file


def ensure_principal(p: Polaris, engine: Engine, secret_file: Path | None = None) -> None:
    secret_file = secret_file or SECRETS_DIR / f"{engine.principal}.env"
    current = read_env_file(secret_file)
    cid = current.get(f"{engine.env_prefix}_CLIENT_ID")
    csecret = current.get(f"{engine.env_prefix}_CLIENT_SECRET")

    created = p.call("POST", "/api/management/v1/principals", {"principal": {"name": engine.principal}})
    if created:
        creds = created["credentials"]
        log.info("created principal %s", engine.principal)
    elif cid and csecret and p.credentials_work(cid, csecret):
        creds = None
        log.info("principal %s exists and stored credentials are valid", engine.principal)
    else:
        # Reset, not rotate: rotate is the principal changing its own secret, and Polaris refuses
        # it to the admin (403 ROTATE_CREDENTIALS).
        creds = p.call("POST", f"/api/management/v1/principals/{engine.principal}/reset", {})["credentials"]
        log.warning("principal %s credentials missing or invalid -> reset", engine.principal)

    if creds:
        write_env_file(
            secret_file,
            {
                f"{engine.env_prefix}_CLIENT_ID": creds["clientId"],
                f"{engine.env_prefix}_CLIENT_SECRET": creds["clientSecret"],
            },
        )

    p.call(
        "PUT",
        f"/api/management/v1/principals/{engine.principal}/principal-roles",
        {"principalRole": {"name": engine.principal_role}},
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    p = Polaris(POLARIS_URL, REALM)
    p.login()
    ensure_catalog(p)
    ensure_namespaces(p)
    ensure_roles_and_grants(p)
    for engine in ENGINES:
        ensure_principal(p, engine)
    log.info("polaris bootstrap complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
