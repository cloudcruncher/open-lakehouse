"""Declarative, idempotent Keycloak reconciliation: clients, mappers and users from the realm file.

Keycloak's `--import-realm` only runs when the realm doesn't exist yet, so on a
long-lived platform the realm file would silently drift from what's running. This
reconciler makes `infra/keycloak/bank-realm.json` the single source of truth, the
same way `polaris.py` does for the catalog:

  * clients missing  -> created (with their protocol mappers)
  * clients present  -> updated in place; missing or changed mappers are fixed
  * users missing    -> created (existing users and their passwords are left alone)

`${VAR}` placeholders are filled from the environment, exactly as Keycloak does on import.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential_jitter

log = logging.getLogger("keycloak-reconcile")

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080")
REALM_FILE = Path(os.environ.get("REALM_FILE", "/etc/keycloak/bank-realm.json"))
PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")


def load_realm(path: Path) -> dict[str, Any]:
    def fill(m: re.Match[str]) -> str:
        value = os.environ.get(m.group(1))
        if value is None:
            raise SystemExit(f"realm file needs ${{{m.group(1)}}} but it is not set")
        return value

    return json.loads(PLACEHOLDER.sub(fill, path.read_text()))


class Admin:
    def __init__(self, base: str, realm: str) -> None:
        self.base = base.rstrip("/")
        self.realm = realm
        self.http = httpx.Client(timeout=15.0)

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_delay(180),
        reraise=True,
    )
    def login(self, username: str, password: str) -> None:
        resp = self.http.post(
            f"{self.base}/realms/master/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "admin-cli",
                "username": username,
                "password": password,
            },
        )
        resp.raise_for_status()
        self.http.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

    def _url(self, path: str) -> str:
        return f"{self.base}/admin/realms/{self.realm}{path}"

    def get(self, path: str, **params: Any) -> Any:
        resp = self.http.get(self._url(path), params=params)
        resp.raise_for_status()
        return resp.json()

    def send(self, method: str, path: str, body: Any) -> None:
        resp = self.http.request(method, self._url(path), json=body)
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text[:300]}")


def _subset(desired: dict[str, Any], actual: dict[str, Any]) -> bool:
    """Keycloak adds default keys to stored config; only the keys we declare must match."""
    return all(actual.get(k) == v for k, v in desired.items())


def reconcile_client(kc: Admin, desired: dict[str, Any]) -> None:
    client_id = desired["clientId"]
    mappers = desired.get("protocolMappers", [])
    existing = kc.get("/clients", clientId=client_id)
    if not existing:
        kc.send("POST", "/clients", desired)
        log.info("client %s: created", client_id)
        return

    current = existing[0]
    uuid = current["id"]
    body = {k: v for k, v in desired.items() if k != "protocolMappers"}
    body["attributes"] = {**current.get("attributes", {}), **desired.get("attributes", {})}
    kc.send("PUT", f"/clients/{uuid}", {**current, **body, "id": uuid})

    have = {m["name"]: m for m in kc.get(f"/clients/{uuid}/protocol-mappers/models")}
    for m in mappers:
        cur = have.get(m["name"])
        if cur is None:
            kc.send("POST", f"/clients/{uuid}/protocol-mappers/models", m)
            log.info("client %s: added mapper %s", client_id, m["name"])
        elif not _subset(m.get("config", {}), cur.get("config", {})) or cur.get("protocolMapper") != m.get(
            "protocolMapper"
        ):
            kc.send("PUT", f"/clients/{uuid}/protocol-mappers/models/{cur['id']}", {**m, "id": cur["id"]})
            log.info("client %s: updated mapper %s", client_id, m["name"])
    log.info("client %s: in sync", client_id)


def reconcile_user(kc: Admin, desired: dict[str, Any]) -> None:
    if kc.get("/users", username=desired["username"], exact="true"):
        return
    kc.send("POST", "/users", desired)
    log.info("user %s: created", desired["username"])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    realm = load_realm(REALM_FILE)
    kc = Admin(KEYCLOAK_URL, realm["realm"])
    kc.login(os.environ.get("KEYCLOAK_ADMIN_USER", "admin"), os.environ["KEYCLOAK_ADMIN_PASSWORD"])
    # Order matters: audience mappers reference other clients by name.
    clients = sorted(realm.get("clients", []), key=lambda c: bool(c.get("protocolMappers")))
    for client in clients:
        reconcile_client(kc, client)
    for user in realm.get("users", []):
        reconcile_user(kc, user)
    log.info("realm %s reconciled", realm["realm"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
