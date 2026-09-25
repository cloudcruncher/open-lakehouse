"""Declarative CDC connector: desired config in code, reconciled into Kafka Connect.

`PUT /connectors/<name>/config` is create-or-update, so re-running is always safe.
After applying, it waits for the connector and every task to be RUNNING and
restarts failed tasks (a source-database blip leaves tasks FAILED until someone
restarts them: this does it, and the healer calls it on a loop).

The database password is never sent to Connect. The config references
`${env:DEBEZIUM_DB_PASSWORD}`, which the worker resolves from its own environment
(EnvVarConfigProvider), so the secret isn't stored in the config topic or shown by
the REST API.
"""

from __future__ import annotations

import logging
import os
import sys
import time

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential_jitter

log = logging.getLogger("cdc-connector")

CONNECT_URL = os.environ.get("CONNECT_URL", "http://cdc-connect:8083")
NAME = "corebank-cdc"
TABLES = ["core.customers", "core.accounts", "core.transactions", "core.complaints"]

CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${env:DEBEZIUM_DB_PASSWORD}",
    "database.dbname": "corebank",
    "topic.prefix": "corebank",
    "plugin.name": "pgoutput",
    "publication.name": "corebank_cdc",
    "publication.autocreate.mode": "disabled",
    "slot.name": "corebank_cdc",
    "table.include.list": ",".join(TABLES),
    # Backfill is a batch concern (JDBC ingest); the stream tails changes from the moment
    # the slot exists. Start the connector *before* the backfill so nothing falls in a gap.
    "snapshot.mode": "no_data",
    # Plain JSON: exact decimals as strings, no base64-encoded bytes.
    "decimal.handling.mode": "string",
    "tombstones.on.delete": "false",
    # Heartbeats keep the replication slot advancing when the tracked tables are quiet,
    # so Postgres can recycle WAL (a stuck slot is the classic way CDC fills a disk).
    "heartbeat.interval.ms": "10000",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "3",
    "topic.creation.default.cleanup.policy": "delete",
    "topic.creation.default.retention.ms": str(7 * 24 * 3600 * 1000),
    "errors.retry.timeout": "300000",
    "errors.retry.delay.max.ms": "10000",
}


class Connect:
    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")
        self.http = httpx.Client(timeout=15.0)

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_delay(240),
        reraise=True,
    )
    def apply(self) -> None:
        resp = self.http.put(f"{self.base}/connectors/{NAME}/config", json=CONFIG)
        resp.raise_for_status()

    def status(self) -> dict | None:
        resp = self.http.get(f"{self.base}/connectors/{NAME}/status")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def restart_failed(self) -> None:
        self.http.post(
            f"{self.base}/connectors/{NAME}/restart", params={"includeTasks": "true", "onlyFailed": "true"}
        ).raise_for_status()


def healthy(status: dict | None) -> bool:
    if not status or status["connector"]["state"] != "RUNNING":
        return False
    return bool(status["tasks"]) and all(t["state"] == "RUNNING" for t in status["tasks"])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    connect = Connect(CONNECT_URL)
    connect.apply()
    log.info("connector %s: config applied", NAME)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        status = connect.status()
        if healthy(status):
            log.info("connector %s: RUNNING", NAME)
            return 0
        states = [t["state"] for t in (status or {}).get("tasks", [])]
        if status and ("FAILED" in states or status["connector"]["state"] == "FAILED"):
            log.warning("connector %s: failed tasks, restarting", NAME)
            connect.restart_failed()
        time.sleep(3)
    log.error("connector %s did not reach RUNNING: %s", NAME, connect.status())
    return 1


if __name__ == "__main__":
    sys.exit(main())
