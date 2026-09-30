"""Stream freshness and lag for every tenant, measured by the platform (ADR 14).

A tenant declares `observe:` tables in its tenant file. This service reads, for each one, the
current snapshot's commit time and record count from Polaris's table metadata (a read-only
principal, no data access), and for tables that mirror a topic, the topic's log-end offsets from
Kafka. Spark streams don't commit to Kafka consumer groups, so the Kafka exporter cannot see their
lag; a bronze table that appends every record is exactly as far behind as the topic has records
it does not.

Metrics (one sample per observed table, labelled tenant and table):
  tenant_table_last_commit_timestamp_seconds  when the table's current snapshot was committed
  tenant_table_records                        records in that snapshot (Iceberg's total-records)
  tenant_topic_records{tenant, topic}         records ever written to the topic (sum of log ends)
  tenant_table_lag_records{tenant, table, topic}
                                              topic records minus table records; exact for a
                                              table filled from the topic's first record, and
                                              negative if the table holds duplicates
  tenant_table_observed{tenant, table}        1 if the table could be read, 0 if not
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import yaml
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily

log = logging.getLogger("tenant-metrics")

TENANTS_DIR = Path(os.environ.get("TENANTS_DIR", "/etc/tenants"))
POLARIS_URL = os.environ.get("POLARIS_URL", "http://polaris:8181").rstrip("/")
POLARIS_REALM = os.environ.get("POLARIS_REALM", "bank")
CATALOG = os.environ.get("POLARIS_CATALOG", "lakehouse")
CREDENTIALS = Path(os.environ.get("METRICS_POLARIS_ENV_FILE", "/run/platform-secrets/platform_metrics.env"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INTERVAL = int(os.environ.get("INTERVAL_SECONDS", "30"))
PORT = int(os.environ.get("METRICS_PORT", "9109"))


@dataclass(frozen=True)
class Observed:
    tenant: str
    table: str  # <namespace>.<table>
    topic: str | None = None


@dataclass(frozen=True)
class Sample:
    tenant: str
    table: str
    topic: str | None
    observed: bool
    commit_timestamp: float | None = None
    records: int | None = None
    topic_records: int | None = None

    @property
    def lag(self) -> int | None:
        """Negative when the table holds more records than the topic ever had: duplicates."""
        if self.topic_records is None or self.records is None:
            return None
        return self.topic_records - self.records


def observed_tables(tenants_dir: Path = TENANTS_DIR) -> list[Observed]:
    """Every table every tenant file asks the platform to watch."""
    found = []
    for path in sorted(tenants_dir.glob("*.yaml")):
        t = yaml.safe_load(path.read_text())
        for o in t.get("observe", []):
            found.append(Observed(t["name"], o["table"], o.get("topic")))
    return found


def snapshot_facts(metadata: dict) -> tuple[float | None, int | None]:
    """(commit time in seconds, total records) of a table's current snapshot, from Iceberg
    table metadata; (None, None) for a table that has never been committed to."""
    current = metadata.get("current-snapshot-id")
    for snap in metadata.get("snapshots", []):
        if snap.get("snapshot-id") == current:
            records = snap.get("summary", {}).get("total-records")
            return snap["timestamp-ms"] / 1000, int(records) if records is not None else None
    return None, None


def collect(
    watched: Iterable[Observed],
    load_table: Callable[[str], dict | None],
    topic_records: Callable[[str], int | None],
) -> list[Sample]:
    """One sample per watched table. A source that fails or has no answer yields observed=False or
    missing values, never an exception: one broken table must not blank the others."""
    topics: dict[str, int | None] = {}
    samples = []
    for o in watched:
        try:
            metadata = load_table(o.table)
        except Exception as exc:  # noqa: BLE001 - network, auth, catalog errors: report the table unobserved
            log.warning("%s: cannot read %s: %s", o.tenant, o.table, exc)
            metadata = None
        if metadata is None:
            samples.append(Sample(o.tenant, o.table, o.topic, observed=False))
            continue
        ts, records = snapshot_facts(metadata)
        total = None
        if o.topic:
            if o.topic not in topics:
                try:
                    topics[o.topic] = topic_records(o.topic)
                except Exception as exc:  # noqa: BLE001 - a topic we cannot read leaves lag unreported
                    log.warning("%s: cannot read topic %s: %s", o.tenant, o.topic, exc)
                    topics[o.topic] = None
            total = topics[o.topic]
        samples.append(Sample(o.tenant, o.table, o.topic, True, ts, records, total))
    return samples


class Collector:
    """Serves the latest samples; replaced whole each cycle, so a scrape never sees half a cycle."""

    def __init__(self) -> None:
        self.samples: list[Sample] = []

    def collect(self) -> Iterator[GaugeMetricFamily]:
        commit = GaugeMetricFamily(
            "tenant_table_last_commit_timestamp_seconds",
            "When the table's current snapshot was committed",
            labels=["tenant", "table"],
        )
        records = GaugeMetricFamily(
            "tenant_table_records", "Records in the current snapshot", labels=["tenant", "table"]
        )
        observed = GaugeMetricFamily(
            "tenant_table_observed",
            "1 if the platform could read the table, else 0",
            labels=["tenant", "table"],
        )
        lag = GaugeMetricFamily(
            "tenant_table_lag_records",
            "Topic records minus table records; negative means the table holds more than the topic ever had",
            labels=["tenant", "table", "topic"],
        )
        topic = GaugeMetricFamily(
            "tenant_topic_records",
            "Records ever written to the topic (sum of log-end offsets)",
            labels=["tenant", "topic"],
        )
        seen_topics = set()
        for s in self.samples:
            observed.add_metric([s.tenant, s.table], 1 if s.observed else 0)
            if s.commit_timestamp is not None:
                commit.add_metric([s.tenant, s.table], s.commit_timestamp)
            if s.records is not None:
                records.add_metric([s.tenant, s.table], s.records)
            if s.lag is not None:
                lag.add_metric([s.tenant, s.table, s.topic], s.lag)
            if s.topic and s.topic_records is not None and (s.tenant, s.topic) not in seen_topics:
                seen_topics.add((s.tenant, s.topic))
                topic.add_metric([s.tenant, s.topic], s.topic_records)
        yield from (observed, commit, records, lag, topic)


# ------------------------------------------------------------------ sources


def read_credentials(path: Path = CREDENTIALS) -> tuple[str, str]:
    env = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
    return env["METRICS_POLARIS_CLIENT_ID"], env["METRICS_POLARIS_CLIENT_SECRET"]


class PolarisTables:
    """Table metadata from Polaris's REST catalog, as the platform's read-only metrics principal."""

    def __init__(self) -> None:
        self.http = httpx.Client(timeout=15.0)
        self.headers = {"Polaris-Realm": POLARIS_REALM}

    def login(self) -> None:
        client_id, secret = read_credentials()
        resp = self.http.post(
            f"{POLARIS_URL}/api/catalog/v1/oauth/tokens",
            headers=self.headers,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        )
        resp.raise_for_status()
        self.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

    def load(self, table: str) -> dict | None:
        namespace, name = table.split(".", 1)
        url = f"{POLARIS_URL}/api/catalog/v1/{CATALOG}/namespaces/{namespace}/tables/{name}"
        resp = self.http.get(url, headers=self.headers)
        if resp.status_code == 401:  # token expired: log in again once
            self.login()
            resp = self.http.get(url, headers=self.headers)
        if resp.status_code == 404:
            return None  # not created yet (a stream that has not started)
        resp.raise_for_status()
        return resp.json()["metadata"]


async def topic_records(consumer, admin, topic: str) -> int | None:
    """Records ever written to a topic: the sum of its partitions' log-end offsets. The partitions
    come from the admin client, since a consumer knows only the topics it subscribes to."""
    from aiokafka import TopicPartition

    described = await admin.describe_topics([topic])
    if not described or described[0]["error_code"] != 0:
        return None
    ends = await consumer.end_offsets(
        [TopicPartition(topic, p["partition"]) for p in described[0]["partitions"]]
    )
    return sum(ends.values())


async def run() -> None:
    from aiokafka import AIOKafkaConsumer
    from aiokafka.admin import AIOKafkaAdminClient

    collector = Collector()
    REGISTRY.register(collector)
    start_http_server(PORT)
    tables = PolarisTables()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    consumer = AIOKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer.start()
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    await admin.start()
    log.info("serving /metrics on :%d every %ds", PORT, INTERVAL)
    try:
        while not stop.is_set():
            watched = observed_tables()
            try:
                if "Authorization" not in tables.headers:
                    await asyncio.to_thread(tables.login)
                ends = {}
                for topic in {o.topic for o in watched if o.topic}:
                    ends[topic] = await topic_records(consumer, admin, topic)
                collector.samples = await asyncio.to_thread(collect, watched, tables.load, ends.get)
            except Exception:
                log.exception("cycle failed; serving the last samples")
            try:
                await asyncio.wait_for(stop.wait(), INTERVAL)
            except TimeoutError:
                pass
    finally:
        await consumer.stop()
        await admin.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
