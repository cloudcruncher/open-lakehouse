"""Provenance for every tool answer: how the platform produced it, step by step.

The gateway records, per query:
  * which Iceberg snapshot of which table was read, and the query is *pinned* to it
    (`FOR VERSION AS OF`), so the answer is reproducible by time travel later;
  * which pipeline writes that table (CDC stream or batch WAP);
  * the Trino query id and the identity it ran as (the colleague, not a service);
  * what OPA decided for that colleague on that table: row filter and column masks,
    asked of the same policy endpoints Trino uses, so it is the real decision;
  * the audit row (sequence number and chain hash) that recorded the access.

Explaining the policy never gates the answer: if OPA can't be asked here, the query
was still authorised by Trino and the explanation just says it is unavailable.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Who maintains each table the gateway reads. Static on purpose: it documents the
# platform's contract. Which process made a particular commit is read from that
# snapshot (see `committed_by`), since e.g. a backfill can also write silver.
WRITERS = {
    "silver.accounts": "CDC stream: core banking -> Debezium -> Kafka -> Spark MERGE (every 10 s)",
    "silver.transactions": "CDC stream: core banking -> Debezium -> Kafka -> Spark MERGE (every 10 s)",
    "silver.complaints": "CDC stream: core banking -> Debezium -> Kafka -> Spark MERGE (every 10 s)",
    "gold.customer_360": "Batch WAP (Dagster): staged on a branch, published only if audit checks pass",
}
# Every pipeline job names its Spark app; Iceberg records it in each snapshot summary,
# so provenance can say which job made the commit that answered the question.
COMMITTERS = {
    "cdc-stream": "CDC stream micro-batch (Spark app cdc-stream)",
    "silver-conform": "batch silver conform (Spark app silver-conform)",
    "gold-customer-360": "batch gold build (Spark app gold-customer-360)",
    "table-maintenance": "table maintenance: compaction, no data change",
}
SUMMARY_KEYS = ("added-records", "deleted-records", "total-records", "published-wap-id", "wap.id")


def snapshot_sql(table: str) -> str:
    """The table's current `main` snapshot. `$refs` skips snapshots staged on WAP branches."""
    if table not in WRITERS:  # identifiers can't be bound parameters: allow-list them instead
        raise ValueError(f"unknown table {table!r}")
    schema, name = table.split(".")
    # Identifiers come from the WRITERS allow-list above.
    refs, snaps = f'{schema}."{name}$refs"', f'{schema}."{name}$snapshots"'
    return (
        "SELECT s.snapshot_id, s.committed_at, s.operation, s.summary "  # noqa: S608
        f"FROM {refs} r JOIN {snaps} s ON s.snapshot_id = r.snapshot_id WHERE r.name = 'main'"
    )



def describe_snapshot(row: dict[str, Any] | None, queried_at: datetime) -> dict[str, Any] | None:
    if not row:
        return None
    committed = row["committed_at"]
    if committed.tzinfo is None:
        committed = committed.replace(tzinfo=UTC)
    summary = row.get("summary") or {}
    app = summary.get("app-name")
    committed_by = COMMITTERS.get(app, f"Spark job {app!r}" if app else "unknown writer")
    if "published-wap-id" in summary:
        committed_by += ", published after its audit checks passed"
    return {
        "committed_by": committed_by,
        "id": str(row["snapshot_id"]),  # int64: keep exact in JSON
        "committed_at": committed.isoformat(),
        "committed_seconds_before_query": round((queried_at - committed).total_seconds(), 1),
        "operation": row.get("operation"),
        "summary": {k: summary[k] for k in SUMMARY_KEYS if k in summary},
    }


class PolicyExplainer:
    """Asks OPA what it decides for a user on a table: the same input Trino sends."""

    def __init__(self, url: str, ttl_s: float = 60.0) -> None:
        self.url = url.rstrip("/")
        self.ttl_s = ttl_s
        self._cache: dict[tuple, tuple[float, dict]] = {}
        self._http = httpx.Client(timeout=1.5)

    def explain(self, user: str, table: str, columns: list[tuple[str, str]]) -> dict[str, Any]:
        key = (user, table, tuple(columns))
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < self.ttl_s:
            return hit[1]
        schema, name = table.split(".")
        ctx = {"identity": {"user": user, "groups": []}, "softwareStack": {"trinoVersion": "gateway"}}
        tbl = {"catalogName": "lakehouse", "schemaName": schema, "tableName": name}
        try:
            filters = self._ask(
                "rowFilters",
                {"context": ctx, "action": {"operation": "GetRowFilters", "resource": {"table": tbl}}},
            )
            masks = self._ask(
                "batchColumnMasks",
                {
                    "context": ctx,
                    "action": {
                        "operation": "GetColumnMask",
                        "filterResources": [
                            {"column": {**tbl, "columnName": c, "columnType": t}} for c, t in columns
                        ],
                    },
                },
            )
            out = {
                "engine": "OPA (same policy Trino enforces)",
                "row_filter": " AND ".join(f["expression"] for f in filters) or None,
                "masked_columns": {columns[m["index"]][0]: m["viewExpression"]["expression"] for m in masks},
            }
        except (httpx.HTTPError, KeyError, IndexError, TypeError) as exc:
            log.warning("policy explanation unavailable for %s on %s: %s", user, table, exc)
            return {"engine": "OPA", "unavailable": True}
        self._cache[key] = (time.monotonic(), out)
        return out

    def _ask(self, rule: str, payload: dict) -> list[dict]:
        r = self._http.post(f"{self.url}/v1/data/trino/{rule}", json={"input": payload})
        r.raise_for_status()
        return list(r.json().get("result") or [])
