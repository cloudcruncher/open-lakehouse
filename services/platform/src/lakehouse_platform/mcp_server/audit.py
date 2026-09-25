"""Append-only, hash-chained audit trail of every tool call (see infra/postgres/init/03)."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field

import psycopg

log = logging.getLogger(__name__)


@dataclass
class AuditEvent:
    colleague: str
    agent_client: str
    tool: str
    arguments: dict
    purpose: str | None
    outcome: str = "ok"
    rows_returned: int | None = None
    trino_query_ids: list[str] = field(default_factory=list)
    latency_ms: int | None = None
    error: str | None = None


class AuditLog:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def write(self, e: AuditEvent) -> None:
        """Audit is part of the call: if the trail can't be written, the call fails.
        An unaudited data access is worse than an unanswered question."""
        with psycopg.connect(self.dsn, connect_timeout=3, autocommit=True) as conn:
            conn.execute(
                """INSERT INTO audit.tool_calls (event_id, colleague, agent_client, tool, arguments, purpose,
                                                 outcome, rows_returned, trino_query_ids, latency_ms, error)
                   VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)""",
                (
                    uuid.uuid4(),
                    e.colleague,
                    e.agent_client,
                    e.tool,
                    json.dumps(e.arguments, sort_keys=True),
                    e.purpose,
                    e.outcome,
                    e.rows_returned,
                    e.trino_query_ids or None,
                    e.latency_ms,
                    (e.error or "")[:500] or None,
                ),
            )

    def ping(self) -> bool:
        try:
            with psycopg.connect(self.dsn, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            return True
        except psycopg.Error:
            return False
