"""Query layer: parameterised, bounded, identity-propagating Trino access."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import trino
from trino.auth import JWTAuthentication

from .resilience import CircuitBreaker

log = logging.getLogger(__name__)

# Errors worth one retry (network blips, a coordinator restart). Authorization and
# user errors are never retried: they're deterministic.
TRANSIENT = (trino.exceptions.TrinoConnectionError, trino.exceptions.Http503Error)


@dataclass(frozen=True)
class TrinoConfig:
    host: str
    port: int
    ca_file: str
    max_execution: str = "10s"
    max_rows: int = 200


@dataclass
class QueryResult:
    rows: list[dict[str, Any]]
    query_id: str | None


class DataAccess:
    def __init__(self, cfg: TrinoConfig, breaker: CircuitBreaker) -> None:
        self.cfg = cfg
        self.breaker = breaker

    def query(self, token: str, sql: str, params: list[Any]) -> QueryResult:
        self.breaker.before_call()
        attempts = 2
        for attempt in range(1, attempts + 1):
            try:
                result = self._run(token, sql, params)
                self.breaker.record_success()
                return result
            except TRANSIENT as exc:
                self.breaker.record_failure()
                if attempt == attempts:
                    raise
                log.warning("transient Trino error, retrying: %s", exc)
                time.sleep(0.3)
        raise AssertionError("unreachable")

    def _run(self, token: str, sql: str, params: list[Any]) -> QueryResult:
        conn = trino.dbapi.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            http_scheme="https",
            verify=self.cfg.ca_file,
            auth=JWTAuthentication(token),
            catalog="lakehouse",
            source="mcp-gateway",
            session_properties={"query_max_execution_time": self.cfg.max_execution},
            request_timeout=15,
        )
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchmany(self.cfg.max_rows)
            cols = [d[0] for d in cur.description or []]
            return QueryResult([dict(zip(cols, r, strict=True)) for r in rows], cur.query_id)
        finally:
            conn.close()

    def ping(self) -> bool:
        import httpx

        try:
            r = httpx.get(
                f"https://{self.cfg.host}:{self.cfg.port}/v1/info", verify=self.cfg.ca_file, timeout=2.0
            )
            return r.status_code == 200 and not r.json().get("starting", True)
        except httpx.HTTPError:
            return False
