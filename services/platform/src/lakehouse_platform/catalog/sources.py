"""Live state for the catalog: table freshness from Prometheus (the platform's `tenant-metrics`) and
build facts from Dagster (last build, upstream assets, check results).

Every call fails soft: an unreachable source shows as "not measured" on the page, never as an error,
and results are cached for a few seconds so a busy page does not hammer either service.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

log = logging.getLogger("catalog")

DAGSTER_QUERY = """
query($key: AssetKeyInput!) {
  assetNodeOrError(assetKey: $key) {
    __typename
    ... on AssetNode {
      groupName
      kinds
      jobNames
      dependencyKeys { path }
      assetMaterializations(limit: 1) { timestamp runId }
      assetChecksOrError {
        __typename
        ... on AssetChecks { checks { name executionForLatestMaterialization { status timestamp } } }
      }
    }
  }
}"""


@dataclass(frozen=True)
class Freshness:
    age_seconds: float | None
    records: int | None


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str  # SUCCEEDED, FAILED, ... or "NOT RUN"


@dataclass(frozen=True)
class Build:
    group: str
    kinds: tuple[str, ...]
    upstream: tuple[str, ...]  # `namespace.table` keys of upstream assets Dagster knows
    last_build: float | None  # epoch seconds
    checks: tuple[CheckResult, ...]


class Sources:
    def __init__(self, prometheus: str, dagster: str, ttl: float = 15.0) -> None:
        self.prometheus = prometheus.rstrip("/")
        self.dagster = dagster.rstrip("/")
        self.ttl = ttl
        self.http = httpx.Client(timeout=5.0)
        self._cache: dict[str, tuple[float, object]] = {}

    def _cached(self, key: str, load):
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < self.ttl:
            return hit[1]
        value = load()
        self._cache[key] = (time.monotonic(), value)
        return value

    # -------------------------------------------------------------- freshness

    def _prom(self, query: str) -> dict[str, float]:
        resp = self.http.get(f"{self.prometheus}/api/v1/query", params={"query": query})
        resp.raise_for_status()
        return {r["metric"]["table"]: float(r["value"][1]) for r in resp.json()["data"]["result"]}

    def freshness(self) -> dict[str, Freshness]:
        """`namespace.table` -> (seconds since its last commit, records), for tables tenants observe."""

        def load() -> dict[str, Freshness]:
            try:
                age = self._prom("time() - tenant_table_last_commit_timestamp_seconds")
                records = self._prom("tenant_table_records")
            except (httpx.HTTPError, KeyError, ValueError) as exc:
                log.warning("prometheus unavailable: %s", exc)
                return {}
            return {
                t: Freshness(age.get(t), int(records[t]) if t in records else None)
                for t in age.keys() | records.keys()
            }

        return self._cached("freshness", load)  # type: ignore[return-value]

    # -------------------------------------------------------------- dagster

    def build(self, namespace: str, table: str) -> Build | None:
        """What Dagster knows of the asset, or None (not an asset, or Dagster unreachable)."""

        def load() -> Build | None:
            try:
                resp = self.http.post(
                    f"{self.dagster}/graphql",
                    json={"query": DAGSTER_QUERY, "variables": {"key": {"path": [namespace, table]}}},
                )
                resp.raise_for_status()
                node = resp.json()["data"]["assetNodeOrError"]
            except (httpx.HTTPError, KeyError, ValueError) as exc:
                log.warning("dagster unavailable: %s", exc)
                return None
            return parse_asset(node)

        return self._cached(f"build:{namespace}.{table}", load)  # type: ignore[return-value]


def parse_asset(node: dict) -> Build | None:
    """A Dagster `assetNodeOrError` answer as a Build; None unless it is an asset."""
    if node.get("__typename") != "AssetNode":
        return None
    mats = node.get("assetMaterializations") or []
    checks = []
    found = node.get("assetChecksOrError") or {}
    for c in found.get("checks", []) if found.get("__typename") == "AssetChecks" else []:
        run = c.get("executionForLatestMaterialization") or {}
        checks.append(CheckResult(c["name"], run.get("status") or "NOT RUN"))
    return Build(
        group=node.get("groupName") or "",
        kinds=tuple(node.get("kinds") or ()),
        upstream=tuple(".".join(k["path"]) for k in node.get("dependencyKeys") or []),
        last_build=int(mats[0]["timestamp"]) / 1000 if mats else None,  # Dagster reports milliseconds
        checks=tuple(checks),
    )
