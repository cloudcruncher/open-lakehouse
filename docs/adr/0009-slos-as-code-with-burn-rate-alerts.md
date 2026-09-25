# 9. SLOs as code, alerting on error-budget burn

**Status:** accepted

## Context
Threshold alerts ("error rate > 1%") page for blips and miss slow leaks. For
agents on live calls, what matters is whether we're spending the reliability budget
colleagues were promised.

## Decision
- The SLIs are recorded in Prometheus (`infra/prometheus/rules/slo.yml`): agent tool
  availability (unavailable or error ÷ all calls, where **denied is never an error**),
  latency at p95 and p99, CDC freshness, and guidance latency.
- Alerts use multi-window, multi-burn-rate rules (SRE workbook). A fast burn (14.4× over
  1h and 5m) pages; a slow burn (6× over 6h and 30m) raises a ticket. Every alert
  links to a runbook section.
- Trino's metrics are a protected management resource, scraped with a Keycloak
  machine identity that OPA allows to read system information and nothing else.
- Grafana dashboards are generated from code (`infra/grafana/build_dashboards.py`),
  and CI fails if the committed JSON is stale.

## Consequences
- Pages mean users are affected; tickets mean budget is leaking.
- Observability never widens the network: Prometheus joins each segment it scrapes
  but doesn't route between them.
