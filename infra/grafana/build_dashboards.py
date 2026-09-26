# /// script
# requires-python = ">=3.12"
# ///
"""Dashboards as code: generates the provisioned Grafana dashboards (JSON) from this file.

Run `uv run infra/grafana/build_dashboards.py` after editing; CI fails if the committed
JSON is out of date. Reviewing a dashboard change means reviewing this Python diff,
not thousands of lines of generated JSON.
"""

from __future__ import annotations

import json
from itertools import count
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}
_ids = count(1)


def ts(
    title, exprs, unit="short", x=0, y=0, w=12, h=8, thresholds=None, legend=True, soft_max=None
):
    panel = {
        "id": next(_ids),
        "type": "timeseries",
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [
            {"refId": chr(65 + i), "expr": e, "legendFormat": lf, "datasource": DS}
            for i, (e, lf) in enumerate(exprs)
        ],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {"lineWidth": 2, "fillOpacity": 12, "showPoints": "never"},
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "showLegend": legend,
                "displayMode": "list",
                "placement": "bottom",
            },
            "tooltip": {"mode": "multi"},
        },
    }
    if thresholds:
        panel["fieldConfig"]["defaults"]["thresholds"] = {
            "mode": "absolute",
            "steps": thresholds,
        }
        panel["fieldConfig"]["defaults"]["custom"]["thresholdsStyle"] = {
            "mode": "line+area"
        }
    if soft_max is not None:
        # A flat zero series autoscales to a meaningless range; keep the alert line in view.
        panel["fieldConfig"]["defaults"]["custom"].update(axisSoftMin=0, axisSoftMax=soft_max)
    return panel


def stat(title, expr, unit="short", x=0, y=0, w=4, h=4, steps=None, decimals=None):
    d = {
        "unit": unit,
        "thresholds": {
            "mode": "absolute",
            "steps": steps or [{"color": "green", "value": None}],
        },
    }
    if decimals is not None:
        d["decimals"] = decimals
    return {
        "id": next(_ids),
        "type": "stat",
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": "A", "expr": expr, "datasource": DS}],
        "fieldConfig": {"defaults": d, "overrides": []},
        "options": {
            "colorMode": "background",
            "graphMode": "area",
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "textMode": "value_and_name",
        },
    }


def row(title, y):
    return {
        "id": next(_ids),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
    }


def dashboard(uid, title, panels, description):
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "editable": False,
        "schemaVersion": 39,
        "time": {"from": "now-30m", "to": "now"},
        "refresh": "10s",
        "tags": ["open-lakehouse"],
        "timezone": "utc",
        "panels": panels,
        "templating": {"list": []},
        "annotations": {"list": []},
    }


GREEN, AMBER, RED = "green", "orange", "red"


def slo_dashboard():
    avail = (
        '1 - ((sum(increase(mcp_tool_calls_total{outcome=~"unavailable|error"}[$__range])) or vector(0))'
        " / clamp_min(sum(increase(mcp_tool_calls_total[$__range])) or vector(0), 1))"
    )
    return dashboard(
        "platform-slos",
        "Platform SLOs — agents on live calls",
        [
            row(
                "Agent tool calls (MCP gateway) — SLO: 99.9% available, p95 < 500 ms, p99 < 1.5 s",
                0,
            ),
            stat(
                "Availability (range)",
                avail,
                "percentunit",
                0,
                1,
                4,
                4,
                [
                    {"color": RED, "value": None},
                    {"color": AMBER, "value": 0.99},
                    {"color": GREEN, "value": 0.999},
                ],
                3,
            ),
            stat(
                "p95 latency (5m)",
                "sli:mcp_tool_latency:p95_5m",
                "s",
                4,
                1,
                4,
                4,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 0.5},
                    {"color": RED, "value": 1.5},
                ],
                2,
            ),
            stat(
                "p99 latency (5m)",
                "sli:mcp_tool_latency:p99_5m",
                "s",
                8,
                1,
                4,
                4,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 1},
                    {"color": RED, "value": 1.5},
                ],
                2,
            ),
            stat(
                "Error-budget burn (1h)",
                "sli:mcp_tool_errors:ratio_rate1h / 0.001",
                "x",
                12,
                1,
                4,
                4,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 6},
                    {"color": RED, "value": 14.4},
                ],
                1,
            ),
            stat(
                "Circuit breaker",
                "max(mcp_circuit_breaker_open)",
                "none",
                16,
                1,
                4,
                4,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 1}],
            ),
            stat(
                "Firing alerts",
                'count(ALERTS{alertstate="firing"}) or vector(0)',
                "none",
                20,
                1,
                4,
                4,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 1}],
            ),
            ts(
                "Tool calls by outcome (denied = policy working, not an error)",
                [("sum by (outcome) (rate(mcp_tool_calls_total[1m]))", "{{outcome}}")],
                "reqps",
                0,
                5,
                12,
                8,
            ),
            ts(
                "Tool latency p50 / p95 / p99",
                [
                    (
                        "histogram_quantile(0.5, sum by (le) (rate(mcp_tool_call_duration_seconds_bucket[5m])))",
                        "p50",
                    ),
                    ("sli:mcp_tool_latency:p95_5m", "p95"),
                    ("sli:mcp_tool_latency:p99_5m", "p99"),
                ],
                "s",
                12,
                5,
                12,
                8,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 1.5}],
            ),
            ts(
                "Latency p95 by tool",
                [
                    (
                        "histogram_quantile(0.95, sum by (le, tool) (rate(mcp_tool_call_duration_seconds_bucket[5m])))",
                        "{{tool}}",
                    )
                ],
                "s",
                0,
                13,
                12,
                8,
            ),
            ts(
                "Error-budget burn rate (1 = on budget)",
                [
                    ("sli:mcp_tool_errors:ratio_rate5m / 0.001", "5m"),
                    ("sli:mcp_tool_errors:ratio_rate1h / 0.001", "1h"),
                    ("sli:mcp_tool_errors:ratio_rate6h / 0.001", "6h"),
                ],
                "x",
                12,
                13,
                12,
                8,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 6},
                    {"color": RED, "value": 14.4},
                ],
            ),
            row("Engines, catalog, identity", 21),
            ts(
                "Trino queries",
                [
                    (
                        "rate(trino_execution_name_QueryManager_CompletedQueries[1m])",
                        "completed/s",
                    ),
                    (
                        "rate(trino_execution_name_QueryManager_UserErrorFailures[1m])",
                        "user errors/s (incl. denials)",
                    ),
                    (
                        "rate(trino_execution_name_QueryManager_InternalFailures[1m])",
                        "internal failures/s",
                    ),
                ],
                "qps",
                0,
                22,
                8,
                8,
            ),
            ts(
                "Polaris catalog requests",
                [
                    (
                        'sum by (outcome) (rate(http_server_requests_seconds_count{job="polaris"}[1m]))',
                        "{{outcome}}",
                    )
                ],
                "reqps",
                8,
                22,
                8,
                8,
            ),
            ts(
                "Scrape targets up",
                [("up", "{{job}}")],
                "none",
                16,
                22,
                8,
                8,
                legend=True,
            ),
        ],
        "Golden signals and SLO burn for the agent data path. Alert rules: infra/prometheus/rules/slo.yml",
    )


def streaming_dashboard():
    return dashboard(
        "streaming",
        "Streaming — CDC freshness and data quality",
        [
            row("Source commit → silver (as a colleague) — SLO: < 60 s", 0),
            stat(
                "End-to-end lag (last batch)",
                "max(cdc_end_to_end_lag_seconds)",
                "s",
                0,
                1,
                6,
                4,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 30},
                    {"color": RED, "value": 60},
                ],
                1,
            ),
            stat(
                "Seconds since progress",
                "time() - max(cdc_last_progress_timestamp_seconds)",
                "s",
                6,
                1,
                6,
                4,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 60},
                    {"color": RED, "value": 120},
                ],
                0,
            ),
            stat(
                "Replication slot active",
                "max(pg_replication_slots_slot_is_active)",
                "none",
                12,
                1,
                6,
                4,
                [{"color": RED, "value": None}, {"color": GREEN, "value": 1}],
            ),
            stat(
                "Quarantined (1h)",
                "sum(increase(cdc_quarantined_total[1h]))",
                "none",
                18,
                1,
                6,
                4,
                [{"color": GREEN, "value": None}, {"color": AMBER, "value": 1}],
                0,
            ),
            ts(
                "Change events by entity and op",
                [
                    (
                        "sum by (entity, op) (rate(cdc_events_total[1m]))",
                        "{{entity}} {{op}}",
                    )
                ],
                "ops",
                0,
                5,
                12,
                8,
            ),
            ts(
                "End-to-end lag",
                [("max(cdc_end_to_end_lag_seconds)", "lag")],
                "s",
                12,
                5,
                12,
                8,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 60}],
            ),
            ts(
                "Micro-batch duration p95",
                [
                    (
                        "histogram_quantile(0.95, sum by (le) (rate(cdc_batch_duration_seconds_bucket[5m])))",
                        "p95",
                    )
                ],
                "s",
                0,
                13,
                8,
                8,
            ),
            ts(
                "Quarantine rate by entity (alert > 1%)",
                [("max by (entity) (cdc_quarantine_rate)", "{{entity}}")],
                "percentunit",
                8,
                13,
                8,
                8,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 0.01}],
                soft_max=0.05,
            ),
            ts(
                "WAL retained by replication slots",
                [
                    (
                        "max by (slot_name) (pg_replication_slots_pg_wal_lsn_diff)",
                        "{{slot_name}}",
                    )
                ],
                "bytes",
                16,
                13,
                8,
                8,
            ),
            ts(
                "Kafka topic throughput",
                [
                    (
                        'sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"_.*"}[1m]))',
                        "{{topic}}",
                    )
                ],
                "msgps",
                0,
                21,
                12,
                8,
            ),
            ts(
                "Consumer group lag",
                [
                    (
                        "sum by (consumergroup) (kafka_consumergroup_lag)",
                        "{{consumergroup}}",
                    )
                ],
                "short",
                12,
                21,
                12,
                8,
            ),
        ],
        "Debezium → Kafka → Spark Structured Streaming → Iceberg. Freshness measured from the source commit timestamp.",
    )


def assist_dashboard():
    return dashboard(
        "live-call-assist",
        "Live Call Assist — guidance quality and speed",
        [
            row("Caller speaks → guidance on screen — target p95 < 2 s", 0),
            stat("Active calls", "sum(assist_active_calls)", "none", 0, 1, 4, 4),
            stat(
                "Guidance p95 (5m)",
                "sli:assist_guidance_latency:p95_5m",
                "s",
                4,
                1,
                5,
                4,
                [
                    {"color": GREEN, "value": None},
                    {"color": AMBER, "value": 1},
                    {"color": RED, "value": 2},
                ],
                2,
            ),
            stat(
                "Cards shown (1h)",
                "sum(increase(assist_cards_total[1h]))",
                "none",
                9,
                1,
                5,
                4,
                decimals=0,
            ),
            stat(
                "Ungrounded withheld (1h)",
                "sum(increase(assist_cards_withheld_total[1h])) or vector(0)",
                "none",
                14,
                1,
                5,
                4,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 1}],
                0,
            ),
            stat(
                "Transcript lag (msgs)",
                'sum(kafka_consumergroup_lag{consumergroup="call-assist"}) or vector(0)',
                "none",
                19,
                1,
                5,
                4,
                [{"color": GREEN, "value": None}, {"color": RED, "value": 50}],
                0,
            ),
            ts(
                "Guidance latency by card kind (p95)",
                [
                    (
                        "histogram_quantile(0.95, sum by (le, kind) (rate(assist_guidance_latency_seconds_bucket[5m])))",
                        "{{kind}}",
                    )
                ],
                "s",
                0,
                5,
                12,
                8,
            ),
            ts(
                "Cards by kind",
                [("sum by (kind) (rate(assist_cards_total[5m])) * 60", "{{kind}}")],
                "short",
                12,
                5,
                12,
                8,
            ),
            ts(
                "Engine tool calls by outcome",
                [
                    (
                        "sum by (tool, outcome) (rate(assist_tool_calls_total[5m])) * 60",
                        "{{tool}} {{outcome}}",
                    )
                ],
                "short",
                0,
                13,
                24,
                8,
            ),
        ],
        "The agent's own SLIs: speed, grounding and safety. Offline quality gates: call-assist-evals in CI.",
    )


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for d in (slo_dashboard(), streaming_dashboard(), assist_dashboard()):
        (OUT / f"{d['uid']}.json").write_text(json.dumps(d, indent=1) + "\n")
        print(f"wrote {d['uid']}.json ({len(d['panels'])} panels)")


if __name__ == "__main__":
    main()
