# /// script
# requires-python = ">=3.12"
# dependencies = ["playwright>=1.55"]
# ///
"""Build the "Markets & Payments Intelligence" dashboard in Superset, as a colleague, from gold.

Analytics are the tenant's to build (platform owns observability, tenants own Superset): this is
what a consumer does by hand, written down so it can be rebuilt on any stack. It signs in as the
given persona (default carol, an analyst: she reads gold, never silver), so every query runs
under her own Trino token and OPA decision. Idempotent: re-running updates in place.
Reads the demo password from .env itself and never prints it. Screenshot: .tour/markets-dashboard.png.

Usage: uv run scripts/markets_dashboard.py [persona]
"""

import json
import sys
import time
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
PW = next(
    line.split("=", 1)[1].strip()
    for line in (ROOT / ".env").read_text().splitlines()
    if line.startswith("DEMO_USER_PASSWORD=")
)
BASE = "http://localhost:3004"
TITLE = "Markets & Payments Intelligence"
SLUG = "markets-payments"


def sql(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


# (name, dataset, viz_type, extra form_data, grid width of 12)
CHARTS = [
    (
        "Card authorisations",
        "card_auth_daily",
        "big_number_total",
        {"metric": sql("sum(auths)", "auths")},
        3,
    ),
    (
        "Approval rate",
        "card_auth_daily",
        "big_number_total",
        {
            "metric": sql("sum(approved) * 1.0 / sum(auths)", "approval rate"),
            "y_axis_format": ".1%",
        },
        3,
    ),
    (
        "Authorised spend (EUR)",
        "card_auth_daily",
        "big_number_total",
        {"metric": sql("sum(approved_amount_eur)", "EUR"), "y_axis_format": ",.0f"},
        3,
    ),
    (
        "Sanctioned merchants hit",
        "sanctions_hits",
        "big_number_total",
        {"metric": sql("count(*)", "merchants")},
        3,
    ),
    *[
        (
            f"{product} close price (hourly)",
            "crypto_ohlcv_1m",
            "echarts_timeseries_line",
            {
                "x_axis": "minute",
                "time_grain_sqla": "PT1H",
                "metrics": [sql("avg(close)", "close")],
                "groupby": [],
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",
                        "clause": "WHERE",
                        "sqlExpression": f"product_id = '{product}'",
                    }
                ],
                "row_limit": 10000,
                "y_axis_format": ",.2f",
                "truncateYAxis": True,
            },
            6,
        )
        for product in ("BTC-EUR", "ETH-EUR")
    ],
    (
        "Crypto traded notional by product",
        "crypto_ohlcv_1m",
        "echarts_timeseries_bar",
        {
            "x_axis": "minute",
            "time_grain_sqla": "PT1H",
            "metrics": [sql("sum(notional)", "notional")],
            "groupby": ["product_id"],
            "row_limit": 10000,
            "stack": True,
        },
        6,
    ),
    (
        "Approval rate by channel",
        "card_auth_daily",
        "echarts_timeseries_bar",
        {
            "x_axis": "channel",
            "metrics": [sql("sum(approved) * 1.0 / sum(auths)", "approval rate")],
            "groupby": [],
            "row_limit": 20,
            "y_axis_format": ".1%",
        },
        6,
    ),
    (
        "Card spend by merchant country (EUR)",
        "card_auth_daily",
        "echarts_timeseries_bar",
        {
            "x_axis": "merchant_country",
            "metrics": [sql("sum(amount_eur)", "EUR")],
            "groupby": [],
            "row_limit": 30,
        },
        12,
    ),
    (
        "Sanctions screening: listed merchants",
        "sanctions_hits",
        "table",
        {
            "query_mode": "raw",
            "all_columns": [
                "merchant_name",
                "merchant_country",
                "program_ids",
                "auths",
                "amount_eur",
                "last_auth",
            ],
            "row_limit": 50,
        },
        12,
    ),
]
DATASETS = sorted({c[1] for c in CHARTS})


def main(user):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1500})
        page.goto(f"{BASE}/login/keycloak")
        page.wait_for_selector("#username", timeout=20000)
        page.fill("#username", user)
        page.fill("#password", PW)
        page.click("#kc-login")
        time.sleep(4)
        csrf = page.request.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        h = {
            "X-CSRFToken": csrf,
            "Referer": f"{BASE}/",
            "Content-Type": "application/json",
        }

        def call(method, path, body=None):
            r = page.request.fetch(
                f"{BASE}{path}",
                method=method,
                headers=h,
                data=json.dumps(body) if body else None,
            )
            try:
                return r.status, r.json()
            except PlaywrightError:
                return r.status, {}

        def find(path, col, value):
            _, found = call(
                "GET",
                f"{path}?q=(filters:!((col:{col},opr:eq,value:'{quote(value, safe='')}')))",
            )
            return found["result"][0]["id"] if found.get("result") else None

        _, dbs = call("GET", "/api/v1/database/")
        db = next(d["id"] for d in dbs["result"] if d["database_name"] == "Lakehouse")
        # A fresh sign-in holds no Trino token: follow the OAuth2 redirect once.
        _, probe = call(
            "POST",
            "/api/v1/sqllab/execute/",
            {
                "database_id": db,
                "sql": "SELECT 1",
                "runAsync": False,
                "schema": "",
                "client_id": f"dash{user}",
                "queryLimit": 1,
            },
        )
        redirect = next(
            (
                e["extra"]["url"]
                for e in probe.get("errors", [])
                if e.get("error_type") == "OAUTH2_REDIRECT"
            ),
            None,
        )
        if redirect:
            page.goto(redirect)
            time.sleep(6)

        ds_ids = {}
        for table in DATASETS:
            existing = find("/api/v1/dataset/", "table_name", table)
            if existing:
                ds_ids[table] = existing
                continue
            status, made = call(
                "POST",
                "/api/v1/dataset/",
                {"database": db, "schema": "markets_gold", "table_name": table},
            )
            assert status == 201, f"dataset {table}: {status} {made}"
            ds_ids[table] = made["id"]

        chart_ids = []
        for name, table, viz, extra, _ in CHARTS:
            ds = ds_ids[table]
            form = {
                "viz_type": viz,
                "datasource": f"{ds}__table",
                "adhoc_filters": [],
                "time_range": "No filter",
                **extra,
            }
            body = {
                "slice_name": name,
                "viz_type": viz,
                "datasource_id": ds,
                "datasource_type": "table",
                "params": json.dumps(form),
            }
            cid = find("/api/v1/chart/", "slice_name", name)
            status, made = call(
                "PUT" if cid else "POST",
                f"/api/v1/chart/{cid}" if cid else "/api/v1/chart/",
                body,
            )
            assert status in (200, 201), f"chart {name}: {status} {made}"
            chart_ids.append(cid or made["id"])

        # Layout: rows filled left to right up to 12 grid columns.
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {
                "type": "GRID",
                "id": "GRID_ID",
                "children": [],
                "parents": ["ROOT_ID"],
            },
        }
        row, used, n = None, 12, 0
        for cid, (name, _t, _v, _e, width) in zip(chart_ids, CHARTS):
            if used + width > 12:
                n += 1
                row = f"ROW-{n}"
                position[row] = {
                    "type": "ROW",
                    "id": row,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"},
                }
                position["GRID_ID"]["children"].append(row)
                used = 0
            key = f"CHART-{cid}"
            position[key] = {
                "type": "CHART",
                "id": key,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row],
                "meta": {
                    "width": width,
                    "height": 25
                    if width == 3
                    else 55
                    if "table" not in name.lower() and "screening" not in name.lower()
                    else 30,
                    "chartId": cid,
                    "sliceName": name,
                },
            }
            position[row]["children"].append(key)
            used += width

        dash = {
            "dashboard_title": TITLE,
            "slug": SLUG,
            "published": True,
            "position_json": json.dumps(position),
            "json_metadata": json.dumps({"refresh_frequency": 300, "color_scheme": ""}),
        }
        did = find("/api/v1/dashboard/", "dashboard_title", TITLE)
        status, made = call(
            "PUT" if did else "POST",
            f"/api/v1/dashboard/{did}" if did else "/api/v1/dashboard/",
            dash,
        )
        assert status in (200, 201), f"dashboard: {status} {made}"
        did = did or made["id"]
        _, linked = call("GET", f"/api/v1/dashboard/{did}/charts")
        wanted = {c[0] for c in CHARTS}
        for old in linked.get("result", []):  # charts this script no longer builds
            if old["slice_name"] not in wanted:
                call("DELETE", f"/api/v1/chart/{old['id']}")
        for cid in chart_ids:  # a chart shows on a dashboard once the link exists
            status, made = call("PUT", f"/api/v1/chart/{cid}", {"dashboards": [did]})
            assert status == 200, f"link chart {cid}: {status} {made}"

        page.goto(f"{BASE}/superset/dashboard/{SLUG}/")
        time.sleep(15)
        shot = ROOT / ".tour" / "markets-dashboard.png"
        shot.parent.mkdir(exist_ok=True)
        page.screenshot(path=str(shot), full_page=True)
        print(
            f"dashboard {TITLE!r} built as {user}: {BASE}/superset/dashboard/{SLUG}/  (screenshot {shot})"
        )
        browser.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "carol")
