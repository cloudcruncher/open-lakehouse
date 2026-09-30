# /// script
# requires-python = ">=3.12"
# dependencies = ["playwright>=1.55"]
# ///
"""Sign in to every portal as each persona; record the outcome and a screenshot.

Checks what each colleague can open (open access work: docs/next-steps.md). Reads the
demo password from .env itself and never prints it. Screenshots go to .tour/ (git-ignored).
Needs: `uv run --with playwright python -m playwright install chromium` once.

Usage: uv run scripts/portal_tour.py [name-filter ...]   e.g. `dagster grafana`
"""

import json
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
PW = next(
    l.split("=", 1)[1].strip()
    for l in (ROOT / ".env").read_text().splitlines()
    if l.startswith("DEMO_USER_PASSWORD=")
)
OUT = ROOT / ".tour"
OUT.mkdir(exist_ok=True)
results = []


def kc_login(page, user):
    page.wait_for_selector("#username", timeout=20000)
    page.fill("#username", user)
    page.fill("#password", PW)
    page.click("#kc-login")


def sso_button(page):
    oauth = page.locator("a[href*='login/generic_oauth']")
    if oauth.count():
        oauth.first.click()
        return
    for label in [
        "Sign in with Bank SSO",
        "Sign in with OpenID Connect",
        "Sign in with",
    ]:
        loc = page.get_by_text(label, exact=False)
        if loc.count():
            loc.first.click()
            return


def body_text(page, n=400):
    try:
        return re.sub(r"\s+", " ", page.inner_text("body"))[:n]
    except Exception as e:  # noqa: BLE001
        return f"<{e}>"


def step(name, fn, browser):
    ctx = browser.new_context(
        viewport={"width": 1440, "height": 900}, ignore_https_errors=True
    )
    page = ctx.new_page()
    rec = {"name": name}
    try:
        status = fn(page)
        rec.update(ok=True, status=status)
    except Exception as e:  # noqa: BLE001
        rec.update(ok=False, error=str(e).splitlines()[0][:200])
    rec.update(url=page.url, title=page.title(), text=body_text(page))
    try:
        page.screenshot(path=str(OUT / f"{name}.png"), timeout=60000, animations="disabled")
    except Exception as e:  # noqa: BLE001
        rec["screenshot_error"] = str(e).splitlines()[0][:120]
    ctx.close()
    results.append(rec)
    print(json.dumps(rec)[:600], flush=True)


def call_assist(user, run_call=False):
    def fn(page):
        page.goto("http://localhost:8090/")
        page.click("#login")
        kc_login(page, user)
        page.wait_for_selector(".scenarios button", timeout=20000)
        time.sleep(1.5)
        if run_call:
            page.get_by_role("button", name="Card stolen", exact=False).first.click()
            deadline = time.monotonic() + 120
            verified = False
            while time.monotonic() < deadline:
                time.sleep(0.8)
                if not verified and page.is_visible("#verifybar"):
                    time.sleep(1.5)
                    page.click("#verify")
                    verified = True
                if page.locator(".card.summary").count():
                    break
            xray = page.locator(".card:has(.xray)").first.locator(".xray")
            if xray.count():
                xray.evaluate("d => d.open = true")
                xray.scroll_into_view_if_needed()
            time.sleep(1)

    return fn


def grafana(user, path):
    def fn(page):
        page.goto("http://localhost:3001/login")
        kc_login(page, user)
        page.wait_for_url(re.compile(r"localhost:3001/(?!login)"), timeout=20000)
        page.goto(f"http://localhost:3001{path}?refresh=&from=now-1h&to=now")
        time.sleep(6)
        return page.evaluate(
            "fetch('/api/user').then(r=>r.json()).then(u=>u.login+' orgRole='+(u.orgRole||'?'))"
        )

    return fn


def grafana_role(user):
    def fn(page):
        page.goto("http://localhost:3001/login")
        kc_login(page, user)
        page.wait_for_url(re.compile(r"localhost:3001/(?!login)"), timeout=20000)
        page.goto("http://localhost:3001/admin/users")
        time.sleep(4)
        return page.evaluate(
            "fetch('/api/user/orgs').then(r=>r.json()).then(o=>JSON.stringify(o))"
        )

    return fn


def proxied(port, user, path="/"):
    def fn(page):
        page.goto(f"http://localhost:{port}{path}")
        time.sleep(1)
        sso_button(page)
        kc_login(page, user)
        time.sleep(6)
        skip = page.get_by_role("button", name="Skip")
        if skip.count():
            skip.first.click()
            time.sleep(1)
        if path != "/":
            page.goto(f"http://localhost:{port}{path}")
            time.sleep(6)

    return fn


def superset(user, path):
    def fn(page):
        page.goto("http://localhost:3004/login/keycloak")
        kc_login(page, user)
        time.sleep(4)
        page.goto(f"http://localhost:3004{path}")
        time.sleep(5)

    return fn


def superset_build(user, schema, table, expect_ok):
    """Register a table as a dataset, chart it, put it on a dashboard and read it back, as `user`.

    Superset lets colleagues build, but what a chart returns is decided per colleague by Trino and
    OPA: a table the colleague may not read must fail here, at registration or at query time.
    """

    def fn(page):
        base = "http://localhost:3004"
        page.goto(f"{base}/login/keycloak")
        kc_login(page, user)
        time.sleep(4)
        csrf = page.request.get(f"{base}/api/v1/security/csrf_token/").json()["result"]
        h = {"X-CSRFToken": csrf, "Referer": f"{base}/", "Content-Type": "application/json"}

        def call(method, path, body=None):
            r = page.request.fetch(f"{base}{path}", method=method, headers=h, data=json.dumps(body) if body else None)
            return r.status, (r.json() if r.headers.get("content-type", "").startswith("application/json") else {})

        _, dbs = call("GET", "/api/v1/database/")
        db = next(d["id"] for d in dbs["result"] if d["database_name"] == "Lakehouse")
        # A fresh sign-in holds no Trino token: the first query answers with an OAuth2 redirect, which a
        # colleague's browser follows (Keycloak's session is live, so it comes straight back).
        _, probe = call(
            "POST",
            "/api/v1/sqllab/execute/",
            {"database_id": db, "sql": "SELECT 1", "runAsync": False, "schema": "", "client_id": f"tour{user}", "queryLimit": 1},
        )
        redirect = next((e["extra"]["url"] for e in probe.get("errors", []) if e.get("error_type") == "OAUTH2_REDIRECT"), None)
        if redirect:
            page.goto(redirect)
            time.sleep(6)
        status, made = call("POST", "/api/v1/dataset/", {"database": db, "schema": schema, "table_name": table})
        if status == 422 and "already exists" in json.dumps(made):
            _, found = call("GET", f"/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:{table})))")
            ds = found["result"][0]["id"]
        elif status == 201:
            ds = made["id"]
        else:
            if expect_ok:
                raise AssertionError(f"{user} could not register {schema}.{table}: {status} {json.dumps(made)[:150]}")
            return f"refused at registration ({status}), as expected: {json.dumps(made)[:110]}"
        q = {
            "datasource": {"id": ds, "type": "table"},
            "queries": [{"columns": [], "metrics": [{"expressionType": "SQL", "sqlExpression": "count(*)", "label": "n"}], "row_limit": 5}],
            "result_format": "json",
            "result_type": "full",
        }
        status, data = call("POST", "/api/v1/chart/data", q)
        if not expect_ok:
            if status == 200 and not data["result"][0].get("error"):
                raise AssertionError(f"{user} read {schema}.{table} through a chart: it must be refused")
            return f"refused at query time ({status}), as expected"
        if status != 200 or data["result"][0].get("error"):
            raise AssertionError(f"{user}'s chart on {schema}.{table} failed: {status} {json.dumps(data)[:150]}")
        rows = data["result"][0]["data"]
        params = json.dumps({"viz_type": "table", "datasource": f"{ds}__table", "metrics": [], "all_columns": []})
        cs, _ = call("POST", "/api/v1/chart/", {"slice_name": f"tour: {table} as {user}", "viz_type": "table", "datasource_id": ds, "datasource_type": "table", "params": params})
        ds_, _ = call("POST", "/api/v1/dashboard/", {"dashboard_title": f"tour: {user}'s {schema}", "published": False})
        if cs not in (201, 422) or ds_ not in (201, 422):
            raise AssertionError(f"chart {cs} / dashboard {ds_}")
        return f"built dataset, chart and dashboard; chart returned {rows}"

    return fn


def plain(url, wait=3):
    def fn(page):
        r = page.goto(url)
        time.sleep(wait)
        return r.status if r else None

    return fn


def kc_account(user):
    def fn(page):
        page.goto("http://localhost:8280/realms/bank/account")
        kc_login(page, user)
        time.sleep(5)

    return fn


STEPS = {
    "01-keycloak-account-alice": kc_account("alice"),
    "02-callassist-alice": call_assist("alice"),
    "03-callassist-alice-call": call_assist("alice", run_call=True),
    "04-callassist-bob": call_assist("bob"),
    "05-callassist-carol": call_assist("carol"),
    "06-grafana-streaming-alice": grafana("alice", "/d/streaming"),
    "07-grafana-slos-ops": grafana("ops_admin", "/d/platform-slos"),
    "08-grafana-callassist-ops": grafana("ops_admin", "/d/live-call-assist"),
    "09-grafana-admin-alice": grafana_role("alice"),
    "10-dagster-alice": proxied(3002, "alice"),
    "11-dagster-ops": proxied(3002, "ops_admin", "/runs"),
    "12-dagster-assets-ops": proxied(3002, "ops_admin", "/asset-groups?expanded=bronze,silver,sources,data_products"),
    "18-dagster-jobs-ops": proxied(3002, "ops_admin", "/automation"),
    "13-marquez-carol": proxied(3003, "carol"),
    "14-prometheus-alerts": plain("http://localhost:9090/alerts"),
    "15-prometheus-targets": plain("http://localhost:9090/targets"),
    "16-trino-ui": plain("https://localhost:8443/ui/"),
    "17-mcp-healthz": plain("http://localhost:8000/healthz", 1),
    "19-superset-sqllab-alice": superset("alice", "/sqllab/"),
    "20-superset-users-alice": superset("alice", "/users/list/"),
    "21-superset-users-ops": superset("ops_admin", "/users/list/"),
    "22-superset-build-gold-carol": superset_build("carol", "markets_gold", "card_auth_daily", True),
    "23-superset-build-gold-alice": superset_build("alice", "markets_gold", "crypto_ohlcv_1m", True),
    "24-superset-silver-refused-carol": superset_build("carol", "markets_silver", "trades", False),
    "25-superset-silver-ok-ops": superset_build("ops_admin", "markets_silver", "trades", True),
}

only = sys.argv[1:]
with sync_playwright() as p:
    browser = p.chromium.launch()
    for name, fn in STEPS.items():
        if not only or any(o in name for o in only):
            step(name, fn, browser)
    browser.close()
(OUT / "results.json").write_text(json.dumps(results, indent=2))
