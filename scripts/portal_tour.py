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
}

only = sys.argv[1:]
with sync_playwright() as p:
    browser = p.chromium.launch()
    for name, fn in STEPS.items():
        if not only or any(o in name for o in only):
            step(name, fn, browser)
    browser.close()
(OUT / "results.json").write_text(json.dumps(results, indent=2))
