# ruff: noqa: E501  (CSS and HTML string literals: wrapping them would only make the markup harder to read)
"""HTML for the data product catalog: an index, and one page per product.

Plain functions from data to a string (no templates, no JavaScript beyond a filter box), so the pages
are unit-tested and need no build step. Everything dynamic goes through `e()`; links from a contract
are only followed if they are http(s) or mailto.
"""

from __future__ import annotations

import html
import time
from collections.abc import Callable
from urllib.parse import urlparse

from lakehouse_platform.catalog import model as m
from lakehouse_platform.catalog.sources import Build, Freshness

SQL_LAB = "http://localhost:3004/sqllab/"
DAGSTER = "http://localhost:3002"
STATE_LABEL = {"fresh": "Fresh", "late": "Late", "stale": "Stale", "unknown": "Not measured"}
CHECK_OK = {"SUCCEEDED"}


def e(value: object) -> str:
    return html.escape(str(value), quote=True)


def safe_url(url: str) -> str:
    return url if urlparse(url).scheme in ("http", "https", "mailto") else "#"


CSS = """
:root{--bg:#f7f7f4;--panel:#fff;--ink:#1c1f23;--muted:#5d6570;--line:#dcdfe3;--accent:#0b5cad;
--ok:#15803d;--okbg:#e6f4ea;--warn:#b45309;--warnbg:#fdf0d8;--bad:#b91c1c;--badbg:#fbe4e4;
--gold:#8a6d00;--silver:#596273;--bronze:#8c4f24;--chip:#eef0f3}
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#14161a;--panel:#1c1f24;--ink:#e8eaed;
--muted:#9aa3ae;--line:#2d323a;--accent:#6cb0ff;--ok:#4ade80;--okbg:#12301d;--warn:#fbbf24;--warnbg:#3a2c0d;
--bad:#f87171;--badbg:#3d1618;--gold:#e5c14a;--silver:#aab3c2;--bronze:#d99a6c;--chip:#272b32}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,sans-serif}
a{color:var(--accent)}header{border-bottom:1px solid var(--line);background:var(--panel)}
.wrap{max-width:1100px;margin:0 auto;padding:0 16px}header .wrap{display:flex;gap:16px;align-items:baseline;padding:14px 16px}
header b{font-size:17px}header span{color:var(--muted)}main{padding:24px 0 64px}h1{font-size:26px;margin:4px 0 6px}
h2{font-size:17px;margin:0 0 10px}section{background:var(--panel);border:1px solid var(--line);border-radius:10px;
padding:18px 20px;margin:0 0 16px}.lede{color:var(--muted);margin:0 0 16px;max-width:70ch}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.fact{background:var(--chip);border-radius:8px;padding:10px 12px}.fact small{display:block;color:var(--muted)}
.fact b{font-size:16px}.badge{display:inline-block;border-radius:999px;padding:1px 10px;font-size:12.5px;font-weight:600;
background:var(--chip);color:var(--muted);white-space:nowrap}
.fresh,.ok{background:var(--okbg);color:var(--ok)}.late,.warn{background:var(--warnbg);color:var(--warn)}
.stale,.bad{background:var(--badbg);color:var(--bad)}.gold{color:var(--gold)}.silver{color:var(--silver)}.bronze{color:var(--bronze)}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line);
vertical-align:top}th{font-size:12.5px;color:var(--muted);font-weight:600}.scroll{overflow-x:auto}
code,pre{font:13px ui-monospace,monospace}pre{background:var(--chip);padding:12px;border-radius:8px;overflow-x:auto;margin:0}
.flow{display:flex;gap:10px;align-items:stretch;overflow-x:auto;padding-bottom:4px}
.col{display:flex;flex-direction:column;gap:8px;justify-content:center}.arrow{align-self:center;color:var(--muted)}
.node{border:1px solid var(--line);border-radius:8px;padding:8px 12px;background:var(--bg);min-width:170px}
.node.here{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent)}.node small{display:block;color:var(--muted)}
input[type=search]{width:100%;max-width:420px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;
background:var(--panel);color:var(--ink);font:inherit}.muted{color:var(--muted)}ul.plain{margin:0;padding-left:18px}
@media(max-width:640px){main{padding-top:14px}section{padding:14px}.node{min-width:140px}}
"""


def page(title: str, body: str) -> str:
    return (
        f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{e(title)}</title>'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<style>{CSS}</style></head><body><header><div class=wrap><b><a href=/ style='text-decoration:none;"
        "color:inherit'>Data products</a></b><span>what each table is, who owns it, whether to trust it</span>"
        f"</div></header><main><div class=wrap>{body}</div></main></body></html>"
    )


def badge(text: str, kind: str = "") -> str:
    return f'<span class="badge {e(kind)}">{e(text)}</span>'


def layer_badge(p: m.Product) -> str:
    return f'<span class="badge {e(p.layer)}">{e(p.layer)}</span>'


def state_badge(state: str) -> str:
    return badge(STATE_LABEL[state], state if state != "unknown" else "")


def check_summary(build: Build | None, declared: int) -> tuple[str, str]:
    """(label, kind) for a product's checks: the latest result of each check Dagster runs for it."""
    if build is None or not build.checks:
        return (f"{declared} declared" if declared else "none", "")
    bad = [c for c in build.checks if c.status not in CHECK_OK]
    if bad:
        return (f"{len(bad)} of {len(build.checks)} not passing", "bad")
    return (f"{len(build.checks)} of {len(build.checks)} passing", "ok")


def freshness_note(age: float | None, slo: int | None) -> str:
    """'5 min ago, promise 70 min', leaving out whatever is not measured or not promised."""
    parts = [m.human_age(age) if age is not None else "not measured"]
    if slo is not None:
        parts.append(f"promise {m.human_duration(slo)}")
    return ", ".join(parts)


def age_of(p: m.Product, fresh: dict[str, Freshness]) -> float | None:
    f = fresh.get(p.key)
    return f.age_seconds if f else None


# ------------------------------------------------------------------ index


def index(
    products: list[m.Product],
    fresh: dict[str, Freshness],
    builds: Callable[[m.Product], Build | None],
    owners: dict[str, dict],
) -> str:
    rows = []
    for p in products:
        state = m.freshness_state(age_of(p, fresh), p.freshness_slo_seconds)
        label, kind = check_summary(builds(p), len(p.checks))
        blurb = p.description or p.purpose
        rows.append(
            f'<tr data-q="{e((p.key + " " + p.tenant + " " + blurb + " " + " ".join(p.tags)).lower())}" '
            f'data-layer="{e(p.layer)}"><td><a href="{e(p.path)}"><b>{e(p.table)}</b></a>'
            f"<div class=muted>{e(p.namespace)}</div></td><td>{layer_badge(p)}</td>"
            f"<td>{e(owners.get(p.tenant, {}).get('team') or p.tenant)}</td><td>{e(blurb)}</td>"
            f"<td>{state_badge(state)}<div class=muted>{e(freshness_note(age_of(p, fresh), p.freshness_slo_seconds))}"
            f"</div></td><td>{badge(label, kind)}</td></tr>"
        )
    layers = "".join(
        f'<label style="margin-right:14px"><input type=radio name=layer value="{v}"{" checked" if v == "gold" else ""}> '
        f"{t}</label>"
        for v, t in (("gold", "Data products (gold)"), ("", "Every layer"))
    )
    script = (
        "<script>const q=document.getElementById('q'),rows=[...document.querySelectorAll('tbody tr')];"
        "function go(){const t=q.value.toLowerCase(),l=document.querySelector('input[name=layer]:checked').value;"
        "let n=0;rows.forEach(r=>{const ok=r.dataset.q.includes(t)&&(!l||r.dataset.layer===l);r.hidden=!ok;n+=ok});"
        "document.getElementById('n').textContent=n}q.oninput=go;"
        "document.querySelectorAll('input[name=layer]').forEach(i=>i.onchange=go);go()</script>"
    )
    body = (
        "<h1>Data products</h1><p class=lede>Gold tables are built to be used as they are. Open one to see what "
        "it is for, who owns it, how it is built, how fresh it is against its promise, and what you may see.</p>"
        f'<section><input id=q type=search placeholder="Search by name, team, purpose" aria-label=Search> '
        f'<p style="margin:10px 0 0">{layers}<span class=muted><span id=n></span> shown</span></p></section>'
        "<section class=scroll><table><thead><tr><th>Product<th>Layer<th>Owner<th>What it is<th>Freshness<th>Checks"
        f"</thead><tbody>{''.join(rows)}</tbody></table></section>{script}"
    )
    return page("Data products", body)


# ------------------------------------------------------------------ product page


def lineage_columns(
    p: m.Product, upstream_of: Callable[[str], tuple[str, ...]], depth: int = 5
) -> list[list[str]]:
    """Upstream keys by distance from the product, furthest first, ending with the product itself."""
    levels: list[list[str]] = [[p.key]]
    seen = {p.key}
    for _ in range(depth):
        nxt = []
        for key in levels[-1]:
            for up in upstream_of(key):
                if up not in seen:
                    seen.add(up)
                    nxt.append(up)
        if not nxt:
            break
        levels.append(nxt)
    return levels[::-1]


def node(key: str, here: m.Product, by_key: dict[str, m.Product]) -> str:
    target = by_key.get(key)
    if key == here.key:
        return f'<div class="node here"><b>{e(key)}</b><small>this product</small></div>'
    if target:
        return (
            f'<div class=node><a href="{e(target.path)}"><b>{e(key)}</b></a>'
            f"<small>{e(target.layer)}, {e(target.tenant)}</small></div>"
        )
    kind = "Kafka topic" if key.startswith("kafka:") else "source"
    return f"<div class=node><b>{e(key.removeprefix('kafka:'))}</b><small>{kind}</small></div>"


def fact(label: str, value: str) -> str:
    return f"<div class=fact><small>{e(label)}</small><b>{value}</b></div>"


def product_page(
    p: m.Product,
    *,
    fresh: dict[str, Freshness],
    build: Build | None,
    by_key: dict[str, m.Product],
    upstream_of: Callable[[str], tuple[str, ...]],
    used_by: list[m.Product],
    access: list[m.Access],
    owner: dict,
) -> str:
    age = age_of(p, fresh)
    state = m.freshness_state(age, p.freshness_slo_seconds)
    records = fresh[p.key].records if p.key in fresh else None
    label, kind = check_summary(build, len(p.checks))
    result = {c.name: c.status for c in build.checks} if build else {}

    facts = "".join(
        [
            fact("Freshness", f"{state_badge(state)} {e(m.human_age(age))}"),
            fact("Promise", f"newest commit at most {e(m.human_duration(p.freshness_slo_seconds))} old"),
            fact("Checks", badge(label, kind)),
            fact("Records", e(f"{records:,}") if records is not None else "not measured"),
            fact(
                "Last build",
                e(
                    m.human_age(time.time() - build.last_build)
                    if build and build.last_build
                    else "not a scheduled asset"
                ),
            ),
            fact("Contract", e(f"{p.contract} {p.version}, {p.status}")),
        ]
    )
    overview = (
        f"<section><h2>What it is for</h2><p>{e(p.purpose)}</p>"
        + (
            f"<p><b>Grain.</b> {e(p.granularity[:1].upper() + p.granularity[1:])}.</p>"
            if p.granularity
            else ""
        )
        + (f"<p><b>How to use it.</b> {e(p.usage)}</p>" if p.usage else "")
        + (f"<p><b>Know before you rely on it.</b> {e(p.limitations)}</p>" if p.limitations else "")
        + f"<p class=muted>Owner: {e(owner.get('team') or p.tenant)}"
        + (
            f', <a href="{e(safe_url("mailto:" + owner["email"]))}">{e(owner["email"])}</a>'
            if owner.get("email")
            else ""
        )
        + (f', <a href="{e(safe_url(owner["repo"]))}">repository</a>' if owner.get("repo") else "")
        + "</p></section>"
    )

    levels = lineage_columns(p, upstream_of)
    flow = "<span class=arrow>&rarr;</span>".join(
        f"<div class=col>{''.join(node(k, p, by_key) for k in level)}</div>" for level in levels
    )
    impl = [ln for ln in p.links if ln.type in ("transformationImplementation", "implementation")]
    built = (
        "<section><h2>How it is built</h2>"
        + (
            f"<div class=scroll><div class=flow>{flow}</div></div>"
            if len(levels) > 1
            else "<p class=muted>No upstream tables are declared for this product.</p>"
        )
        + "<ul class=plain style='margin-top:12px'>"
        + "".join(
            f'<li>Code: <a href="{e(safe_url(ln.url))}">{e(ln.description or ln.url)}</a></li>' for ln in impl
        )
        + (
            f"<li>Dagster asset in group <b>{e(build.group)}</b> ({e(', '.join(build.kinds))}), "
            f'<a href="{DAGSTER}/assets/{e(p.namespace)}/{e(p.table)}">open in Dagster</a></li>'
            if build
            else ""
        )
        + "</ul>"
        + (
            f"<p class=muted style='margin-bottom:0'>Used by: {', '.join(f'<a href="{e(u.path)}">{e(u.key)}</a>' for u in used_by)}</p>"
            if used_by
            else ""
        )
        + "</section>"
    )

    declared = {c.name: c for c in p.checks}
    names = list(dict.fromkeys([*declared, *result]))
    check_rows = "".join(
        f"<tr><td><code>{e(n)}</code></td><td>{e(declared[n].description) if n in declared else ''}</td>"
        f"<td>{e(declared[n].dimension) if n in declared else ''}</td>"
        f"<td>{badge(result.get(n, 'not run yet'), 'ok' if result.get(n) in CHECK_OK else ('bad' if n in result else ''))}</td></tr>"
        for n in names
    )
    checks = (
        f"<section><h2>Quality checks</h2><div class=scroll><table><thead><tr><th>Check<th>What it asserts<th>Dimension"
        f"<th>Latest result</thead><tbody>{check_rows}</tbody></table></div></section>"
        if names
        else "<section><h2>Quality checks</h2><p class=muted>No checks are declared for this product.</p></section>"
    )

    col_rows = "".join(
        f"<tr><td><code>{e(c.name)}</code>{' ' + badge('key') if c.primary_key else ''}</td><td><code>{e(c.type)}</code></td>"
        f"<td>{e(c.description)}</td><td>{badge(c.classification) if c.classification else ''} "
        f"{''.join(badge(t, 'warn') for t in c.tags)}</td></tr>"
        for c in p.columns
    )
    schema = (
        f"<section><h2>Columns</h2><div class=scroll><table><thead><tr><th>Column<th>Type<th>Meaning<th>Classification"
        f"</thead><tbody>{col_rows}</tbody></table></div></section>"
    )

    acc_rows = "".join(
        f"<tr><td><b>{e(a.persona)}</b><div class=muted>{e(', '.join(a.users))}</div></td>"
        f"<td>{badge('can read', 'ok') if a.can_read else badge('no access', 'bad')}</td>"
        f"<td>{e('; '.join(f'{c}: {h}' for c, h in a.handling)) if a.handling else ('no personal data' if a.can_read else '')}</td></tr>"
        for a in access
    )
    who = (
        "<section><h2>Who can see what</h2><p class=muted style='margin-top:0'>Decided per person by the platform "
        "(Trino and OPA) from the same entitlements, not by this page.</p><div class=scroll><table><thead><tr>"
        f"<th>Persona<th>Access<th>Personal data</thead><tbody>{acc_rows}</tbody></table></div></section>"
    )

    sql = f"SELECT *\nFROM lakehouse.{p.namespace}.{p.table}\nLIMIT 10"
    use = (
        f'<section><h2>Use it</h2><p class=muted style="margin-top:0">In the <a href="{SQL_LAB}">SQL workbench</a> '
        "(sign in on localhost), or through any Trino client as yourself:</p>"
        f'<pre>{e(sql)}</pre><p class=muted style="margin-bottom:0"><a href="/contracts/{e(p.tenant)}/{e(p.source_file)}">'
        "Read the data contract</a> (ODCS v3.2)</p></section>"
    )

    top = (
        f'<p class=muted><a href="/">Data products</a> / {e(p.tenant)} / {e(p.namespace)}</p>'
        f"<h1>{e(p.table)} {layer_badge(p)}</h1>"
        f"<p class=lede>{e(p.description or p.purpose)}</p><section><div class=grid>{facts}</div></section>"
    )
    return page(f"{p.table}: data product", top + overview + built + checks + schema + who + use)
