"""The data product catalog service: pages rendered live from contracts, tenant files, Prometheus and Dagster.

Nothing is published or synced at request time, so a page is as current as its sources (a few seconds).
Contracts come from two folders: the platform's own, and the ones tenants ship in their code-location
image under /contracts (copied here by `make catalog-sync`).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.routing import Route

from lakehouse_platform.catalog import model as m
from lakehouse_platform.catalog import render
from lakehouse_platform.catalog.sources import Build, Sources

CONTRACT_DIRS = [
    Path(d) for d in os.environ.get("CONTRACT_DIRS", "/contracts:/tenant-contracts").split(":") if d
]
TENANTS_DIR = Path(os.environ.get("TENANTS_DIR", "/etc/tenants"))
ENTITLEMENTS = Path(os.environ.get("ENTITLEMENTS_FILE", "/etc/entitlements.json"))
PORT = int(os.environ.get("PORT", "8100"))

log = logging.getLogger("catalog")
sources = Sources(
    os.environ.get("PROMETHEUS_URL", "http://prometheus:9090"),
    os.environ.get("DAGSTER_URL", "http://dagster-webserver:3000"),
)


def entitlements() -> dict:
    return json.loads(ENTITLEMENTS.read_text())["entitlements"]


def world() -> tuple[list[m.Product], dict[str, m.Product]]:
    """Products are re-read on every request: contracts change when a tenant ships, and there are few."""
    products = m.load_products(CONTRACT_DIRS)
    return products, {p.key: p for p in products}


def builds(p: m.Product) -> Build | None:
    return sources.build(p.namespace, p.table)


def upstream_of(by_key: dict[str, m.Product]):
    def up(key: str) -> tuple[str, ...]:
        p = by_key.get(key)
        if p is None:
            return ()
        known = builds(p)
        return tuple(dict.fromkeys([*p.upstream, *(known.upstream if known else ())]))

    return up


async def home(request: Request) -> HTMLResponse:
    products, _ = world()
    return HTMLResponse(render.index(products, sources.freshness(), builds, m.owners(TENANTS_DIR)))


async def product(request: Request) -> HTMLResponse:
    tenant, namespace, table = (request.path_params[k] for k in ("tenant", "namespace", "table"))
    products, by_key = world()
    p = next((x for x in products if (x.tenant, x.namespace, x.table) == (tenant, namespace, table)), None)
    if p is None:
        raise HTTPException(404, "No such data product")
    up = upstream_of(by_key)
    used_by = [x for x in products if p.key in up(x.key)]
    return HTMLResponse(
        render.product_page(
            p,
            fresh=sources.freshness(),
            build=builds(p),
            by_key=by_key,
            upstream_of=up,
            used_by=used_by,
            access=m.access_for(p, entitlements()),
            owner=m.owners(TENANTS_DIR).get(p.tenant, {}),
        )
    )


async def products_json(request: Request) -> JSONResponse:
    """One entry per product with its live state: what `make verify` and other tools read."""
    products, _ = world()
    fresh = sources.freshness()
    out = []
    for p in products:
        age = render.age_of(p, fresh)
        b = builds(p)
        out.append(
            {
                "tenant": p.tenant,
                "key": p.key,
                "layer": p.layer,
                "contract": f"{p.contract} {p.version}",
                "freshness": {
                    "state": m.freshness_state(age, p.freshness_slo_seconds),
                    "age_seconds": age,
                    "promise_seconds": p.freshness_slo_seconds,
                },
                "checks": {c.name: c.status for c in b.checks} if b else {},
                "declared_checks": [c.name for c in p.checks],
                "upstream": list(p.upstream),
                "has_owner": p.tenant in m.owners(TENANTS_DIR),
            }
        )
    return JSONResponse(out)


async def contract_file(request: Request) -> PlainTextResponse:
    tenant, name = request.path_params["tenant"], request.path_params["name"]
    for folder in CONTRACT_DIRS:
        try:
            path = (folder / name).resolve()
        except (ValueError, OSError):  # a NUL byte or an unreadable path is not a contract
            continue
        if path.is_file() and folder.resolve() in path.parents and path.name.endswith(".odcs.yaml"):
            if yaml_tenant(path) == tenant:
                return PlainTextResponse(path.read_text(), media_type="text/yaml; charset=utf-8")
    raise HTTPException(404, "No such contract")


def yaml_tenant(path: Path) -> str:
    import yaml

    data = yaml.safe_load(path.read_text())
    return str(data.get("tenant", "")) if isinstance(data, dict) else ""


async def health(request: Request) -> PlainTextResponse:
    return PlainTextResponse("ok")


app = Starlette(
    routes=[
        Route("/", home),
        Route("/healthz", health),
        Route("/api/products.json", products_json),
        Route("/products/{tenant}/{namespace}/{table}", product),
        Route("/contracts/{tenant}/{name:path}", contract_file),
    ]
)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")  # noqa: S104 - inside the compose network
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
