"""Live Call Assist service: transcript stream in, guidance to the colleague's console out.

    transcripts (Kafka) --> per-call CallSession --> events --> console (SSE)
                                  |                    \\--> assist-events (Kafka)
                                  \\--> MCP gateway, as the colleague (their token)

Security model:
  * The console signs the colleague in with Keycloak (authorization code + PKCE);
    this service verifies every request's token (signature, issuer, audience
    `call-assist`, expiry). No passwords, no service account.
  * A colleague can only open *their own* calls (call's colleague == token subject).
  * The engine calls data tools with that colleague's token, so the gateway's
    on-behalf-of exchange and OPA apply their own row filters and masks.
  * This service has no database credentials and no route to storage or catalog.

Scaling: transcripts are keyed by call id, so a call's utterances land on one
partition and one consumer. Run N replicas in one consumer group. Assist events
also go to Kafka, so the console fan-out can move to a separate push gateway
without changing this service.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from ..mcp_server.identity import IdentityConfig, KeycloakTokenVerifier
from .engine import CallSession, Utterance
from .knowledge import ProcedureIndex
from .signals import default_extractor
from .tools import MCPTools

log = logging.getLogger("call-assist")

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TRANSCRIPTS = os.environ.get("TRANSCRIPT_TOPIC", "contact-centre.transcripts")
ASSIST_EVENTS = os.environ.get("ASSIST_EVENTS_TOPIC", "contact-centre.assist-events")
MCP_URL = os.environ.get("MCP_URL", "http://mcp-gateway:8000/mcp")
CALL_SIM_URL = os.environ.get("CALL_SIM_URL", "http://call-sim:8091")
ISSUER = os.environ.get("OIDC_ISSUER", "http://localhost:8280/realms/bank")
STATIC = Path(__file__).parent / "static"
SESSION_TTL_S = 900

ACTIVE = Gauge("assist_active_calls", "Calls with a live session")

verifier = KeycloakTokenVerifier(
    IdentityConfig(
        issuer=ISSUER,
        internal_base=os.environ.get("OIDC_INTERNAL_BASE", "http://keycloak:8080/realms/bank"),
        audience="call-assist",
        client_id="",
        client_secret="",
    )
)
extractor = default_extractor()
index = ProcedureIndex.default()


@dataclass
class Live:
    """One call: its engine session, event history (for reconnects) and console subscribers."""

    call_id: str
    colleague: str
    session: CallSession
    history: list[dict[str, Any]] = field(default_factory=list)
    subscribers: set[asyncio.Queue] = field(default_factory=set)
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue)
    tools: MCPTools | None = None
    worker: asyncio.Task | None = None
    last_seen: float = field(default_factory=time.time)
    ended: bool = False


calls: dict[str, Live] = {}
producer: AIOKafkaProducer | None = None


def get_or_create(call_id: str, colleague: str) -> Live:
    live = calls.get(call_id)
    if live is None:

        async def emit(event: dict[str, Any], _id: str = call_id) -> None:
            await publish(_id, event)

        session = CallSession(call_id, colleague, emit, extractor, index)
        live = Live(call_id, colleague, session)
        live.worker = asyncio.create_task(run(live))
        calls[call_id] = live
        ACTIVE.set(len(calls))
    return live


async def publish(call_id: str, event: dict[str, Any]) -> None:
    live = calls.get(call_id)
    if live is None:
        return
    event = {"at": time.time(), **event}
    live.history.append(event)
    live.last_seen = time.time()
    for q in list(live.subscribers):
        q.put_nowait(event)
    if producer is not None:
        # Fire and forget: analytics and QA consume these; the console never waits on Kafka.
        task = asyncio.create_task(
            producer.send(ASSIST_EVENTS, value={"colleague": live.colleague, **event}, key=call_id)
        )
        task.add_done_callback(lambda t: t.exception() and log.warning("assist event not published"))


async def run(live: Live) -> None:
    """Process one call's transcript strictly in order."""
    while True:
        item = await live.inbox.get()
        try:
            if item["type"] == "utterance":
                await live.session.on_utterance(
                    Utterance(
                        live.call_id, live.colleague, item["seq"], item["speaker"], item["text"], item["ts"]
                    )
                )
            elif item["type"] == "start":
                await publish(live.call_id, {"type": "call_started", "title": item.get("title", "")})
            elif item["type"] == "end":
                await live.session.end()
                live.ended = True
                return
        except Exception:
            log.exception("call %s: failed to process %s", live.call_id, item.get("type"))


async def consume() -> None:
    while True:
        consumer = AIOKafkaConsumer(
            TRANSCRIPTS,
            bootstrap_servers=KAFKA,
            group_id="call-assist",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v),
        )
        try:
            await consumer.start()
            log.info("consuming %s", TRANSCRIPTS)
            async for msg in consumer:
                ev = msg.value
                live = get_or_create(ev["call_id"], ev["colleague"])
                live.inbox.put_nowait(ev)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - reconnect forever; Kafka outages are transient
            log.warning("transcript consumer error, reconnecting: %s", exc)
            await asyncio.sleep(3)
        finally:
            with suppress(Exception):
                await consumer.stop()


async def reaper() -> None:
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for call_id, live in list(calls.items()):
            if now - live.last_seen > SESSION_TTL_S:
                if live.tools:
                    await live.tools.close()
                if live.worker:
                    live.worker.cancel()
                calls.pop(call_id, None)
        ACTIVE.set(len(calls))


# ------------------------------------------------------------------ auth
async def colleague(request: Request) -> tuple[str, str] | None:
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        return None
    token = header[7:]
    access = await verifier.verify_token(token)
    if access is None:
        return None
    return (access.claims or {}).get("preferred_username") or access.subject, token


def unauthorized() -> JSONResponse:
    return JSONResponse(
        {"error": "sign in required"}, status_code=401, headers={"WWW-Authenticate": "Bearer"}
    )


def attach(live: Live, token: str) -> None:
    if live.tools is None:
        live.tools = MCPTools(MCP_URL, token)
        asyncio.create_task(live.session.attach_tools(live.tools))
    else:
        live.tools.update_token(token)


# ---------------------------------------------------------------- routes
async def start_demo_call(request: Request) -> JSONResponse:
    who = await colleague(request)
    if who is None:
        return unauthorized()
    user, token = who
    body = await request.json()
    call_id = f"CALL-{uuid.uuid4().hex[:8].upper()}"
    live = get_or_create(call_id, user)
    attach(live, token)
    async with httpx.AsyncClient(timeout=5) as http:
        resp = await http.post(
            f"{CALL_SIM_URL}/calls",
            json={
                "call_id": call_id,
                "colleague": user,
                "scenario": body.get("scenario"),
                "speed": body.get("speed"),
            },
        )
    if resp.status_code >= 400:
        return JSONResponse({"error": resp.text}, status_code=resp.status_code)
    return JSONResponse({"call_id": call_id, **resp.json()})


async def events(request: Request) -> Response:
    who = await colleague(request)
    if who is None:
        return unauthorized()
    user, token = who
    call_id = request.path_params["call_id"]
    live = get_or_create(call_id, user)
    if live.colleague != user:
        return JSONResponse({"error": "not your call"}, status_code=403)
    attach(live, token)
    queue: asyncio.Queue = asyncio.Queue()
    backlog = list(live.history)
    live.subscribers.add(queue)

    async def stream() -> AsyncIterator[bytes]:
        try:
            for ev in backlog:
                yield f"data: {json.dumps(ev, default=str)}\n\n".encode()
            while True:
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(ev, default=str)}\n\n".encode()
                    if ev.get("type") == "ended":
                        return
                except TimeoutError:
                    yield b": keep-alive\n\n"
        finally:
            live.subscribers.discard(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


async def refresh_token(request: Request) -> JSONResponse:
    who = await colleague(request)
    if who is None:
        return unauthorized()
    user, token = who
    live = calls.get(request.path_params["call_id"])
    if live is None or live.colleague != user:
        return JSONResponse({"error": "no such call"}, status_code=404)
    attach(live, token)
    return JSONResponse({"ok": True})


async def verified(request: Request) -> JSONResponse:
    who = await colleague(request)
    if who is None:
        return unauthorized()
    live = calls.get(request.path_params["call_id"])
    if live is None or live.colleague != who[0]:
        return JSONResponse({"error": "no such call"}, status_code=404)
    await live.session.mark_verified()
    return JSONResponse({"ok": True})


async def scenarios(_: Request) -> JSONResponse:
    async with httpx.AsyncClient(timeout=3) as http:
        try:
            return JSONResponse((await http.get(f"{CALL_SIM_URL}/scenarios")).json())
        except httpx.HTTPError:
            return JSONResponse([], status_code=503)


async def config(_: Request) -> JSONResponse:
    return JSONResponse(
        {
            "issuer": ISSUER,
            "client_id": os.environ.get("CONSOLE_CLIENT_ID", "agent-console"),
            "extractor": getattr(extractor, "name", "rules"),
            "model": getattr(extractor, "model", None),
        }
    )


async def index_page(_: Request) -> FileResponse:
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store"})


async def healthz(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "calls": len(calls)})


async def metrics(_: Request) -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@asynccontextmanager
async def lifespan(_: Starlette) -> AsyncIterator[None]:
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
    )
    for attempt in range(30):
        try:
            await producer.start()
            break
        except Exception as exc:  # noqa: BLE001 - Kafka may still be starting
            log.warning("kafka not ready (%s), retry %d", exc, attempt + 1)
            await asyncio.sleep(2)
    tasks = [asyncio.create_task(consume()), asyncio.create_task(reaper())]
    yield
    for t in tasks:
        t.cancel()
    await producer.stop()


SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'self'; connect-src 'self' http://localhost:8280; "
    "img-src 'self' data:; style-src 'self'; script-src 'self'; frame-ancestors 'none'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}


class SecurityHeaders:
    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        async def with_headers(message: dict) -> None:
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                headers.extend((k.lower().encode(), v.encode()) for k, v in SECURITY_HEADERS.items())
            await send(message)

        await self.app(scope, receive, with_headers if scope["type"] == "http" else send)


app = SecurityHeaders(
    Starlette(
        routes=[
            Route("/", index_page),
            Route("/config", config),
            Route("/healthz", healthz),
            Route("/metrics", metrics),
            Route("/api/scenarios", scenarios),
            Route("/api/calls", start_demo_call, methods=["POST"]),
            Route("/api/calls/{call_id}/events", events),
            Route("/api/calls/{call_id}/token", refresh_token, methods=["POST"]),
            Route("/api/calls/{call_id}/verified", verified, methods=["POST"]),
            Mount("/static", StaticFiles(directory=STATIC), name="static"),
        ],
        lifespan=lifespan,
    )
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8090)), log_level="warning")  # noqa: S104
