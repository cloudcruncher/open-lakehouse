"""Call simulator: plays the contact-centre platform (telephony + streaming speech-to-text).

POST /calls {call_id, colleague, scenario} starts a call:
  1. picks a real customer on the colleague's brand line from the core-banking source
     (someone whose surname + postcode district is unique, so the demo is unambiguous);
  2. makes the world match the story: e.g. the fraudulent payment is written into core
     banking and has to reach the lakehouse through CDC like any other change;
  3. streams the transcript to Kafka (`contact-centre.transcripts`, keyed by call id,
     so one call's utterances stay ordered), paced like real speech.

This service owns the *source system's* write credentials, like the bank's own
systems do. The assist service never gets them: it only ever sees the transcript.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import uvicorn
import yaml
from aiokafka import AIOKafkaProducer
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

log = logging.getLogger("call-sim")

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TRANSCRIPT_TOPIC", "contact-centre.transcripts")
SPEED = float(os.environ.get("CALL_SIM_SPEED", 1.0))  # >1 = faster than real speech
CONFIG = yaml.safe_load((Path(__file__).parent / "scenarios.yaml").read_text())
SCENARIOS: dict[str, Any] = CONFIG["scenarios"]
QUEUES: dict[str, str] = CONFIG["queues"]

producer: AIOKafkaProducer | None = None
running: dict[str, asyncio.Task] = {}


def dsn() -> str:
    return (
        f"host={os.environ.get('COREBANK_HOST', 'postgres')} dbname=corebank user=corebank "
        f"password={os.environ['COREBANK_DB_PASSWORD']}"
    )


def pick_customer(brand: str) -> dict[str, Any]:
    """A random customer on this brand line, with an open current account and a unique name+district."""
    sql = """
        SELECT c.customer_id, c.first_name, c.last_name, c.postcode, a.account_id
        FROM core.customers c JOIN core.accounts a USING (customer_id)
        WHERE c.brand = %s AND a.product = 'current_account' AND a.status = 'open'
          AND NOT EXISTS (
            SELECT 1 FROM core.customers o
            WHERE o.brand = c.brand AND o.customer_id <> c.customer_id AND o.last_name = c.last_name
              AND split_part(o.postcode, ' ', 1) = split_part(c.postcode, ' ', 1))
        ORDER BY random() LIMIT 1
    """
    with psycopg.connect(dsn(), connect_timeout=5) as conn:
        row = conn.execute(sql, (brand,)).fetchone()
    if row is None:
        raise RuntimeError(f"no suitable {brand} customer in the source system")
    keys = ["customer_id", "first", "last", "postcode", "account_id"]
    return dict(zip(keys, row, strict=True))


def apply_world(world: dict[str, Any], cust: dict[str, Any]) -> dict[str, Any]:
    """Write the scenario's facts into core banking. They reach the lakehouse via CDC."""
    values: dict[str, Any] = {}
    now = datetime.now(UTC)
    with psycopg.connect(dsn(), connect_timeout=5) as conn:
        if fp := world.get("fraud_payment"):
            conn.execute(
                "INSERT INTO core.transactions (txn_id, account_id, txn_ts, amount, currency, merchant, category, "
                "channel, status) VALUES (%s, %s, %s, %s, 'GBP', %s, 'shopping', 'card', 'pending')",
                (
                    f"T{uuid.uuid4().hex[:20]}",
                    cust["account_id"],
                    now - timedelta(seconds=60),
                    -Decimal(str(fp["amount"])),
                    fp["merchant"],
                ),
            )
            values.update(amount=f"{fp['amount']:.2f}", merchant=fp["merchant"])
        if op := world.get("outgoing_payment"):
            conn.execute(
                "INSERT INTO core.transactions (txn_id, account_id, txn_ts, amount, currency, merchant, category, "
                "channel, status) VALUES (%s, %s, %s, %s, 'GBP', %s, 'transfer', 'faster_payment', 'posted')",
                (
                    f"T{uuid.uuid4().hex[:20]}",
                    cust["account_id"],
                    now - timedelta(days=op["days_ago"], hours=3),
                    -Decimal(str(op["amount"])),
                    op["merchant"],
                ),
            )
            values.update(amount=f"{op['amount']:.2f}", merchant=op["merchant"])
        if oc := world.get("open_complaint"):
            conn.execute(
                "INSERT INTO core.complaints (complaint_id, customer_id, opened_at, channel, category, summary, "
                "status) VALUES (%s, %s, %s, 'phone', %s, %s, 'investigating')",
                (
                    f"CMP{random.randint(5_000_000, 9_999_999)}",
                    cust["customer_id"],  # noqa: S311 - demo ids
                    now - timedelta(days=oc["days_ago"]),
                    oc["category"],
                    oc["summary"],
                ),
            )
        conn.commit()
    return values


def speaking_time(text: str, speed: float) -> float:
    """~2.8 words/s plus a turn-taking pause, like a real phone conversation."""
    return (0.9 + len(text.split()) / 2.8) / speed


async def send(event: dict[str, Any]) -> None:
    assert producer is not None
    await producer.send_and_wait(TOPIC, value=event, key=event["call_id"])


async def play(call_id: str, colleague: str, name: str, speed: float = SPEED) -> None:
    sc = SCENARIOS[name]
    try:
        cust = await asyncio.to_thread(pick_customer, QUEUES.get(colleague, QUEUES["default"]))
        values = {**cust, **await asyncio.to_thread(apply_world, sc.get("world") or {}, cust)}
        log.info("call %s: %s for %s (customer %s)", call_id, name, colleague, cust["customer_id"])
        await send(
            {
                "type": "start",
                "call_id": call_id,
                "colleague": colleague,
                "scenario": name,
                "title": sc["title"],
                "ts": time.time(),
            }
        )
        await asyncio.sleep(1.5 / speed)
        for seq, (speaker, line) in enumerate(sc["lines"], start=1):
            text = line.format(**values)
            await asyncio.sleep(speaking_time(text, speed))
            await send(
                {
                    "type": "utterance",
                    "call_id": call_id,
                    "colleague": colleague,
                    "seq": seq,
                    "speaker": speaker,
                    "text": text,
                    "is_final": True,
                    "ts": time.time(),
                }
            )
        await asyncio.sleep(2 / speed)
        await send({"type": "end", "call_id": call_id, "colleague": colleague, "ts": time.time()})
    except Exception:
        log.exception("call %s failed", call_id)
        await send(
            {"type": "end", "call_id": call_id, "colleague": colleague, "ts": time.time(), "error": True}
        )
    finally:
        running.pop(call_id, None)


async def start_call(request: Request) -> JSONResponse:
    body = await request.json()
    name, call_id, colleague = body.get("scenario"), body.get("call_id"), body.get("colleague")
    if name not in SCENARIOS or not call_id or not colleague:
        return JSONResponse({"error": "need scenario, call_id, colleague"}, status_code=400)
    speed = min(max(float(body.get("speed") or SPEED), 0.5), 5.0)  # tests run calls faster
    running[call_id] = asyncio.create_task(play(call_id, colleague, name, speed))
    return JSONResponse(
        {"call_id": call_id, "scenario": name, "title": SCENARIOS[name]["title"]}, status_code=202
    )


async def list_scenarios(_: Request) -> JSONResponse:
    return JSONResponse([{"id": k, "title": v["title"]} for k, v in SCENARIOS.items()])


async def healthz(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "active_calls": len(running)})


@asynccontextmanager
async def lifespan(_: Starlette) -> AsyncIterator[None]:
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA,
        acks="all",
        enable_idempotence=True,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    for attempt in range(30):
        try:
            await producer.start()
            break
        except Exception as exc:  # noqa: BLE001 - Kafka may still be starting
            log.warning("kafka not ready (%s), retry %d", exc, attempt + 1)
            await asyncio.sleep(2)
    yield
    await producer.stop()


app = Starlette(
    routes=[
        Route("/calls", start_call, methods=["POST"]),
        Route("/scenarios", list_scenarios),
        Route("/healthz", healthz),
    ],
    lifespan=lifespan,
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8091)), log_level="warning")  # noqa: S104
