# /// script
# requires-python = ">=3.12"
# dependencies = ["psycopg[binary]>=3.2", "trino>=0.340", "httpx>=0.28"]
# ///
"""Measure real end-to-end freshness: source commit -> queryable in silver, as a colleague.

Writes one uniquely tagged transaction into the core-banking source (the way a card
payment would land), then polls Trino *as a colleague* until it shows up, through
CDC -> Kafka -> stream -> Iceberg -> Polaris -> Trino -> OPA. Prints the seconds
taken; exits non-zero past the budget.

Usage: uv run scripts/freshness_probe.py [--budget 60]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import trino
from trino.auth import JWTAuthentication

ROOT = Path(__file__).resolve().parent.parent
CUSTOMER = "C0000052"  # a Meridian customer alice can serve


def env(key: str) -> str:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise SystemExit(f"{key} missing from .env; run make secrets")


def token(user: str) -> str:
    resp = httpx.post(
        "http://localhost:8280/realms/bank/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "trino-cli",
            "username": user,
            "password": env("DEMO_USER_PASSWORD"),
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def insert_source_txn(txn_id: str) -> None:
    sql = (
        "INSERT INTO core.transactions (txn_id, account_id, txn_ts, amount, currency, merchant, category, "
        "channel, status) SELECT %s, account_id, now(), -12.34, 'GBP', 'Freshness Probe Ltd', 'shopping', "
        "'card', 'pending' FROM core.accounts WHERE customer_id = %s AND product = 'current_account' LIMIT 1"
    )
    literal = sql.replace("%s", "'{}'").format(txn_id, CUSTOMER)
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "psql",
            "-q",
            "-U",
            "postgres",
            "-d",
            "corebank",
            "-c",
            literal,
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=float, default=60.0)
    ap.add_argument("--user", default="alice")
    args = ap.parse_args()

    conn = trino.dbapi.connect(
        host="localhost",
        port=8443,
        http_scheme="https",
        auth=JWTAuthentication(token(args.user)),
        verify=str(ROOT / ".secrets/ca.pem"),
        catalog="lakehouse",
    )
    cur = conn.cursor()
    txn_id = f"TPROBE{uuid.uuid4().hex[:14]}"
    insert_source_txn(txn_id)
    start = time.monotonic()
    while (elapsed := time.monotonic() - start) < args.budget:
        cur.execute(
            "SELECT count(*) FROM silver.transactions WHERE txn_id = ?", [txn_id]
        )
        if cur.fetchone()[0]:
            print(
                f"freshness: {elapsed:.1f}s (source commit -> visible to {args.user} in silver)"
            )
            return 0
        time.sleep(1)
    print(f"freshness: NOT visible after {args.budget:.0f}s")
    return 1


if __name__ == "__main__":
    sys.exit(main())
