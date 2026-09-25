"""Synthetic core-banking data: customers, accounts, transactions, complaints.

Modes:
  (default)        initial load; a no-op if data already exists (idempotent)
  --increment N    simulate a day of activity: N new transactions, profile updates, complaints
  --inject-bad N   write N transactions that break data contracts (unknown currency,
                   future timestamps) to prove the WAP gate stops them reaching readers
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
from faker import Faker

log = logging.getLogger("seed-corebank")

BRANDS = {"Meridian": 0.55, "Northgate": 0.35, "Isle": 0.10}  # fictional brands
REGIONS = ["London", "South East", "North West", "Scotland", "Wales", "Midlands", "Northern Ireland"]
SEGMENTS = ["retail", "premier", "business"]
PRODUCTS = ["current_account", "savings", "credit_card", "mortgage", "personal_loan"]
CATEGORIES = ["groceries", "travel", "utilities", "dining", "transfer", "cash", "shopping", "salary", "fees"]
CHANNELS = ["card", "online", "mobile", "branch", "direct_debit", "faster_payment"]
COMPLAINT_CATEGORIES = [
    ("fees_and_charges", "Customer disputes an overdraft fee charged on {d}."),
    ("fraud_scam", "Customer reports an unrecognised card payment of £{a} to {m}."),
    ("service", "Customer unhappy with wait time on the phone line when reporting a lost card."),
    ("payments", "Faster payment of £{a} to {m} has not arrived after two days."),
    ("mortgage", "Customer questions the rate applied after their fixed term ended."),
    ("vulnerability_support", "Customer asked for additional support after a bereavement; follow-up missed."),
]


def dsn() -> str:
    return (
        f"host={os.environ.get('COREBANK_HOST', 'postgres')} dbname=corebank user=corebank "
        f"password={os.environ['COREBANK_DB_PASSWORD']}"
    )


def pick_brand() -> str:
    return random.choices(list(BRANDS), weights=list(BRANDS.values()))[0]


def gen_customers(fake: Faker, n: int) -> list[tuple]:
    now = datetime.now(UTC)
    rows = []
    for i in range(n):
        first, last = fake.first_name(), fake.last_name()
        rows.append(
            (
                f"C{i + 1:07d}",
                pick_brand(),
                first,
                last,
                fake.date_of_birth(minimum_age=18, maximum_age=90),
                f"{first}.{last}{random.randint(1, 999)}@example.com".lower(),
                f"07{random.randint(100000000, 999999999)}",
                fake.postcode(),
                random.choice(REGIONS),
                random.choices(SEGMENTS, weights=[0.8, 0.15, 0.05])[0],
                random.random() < 0.04,
                now - timedelta(days=random.randint(30, 3650)),
                now,
            )
        )
    return rows


def gen_accounts(customers: list[tuple]) -> list[tuple]:
    now = datetime.now(UTC)
    rows = []
    n = 0
    for c in customers:
        for product in ["current_account", *random.sample(PRODUCTS[1:], k=random.randint(0, 2))]:
            n += 1
            balance = Decimal(random.randint(-150000, 2500000)) / 100
            if product in ("mortgage", "personal_loan", "credit_card"):
                balance = -abs(balance) * 10
            rows.append(
                (
                    f"A{n:08d}",
                    c[0],
                    product,
                    f"GB{random.randint(10, 99)}MERI{random.randint(10**13, 10**14 - 1)}",
                    random.choices(["open", "closed", "frozen"], weights=[0.93, 0.05, 0.02])[0],
                    balance,
                    "GBP",
                    c[11] + timedelta(days=random.randint(0, 30)),
                    now,
                )
            )
    return rows


def gen_transactions(
    fake: Faker, accounts: list[tuple], n: int, days: int = 90, start: int = 0
) -> list[tuple]:
    now = datetime.now(UTC)
    rows = []
    for i in range(n):
        acc = random.choice(accounts)
        category = random.choice(CATEGORIES)
        amount = Decimal(random.randint(100, 250000)) / 100
        if category != "salary":
            amount = -amount
        rows.append(
            (
                f"T{uuid.uuid4().hex[:20]}" if start else f"T{i + 1:010d}",
                acc[0],
                now - timedelta(seconds=random.randint(0, days * 86400)),
                amount,
                "GBP",
                fake.company() if category not in ("cash", "salary", "fees") else None,
                category,
                random.choice(CHANNELS),
                random.choices(["posted", "pending", "reversed"], weights=[0.94, 0.05, 0.01])[0],
                now,
            )
        )
    return rows


def gen_complaints(fake: Faker, customers: list[tuple], n: int, start: int = 0) -> list[tuple]:
    now = datetime.now(UTC)
    rows = []
    for i in range(n):
        c = random.choice(customers)
        category, template = random.choice(COMPLAINT_CATEGORIES)
        opened = now - timedelta(days=random.randint(0, 120))
        status = random.choices(
            ["open", "investigating", "resolved", "referred_to_fos"], weights=[0.25, 0.25, 0.45, 0.05]
        )[0]
        rows.append(
            (
                f"CMP{start + i + 1:07d}",
                c[0],
                opened,
                random.choice(["phone", "branch", "online", "letter"]),
                category,
                template.format(d=opened.date(), a=random.randint(20, 900), m=fake.company()),
                status,
                "Fee refunded as goodwill." if status == "resolved" else None,
                now,
            )
        )
    return rows


def copy_rows(cur: psycopg.Cursor, table: str, columns: list[str], rows: list[tuple]) -> None:
    with cur.copy(f"COPY core.{table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    log.info("loaded %s rows into core.%s", f"{len(rows):,}", table)


CUSTOMER_COLS = [
    "customer_id",
    "brand",
    "first_name",
    "last_name",
    "date_of_birth",
    "email",
    "phone",
    "postcode",
    "region",
    "segment",
    "vulnerability_flag",
    "created_at",
    "updated_at",
]
ACCOUNT_COLS = [
    "account_id",
    "customer_id",
    "product",
    "iban",
    "status",
    "balance",
    "currency",
    "opened_at",
    "updated_at",
]
TXN_COLS = [
    "txn_id",
    "account_id",
    "txn_ts",
    "amount",
    "currency",
    "merchant",
    "category",
    "channel",
    "status",
    "updated_at",
]
COMPLAINT_COLS = [
    "complaint_id",
    "customer_id",
    "opened_at",
    "channel",
    "category",
    "summary",
    "status",
    "resolution",
    "updated_at",
]


def initial_load(conn: psycopg.Connection, customers_n: int, txns_n: int, complaints_n: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM core.customers")
        if cur.fetchone()[0] > 0:
            log.info("core banking already seeded; nothing to do")
            return
        fake = Faker("en_GB")
        Faker.seed(42)
        random.seed(42)
        customers = gen_customers(fake, customers_n)
        accounts = gen_accounts(customers)
        copy_rows(cur, "customers", CUSTOMER_COLS, customers)
        copy_rows(cur, "accounts", ACCOUNT_COLS, accounts)
        copy_rows(cur, "transactions", TXN_COLS, gen_transactions(fake, accounts, txns_n))
        copy_rows(cur, "complaints", COMPLAINT_COLS, gen_complaints(fake, customers, complaints_n))
    conn.commit()


def load_existing(cur: psycopg.Cursor) -> tuple[list[tuple], list[tuple]]:
    cur.execute("SELECT customer_id FROM core.customers")
    customers = [(r[0],) for r in cur.fetchall()]
    cur.execute("SELECT account_id FROM core.accounts WHERE status = 'open'")
    accounts = [(r[0],) for r in cur.fetchall()]
    return customers, accounts


def increment(conn: psycopg.Connection, n: int) -> None:
    fake = Faker("en_GB")
    with conn.cursor() as cur:
        customers, accounts = load_existing(cur)
        copy_rows(cur, "transactions", TXN_COLS, gen_transactions(fake, accounts, n, days=1, start=1))
        cur.execute("SELECT count(*) FROM core.complaints")
        start = cur.fetchone()[0]
        copy_rows(
            cur, "complaints", COMPLAINT_COLS, gen_complaints(fake, customers, max(1, n // 200), start=start)
        )
        # Profile changes: exercise upserts (SCD1) downstream.
        cur.execute(
            """
            UPDATE core.customers SET phone = '07' || (floor(random() * 899999999) + 100000000)::bigint,
                                      updated_at = now()
            WHERE customer_id IN (SELECT customer_id FROM core.customers ORDER BY random() LIMIT %s)
        """,
            (max(1, n // 100),),
        )
        cur.execute(
            """
            UPDATE core.complaints SET status = 'resolved', resolution = 'Resolved after review.',
                                       updated_at = now()
            WHERE complaint_id IN (SELECT complaint_id FROM core.complaints
                                   WHERE status = 'open' ORDER BY random() LIMIT %s)
        """,
            (max(1, n // 400),),
        )
    conn.commit()


def inject_bad(conn: psycopg.Connection, n: int) -> None:
    fake = Faker("en_GB")
    with conn.cursor() as cur:
        _, accounts = load_existing(cur)
        rows = gen_transactions(fake, accounts, n, days=1, start=1)
        future = datetime.now(UTC) + timedelta(days=700)
        bad = [(r[0], r[1], future, r[3], "ZZZ", *r[5:]) for r in rows]
        copy_rows(cur, "transactions", TXN_COLS, bad)
    conn.commit()
    log.warning("injected %d contract-violating transactions", n)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=int(os.environ.get("SEED_CUSTOMERS", 20000)))
    ap.add_argument("--transactions", type=int, default=int(os.environ.get("SEED_TRANSACTIONS", 400000)))
    ap.add_argument("--complaints", type=int, default=int(os.environ.get("SEED_COMPLAINTS", 3000)))
    ap.add_argument("--increment", type=int, metavar="N")
    ap.add_argument("--inject-bad", type=int, metavar="N")
    args = ap.parse_args()

    with psycopg.connect(dsn()) as conn:
        if args.inject_bad:
            inject_bad(conn, args.inject_bad)
        elif args.increment:
            increment(conn, args.increment)
        else:
            initial_load(conn, args.customers, args.transactions, args.complaints)
    return 0


if __name__ == "__main__":
    sys.exit(main())
