-- Synthetic "core banking" source system. Every table has updated_at so ingestion
-- can pull incrementally, and wal_level=logical (set in compose) enables CDC later.
\connect corebank
SET ROLE corebank;

CREATE SCHEMA core AUTHORIZATION corebank;

CREATE TABLE core.customers (
    customer_id        TEXT PRIMARY KEY,
    brand              TEXT        NOT NULL,
    first_name         TEXT        NOT NULL,
    last_name          TEXT        NOT NULL,
    date_of_birth      DATE        NOT NULL,
    email              TEXT        NOT NULL,
    phone              TEXT        NOT NULL,
    postcode           TEXT        NOT NULL,
    region             TEXT        NOT NULL,
    segment            TEXT        NOT NULL,
    vulnerability_flag BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE core.accounts (
    account_id  TEXT PRIMARY KEY,
    customer_id TEXT          NOT NULL REFERENCES core.customers,
    product     TEXT          NOT NULL,
    iban        TEXT          NOT NULL,
    status      TEXT          NOT NULL,
    balance     NUMERIC(18,2) NOT NULL,
    currency    TEXT          NOT NULL DEFAULT 'GBP',
    opened_at   TIMESTAMPTZ   NOT NULL,
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE core.transactions (
    txn_id     TEXT PRIMARY KEY,
    account_id TEXT          NOT NULL REFERENCES core.accounts,
    txn_ts     TIMESTAMPTZ   NOT NULL,
    amount     NUMERIC(18,2) NOT NULL,
    currency   TEXT          NOT NULL DEFAULT 'GBP',
    merchant   TEXT,
    category   TEXT          NOT NULL,
    channel    TEXT          NOT NULL,
    status     TEXT          NOT NULL,
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE core.complaints (
    complaint_id TEXT PRIMARY KEY,
    customer_id  TEXT        NOT NULL REFERENCES core.customers,
    opened_at    TIMESTAMPTZ NOT NULL,
    channel      TEXT        NOT NULL,
    category     TEXT        NOT NULL,
    summary      TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    resolution   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Incremental extraction reads by updated_at, so index it.
CREATE INDEX ON core.customers (updated_at);
CREATE INDEX ON core.accounts (updated_at);
CREATE INDEX ON core.transactions (updated_at);
CREATE INDEX ON core.complaints (updated_at);

RESET ROLE;
GRANT USAGE ON SCHEMA core TO corebank_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO corebank_reader;
