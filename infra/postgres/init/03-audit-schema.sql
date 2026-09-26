-- Tamper-evident audit log for every agent tool call.
--  * audit_writer can only INSERT (no UPDATE/DELETE/TRUNCATE grants).
--  * A trigger blocks UPDATE/DELETE even for the table owner, so the log stays
--    append-only unless someone drops the trigger — and that is itself visible in DDL
--    history.
--  * Each row carries a SHA-256 hash chained to the previous row, so any edit made
--    by going around these controls breaks the chain; `verify_chain()` detects it.
-- In production this stream also lands in WORM object storage and the SIEM.
\connect audit

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA audit;

CREATE TABLE audit.tool_calls (
    seq             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    colleague       TEXT        NOT NULL,   -- human the agent acted for (token sub / username)
    agent_client    TEXT        NOT NULL,   -- OAuth client that called the MCP gateway
    tool            TEXT        NOT NULL,
    arguments       JSONB       NOT NULL,
    purpose         TEXT,                   -- declared business reason (e.g. call id)
    outcome         TEXT        NOT NULL CHECK (outcome IN ('ok', 'denied', 'error', 'unavailable')),
    rows_returned   INTEGER,
    trino_query_ids TEXT[],
    latency_ms      INTEGER,
    error           TEXT,
    prev_hash       TEXT        NOT NULL,
    row_hash        TEXT        NOT NULL
);
CREATE INDEX ON audit.tool_calls (colleague, occurred_at);

-- The hash chain (trigger and verify_chain) is defined in infra/postgres/reconcile.sql,
-- which runs before any service writes and again on every `up`, so fixes reach old databases.

CREATE FUNCTION audit.block_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit.tool_calls is append-only';
END $$;

CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON audit.tool_calls
    FOR EACH ROW EXECUTE FUNCTION audit.block_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON audit.tool_calls
    FOR EACH STATEMENT EXECUTE FUNCTION audit.block_mutation();

GRANT USAGE ON SCHEMA audit TO audit_writer;
GRANT INSERT ON audit.tool_calls TO audit_writer;
-- Read back only its own row's number and chain hash (provenance), never the content.
GRANT SELECT (seq, row_hash) ON audit.tool_calls TO audit_writer;
