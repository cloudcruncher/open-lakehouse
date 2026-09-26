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

-- Chain: row_hash = sha256(prev_hash || canonical row content). An advisory lock
-- serialises writers so the chain stays linear under concurrency.
CREATE FUNCTION audit.chain_hash() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, public AS $$
DECLARE
    last_hash TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('audit.tool_calls'));
    SELECT row_hash INTO last_hash FROM audit.tool_calls ORDER BY seq DESC LIMIT 1;
    NEW.prev_hash := COALESCE(last_hash, 'genesis');
    NEW.row_hash := encode(digest(
        NEW.prev_hash || '|' || NEW.event_id::text || '|' || extract(epoch FROM NEW.occurred_at)::text || '|' ||
        NEW.colleague || '|' || NEW.agent_client || '|' || NEW.tool || '|' ||
        NEW.arguments::text || '|' || NEW.outcome || '|' || COALESCE(NEW.rows_returned::text, ''),
        'sha256'), 'hex');
    RETURN NEW;
END $$;

CREATE TRIGGER chain_hash BEFORE INSERT ON audit.tool_calls
    FOR EACH ROW EXECUTE FUNCTION audit.chain_hash();

CREATE FUNCTION audit.block_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit.tool_calls is append-only';
END $$;

CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON audit.tool_calls
    FOR EACH ROW EXECUTE FUNCTION audit.block_mutation();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON audit.tool_calls
    FOR EACH STATEMENT EXECUTE FUNCTION audit.block_mutation();

-- Recomputes the chain; returns the first broken seq, or NULL if intact.
CREATE FUNCTION audit.verify_chain() RETURNS BIGINT
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = audit, public AS $$
DECLARE
    r RECORD;
    expected_prev TEXT := 'genesis';
BEGIN
    FOR r IN SELECT * FROM audit.tool_calls ORDER BY seq LOOP
        IF r.prev_hash <> expected_prev OR r.row_hash <> encode(digest(
            r.prev_hash || '|' || r.event_id::text || '|' || extract(epoch FROM r.occurred_at)::text || '|' ||
            r.colleague || '|' || r.agent_client || '|' || r.tool || '|' ||
            r.arguments::text || '|' || r.outcome || '|' || COALESCE(r.rows_returned::text, ''),
            'sha256'), 'hex') THEN
            RETURN r.seq;
        END IF;
        expected_prev := r.row_hash;
    END LOOP;
    RETURN NULL;
END $$;

GRANT USAGE ON SCHEMA audit TO audit_writer;
GRANT INSERT ON audit.tool_calls TO audit_writer;
-- Read back only its own row's number and chain hash (provenance), never the content.
GRANT SELECT (seq, row_hash) ON audit.tool_calls TO audit_writer;
GRANT EXECUTE ON FUNCTION audit.verify_chain() TO audit_writer;
