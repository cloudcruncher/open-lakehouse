-- Idempotent database reconciliation, run on every `make up` (service: db-migrate).
--
-- The initdb scripts in init/ only run on an empty volume. Everything added later
-- lives here as desired state, so an existing platform converges without a rebuild
-- and passwords re-sync from the secret store on every run (rotation = change the
-- secret, re-run). Variables come from psql -v; values are quoted with %L/%I, so a
-- secret can never break out of the statement.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------- roles
SELECT format('CREATE ROLE %I LOGIN', r)
FROM unnest(ARRAY['debezium', 'marquez', 'dagster', 'monitoring', 'superset']) AS r
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) \gexec

ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD :'debezium_password';
ALTER ROLE marquez  WITH LOGIN PASSWORD :'marquez_password';
ALTER ROLE dagster  WITH LOGIN PASSWORD :'dagster_password';
ALTER ROLE superset WITH LOGIN PASSWORD :'superset_password';
-- Metrics only: pg_monitor reads statistics and replication-slot state, never table data.
ALTER ROLE monitoring WITH LOGIN PASSWORD :'monitoring_password';
GRANT pg_monitor TO monitoring;

-- ------------------------------------------------------------ databases
SELECT format('CREATE DATABASE %I OWNER %I', d, d)
FROM unnest(ARRAY['marquez', 'dagster', 'superset']) AS d
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = d) \gexec

REVOKE ALL ON DATABASE marquez FROM PUBLIC;
REVOKE ALL ON DATABASE dagster FROM PUBLIC;
REVOKE ALL ON DATABASE superset FROM PUBLIC;
GRANT CONNECT ON DATABASE marquez TO marquez;
GRANT CONNECT ON DATABASE dagster TO dagster;
GRANT CONNECT ON DATABASE superset TO superset;
GRANT CONNECT ON DATABASE corebank TO debezium;

-- ---------------------------------------------------- CDC on the source
\connect corebank

-- Debezium reads the initial snapshot (SELECT) and the WAL (REPLICATION). It can't
-- write to the source and can't see anything outside the published tables.
GRANT USAGE ON SCHEMA core TO debezium;
GRANT SELECT ON core.customers, core.accounts, core.transactions, core.complaints TO debezium;

-- An explicit publication, created here by the platform. Debezium doesn't create
-- one (publication.autocreate.mode=disabled), so the CDC identity never needs
-- ownership of source tables.
SELECT 'CREATE PUBLICATION corebank_cdc FOR TABLE core.customers, core.accounts, core.transactions, core.complaints'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'corebank_cdc') \gexec

-- ------------------------------------------------------------- audit trail
\connect audit

-- The gateway links each answer to its audit row (provenance), so INSERT ... RETURNING
-- needs to read back the row's number and chain hash. Only those two columns: the
-- writer still can't read who looked at what.
GRANT SELECT (seq, row_hash) ON audit.tool_calls TO audit_writer;

-- Hash chain: row_hash = sha256(prev_hash || canonical row content). An advisory lock
-- serialises writers so the chain stays linear. The row's seq is assigned under the
-- same lock (the identity value is overridden), so seq order is chain order: two tool
-- calls in the same millisecond once took seq values in one order and the lock in the
-- other, and verify_chain reported a false break.
CREATE OR REPLACE FUNCTION audit.chain_hash() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, public AS $$
DECLARE
    last_seq BIGINT;
    last_hash TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('audit.tool_calls'));
    SELECT seq, row_hash INTO last_seq, last_hash FROM audit.tool_calls ORDER BY seq DESC LIMIT 1;
    NEW.seq := COALESCE(last_seq, 0) + 1;
    NEW.prev_hash := COALESCE(last_hash, 'genesis');
    NEW.row_hash := encode(digest(
        NEW.prev_hash || '|' || NEW.event_id::text || '|' || extract(epoch FROM NEW.occurred_at)::text || '|' ||
        NEW.colleague || '|' || NEW.agent_client || '|' || NEW.tool || '|' ||
        NEW.arguments::text || '|' || NEW.outcome || '|' || COALESCE(NEW.rows_returned::text, ''),
        'sha256'), 'hex');
    RETURN NEW;
END $$;

CREATE OR REPLACE TRIGGER chain_hash BEFORE INSERT ON audit.tool_calls
    FOR EACH ROW EXECUTE FUNCTION audit.chain_hash();

-- Recomputes the chain in seq order; returns the first broken seq, or NULL if intact.
-- Rows written before seq was assigned under the lock can show a false break (or, where
-- the old trigger chained to the highest seq, a real fork): see docs/runbooks.md#audit-chain.
CREATE OR REPLACE FUNCTION audit.verify_chain() RETURNS BIGINT
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

GRANT EXECUTE ON FUNCTION audit.verify_chain() TO audit_writer;
