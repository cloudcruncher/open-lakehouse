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
