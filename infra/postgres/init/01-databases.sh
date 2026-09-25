#!/usr/bin/env bash
# One Postgres instance hosts four logically separate databases, each with its own
# owner role, so they can be split onto separate clusters later without code
# changes. In production each would be its own HA cluster (e.g. CloudNativePG with a
# synchronous replica), and the source system would never share a server with the
# platform's own metadata.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
  CREATE ROLE polaris   LOGIN PASSWORD '${POLARIS_DB_PASSWORD}';
  CREATE ROLE keycloak  LOGIN PASSWORD '${KEYCLOAK_DB_PASSWORD}';
  CREATE ROLE corebank  LOGIN PASSWORD '${COREBANK_DB_PASSWORD}';
  -- Read-only identity used by ingestion. It can SELECT from the source but never write.
  CREATE ROLE corebank_reader LOGIN PASSWORD '${COREBANK_READER_PASSWORD}';
  -- The agent gateway can only INSERT audit events: no UPDATE, DELETE or TRUNCATE.
  CREATE ROLE audit_writer LOGIN PASSWORD '${AUDIT_WRITER_PASSWORD}';

  CREATE DATABASE polaris  OWNER polaris;
  CREATE DATABASE keycloak OWNER keycloak;
  CREATE DATABASE corebank OWNER corebank;
  CREATE DATABASE audit    OWNER postgres;
SQL

# Nobody gets implicit access through PUBLIC.
for db in polaris keycloak corebank audit; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -c "REVOKE ALL ON DATABASE ${db} FROM PUBLIC;"
done
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
  GRANT CONNECT ON DATABASE polaris  TO polaris;
  GRANT CONNECT ON DATABASE keycloak TO keycloak;
  GRANT CONNECT ON DATABASE corebank TO corebank, corebank_reader;
  GRANT CONNECT ON DATABASE audit    TO audit_writer;
SQL
