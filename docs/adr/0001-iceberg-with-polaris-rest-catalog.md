# 1. Apache Iceberg tables behind an Iceberg REST catalog (Apache Polaris)

**Status:** accepted

## Context
Tables must be readable and writable by several engines (Spark, Trino, and, for
interop, commercial warehouses) without copies. Access control and storage credentials
must not live inside each engine.

## Decision
Store all tables as Iceberg. Serve them through Polaris over the Iceberg REST protocol.
Polaris owns table metadata, engine-level RBAC and **credential vending**: engines get
short-lived, table-scoped storage credentials per table load and never hold bucket keys.

## Consequences
- Any REST-compatible engine can join, including Snowflake and Databricks via catalog
  federation. The tables stay ours, in open formats, in our storage.
- Polaris is in every query's path, so it must run HA (stateless replicas, shared token
  signing key, Postgres HA).
- Vending needs an STS-capable object store. RustFS locally (`kmsUnavailable` because it
  has no KMS); S3 + IAM role in production.
