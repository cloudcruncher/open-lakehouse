# 8. Assets, lineage and contracts as code (Dagster, OpenLineage, ODCS)

**Status:** accepted

## Context
Cron plus scripts can't tell you which table is stale, what broke, what a column
means or who is affected. Governance documents that aren't executable drift from
reality.

## Decision
- **Dagster** models every table as an asset, from source to data product. Spark jobs
  run unchanged under **Dagster Pipes** and report materializations (rows, quarantine
  counts, Iceberg snapshot id) and the **same WAP checks** that gate publishing. Only
  explicit reports count (no implicit materializations). Retries use exponential
  backoff with jitter; a data-quality halt is `Failure(allow_retries=False)`. The data
  product declares a freshness policy.
- **OpenLineage** is emitted automatically by the Spark listener in every job, including
  the stream, into **Marquez**. It is best effort: if lineage is down, data still flows.
- **ODCS v3.2 data contracts** (`contracts/`) are the source of truth for schema and
  classification. CI validates them against the official JSON schema and fails if OPA's
  masking tags and the contract's PII tags disagree in either direction. `make verify`
  fails if the live tables drift from the contract.
- Dagster and Marquez have no login of their own, so both sit behind oauth2-proxy with
  Keycloak SSO. Dagster is restricted to platform admins.

## Consequences
- A new PII column can't ship unmasked, and a mask can't drift from its classification.
- Stale or failed assets are visible and alertable before a colleague notices.
- Two more services to run; Marquez is amd64-only upstream (emulated on Apple silicon).
