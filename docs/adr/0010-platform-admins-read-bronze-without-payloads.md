# 10. Platform admins read bronze, but never raw record payloads

**Status:** accepted. Amends [ADR 2](0002-two-layer-authorization.md), which kept bronze
unreachable from SQL for everyone.

## Context
Data engineers debugging ingestion need to answer "did this change arrive, when, in what
order, and why isn't it in silver?". With bronze closed to SQL, the only way in was a
Spark job. But bronze can't simply be opened: `bronze.cdc_events.payload` (and the
`payload` of `ops.quarantine` and `ops.cdc_pending`) is the whole source row as JSON.
Column masks work per column, so they can't reach inside it, and bronze has no brand
column for row filters. The same gap already existed for quarantine: platform admins, who
may see no PII, could read quarantined records in full.

## Decision
- **Polaris:** the query-engine principal may read bronze (same read-only privileges as
  silver, gold and ops). OPA remains the per-person gate.
- **OPA:** only the platform-admin persona has bronze, and only with every brand (bronze
  is not brand-filtered). Alice, Bob and Carol are refused.
- **A new tag, `pii.raw_record`,** marks whole-record payloads. It is all-or-nothing: NULL
  below full PII clearance, whatever the persona. The raw bronze copies of source tables
  carry the same PII tags as silver.
- **Contracts:** `contracts/bronze.odcs.yaml` and `contracts/ops.odcs.yaml` declare these
  tables and tags, so the existing contract/policy agreement check covers them.

## Consequences
- Platform admins can inspect every event's entity, operation, key, source LSN, Kafka
  offset and arrival time in SQL, and filtering on `payload` reveals nothing (masks apply
  before predicates). `make verify` asserts both, and that colleagues are refused.
- Reading a record's contents still needs full PII clearance. A time-boxed, audited
  break-glass path for admins is future work.
- The contract check covers declared tables and tagged columns only. A new bronze table
  must get a contract and its tags in the same change, or platform admins would see it
  unmasked.
