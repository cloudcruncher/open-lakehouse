# 2. Two authorization layers: engines at the catalog, people in the engine

**Status:** accepted

## Context
Colleagues, BI tools and AI agents all read the same tables but need different rows and
columns. Raw (bronze) data contains unmasked PII.

## Decision
- **Layer 1, Polaris RBAC (coarse, per engine):** the ETL principal owns content; the
  query-engine principal can read only silver, gold and ops. Bronze is unreachable from
  SQL for everyone, admins included.
- **Layer 2, OPA (fine, per person):** Trino delegates every decision to OPA: schema
  access by persona, brand row filters, and tag-driven column masks that keep the column
  type. Entitlements come from IdP groups and catalog column tags as data, not code.

## Consequences
- Defence in depth: a policy bug in one layer doesn't expose raw data.
- Policies are unit-tested (`opa test`) like application code.
- Trino fails closed when OPA is unreachable (verified by chaos testing).
- Brand is denormalised onto every customer-scoped table so row filters are cheap
  predicates, not joins.
