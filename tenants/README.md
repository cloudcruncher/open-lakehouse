# Tenants

Teams build on the platform from their own repos ([ADR 14](../docs/adr/0014-platform-and-tenant-teams-in-separate-repos.md)).
To join, open a PR here that adds `tenants/<name>.yaml`; `markets-data.yaml` is the example.

A tenant gets, and may only use, what its file declares:
- Kafka topics starting with `<domain>.`: partitions and retention are part of the request,
  and retention bounds how far back the team can replay (Kappa reprocessing).
- Polaris namespaces starting with `<domain>_`.
- A Keycloak group `tenant-<name>`.
- A Dagster code location running `codeLocation.image` (a pinned tag, never `latest`).

`make tenants` validates every file ([schema](schema/tenant.schema.json)) and `make test` runs
the checker's tests. A file is refused if it breaks the schema, is not named after the tenant,
reaches outside its domain, takes a platform name (`bronze`, `corebank.*`, ...), or reuses
another tenant's domain.

The reconciler (`tenant-reconcile`, run by `make up` and `make tenants-apply`) makes the
platform match these files. It validates them with the same checker first, and applies nothing
if any file is invalid. It never deletes: a topic or namespace no tenant declares is logged as an
orphan. Partitions only grow (a shrink is refused), and the tenant's Polaris identity
`tenant_<name>` can manage tables in its own namespaces only. Its credentials are written to the
platform secrets volume as `tenant_<name>.env`. Colleagues see nothing of the tenant's data until
its contracts grant access (OPA denies by default).
