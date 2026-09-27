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

## What a tenant repo builds against (the platform's published interface)

These names are a versioned contract: renaming one is a breaking change for every tenant.

| Interface | Value |
|---|---|
| Base image | `ghcr.io/cloudcruncher/open-lakehouse-spark:<version>` (Spark, Iceberg, Kafka and OpenLineage jars baked in; signed, multi-arch; published by `release.yml`) |
| Networks | `open-lakehouse_data` (Polaris, object storage), `open-lakehouse_stream` (Kafka), `open-lakehouse_meta` (Dagster, Marquez) as `external: true` in the tenant's own Compose file for local development |
| Code server | `dagster code-server` on port 4000, module from `codeLocation.module`; the platform runs it as `tenant-<name>-code` once `codeLocation.deploy: true` |
| Credentials | `/run/tenant-secrets/polaris.env` (`POLARIS_CLIENT_ID`, `POLARIS_CLIENT_SECRET`); the mount holds this tenant's folder only (`POLARIS_ENV_FILE` points at it) |
| Other environment | `TENANT`, `KAFKA_BOOTSTRAP=kafka:9092`, `OPENLINEAGE_URL=http://marquez:5000`, `AWS_REGION` |
| Contracts check | `uses: cloudcruncher/open-lakehouse/.github/workflows/tenant-contracts.yml@<version>` with `tenant: <name>`, `platform-ref: <version>`: ODCS schema, tables only in the tenant's namespaces, and every `pii.*` / `special_category` column masked by the platform's OPA policy (a new mask is a platform PR) |
| Long-running services | `services:` in the tenant file (name, image, `command`, `memoryMb`, `deploy`): producers and streams run as `tenant-<name>-<service>` with the same identity and credentials mount as the code server, on `data` and `stream` only (no `meta`: no Dagster, source or audit database). `stateVolume: true` mounts a volume at `/state` that survives restarts (streaming checkpoints); the base image from `0.3.0` creates `/state` owned by `spark` |
| Secrets | `secrets:` in the tenant file names each slot (a vendor licence, an API key); a service lists the ones it uses and gets `<NAME>_ENV_FILE=/run/tenant-secrets/<name>.env`. The team fills a slot itself: `make tenant-secret TENANT=<name> NAME=<slot> FILE=<env file>` checks the slot is declared and the file is KEY=VALUE lines, stores it in the tenant's own folder of the secrets volume (next to `polaris.env`, so only the tenant's containers can read it), prints key names only, and restarts the services using it. Values never enter git; a service must cope with the file missing (the platform's CI has no licences) |

Deploying a new image is a PR here that bumps `codeLocation.image` (and sets `deploy: true` the
first time), then `make tenants-render`: the generated `compose.yaml` block and Dagster's
`workspace.yaml` change in the same PR, so what runs is reviewed like any other change.

Memory: all deployed tenant workloads together (code servers and services, by `memoryMb`) get
`MEMORY_BUDGET_MB` (4608: the canary, and markets-data's code server, stream and producers) in
`tenants/render.py`; `make lint` fails over it, so the next workload is a decision in its PR. `make mem` shows use against limits per container. Tenant code
servers are switched off with `make up PROFILES="streaming ops"` (after `make down` if running);
the reconciler still runs, so topics, namespaces and grants stay in place.

The canary (`tenants/canary.yaml`) is the platform's own tenant: five synthetic people in
`canary_data.people`, email tagged PII. Its code (`jobs/spark/orchestration/canary_tenant/`) runs
in the generated `tenant-canary-code` server on the platform's own image, so every platform PR
exercises the real path: reconcile, code server, tenant-only credentials, Spark writing through
Polaris. `make canary` runs it (about 25 s, peaks near 850 MiB of its 1024). Dagster's
`workspace.yaml` is baked into the orchestrator image, so a new location appears after `make up`
(it rebuilds); with tenants switched off, their locations show as unavailable in the UI.

Known laptop compromise: tenant code servers share the Dagster instance database with the
platform (runs execute in the code server). In production each tenant gets its own run launcher
and database credentials.
