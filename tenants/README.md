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
| Contracts in the image | `/contracts/*.odcs.yaml`: the team's data contracts, copied there by its Dockerfile. The platform's catalog reads them from the image it runs (`make catalog-sync`), so a product page shows what that release was built and checked with. Standard ODCS fields carry the product information: `description.purpose/usage/limitations`, `dataGranularityDescription`, `slaProperties` (`freshness` per table), `quality` (names matching the Dagster asset checks), `customProperties` `upstream`, `authoritativeDefinitions` `transformationImplementation`, and per-column `description`, `classification`, `tags` |
| Contracts check | `uses: cloudcruncher/open-lakehouse/.github/workflows/tenant-contracts.yml@<version>` with `tenant: <name>`, `platform-ref: <version>`: ODCS schema, tables only in the tenant's namespaces, and every `pii.*` / `special_category` column masked by the platform's OPA policy (a new mask is a platform PR) |
| Image bump | `uses: cloudcruncher/open-lakehouse/.github/workflows/tenant-image-bump.yml@<version>` from the tenant's release workflow, with `tenant: <name>`, `image-ref: <image without tag>`, `version: <new tag>` and secret `platform-token` (push and PR rights on the platform repo). It rewrites only the `image:` lines of that exact ref in `tenants/<name>.yaml` (code location and services; other images, such as `card-auths-0.7.0`, stay), regenerates `compose.yaml`, runs `tenants/check.py` and opens PR `bump/<name>-<version>` listing the changed lines. It never merges or pushes to main: platform CI runs and the platform merges. The same version again opens nothing |
| Long-running services | `services:` in the tenant file (name, image, `command`, `memoryMb`, `deploy`): producers and streams run as `tenant-<name>-<service>` with the same identity and credentials mount as the code server, on `data` and `stream` only (no `meta`: no Dagster, source or audit database). `stateVolume: true` mounts a volume at `/state` that survives restarts (streaming checkpoints); the base image from `0.3.0` creates `/state` owned by `spark` |
| Secrets | `secrets:` in the tenant file names each slot (a vendor licence, an API key); a service lists the ones it uses and gets `<NAME>_ENV_FILE=/run/tenant-secrets/<name>.env`. The team fills a slot itself: `make tenant-secret TENANT=<name> NAME=<slot> FILE=<env file>` checks the slot is declared and the file is KEY=VALUE lines, stores it in the tenant's own folder of the secrets volume (next to `polaris.env`, so only the tenant's containers can read it), prints key names only, and restarts the services using it. Values never enter git; a service must cope with the file missing (the platform's CI has no licences) |
| Trino identity | The reconciler makes a Keycloak client `tenant-<name>` (client credentials, audience `trino`) and writes `TRINO_CLIENT_ID` / `TRINO_CLIENT_SECRET` to `trino.env` in the tenant's folder of the secrets volume. Trino sees it as `service-account-tenant-<name>`; OPA (`entitlements.tenants`, a unit test keeps it equal to `namespaces:`) lets it read those namespaces unmasked and nothing else. `scripts/trino-tenant-sql.sh <tenant> "<sql>"` runs as it |
| Restart | Tenants have no Docker socket. The platform restarts on request: `make tenant-restart T=<tenant> S=<service>` (`S=code` for the code server). Only what the tenant file declares and deploys can be named |
| Kafka identity | Kafka's SASL listener (`kafka:9094`, SCRAM-SHA-512) gives each tenant a user `tenant-<name>`; the reconciler writes `KAFKA_BOOTSTRAP`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_USERNAME`, `KAFKA_PASSWORD` to `kafka.env` in the tenant's folder. ACLs allow read/write/describe on the topics in its tenant file and reads in consumer groups prefixed `<name>-` (Spark: `groupIdPrefix`). Not enforced until a tenant moves off the trusted listener (`9092`) |

What a tenant can do by itself, inside its own namespaces and with no platform PR: create, rename and
drop tables and views, and write their properties. The catalog allows `DROP` with purge (Spark's
`DROP TABLE` always purges), and the canary proves all of it in `make verify`. Not included: dropping
a namespace, or touching another tenant's or the platform's. A view written by Spark
uses Spark's SQL dialect, which Trino refuses to read, so a tenant's shared interface is a table (a Kappa
replay swap is two table renames, markets-data's `make replay-swap`) or a **Trino view** made with its own
Trino identity (`CREATE VIEW` in its namespaces; the query engine holds `VIEW_CREATE` there, OPA is the gate).
A view runs as its owner, so it would carry a tagged (PII) column past the masks: OPA refuses to let anyone
read a view that selects one, so it fails closed. Share those columns through the table, which Trino masks.

The **catalog** (`make urls`: Data products) gives each table a page a consumer can trust before writing a
query: what it is for and what one row means, who owns it, how it is built (upstream tables and a link to
the code), its freshness against the promise in the contract, the latest result of each quality check, what
every column means, and what each persona may see. Tenants write that in their contracts; the platform
renders and measures it, so every tenant gets it the same way. No login yet (metadata only; data still goes
through Trino as the colleague).

Observability is the platform's, analytics are the tenant's. List tables under `observe:` in the tenant
file (add `topic:` for a table that appends every record of one of your topics) and the platform watches
them: `tenant-metrics` reads commit times and record counts from Polaris table metadata and log-end
offsets from Kafka (no Spark, no data access), and the **Tenant streams — freshness and lag** dashboard in
Grafana shows minutes since each table's last commit, records behind the topic, and a red panel if a table
holds more records than its topic ever had (duplicates). Everyone except platform admins is a read-only
Grafana viewer. Superset is for building: every colleague may register datasets and make charts and
dashboards on it (Alpha role); what a chart returns is still decided per colleague by Trino and OPA, so a
table you may not read is refused or not even visible to you.

Deploying a new image is a PR here that bumps `codeLocation.image` (and sets `deploy: true` the
first time), then `make tenants-render`: the generated `compose.yaml` block and Dagster's
`workspace.yaml` change in the same PR, so what runs is reviewed like any other change.

Blueprints: `blueprints:` lists the platform blueprints the tenant needs (`streaming`, `orchestration`, `corebank`,
`observability`, `lineage`, `bi`, `ai`; default `streaming, orchestration`). `make up USE=<tenant>` starts the core,
the union of those blueprints and only that tenant's `tenant-<name>` services; `USE=bank` is the core-banking preset.
`make first-data T=<tenant>` times that stack until the tenant's `*_gold` tables in `observe:` return rows.

Memory: all deployed tenant workloads together (code servers and services, by `memoryMb`) get
`MEMORY_BUDGET_MB` (4608, sized for a 10 GB Docker VM: the canary, and markets-data's code server, streams app and producers) in
`tenants/render.py`; `make lint` fails over it, so the next workload is a decision in its PR. `make mem` shows use against limits per container. Tenant code
servers are switched off by leaving `tenants` out of `PROFILES`, e.g. `make up PROFILES="streaming orchestration"` (after `make down` if running);
the reconciler still runs, so topics, namespaces and grants stay in place.

The canary (`tenants/canary.yaml`) is the platform's own tenant: five synthetic people in
`canary_data.people`, email tagged PII. Its code (`jobs/spark/orchestration/canary_tenant/`) runs
in the generated `tenant-canary-code` server on the platform's own image, so every platform PR
exercises the real path: reconcile, code server, tenant-only credentials, Spark writing through
Polaris. `make canary` runs it (about 25 s, peaks near 850 MiB of its 1024). Dagster's
`workspace.yaml` is baked into the orchestrator image, so a new location appears after `make up`
(it rebuilds); with tenants switched off, their locations show as unavailable in the UI.

Tenant workloads also get `PLATFORM_SCALE` (`laptop` or `full`, from `make SCALE=`), so a team can
run its streams as scheduled catch-ups on a laptop and always-on at scale (markets-data does).

Known laptop compromise: tenant code servers share the Dagster instance database with the
platform (runs execute in the code server). In production each tenant gets its own run launcher
and database credentials.
