# Working in open-lakehouse

A governed lakehouse for a bank, runnable on a laptop: Postgres core banking → Debezium → Kafka →
Spark Structured Streaming → Iceberg (Polaris REST catalog, RustFS) → Trino + OPA → MCP gateway →
Live Call Assist. Open work is in [docs/next-steps.md](docs/next-steps.md); read it first.

## Commands
- `make demo`: start everything, seed, run pipelines, then `make verify`. Safe to re-run.
- A stack without core banking (tenants only, ADR 15): `make up PROFILES="streaming orchestration tenants"`; `make verify`
  then skips the bank sections. `corebank` is the blueprint with the CDC services.
- `make stop BLUEPRINTS="bi lineage"`: stop only those blueprints (the reverse of `make up PROFILES=...`).
- `make verify`: 73 end-to-end checks. Run after any change that touches the running stack.
- `make test` (OPA + Python unit tests), `make lint`, `make evals`, `make contracts`.
- Tenant teams (ADR 14): `tenants/*.yaml`, `make tenants` (validate), `make tenants-apply` (reconcile),
  `make mem` (memory per container; tenant code servers are budgeted in `tenants/render.py`).
- `make sql U=alice Q="..."` and `make agent U=alice T=get_customer_360 A='{...}'` run as a colleague.
- `make urls` lists every portal (SQL workbench: Superset SQL Lab on `localhost:3004`).
  Personas: alice (contact centre), bob (complaints), carol (analyst), ops_admin (platform
  admin). Access rules: `infra/opa/data/entitlements.json`.

## Conventions
- Python via uv only. Work on a branch, open a PR; CI (`ci`, `e2e`) must be green before merging.
  Docs-only PRs skip `e2e`; PRs skip its chaos sample (it runs on `main` and weekly). Keep CI
  fast: the target is a release an hour, here and in tenant repos.
- Grafana dashboards are generated: edit `infra/grafana/build_dashboards.py`, run `make dashboards`,
  commit the JSON. `make lint` fails if committed JSON differs from the builder output, so it
  reports a failure until regenerated dashboards are committed.
- Orchestration code is baked into the `open-lakehouse/spark:dev` image. After editing
  `jobs/spark/orchestration/`, run `docker compose --profile orchestration build dagster-code` and
  `docker compose --profile orchestration up -d dagster-code`.
- Data contracts (`contracts/*.odcs.yaml`) and OPA column tags must agree; `make contracts` checks it.

## Claude setup (repo layer)
Agent teams, autonomy, cost and alert rules are global (`~/.claude`); this repo adds only:
- `contracts-reviewer` agent (contracts, OPA column tags, write-audit-publish). Use it in `/team-review`.
- `.claude/hooks/project-checks.sh`: the task-completion gate also runs this repo's ruff rules (120 cols, E,F,B),
  `make contracts` and `make tenants` on changed files.
- Never read bulky or generated files: `infra/grafana/dashboards/*.json`, `services/*/uv.lock`, `*requirements.txt`,
  `contracts/schema/*.json`, `docs/img/*`. Run the builder or the check instead. Tail `make verify`.

## Gotchas
- Never read or print `.env` (generated secrets, Anthropic key). The demo password is in it;
  ask the user to type it into Keycloak when a browser sign-in is needed.
- The CDC stream owns silver through a writer lease (a Polaris namespace property). Batch silver
  refuses to run while the lease is live; scheduled runs skip it instead.
- Gold waits for the stream to drain (`writer.drained-at`) before reading silver.
- PySpark streaming offsets arrive as dicts, not JSON strings.
- MinIO images are gone from Docker Hub, hence RustFS. Polaris needs `kmsUnavailable` on RustFS.
- OPA: `x in {ruleRef, "lit"}` misbehaves; use literal sets.
- Bronze is readable by ops_admin only; `payload` columns (tag `pii.raw_record`) are NULL below
  full PII clearance. A new bronze/ops table needs a contract and tags, or `make contracts` misses it.
- Superset queries Trino with each colleague's own OAuth2 token (no service account). Config:
  `infra/superset/`. Portals must be opened on `localhost`, not `127.0.0.1` (Keycloak redirect URIs).
  The token dies with the Keycloak session (30 min idle); SQL Lab then asks to authorize again.
- `make sql` passes the query through make, so `$` is eaten: for `"table$snapshots"` use
  `scripts/trino-sql.sh ops_admin '...'` directly.
- `dagster.yaml` is a single-file bind mount: editing it with `sed -i` (a new file) leaves running containers
  seeing it as missing. Edit in place, then recreate `dagster-code`, `dagster-daemon`, `dagster-webserver` and the
  tenant code servers by name (`up -d --force-recreate <names>`; with no names it recreates every profile).
- `docker kill` bypasses restart policies; `make heal` reconciles.
- Counter series only exist once incremented: PromQL over error counters needs `or vector(0)`.
- GitHub: poll `gh` sparingly (secondary rate limit). The e2e workflow cancels superseded runs,
  so "cancelled" on an older commit is normal.
