# Next steps

State on 26 Sep 2026. Fixed that day: Dagster open to any colleague, Grafana memory and SSO-only
login, the nightly silver lease race, SLO panels showing "No data", and the quarantine-rate axis
(PR #5). Then (PR #7): a SQL workbench (Superset SQL Lab) that queries Trino as each colleague,
bronze readable by platform admins with raw payloads NULL (which also closed quarantine payloads
to them), `SHOW TABLES` working, and sign-in via `127.0.0.1` redirected. Then the AI layer
(PRs #8 to #11): a labelling guide that brought Claude's precision to 1.00 and is prompt-cached,
the after-call note drafted by Claude from the transcript, a daily spend cap, AI usage and
cost on the Live Call Assist dashboard with two alerts, and "Ask the assistant" (pilot).
Then fixes: Superset asks to authorize again after a 30-minute idle (PR #12), and the audit
chain stays linear when tool calls land together (PR #13; the local audit log was reset
because the old bug had forked it). `make verify`: 56/56. API spend that day: about $0.40.

## Resume here: phase 0, onboarding tenant teams
From 27 Sep the stack is used as a platform for teams in their own repos
([ADR 14](adr/0014-platform-and-tenant-teams-in-separate-repos.md)): `lakehouse-markets-data`
(data engineering, "Markets & Payments Intelligence"), `lakehouse-ai-desk` (AI engineering)
and `lakehouse-risk-signals` (hybrid). Still **one small step per turn, the repo owner checks
each step before the next**. Phase 0 makes onboarding a single PR here:

1. The tenant file: a schema for `tenants/<team>.yaml` (owner, Keycloak group, Kafka topics,
   Polaris namespaces, code-location image) and a test that a bad file fails `make test`.
2. The reconciler: topics, namespaces and grants, and a Keycloak group, from the tenant files.
3. Shared interfaces: external networks, the Spark base image tag, and a Dagster code location
   per tenant in `services/orchestrator/workspace.yaml`.
4. A reusable GitHub workflow that runs the contract/policy check on a tenant's contracts.
5. A tenant Compose profile, with memory measured before and after.
6. A canary tenant (tiny synthetic data) and platform checks that run against it.

Then phase 1: `lakehouse-markets-data` onboards as a new tenant. Phase 2: core banking moves
out to `lakehouse-corebank-data` and Live Call Assist to the AI team, with their `verify`
checks, so platform e2e shrinks to a few minutes. Platform releases get versioned tags that
tenants pin.

Step 1 is done (PR #16: `tenants/`, `make tenants`). Step 2 is done: `tenant-reconcile`
(`make tenants-apply`) with three `verify` checks. Step 3 is done: generated code servers and
Dagster locations (`make tenants-render`, `codeLocation.deploy`), per-tenant credential folders,
a network-join check, and the interface table in `tenants/README.md`; `v0.1.0` is tagged and its
images are public. Step 4 is done: `.github/workflows/tenant-contracts.yml` (`workflow_call`) runs
`contracts/check.py --tenant` on a tenant repo's contracts, released as `v0.2.0`. Step 5 is done:
`tenants` in `PROFILES` switches tenant code servers on/off, a 3072 MB budget for them fails
`make lint`, and `make mem`. Baseline with no tenant: 6.7 of 11.7 GiB used, limits add up to
18.5 GiB; `rustfs` (98%) and `cdc-connect` (96%) sit at their limits. The "after" figure comes
with the canary tenant. Step 6a is done: the canary tenant (`tenants/canary.yaml`, `make canary`)
runs in its generated code server and writes `canary_data.people` as its own principal (200 on
its namespace, 403 on `silver` and `markets_bronze`); memory after: 7.0 GiB used, the canary
idles at ~190 MiB and peaks at ~850 of 1024 MiB. Step 6b is done: `contracts/canary.odcs.yaml`
(name `pii.name`, email `pii.contact`), `canary_data` readable by every persona, OPA tests, and five
`verify` checks (the canary writes as itself; bob sees the email, alice masked, carol NULL and an
initial; no brand filter). `make verify`: 66 checks. Phase 0 is complete; next is phase 1
(`lakehouse-markets-data`). The CDC stream now audits silver with the WAP checks (on start, then
every 10 minutes when it changed), records them in `ops.dq_results`, and reports runless
materializations and check results to Dagster, so silver's asset page shows the stream's writes.

Phase 1 (1.1 FX, 1.2 deploy, 1.3 tenant services, 1.4 Coinbase, 1.5 Kappa stream, 1.6 card auths,
1.7 sanctions, 1.8 gold, 1.9 replay, 1.10 dashboards). Step 1.1: the repo
`cloudcruncher/lakehouse-markets-data` (public) lands ECB FX rates in `markets_bronze.fx_rates`
(13,132 rows from 2025-01-01) as its own identity; its CI runs in ~30 s. Step 1.2: it publishes
a signed `lakehouse-markets-data:0.1.0`, the platform deploys it (`deploy: true`, 2560 of 3072 MB
tenant budget), and OPA grants `markets_gold` to every colleague and `markets_bronze` /
`markets_silver` to platform admins only. Step 1.3: `services:` in the
tenant file (rendered as `tenant-<name>-<service>`, data and stream networks only, optional
`/state` volume), the tenant budget raised to 4608 MB for all workloads, and `/state` in the Spark
base image (released as `v0.3.0`). Step 1.4: markets-data `0.2.0` adds a Coinbase producer (public
`matches` feed, no key, keyed by product), run by the platform as the tenant service
`coinbase-feed` (128 MB, ~19 MiB used). The lineage UI now sits behind nginx, with oauth2-proxy
only signing in: oauth2-proxy rewrote encoded dataset namespaces into 404s, and cookies from every
localhost portal overflowed Marquez's 8 KiB header limit. `make verify`: 69 checks. Next is 1.5:
the Kappa stream (Kafka -> `markets_bronze.trades` -> `markets_silver.trades`) in markets-data.

## Parked: the AI data engineer routine, step 4
Done before the pivot: 1 platform health (Grafana, Dagster), 2 querying as each colleague
(and why carol is refused silver), 3 a SQL workbench (Superset) and bronze for platform
admins. Step 4 was **a change request**: core banking adds a column and it is carried end to
end, one step per turn:

1. The source change: add a column to `core.customers`, for example `secondary_phone`
   (personal data, so it exercises the governance path). Source DDL:
   `infra/postgres/init/02-corebank-schema.sql` plus `infra/postgres/reconcile.sql` for the
   running database; data: `services/platform/src/lakehouse_platform/seed/corebank.py`.
2. Watch it arrive: Debezium carries the new field into `bronze.cdc_events` (as ops_admin;
   the payload stays NULL, so check the batch `bronze.customers` or the stream's logs).
3. The contract: add the column with a `pii.contact` tag to `contracts/*.odcs.yaml`, and
   see `make contracts` fail until OPA agrees.
4. The policy: tag it in `infra/opa/data/entitlements.json` (`column_tags`), add an OPA test.
5. Silver and gold: map it in the silver pipeline (`jobs/spark/`), Iceberg schema evolution,
   and into gold only if a data product needs it.
6. Prove it as each colleague (Superset or `make sql`): alice sees it masked, bob in full,
   carol not at all; then a `verify` check, docs, and a PR.

Before starting: confirm item 1 below (the 02:00 UTC `nightly_refresh` run) in Dagster.

Ordered by value. Each item says where to start and how to know it is done.

## 1. Confirm the nightly lease fix in real use
The fix is covered by a verify check, but the schedule has not yet run with it.
- Where: Dagster → Automation → `nightly_refresh` (02:00 UTC).
- Done when: the next scheduled run is green. If the stream held the lease, the silver step's
  log says "silver skipped" and gold is skipped for that run (gold_refresh rebuilds it).

## 2. Make the fraud-payment call robust to CDC lag
Main's e2e (run 36242946867) failed once in the re-verify after chaos, then passed on re-run:
"fraud payment written seconds ago is pinpointed (CDC -> agent)". CDC lag was 10 s after chaos and
the scripted call plays at 3x speed, so the agent looked up transactions before the payment reached
silver. A real caller can do the same.
- Already there: `_recent()` in `call_assist/engine.py` looks again when the caller names an
  amount it can't see yet, but only `FRESH_RETRIES = 3` times, 4 s apart: an 8 s window, and
  the lag after chaos was 10 s.
- Where: same place. Size the window from measured freshness instead of a fixed count (keep
  looking while the tool's `data_as_of` is older than the caller's report, up to a cap such as
  30 s), and say "still arriving" rather than "not found" meanwhile.
- Done when: the call finds the payment with the stream deliberately lagged (for example, pause
  `cdc-stream` for 15 s during the call), and the e2e check passes repeatedly after chaos.

## 3. Show streaming writes in Dagster
Dagster only sees batch writes, so silver shows "Failed, 13 hours ago" while the CDC stream keeps
it seconds fresh. Anyone checking Dagster in the morning is misled.
- Where: `jobs/spark/pipelines/stream_cdc.py` (per micro-batch) → report an asset event to Dagster
  for each silver table written, with the Iceberg snapshot id and row counts as metadata.
- Done when: Dagster's silver assets show a recent event from the stream, and a verify check
  asserts the latest silver event is under 5 minutes old while the stream runs.

## 4. Auditor persona and audit view
The audit log (`audit.tool_calls`) can only be read by the Postgres owner.
- Where: `infra/postgres/init/03-audit-schema.sql` + `infra/postgres/reconcile.sql` (a read-only
  role on `audit.tool_calls`), Keycloak realm (an `auditor` user), a Grafana dashboard backed by
  that role (who looked up which customer, for which call, outcome, chain status).
- Done when: the auditor can answer "who looked at customer X this week" in Grafana without
  database access, and cannot read any customer data.

## 5. Test that non-admins are refused by Dagster
The Dagster wildcard bug was not caught because `verify.sh` only checks that anonymous users
are refused.
- Where: `scripts/verify.sh`. Sign in as alice through Keycloak (follow the oauth2-proxy redirect
  with a cookie jar) and expect HTTP 403 from `localhost:3002`.
- Done when: re-adding `OAUTH2_PROXY_EMAIL_DOMAINS: "*"` makes `make verify` fail.

## 6. Decide who may open Live Call Assist
carol (analyst) can sign in to the call console. Data stays masked, but the console is for call
handlers.
- Where: `services/platform/src/lakehouse_platform/call_assist/app.py` (check the persona claim at
  sign-in) or a Keycloak client role on `agent-console`.
- Done when: carol gets a clear "not for your role" page; alice and bob are unaffected.

## 7. Prove the SQL workbench in `make verify`
Superset querying as each colleague is proven by the portal tour and a manual API run, not by
`verify`, so CI would not catch a regression (for example a shared connection added by hand).
- Where: `scripts/verify.sh` or a small Playwright check. Sign in as alice and carol, authorize
  the Lakehouse connection, run one query through `/api/v1/sqllab/execute/` each.
- Done when: alice gets masked phones and Meridian only, carol is denied silver, and OPA's
  decision log shows each colleague (never a Superset identity).

## 8. Break-glass for raw payloads
Platform admins see bronze and quarantine metadata, but a record's contents need full PII
clearance ([ADR 10](adr/0010-platform-admins-read-bronze-without-payloads.md)). Triage sometimes
needs the contents.
- Where: OPA (a time-boxed grant keyed on a ticket id), Keycloak (step-up), the audit log.
- Done when: ops_admin can read one payload for a stated reason for a limited time, and the
  access is in the audit chain.

## 9. Prove "Ask the assistant" end to end (take it out of pilot)
Unit tests cover every route with a fake model; `verify` does not ask a question yet.
- Where: `scripts/verify.sh` (CI has no key, so it proves the search fallback) and an eval
  set of typed questions with expected routes for `call-assist-evals --extractor claude`.
- Done when: a question in CI returns the right procedure card, and the eval reports route
  accuracy for Claude.

## 10. Grow the understanding eval set
35 labelled lines is a small sample; 1.00 on it is not proof. Add 100+ messier lines (from
the call simulator's scenarios and real phrasing), keep them out of `label_guide.md`, and
re-check Haiku 4.5 against the floors before changing model.

## 11. Smaller items
- The AI spend estimate lives in memory: it restarts at 0 when `call-assist` restarts, so
  the cap is per process-day. Persist it (Postgres) if the cap must be strict.
- Grafana, Dagster and Marquez opened on `127.0.0.1` send Keycloak a `localhost` callback, so the
  sign-in cookie lands on the other host and can fail. Console and Superset now redirect to
  `localhost`; do the same (or document `localhost` only) for the rest.
- 16,811 transactions in the last 30 days have an empty `merchant` (about £21M, probably transfers).
  Decide whether that is valid source data; if so, label it in silver or gold.
- Memory headroom: checked 26 Sep, no OOM kills or restarts (Grafana 486 MiB of 1 GiB,
  `cdc-connect` 374 of 768 MiB, after peaks of ~980 and ~739). Re-check after a long run;
  raise the caps only if `docker inspect` shows `OOMKilled`.
- Keycloak account console (`/realms/bank/account`) shows "Something went wrong". No persona needs
  it; either fix or disable the account client.
- Prometheus (`:9090`) has no login. It is loopback-only; in a real deployment put it behind the
  same oauth2-proxy pattern as Dagster.
- Marquez UI opens on an empty `default` namespace. Point it at `open-lakehouse` / `s3://lakehouse`.
- Tool latency p95 (about 640 ms) is above the 500 ms target at very low traffic: cold first
  queries after restarts. Consider a warm-up query on gateway start.
- Trino web UI returns 401 (JWT only). Optional: enable its OAuth2 web login for ops_admin.
- Protect `main`: require the `ci` and `e2e` checks before merging. Without required checks,
  `gh pr merge --auto` merges immediately.

## Useful references
- Persona walkthrough page (access matrix, same query as four colleagues, a day in each job):
  private artifact, link held by the repo owner.
- `uv run scripts/portal_tour.py`: signs in to every portal as each persona and saves a screenshot
  and outcome per step to `.tour/results.json`. Re-run after any access change
  (`uv run scripts/portal_tour.py superset` for the workbench only).
- [Runbooks → missing-change](runbooks.md#missing-change): trace a change through bronze,
  quarantine and pending.
