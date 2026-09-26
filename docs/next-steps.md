# Next steps

State on 26 Sep 2026, after a walkthrough of every portal as each persona. Fixed that day (PR #5):
Dagster open to any colleague, Grafana memory and SSO-only login, the nightly silver lease race,
SLO panels showing "No data", and the quarantine-rate axis. `make verify`: 54/54.

Ordered by value. Each item says where to start and how to know it is done.

## 1. Confirm the nightly lease fix in real use
The fix is covered by a verify check, but the schedule has not yet run with it.
- Where: Dagster → Automation → `nightly_refresh` (02:00 UTC).
- Done when: the next scheduled run is green. If the stream held the lease, the silver step's
  log says "silver skipped" and gold is skipped for that run (gold_refresh rebuilds it).

## 2. Show streaming writes in Dagster
Dagster only sees batch writes, so silver shows "Failed, 13 hours ago" while the CDC stream keeps
it seconds fresh. Anyone checking Dagster in the morning is misled.
- Where: `jobs/spark/pipelines/stream_cdc.py` (per micro-batch) → report an asset event to Dagster
  for each silver table written, with the Iceberg snapshot id and row counts as metadata.
- Done when: Dagster's silver assets show a recent event from the stream, and a verify check
  asserts the latest silver event is under 5 minutes old while the stream runs.

## 3. Auditor persona and audit view
The audit log (`audit.tool_calls`) can only be read by the Postgres owner.
- Where: `infra/postgres/init/03-audit-schema.sql` + `infra/postgres/reconcile.sql` (a read-only
  role on `audit.tool_calls`), Keycloak realm (an `auditor` user), a Grafana dashboard backed by
  that role (who looked up which customer, for which call, outcome, chain status).
- Done when: the auditor can answer "who looked at customer X this week" in Grafana without
  database access, and cannot read any customer data.

## 4. Test that non-admins are refused by Dagster
The Dagster wildcard bug was not caught because `verify.sh` only checks that anonymous users
are refused.
- Where: `scripts/verify.sh`. Sign in as alice through Keycloak (follow the oauth2-proxy redirect
  with a cookie jar) and expect HTTP 403 from `localhost:3002`.
- Done when: re-adding `OAUTH2_PROXY_EMAIL_DOMAINS: "*"` makes `make verify` fail.

## 5. Decide who may open Live Call Assist
carol (analyst) can sign in to the call console. Data stays masked, but the console is for call
handlers.
- Where: `services/platform/src/lakehouse_platform/call_assist/app.py` (check the persona claim at
  sign-in) or a Keycloak client role on `agent-console`.
- Done when: carol gets a clear "not for your role" page; alice and bob are unaffected.

## 6. Smaller items
- Keycloak account console (`/realms/bank/account`) shows "Something went wrong". No persona needs
  it; either fix or disable the account client.
- Prometheus (`:9090`) has no login. It is loopback-only; in a real deployment put it behind the
  same oauth2-proxy pattern as Dagster.
- Marquez UI opens on an empty `default` namespace. Point it at `open-lakehouse` / `s3://lakehouse`.
- Tool latency p95 (about 640 ms) is above the 500 ms target at very low traffic: cold first
  queries after restarts. Consider a warm-up query on gateway start.
- Trino web UI returns 401 (JWT only). Optional: enable its OAuth2 web login for ops_admin.

## Useful references
- Persona walkthrough page (access matrix, same query as four colleagues, a day in each job):
  private artifact, link held by the repo owner.
- `uv run scripts/portal_tour.py`: signs in to every portal as each persona and saves a screenshot
  and outcome per step to `.tour/results.json`. Re-run after any access change.
