# 11. The SQL workbench queries Trino as the colleague, not as a service

**Status:** accepted

## Context
Colleagues wanted a browser SQL workbench (like Athena or Snowflake worksheets) instead of
the command line. BI tools usually connect with one service account, which would turn every
colleague into that account: row filters, masks and schema access would stop following the
person. Trino impersonation (a service account asking to run "as alice") keeps the person
but gives one secret the power to be anyone.

## Decision
- **Superset SQL Lab** at `localhost:3004`, with Keycloak sign-in only (no local passwords).
  `ops_admin` administers Superset; everyone else gets SQL Lab.
- **Per-colleague OAuth2 to Trino.** The first query asks the colleague to authorize once;
  Superset stores their own Keycloak token (audience `trino`), refreshes it, and sends it
  on every query. Trino authenticates the colleague's JWT and OPA applies their rules.
- **No impersonation grant.** OPA still denies `ImpersonateUser`; there is no Superset
  service account in Trino.
- The Lakehouse connection is created at start-up (`infra/superset/provision.py`), read-only
  (no DML, no uploads). Superset's own permission only lets SQL Lab users pick it.

## Consequences
- The same query gives alice, bob and carol the same answers in Superset as in
  `make sql` or through the agent: one policy, every consumer.
- Tokens are stored encrypted in Superset's metadata database, per colleague.
- Charts and dashboards on this connection also query as the viewer. Superset's result
  cache is off here; if it is turned on, its cache key includes the colleague, so one
  viewer's results are never served to another.
- Sign-in depends on `localhost`; visits via `127.0.0.1` are redirected, since Keycloak
  only accepts the registered address.
