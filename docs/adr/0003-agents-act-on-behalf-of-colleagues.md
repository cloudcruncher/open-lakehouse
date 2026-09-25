# 3. AI agents act on behalf of a colleague, never as a privileged service

**Status:** accepted

## Context
The common shortcut, an agent service account with broad read access, gives every
agent the union of everyone's permissions and makes the audit trail meaningless.

## Decision
The agent passes the colleague's access token (audience `mcp-gateway`). The gateway
verifies it and performs an **RFC 8693 token exchange** at Keycloak for a Trino-audience
token whose subject is still the colleague. Trino authenticates that token, and OPA
applies the colleague's own filters and masks. Every call also carries a mandatory
purpose (call reference) and is written to an append-only, hash-chained audit log
**before** the result returns. If the audit write fails, the call fails.

## Consequences
- An agent can never see more than the human it assists. Permission reviews stay
  human-centric.
- Every access is attributable to (colleague, agent client, purpose, query id).
- Token exchange adds a hop. It's cached per subject token until near expiry.
