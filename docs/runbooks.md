# Runbooks

Every alert in `infra/prometheus/rules/slo.yml` links to a section here. Each section is
laid out the same way: what it means, what colleagues experience, how to confirm it, and
how to mitigate. Self-healing handles most crashes (see [resilience.md](resilience.md)).
These are for what's left.

## agent-tool-availability
**Means:** agent tool calls return `unavailable` or `error` fast enough to burn the
99.9% monthly budget (a denial is policy working, never counted).
**Colleagues see:** "Customer data is temporarily unavailable", with no wrong data.
**Confirm:** Grafana → *Platform SLOs* → "Tool calls by outcome". Then check
`mcp_circuit_breaker_open` and `up{job=~"trino|polaris|keycloak"}`.
**Mitigate:** find the sick dependency (Trino, the catalog, the IdP, the audit DB). If the
audit DB is down, calls fail **by design**: fix the DB rather than bypass audit. Roll back
the last deploy if the errors started with it.

## agent-tool-latency
**Means:** p99 above 1.5 s for 10 minutes.
**Confirm:** "Latency p95 by tool". Is it a single tool (a query plan) or all of them (the
engine or the catalog)? Check `trino_execution_name_QueryManager_QueuedQueries`.
**Mitigate:** scale the agents' Trino cluster, check file-system cache hit rate, run
`make maintenance` if small files built up (planning time grows with file count).

## circuit-breaker-open
**Means:** the gateway stopped calling Trino after repeated failures. It retries in 30 s.
**Mitigate:** treat it as a Trino outage. The breaker is protecting Trino from a retry
storm; don't disable it.

## cdc-freshness
**Means:** source → silver lag above 60 s for 5 minutes.
**Confirm:** Grafana → *Streaming*: is it input volume (events/s spiking) or batch
duration? `make freshness` measures it end to end.
**Mitigate:** a backlog after an outage is expected and drains on its own. If batch
duration is climbing, compact silver (`make maintenance`) or give the stream more cores.

## cdc-stalled
**Means:** the stream made no progress for 2 minutes. The healthcheck fails and the healer
restarts the stream, which resumes from its checkpoint.
**Confirm:** `docker compose logs cdc-stream`. Repeated restarts usually mean a poison
batch or a catalog or storage outage.
**Mitigate:** fix the dependency. The checkpoint and Kafka retention (7 days) mean
nothing is lost while you do.

## quarantine
**Means:** more than 1% of an entity's changes violate the contract. That's usually an
upstream release.
**Confirm:** `make sql U=ops_admin Q="SELECT entity, reasons, count(*) FROM lakehouse.ops.quarantine GROUP BY 1, 2 ORDER BY 3 DESC"`
**Mitigate:** contact the source owner (see the contract's `team`). Good rows keep
flowing; quarantined rows can be replayed from `bronze.cdc_events` once they're fixed.

## audit-chain
Not an alert: `make verify` reports "hash chain intact" failing with a row number.
**Means:** `audit.verify_chain()` found a row whose hash or predecessor doesn't match, in
seq order. Either the log was changed around its controls, or the row predates the
writer fix (seq now assigned under the chain lock): before it, two tool calls in the same
millisecond could take seq numbers in one order and the lock in the other, and the next
row could chain to the wrong one (a fork).
**Confirm:** as the Postgres owner, look at the rows around the reported seq:
`SELECT seq, occurred_at, tool, purpose, left(prev_hash,10), left(row_hash,10) FROM audit.tool_calls WHERE seq BETWEEN n-2 AND n+3 ORDER BY seq`.
A bug-made fork: rows milliseconds apart, same call, every hash recomputes, and two rows
share a `prev_hash`. Anything else (a hash that doesn't recompute, a missing seq) is
tampering until proven otherwise: escalate to security and keep the database as evidence.
**Mitigate:** a bug-made fork stays in the log (it is append-only); record the finding.
For a demo stack, recreating the `audit` database starts a fresh chain.

## missing-change
Not an alert: a colleague says a change made in core banking isn't in silver.
**Means:** the change never arrived, was quarantined, or is parked waiting for its parent.
**Confirm** as `ops_admin`, with the record's key (here a transaction id), in this order:
1. Did it arrive? `make sql U=ops_admin Q="SELECT entity, op, source_lsn, ingested_at FROM lakehouse.bronze.cdc_events WHERE record_key = 'T…' ORDER BY source_lsn"`.
   No rows: look upstream (Debezium, see [cdc-stalled](#cdc-stalled)).
2. Was it rejected? `make sql U=ops_admin Q="SELECT reasons, quarantined_at FROM lakehouse.ops.quarantine WHERE record_key = 'T…'"`
3. Is it waiting for its parent? `make sql U=ops_admin Q="SELECT entity, first_seen_at FROM lakehouse.ops.cdc_pending WHERE record_key = 'T…'"`

The `payload` column is NULL for platform admins in all three tables: record contents need
full PII clearance ([ADR 10](adr/0010-platform-admins-read-bronze-without-payloads.md)).
**Mitigate:** quarantined: see [quarantine](#quarantine). Pending: it is retried until its
parent arrives, then quarantined after `CDC_ORPHAN_GRACE` (15 min).

## replication-slot
**Means:** the CDC slot has no consumer, so Postgres is retaining WAL. At
`max_slot_wal_keep_size` (2 GB) the slot is invalidated to protect the source.
**Mitigate:** restart `cdc-connect` (the healer also restarts failed tasks). If the slot
was invalidated: re-create the connector, then run a batch backfill (`make pipeline`
with the stream stopped) to close the gap.

## dagster-queue-stuck
**Means:** runs sit in QUEUED and nothing starts (gold empty, schedules ticking but not running).
Dagster runs one job at a time, and a run whose worker died (the stack was stopped mid-run) can stay
STARTED and hold the only slot. The daemon log says "1 runs are currently in progress. Maximum is 1".
`run_monitoring.max_runtime_seconds` (3600, `services/orchestrator/dagster.yaml`) now fails such a run
within an hour, and `make verify` checks the cap and that no run is older than it.
**Mitigate:** in Dagster's Runs page, terminate the old STARTED run (choose to mark it canceled);
the queue then drains in order. Cancel queued duplicates from before the outage if they are stale.

## ai-budget
**Means:** today's estimated model spend is past 80% of `CALL_ASSIST_DAILY_BUDGET_USD`
([ADR 12](adr/0012-ai-call-note-and-a-spend-cap.md)). At 100% the assistant stops calling
the model until midnight UTC: lines are understood by rules and notes use the template.
**Colleagues see:** nothing yet; at the cap, `rules only · Claude not used: daily AI budget
reached` under caller lines and template call notes.
**Confirm:** Grafana → *Live Call Assist* → "AI usage and cost"; `curl -s localhost:8090/config`.
Is it more calls than usual, or more cost per call (prompt cache hit rate falling)?
**Mitigate:** if the cache hit rate dropped, check the service log for "prompt not cached".
Raise the cap only deliberately (set the variable, `docker compose up -d call-assist`).
The estimate restarts at 0 when the service restarts; the provider's console is the bill.

## ai-degraded
**Means:** more than half of caller lines fall back to rules for reasons other than the cap.
**Colleagues see:** guidance still arrives (rules always run), with less recall on unusual
phrasing; each line says why Claude was not used.
**Confirm:** "Model calls by outcome": `auth` (bad or rotated key), `api_error` (often
"credit balance is too low"), `timeout` (latency panel near 2.5 s), `rate_limited`.
**Mitigate:** fix the key or credit; for timeouts, check the provider's status page. No
restart is needed: every line tries the model again.

## assist-lag
**Means:** Live Call Assist is behind the transcript stream, so guidance arrives late.
**Mitigate:** scale `call-assist` replicas (one consumer group; transcripts are keyed by
call, so a call stays on one replica). Check the gateway's latency too: slow tools
slow every card.
