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

## assist-lag
**Means:** Live Call Assist is behind the transcript stream, so guidance arrives late.
**Mitigate:** scale `call-assist` replicas (one consumer group; transcripts are keyed by
call, so a call stays on one replica). Check the gateway's latency too: slow tools
slow every card.
