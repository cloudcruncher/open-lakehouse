# Resilience: fail safe, recover on your own

The target is not "never fails". Every component *will* fail. The target is:

1. **Fail safe.** Never return wrong data, and never return data without policy or audit.
2. **Fail fast.** A colleague on a live call gets an honest "unavailable" in seconds,
   not silence.
3. **Recover without humans.** Reconciliation brings components back and clients retry.
4. **Contain the blast radius.** One sick dependency must not take down everything else.

## Mechanisms

| Concern | Mechanism | Where |
|---|---|---|
| Crash recovery | Desired-state reconciliation: missing or exited service → start; unhealthy → restart | `scripts/healer.sh` (local); kubelet probes + controllers (prod) |
| Startup ordering | Healthchecks + `depends_on: service_healthy / completed_successfully` | `compose.yaml` |
| Safe re-runs | Every init job and pipeline is idempotent (declarative reconcile, MERGE on keys) | `bootstrap/polaris.py`, `silver.py` |
| Config drift | Polaris reconciler diffs actual vs desired storage config and fixes it | `ensure_catalog()` |
| Credential loss | Engine credentials are validated each run and rotated if invalid | `ensure_principal()` |
| Transient errors | Bounded retries with backoff (pipelines: 3 attempts; gateway: 1 retry) | `run.sh`, `data.py` |
| Bad data | WAP + contract + quarantine; halt above 1%; halts are never auto-retried | `common.py`, `silver.py` |
| Rollback | Iceberg snapshots: `CALL lakehouse.system.rollback_to_snapshot(...)` | any table |
| Slow dependencies | 8 s tool deadline; fail-fast S3 client (2 s connect, 2 retries) | `server.py`, `lakehouse.properties` |
| Sick dependencies | Circuit breaker: 5 failures → open 30 s → half-open trial | `resilience.py` |
| Noisy neighbours | Per-colleague rate limit; per-query memory and time caps in Trino | `resilience.py`, `config.properties` |
| Security under failure | OPA down → Trino denies everything; audit DB down → gateway refuses | verified by `make chaos` |
| Log exhaustion | json-file rotation (10 MB × 3) on every container | `compose.yaml` |
| Table rot | Compaction, delete-file rewrite, manifest rewrite, snapshot expiry, orphan cleanup | `maintenance.py` |
| Streaming crash | Spark checkpoint + idempotent bronze (batch id in snapshot summary) + idempotent MERGE | `stream_cdc.py` |
| Stuck stream | Liveness = progress (healthcheck fails if no progress for 120 s), so the healer restarts it | `healthcheck_stream.py` |
| Failed CDC task | Healer restarts failed connector tasks; connector config reconciled on every `up` | `healer.sh`, `bootstrap/cdc.py` |
| Source protection | `max_slot_wal_keep_size=2GB`; inactive-slot alert | `compose.yaml`, `slo.yml` |
| Two writers, one table | Silver writer lease (catalog property); batch and scheduler defer to it | `common.py`, `definitions.py` |
| Orchestrated retries | Exponential backoff + jitter; data-quality halts never retried | `definitions.py` |

## Chaos results (latest run)

```
component    | behaviour during outage                                                              |  TTR
-------------+--------------------------------------------------------------------------------------+------
opa          | failed closed: Query failed; the data platform team has been notified                 |   12s
trino        | failed closed: Customer data is temporarily unavailable                               |   17s
polaris      | failed closed: Query failed; the data platform team has been notified                 |    8s
keycloak     | sign-in unavailable (no new tokens; nothing served unauthenticated)                   |   27s
postgres     | sign-in unavailable (no new tokens; nothing served unauthenticated)                   |    4s
rustfs       | failed closed: Customer data is temporarily unavailable                               |   12s
mcp-gateway  | gateway unreachable (agent told: no data, retry)                                      |    7s
kafka        | reads still served; freshness paused; change made during outage arrived (no loss)     |   30s
cdc-connect  | reads still served; freshness paused; change made during outage arrived (no loss)     |   29s
cdc-stream   | reads still served; freshness paused; change made during outage arrived (no loss)     |   25s
call-assist  | console unavailable; data platform unaffected                                         |    4s

Audit chain intact after chaos: yes
```

Methodology: each component is `kill -9`'d.
For the streaming tier, the journey differs: a transaction is committed to the source
*during* the outage, and "recovered" means it arrived in silver, which proves no loss.
 The degraded behaviour is observed with the
healer **paused**, so nothing masks the failure mode. Then the healer starts, and time to
recover (TTR) is measured until the real user journey (an agent fetching a customer as
alice) succeeds again.

## What chaos testing caught (and why it matters)

**Finding 1: a false "served during OPA outage".** The first run reported that queries
still succeeded with OPA dead. That would be a critical fail-open bug. Investigation:
the healer restarted OPA (sub-second startup) before the probe's query arrived. A direct
test with the healer paused proved Trino fails closed. The fix was to the *test
methodology*: observe the outage with the healer paused. Lesson: a chaos test that can't
see the failure is worse than none.

**Finding 2: a real hang on storage outage.** With object storage down, the tool call
hung for ~30 s. Trino's S3 client retried with backoff, and the gateway had no overall
deadline. No wrong data was returned, but on a live call 30 s of silence *is* the failure.
The fix was layered deadlines: a fail-fast S3 client in the serving engine
(`s3.socket-connect-timeout=2s`, `s3.max-error-retries=2`) plus an 8 s gateway deadline
that counts toward the circuit breaker. Result: fails in seconds with an honest
message, and storage TTR fell from 32 s to 9 s.

## Production targets (the shape this maps to)

| Component | Local | Production | RPO | RTO |
|---|---|---|---|---|
| Object storage | 1× RustFS | S3 (11 nines durability), versioning + cross-region replication | 0 in-region; minutes cross-region | minutes |
| Catalog | 1× Polaris | ≥3 replicas across AZs, shared signing key, PodDisruptionBudget | = metadata DB | seconds (other replicas serve) |
| Metadata DB | 1× Postgres | HA (sync replica), PITR backups | ~0 | < 1 min failover |
| Query engine | 1× Trino | Trino Gateway → several clusters; fault-tolerant execution for batch | n/a (stateless) | seconds (route to another cluster) |
| Policy | 1× OPA | OPA sidecar per coordinator + signed bundles (no network hop) | n/a | n/a (local) |
| IdP | 1× Keycloak | Clustered, or corporate IdP (Entra ID) | per IdP | per IdP |
| Gateway | 1× | N stateless replicas behind a load balancer (`stateless_http=True`) | n/a | seconds |

**Finding 3: an idle stream silently lost ownership of silver.** The lease was renewed
inside the micro-batch function, which only runs when there's data. With the source
quiet for 5 minutes, the lease expired, and the first orchestrated backfill ran batch
silver *alongside* the live stream: two writers on one table. Dagster caught it, because
its run reported silver materializations that shouldn't have happened. The fix was to
renew on a clock in the stream's driver loop. Lesson: liveness signals must not depend
on traffic.

**Finding 4: a silent fallback hid every data-quality check from the orchestrator.**
The Pipes integration imported a helper that doesn't exist, and a defensive
`except ImportError: run without Pipes` swallowed it. Jobs succeeded but reported nothing;
Dagster then failed the run for missing checks. The fix: detect orchestration from its
environment variable and let a missing library fail loudly. Lesson: a fallback that
turns "broken" into "quietly degraded" is a bug factory.

**Finding 5: a transitive upgrade broke orchestration state.** SQLAlchemy 2.1 made
psycopg 3 the default Postgres driver. Dagster's storage issues `NOTIFY` with a bound
parameter, which only psycopg2's client-side binding accepts. Pinned `sqlalchemy<2.1`
with the reason in the requirements file. Hash-locked requirements made the change
visible and reversible. The weekly e2e run exists to catch the next one.
