# Scaling to 50,000 colleagues and petabytes

The local stack proves *behaviour*. This document is the *capacity* model: explicit
assumptions, the load they imply, and the architecture changes that load forces. Every
number below is an assumption to validate with real traffic. Change the inputs and the
design choices follow.

## The machine it runs on today (laptop scale)

Everything, platform and tenants, runs on one laptop, so volumes are sized to prove every
path, not to load it. Measured 28 Sep 2026:

| | |
|---|---|
| Mac | Apple M2 Pro, 12 cores (8 performance + 4 efficiency), 16 GB RAM |
| Docker Desktop | 12 GB RAM, 1 GB swap; CPUs: 8 recommended (all 12 starve macOS under load) |
| Stack with everything on | ~9–10 GB of the 12 GB VM |

What that sizing means:

- **One Spark driver per job family.** A driver carries ~450 MB besides its heap, so markets-data
  runs both of its streams as one application (`tenant-markets-data-streams`, 1536 MB).
- **Tenant budget 4608 MB** (`MEMORY_BUDGET_MB`, `tenants/render.py`), 4224 MB in use: the canary,
  markets-data's code server, its streams app and two producers.
- **RustFS 1 GiB.** At 512 MiB it thrashed at its limit once tenant streams committed, and every
  Iceberg commit, Trino query and stream batch waited on it.
- **Fewer commits.** CDC every 20 s (the freshness promise is 60 s); tenant streams every 2
  minutes. Every commit is object-store work.
- **Small volumes.** markets-data streams two Coinbase euro books (~1 trade/s) and ~1 card
  authorisation a second.

The OOM that set these numbers: with a driver per stream and RustFS at 512 MiB, the VM ran out
(swap full) and the kernel killed Trino twice.

Scaling up is settings, not code: Docker memory, `MEMORY_BUDGET_MB`, `memoryMb` in a tenant
file, `CDC_TRIGGER`, and each tenant's own volume knobs (markets-data: README "Volumes").
Longer term, the architecture below is the target, not a bigger laptop. For a
session on a big machine without buying one, a GitHub Codespace (up to 32 cores / 128 GB, billed
per hour) can run the stack; for always-on hosting, a VM.

## Load model

| Input (assumption) | Value |
|---|---|
| Colleagues | 50,000 |
| Contact-centre colleagues | 10,000 (20%) |
| Peak simultaneous live calls | 3,000 (≈60% of the ~5,000 on shift) |
| Average call length | 8 min → 7.5 calls per line per hour |
| Agent tool calls per call | ~6 in the first minute, then ~1/min → ~13 per call |
| Peak-hour analysts / BI users | 500 concurrent, 1 query per 10 s |
| Back-office agents (complaints, due diligence) | ~20 req/s |

**Derived peak:**

- Live-call assist: 3,000 × 7.5 × 13 ≈ **290k tool calls/hour ≈ 80/s**, with bursts at
  call start of **~250/s**. These are point lookups; the p99 budget is < 1 s end to end.
- BI and analytics: ~**50 queries/s**, mostly aggregates, and very cacheable.
- Total: **~300 req/s at peak**, dominated by small, latency-critical lookups.

## What that load forces

**1. Split serving from analytics.** Hundreds of point lookups per second through a
distributed SQL engine works, but it's expensive and p99-hostile. Every query pays for
planning, catalog load, and policy calls. Two options:

- **A. Trino only.** Use a dedicated "agents" cluster behind Trino Gateway, the
  file-system cache, and metadata caching. It's simplest and keeps one enforcement point.
  Good to roughly 100/s per cluster; beyond that, add clusters.
- **B. Serving store (recommended at 50k).** On every gold publish, sync
  `customer_360` into a low-latency store (Postgres read replicas or a key-value store).
  The gateway then becomes the policy enforcement point, evaluating **the same Rego
  policies** in OPA to get the row filter and masks for each colleague. Lakehouse = truth,
  serving store = speed, one policy source.

**2. Workload isolation.** Trino Gateway routes by source to separate clusters (agents,
BI, ad-hoc, batch). Resource groups cap each group, so a bad dashboard can't starve a
live call.

**3. Elasticity.** Trino workers autoscale on queued queries (KEDA). Spark on
Kubernetes uses dynamic allocation and a remote shuffle service (Apache Celeborn), so
executors can come and go safely.

**4. Stateless everything in the request path.** The MCP gateway (`stateless_http=True`),
Polaris (shared signing key) and Trino coordinators behind the gateway all scale
horizontally. State lives only in Postgres (HA), object storage, and the IdP.

**5. Policy without a network hop.** Run OPA as a sidecar per coordinator or gateway pod,
with signed bundles built from IdP groups and catalog tags. Decisions stay local and
sub-millisecond, and an IdP or bundle-server outage never blocks queries (the last good
bundle keeps serving).

**6. Rate limits and quotas.** Move per-colleague limits into a shared store (Redis) or
the API gateway. Add per-team query quotas and chargeback (cost per team, per agent).

## Streaming and Live Call Assist at 50k colleagues

| Component | Local | At scale | Scales on |
|---|---|---|---|
| Kafka | 1 KRaft node | 3 controllers + 3–6 brokers across AZs, RF=3, `min.insync.replicas=2`, SASL/mTLS + ACLs | partitions, throughput |
| Debezium | 1 Connect worker | Connect cluster on Strimzi (tasks rebalanced on failure); one connector per source DB | source WAL volume |
| CDC apply | Spark Structured Streaming, 10 s trigger | Same job on Kubernetes; or Flink when sub-second latency or complex event-time logic is needed | events/s, batch duration |
| Transcripts | 6 partitions | ≥ 64 partitions keyed by call id (3,000 concurrent calls) | concurrent calls |
| call-assist | 1 replica | N replicas, one consumer group; each call pinned to one partition, so to one replica | calls per replica (~200) |
| Console fan-out | SSE from call-assist | Push gateway consuming `contact-centre.assist-events` | connected colleagues |

**Load:** the transcript stream for 3,000 concurrent calls is ~1,500 utterances/min
(about 25/s), which is trivial for Kafka. The real load is the ~13 tool calls per call,
already in the budget above (~80/s steady, ~250/s bursts). Guidance latency is dominated
by tool latency, which is why the serving tier (option B) matters most for the agent.

**Freshness at scale:** micro-batch MERGE into merge-on-read tables commits every 10 s
per table (~8,600 commits/day). That's manageable with snapshot expiry and compaction on
a schedule, which `maintenance.py` and the Dagster schedule already do. For much higher
change rates, partition the MERGE target and run compaction per partition.

## Petabyte-scale table design

| Practice | Why |
|---|---|
| Partition by time, bucket by key (`bucket(N, customer_id)`), sort within files | Scans prune to a few files; point lookups read KBs, not GBs |
| Merge-on-read for frequently updated tables, with scheduled compaction | Cheap writes, bounded read amplification |
| Iceberg v3 (deletion vectors, row lineage) once every engine in the estate supports it | Faster deletes and GDPR erasure, and change tracking without CDC tables |
| Maintenance as policy, driven by table health metrics (file count, delete ratio) | Keeps planning time flat as commits accumulate |
| Snapshot expiry + orphan cleanup (never younger than 3 days) | Bounded metadata and storage; in-flight writes are never touched |
| Catalog federation (Polaris external catalogs) | Other engines (Snowflake, Databricks, SageMaker) read the same tables without copies |

## Sizing sketch (starting point, not a quote)

| Tier | Starting size | Scales on |
|---|---|---|
| Trino "agents" cluster | 1 coordinator + 6 workers (16 vCPU / 64 GB) | queued queries, p95 latency |
| Trino "BI" cluster | 1 coordinator + 10 workers | concurrency, cache hit rate |
| Polaris | 3 replicas (2 vCPU / 4 GB) | request rate |
| MCP gateway | 6 replicas (1 vCPU / 1 GB) | RPS, p99 |
| Serving store (option B) | primary + 2 read replicas | read RPS |
| Postgres (catalog / IdP / audit) | HA pair, PITR | connections, WAL volume |
| Spark | on-demand per job, dynamic allocation | backlog / SLA |
| Object storage | S3 | effectively unlimited |

## SLOs to publish

| SLO | Target |
|---|---|
| Agent tool call availability | 99.9% monthly |
| Agent tool call latency | p95 < 500 ms, p99 < 1.5 s |
| `customer_360` freshness | < 1 h (Dagster freshness policy; rebuilt every 30 min) |
| Silver freshness (CDC) | < 60 s source commit → queryable (measured ~10 s) |
| Live Call Assist guidance | p95 < 2 s from the caller speaking (measured 0.4–1 s) |
| Silver / gold publish success (non-halted) | 99.5% of scheduled runs |
| Unaudited data access | 0 (hard invariant, not an SLO) |
