# 6. CDC into the lakehouse with Debezium, Kafka and Spark, under a single-writer lease

**Status:** accepted

## Context
A colleague on a live call needs the transaction that happened a minute ago, not
last night's batch. JDBC polling loads the source, misses deletes and re-reads
overlapping rows. Streaming and batch jobs writing the same Iceberg tables would also
fight: concurrent MERGEs fail each other's commit validation, and WAP fast-forward
needs `main` not to move underneath it.

## Decision
- **Capture:** Debezium (Postgres `pgoutput`, an explicit publication, and a replication
  slot with heartbeats) publishes change events to Kafka. It never gets write access to
  the source, and its password never reaches the Connect config topic
  (`${env:...}` config provider).
- **Apply:** Spark Structured Streaming micro-batches (10 s) append raw events to
  `bronze.cdc_events` (idempotent, keyed by batch id in the snapshot summary). They then
  apply **the same row contract as batch silver**, quarantine violations, and MERGE
  inserts, updates and deletes into silver (newer wins, so replays are harmless).
- **Ownership:** the stream renews a writer lease on the `silver` namespace (a catalog
  property, not a table commit). Batch silver refuses to run while the lease is live.
  The nightly Dagster schedule checks the lease and leaves silver out of its request.
- **Backfill vs tail:** the slot starts before the backfill (`snapshot.mode=no_data`),
  so the JDBC backfill and the stream overlap rather than leave a gap. Overlapping
  writes are idempotent.
- **Guard rails:** `max_slot_wal_keep_size=2GB`, so a dead consumer can't fill the source's
  disk; alerts on inactive slots, stalled progress, freshness over 60 s, and
  quarantine rate over 1%.

## Consequences
- Measured source-commit → visible-to-a-colleague: **~10 s**, through every
  governance layer (`make freshness`).
- A streaming outage makes data staler, never unavailable or wrong: reads continue,
  and changes queue in WAL and Kafka then catch up. Chaos tests prove no loss.
- A stream can't "halt the world" the way batch WAP does. Row-level contracts still
  apply, and a high quarantine rate alerts instead of stopping freshness for everyone.
- Production: Kafka RF=3 with SASL/mTLS and ACLs, Debezium on Strimzi, and the Spark
  checkpoint on durable storage. Flink is the alternative when sub-second latency or
  complex event-time logic is required.
