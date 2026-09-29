# 15. Blueprints, and a stack per use case

**Status:** accepted; being built (slices listed under Decision).

## Context
The platform started as one demo: core banking, CDC, a call-assist AI and a set of consoles, all
started together. [ADR 14](0014-platform-and-tenant-teams-in-separate-repos.md) then made the
platform a base for tenant teams. Two things followed.

First, a tenant does not need most of the platform. `lakehouse-markets-data` needs Kafka, the
catalog, the query engine, policy and Dagster. It does not need core-banking change capture, the
AI demo or the call simulator. Yet `streaming` bundled Kafka with Debezium and the CDC stream
(about 1.5 GiB), so a markets-only run paid for banking.

Second, the laptop is the constraint. On 28 Sep 2026 the full stack on a 16 GB Mac (Docker at
12 GB) ran the VM out of memory and the kernel killed Trino twice. A 10 GB VM with the laptop set
runs stably at about 6.5 GiB of containers, which leaves little room for anything a tenant does
not use.

The purpose of the showcase is to prove that the platform works, that it scales by settings, that
teams federate without stepping on each other, and that it is portable. Running only what a use
case needs serves all four.

## Decision
- **Blueprints are the unit of choice.** A blueprint is a Compose profile: a group of services that
  solves one kind of problem. The **core** (storage, catalog, query engine, policy, identity,
  gateway) always runs. The rest are opt-in: `streaming`, `orchestration`, `corebank`,
  `observability`, `lineage`, `bi`, `ai`. This is how cloud data platforms offer blueprints that a
  team instantiates; here every option is available out of the box and a team picks.
- **Kafka and core banking are separate concerns.** `streaming` is Kafka and the tenant reconciler.
  `corebank` holds the CDC connector and stream, and the Kafka topics for the bank and the assistant
  (`kafka-init`, shared with `ai`). The seed and the batch jobs stay on-demand `jobs`, and Dagster's
  bank schedules follow the blueprint (`COREBANK_ENABLED`, set by `make` from `PROFILES`). A tenant that only needs streams does not start CDC.
- **A use case picks its own stack.** `make up USE=<names>` starts the core, the union of the
  blueprints those use cases declare, and only their own code servers and services:
  - a tenant declares `blueprints:` in `tenants/<team>.yaml` (validated against a fixed list);
  - the platform's own demo is the preset `bank` (core banking, CDC, call assist, consoles),
    which becomes an ordinary tenant when core banking moves out in phase 2 of ADR 14;
  - `make up` without `USE` keeps starting `bank`, so `make demo` and CI keep working;
  - each tenant's workloads get a `tenant-<name>` profile, replacing the single `tenant-code`
    profile, so tenants start independently.
- **`SCALE=laptop|full` stays** as the platform's size, passed to tenants as `PLATFORM_SCALE`. A
  tenant decides what that means for its own workloads (markets-data runs catch-ups on a laptop and
  always-on streams at full scale).
- **The platform tests capabilities with the canary; tenants test themselves.** A stack that
  declares `[streaming, orchestration]` coming up and passing is a platform test, run with the canary
  tenant. Data checks about a team's own streams and products live in that team's repo. Timing how
  long `USE=markets-data` takes to reach queryable gold is a demo target, not a required platform
  CI job.
- **Slices, each verified locally before the next:** (1) split `corebank` out of `streaming` and
  make Dagster's platform code location cope without it (done: markets-only 4.7 GiB against 6.4 GiB, `verify` 24 of 24 without the bank and 57 of 57 with it); (2) `blueprints:` in the tenant schema,
  per-tenant profiles and `USE=`; (3) a time-to-first-data target that prints elapsed time and
  container memory.

## Consequences
- The core stays always on, so a use case can never drop the governance path: identity, policy and
  audit are not optional.
- Dagster's platform schedules and `verify`'s bank sections assume core-banking data. Without `corebank`
  the nightly and gold schedules are declared but stopped (maintenance still runs), and `verify` skips the
  bank sections and checks that the schedules are stopped. One-shot services must be waited on by a
  dependent or `up --wait` fails, which is why `kafka-init` moved into `corebank` and `ai` and the tenant
  reconciler depends on Kafka directly.
- Adding a blueprint is a platform change (compose profile, name in the schema, docs). Adding a
  tenant that only combines existing blueprints is one PR of a YAML file.
- E2E runs more than one shape of stack over time: `bank` (today's), and a canary stack for tenants.
  Keeping PR e2e short means running the second only when tenant or blueprint files change.
- The stack per use case makes the memory budget per use case rather than global, so
  `MEMORY_BUDGET_MB` should be checked against the tenants in a stack, not all tenants at once.
  Until that is built, the single budget stays as a safe upper bound.
