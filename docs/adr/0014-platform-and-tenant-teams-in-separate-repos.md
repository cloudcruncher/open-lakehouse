# 14. The platform serves tenant teams that live in their own repos

**Status:** accepted.

## Context
Everything so far lives in this repo: infrastructure, the core-banking pipelines, the MCP
gateway and Live Call Assist. That proves the stack, but it hides the boundary an
enterprise depends on: a platform team runs the lakehouse, and product teams (data
engineering, AI engineering, and people who do both) build on it from their own repos, on
their own release cadence, without editing platform code or asking for hand-made grants.

The next showcase is a real data-engineering project ("Markets & Payments Intelligence":
live market and card-authorisation streams, FX and sanctions reference data, Kappa-style
reprocessing, gold data products, dashboards), then an AI team and a hybrid AI/data
engineer building on its data products. It must be built the way a tenant would build it.

## Decision
- **Four repos, one owner each.**
  - `open-lakehouse` (platform team): infrastructure, catalog, query engine and policy,
    identity, Kafka, the Dagster control plane, and the golden paths below.
  - `lakehouse-markets-data` (data engineering): ingestion, streaming and batch jobs,
    contracts, gold data products and their dashboards.
  - `lakehouse-ai-desk` (AI engineering): an assistant built only on published data
    products, through the MCP gateway and Trino. It never reads bronze or silver.
  - `lakehouse-risk-signals` (hybrid): streaming features, a model scored in the stream,
    and a new data product; the LLM explains flagged cases only.
- **Onboarding is one PR to this repo.** A tenant is declared in `tenants/<team>.yaml`
  (owner, Keycloak group, Kafka topics, Polaris namespaces, code-location image). The
  platform reconciles it into topics, namespaces and grants, a Keycloak group, and a Dagster
  code location. Nothing is granted by hand.
- **Tenants consume published interfaces only:**
  - the Compose networks (`open-lakehouse_data`, `_stream`, `_edge`) as external networks;
  - the `open-lakehouse/spark` image as a base image;
  - a Dagster gRPC code server per tenant, loaded by the platform's `workspace.yaml`;
  - a reusable GitHub workflow that runs the contract/policy agreement check
    ([ADR 8](0008-orchestration-lineage-and-contracts-as-code.md)) against the tenant's
    contracts. A tenant's contracts produce its OPA column tags; tenants do not edit
    `entitlements.json` directly.
- **Kappa for the new domain.** Streams are the single processing path; reprocessing means
  replaying Kafka into versioned tables and switching readers, not a parallel batch job.
  Batch is kept for reference data that really is batch (daily FX, sanctions lists).
- **Tenant workloads are a Compose profile** that can be switched off, because the laptop is
  already close to its memory limit.
- **The platform proves itself with a canary tenant.** Masks, row filters and gateway
  guarantees can't be tested without data, so this repo keeps a tiny synthetic tenant (one
  topic, one table with tagged PII, a few rows) that goes through the real onboarding path.
  Platform e2e becomes: services up, reconcile, canary tenant read as each colleague and
  through the gateway. Checks about a team's data (CDC, medallion layers, WAP, freshness,
  call-assist evals) move to the repo that owns that data.
- **Platform releases are versioned.** The platform tags releases (images, network names,
  the reusable workflow). Tenant repos pin a version, and also run their e2e nightly against
  the platform's `main`, so a breaking platform change is caught within a day without
  slowing either side's hourly releases.
- **Phases.** 0: the onboarding path (registry, reconciler, shared interfaces, reusable
  workflow, tenant profile, canary tenant). 1: `lakehouse-markets-data` onboards as a new
  tenant. 2: core banking moves out to `lakehouse-corebank-data` and Live Call Assist to the
  AI team, strangler-style: the new repo runs green on the platform before the code is
  deleted here. Moving existing workloads last is the real test of the golden path.

## Consequences
- The platform gains a registry and a reconciler that it must test like any other code; a
  broken tenant file must fail CI here, not at runtime.
- Tenant repos depend on this repo's published names (networks, image tag, workflow). Those
  become a versioned interface: renaming one is a breaking change for every tenant.
- Until phase 2, the core-banking pipelines stay here and platform e2e still runs them
  (~10 min on a PR). After phase 2 it should drop to a few minutes, which the hourly
  release target needs.
- `make verify` splits by owner. Its platform checks stay here (against the canary tenant);
  the rest move with their pipelines, so no check is lost in the move.
- Existing guarantees still hold for tenant data: OPA per colleague, masks from contract
  tags, the audit chain for agent tool calls.
