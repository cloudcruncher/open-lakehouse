# 16. An industry-standard toolchain: Airflow, dbt, DuckDB, Flink and a metadata catalog

**Status:** proposed (14 Oct 2026); built in the slices listed under Decision, each its own PR.

## Context
The platform proves four things: it works, it scales by settings, teams federate, it is portable.
A fifth matters for a showcase: **the tools are the ones employers ask for**, with open-source
equivalents of the managed services, because the fundamentals are the same.

ITJobsWatch, permanent Data Engineer ads in London, 6 months to 14 Oct 2026 (631 ads): Python and
SQL 41% each, Azure 33%, data modelling 29%, ETL 27%, data quality 26%, CI/CD 23%, AWS 19%,
Databricks 18%, Azure Data Factory 13%, data governance 13%, Power BI 12%, Spark 11%. Airflow
appears in ads next to dbt 53% of the time (dbt in 157 London ads, median £85k). A typical
financial-services spec adds Iceberg, Snowflake, Glue/Lake Formation/EMR/Athena/EventBridge/Step
Functions, and a metadata tool (DataHub, Amundsen, Atlas, OpenDataDiscovery or Marquez).
Counts fell across the board year on year, so these are shares and rankings, not growth claims.

Gaps against that picture: the orchestrator is Dagster (Airflow is what the market lists, and
Prefect announced in July 2026 it would acquire Dagster); gold is hand-written Spark with no
transformation framework (dbt); no embedded analytical engine (DuckDB); streaming is Spark only
(Flink); the only metadata tool is Marquez; dashboards are thin; there is no AWS mapping.

Already in line with the market: Spark, Iceberg on a REST catalog (Polaris, the Snowflake-born
catalog), Trino, Postgres, Kafka and Debezium, OpenLineage, Superset, OPA for the Lake Formation
role, contracts and quality gates.

## Decision
Add or swap, as opt-in blueprints ([ADR 15](0015-blueprints-and-a-stack-per-use-case.md)), so the
laptop profile stays inside its memory budget. Order matters; each slice is one PR the repo owner
checks before the next.

1. **Airflow 3 replaces Dagster** (supersedes the Dagster part of
   [ADR 8](0008-orchestration-lineage-and-contracts-as-code.md); OpenLineage, Marquez and ODCS
   contracts stay). Same job: assets and schedules for the bank and tenants, the writer-lease
   and drain rules, retries, SLOs. Asset-aware scheduling uses Airflow Assets; lineage uses the
   OpenLineage provider; tenant code servers become Airflow DAG bundles. Sequence: run beside
   Dagster, port bank pipelines, port tenants, move `verify`, then delete Dagster. Memory is
   roughly neutral (api-server, scheduler, dag-processor replace webserver, daemon, code servers).
2. **dbt** builds gold: `dbt-trino` against the Iceberg tables, run by Airflow (Cosmos). dbt
   tests complement, and do not replace, the write-audit-publish checks and ODCS contracts;
   column tags still come from the contracts.
3. **DuckDB** as the fast, free engine: `dbt-duckdb` over Parquet fixtures for the tenant CI
   target (a release an hour), and a fourth engine in `verify` that reads the same Iceberg tables
   through Polaris. "Spark, Trino, DuckDB and Flink read one table" is the portability proof, and
   the Snowflake interoperability story (it reads Iceberg through a Polaris-style catalog).
4. **Flink** in a `flink` blueprint: one real job (card-authorisation velocity windows from
   `card-auths`, written to Iceberg) next to the Spark Kappa stream, to compare the two.
5. **Dashboards as code**: Superset dashboards over gold, imported from files the way Grafana
   dashboards are generated, with a `make lint` drift check; Grafana stays for operations.
6. **Metadata catalog**: DataHub in a `catalog` blueprint for the full profile only (it needs
   Elasticsearch and more memory than the laptop set has), ingesting Trino, Polaris, Airflow,
   dbt and OpenLineage. Marquez stays as the lightweight lineage option.
7. **AWS mapping and portability**: a table of service to local equivalent (Lake Formation to
   OPA plus Polaris, Glue catalog to Polaris, EMR to Spark, Athena to Trino, EventBridge and
   Step Functions to Kafka and Airflow, S3 to RustFS), with LocalStack or Terraform only where
   it runs without a paid licence (to be checked when we get there), and the Terraform gap noted
   in memory closed for the pieces we deploy.

## Consequences
- Dagster code, `dagster.yaml` gotchas and the Dagster Pipes reporting are retired; the Pipes
  contract (rows, quarantine counts, snapshot id, WAP results) moves to Airflow task outputs and
  OpenLineage facets. This is the largest cost and the reason for the staged order.
- Memory: Airflow about neutral, Flink about 1.5 GiB when on, DataHub several GiB (full profile
  only), dbt and DuckDB none at rest. The 11.7 GiB budget and `make lint` memory check still apply.
- `make verify` grows checks per slice and must stay green at every step; CI stays fast (ADR 14).
- The reference list above can go stale; re-run the job search before adding more.
