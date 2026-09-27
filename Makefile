SHELL := /bin/bash
# Profiles to run. Default: everything (core + streaming + ops + tenants). Core only:
# `make up PROFILES=`. Without tenant code servers (ADR 14, memory): `make up PROFILES="streaming ops"`.
PROFILES ?= streaming ops tenants
# `tenants` is a switch for `make up`, not a Compose profile passed on every command.
COMPOSE := docker compose $(addprefix --profile ,$(filter-out tenants,$(PROFILES)))
JOBS := $(COMPOSE) --profile jobs

.DEFAULT_GOAL := help

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

secrets: ## Generate local secrets and TLS certificates (idempotent)
	@scripts/gen-secrets.sh

up: secrets ## Start the platform (idempotent: safe to re-run at any time)
	$(COMPOSE) up -d --build --wait
	$(if $(filter streaming,$(PROFILES)),$(COMPOSE) run --rm tenant-reconcile)
	$(if $(and $(filter tenants,$(PROFILES)),$(filter ops,$(PROFILES))),$(COMPOSE) --profile tenant-code up -d --wait)
	@$(MAKE) --no-print-directory urls

seed: ## Load synthetic core-banking data (no-op if already loaded)
	$(JOBS) run --rm seed-corebank

activity: ## Simulate a day of new activity in the source system
	$(JOBS) run --rm seed-corebank --increment 5000

pipeline: ## Batch path: bronze -> silver (WAP; skipped while the stream owns it) -> gold
	$(JOBS) run --rm spark-job ingest_bronze
	$(JOBS) run --rm spark-job silver
	$(JOBS) run --rm spark-job gold

maintenance: ## Compact, expire snapshots, remove orphans, drop stale WAP branches
	$(JOBS) run --rm spark-job maintenance

demo: up seed pipeline ## Everything from zero to queryable and streaming, then verify
	@scripts/verify.sh

verify: ## End-to-end security, streaming, agent, and governance assertions
	@scripts/verify.sh

call: ## Live Call Assist, headless: make call U=alice S=card_fraud
	@uv run --quiet scripts/live_call.py $(or $(U),alice) $(or $(S),card_fraud)

live-demo: ## Play every demo call as alice (open http://localhost:8090 to watch)
	@for s in card_fraud complaint_chase payment_missing bereavement injection; do \
	  uv run --quiet scripts/live_call.py alice $$s; echo; done

freshness: ## Measure source-commit -> visible-to-a-colleague latency through CDC
	@uv run --quiet scripts/freshness_probe.py

evals: ## Live Call Assist offline evals (understanding + scripted calls); CI gate
	cd services/platform && uv run call-assist-evals

contracts: ## Validate data contracts (ODCS schema + policy-tag agreement)
	@uv run --quiet contracts/check.py

tenants: ## Validate tenant onboarding files (tenants/*.yaml, ADR 14)
	@uv run --quiet tenants/check.py

tenants-apply: ## Reconcile tenant files into Kafka, Polaris and Keycloak (runs on `make up` too)
	$(COMPOSE) run --rm tenant-reconcile

tenant-secret: ## Store a tenant's secret from an env file, never printed: make tenant-secret TENANT=markets-data NAME=shadowtraffic FILE=~/license.env
	@COMPOSE="$(COMPOSE)" uv run --quiet tenants/secret.py "$(TENANT)" "$(NAME)" "$(FILE)"

canary: ## Run the canary tenant's pipeline in its own code server, as its own identity (ADR 14)
	$(COMPOSE) --profile tenant-code exec -T tenant-canary-code dagster asset materialize -m canary_tenant.definitions --select canary_data/people

mem: ## Memory per container against its limit, and the total against Docker's (ADR 14)
	@scripts/mem.sh

tenants-render: ## Regenerate tenant code servers (compose.yaml) and Dagster's workspace.yaml
	@uv run --quiet tenants/render.py

chaos: ## Kill every component in turn; measure safe degradation and time-to-recover
	@scripts/chaos.sh

heal: ## Run the desired-state reconciler in the foreground (Ctrl-C to stop)
	@scripts/healer.sh

test: ## Unit tests: OPA policies + Python services
	docker run --rm -v "$$PWD/infra/opa:/work:ro" openpolicyagent/opa:1.21.0-static test /work/policies /work/data -v
	cd services/platform && uv run pytest -q
	uv run --quiet --no-project --with pytest --with jsonschema --with pyyaml pytest -q tenants contracts/tests

lint: ## Lint Python, check Rego formatting, validate alert rules and dashboards-as-code
	cd services/platform && uvx ruff check src tests
	uvx ruff check --line-length 120 --select E,F,B jobs/spark tenants contracts
	docker run --rm -v "$$PWD/infra/opa:/work:ro" openpolicyagent/opa:1.21.0-static fmt --list --fail /work/policies
	docker run --rm --entrypoint promtool -v "$$PWD/infra/prometheus:/w:ro" prom/prometheus:v3.15.0 check rules /w/rules/slo.yml
	uv run --quiet infra/grafana/build_dashboards.py >/dev/null && git diff --exit-code -- infra/grafana/dashboards
	uv run --quiet tenants/render.py --check
	docker run --rm -v "$$PWD:/mnt" -w /mnt koalaman/shellcheck:v0.11.0 -S warning scripts/*.sh jobs/spark/pipelines/run.sh infra/postgres/init/*.sh

dashboards: ## Regenerate Grafana dashboards from infra/grafana/build_dashboards.py
	uv run --quiet infra/grafana/build_dashboards.py

sql: ## Open a query as a colleague: make sql U=alice Q="SELECT ..."
	@scripts/trino-sql.sh $(U) "$(Q)"

agent: ## Call an MCP tool as a colleague: make agent U=alice T=get_customer_360 A='{...}'
	@uv run --quiet scripts/agent_call.py $(U) $(T) '$(A)'

urls: ## Where everything is (all bound to localhost only)
	@echo "  Live Call Assist console  http://localhost:8090   (sign in: alice / bob / carol)"
	@echo "  Grafana (SSO)             http://localhost:3001"
	@echo "  Dagster (SSO, ops_admin)  http://localhost:3002"
	@echo "  Lineage / Marquez (SSO)   http://localhost:3003"
	@echo "  SQL workbench / Superset (SSO) http://localhost:3004"
	@echo "  Prometheus                http://localhost:9090"
	@echo "  Keycloak                  http://localhost:8280"
	@echo "  Trino (TLS + JWT)         https://localhost:8443"
	@echo "  MCP gateway               http://localhost:8000/mcp"
	@echo "  Demo password: grep DEMO_USER_PASSWORD .env"

down: ## Stop the platform (data is kept)
	$(COMPOSE) --profile jobs --profile tenant-code down

destroy: ## Stop and DELETE all data volumes (asks first)
	@read -p "Delete ALL lakehouse data volumes? [y/N] " a && [[ $$a == y ]] && $(COMPOSE) --profile jobs --profile tenant-code down -v

.PHONY: help secrets up seed activity pipeline maintenance demo verify call live-demo freshness evals contracts tenants tenants-apply tenants-render tenant-secret canary mem chaos heal test lint dashboards sql agent urls down destroy
