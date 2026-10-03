SHELL := /bin/bash
# Blueprints (docs/scale.md): the governed core always runs; pick the rest. SCALE picks a default
# set and tells tenants the platform's size (PLATFORM_SCALE). A laptop (SCALE=laptop, the
# default) runs core + streaming + corebank + orchestration + tenants; SCALE=full adds every console
# and the AI demo. Any set: `make up PROFILES="streaming orchestration bi"`; core only: `PROFILES=`.
# Tenants that don't need core banking leave `corebank` out: `make up PROFILES="streaming orchestration tenants"`.
SCALE ?= laptop
BLUEPRINTS_laptop := streaming corebank orchestration tenants
BLUEPRINTS_full := streaming corebank orchestration tenants observability lineage bi ai
# USE picks a stack per use case (ADR 15): the union of the blueprints each named tenant declares
# (tenants/<name>.yaml `blueprints:`) or the preset `bank`, e.g. `make up USE=markets-data`.
# Only the named tenants' own code servers and services start. PROFILES on the command line wins.
ifneq ($(USE),)
USE_PROFILES := $(shell scripts/use.py $(USE))
ifeq ($(USE_PROFILES),)
$(error USE=$(USE): see the message above)
endif
PROFILES ?= $(USE_PROFILES)
TENANTS := $(shell scripts/use.py --tenants $(USE))
endif
PROFILES ?= $(BLUEPRINTS_$(SCALE))
export PLATFORM_SCALE := $(SCALE)
export COREBANK_ENABLED := $(if $(filter corebank,$(PROFILES)),1,0)
ifeq ($(filter laptop full,$(SCALE)),)
$(error SCALE=$(SCALE): expected laptop or full)
endif
ifneq ($(filter ai observability corebank,$(PROFILES)),)
ifeq ($(filter streaming,$(PROFILES)),)
$(error the ai, observability and corebank blueprints read Kafka: add streaming to PROFILES)
endif
endif
# `tenants` is a switch for `make up`, not a Compose profile passed on every command.
COMPOSE := docker compose $(addprefix --profile ,$(filter-out tenants,$(PROFILES)))
# Every service whatever was picked: for stopping, and for scripts that only look.
ALL := docker compose --profile '*'
JOBS := $(COMPOSE) --profile jobs

.DEFAULT_GOAL := help

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

secrets: ## Generate local secrets and TLS certificates (idempotent)
	@scripts/gen-secrets.sh

up: secrets ## Start the platform (idempotent: safe to re-run at any time)
	$(COMPOSE) up -d --build --wait
	$(if $(filter streaming,$(PROFILES)),$(COMPOSE) run --rm tenant-reconcile)
	$(if $(and $(filter tenants,$(PROFILES)),$(filter orchestration,$(PROFILES))),$(COMPOSE) $(if $(USE),$(addprefix --profile tenant-,$(TENANTS)),--profile tenant-code) up -d --wait)
	$(if $(filter bi,$(PROFILES)),@scripts/catalog-sync.sh)
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

catalog-sync: ## Copy each deployed tenant's contracts out of its image for the data product catalog
	@scripts/catalog-sync.sh

tenant-secret: ## Store a tenant's secret from an env file, never printed: make tenant-secret TENANT=markets-data NAME=shadowtraffic FILE=~/license.env
	@COMPOSE="$(COMPOSE)" uv run --quiet tenants/secret.py "$(TENANT)" "$(NAME)" "$(FILE)"

tenant-restart: ## Restart a tenant's service or its code server (tenants have no Docker socket): make tenant-restart T=markets-data S=coinbase-feed (S=code for the code server)
	@COMPOSE="$(COMPOSE)" uv run --quiet tenants/restart.py "$(T)" "$(S)"

canary: ## Run the canary tenant's pipeline in its own code server, as its own identity (ADR 14)
	$(COMPOSE) --profile tenant-code exec -T tenant-canary-code dagster asset materialize -m canary_tenant.definitions --select canary_data/people

canary-self-service: ## The canary creates, renames and drops tables and views in its own namespace
	$(COMPOSE) --profile tenant-code exec -T tenant-canary-code dagster asset materialize -m canary_tenant.definitions --select canary_data/self_service

first-data: ## Time a tenant's stack to queryable gold, then memory: make first-data T=markets-data (ADR 15)
	@[ -n "$(T)" ] || { echo 'usage: make first-data T=markets-data'; exit 2; }
	@scripts/first-data.py $(T)

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
	@echo "  Data products (catalog)   http://localhost:3005"
	@echo "  Prometheus                http://localhost:9090"
	@echo "  Keycloak                  http://localhost:8280"
	@echo "  Trino (TLS + JWT)         https://localhost:8443"
	@echo "  MCP gateway               http://localhost:8000/mcp"
	@echo "  Demo password: grep DEMO_USER_PASSWORD .env"

down: ## Stop the whole platform (data is kept)
	$(ALL) down

# The reverse of `make up PROFILES=...` for one blueprint: its services only, the rest keeps running.
# The core has no profile, so it can never be named here. `tenants` is the tenant-code profile.
stop: ## Stop and remove only these blueprints, e.g. make stop BLUEPRINTS="bi lineage" (data is kept)
	@[ -n "$(BLUEPRINTS)" ] || { echo 'usage: make stop BLUEPRINTS="bi lineage"'; exit 2; }
	@svcs=$$(scripts/blueprint-services.py $(BLUEPRINTS)) || exit $$?; \
	 echo "stopping: $$(echo $$svcs)"; $(ALL) rm -sf $$svcs

destroy: ## Stop and DELETE all data volumes (asks first)
	@read -p "Delete ALL lakehouse data volumes? [y/N] " a && [[ $$a == y ]] && $(ALL) down -v

.PHONY: help secrets up seed activity pipeline maintenance demo verify call live-demo freshness evals contracts tenants tenants-apply tenants-render tenant-secret tenant-restart catalog-sync canary canary-self-service mem chaos heal test lint dashboards sql agent urls down stop destroy
