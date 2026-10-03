#!/usr/bin/env bash
# End-to-end verification: every security and correctness claim in the README, as
# an executable assertion against the running platform. Exit code = failure count.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
DC=(docker compose --profile '*')   # every blueprint: checks skip what isn't running
running() { [[ -n "$("${DC[@]}" ps -q --status running "$1" 2>/dev/null)" ]]; }

PASS=0; FAIL=0
ok()   { PASS=$((PASS + 1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { FAIL=$((FAIL + 1)); printf '  \033[31mFAIL\033[0m %s\n       %s\n' "$1" "${2:-}"; }
sql()  { scripts/trino-sql.sh "$1" "$2" 2>&1; }
last() { tail -1 <<<"$1"; }
agent() { uv run --quiet scripts/agent_call.py "$@" 2>&1; }
expect_contains() { if grep -q -- "$3" <<<"$2"; then ok "$1"; else bad "$1" "got: $(head -c 200 <<<"$2")"; fi; }

echo "▸ Platform health"
for svc in postgres rustfs polaris keycloak trino mcp-gateway; do
  st=$("${DC[@]}" ps "$svc" --format '{{.Status}}')
  [[ "$st" == *"(healthy)"* ]] && ok "$svc healthy" || bad "$svc healthy" "$st"
done

echo "▸ Policy and unit tests"
out=$(docker run --rm -v "$PWD/infra/opa:/work:ro" openpolicyagent/opa:1.21.0-static test /work/policies /work/data 2>&1)
expect_contains "OPA policy unit tests" "$out" "PASS:"
out=$(cd services/platform && uv run --quiet pytest -q 2>&1)
expect_contains "gateway unit tests" "$out" "passed"

# Everything from here to the canary block reads core-banking data (customers, transactions, audit
# of agent lookups), so it runs only with the corebank blueprint. A stack that only runs tenants
# (ADR 15) still gets platform health, tenant onboarding, the canary and orchestration checks.
BANK=false; running cdc-connect && BANK=true
if $BANK; then
echo "▸ Row-level security (brand scoping)"
q="SELECT array_join(array_sort(array_agg(DISTINCT brand)), ',') FROM lakehouse.gold.customer_360"
expect_contains "alice (contact centre) sees Meridian only" "$(last "$(sql alice "$q")")" "^Meridian$"
expect_contains "bob (complaints) sees Meridian + Northgate" "$(last "$(sql bob "$q")")" "^Meridian,Northgate$"
expect_contains "carol (analyst) sees all brands" "$(last "$(sql carol "$q")")" "^Isle,Meridian,Northgate$"

echo "▸ Column masking (PII by persona)"
q="SELECT phone, email FROM lakehouse.gold.customer_360 WHERE customer_id = 'C0000052'"
expect_contains "alice: phone partially masked" "$(last "$(sql alice "$q")")" '^\*\*\*\*\*\*\*[0-9]\{4\} | .\*\*\*@'
expect_contains "bob: phone in clear" "$(last "$(sql bob "$q")")" "^07[0-9]\{9\} |"
expect_contains "carol: contact details nulled" "$(last "$(sql carol "$q")")" "^ | $"
q="SELECT count(*) FROM lakehouse.gold.customer_360 WHERE vulnerability_flag"
expect_contains "carol cannot infer vulnerability by filtering on masked column" "$(last "$(sql carol "$q")")" "^0$"

echo "▸ Least privilege (defence in depth)"
expect_contains "carol denied silver (OPA)" "$(sql carol "SELECT 1 FROM lakehouse.silver.customers LIMIT 1")" "PERMISSION_DENIED"
expect_contains "colleagues denied bronze (OPA)" "$(sql bob "SELECT 1 FROM lakehouse.bronze.cdc_events LIMIT 1")" "PERMISSION_DENIED"
q="SELECT count(*) FROM lakehouse.bronze.cdc_events WHERE payload IS NOT NULL"
expect_contains "admin reads bronze, raw payloads always NULL" "$(last "$(sql ops_admin "$q")")" "^0$"
expect_contains "writes via Trino denied" "$(sql alice "DELETE FROM lakehouse.gold.customer_360")" "PERMISSION_DENIED"

echo "▸ Agent gateway (MCP, on-behalf-of)"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST localhost:8000/mcp -H 'content-type: application/json' -d '{}')
[[ "$code" == 401 ]] && ok "unauthenticated MCP call rejected (401)" || bad "unauthenticated MCP call rejected" "HTTP $code"
out=$(agent alice get_customer_360 '{"call_reference":"VERIFY-1","customer_id":"C0000052"}')
expect_contains "alice's agent gets customer 360" "$out" '"found": true'
expect_contains "agent inherits alice's masks" "$out" '"phone": "\*\*\*\*\*\*\*'
expect_contains "customer free text isolated as untrusted" "$out" "customer_authored_text"
expect_contains "answer pinned to an Iceberg snapshot (time-travel reproducible)" "$out" '"pinned_to_snapshot": true'
expect_contains "answer carries OPA's decision for alice" "$out" "brand IN ('Meridian')"
expect_contains "answer linked to its audit row" "$out" '"seq": [0-9][0-9]*'
out=$(agent alice get_customer_360 '{"call_reference":"VERIFY-2","customer_id":"C0000002"}')
expect_contains "alice's agent cannot see another brand's customer" "$out" '"found": false'
out=$(agent carol get_recent_transactions '{"call_reference":"VERIFY-3","customer_id":"C0000052"}')
expect_contains "carol's agent denied transactions" "$out" "Access denied"
out=$(agent alice get_customer_360 "{\"call_reference\":\"VERIFY-4\",\"customer_id\":\"C1' OR '1'='1\"}")
expect_contains "injection rejected before any SQL" "$out" "string_pattern_mismatch"
out=$(agent alice get_customer_360 '{"customer_id":"C0000052"}')
expect_contains "purpose (call reference) is mandatory" "$out" "call_reference"

echo "▸ Audit trail"
psql_audit() { "${DC[@]}" exec -T postgres psql -U postgres -d audit -tAc "$1" 2>&1; }
expect_contains "denied calls are audited too" "$(psql_audit "SELECT count(*) > 0 FROM audit.tool_calls WHERE purpose = 'VERIFY-3' AND outcome = 'denied'")" "^t$"
expect_contains "hash chain intact" "$(psql_audit "SELECT coalesce(audit.verify_chain()::text, 'intact')")" "^intact$"
expect_contains "audit log is append-only" "$(psql_audit "DELETE FROM audit.tool_calls WHERE seq = 1")" "append-only"

echo "▸ Data quality and freshness"
expect_contains "no contract-violating rows reached silver" \
  "$(last "$(sql ops_admin "SELECT count_if(currency NOT IN ('GBP','EUR','USD') OR txn_ts > current_timestamp + INTERVAL '1' HOUR) FROM lakehouse.silver.transactions")")" "^0$"
expect_contains "gold row count equals silver customers" \
  "$(last "$(sql ops_admin "SELECT (SELECT count(*) FROM lakehouse.gold.customer_360) = (SELECT count(*) FROM lakehouse.silver.customers)")")" "^True$"
expect_contains "gold refreshed within 24h" \
  "$(last "$(sql ops_admin "SELECT max(refreshed_at) > current_timestamp - INTERVAL '1' DAY FROM lakehouse.gold.customer_360")")" "^True$"

fi

# The canary tenant (ADR 14) goes through the real path: its code server writes as its own
# identity, then each colleague reads it through Trino + OPA. Runs before the contracts check,
# which compares its live table with contracts/canary.odcs.yaml.
if [[ -n "$("${DC[@]}" --profile tenant-code ps -q --status running tenant-canary-code 2>/dev/null)" ]]; then
  echo "▸ Canary tenant (ADR 14)"
  expect_contains "canary code server writes canary_data.people as its own identity" \
    "$(make --no-print-directory canary 2>&1)" "RUN_SUCCESS"
  expect_contains "canary tenant creates, renames and drops its own tables and views (no platform help)" \
    "$(make --no-print-directory canary-self-service 2>&1)" "RUN_SUCCESS"
  q="SELECT coalesce(email, 'NULL') || ' ' || name FROM lakehouse.canary_data.people WHERE person_id = 1"
  expect_contains "canary: bob (full PII) sees the email" "$(last "$(sql bob "$q")")" "^ada@canary.example Ada Canary$"
  expect_contains "canary: alice (partial PII) sees it masked" "$(last "$(sql alice "$q")")" '^a\*\*\*@canary.example Ada Canary$'
  expect_contains "canary: carol (no PII) gets NULL email, initial only" "$(last "$(sql carol "$q")")" "^NULL A\.$"
  expect_contains "canary: no brand filter on tenant data (5 rows for alice)" \
    "$(last "$(sql alice "SELECT count(*) FROM lakehouse.canary_data.people")")" "^5$"
fi

# --live compares every contract with its running table, including core banking's.
if $BANK; then
  echo "▸ Data contracts (governance as code)"
  expect_contains "contracts valid (ODCS), policy tags agree with OPA, live tables match" \
    "$(uv run --quiet contracts/check.py --live 2>&1)" ": OK$"
fi

if running kafka; then
  echo "▸ Tenant onboarding (ADR 14)"
  topics=$("${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic markets.reference.fx-rates 2>&1)
  expect_contains "tenant topic created with its declared policy (compacted FX rates)" "$topics" "cleanup.policy=compact"
  expect_contains "re-running the reconciler changes nothing (idempotent)" \
    "$("${DC[@]}" run --rm -T tenant-reconcile 2>&1)" "0 change(s), 0 refused"
  expect_contains "tenant identity writes its own namespace, refused on silver" \
    "$("${DC[@]}" run --rm -T --entrypoint tenant-reconcile tenant-reconcile --probe 2>&1)" \
    "create in markets_bronze -> 200; create in silver -> 403"
  expect_contains "tenants on the SASL listener see only their own topics and cannot write another's (Kafka ACLs)" \
    "$("${DC[@]}" run --rm -T --entrypoint tenant-reconcile tenant-reconcile --probe-kafka 2>&1 | grep probe-kafka | tr '\n' ' ')" \
    "tenant-canary: sees \['canary.events'\].*TOPIC_AUTHORIZATION_FAILED.*tenant-markets-data: sees .*TOPIC_AUTHORIZATION_FAILED"
  # Running, not producing: whether Coinbase answers is the tenant's concern, not the platform's.
  # card-auths runs without a licence too (it idles): CI has none.
  expect_contains "tenant services run from their tenant file (markets-data card-auths, coinbase-feed, streams)" \
    "$("${DC[@]}" --profile tenant-code ps -a --format '{{.Service}}={{.State}}' tenant-markets-data-card-auths \
      tenant-markets-data-coinbase-feed tenant-markets-data-streams 2>&1 | sort | tr '\n' ' ')" \
    "^tenant-markets-data-card-auths=running tenant-markets-data-coinbase-feed=running tenant-markets-data-streams=running $"
  # A tenant repo's own Compose project joins the platform's networks by name (ADR 14).
  expect_contains "a container outside this project joins open-lakehouse_data and reaches Polaris" \
    "$(docker run --rm --network open-lakehouse_data --entrypoint python open-lakehouse/platform:dev \
      -c 'import httpx; print(httpx.get("http://polaris:8182/q/health/ready").status_code)' 2>&1)" "^200$"
fi

if running cdc-stream; then
  echo "▸ Streaming (CDC -> Kafka -> Spark -> Iceberg)"
  st=$("${DC[@]}" exec -T cdc-connect curl -fsS localhost:8083/connectors/corebank-cdc/status 2>&1)
  expect_contains "Debezium connector and task RUNNING" "$st" '"tasks":\[{"id":0,"state":"RUNNING"'
  expect_contains "replication slot active (WAL is being consumed)" \
    "$(psql_audit "SELECT active FROM pg_replication_slots WHERE slot_name = 'corebank_cdc'")" "^t$"
  out=$(uv run --quiet scripts/freshness_probe.py --budget 60 2>&1)
  expect_contains "source commit visible to a colleague in < 60 s ($(grep -oE '[0-9.]+s' <<<"$out" | head -1))" "$out" "^freshness: [0-9.]*s"
  if running dagster-code; then
    expect_contains "stream holds the silver single-writer lease (batch defers to it)" \
      "$("${DC[@]}" exec -T dagster-code python3 -c 'from lakehouse_orchestration.definitions import silver_lease_holder as h; print(h())' 2>&1)" "^corebank-cdc-v1$"
  fi
fi

if running call-assist; then
  echo "▸ Live Call Assist (transcript stream -> agent -> governed tools, as the colleague)"
  out=$(cd services/platform && uv run --quiet call-assist-evals 2>&1)
  expect_contains "offline evals pass (understanding + scripted calls, zero ungrounded cards)" "$out" "^Gate: PASS"
  out=$(uv run --quiet scripts/live_call.py alice card_fraud --json --speed=3 2>&1)
  expect_contains "fraud payment written seconds ago is pinpointed (CDC -> agent)" "$out" '"title": "Unrecognised payment located"'
  expect_contains "account cards stay locked until ID&V" "$out" '"requires_verification": true'
  expect_contains "every card cites a procedure or evidence" "$out" '"procedure": {"id": "CARD-FRAUD-01"'
  call_id=$(python3 -c 'import sys,json; print(json.loads(sys.stdin.read())["call"]["call_id"])' <<<"$out" 2>/dev/null)
  expect_contains "the agent's lookups are audited under the call reference" \
    "$(psql_audit "SELECT count(*) >= 3 FROM audit.tool_calls WHERE purpose = '${call_id}' AND colleague = 'alice'")" "^t$"
  out=$(uv run --quiet scripts/live_call.py carol card_fraud --json --speed=3 2>&1)
  expect_contains "an analyst's agent can't identify callers (sees no PII)" "$out" '"title": "No matching customer you can serve"'
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST localhost:8090/api/calls -H 'content-type: application/json' -d '{"scenario":"card_fraud"}')
  [[ "$code" == 401 ]] && ok "assist API rejects unauthenticated calls (401)" || bad "assist API rejects unauthenticated calls" "HTTP $code"
fi

if running prometheus; then
  echo "▸ Observability (SLOs as code)"
  # Targets of blueprints that are not running (core banking's CDC stream, Live Call Assist) are not expected up.
  skip="none"
  $BANK || skip+="|cdc-stream"
  running call-assist || skip+="|call-assist"
  down=$(curl -fsS localhost:9090/api/v1/query --data-urlencode "query=count(up{job!~\"$skip\"} == 0) or vector(0)" | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["result"][0]["value"][1])')
  [[ "$down" == 0 ]] && ok "every scrape target up (incl. Trino via machine identity + OPA)" || bad "every scrape target up" "$down down"
  n=$(curl -fsS localhost:9090/api/v1/rules | python3 -c 'import sys,json; print(sum(len(g["rules"]) for g in json.load(sys.stdin)["data"]["groups"]))')
  (( n >= 20 )) && ok "SLO recording + burn-rate alert rules loaded ($n)" || bad "SLO rules loaded" "$n"
  if running call-assist; then
    cap=$(curl -fsS localhost:9090/api/v1/query --data-urlencode 'query=max(assist_llm_budget_usd)' | python3 -c 'import sys,json; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "none")')
    rules=$(curl -fsS localhost:9090/api/v1/rules | grep -c AssistAIBudgetNearlySpent)
    [[ "$cap" != none && "$rules" -ge 1 ]] && ok "AI spend cap exported and alerted on (cap \$$cap/day)" || bad "AI spend cap exported and alerted on" "cap=$cap rule=$rules"
  fi
  gpw=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2)
  n=$(curl -fsS -u "admin:$gpw" "localhost:3001/api/search?tag=open-lakehouse" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
  (( n >= 3 )) && ok "Grafana dashboards provisioned from code ($n)" || bad "Grafana dashboards provisioned" "$n"
  if running tenant-metrics; then
    prom() { curl -fsS localhost:9090/api/v1/query --data-urlencode "query=$1" | python3 -c 'import sys,json; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "none")'; }
    # tenant-metrics refreshes every 30 s and Prometheus scrapes every 15 s: allow the first round.
    for _ in $(seq 1 20); do seen=$(prom 'count(tenant_table_observed == 1)'); [[ "$seen" != none ]] && break; sleep 3; done
    # A declared table whose job has not run yet (gold, reference data on a fresh stack) does not exist:
    # that is not a read failure. Only tables that exist and cannot be read count.
    unseen=$(prom 'count((tenant_table_observed == 0) unless on(tenant, table) (tenant_table_exists == 0)) or vector(0)')
    [[ "$seen" != none && "$unseen" == 0 ]] && ok "platform reads every declared table that exists ($seen observed)" || bad "platform reads every declared table that exists" "observed=$seen unreadable=$unseen"
    dup=$(prom 'count(tenant_table_lag_records < 0) or vector(0)')
    [[ "$dup" == 0 ]] && ok "no tenant table mirrors more records than its topic holds (no duplicates)" || bad "no tenant table holds duplicates" "$dup table(s) hold more records than their topic"
    n=$(curl -fsS -u "admin:$gpw" "localhost:3001/api/search?query=Tenant%20streams" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
    (( n >= 1 )) && ok "Grafana: Tenant streams dashboard provisioned" || bad "Grafana: Tenant streams dashboard provisioned" "$n"
  fi
  if running catalog; then
    # Tenant-owned gold products, as the catalog sees them (contracts + live freshness + Dagster checks).
    gold() { curl -fsS localhost:3005/api/products.json | python3 -c "
import sys, json
rows = [r for r in json.load(sys.stdin) if r['layer'] == 'gold' and r['has_owner']]
$1"; }
    out=$(gold 'print(len(rows) or "none", [r["key"] for r in rows if r["freshness"]["promise_seconds"] is None or not r["declared_checks"]])')
    [[ "$out" == "none"* || "$out" == *"['"* ]] && bad "catalog: every gold product has a freshness promise and declared checks" "$out" || ok "catalog: every tenant gold product has an owner, a freshness promise and declared checks (${out%% *})"
    late=$(gold 'print([r["key"] for r in rows if r["freshness"]["state"] in ("late", "stale")])')
    [[ "$late" == "[]" ]] && ok "catalog: no gold product is late or stale against its promise" || bad "catalog: no gold product is late or stale" "$late"
    failing=$(gold 'print([r["key"] + "." + n for r in rows for n, s in r["checks"].items() if s == "FAILED"])')
    [[ "$failing" == "[]" ]] && ok "catalog: no quality check on a gold product is failing in Dagster" || bad "catalog: a gold product check is failing" "$failing"
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3002/)
  [[ "$code" == 302 || "$code" == 403 ]] && ok "Dagster UI requires SSO (HTTP $code without a session)" || bad "Dagster UI requires SSO" "HTTP $code"
fi

if running dagster-code; then
  echo "▸ Orchestration"
  out=$("${DC[@]}" exec -T dagster-code python3 -c '
from lakehouse_orchestration.definitions import defs
g = defs.resolve_asset_graph()
print(len(g.get_all_asset_keys()), len(g.asset_check_keys), len(list(defs.schedules)))' 2>&1 | tail -1)
  expect_contains "Dagster: 13 assets, 19 WAP checks, 3 schedules" "$out" "^13 19 3$"
  out=$("${DC[@]}" exec -T dagster-code python3 - < jobs/spark/orchestration/tests/lease_race.py 2>&1 | tail -1)
  expect_contains "Dagster: scheduled run skips silver the stream owns; manual backfill refused" "$out" "^OK$"
  # Tenant code servers are generated from tenants/*.yaml (ADR 14); each must load in Dagster.
  out=$("${DC[@]}" exec -T dagster-webserver python3 -c '
import json, urllib.request as u
q = "{workspaceOrError{... on Workspace{locationEntries{name locationOrLoadError{__typename}}}}}"
r = json.load(u.urlopen(u.Request("http://localhost:3000/graphql", json.dumps({"query": q}).encode(), {"Content-Type": "application/json"})))
print(",".join(sorted(e["name"] for e in r["data"]["workspaceOrError"]["locationEntries"] if e["locationOrLoadError"]["__typename"] == "RepositoryLocation")))' 2>&1 | tail -1)
  expect_contains "Dagster: platform and tenant code locations load (canary, markets-data)" "$out" "^canary,lakehouse,markets-data$"
  if ! $BANK; then
    # Without corebank the platform's bank schedules are declared but stopped (ADR 15); maintenance runs.
    out=$("${DC[@]}" exec -T dagster-webserver python3 -c '
import json, urllib.request as u
q = "{workspaceOrError{... on Workspace{locationEntries{name locationOrLoadError{... on RepositoryLocation{repositories{schedules{name scheduleState{status}}}}}}}}}"
r = json.load(u.urlopen(u.Request("http://localhost:3000/graphql", json.dumps({"query": q}).encode(), {"Content-Type": "application/json"})))
print(",".join(sorted(s["name"] + "=" + s["scheduleState"]["status"] for e in r["data"]["workspaceOrError"]["locationEntries"] if e["name"] == "lakehouse" for repo in e["locationOrLoadError"]["repositories"] for s in repo["schedules"])))' 2>&1 | tail -1)
    expect_contains "Dagster: without corebank the bank schedules are stopped and maintenance still runs" "$out" \
      "^gold_refresh_schedule=STOPPED,maintenance_job_schedule=RUNNING,nightly_refresh=STOPPED$"
  fi
  # One run at a time: a run whose worker died but stays STARTED would hold the only slot and queue
  # every schedule behind it (29 Sep 2026: markets gold stayed empty). max_runtime_seconds fails it.
  expect_contains "Dagster: run monitoring caps a run's runtime (a dead worker cannot block the queue)" \
    "$("${DC[@]}" exec -T dagster-daemon grep -o 'max_runtime_seconds: [0-9]*' /opt/dagster/home/dagster.yaml 2>&1)" "^max_runtime_seconds: 3600$"
  out=$("${DC[@]}" exec -T dagster-webserver python3 -c '
import json, time, urllib.request as u
q = "{runsOrError(filter:{statuses:[STARTED]}){... on Runs{results{startTime}}}}"
r = json.load(u.urlopen(u.Request("http://localhost:3000/graphql", json.dumps({"query": q}).encode(), {"Content-Type": "application/json"})))
print(sum(1 for x in r["data"]["runsOrError"]["results"] if x["startTime"] and time.time() - x["startTime"] > 3900))' 2>&1 | tail -1)
  expect_contains "Dagster: no run has been in progress past the cap" "$out" "^0$"
  if running cdc-stream; then
    # The stream writes silver outside Dagster; it reports its writes and WAP checks (runless).
    out=$("${DC[@]}" exec -T dagster-webserver python3 -c '
import json, urllib.request as u
q = "{assetNodes(group:{groupName:\"silver\",repositoryName:\"__repository__\",repositoryLocationName:\"lakehouse\"}){assetChecksOrError{... on AssetChecks{checks{executionForLatestMaterialization{status}}}}}}"
r = json.load(u.urlopen(u.Request("http://localhost:3000/graphql", json.dumps({"query": q}).encode(), {"Content-Type": "application/json"})))
s = [(c["executionForLatestMaterialization"] or {}).get("status") for n in r["data"]["assetNodes"] for c in n["assetChecksOrError"]["checks"]]
print(len(s), "SUCCEEDED" if set(s) == {"SUCCEEDED"} else s)' 2>&1 | tail -1)
    expect_contains "Dagster: silver's stream writes and 16 WAP checks reported, all passing" "$out" "^16 SUCCEEDED$"
  fi
fi

if running marquez; then
  echo "▸ Lineage (OpenLineage -> Marquez)"
  out=$("${DC[@]}" exec -T marquez curl -fsS "localhost:5000/api/v1/lineage?nodeId=dataset:s3://lakehouse:warehouse/gold/customer_360&depth=6" 2>&1)
  expect_contains "lineage: gold.customer_360 traces back to silver (OpenLineage)" "$out" "warehouse/silver/customers"
  # Browsers send every localhost portal's cookies through the UI to the API (was a 431 past 8 KiB).
  expect_contains "lineage UI API answers with a browser's worth of cookies (20 KiB)" \
    "$("${DC[@]}" exec -T marquez curl -s -o /dev/null -w '%{http_code}' -H "Cookie: pad=$(printf '%020000d' 0)" \
      http://marquez-web:3000/api/v1/namespaces 2>&1)" "^200$"
fi

echo
echo "Result: ${PASS} passed, ${FAIL} failed"
exit "$FAIL"
