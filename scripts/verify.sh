#!/usr/bin/env bash
# End-to-end verification: every security and correctness claim in the README, as
# an executable assertion against the running platform. Exit code = failure count.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
DC=(docker compose --profile streaming --profile ops)
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

echo "▸ Data contracts (governance as code)"
expect_contains "contracts valid (ODCS), policy tags agree with OPA, live tables match" \
  "$(uv run --quiet contracts/check.py --live 2>&1)" ": OK$"

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
  down=$(curl -fsS localhost:9090/api/v1/query --data-urlencode 'query=count(up == 0) or vector(0)' | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["result"][0]["value"][1])')
  [[ "$down" == 0 ]] && ok "every scrape target up (incl. Trino via machine identity + OPA)" || bad "every scrape target up" "$down down"
  n=$(curl -fsS localhost:9090/api/v1/rules | python3 -c 'import sys,json; print(sum(len(g["rules"]) for g in json.load(sys.stdin)["data"]["groups"]))')
  (( n >= 20 )) && ok "SLO recording + burn-rate alert rules loaded ($n)" || bad "SLO rules loaded" "$n"
  gpw=$(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2)
  n=$(curl -fsS -u "admin:$gpw" "localhost:3001/api/search?tag=open-lakehouse" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
  (( n >= 3 )) && ok "Grafana dashboards provisioned from code ($n)" || bad "Grafana dashboards provisioned" "$n"
  code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3002/)
  [[ "$code" == 302 || "$code" == 403 ]] && ok "Dagster UI requires SSO (HTTP $code without a session)" || bad "Dagster UI requires SSO" "HTTP $code"
fi

if running dagster-code; then
  echo "▸ Orchestration and lineage"
  out=$("${DC[@]}" exec -T dagster-code python3 -c '
from lakehouse_orchestration.definitions import defs
g = defs.resolve_asset_graph()
print(len(g.get_all_asset_keys()), len(g.asset_check_keys), len(list(defs.schedules)))' 2>&1 | tail -1)
  expect_contains "Dagster: 13 assets, 19 WAP checks, 3 schedules" "$out" "^13 19 3$"
  out=$("${DC[@]}" exec -T dagster-code python3 - < jobs/spark/orchestration/tests/lease_race.py 2>&1 | tail -1)
  expect_contains "Dagster: scheduled run skips silver the stream owns; manual backfill refused" "$out" "^OK$"
  out=$("${DC[@]}" exec -T marquez curl -fsS "localhost:5000/api/v1/lineage?nodeId=dataset:s3://lakehouse:warehouse/gold/customer_360&depth=6" 2>&1)
  expect_contains "lineage: gold.customer_360 traces back to silver (OpenLineage)" "$out" "warehouse/silver/customers"
fi

echo
echo "Result: ${PASS} passed, ${FAIL} failed"
exit "$FAIL"
