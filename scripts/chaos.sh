#!/usr/bin/env bash
# Chaos suite: hard-kill each component while the platform is in use, then check
#   1. it degrades SAFELY (fails closed, fails fast, never returns wrong data)
#   2. it recovers ON ITS OWN (the healer reconciles; no human steps)
#   3. how long recovery took (time-to-recover, TTR)
# Streaming components have a different journey: a change committed to the source
# DURING the outage must reach silver after recovery (no data loss), and reads must
# keep working meanwhile (a CDC outage makes data staler, never unavailable or wrong).
# Usage: scripts/chaos.sh [component ...]   (default: all that are running)
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PROFILES=(--profile streaming --profile ops)
dc() { docker compose "${PROFILES[@]}" "$@"; }
SERVING=(opa trino polaris keycloak postgres rustfs mcp-gateway)
STREAMING=(kafka cdc-connect cdc-stream call-assist)
running() { [[ -n "$(dc ps -q --status running "$1" 2>/dev/null)" ]]; }
ALL=("${SERVING[@]}")
for s in "${STREAMING[@]}"; do running "$s" && ALL+=("$s"); done
TARGETS=("${@:-${ALL[@]}}")
RESULTS=()

probe_call() {  # the real user journey: agent fetches a customer as alice
  uv run --quiet scripts/agent_call.py alice get_customer_360 \
    '{"call_reference":"CHAOS-PROBE","customer_id":"C0000052"}' 2>/dev/null | grep -q '"found": true'
}

degraded_behaviour() {
  local out
  out=$(uv run --quiet scripts/agent_call.py alice get_customer_360 \
        '{"call_reference":"CHAOS-DEGRADED","customer_id":"C0000052"}' 2>&1 | tr '\n' ' ')
  if grep -q '"found": true' <<<"$out"; then
    echo "served (redundancy/cache)"
  elif grep -q 'colleague_token' <<<"$out"; then
    echo "sign-in unavailable (no new tokens; nothing served unauthenticated)"
  elif grep -qiE 'ConnectError|Connection refused|ConnectionRefused' <<<"$out"; then
    echo "gateway unreachable (agent told: no data, retry)"
  elif grep -qiE 'ReadTimeout' <<<"$out"; then
    echo "HUNG until client timeout (bad: no fail-fast)"
  elif grep -qE 'unavailable|denied|failed|TOOL ERROR' <<<"$out"; then
    echo "failed closed: $(grep -oE 'TOOL ERROR: [^:]*: [^.]*' <<<"$out" | head -1 | sed 's/TOOL ERROR: Error executing tool [a-z_0-9]*: //' | cut -c1-48)"
  else
    echo "client error: $(cut -c1-60 <<<"$out")"
  fi
}

is_streaming() { [[ " ${STREAMING[*]} " == *" $1 "* ]]; }

insert_probe_txn() {  # a card payment lands in core banking while streaming is down
  local id
  id="TCHAOS$(openssl rand -hex 7)"
  dc exec -T postgres psql -q -U postgres -d corebank -c "INSERT INTO core.transactions
    (txn_id, account_id, txn_ts, amount, currency, merchant, category, channel, status)
    SELECT '$id', account_id, now(), -1.23, 'GBP', 'Chaos Probe', 'shopping', 'card', 'posted'
    FROM core.accounts WHERE customer_id = 'C0000052' LIMIT 1" >/dev/null && echo "$id"
}

txn_visible() {
  scripts/trino-sql.sh ops_admin "SELECT count(*) FROM lakehouse.silver.transactions WHERE txn_id = '$1'" 2>/dev/null |
    tail -1 | grep -q '^1$'
}

assist_up() { curl -fs -o /dev/null --max-time 3 localhost:8090/healthz 2>/dev/null; }

wait_until_healthy() {
  local deadline=$((SECONDS + 240))
  while (( SECONDS < deadline )); do
    probe_call && return 0
    sleep 2
  done
  return 1
}

echo "Pre-flight: platform must be healthy"
probe_call || { echo "Platform not healthy before chaos; run 'make up' first." >&2; exit 1; }

HEALER=""
trap '[[ -n "$HEALER" ]] && kill $HEALER 2>/dev/null' EXIT

wait_until() {  # wait_until <seconds> <command...>
  local deadline=$((SECONDS + $1)); shift
  while (( SECONDS < deadline )); do "$@" && return 0; sleep 2; done
  return 1
}

for svc in "${TARGETS[@]}"; do
  echo "──── chaos: kill -9 ${svc}"
  docker kill -s KILL "$(dc ps -q "$svc")" >/dev/null
  start=$SECONDS
  # Observe the outage with no healer running, so nothing masks the failure mode.
  if is_streaming "$svc"; then
    probe_id=$(insert_probe_txn)
    if [[ "$svc" == call-assist ]]; then
      assist_up && during="assist still up (?)" || during="console unavailable; data platform unaffected"
    else
      probe_call && during="reads still served; freshness paused" \
                 || during="reads FAILED (streaming outage leaked into serving)"
    fi
  else
    during=$(degraded_behaviour)
  fi
  echo "   during outage : ${during}"
  scripts/healer.sh >>.chaos-healer.log 2>&1 &
  HEALER=$!
  if is_streaming "$svc"; then
    if [[ "$svc" == call-assist ]]; then
      wait_until 240 assist_up && ok=1 || ok=0
    else
      # Recovered = the change made during the outage arrived: nothing was lost.
      wait_until 300 txn_visible "$probe_id" && ok=1 || ok=0
      (( ok )) && during="${during}; change made during outage arrived (no loss)"
    fi
  else
    wait_until_healthy && ok=1 || ok=0
  fi
  if (( ok )); then
    ttr=$((SECONDS - start))
    echo "   recovered     : yes, TTR ${ttr}s"
    RESULTS+=("$(printf '%-12s | %-84s | %4ss' "$svc" "${during:0:84}" "$ttr")")
  else
    echo "   recovered     : NO within 240s" >&2
    RESULTS+=("$(printf '%-12s | %-84s | %5s' "$svc" "${during:0:84}" "FAIL")")
  fi
  kill $HEALER 2>/dev/null; wait $HEALER 2>/dev/null; HEALER=""
done

echo
echo "component    | behaviour during outage                                                              |  TTR"
echo "-------------+--------------------------------------------------------------------------------------+------"
printf '%s\n' "${RESULTS[@]}"
echo
echo "Audit chain intact after chaos: $(dc exec -T postgres psql -U postgres -d audit -tAc \
  "SELECT COALESCE('BROKEN at seq ' || audit.verify_chain(), 'yes')")"
