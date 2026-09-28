#!/usr/bin/env bash
# Local stand-in for a Kubernetes controller: continuously reconcile desired state.
#   * a long-running service that is missing or exited -> start it again
#   * a container whose healthcheck reports unhealthy  -> restart it (liveness probe)
#   * a CDC connector task that FAILED (e.g. the source was down longer than its
#     retry budget)                                    -> restart the failed tasks
# Desired state = the core platform plus every long-running service of whichever
# profiles are up (anything with a restart policy that isn't "no").
# In production this is the kubelet (liveness/readiness probes) plus Deployments /
# StatefulSets and operators (Strimzi restarts failed connector tasks), with
# PodDisruptionBudgets and multiple replicas so a single restart is invisible.
# It runs on the host because giving a container the Docker socket would hand it
# root on the machine.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

CORE=(postgres rustfs polaris keycloak opa trino mcp-gateway)
INTERVAL=${HEAL_INTERVAL:-3}
PROJECT=open-lakehouse
PROFILES=(--profile '*')   # every blueprint (only named services are touched)

log() { echo "$(date +%H:%M:%S) [healer] $*"; }

desired() {
  printf '%s\n' "${CORE[@]}"
  docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --format '{{.ID}}' |
    xargs docker inspect -f '{{.HostConfig.RestartPolicy.Name}} {{index .Config.Labels "com.docker.compose.service"}}' 2>/dev/null |
    awk '$1 != "no" && $1 != "" {print $2}'
}

tick=0
while true; do
  for svc in $(desired | sort -u); do
    state=$(docker compose "${PROFILES[@]}" ps -a --format '{{.State}}' "$svc" 2>/dev/null | head -1)
    if [[ "$state" != "running" && "$state" != "restarting" ]]; then
      log "$svc is '${state:-missing}' -> starting"
      docker compose "${PROFILES[@]}" up -d --no-deps --no-recreate "$svc" >/dev/null 2>&1
    fi
  done
  for c in $(docker ps -q --filter "label=com.docker.compose.project=$PROJECT" --filter health=unhealthy); do
    log "$(docker inspect -f '{{index .Config.Labels "com.docker.compose.service"}}' "$c") unhealthy -> restarting"
    docker restart "$c" >/dev/null
  done
  # Every ~30s: connector tasks that failed stay failed until someone restarts them.
  if (( tick % 10 == 0 )) && docker compose "${PROFILES[@]}" ps --format '{{.State}}' cdc-connect 2>/dev/null | grep -q running; then
    status=$(docker compose "${PROFILES[@]}" exec -T cdc-connect curl -fsS localhost:8083/connectors/corebank-cdc/status 2>/dev/null)
    if grep -q '"state":"FAILED"' <<<"$status"; then
      log "CDC connector has FAILED tasks -> restarting them"
      docker compose "${PROFILES[@]}" exec -T cdc-connect curl -fsS -X POST \
        "localhost:8083/connectors/corebank-cdc/restart?includeTasks=true&onlyFailed=true" >/dev/null 2>&1
    fi
  fi
  tick=$((tick + 1))
  sleep "$INTERVAL"
done
