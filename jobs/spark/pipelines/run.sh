#!/usr/bin/env bash
# Runs one pipeline step with bounded retries. Transient failures (a catalog
# restart, a storage blip, a commit conflict with a concurrent writer) retry with
# backoff. Every step is idempotent, so a retry can never double-apply data.
# Data-quality halts (exit code 3) are deliberately NOT retried: bad data needs a
# human decision, not another attempt.
set -uo pipefail
set -a; . /run/platform-secrets/spark_etl.env; set +a

step=$1; shift
max_attempts=${MAX_ATTEMPTS:-3}
for attempt in $(seq 1 "$max_attempts"); do
  /opt/spark/bin/spark-submit \
    --master "local[${SPARK_CORES:-4}]" \
    --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
    --conf spark.ui.showConsoleProgress=false \
    --py-files /opt/pipelines/common.py \
    "/opt/pipelines/${step}.py" "$@"
  rc=$?
  [[ $rc -eq 0 ]] && exit 0
  [[ $rc -eq 3 ]] && { echo "[run] ${step}: data-quality halt; not retrying" >&2; exit 3; }
  echo "[run] ${step}: attempt ${attempt}/${max_attempts} failed (rc=${rc})" >&2
  sleep $(( attempt * attempt * 5 ))
done
exit 1
