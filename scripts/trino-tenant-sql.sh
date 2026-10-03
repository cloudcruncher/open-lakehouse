#!/usr/bin/env bash
# Run SQL against Trino as a tenant's own identity (ADR 14): the Keycloak client the reconciler
# made for it, signed in with client credentials. Reads only the tenant's own namespaces.
# Usage: scripts/trino-tenant-sql.sh <tenant> "<sql>"
set -euo pipefail
cd "$(dirname "$0")/.."
tenant=$1; sql=$2
creds=$(docker compose --profile streaming run --rm -T --no-deps --entrypoint cat tenant-reconcile \
  "/run/platform-secrets/tenants/$tenant/trino.env")
cid=$(sed -n 's/^TRINO_CLIENT_ID=//p' <<<"$creds")
secret=$(sed -n 's/^TRINO_CLIENT_SECRET=//p' <<<"$creds")
token=$(curl -fsS http://localhost:8280/realms/bank/protocol/openid-connect/token \
  -d grant_type=client_credentials -d "client_id=$cid" --data-urlencode "client_secret=$secret" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
TOKEN=$token SQL=$sql uv run --quiet --with 'trino>=0.340' python - <<'PY'
import os, sys, trino
from trino.auth import JWTAuthentication
conn = trino.dbapi.connect(host="localhost", port=8443, http_scheme="https",
                           auth=JWTAuthentication(os.environ["TOKEN"]), verify=".secrets/ca.pem",
                           catalog="lakehouse")
cur = conn.cursor()
try:
    cur.execute(os.environ["SQL"])
    rows = cur.fetchall()
except trino.exceptions.TrinoQueryError as e:
    print(f"ERROR {e.error_name}: {e.message}"); sys.exit(2)
if cur.description is None:
    print("OK"); sys.exit(0)
cols = [d[0] for d in cur.description]
print(" | ".join(cols))
for r in rows:
    print(" | ".join("" if v is None else str(v) for v in r))
PY
