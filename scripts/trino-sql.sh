#!/usr/bin/env bash
# Run SQL against Trino as a demo colleague: gets a Keycloak token (password grant
# is for local demos only; real users use SSO), then submits over TLS.
# Usage: scripts/trino-sql.sh <user> "<sql>"
set -euo pipefail
cd "$(dirname "$0")/.."
user=$1; sql=$2
pw=$(grep '^DEMO_USER_PASSWORD=' .env | cut -d= -f2)
token=$(curl -fsS http://localhost:8280/realms/bank/protocol/openid-connect/token \
  -d grant_type=password -d client_id=trino-cli -d "username=$user" --data-urlencode "password=$pw" \
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
cols = [d[0] for d in cur.description]
print(" | ".join(cols))
for r in rows:
    print(" | ".join("" if v is None else str(v) for v in r))
PY
