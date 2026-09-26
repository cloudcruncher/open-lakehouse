#!/usr/bin/env bash
# Generates every secret the local platform needs: random passwords in .env and a
# local CA plus a Trino TLS certificate in .secrets/. Safe to re-run: existing
# values are kept, so credentials never change under running services.
# In production these come from a secrets manager (OpenBao/Vault, AWS Secrets
# Manager) and a real PKI — nothing in this repo is meant to be long-lived.
set -euo pipefail
cd "$(dirname "$0")/.."

ENV_FILE=.env
SECRETS_DIR=.secrets
touch "$ENV_FILE"
chmod 600 "$ENV_FILE"
mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

rand() { openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c "${1:-32}"; }

ensure() {
  local key=$1 value=$2
  if ! grep -q "^${key}=" "$ENV_FILE"; then
    # A hand-edited last line may lack a newline; never glue a new key onto it.
    [[ -s $ENV_FILE && -n $(tail -c1 "$ENV_FILE") ]] && echo >>"$ENV_FILE"
    echo "${key}=${value}" >>"$ENV_FILE"
    echo "  generated ${key}"
  fi
}

echo "Secrets -> ${ENV_FILE}"
ensure POSTGRES_PASSWORD "$(rand)"
ensure POLARIS_DB_PASSWORD "$(rand)"
ensure KEYCLOAK_DB_PASSWORD "$(rand)"
ensure COREBANK_DB_PASSWORD "$(rand)"
ensure COREBANK_READER_PASSWORD "$(rand)"
ensure AUDIT_WRITER_PASSWORD "$(rand)"
ensure STORAGE_ROOT_USER "lakehouse-root"
ensure STORAGE_ROOT_PASSWORD "$(rand)"
ensure POLARIS_ROOT_CLIENT_SECRET "$(rand)"
ensure POLARIS_TOKEN_KEY "$(rand 64)"
ensure KEYCLOAK_ADMIN_PASSWORD "$(rand)"
ensure MCP_GATEWAY_CLIENT_SECRET "$(rand)"
ensure DEMO_USER_PASSWORD "$(rand 20)"
ensure TRINO_SHARED_SECRET "$(rand 64)"
ensure DEBEZIUM_DB_PASSWORD "$(rand)"
ensure MARQUEZ_DB_PASSWORD "$(rand)"
ensure DAGSTER_DB_PASSWORD "$(rand)"
ensure GRAFANA_ADMIN_PASSWORD "$(rand)"
ensure GRAFANA_CLIENT_SECRET "$(rand)"
ensure PROMETHEUS_CLIENT_SECRET "$(rand)"
ensure MONITORING_DB_PASSWORD "$(rand)"
ensure DAGSTER_UI_CLIENT_SECRET "$(rand)"
ensure LINEAGE_UI_CLIENT_SECRET "$(rand)"
ensure OAUTH2_PROXY_COOKIE_SECRET "$(rand 32)"
ensure SUPERSET_DB_PASSWORD "$(rand)"
ensure SUPERSET_SECRET_KEY "$(rand 64)"
ensure SUPERSET_CLIENT_SECRET "$(rand)"

# --- Local PKI: a throwaway CA and a server cert for Trino (TLS is mandatory
# --- for Trino authentication, and we do not turn that safety check off).
if [[ ! -f "$SECRETS_DIR/ca.pem" ]]; then
  echo "PKI -> ${SECRETS_DIR}/"
  openssl req -x509 -newkey rsa:3072 -nodes -days 825 -sha256 \
    -subj "/CN=open-lakehouse local CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash" \
    -keyout "$SECRETS_DIR/ca.key" -out "$SECRETS_DIR/ca.pem" 2>/dev/null
fi
if [[ ! -f "$SECRETS_DIR/trino.pem" ]]; then
  openssl req -newkey rsa:3072 -nodes -sha256 -subj "/CN=trino" \
    -keyout "$SECRETS_DIR/trino.key" -out "$SECRETS_DIR/trino.csr" 2>/dev/null
  openssl x509 -req -in "$SECRETS_DIR/trino.csr" -CA "$SECRETS_DIR/ca.pem" -CAkey "$SECRETS_DIR/ca.key" \
    -CAcreateserial -days 397 -sha256 -out "$SECRETS_DIR/trino.crt" \
    -extfile <(printf "%s\n" "subjectAltName=DNS:trino,DNS:localhost,IP:127.0.0.1" \
      "basicConstraints=critical,CA:FALSE" "keyUsage=critical,digitalSignature,keyEncipherment" \
      "extendedKeyUsage=serverAuth" "subjectKeyIdentifier=hash" "authorityKeyIdentifier=keyid,issuer") 2>/dev/null
  # Trino reads a PEM keystore containing the private key followed by the cert chain.
  cat "$SECRETS_DIR/trino.key" "$SECRETS_DIR/trino.crt" >"$SECRETS_DIR/trino.pem"
  rm -f "$SECRETS_DIR/trino.csr"
fi
# Prometheus reads its OAuth client secret from a file (its config has no env expansion).
grep '^PROMETHEUS_CLIENT_SECRET=' "$ENV_FILE" | cut -d= -f2- | tr -d '\n' >"$SECRETS_DIR/prometheus_client_secret"
chmod 644 "$SECRETS_DIR/prometheus_client_secret"
# Containers run as non-root users; they need to read these (the dir itself stays 700).
chmod 644 "$SECRETS_DIR"/*.pem "$SECRETS_DIR"/*.crt
echo "Done."
