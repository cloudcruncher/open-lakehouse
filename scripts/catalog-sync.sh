#!/usr/bin/env bash
# Copy each deployed tenant's data contracts out of its code-location image into the catalog's volume.
# A tenant ships them at /contracts in its image (tenants/README.md); the platform's own contracts
# (and the canary's) are mounted straight from ./contracts. Safe to re-run: each tenant's folder is replaced.
set -euo pipefail
cd "$(dirname "$0")/.."

volume="${COMPOSE_PROJECT_NAME:-open-lakehouse}_catalog-contracts"
docker volume create "$volume" >/dev/null

images=$(uv run --quiet --no-project --with pyyaml python3 - <<'PY'
import pathlib, yaml
for path in sorted(pathlib.Path("tenants").glob("*.yaml")):
    t = yaml.safe_load(path.read_text())
    loc = t["codeLocation"]
    if loc.get("deploy") and not loc["image"].startswith("open-lakehouse/"):  # the platform's own image has none
        print(t["name"], loc["image"])
PY
)

while read -r name image; do
  [[ -n "$name" ]] || continue
  if docker run --rm --user 0 --entrypoint sh -v "$volume:/out" "$image" \
    -c "rm -rf /out/$name && mkdir -p /out/$name && cp /contracts/*.odcs.yaml /out/$name/ && chmod -R a+rX /out/$name" 2>/dev/null; then
    echo "catalog: $name contracts copied from $image"
  else
    echo "catalog: $name ships no /contracts in $image (skipped)"
  fi
done <<<"$images"
