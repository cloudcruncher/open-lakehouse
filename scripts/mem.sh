#!/usr/bin/env bash
# Memory per container against its limit, and totals against the Docker VM (ADR 14: measure
# before and after a tenant goes live). Usage: make mem
set -euo pipefail

ids=$(docker ps -q --filter label=com.docker.compose.project=open-lakehouse)
[[ -n $ids ]] || { echo "nothing running"; exit 0; }
vm=$(docker info --format '{{.MemTotal}}')

# shellcheck disable=SC2086  # word-split the container ids
docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' $ids | awk -F'\t' -v vm="$vm" '
  function mib(s,   n) {
    n = s + 0
    if (s ~ /GiB/) return n * 1024
    if (s ~ /KiB/) return n / 1024
    if (s ~ /B$/ && s !~ /[KMG]iB/) return n / 1048576
    return n
  }
  {
    split($2, m, " / "); used = mib(m[1]); lim = mib(m[2])
    name = $1; sub(/^open-lakehouse-/, "", name); sub(/-[0-9]+$/, "", name)
    tenant = name ~ /^tenant-/
    rows[NR] = sprintf("%-24s %7.0f %7.0f %4.0f%%%s", name, used, lim, 100 * used / lim, used / lim > 0.9 ? "  near limit" : "")
    sort_key[NR] = used
    total += used; limits += lim
    if (tenant) { t_used += used; t_lim += lim; n_t++ }
  }
  END {
    printf "%-24s %7s %7s %5s\n", "container", "MiB", "limit", "use"
    for (i = 1; i <= NR; i++) for (j = i + 1; j <= NR; j++) if (sort_key[j] > sort_key[i]) {
      k = sort_key[i]; sort_key[i] = sort_key[j]; sort_key[j] = k; r = rows[i]; rows[i] = rows[j]; rows[j] = r
    }
    for (i = 1; i <= NR; i++) print rows[i]
    vm_mib = vm / 1048576
    printf "\n%d containers use %.1f GiB of %.1f GiB (%.0f%%); their limits add up to %.1f GiB\n", NR, total / 1024, vm_mib / 1024, 100 * total / vm_mib, limits / 1024
    printf "tenant code servers: %d, using %.0f MiB (limits %.0f MiB)\n", n_t, t_used, t_lim
  }'
