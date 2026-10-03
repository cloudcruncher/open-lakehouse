#!/bin/bash
# Repo layer for the global TaskCompleted hook (~/.claude/hooks/task-completed.sh).
# Changed files arrive on stdin; a non-zero exit with output means "not complete".
# Generic checks (ruff with each project's own config, bash -n, jq) already ran globally; this adds
# only what is specific to this repo: its stricter ruff rules, and contracts and tenant validation.
changed=$(cat)
rc=0

other=$(grep -E '^(jobs/spark|tenants|contracts)/.*\.py$' <<<"$changed")
if [ -n "$other" ]; then
  tr '\n' '\0' <<<"$other" | xargs -0 uvx ruff check --quiet --line-length 120 --select E,F,B || rc=1
fi

# Data contracts and OPA column tags must agree; tenant files must validate (ADR 14)
if grep -qE '^(contracts/.*\.odcs\.yaml|infra/opa/data/)' <<<"$changed"; then
  uv run --quiet contracts/check.py || rc=1
fi
if grep -qE '^tenants/.*\.yaml$' <<<"$changed"; then
  uv run --quiet tenants/check.py || rc=1
fi
exit $rc
