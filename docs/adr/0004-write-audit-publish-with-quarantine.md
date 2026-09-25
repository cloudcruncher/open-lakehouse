# 4. Write-Audit-Publish with quarantine and a halt threshold

**Status:** accepted

## Context
Agents act on data in real time. A bad batch published to readers is worse than a late
one. But halting the whole pipeline for three malformed rows is also a failure.

## Decision
- Row-level contract violations go to `ops.quarantine` with reasons. They are never
  silently dropped.
- Good rows MERGE onto a per-run Iceberg branch, which is audited (key uniqueness, nulls,
  quarantine rate). `main` fast-forwards only if every check passes.
- A quarantine rate above 1% signals an upstream incident: the pipeline **halts** with
  exit code 3, the retry wrapper does not retry, and readers keep the last good snapshot.
  An operator can publish good rows explicitly with `--accept-quarantine`.

## Consequences
- Readers never see partially validated data. Rollback is an Iceberg snapshot operation.
- Failed branches are kept for forensics, and maintenance drops them after 7 days.
