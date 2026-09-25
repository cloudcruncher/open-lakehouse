# 5. RustFS for local S3-compatible storage

**Status:** accepted (September 2026)

## Context
MinIO's community edition was archived in 2026 and its images were removed from Docker
Hub, so reference stacks pinned to `minio/minio` no longer start. We also need STS
AssumeRole locally to exercise real credential vending.

## Decision
Use RustFS (Apache-2.0, S3-compatible, supports STS AssumeRole) locally, as in the
upstream Polaris getting-started guides. Production uses Amazon S3 (or another
enterprise S3 service) with an IAM role for Polaris.

## Consequences
- Local vending is real, not stubbed. `kmsUnavailable=true` because RustFS has no KMS;
  production sets a KMS key per domain.
- Anything S3-specific (object lock, lifecycle rules) must be re-verified on the
  production store.
