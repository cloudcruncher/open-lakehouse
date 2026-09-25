# Security

This is a demonstration platform with **synthetic data only**. No real customer data,
credentials or keys belong in this repository.

- Secrets are generated per machine by `scripts/gen-secrets.sh` into `.env` and
  `.secrets/`, both git-ignored. CI scans the full history with gitleaks and the tree
  with Trivy on every push.
- Every published image has an SBOM and SLSA provenance, and is signed keylessly with
  cosign. Verify before running (see `.github/workflows/release.yml`).
- The local-only shortcuts are listed in the README under "Honest limitations".

To report a vulnerability, please open a private security advisory on this repository
(Security → Advisories → "Report a vulnerability") rather than a public issue.
