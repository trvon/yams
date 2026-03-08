# Benchmarks

This section centralizes public benchmark guidance and latest reported results.

## Available benchmark docs

- [Performance Report](performance_report.md) - canonical performance tables and release baselines.
- [Storage Backends (Local vs S3-compatible)](storage_backends.md) - benchmark runner for
  local storage compared with S3-compatible providers.

## Validation status policy

- Public benchmark docs distinguish between:
  - **tested** paths validated in this repository, and
  - **supported** paths that are available but not currently part of automated validation.

When in doubt, treat tested paths as production-ready defaults.

## Public vs internal benchmark assets

- Public docs in this section contain scope, status, and published results.
- Internal harness scripts and run plumbing are intentionally not shipped in public docs.
