# Storage Backends Benchmark

This page tracks local vs S3-compatible backend behavior for CLI-level CRUD workflows.

## Scope

- Compare backend behavior for `add`, `get`, `search`, and `delete`.
- Measure end-to-end CLI latency (includes command startup + daemon path), not storage microbenchmarks.
- Keep embeddings/model loading disabled for stable backend-path comparisons.

## Provider validation status

- **Cloudflare R2**: validated in this repository, including `temp_credentials` flow.
- **AWS S3**: supported by the same S3-compatible path, but not currently part of automated validation here.

## Latest validated run (2026-03-08)

- Backends: `local,r2`
- Params: `iterations=1`, `files=60`, `file_size_kb=8`, `retrieve_count=20`
- R2 auth mode: `temp_credentials` (bearer token + account id bootstrap)

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local | 40.34 | 1675.76 | 0.60 | 1519.15 | 0.66 | 1500.77 | 0.67 |
| r2 | 1.14 | 5126.69 | 0.20 | 4398.37 | 0.23 | 4190.88 | 0.24 |

## Validation notes

- Remote fallback guard stayed clean: R2 iterations wrote `0` local object files.
- CRUD gate passed (`PUT/GET/UPDATE/DELETE` semantics validated by harness checks).
- List output currently reports duplicate rows (`120` rows for `60` unique paths) for both backends; benchmark normalizes to unique paths for timing.

## Multi-client backend benchmark track

- Scope: agent-like read-heavy workloads.
- Profiles: `mixed`, `external_agent_churn`.
- Transports: `daemon_ipc`, `mcp`.
- Client counts: `4`, `8` total concurrent clients.
- Backends: `local`, `r2` (R2 in `temp_credentials` mode).

### Latest validated multi-client run (2026-03-08)

- Parameters: `iterations=1`, `files=120`, `file_size_kb=8`, `ops_per_client=12`

| Backend | Profile | Transport | Clients | Ops/s | Fail rate | Search p95 (ms) |
|---|---|---|---:|---:|---:|---:|
| local | mixed | daemon_ipc | 4 | 23.80 | 0.0000 | 5.67 |
| local | mixed | mcp | 4 | 23.83 | 0.0000 | 5.75 |
| local | mixed | daemon_ipc | 8 | 47.56 | 0.0000 | 5.75 |
| local | mixed | mcp | 8 | 47.63 | 0.0000 | 5.79 |
| local | external_agent_churn | daemon_ipc | 4 | 23.82 | 0.0000 | 5.72 |
| local | external_agent_churn | mcp | 4 | 23.81 | 0.0000 | 7.56 |
| local | external_agent_churn | daemon_ipc | 8 | 47.52 | 0.0000 | 5.69 |
| local | external_agent_churn | mcp | 8 | 47.63 | 0.0000 | 7.41 |
| r2 | mixed | daemon_ipc | 4 | 19.32 | 0.2308 | 5.76 |
| r2 | mixed | mcp | 4 | 19.35 | 0.2308 | 5.89 |
| r2 | mixed | daemon_ipc | 8 | 34.28 | 0.3913 | 5.72 |
| r2 | mixed | mcp | 8 | 34.19 | 0.3913 | 5.90 |
| r2 | external_agent_churn | daemon_ipc | 4 | 20.85 | 0.1429 | 5.70 |
| r2 | external_agent_churn | mcp | 4 | 20.87 | 0.1429 | 7.30 |
| r2 | external_agent_churn | daemon_ipc | 8 | 38.61 | 0.2308 | 5.70 |
| r2 | external_agent_churn | mcp | 8 | 38.75 | 0.2308 | 7.34 |

Key findings:

- Local backend scaled near-linearly from 4 to 8 clients, with zero observed failures in this run.
- R2 had lower throughput and non-zero failure rates under this load pattern (roughly 14% to 39%).
- MCP and daemon IPC showed similar throughput bands within each backend/profile/client combination.

## Notes

- Benchmark harness scripts are internal and are not shipped as part of public docs.
- Storage backend validation remains a stabilization priority before expanding benchmark scope.
