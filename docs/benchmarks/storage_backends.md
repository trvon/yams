# Storage Backends Benchmark

Local vs S3-compatible backend behavior for CLI-level CRUD workflows.

## Scope

- Compare backend behavior for `add`, `get`, `search`, and `delete`.
- Measure end-to-end CLI latency (includes command startup + daemon path), not storage microbenchmarks.
- Keep embeddings/model loading disabled for stable backend-path comparisons.

## Provider Validation Status

- **Cloudflare R2**: validated in this repository, including `temp_credentials` flow.
- **AWS S3**: supported by the same S3-compatible path, but not currently part of automated validation here.

## Latest Local Refresh (2026-06-09)

- Backends: `local`
- Compression modes: `compressed,raw`
- Params: `iterations=1`, `files=60`, `file_size_kb=8`, `retrieve_count=20`
- Build: Debug ASAN+coverage (`builddir`)

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local_compressed | 58.07 | 2072.64 | 0.48 | 1047.84 | 0.95 | 1054.69 | 0.95 |
| local_raw | 58.27 | 2076.83 | 0.48 | 1044.87 | 0.96 | 1044.27 | 0.96 |

## Latest Live Local/R2 Validation

- Backends: `local,r2`
- Params: `iterations=1`, `files=60`, `file_size_kb=8`, `retrieve_count=20`
- R2 auth mode: `temp_credentials` (bearer token + account id bootstrap)

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local | 40.34 | 1675.76 | 0.60 | 1519.15 | 0.66 | 1500.77 | 0.67 |
| r2 | 1.14 | 5126.69 | 0.20 | 4398.37 | 0.23 | 4190.88 | 0.24 |

## Compression-Mode Speed Track

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local_compressed | 58.07 | 2072.64 | 0.48 | 1047.84 | 0.95 | 1054.69 | 0.95 |
| local_raw | 58.27 | 2076.83 | 0.48 | 1044.87 | 0.96 | 1044.27 | 0.96 |

## Multi-Client Backend Benchmark Track

- Scope: agent-like read-heavy workloads.
- Profiles: `mixed`, `external_agent_churn`.
- Transports: `daemon_ipc`, `mcp`.
- Client counts: `4`, `8` total concurrent clients.
- Backends: `local`, `r2` (R2 in `temp_credentials` mode).

### Multi-Client Run Results

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
