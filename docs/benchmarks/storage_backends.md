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

## Compression-mode speed track

R2/S3 writes now use the same transparent compression wrapper as local storage: content is hashed
as original bytes, then compressible chunks are compressed before the object-storage backend sees
the payload. This affects both upload bandwidth/cost and retrieval latency, so live backend runs
should compare both modes.

Harness:

```bash
YAMS_BENCH_BIN=$PWD/build/debug/tools/yams-cli/yams-cli \
YAMS_BENCH_R2_BUCKET=<bucket> \
YAMS_BENCH_R2_ENDPOINT=<account-id>.r2.cloudflarestorage.com \
YAMS_BENCH_R2_ACCESS_KEY=<r2-s3-access-key> \
YAMS_BENCH_R2_SECRET_KEY=<r2-s3-secret-key> \
YAMS_BENCH_R2_REGION=auto \
python3 tests/benchmarks/run_local_vs_r2_benchmark.py \
  --backends r2 \
  --compression-modes both \
  --iterations 3 \
  --files 60 \
  --file-size-kb 64 \
  --retrieve-count 20 \
  --require-remote \
  --output-dir bench_results/storage_backends/r2-compression-$(date -u +%Y%m%dT%H%M%SZ)
```

For temporary Cloudflare R2 credentials instead of direct S3 keys:

```bash
YAMS_BENCH_R2_AUTH_MODE=temp_credentials
YAMS_BENCH_R2_API_TOKEN=<cloudflare-api-token>
YAMS_BENCH_R2_ACCOUNT_ID=<account-id> # optional if encoded in endpoint
```

Latest local harness smoke (2026-05-02, credentials not present in this shell for live R2):

- Backends: `local`
- Compression modes: `compressed,raw`
- Params: `iterations=1`, `files=8`, `file_size_kb=4`, `retrieve_count=3`

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local_compressed | 7.61 | 2171.09 | 0.46 | 1064.63 | 0.94 | 1066.49 | 0.94 |
| local_raw | 7.60 | 2197.76 | 0.46 | 1067.01 | 0.94 | 1071.99 | 0.93 |

Live R2 result status:

- Not run in this shell: required `YAMS_BENCH_R2_*` credentials were absent.
- The harness now fails fast with `--require-remote` when credentials are missing, and it uses
  distinct object prefixes for `compressed` vs `raw` runs.

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
