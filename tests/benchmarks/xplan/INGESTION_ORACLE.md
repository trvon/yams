# Full Simeon ingestion oracle

This contract defines the correctness and identity gates for optimizing the full YAMS ingestion
pipeline with built-in Simeon embeddings. It complements `plans/ingest_pipeline.json`; synthetic
text is valid here for throughput and lifecycle attribution, but not for retrieval-quality claims.

## Existing execution path

| Surface | Path |
|---|---|
| Plan | `tests/benchmarks/xplan/plans/ingest_pipeline.json` |
| Worker | `tests/benchmarks/xplan/workers/ingestion_e2e.py` |
| Benchmark | `tests/benchmarks/search/ingestion_e2e_bench.cpp` |
| Binary | `build/release/tests/benchmarks/ingestion_e2e_bench` |

The benchmark uses an isolated in-process daemon and temporary data, state, socket, PID, and log
paths. Its measured path is directory admission followed by content storage, extraction, metadata
and FTS updates, Simeon embedding inference, vector writes, KG/post-ingest work, queue drain,
`WriteCoordinator` flush, fixed search probes, and clean daemon shutdown.

Run the checked-in plan through xplan; do not create a parallel multi-arm shell matrix:

```bash
python3 tests/benchmarks/xplan/runner.py self-test
python3 tests/benchmarks/xplan/runner.py run ingest_pipeline \
  --build-dir build/release --arm baseline
```

Use the baseline arm first. Run all ablation arms only after the full-path oracle below passes.
Decision-grade measurements require at least three repeats and immutable artifacts under
`build/benchmarks/ingest_pipeline/<stamp>/`.

## Workload tiers

### Smoke contract

The fast smoke fixture is five generated text documents, 1,000 target bytes per document, seed 42,
directory ingestion, concurrency 1, 20 ms polling, 2 ms post-ingest coalescing, vectors/KG enabled,
and the benchmark's fixed `simeon-default` embedding identity. The isolated fixture pins Simeon
through typed config and ignores inherited backend/model selection. It proves wiring only; it is
not a performance or SPQ persistence baseline.

```bash
env -u YAMS_DISABLE_VECTORS -u YAMS_BENCH_FORCE_MOCK_EMBEDDINGS \
  -u YAMS_BENCH_DISABLE_KG -u YAMS_DISABLE_GLINER_TITLES \
  YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
  YAMS_BENCH_CORPUS_SIZE=5 YAMS_BENCH_DOC_SIZE=1000 \
  YAMS_BENCH_POLL_INTERVAL_MS=20 YAMS_BENCH_INGEST_MODE=directory \
  YAMS_BENCH_INGEST_CONCURRENCY=1 YAMS_BENCH_POST_INGEST_COALESCE_MS=2 \
  build/release/tests/benchmarks/ingestion_e2e_bench
```

### Decision contract

Decision evidence uses two complementary seed-42, directory-ingestion lanes:

1. Five 1,000-byte documents with real GLiNER must consume exact post/embed/KG/title counts and
   preserve the model, Simeon, search, and lifecycle identities.
2. The 100-by-8,192-byte no-GLiNER persistence lane must produce at least 256 stored vector rows,
   train and persist SPQ, reload the persisted generation, and preserve exact output fingerprints.

Both lanes require three identical workload identities. Task-specific calibration may increase
`corpus_size` and `doc_size`, but a before/after comparison must use the identical generated corpus
fingerprint and parameters. Record the selected values in the run report rather than silently
changing this contract.

## Required experiment identity

Every run must record these fields in machine-readable output:

- Git revision, dirty state, build type, compiler/architecture, and benchmark binary SHA-256;
- corpus seed, corpus fingerprint, corpus size, document size, ingest mode/concurrency, coalesce
  window, and batch size;
- embedding backend and model name;
- provider name/version and model URI or artifact path;
- embedding dimension and embedding-space/recipe identity;
- KG, vector, GLiNER/title-extraction, and plugin activation state; and
- SPQ recipe/generation relevant to persisted-index reuse.

For the built-in default used by the smoke fixture, there is no external model file to download or
hash. Its artifact identity is the benchmark binary plus:

```text
provider=Simeon
provider_version=1.0.0
model_uri=simeon://simeon-default
model=simeon-default
embedding_dimension=1024
recipe=simeon-config-v1:char_and_word:3-5:sketch=4096:output=1024:projection=fwht:l2=1
```

Do not substitute an ONNX model, mock provider, or different Simeon encoder recipe under the same
experiment identity.

## Lossless correctness oracle

A valid run satisfies all of the following:

1. Admission failures are zero and stored document count equals generated document count.
2. `observed_post == expected_post`, `observed_embed == expected_embed`, and
   `observed_kg == expected_kg`; over-counts are duplicate work and fail the run.
3. Extraction, embedding, post-ingest, KG, symbol, entity, and title queues are drained with zero
   dropped work; the `WriteCoordinator` flushes with zero commit errors or capacity rejection.
4. Stored vector count equals the emitted chunk-vector count. Every vector has the declared
   dimension, and a deterministic sample of normalized `(document identity, chunk identity,
   embedding bits)` has the same fingerprint across repeats and before/after runs.
5. Keyword, semantic, and graph/hybrid probes succeed after drain. Their fingerprint uses stable
   document/hash or normalized corpus-relative path identities, not transient numeric row IDs.
6. A corpus large enough for PQ records the current vector generation, persists the SPQ snapshot,
   reopens it as reusable, and returns the same top-result fingerprint before and after restart.
7. Content hashes and document/chunk/vector identities are unchanged by an optimization.
8. Shutdown succeeds without an isolated socket, PID, WAL, SHM, queue worker, or lease remaining.

A run that misses an oracle field is incomplete evidence, even if `pipeline_complete` is true.

## Performance evidence

Keep correctness gates separate from optimization KPIs. Record at minimum:

- admission, storage-ready, pipeline-drain, enrichment-ready, and searchability-ready time;
- documents/s and MiB/s;
- content-store, extraction/chunking, metadata transaction, Simeon gather/infer/build-record,
  vector insertion, KG, and post-ingest phase distributions;
- embedding batch p50/p95/p99 and batch sizes;
- queue depth/in-flight high-water marks, DB lock/retry counts, and writer queue/apply time;
- SPQ build/persist/reload time and generation;
- process CPU, peak RSS, WAL growth, and shutdown latency.

Use the same workload for profiling and before/after runs. Reject changes within pooled variance or
with an unplanned identity, quality, tail-latency, RSS, or durability regression.

## Smoke evidence and known gaps

Two isolated release smoke runs at revision `265e5a334ec41b169a592977e3b61bf402cccfe1`
used binary SHA-256 `ffd216da4a580fffcdd669a22729a72e9edc770b97209a83d605838687b33410`.
Both produced corpus fingerprint `fnv1a64:b583b2f47f84f6dd`, the Simeon identity above, exact
post/embed/KG counts of 5/5, zero drops and commit errors, drained queues, successful required
search probes, and a clean shutdown. Artifacts:

- `/tmp/yams-simeon-ingestion-oracle-smoke.json`
- `/tmp/yams-simeon-ingestion-oracle-smoke-repeat.json`

The audit found these gaps, which must be closed before treating the next xplan stamp as the full
baseline:

- `simeon-default` currently disables plugin autoload in the benchmark, and the smoke stage snapshot
  showed `title=false`; therefore the plan's `gliner=on` baseline does not currently prove GLiNER
  title extraction ran.
- JSON omits provider version/model URI, actual embedding dimension/space identity, stored document
  and vector counts, embedding/chunk fingerprints, SPQ generation/reuse, CPU/RSS/WAL, and shutdown
  metrics.
- Semantic and hybrid `top_ids` were five repeated `"0"` values, so they are not a valid result
  identity oracle. Capture stable hashes or normalized corpus-relative paths instead.
- The default 100-document, 1,000-byte plan is below the 256-vector PQ training threshold when each
  document produces one vector; it cannot establish SPQ persistence behavior.
- Millisecond phase counters round the five-document Simeon smoke to zero. Decision runs need a
  larger fixture and/or microsecond/nanosecond timing for hot stages.

Task #42 should extend the existing benchmark/worker contract only where needed to close these gaps,
then capture the three-repeat baseline. Do not weaken a gate to make an arm pass.
