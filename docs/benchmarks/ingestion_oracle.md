# Full Simeon ingestion oracle

This contract defines the correctness and identity gates for optimizing the full YAMS ingestion
pipeline with built-in Simeon embeddings. It complements
`tests/benchmarks/xplan/plans/ingest_pipeline.json`; synthetic text is valid here for throughput
and lifecycle attribution, but not for retrieval-quality claims.

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

Run the existing plan through xplan; do not create a parallel multi-arm shell matrix:

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

The decision fixture retains seed 42 and directory ingestion. Decision evidence uses two
complementary isolated lanes:

1. A combined real-GLiNER lane uses five 1,000-byte documents and proves the exact local model,
   exact post/embed/KG/title accounting, Simeon identity, stable search paths, inference timing,
   and clean lifecycle.
2. A multi-chunk persistence lane disables GLiNER and uses 100 documents of 8,192 target bytes.
   It must produce at least 256 vector rows, train and persist SPQ, reload the persisted generation,
   and preserve exact output fingerprints.

Both lanes require three repeats with identical workload identities. The combined lane must consume
all five KG and title jobs; partial progress is a liveness failure, not a timeout to relax. A product
decision also requires a fresh-process SPQ restart-reuse check.

Any before/after comparison must use the identical generated corpus fingerprint and parameters.
Record the selected values in the run report rather than silently changing this contract.

### Fresh-process restart and replay

The benchmark accepts harness-only `YAMS_BENCH_PHASE=initial|replay|resume` and
`YAMS_BENCH_FIXTURE_ROOT`. An explicit root must be an isolated `yams_e2e_bench_*` directory below
the system temporary directory; the external driver owns that root and must remove it in a
`finally`/signal-cleanup path. Default invocations remain self-cleaning.

A restart gate runs `initial`, `replay`, and a second `replay` in separate benchmark processes over
the same root. Each replay must observe the complete corpus before admission, reuse the persisted
SPQ generation, and preserve exact document/vector counts, embedding bits, direct-vector top-k
identities and score bits, normalized application top-k paths, and clean shutdown. Startup and
post-replay fingerprints must match, as must both replay processes. Application fusion scores may
only be compared exactly within the same persisted fixture because KG edges carry creation times;
across independently timestamped fixtures, compare the exact direct-vector score fingerprint and
normalized application result identities unless the KG timestamp identity is also pinned.

An interruption gate sends termination only to the isolated child during serial admission, requires
normal daemon shutdown, then runs `resume` over the retained partial fixture. Resume must report the
partial startup counts and converge to the exact full document/vector fingerprints without drops,
duplicates, lock errors, writer errors, or remnants.

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
   dimension and only finite values. A deterministic ordering of normalized `(document identity,
   chunk identity, offsets, model identity, embedding bits)` has the same fingerprint across
   repeats and before/after runs. A canonical direct-vector query returns finite top-k scores and an
   exact identity/score-bit fingerprint.
5. Keyword, semantic, and graph/hybrid probes succeed after drain. Their fingerprint uses stable
   document/hash or normalized corpus-relative path identities, not transient numeric row IDs.
   Required score vectors are finite and aligned with those identities.
6. A corpus large enough for PQ records the current vector generation, persists the SPQ snapshot,
   reopens it as reusable, and returns the same direct-vector top-k identity/score fingerprint
   before and after restart.
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

## Evidence storage

Keep generated JSON, CSV, reports, logs, machine-specific paths, binary hashes, and measured run
summaries out of the public source tree. They remain in gitignored `build/benchmarks/**` locally and
are stored in YAMS with `task`, `phase`, `owner`, and `source=evidence` metadata for retrieval and
handoff. This document records only the stable benchmark contract and acceptance gates.
