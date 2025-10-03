# Performance Tuning

Concise guidance to size, configure, and run YAMS efficiently. Tune based on your workload profile: ingest-heavy, query-heavy, or mixed.

## Daemon Metrics & Auto‑Tuning (new)

YAMS includes a centralized TuneAdvisor and a lightweight ResourceTuner that adapt behavior based on accurate daemon metrics:

- Metrics (daemon):
  - CPU utilization derived from /proc deltas (percent of total machine capacity).
  - Memory footprint prefers PSS (from smaps_rollup) and falls back to RSS.
  - Mux backlogs and active connection counts inform server pressure.
- Auto‑tuning loop (daemon):
  - Shrinks worker pools when idle (low CPU, no queued work, zero active connections).
  - Grows conservatively under pressure (CPU high, worker queue depth above threshold, or mux backlog).
- Auto‑embed policy (ingest):
  - Default policy is Idle: newly added documents embed only when the daemon is idle; otherwise embedding is deferred to the background repair coordinator.
  - Policies: Never | Idle (default) | Always. These are defined centrally in TuneAdvisor and used by the RequestDispatcher’s auto‑embedding path.

Notes:
- The doctor output (“Resources: RAM=…, CPU=…”) now reflects improved metrics granularity, so tuning decisions align with what you see in `yams doctor`/`yams status`.
- Background repair respects system load (deferred when busy) and processes in conservative, adaptive batches.

### Embedding Batch Controls (conservative by default)

Embedding generation uses an adaptive DynamicBatcher with these centralized knobs (via TuneAdvisor):

- Safety factor: reserves headroom in token budget; default 0.90.
- Advisory doc cap: optional upper bound on documents per dynamic batch; default unset.
- Inter‑batch pause: optional millisecond pause after each persisted batch; default 0ms.

These reduce sustained CPU pressure during large repairs and improve fairness with concurrent queries.

### Worker Pools & Idle CPU

- CPU worker pool threads run with a longer idle wait to reduce wakeups (default ~250ms tick internally), lowering idle CPU without harming responsiveness.
- Socket server IO workers scale based on connection and mux pressure.

Best practice: rely on the built‑in auto‑tuning first. If you need stricter behavior (e.g., keep the system cold while indexing), disable auto‑embed or increase inter‑batch pauses (see Recipes).

## TL;DR (Quick wins)

- Hardware: prefer NVMe SSDs; allocate fast storage to the data directory.
- Filesystem: use ext4/xfs with `relatime`; ensure sufficient IOPS.
- Concurrency: use moderate parallelism (≈ number of physical cores) for ingest if your version supports it.
- Compression: use zstd for balanced speed/ratio; reserve LZMA for cold archives.
- Deduplication: choose chunk sizes per content type (text vs binaries) to balance dedupe ratio vs index overhead.
- SQLite/WAL: keep WAL enabled; checkpoint during low traffic; use `synchronous=NORMAL` for bulk ingest and `FULL` for strict durability.
- FTS5: index only necessary fields; use appropriate tokenizers; run `optimize` after large ingests.
- Vector search: pick parameters for your recall/latency target; precompute embeddings offline when possible.
- OS limits: raise `nofile` (≥ 16384); monitor CPU, disk latency (p99), and cache hit rates.

## Configuration Quick Reference

TuneAdvisor covers the dynamic knobs, but static configuration still matters. The table below links the most common performance levers to concrete `config.toml` keys so you can codify experiments instead of relying on ad-hoc overrides.

| Area | Config key(s) | Primary effect | When to tweak |
| --- | --- | --- | --- |
| Worker pools | `[performance].num_worker_threads`, `[performance].io_thread_pool_size` | Caps CPU-bound and IO-bound parallelism | Increase for ingest bursts when CPU headroom exists; decrease on constrained hosts to stay within quota |
| Request backlog | `[performance].max_concurrent_operations` | Limits simultaneous daemon tasks | Lower to protect latency-sensitive workloads; raise when long-running ingest jobs block short queries |
| Chunking window | `[chunking].min_chunk_size`, `[chunking].max_chunk_size`, `[chunking].average_chunk_size` | Controls dedupe granularity vs index fan-out | Smaller window for text diffs, larger for large binaries to reduce index pressure |
| Compression | `[compression].algorithm`, `[compression].zstd_level`, `[compression].async_compression` | Balances CPU cost and storage savings | Drop levels to keep ingest real-time; raise during off-peak archival passes |
| Embeddings | `[embeddings].auto_on_add`, `[embeddings].batch_size`, `[embeddings].generation_delay_ms` | Tunes immediate vs deferred embedding cost | Disable auto-on-add for cold ingest pipelines; trim batch size if GPU/CPU memory is tight |
| Vector index | `[vector_database].index_type`, `[vector_database].num_partitions`, `[vector_database].num_sub_quantizers` | Trades recall for query latency and build cost | Start with defaults; increase partitions/sub-quantizers for massive corpora after baseline data collection |
| WAL & durability | `[wal].sync_interval`, `[wal].sync_timeout_ms`, `[wal].enable_group_commit` | Determines fsync cadence | Relax during bulk ingest (higher interval) and tighten once steady-state resumes |

Workflow tip: snapshot the current config (`yams config dump`) before each experiment and commit deltas alongside benchmark notes. This keeps TuneAdvisor highlights, CLI overrides, and long-term configuration changes aligned.

---

## Workload Profiles

- Ingest-heavy
  - Batch files (bulk “add”); increase commit intervals.
  - Lower fsync cost (WAL + `synchronous=NORMAL`) during batch ingest; restore stricter settings after.
  - Defer expensive index maintenance (e.g., FTS optimize) until after ingest.

- Query-heavy
  - Favor higher fsync levels and smaller WAL checkpoint intervals.
  - Increase caches (OS and DB cache) if supported by your version.
  - Optimize query shapes (prefix/suffix search, fields, filters) and avoid unbounded scans.

- Mixed
  - Schedule maintenance (FTS optimize, WAL checkpoint) during off-peak windows.
  - Use conservative concurrency and throttle ingest during peak query hours.

---

## Storage and Filesystem

- Location: keep the YAMS data directory on fast local SSD/NVMe.
- Filesystem options:
  - Mount with `relatime` to reduce metadata writes.
  - Avoid `noatime` if your backup/monitoring depends on access times.
- Device: prefer low-latency storage (check `fio` iodepth=1 latency).
- IO scheduler: `none`/`mq-deadline` (depends on distro/kernel) for NVMe.

---

## Compression

- zstd: good default; tune compression level for speed/ratio (typical 1–6).
- LZMA: higher compression, slower; use for archival/cost reduction, not hot paths.
- Guidance:
  - Hot data: low zstd level (1–3).
  - Warm data: zstd (3–6).
  - Cold data: LZMA (offline compaction/archival if supported).

---

## Deduplication (Rabin Fingerprinting)

- Chunk size impacts:
  - Smaller chunks: better dedupe on small edits; more index overhead.
  - Larger chunks: fewer index entries; lower dedupe on small diffs.
- Suggested targets:
  - Text/documents: smaller median chunk sizes (e.g., 8–16 KiB) for higher dedupe on edits.
  - Binaries/media: larger (e.g., 32–64 KiB) to reduce overhead.
- Keep chunking parameters consistent for comparability and cache locality.

---

## Indexing and Search (FTS5)

- Tokenizer: pick per language/content. For code/docs, a simple tokenizer often outperforms heavy analyzers.
- Prefix indexes: enable only where needed (e.g., prefix=2,3 for short prefixes). Overuse increases index size.
- Fields: index only fields queried; skip large, seldom-used fields.
- Optimize:
  - Run FTS5 `optimize` after bulk ingests to compress segments and improve query speed.
  - Schedule optimize off-peak; it may be IO-heavy.
- Query tips:
  - Use bounded terms and filters to avoid full scans.
  - Prefer exact and prefix matches over broad wildcards where possible.

---

## Vector Search

- Embeddings:
  - Precompute embeddings offline to reduce ingest-time latency.
  - Normalize vectors if your metric requires it (e.g., cosine).
- Index backend parameters (if your version supports ANN structures):
  - HNSW: tune `M`/`efConstruction` (build) and `efSearch` (query) to trade recall vs latency.
  - IVF/PQ: select `nlist`/`nprobe` and quantization settings based on dataset size and latency targets.
- Recall/latency:
  - Start with moderate recall targets; measure p50/p95 latency; increase search params if recall is insufficient.
- Hybrid:
  - If using hybrid (keyword + vector), filter with keyword first to reduce vector candidates.

### Vector & Embedding Tuning (daemon)

- Auto‑embedding policy (TuneAdvisor):
  - Idle (default): embed on add only when CPU is low and there are no active connections; otherwise defer to the repair coordinator.
  - Never: always defer; run `yams repair --embeddings` or let the background repair catch up when idle.
  - Always: embed immediately (use sparingly; may increase ingest CPU).
- Embedding batches (TuneAdvisor):
  - Safety factor ≈ 0.90 default; lower to be more conservative on memory/CPU (e.g., 0.75).
  - Advisory doc cap: cap per‑batch documents to smooth CPU usage on heterogeneous inputs.
  - Inter‑batch pause: add a small delay (e.g., 50–100ms) after each persisted batch to avoid prolonged CPU plateaus.
  
Operational guidance:
- For shared hosts or during peak hours: Idle policy + small inter‑batch pause.
- For bulk backfills off‑hours: Always policy + higher safety to maximize throughput while keeping stability.

---

## Concurrency, Batching, and Commit Strategy

- Concurrency:
  - Set worker threads roughly to physical core count; avoid over-subscription (IO-bound phases may benefit from a few extra).
- Batching:
  - Ingest in batches (e.g., 100–1000 docs) to amortize transaction overhead.
  - Group small files and stdin streams when possible.
  - Let the embedding DynamicBatcher pick doc batches based on token budget; use safety/doc‑cap/pause to shape CPU profile.
- Transactions:
  - Larger transactions speed ingest but increase rollback scope; choose a safe middle ground for your failure model.

---

## SQLite/WAL and Checkpointing

- WAL: keep enabled for write-concurrent workloads.
- `synchronous`:
  - Ingest windows: `NORMAL` to speed writes.
  - Steady-state: `FULL` for maximum durability.
- Checkpointing:
  - Use passive or truncate checkpoints during low traffic.
  - Monitor WAL size; avoid unbounded growth.
- Page size and cache:
  - If exposed via config, choose a page size that aligns with storage (commonly 4–8 KiB).
  - Increase page/cache size if memory allows; validate improved hit rates.

---

## Memory and Caches

- OS page cache: ensure adequate free memory for the working set.
- Application/DB cache (if configurable): size to fit hot indexes and frequent doc blocks.
- Monitor OOM risks; set reasonable limits in containers.

---

## Maintenance Tasks

- FTS5 optimize after bulk ingest.
- Periodic WAL checkpoint.
- Optional compaction/cleanup jobs if your version exposes them (e.g., removing tombstones, reclaiming space).
- Validate integrity:
  - Periodic verification on sample sets.
  - Restore drills from backups.

---

## OS Tuning

- File descriptors: `nofile >= 16384` (increase for larger concurrency).
- CPU scaling: use performance governor for consistent latency (servers).
- Networking (if serving APIs): moderate backlog sizes; keep-alive tuned to traffic patterns.
- Container limits: set `--cpus` and `--memory` to avoid host contention.

---

## Benchmarking Methodology

1. Prepare representative datasets (size, file types, update rates).
2. Separate phases: ingest, index maintenance, query (cold/warm cache).
3. Metrics to capture:
   - Throughput: docs/s, bytes/s (ingest).
   - Latency: p50/p95/p99 for queries; tail latencies matter.
   - Resource: CPU, RSS, IO wait, disk latency (p95+), WAL size.
4. Control variables:
   - Pin CPU frequency/governor.
   - Warm-up runs before measuring steady-state.
5. Repeat and average; keep scripts and configs under version control.

---

## Troubleshooting

- Slow ingest:
  - Reduce fsync cost temporarily (WAL + `synchronous=NORMAL`).
  - Increase batch size and transaction size.
  - Check disk latency; confirm NVMe performance.
- Slow queries:
  - Run FTS optimize; reduce prefix scope; index only needed fields.
  - Check cache sizing; ensure hot set fits in memory.
  - For vector search, increase search params or improve pre-filtering.
- WAL growth:
  - Schedule checkpoints; ensure consumers aren’t holding readers open.
- Memory pressure:
  - Reduce caches; lower concurrency; ensure OS has headroom.
  - Lower embedding batch safety and increase inter‑batch pauses; prefer Idle auto‑embed policy.

---

## References

- Admin → [Configuration](./configuration.md)
- Admin → [Vector Search Tuning](./vector_search_tuning.md)
- Architecture → [Search System](../architecture/search_system.md)
- Architecture → [Vector Search Architecture](../architecture/vector_search_architecture.md)
