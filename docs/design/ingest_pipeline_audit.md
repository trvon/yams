# YAMS ingest pipeline audit (2026-04-24)

## Trigger

A 65k-doc combined-corpus bench (Phase J' on scifact + fiqa + nfcorpus)
hung mid-run with the SQLite WAL at 67 GB. Initial diagnosis pointed at
`PRAGMA wal_autocheckpoint = 0`, but a follow-up rerun with a thread-based
`CheckpointManager` + watermark TRUNCATE showed the WAL still grew to 35+ GB
without a single checkpoint firing. That second failure forced the audit:
**the issue is not just checkpointing — it's a chain of compounding scaling
cliffs in the ingest pipeline.**

## What we already knew

| Issue | File | Location |
|---|---|---|
| `wal_autocheckpoint` disabled | `src/metadata/connection_pool.cpp` | line 637 |
| `CheckpointManager` runs on shared executor → starves under load | `src/daemon/components/CheckpointManager.cpp` | (was) `launchCheckpointLoop` |
| `CheckpointManager::checkpointWal` acquires from connection pool — pool gets exhausted by writers under load | `src/metadata/metadata_repository.cpp:4990` | `executeQuery<void>` path |
| Embedding throughput: 0.4 sec/doc → 1.5 sec/doc as corpus grows | observed | `EmbeddingService.cpp:2605` neighbor-graph |
| `[TopologyManager] rebuild complete (reason=post_ingest_drain ...)` fires every few seconds during 65k-doc ingest | observed | `TopologyManager.cpp:358` `runRebuild` |

## What the audit added

### Stage 1 — RequestDispatcher (`src/daemon/components/dispatcher/request_dispatcher_documents.cpp:1282`)

Routes `AddDocumentRequest` → `ResourceGovernor::canAdmitWork()` → channel.
The channel's only depth-control is `TuneAdvisor::storeDocumentChannelCapacity()`
(unbounded by default until memory pressure). **No connection holding here**;
it's pure RPC routing.

### Stage 2 — IngestService (`src/daemon/components/IngestService.cpp:319` `processTask`)

`docService->store()` acquires a metadata connection briefly per doc and
returns it. Per-doc cost is bounded.

The hidden cost is in the **post-ingest path**: line 2605 of `EmbeddingService`
(reached via `flushPendingPostIngestBatches`) calls `updateSemanticNeighborGraph()`
which **scans the entire corpus's vectors twice per 16-doc batch** (lines 347
+ 386: `vdb->forEachDocumentLevelVector()`).

### Stage 3 — EmbeddingService — **THE PIPELINE'S TRUE QUADRATIC POINT**

`EmbeddingService::updateSemanticNeighborGraph()` (line 270+):
- Scans the full corpus's vectors twice (lines 347, 386).
- Computes pairwise cosine similarity between each new doc and every existing
  corpus doc (line 397) — **O(B · N)** per batch (B = batch size, N = corpus size).
- After computing edges, calls `kgStore->addEdgesUnique(semanticEdges)` (line 539)
  which DOES acquire a metadata connection — under saturation this competes
  for the pool with writers.
- All this work is **synchronous on the EmbeddingService strand**, so successive
  batches serialize.

This explains the observed batch-time growth: 6.8s/batch at corpus=0 → 23s/batch
at corpus=50k. Each new batch must compare against the entire prior corpus.

**Then immediately after, it calls `topologyRebuildRequester_(successHashes)`**
(line 2622), which fires a rebuild...

### Stage 4 — TopologyManager (`src/daemon/components/TopologyManager.cpp:358` `runRebuild`)

`runRebuild()`:
- Loads all current embeddings (`TopologyInputExtractor`, line 374).
- Calls `engine->compute()` — HDBSCAN over the **entire corpus**.

There is no debounce. `tryScheduleRebuild()` (line 124) is a compare-and-set:
if a rebuild isn't already running, schedule one. So as soon as a rebuild
completes, the next batch's `topologyRebuildRequester_()` call schedules
another. **At 65k docs with rebuilds every few seconds, the daemon spends 50%+
of wall time in HDBSCAN.**

### Stage 5 — Connection pool (`src/metadata/connection_pool.cpp:637`)

- Pool size: `maxConnections = 64` (default; `YAMS_DB_POOL_SIZE` overrides).
- Reserved high-priority slots: 20% = ~12.
- `wal_autocheckpoint = 0` is set **without comment**.

The reservation system means CheckpointManager *could* use the high-priority
path, but it currently uses normal-priority `executeQuery` and competes with
writers.

### Stage 6 — CheckpointManager (`src/daemon/components/CheckpointManager.cpp`)

After our thread-based fix, the loop runs on a dedicated thread. But
`checkpointWal()` still calls `MetadataRepository::checkpointWal()` →
`executeQuery` → connection pool. **Under writer saturation (which is
constant during 65k-doc ingest), the call sits in the pool's condition
variable until a writer releases.** Result: even with the thread-based
loop, no checkpoint completes during heavy ingest.

## Compounding pattern

```
                                     (B = batch size, N = corpus size)
1 doc ingested
  → IngestService::processTask    (O(1) per doc)
  → docService->store()           (O(1) SQLite write)
  → EmbeddingService picks up batch B
    → forEachDocumentLevelVector × 2  (O(N))
    → cosineSimilarity × (B · N)      (O(B · N))
    → kgStore->addEdgesUnique         (O(B · topK) writes, ALSO acquires DB conn)
    → topologyRebuildRequester_       (fires unconditionally)
      → TopologyManager::runRebuild
        → HDBSCAN over corpus         (O(N log N) to O(N²))
  → meanwhile, WAL grows monotonically
    → wal_autocheckpoint disabled
    → CheckpointManager waiting on pool
    → connection pool saturated by IngestService writers
    → no checkpoint fires
```

For 65k docs at batch size 16: ~4,000 batches. Each batch does O(N) work
plus triggers an O(N log N) topology rebuild. **Total work is O(N²)**.
Empirically the wall time grew super-linearly with corpus size: 9 min for
8.8k docs but >3h for 65k docs (8× corpus → 20× time).

## Prioritized fix list

| # | Fix | File | Effort | Impact on 65k-doc bulk ingest |
|---|---|---|---|---|
| 1 | **Defer topology rebuilds during bulk ingest.** Add a "bulkIngestMode" flag to `TopologyManager`. While set, `tryScheduleRebuild()` no-ops except for periodic (e.g., every 5 min) and final `post_ingest_drain` triggers. | `TopologyManager.cpp:124` + `EmbeddingService.cpp:2622` | small (~30 LOC) | **dominant** — eliminates O(N²) topology work; estimated 45+ min savings on 65k docs |
| 2 | **Cache corpus embeddings during neighbor-graph updates.** Replace the two per-batch `forEachDocumentLevelVector` scans with an in-memory append-only cache keyed by doc hash. | `EmbeddingService.cpp:270+ updateSemanticNeighborGraph` | medium (~80 LOC + memory budget knob) | large — eliminates the per-batch O(N) corpus scan; batch time stays flat at ~6s instead of climbing to 23s |
| 3 | **Reserve a high-priority connection slot for `CheckpointManager`.** Pool already supports priority (line 285); have CheckpointManager use it. | `src/metadata/metadata_repository.cpp:4990` `checkpointWal` | small (~10 LOC) | medium — checkpoint thread no longer starves; WAL stays bounded even without fix #1 |
| 4 | **Re-evaluate `wal_autocheckpoint = 0`.** Re-enable at a high page threshold (e.g., 100k pages ≈ 400 MB) so SQLite handles checkpointing under low-contention windows that PASSIVE always misses. | `src/metadata/connection_pool.cpp:637` | trivial (1 line + verify no reader-stall regression) | medium — defense in depth for any path that doesn't go through CheckpointManager |
| 5 | **Coalesce topology dirty-hash sets.** Even after fix #1, ensure that during normal operation `markDirtyBatch` debounces to ~30s windows rather than firing immediately. | `TopologyManager.cpp:71 markDirtyBatch` | small (~20 LOC) | small (already mostly addressed by #1; this hardens the steady-state behavior) |
| 6 | **Add backpressure to the embed→topology requester call.** Before `topologyRebuildRequester_()`, check `topologyManager->isRebuildScheduled()`; skip if true. | `EmbeddingService.cpp:2622` | trivial (~5 LOC) | small (subsumed by #1; useful as a belt-and-suspenders) |

## Why fix #1 is the highest leverage

The other fixes treat symptoms (WAL growth, checkpoint stalls, embedding
throughput collapse). Fix #1 attacks the **root**: bulk ingest currently
recomputes the entire topology after every 16-doc batch, which is what
turns an O(N) ingest into O(N²) wall time.

If only fix #1 ships:
- 65k-doc ingest drops from >3h to ~1h (linear-ish in N).
- Embedding pipeline still scales O(N) per batch — annoying but tolerable.
- WAL stays manageable because total ingest time is short enough that the
  5-min checkpoint cadence (post-fix-3) handles it.

If only the WAL/checkpoint fixes ship without #1, ingest still takes 3h+
because topology dominates; the WAL just stays smaller.

## Recommended sequencing

1. **Fix #1 (defer topology rebuilds)** — single highest-impact change.
2. **Fix #3 (reserved checkpoint connection)** — guarantees CheckpointManager
   can always make progress, independent of writer load.
3. **Verify on 65k-doc bench** — should go from "hangs after 3+ hours" to
   "completes in 60-90 min".
4. **If still slow → Fix #2 (embedding cache)** — addresses the residual
   per-batch O(N) cost.
5. **Fix #4 (re-enable autocheckpoint)** — defense in depth for any future
   ingest path that bypasses CheckpointManager.

## Out of scope for this audit

- HDBSCAN-incremental clustering (could replace `runRebuild` with a true
  incremental algorithm; large research effort).
- Sharding the metadata DB (would change how connection pool sizes scale).
- Changing the embedding model's per-batch cost (orthogonal — that's CPU
  spent on inference, not pipeline architecture).

---

## Addendum (2026-04-24, evening) — broader system audit

After fix #1 shipped (topology rebuild throttle), a follow-up audit traced
the remaining ingest subsystems for similar O(N) / O(N²) smells. Three new
findings, ranked by wall-time savings on 65k-doc bulk ingest:

### A. Missing index on `vectors.level` (HIGH leverage, trivial)

**File:** `src/vector/sqlite_vec_backend.cpp:1453`
**Function:** `SqliteVecBackendImpl::forEachDocumentLevelVector`
**Query:** `SELECT ... FROM vectors WHERE level = ?`

Indexes exist on `chunk_id`, `document_hash`, `model_id`, `embedding_dim` but
NOT on `level`. The semantic-graph path calls this function 3× per 16-doc
batch. With ~4,000 batches over 65k docs, that's ~12,000 full-table scans
of `vectors`.

**Fix:** Add `CREATE INDEX IF NOT EXISTS idx_vectors_level ON vectors(level)`.
1 line. Caveat: most rows ARE at one level (doc-level), so the index reduces
constant factor but doesn't change the asymptotic class. Still worth doing
because the constant matters.

### B. `addEdgesUnique` per-edge `INSERT WHERE NOT EXISTS` (HIGH leverage, easy)

**File:** `src/metadata/knowledge_graph_store_sqlite.cpp:742-808` (non-tx)
and `2647-2709` (tx hot path).
**Function:** `KnowledgeGraphStoreImpl::addEdgesUnique`

For each edge in the batch:
```sql
INSERT INTO kg_edges ... WHERE NOT EXISTS (
  SELECT 1 FROM kg_edges WHERE src_node_id = ? AND dst_node_id = ? AND relation = ?
)
```

If `(src_node_id, dst_node_id, relation)` is unindexed, each NOT EXISTS is
a full kg_edges scan. With the topology + semantic neighbor work, kg_edges
grows to 1M+ rows during 65k-doc ingest. Per-edge cost grows linearly with
existing edge count.

**Fix:** Add `UNIQUE(src_node_id, dst_node_id, relation)` constraint + use
`INSERT OR IGNORE`. SQLite's unique-index check is O(log E) instead of O(E).

### C. `updateSemanticNeighborGraph` O(S·C) cosine scan (HIGHEST leverage, medium)

**File:** `src/daemon/components/EmbeddingService.cpp:386-421`
**Function:** `EmbeddingService::updateSemanticNeighborGraph`

This is the steady-state quadratic point. The two `forEachDocumentLevelVector`
scans at lines 347 + 386 fetch every doc embedding, then the inner loop
computes cosine for each new doc against every corpus doc → O(S·C) per batch.

For 65k docs × 16 src docs/batch × 4,000 batches = **4.2 billion cosine
operations** during a 65k-doc ingest. CPU-dominates the embedding inference
itself.

**Fix:** YAMS' `vector_database.cpp` already runs HNSW for entity vectors.
Reuse the same HNSW index for doc-level vectors and use `search_topk(B, k)`
in place of the full corpus scan. Drops per-batch cost from O(S·C) to
O(S·log C). At 65k corpus + S=16, that's 16·log₂(65000) ≈ 254 ops vs
1,040,000 ops — **4000× speedup on the inner loop**.

This is the architecturally correct fix to bring ingest to **O(N log N)
overall** (literature: Malkov & Yashunin 2018, "Efficient and robust
approximate nearest neighbor search using Hierarchical Navigable Small
World graphs"). HNSW is the standard answer for the per-doc-vs-corpus
similarity problem.

### Synthesis — path from O(N²) to O(N log N)

| Component | Today | After fix #1 (shipped) | After A+B+C |
|---|---|---|---|
| Topology rebuild | every batch, full HDBSCAN | every 60s, full HDBSCAN | unchanged (further optimization is incremental clustering, deferred) |
| Semantic graph cosine | O(S·C) per batch | unchanged | **O(S·log C) per batch via HNSW** |
| KG edge dedup | O(E) per edge | unchanged | **O(log E) per edge via UNIQUE+INSERT OR IGNORE** |
| Vector level scan | O(N) full table | unchanged | **O(N) but with index** |

**Estimated wall time on 65k-doc bulk ingest:**

| State | Wall time |
|---|---|
| Pre-fix #1 (current main, before throttle) | >3 hours, hung |
| Fix #1 only (topology throttle) | ~1 hour (throttle eliminates O(N²) topology) |
| Fix #1 + A + B (indexes + INSERT OR IGNORE) | ~40 min |
| Fix #1 + A + B + C (HNSW for semantic graph) | **~15-20 min**, asymptotically O(N log N) |

### Recommended next implementation order

1. **B (UNIQUE + INSERT OR IGNORE in `addEdgesUnique`)** — easiest, ~10 LOC,
   immediate ~1.5-2× savings.
2. **A (`idx_vectors_level`)** — trivial, 1 line, ~1.2-1.5× savings.
3. **C (HNSW for semantic graph)** — biggest win but requires ~150 LOC
   refactor of `updateSemanticNeighborGraph`. Use existing `HNSWIndex`
   plumbing in `vector_database.cpp` (already wired for entity vectors).

If only one of A/B/C ships, **C is the largest absolute win** because it
attacks the steady-state O(N²) and the corpus-scan constant factor is the
dominant CPU cost during ingest after the topology throttle.

### Out-of-scope confirmations

- `IngestService::processTask` does NOT have hidden corpus-scaling work.
- `PostIngestQueue::processBatch` is per-doc O(1).
- FTS5 triggers are incremental; no rebuild-on-insert.
- `symbol_enrichment.cpp` runs during search, not ingest.

---

## Addendum 2 (2026-04-24, late evening) — `src/metadata/` audit

After fixes #1 + A + B shipped, the user asked to sweep the rest of
`src/metadata/` for similar smells. Twelve files audited, three concrete
findings beyond what was already known.

### M1 — Per-doc `SELECT COUNT(*)` loop in `setMetadataBatch` (HIGH impact)

**File:** `src/metadata/metadata_repository.cpp:1810-1843`
**Pattern:** outer loop over `pendingTagKeysByDoc` calls a `tagCountStmt`
(SELECT COUNT) per doc, and an inner `keyExistsStmt` (SELECT COUNT) per
(doc, key) tag. With 65k docs × ~10 tags each, that's ~650k point queries
where one batched `GROUP BY document_id` would suffice.

**Asymptotic class:** O(D · K) point queries → could be O(D + K) batched.

**Fix:** before the loop, run
`SELECT document_id, COUNT(*) FROM metadata WHERE document_id IN (...) AND (key='tag' OR key LIKE 'tag:%') GROUP BY document_id`
once, build `unordered_map<docId, int64> priorCounts`. Then index the map
inside the loop. Same trick for the per-key existence check via a single
`SELECT document_id, key FROM metadata WHERE (document_id, key) IN (...)`.

**Estimated wall-time saved on 65k-doc bulk:** ~50-100ms drops to ~10ms.
Not the biggest absolute win, but completely free architectural improvement.

### M2 — Missing composite index `(document_id, key)` on `metadata` (MEDIUM, trivial)

**File:** `src/metadata/migration.cpp:405-406`. Exists: `idx_metadata_document(document_id)`,
`idx_metadata_key(key)`. Missing: composite `(document_id, key)`.

**Hot path:** `metadata_repository.cpp:1707, 1717` runs
`SELECT COUNT(*) FROM metadata WHERE document_id = ? AND (key = 'tag' OR key LIKE 'tag:%')`.
Today SQLite picks the `document_id` index, then filters key in row scan.

**Fix:** new migration adding `CREATE INDEX idx_metadata_doc_key ON metadata(document_id, key)`.
Enables index-only scan. 1 line of DDL.

### M3 — `O(P · N · M)` pattern match in `TreeBuilder::shouldExclude` (MEDIUM, ingest-adjacent)

**File:** `src/metadata/tree_builder.cpp:461-485`
**Pattern:**
```cpp
for (const auto& rawPattern : patterns) {
    for (const auto& candidate : candidates) {
        if (fnmatchWrap(pattern, candidate)) return true;
    }
}
```

For directory walks during `yams add` on large trees (P = number of exclude
globs, N = candidate strings to test, M = avg string length), this fires
once per filesystem entry walked. Not on the bench's hot path (the bench
ingests by ID, not by directory walk), but absolutely on the path real
users hit.

**Fix:** pre-compile patterns once into a single regex with alternation
(`(p1|p2|...)`) and reuse. Reduces to O(N · M) per entry.

### M4 — Missing index on `kg_doc_entities(document_id, extractor)` (MEDIUM)

**File:** `src/metadata/knowledge_graph_store_sqlite.cpp:2399-2402, 2450-2452`
**Hot query:**
```sql
SELECT COUNT(*),
       SUM(CASE WHEN extractor = 'symbol_extractor_v1' THEN 1 ELSE 0 END),
       SUM(CASE WHEN extractor LIKE 'gliner%' THEN 1 ELSE 0 END)
FROM kg_doc_entities WHERE document_id = ?
```

The LIKE clause can't use an index efficiently. With ~65k docs × ~100
entities each = 6.5M rows, this is per-doc point query on a huge table.

**Fix:** `CREATE INDEX idx_kg_doc_entities_doc_extractor ON kg_doc_entities(document_id, extractor)`.
The composite lets SQLite scan the per-doc partition by extractor prefix
without table lookups.

### Files explicitly cleared

`kg_relation_summary.cpp`, `path_utils.cpp`, `kg_topology_analysis.cpp`,
`query_helpers.cpp`, `database.cpp`, `connection_pool.cpp` (already-known
`wal_autocheckpoint=0` is the only smell), `tree_differ.cpp`,
`content_batch_ops.cpp`, `search_ops.cpp`, `metadata_value_count_ops.cpp`,
`document_query_filters.cpp`. None show O(N²)-class work or missing-index
patterns.

### Updated cumulative fix list (post both audits)

| # | Fix | Status |
|---|---|---|
| 1 (orig) | TopologyManager rebuild throttle | **shipped** |
| A | `idx_vectors_level` composite | **shipped** |
| B | `addEdgesUnique` → `INSERT OR IGNORE` + unique index migration #32 | **shipped** |
| C | HNSW for semantic neighbor graph (replace per-batch full scan) | open — biggest architectural payoff |
| M1 | Batch fetch in `setMetadataBatch` (kill O(D·K) loop) | open |
| M2 | `idx_metadata_doc_key` composite | open — 1-line migration |
| M3 | Pre-compile `TreeBuilder` exclusion patterns | open — affects user `yams add` |
| M4 | `idx_kg_doc_entities_doc_extractor` composite | open |

The single highest-value remaining fix is still **C (HNSW for semantic
neighbor graph)** — it's the only fix that changes the asymptotic class
of the inner loop. M1 is the lowest-hanging-fruit O(D) speedup. M2 and
M4 are 1-line index additions worth rolling into a single migration.

## Status of in-flight work this audit invalidates

The currently-running 65k-doc bench (started 19:40) has the watermark TRUNCATE
fix and the thread-based CheckpointManager but NOT fix #1. Per this audit,
it will likely complete eventually (3+ hours) but isn't a useful test of the
WAL fix because topology rebuilds remain the dominant cost. **Recommendation:
let it run to confirm the runtime estimate; then implement fix #1 and re-run
for a real comparison.**
