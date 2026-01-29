# ADR-0002: Vector Subsystem V2 - Clean Room Rewrite

- Status: Accepted
- Date: 2026-01-06
- Decision Makers: @trvon
- Links: Epic yams-47l2

## Context

The current vector subsystem (`src/vector/sqlite_vec_backend.cpp`) has accumulated significant technical debt:

1. **Dual-table schema** - Embeddings in vec0 virtual table, metadata in regular table requiring manual rowid synchronization
2. **HNSW bolted-on** - In-memory ID mappings (`hnsw_id_to_chunk_`, `chunk_to_hnsw_id_`) that grow unbounded
3. **No HNSW persistence** - Rebuilds O(n) on every startup
4. **Manual JSON serialization** - Hand-rolled, fragile, no proper escaping
5. **Dead code** - Prepared statements struct never used, hollow abstractions (LanceDB, InMemory backends)
6. **Global mutex** - Single `std::mutex` for all operations, no read/write separation

The `sqlite-vec-cpp` library (`third_party/sqlite-vec-cpp/`) provides:
- `HNSWIndex<T, Metric>` with thread-safe operations (shared_mutex)
- `hnsw_persistence.hpp` for SQLite shadow table save/load
- `CosineMetric<float>` for distance calculations
- Soft deletion with `remove()` and `compact()`

## Decision

### 1. New Schema (Unified Table)

Replace dual-table schema with a single unified table:

```sql
-- Main table: stores embeddings + metadata together
CREATE TABLE vectors (
    rowid INTEGER PRIMARY KEY,           -- Auto-increment, used as HNSW node ID
    chunk_id TEXT UNIQUE NOT NULL,       -- UUID for chunk (indexed)
    document_hash TEXT NOT NULL,         -- SHA-256 of source document
    embedding BLOB NOT NULL,             -- float32 array as blob
    content TEXT,                        -- Original text chunk
    start_offset INTEGER DEFAULT 0,
    end_offset INTEGER DEFAULT 0,
    metadata TEXT,                       -- JSON object
    model_id TEXT,
    model_version TEXT,
    embedding_version INTEGER DEFAULT 1,
    content_hash TEXT,
    created_at INTEGER,                  -- Unix timestamp
    embedded_at INTEGER,
    is_stale INTEGER DEFAULT 0,
    level INTEGER DEFAULT 0,             -- EmbeddingLevel enum
    source_chunk_ids TEXT,               -- JSON array
    parent_document_hash TEXT,
    child_document_hashes TEXT           -- JSON array
);

CREATE INDEX idx_vectors_chunk_id ON vectors(chunk_id);
CREATE INDEX idx_vectors_document_hash ON vectors(document_hash);
CREATE INDEX idx_vectors_model ON vectors(model_id, model_version);

-- HNSW persistence (managed by sqlite-vec-cpp)
-- Created by create_hnsw_shadow_tables(db, "main", "vectors", &err)
-- vectors_hnsw_meta: key TEXT PRIMARY KEY, value BLOB
-- vectors_hnsw_nodes: node_id INTEGER PRIMARY KEY, data BLOB
```

### 2. ID Management

**No in-memory maps.** Use SQLite rowid directly as HNSW node ID:

```cpp
// Insert returns rowid
int64_t rowid = insertVectorAndGetRowid(record);

// Insert into HNSW with rowid
hnsw_index_.insert(static_cast<size_t>(rowid), embedding_span);

// Search returns rowids, look up chunk_id from SQLite
auto results = hnsw_index_.search(query_span, k, ef_search);
for (auto& [rowid, distance] : results) {
    auto record = getVectorByRowid(rowid);
    // ...
}
```

**chunk_id to rowid mapping:** Use SQLite index lookup (fast):
```sql
SELECT rowid FROM vectors WHERE chunk_id = ?
```

### 3. HNSW Management

**Lazy loading:** Load HNSW on first search, not on `initialize()`:

```cpp
Result<std::vector<VectorRecord>> searchSimilar(...) {
    ensureHnswLoaded();  // Lazy load
    // ... search
}
```

**Incremental persistence:** Save after batch operations:

```cpp
Result<void> insertVectorsBatch(const std::vector<VectorRecord>& records) {
    // ... insert into SQLite
    // ... insert into HNSW
    
    // Checkpoint if threshold reached
    if (pending_inserts_ >= kCheckpointThreshold) {
        saveHnswCheckpoint();
        pending_inserts_ = 0;
    }
}
```

**Soft deletes:** Use HNSW remove() + compact():

```cpp
Result<void> deleteVector(const std::string& chunk_id) {
    auto rowid = getRowidByChunkId(chunk_id);
    hnsw_index_.remove(rowid);  // Soft delete
    // Delete from SQLite
    
    if (hnsw_index_.needs_compaction()) {
        hnsw_index_ = hnsw_index_.compact();
        saveHnswFull();
    }
}
```

### 4. Thread Safety

Use `std::shared_mutex` for read/write separation:

```cpp
class SqliteVecBackendV2 : public IVectorBackend {
    mutable std::shared_mutex mutex_;
    
    // Read operations: shared lock
    Result<std::optional<VectorRecord>> getVector(...) {
        std::shared_lock lock(mutex_);
        // ...
    }
    
    // Write operations: exclusive lock
    Result<void> insertVector(...) {
        std::unique_lock lock(mutex_);
        // ...
    }
};
```

### 5. JSON Handling

Use `nlohmann::json` (already in codebase) instead of hand-rolled serialization:

```cpp
#include <nlohmann/json.hpp>

std::string serializeMetadata(const std::map<std::string, std::string>& meta) {
    return nlohmann::json(meta).dump();
}

std::map<std::string, std::string> deserializeMetadata(const std::string& json_str) {
    return nlohmann::json::parse(json_str).get<std::map<std::string, std::string>>();
}
```

### 6. Migration Strategy

1. Detect schema version via presence of tables
2. If old schema (doc_embeddings vec0 + doc_metadata), migrate:
   - Read all records from old tables
   - Insert into new unified table
   - Build new HNSW index
   - Drop old tables (or rename for rollback)
3. Store schema version in a meta table

### 7. File Structure

New files:
- `include/yams/vector/sqlite_vec_backend_v2.h`
- `src/vector/sqlite_vec_backend_v2.cpp`

Update:
- `src/vector/vector_backend.cpp` - Factory returns V2

Remove (after V2 stable):
- `src/vector/sqlite_vec_backend.cpp`
- `include/yams/vector/sqlite_vec_backend.h`
- `src/vector/hnsw_backend.cpp`
- `include/yams/vector/hnsw_backend.h`

## Consequences

### Positive

- **Simpler schema**: Single table, no rowid synchronization
- **Fast startup**: HNSW loads from disk, not rebuilt
- **Better concurrency**: shared_mutex allows parallel reads
- **No memory leaks**: No unbounded in-memory ID maps
- **Proper JSON**: nlohmann::json handles escaping correctly
- **Incremental persistence**: Survives crashes with minimal data loss

### Negative

- **Migration required**: Existing databases need one-time migration
- **Temporary storage**: Migration needs 2x storage briefly
- **Breaking change**: V2 schema incompatible with V1 (handled by migration)

### Neutral

- **Same public API**: IVectorBackend and VectorDatabase unchanged
- **Same search quality**: HNSW algorithm unchanged

## Notes

### HNSW Configuration (Recommended Defaults)

| Corpus Size | M | ef_construction | ef_search |
|-------------|---|-----------------|-----------|
| <10K | 16 | 200 | 100 |
| 10K-100K | 16 | 200 | 100 |
| 100K-1M | 32 | 200 | 200 |
| >1M | 48 | 300 | 300 |

### Performance Targets

- Insert: <1ms per vector (batch: <100us amortized)
- Search: <10ms for k=10 at 100K vectors
- Startup: <1s for 100K vectors (HNSW load from disk)
- Memory: O(1) auxiliary (no in-memory ID maps)
