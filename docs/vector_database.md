# Vector Database Integration Guide

## Overview
YAMS uses [sqlite-vec](https://github.com/asg017/sqlite-vec) for vector similarity search. This document covers the implementation details and best practices for working with the vector database.

## Architecture

### Two-Table Design
The vector database uses a two-table architecture to separate vector data from metadata:

1. **`doc_embeddings`** - Virtual table (vec0) storing only the embedding vectors
2. **`doc_metadata`** - Regular SQLite table storing document metadata

```sql
-- Virtual table for vectors
CREATE VIRTUAL TABLE doc_embeddings USING vec0(embedding float[384]);

-- Metadata table with foreign key to virtual table
CREATE TABLE doc_metadata (
    rowid INTEGER PRIMARY KEY,
    document_hash TEXT NOT NULL,
    chunk_id TEXT UNIQUE NOT NULL,
    chunk_text TEXT,
    model_id TEXT,
    metadata TEXT,
    created_at INTEGER DEFAULT (unixepoch()),
    FOREIGN KEY(rowid) REFERENCES doc_embeddings(rowid)
);
```

## Important Implementation Details

### Vector Format
**Critical**: sqlite-vec expects vectors in **JSON string format**, not binary BLOBs.

```cpp
// CORRECT: JSON string format
std::stringstream vec_json;
vec_json << "[";
for (size_t i = 0; i < embedding.size(); ++i) {
    if (i > 0) vec_json << ", ";
    vec_json << embedding[i];
}
vec_json << "]";
sqlite3_bind_text(stmt, 1, vec_json.str().c_str(), -1, SQLITE_TRANSIENT);

// WRONG: Binary BLOB format (causes hangs!)
std::vector<uint8_t> blob = vectorToBlob(embedding);
sqlite3_bind_blob(stmt, 1, blob.data(), blob.size(), SQLITE_STATIC);
```

### Insertion Pattern
Always specify the rowid when inserting into vec0 tables:

```sql
-- Correct: Specify rowid (NULL for auto-increment)
INSERT INTO doc_embeddings (rowid, embedding) VALUES (NULL, '[1.0, 2.0, ...]');

-- Wrong: Omitting rowid can cause issues
INSERT INTO doc_embeddings (embedding) VALUES ('[1.0, 2.0, ...]');
```

### Transaction Management
- Use explicit transactions for batch operations
- Ensure mutex is held throughout the operation
- Avoid nested mutex locks (causes deadlock)

```cpp
// Correct: Single lock for entire batch
std::lock_guard<std::mutex> lock(mutex_);
executeSQL("BEGIN TRANSACTION");
for (const auto& record : records) {
    insertVectorInternal(record, false); // false = don't manage transaction
}
executeSQL("COMMIT");
```

### Duplicate Handling
The implementation supports upsert behavior:
1. Check if chunk_id exists in metadata table
2. If exists, update both vector and metadata
3. If not exists, insert new record

## Common Issues and Solutions

### Issue 1: Test Hangs During Insert
**Cause**: Using binary BLOB format instead of JSON string format  
**Solution**: Convert vectors to JSON strings before binding

### Issue 2: Mutex Deadlock
**Cause**: Nested mutex locks when calling internal methods  
**Solution**: Internal methods should not acquire locks; callers must hold lock

### Issue 3: UNIQUE Constraint Violations
**Cause**: Attempting to insert duplicate chunk_ids  
**Solution**: Implement upsert logic checking for existing records

### Issue 4: SQLITE_STATIC vs SQLITE_TRANSIENT
**Cause**: Using SQLITE_STATIC with temporary data that goes out of scope  
**Solution**: Always use SQLITE_TRANSIENT for safety

## Testing

### Basic Insert Test
```cpp
VectorRecord record;
record.chunk_id = "test_chunk_1";
record.document_hash = "doc_hash_1";
record.embedding = {1.0f, 2.0f, 3.0f, ...}; // 384 dimensions
record.model_id = "test-model-v1";
record.content = "Test content";

auto db = std::make_unique<VectorDatabase>(config);
db->initialize();
EXPECT_TRUE(db->insertVector(record));
```

### Persistence Test
```cpp
// Insert data
{
    auto db = std::make_unique<VectorDatabase>(config);
    db->initialize();
    db->insertVector(record);
}

// Verify persistence
{
    auto db = std::make_unique<VectorDatabase>(config);
    db->initialize();
    auto retrieved = db->getVector("test_chunk_1");
    ASSERT_TRUE(retrieved.has_value());
}
```

## Performance Considerations

1. **Batch Inserts**: Use `insertVectorsBatch()` for multiple records
2. **Indexing**: sqlite-vec automatically handles indexing
3. **Query Optimization**: Use document_hash filter when possible
4. **Connection Management**: Single connection per process

## Future Improvements

1. Support for binary format (when sqlite-vec adds support)
2. Concurrent access from multiple processes
3. Incremental index updates
4. Custom distance metrics beyond cosine similarity
5. Compression for vector storage

## References

- [sqlite-vec GitHub](https://github.com/asg017/sqlite-vec)
- [sqlite-vec Documentation](https://github.com/asg017/sqlite-vec/blob/main/README.md)
- [SQLite Virtual Tables](https://www.sqlite.org/vtab.html)