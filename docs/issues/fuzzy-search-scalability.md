# Fuzzy Search Scalability Limitation

**Status**: Open  
**Priority**: Medium  
**Component**: Search / Fuzzy Index (BK-tree)  
**Created**: 2025-10-22  
**Affects**: v0.7.7+

## Problem Statement

The current fuzzy search implementation uses an in-memory BK-tree (Burkhard-Keller tree) for approximate string matching. While effective for small to medium repositories, this approach has significant memory constraints when indexing large document collections (100K+ documents).

### Observed Behavior

- **Memory Exhaustion**: Building fuzzy index with 130K+ documents triggers `std::bad_alloc`
- **Daemon Crashes**: Memory allocation failures can crash the daemon during index building
- **Performance Degradation**: Index build time increases non-linearly with document count

### Root Cause

The BK-tree stores all indexed strings in memory with their edit-distance relationships. Memory usage is approximately:
```
Memory ≈ (avg_string_length + tree_overhead) × num_documents
```

For large repositories:
- 130K documents × ~100 bytes/entry ≈ 13MB baseline
- Tree structure overhead: ~3-5x multiplier
- **Total**: 40-65MB+ for structure alone
- Content keywords add additional 2-3x multiplier
- **Peak**: Can exceed 200MB+ during build

## Current Mitigations (v0.7.7)

### 1. Intelligent Document Prioritization
The fuzzy index builder now uses metadata and Knowledge Graph to rank documents by relevance:

```sql
ORDER BY (tag_count * 3 + entity_count * 2 + recency_score + code_boost) DESC
```

**Prioritization factors**:
- Tagged documents (weight: 3)
- Knowledge Graph entities/symbols (weight: 2)
- Recently indexed (sliding scale: 4-0)
- Code files (weight: 2)

### 2. Hard Limit with Graceful Degradation
- Default limit: 50,000 documents
- Configurable via `YAMS_FUZZY_INDEX_LIMIT` environment variable
- `std::bad_alloc` handling prevents crashes
- Warning logs when limit reached

### 3. Memory Safety
```cpp
try {
    fuzzySearchIndex_->addDocument(id, name, keywords);
} catch (const std::bad_alloc& e) {
    spdlog::error("Memory exhausted at {} documents", count);
    break;
}
```

## Limitations

### Current Implementation
1. **Fixed Limit**: Even with prioritization, only 50K documents are indexed
2. **Binary Choice**: Documents are either fully indexed or excluded
3. **Static Ranking**: Ranking happens at build time, not query time
4. **No Progressive Loading**: Cannot incrementally add documents as memory allows
5. **Content Duplication**: Both filename and content entries consume space

### Search Quality Impact
- **Large Repositories**: >100K documents means >50% may be excluded from fuzzy search
- **Query Miss Rate**: Fuzzy queries may miss relevant documents outside the top 50K
- **Recency Bias**: Recent documents preferred, but older documents might be more relevant
- **No User Control**: Users cannot influence which documents are prioritized

## Proposed Solutions

### Short-term (v0.7.x)

#### Option 1: Lazy Index Building
- Build index on-demand for query terms
- Cache frequent queries
- Trade build latency for memory

#### Option 2: Tiered Indexing
- Tier 1: In-memory BK-tree (top 25K documents)
- Tier 2: Disk-based trie (next 75K documents)
- Tier 3: Direct SQL LIKE fallback (remaining)

#### Option 3: Query-time Filtering
- Build full index but use smarter query-time filtering
- Leverage SQL indexes for initial candidate selection
- Apply fuzzy matching only to candidates

### Long-term (v0.8+)

#### Option 1: Replace BK-tree with Disk-based Alternative
Consider alternatives:
- **sqlite-fts5**: Built-in full-text search with trigram support
  - Pros: Already integrated, disk-based, mature
  - Cons: Less precise than BK-tree for fuzzy matching
  
- **Levenshtein Automaton**: Efficient disk-based fuzzy search
  - Pros: O(n) performance, bounded memory
  - Cons: Complex implementation, requires custom index
  
- **Embedding-based Similarity**: Use vector embeddings
  - Pros: Semantic similarity, already have infrastructure
  - Cons: Requires embeddings, different from string fuzzy matching

#### Option 2: Hybrid Approach
- BK-tree for exact/near-exact matches (top 50K)
- FTS5 trigrams for approximate matches (remaining)
- Vector embeddings for semantic similarity
- Query router selects appropriate backend

#### Option 3: Distributed Index
- Shard BK-tree across multiple indices
- Query all shards in parallel
- Merge results by edit distance

## Workarounds for Users

### Current Options

1. **Increase Limit** (if memory available):
   ```bash
   export YAMS_FUZZY_INDEX_LIMIT=100000
   yamsd start
   ```

2. **Use Exact Search** instead of fuzzy:
   ```bash
   yams search --fixed-strings "exact term"
   ```

3. **Tag Important Documents**:
   ```bash
   yams update --hash <hash> --tags priority
   ```
   Tagged documents are prioritized in fuzzy index.

4. **Use Glob Patterns** for path-based queries:
   ```bash
   yams list --pattern "docs/**/*.md"
   ```

5. **Leverage Knowledge Graph** queries:
   ```bash
   yams graph query --entity-name "FunctionName"
   ```

## Metrics to Track

- **Index Build Time**: Measure as document count increases
- **Memory Usage**: Track peak memory during build
- **Query Performance**: Compare fuzzy vs exact search latency
- **Hit Rate**: Measure queries finding relevant documents
- **Coverage**: Percentage of documents in fuzzy index

## Testing Strategy

### Unit Tests
- [x] Symbol enrichment with mock KG store
- [ ] BK-tree memory usage profiling
- [ ] Progressive loading simulation

### Integration Tests
- [ ] Large repository simulation (100K+ docs)
- [ ] Memory limit enforcement
- [ ] Graceful degradation behavior
- [ ] Query quality with partial index

### Performance Tests
- [ ] Benchmark: Index build time vs document count
- [ ] Benchmark: Memory usage vs document count  
- [ ] Benchmark: Query latency with full vs partial index
- [ ] Benchmark: Hit rate with different document counts

## Related Work

- **PBI-059**: Symbol extraction and entity graph
- **Search Service**: Hybrid search engine architecture
- **Knowledge Graph**: Entity storage and retrieval

## References

- BK-tree algorithm: Burkhard & Keller, "Some approaches to best-match file searching" (1973)
- Levenshtein Automaton: Schulz & Mihov, "Fast String Correction with Levenshtein Automata" (2002)
- SQLite FTS5: https://www.sqlite.org/fts5.html
- Issue logs: `[2025-10-22 09:59:39] std::bad_alloc building fuzzy index`

## Discussion Points

1. **Should we keep BK-tree or replace it entirely?**
   - BK-tree provides excellent precision for true fuzzy matching
   - But memory constraints are fundamental to the algorithm
   
2. **Is fuzzy search a critical feature?**
   - How often do users rely on typo-tolerant search?
   - Can we achieve similar UX with other approaches?
   
3. **What's the acceptable trade-off?**
   - Memory vs coverage vs precision vs latency
   - Different users may have different preferences

4. **Should fuzzy search be opt-in for large repos?**
   - Auto-disable when document count > threshold
   - User can explicitly enable if they have memory

## Next Steps

1. **Gather Metrics**: Instrument fuzzy index build and query in production
2. **User Research**: Survey how users actually use fuzzy search
3. **Prototype Alternatives**: Build POC with FTS5 trigrams and compare
4. **Performance Testing**: Establish baseline benchmarks
5. **Design Review**: Present hybrid approach for feedback
