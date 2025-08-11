# Vector Search User Guide

## Introduction

YAMS Vector Search enables semantic search capabilities by converting text into high-dimensional vectors and finding similar content based on meaning rather than exact keyword matches. This guide will help you integrate and use vector search effectively.

## Quick Start

### 1. Installation

Ensure YAMS is built with vector search support:

```bash
cmake -DENABLE_VECTOR_SEARCH=ON ..
make -j4
```

### 2. Basic Setup

```cpp
#include <yams/vector/vector_database.h>
#include <yams/vector/embedding_generator.h>

int main() {
    // Initialize vector database
    yams::vector::VectorDatabaseConfig config;
    config.db_path = "my_vectors.db";
    config.embedding_dim = 384;
    
    auto db = yams::vector::createVectorDatabase(config);
    db->initialize();
    db->createTable();
    
    // Initialize embedding generator
    yams::vector::EmbeddingConfig emb_config;
    yams::vector::EmbeddingGenerator generator(emb_config);
    generator.initialize();
    
    // Your search logic here
    return 0;
}
```

### 3. Index Your First Document

```cpp
// Prepare document
std::string document = "Artificial intelligence is revolutionizing healthcare.";

// Generate embedding
auto embedding = generator.generateEmbedding(document);

// Create record
yams::vector::VectorRecord record;
record.chunk_id = yams::vector::utils::generateChunkId("doc_001", 0);
record.document_hash = "doc_001";
record.embedding = embedding;
record.content = document;

// Insert into database
db->insertVector(record);
```

### 4. Perform Semantic Search

```cpp
// Search query
std::string query = "How is AI impacting medicine?";
auto query_embedding = generator.generateEmbedding(query);

// Search parameters
yams::vector::VectorSearchParams params;
params.k = 5;  // Top 5 results
params.similarity_threshold = 0.7f;

// Execute search
auto results = db->searchSimilar(query_embedding, params);

// Display results
for (const auto& result : results) {
    std::cout << "Similarity: " << result.relevance_score << std::endl;
    std::cout << "Content: " << result.content << std::endl;
}
```

## Understanding Vector Search

### How It Works

1. **Text → Vector**: Documents are converted to numerical vectors that capture semantic meaning
2. **Indexing**: Vectors are stored in an optimized index structure
3. **Similarity Search**: Query vectors are compared against indexed vectors
4. **Ranking**: Results are ranked by similarity score

### Key Concepts

- **Embeddings**: Numerical representations of text in high-dimensional space
- **Similarity Metrics**: Methods to measure distance between vectors (cosine, L2)
- **Index Types**: Data structures for efficient search (FLAT, HNSW, IVF)
- **Chunking**: Breaking documents into smaller, meaningful segments

## Document Processing

### Chunking Strategies

Large documents should be chunked for better search results:

```cpp
yams::vector::DocumentChunker chunker;

// Configure chunking
yams::vector::ChunkingStrategy strategy;
strategy.method = yams::vector::ChunkingMethod::SLIDING_WINDOW;
strategy.chunk_size = 512;
strategy.overlap = 50;

// Chunk document
auto chunks = chunker.chunkDocument(large_document, strategy);

// Process each chunk
for (const auto& chunk : chunks) {
    auto embedding = generator.generateEmbedding(chunk.text);
    // Index the chunk...
}
```

### Chunking Methods Comparison

| Method | Best For | Pros | Cons |
|--------|----------|------|------|
| FIXED_SIZE | Uniform documents | Simple, predictable | May break context |
| SENTENCE_BASED | Natural text | Preserves meaning | Variable sizes |
| PARAGRAPH_BASED | Structured docs | Logical units | May be too large |
| SLIDING_WINDOW | General purpose | Good coverage | Overlapping content |
| SEMANTIC | Complex documents | Context-aware | Computationally expensive |

## Search Strategies

### Basic Similarity Search

Find documents similar to a query:

```cpp
auto results = db->searchSimilar(query_embedding, params);
```

### Filtered Search

Search within specific criteria:

```cpp
yams::vector::VectorSearchParams params;
params.k = 10;
params.metadata_filters["category"] = "technical";
params.metadata_filters["year"] = "2024";

auto results = db->searchSimilar(query_embedding, params);
```

### Hybrid Search

Combine vector and keyword search:

```cpp
yams::search::HybridSearchEngine engine;
engine.initialize();

// Configure weights
engine.setVectorWeight(0.7);  // 70% semantic
engine.setKeywordWeight(0.3);  // 30% keyword

auto results = engine.search(query_text, query_embedding, 10);
```

### Multi-Query Search

Search with multiple related queries:

```cpp
std::vector<std::string> queries = {
    "machine learning applications",
    "AI use cases",
    "artificial intelligence benefits"
};

// Generate embeddings for all queries
auto query_embeddings = generator.generateEmbeddings(queries);

// Aggregate results
std::set<std::string> unique_results;
for (const auto& embedding : query_embeddings) {
    auto results = db->searchSimilar(embedding, params);
    for (const auto& r : results) {
        unique_results.insert(r.chunk_id);
    }
}
```

## Performance Optimization

### Index Selection Guide

Choose the right index based on your needs:

| Dataset Size | Speed Priority | Accuracy Priority | Recommended Index |
|--------------|---------------|-------------------|-------------------|
| <10K | High | High | FLAT |
| 10K-100K | High | Medium | HNSW |
| 10K-100K | Medium | High | IVF_FLAT |
| >100K | High | Medium | HNSW |
| >1M | Medium | Medium | IVF_PQ |

### Configuration Examples

#### High Accuracy Configuration
```cpp
yams::vector::IndexConfig config;
config.type = yams::vector::IndexType::FLAT;
config.dimension = 384;
config.distance_metric = yams::vector::DistanceMetric::COSINE;
```

#### Balanced Configuration
```cpp
yams::vector::IndexConfig config;
config.type = yams::vector::IndexType::HNSW;
config.dimension = 384;
config.hnsw_m = 16;
config.hnsw_ef_construction = 200;
config.hnsw_ef_search = 50;
```

#### High Speed Configuration
```cpp
yams::vector::IndexConfig config;
config.type = yams::vector::IndexType::IVF_PQ;
config.dimension = 384;
config.ivf_nlist = 100;
config.ivf_nprobe = 5;
```

### Batch Processing

Process multiple documents efficiently:

```cpp
// Batch size for optimal performance
const size_t BATCH_SIZE = 32;

std::vector<std::string> batch;
std::vector<yams::vector::VectorRecord> records;

for (const auto& doc : documents) {
    batch.push_back(doc);
    
    if (batch.size() >= BATCH_SIZE) {
        // Process batch
        auto embeddings = generator.generateEmbeddings(batch);
        
        // Create records
        for (size_t i = 0; i < batch.size(); ++i) {
            yams::vector::VectorRecord record;
            record.embedding = embeddings[i];
            record.content = batch[i];
            records.push_back(record);
        }
        
        // Batch insert
        db->insertVectorsBatch(records);
        
        // Clear for next batch
        batch.clear();
        records.clear();
    }
}
```

## Model Management

### Available Models

| Model | Dimension | Speed | Quality | Use Case |
|-------|-----------|-------|---------|----------|
| all-MiniLM-L6-v2 | 384 | Fast | Good | General purpose |
| all-MiniLM-L12-v2 | 384 | Medium | Better | Higher accuracy |
| all-mpnet-base-v2 | 768 | Slow | Best | Maximum quality |

### Model Selection



```cpp
// Register models
yams::vector::ModelRegistry registry;

yams::vector::ModelInfo model1;
model1.model_id = "minilm-l6";
model1.embedding_dimension = 384;
model1.throughput_per_sec = 1000;
registry.registerModel(model1);

// Select best model for requirements
std::map<std::string, std::string> requirements;
requirements["min_throughput"] = "500";

auto best_model = registry.selectBestModel(384, requirements);
```

### Model Caching

```cpp
// Configure model cache
yams::vector::ModelCacheConfig cache_config;
cache_config.max_memory_bytes = 2ULL * 1024 * 1024 * 1024; // 2GB
cache_config.max_models = 3;
cache_config.enable_preloading = true;

yams::vector::ModelCache cache(cache_config);
cache.initialize(model_loader);

// Preload frequently used models
cache.preloadModel("minilm-l6");
```

## Monitoring and Maintenance

### Index Statistics

```cpp
auto stats = index_manager->getStats();

std::cout << "Total vectors: " << stats.num_vectors << std::endl;
std::cout << "Index size: " << stats.index_size_bytes << std::endl;
std::cout << "Average search time: " << stats.avg_search_time_ms << " ms" << std::endl;
```

### Index Optimization

```cpp
// Periodic optimization
if (db->getVectorCount() % 10000 == 0) {
    db->optimizeIndex();
    db->compactDatabase();
}
```

### Embedding Lifecycle

```cpp
// Check for stale embeddings
auto stale = db->getStaleEmbeddings("minilm-l6", "1.0.0");

// Re-embed stale content
for (const auto& id : stale.value()) {
    auto record = db->getVector(id);
    if (record.has_value()) {
        // Re-generate embedding with new model
        auto new_embedding = generator.generateEmbedding(record->content);
        record->embedding = new_embedding;
        record->model_version = "2.0.0";
        record->is_stale = false;
        
        db->updateVector(id, record.value());
    }
}
```

## Troubleshooting

### Common Issues

#### Low Search Quality
- **Symptom**: Poor relevance in results
- **Solutions**:
  - Increase embedding dimension
  - Use better model
  - Adjust similarity threshold
  - Improve chunking strategy

#### Slow Performance
- **Symptom**: High search latency
- **Solutions**:
  - Switch to HNSW or IVF index
  - Reduce search parameters (k, ef_search)
  - Enable caching
  - Use batch operations

#### Memory Issues
- **Symptom**: Out of memory errors
- **Solutions**:
  - Use IVF_PQ index for compression
  - Implement pagination
  - Reduce cache size
  - Enable memory-mapped files

### Debug Logging

Enable detailed logging:

```cpp
spdlog::set_level(spdlog::level::debug);

// Log search operations
db->enableDebugLogging(true);
```

## Advanced Topics

### Custom Distance Metrics

```cpp
// Implement custom similarity
auto custom_similarity = [](const std::vector<float>& a, 
                           const std::vector<float>& b) -> float {
    // Your custom logic
    return similarity_score;
};

index_manager->setCustomDistanceFunction(custom_similarity);
```

### Incremental Indexing

```cpp
// Delta index for real-time updates
config.enable_delta_index = true;
config.delta_threshold = 1000;

// Vectors added to delta index first
manager->addVector(id, vector);

// Periodic merge to main index
if (manager->getDeltaSize() >= config.delta_threshold) {
    manager->mergeDeltaIndex();
}
```

### A/B Testing

```cpp
// Test different configurations
std::vector<yams::vector::IndexConfig> configs = {
    config_a, config_b
};

for (const auto& config : configs) {
    auto manager = std::make_unique<VectorIndexManager>(config);
    
    // Measure performance
    auto start = std::chrono::high_resolution_clock::now();
    auto results = manager->search(query, k);
    auto end = std::chrono::high_resolution_clock::now();
    
    // Compare results...
}
```

## Best Practices Summary

1. **Start Simple**: Begin with FLAT index, optimize later
2. **Chunk Appropriately**: 256-512 tokens per chunk
3. **Normalize Vectors**: Always normalize for cosine similarity
4. **Batch Operations**: Process in batches of 32-64
5. **Monitor Performance**: Track metrics and optimize
6. **Version Embeddings**: Track model versions for updates
7. **Cache Wisely**: Cache models and frequent queries
8. **Test Thoroughly**: Benchmark with your actual data

## Next Steps

- Review the [API Reference](../api/vector_search_api.md)
- Explore [Performance Tuning](../admin/vector_search_tuning.md)
- Check [Architecture Documentation](../architecture/vector_search_architecture.md)
- Try the [Example Code](../../examples/vector_search_examples.cpp)