# Vector Search API Reference

## Overview

The YAMS Vector Search API provides semantic search capabilities through document embeddings and vector similarity matching. This API enables you to index documents with their vector representations and perform similarity-based searches.

## Table of Contents

1. [Core Components](#core-components)
2. [Vector Database API](#vector-database-api)
3. [Embedding Generation API](#embedding-generation-api)
4. [Vector Index Manager API](#vector-index-manager-api)
5. [Document Chunking API](#document-chunking-api)
6. [Model Management API](#model-management-api)
7. [Error Handling](#error-handling)
8. [Examples](#examples)

## Core Components

### VectorDatabase

The main interface for vector storage and retrieval operations.

```cpp
#include <yams/vector/vector_database.h>

// Initialize database
yams::vector::VectorDatabaseConfig config;
config.db_path = "vectors.lancedb";
config.embedding_dim = 384;
config.index_type = "IVF_PQ";

auto database = yams::vector::createVectorDatabase(config);
database->initialize();
```

### Key Classes

- `VectorDatabase`: Main database interface
- `VectorRecord`: Individual vector record
- `VectorSearchParams`: Search configuration
- `EmbeddingGenerator`: Text to vector conversion
- `DocumentChunker`: Text segmentation
- `VectorIndexManager`: Index management

## Vector Database API

### VectorDatabase Class

#### Constructor
```cpp
explicit VectorDatabase(const VectorDatabaseConfig& config = {});
```

Creates a new vector database instance with the specified configuration.

**Parameters:**
- `config`: Database configuration options

#### initialize()
```cpp
bool initialize();
```

Initializes the database connection and creates necessary resources.

**Returns:** `true` if successful, `false` otherwise

#### insertVector()
```cpp
bool insertVector(const VectorRecord& record);
```

Inserts a single vector record into the database.

**Parameters:**
- `record`: The vector record to insert

**Returns:** `true` if successful

#### searchSimilar()
```cpp
std::vector<VectorRecord> searchSimilar(
    const std::vector<float>& query_embedding,
    const VectorSearchParams& params = {}
) const;
```

Searches for similar vectors using cosine similarity.

**Parameters:**
- `query_embedding`: Query vector
- `params`: Search parameters (k, threshold, filters)

**Returns:** Vector of matching records sorted by similarity

### VectorRecord Structure

```cpp
struct VectorRecord {
    std::string chunk_id;              // Unique identifier
    std::string document_hash;         // Source document hash
    std::vector<float> embedding;      // Vector representation
    std::string content;               // Original text
    size_t start_offset;              // Position in document
    size_t end_offset;
    std::map<std::string, std::string> metadata;
    
    // Versioning fields
    std::string model_id;
    std::string model_version;
    uint32_t embedding_version;
    bool is_stale;
};
```

### VectorSearchParams Structure

```cpp
struct VectorSearchParams {
    size_t k = 10;                     // Number of results
    float similarity_threshold = 0.7f;  // Minimum similarity
    std::optional<std::string> document_hash;  // Filter by document
    std::map<std::string, std::string> metadata_filters;
    bool include_embeddings = false;   // Include vectors in results
};
```

## Embedding Generation API

### EmbeddingGenerator Class

#### Constructor
```cpp
explicit EmbeddingGenerator(const EmbeddingConfig& config = {});
```

Creates an embedding generator with the specified configuration.

#### generateEmbedding()
```cpp
std::vector<float> generateEmbedding(const std::string& text);
```

Generates an embedding vector for a single text.

**Parameters:**
- `text`: Input text to embed

**Returns:** Embedding vector

#### generateEmbeddings()
```cpp
std::vector<std::vector<float>> generateEmbeddings(
    const std::vector<std::string>& texts
);
```

Generates embeddings for multiple texts in batch.

**Parameters:**
- `texts`: Vector of input texts

**Returns:** Vector of embedding vectors

### EmbeddingConfig Structure

```cpp
struct EmbeddingConfig {
    std::string model_path = "models/all-MiniLM-L6-v2.onnx";
    size_t max_sequence_length = 512;
    size_t embedding_dim = 384;
    size_t batch_size = 32;
    bool normalize_embeddings = true;
    std::string model_version = "1.0.0";
};
```

## Vector Index Manager API

### VectorIndexManager Class

#### Constructor
```cpp
explicit VectorIndexManager(const IndexConfig& config = {});
```

Creates an index manager with the specified configuration.

#### addVector()
```cpp
Result<void> addVector(
    const std::string& id,
    const std::vector<float>& vector,
    const std::map<std::string, std::string>& metadata = {}
);
```

Adds a vector to the index.

**Parameters:**
- `id`: Unique identifier
- `vector`: Embedding vector
- `metadata`: Optional metadata

**Returns:** Result indicating success or error

#### search()
```cpp
Result<std::vector<SearchResult>> search(
    const std::vector<float>& query,
    size_t k,
    const SearchFilter& filter = {}
);
```

Searches for k nearest neighbors.

**Parameters:**
- `query`: Query vector
- `k`: Number of results
- `filter`: Optional search filters

**Returns:** Vector of search results

### IndexConfig Structure

```cpp
struct IndexConfig {
    IndexType type = IndexType::HNSW;
    size_t dimension = 384;
    DistanceMetric distance_metric = DistanceMetric::COSINE;
    
    // HNSW parameters
    size_t hnsw_m = 16;
    size_t hnsw_ef_construction = 200;
    size_t hnsw_ef_search = 50;
    
    // IVF parameters
    size_t ivf_nlist = 100;
    size_t ivf_nprobe = 10;
};
```

### Index Types

```cpp
enum class IndexType {
    FLAT,       // Brute force search
    HNSW,       // Hierarchical Navigable Small World
    IVF_FLAT,   // Inverted File with Flat quantizer
    IVF_PQ,     // Inverted File with Product Quantization
    LSH,        // Locality Sensitive Hashing
};
```

## Document Chunking API

### DocumentChunker Class

#### chunkDocument()
```cpp
std::vector<TextChunk> chunkDocument(
    const std::string& content,
    const ChunkingStrategy& strategy = {}
);
```

Splits a document into chunks for embedding.

**Parameters:**
- `content`: Document text
- `strategy`: Chunking configuration

**Returns:** Vector of text chunks

### ChunkingStrategy Structure

```cpp
struct ChunkingStrategy {
    ChunkingMethod method = ChunkingMethod::SLIDING_WINDOW;
    size_t chunk_size = 512;
    size_t overlap = 50;
    bool preserve_sentences = true;
    size_t min_chunk_size = 100;
    size_t max_chunk_size = 1000;
};
```

### Chunking Methods

```cpp
enum class ChunkingMethod {
    FIXED_SIZE,      // Fixed character count
    SENTENCE_BASED,  // Sentence boundaries
    PARAGRAPH_BASED, // Paragraph boundaries
    SEMANTIC,        // Semantic coherence
    SLIDING_WINDOW,  // Overlapping windows
    RECURSIVE,       // Recursive splitting
    MARKDOWN_AWARE   // Respects markdown structure
};
```

## Model Management API

### ModelRegistry Class

#### registerModel()
```cpp
Result<void> registerModel(const ModelInfo& info);
```

Registers a new embedding model.

**Parameters:**
- `info`: Model information and metadata

#### selectBestModel()
```cpp
Result<std::string> selectBestModel(
    size_t required_dimension,
    const std::map<std::string, std::string>& requirements = {}
);
```

Selects the best model based on requirements.

**Parameters:**
- `required_dimension`: Required embedding dimension
- `requirements`: Additional requirements

**Returns:** Model ID of the best matching model

### ModelCache Class

#### getModel()
```cpp
Result<std::shared_ptr<void>> getModel(const std::string& model_id);
```

Retrieves a cached model or loads it if not cached.

**Parameters:**
- `model_id`: Model identifier

**Returns:** Model handle

## Error Handling

All API methods that can fail return a `Result<T>` type:

```cpp
template<typename T>
class Result {
public:
    bool has_value() const;
    T& value();
    const Error& error() const;
};
```

### Error Codes

```cpp
enum class ErrorCode {
    Success = 0,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    ResourceExhausted,
    InternalError,
    NotSupported,
    InvalidState
};
```

### Error Handling Example

```cpp
auto result = database->insertVector(record);
if (!result.has_value()) {
    spdlog::error("Failed to insert vector: {}", 
                  result.error().message);
    
    switch (result.error().code) {
        case ErrorCode::InvalidArgument:
            // Handle invalid input
            break;
        case ErrorCode::ResourceExhausted:
            // Handle resource limits
            break;
        default:
            // Handle other errors
            break;
    }
}
```

## Examples

### Basic Vector Search

```cpp
#include <yams/vector/vector_database.h>
#include <yams/vector/embedding_generator.h>

// Initialize components
yams::vector::VectorDatabaseConfig db_config;
auto database = yams::vector::createVectorDatabase(db_config);
database->initialize();

yams::vector::EmbeddingConfig emb_config;
yams::vector::EmbeddingGenerator generator(emb_config);
generator.initialize();

// Index a document
std::string doc = "Machine learning is transforming software.";
auto embedding = generator.generateEmbedding(doc);

yams::vector::VectorRecord record;
record.chunk_id = "doc_001_chunk_001";
record.document_hash = "abc123";
record.embedding = embedding;
record.content = doc;

database->insertVector(record);

// Search for similar documents
std::string query = "AI is changing technology";
auto query_embedding = generator.generateEmbedding(query);

yams::vector::VectorSearchParams params;
params.k = 5;
params.similarity_threshold = 0.7f;

auto results = database->searchSimilar(query_embedding, params);

for (const auto& result : results) {
    std::cout << "Score: " << result.relevance_score 
              << " Content: " << result.content << std::endl;
}
```

### Batch Processing

```cpp
// Process multiple documents
std::vector<std::string> documents = {
    "First document text...",
    "Second document text...",
    "Third document text..."
};

// Generate embeddings in batch
auto embeddings = generator.generateEmbeddings(documents);

// Insert in batch
std::vector<yams::vector::VectorRecord> records;
for (size_t i = 0; i < documents.size(); ++i) {
    yams::vector::VectorRecord record;
    record.chunk_id = std::format("doc_{}_chunk_001", i);
    record.embedding = embeddings[i];
    record.content = documents[i];
    records.push_back(record);
}

database->insertVectorsBatch(records);
```

### Advanced Search with Filters

```cpp
// Search with metadata filters
yams::vector::VectorSearchParams params;
params.k = 10;
params.similarity_threshold = 0.6f;
params.metadata_filters["category"] = "technology";
params.metadata_filters["language"] = "en";

// Filter by specific document
params.document_hash = "specific_doc_hash";

auto results = database->searchSimilar(query_embedding, params);
```

### Index Optimization

```cpp
// Create optimized HNSW index
yams::vector::IndexConfig config;
config.type = yams::vector::IndexType::HNSW;
config.dimension = 384;
config.hnsw_m = 32;  // More connections for better recall
config.hnsw_ef_construction = 400;  // Higher quality graph
config.hnsw_ef_search = 100;  // Better search accuracy

yams::vector::VectorIndexManager manager(config);
manager.initialize();

// Add vectors and build index
for (const auto& [id, vector] : vector_data) {
    manager.addVector(id, vector);
}

manager.buildIndex();  // Optimize index structure
```

## Best Practices

1. **Batch Operations**: Use batch methods for better performance
2. **Index Selection**: Choose appropriate index type based on dataset size
3. **Normalization**: Always normalize embeddings for cosine similarity
4. **Chunking Strategy**: Select chunking method based on document structure
5. **Model Caching**: Use model cache to avoid repeated loading
6. **Error Handling**: Always check Result types for errors
7. **Memory Management**: Monitor memory usage with large datasets
8. **Concurrent Access**: Use appropriate locking for thread safety

## Performance Guidelines

- **FLAT Index**: Best for <10K vectors, exact search
- **HNSW Index**: Best for 10K-1M vectors, fast approximate search
- **IVF_PQ Index**: Best for >1M vectors, memory-efficient
- **Batch Size**: 32-64 for embedding generation
- **Chunk Size**: 256-512 tokens for optimal context
- **Cache Size**: 2-4GB for model cache

## Migration from v1 API

For users migrating from the previous API version:

1. Replace `VectorStore` with `VectorDatabase`
2. Update `SearchParams` to `VectorSearchParams`
3. Add embedding versioning fields to records
4. Update error handling to use Result types

## Thread Safety

- `VectorDatabase`: Thread-safe for concurrent reads, single writer
- `EmbeddingGenerator`: Thread-safe after initialization
- `VectorIndexManager`: Thread-safe with internal locking
- `ModelCache`: Thread-safe with LRU eviction

## Resource Limits

- Maximum embedding dimension: 4096
- Maximum vectors per index: 100M
- Maximum batch size: 10000
- Maximum document size: 1MB
- Maximum chunk size: 4096 tokens