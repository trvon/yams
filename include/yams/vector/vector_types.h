#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::vector {

enum class EmbeddingLevel { CHUNK, DOCUMENT };

enum class VectorSearchEngine {
    HnswCosine,
    SimeonPqAdc,
    Vec0L2,
};

[[nodiscard]] inline const char* vectorSearchEngineName(VectorSearchEngine engine) {
    switch (engine) {
        case VectorSearchEngine::HnswCosine:
            return "hnsw_cosine";
        case VectorSearchEngine::SimeonPqAdc:
            return "simeon_pq_adc";
        case VectorSearchEngine::Vec0L2:
            return "vec0_l2";
    }
    return "unknown";
}

[[nodiscard]] inline std::optional<VectorSearchEngine>
parseVectorSearchEngine(std::string_view raw) {
    if (raw == "hnsw" || raw == "hnsw_cosine" || raw == "cosine") {
        return VectorSearchEngine::HnswCosine;
    }
    if (raw == "simeon_pq_adc" || raw == "pq_adc" || raw == "simeon_pq" ||
        raw == "compressed_primary") {
        return VectorSearchEngine::SimeonPqAdc;
    }
    if (raw == "vec0" || raw == "vec0_l2" || raw == "l2") {
        return VectorSearchEngine::Vec0L2;
    }
    return std::nullopt;
}

enum class EntityEmbeddingType { SIGNATURE, DOCUMENTATION, ALIAS, CONTEXT };

struct VectorDatabaseConfig {
    std::string database_path = "vectors.db";
    std::string table_name = "document_embeddings";
    size_t embedding_dim = 0;
    bool create_if_missing = true;
    std::string index_type = "IVF_PQ";
    size_t num_partitions = 256;
    size_t num_sub_quantizers = 96;
    bool enable_checkpoints = true;
    size_t checkpoint_frequency = 1000;
    size_t max_batch_size = 1000;
    float default_similarity_threshold = 0.35f;
    bool use_in_memory = false;
    VectorSearchEngine search_engine = VectorSearchEngine::SimeonPqAdc;
    size_t simeon_pq_subquantizers = 32;
    size_t simeon_pq_centroids = 256;
    size_t simeon_pq_train_limit = 4096;
    size_t simeon_pq_rerank_factor = 2;
    uint64_t simeon_pq_seed = 0xC0FFEE5EED5EEDC0ULL;
    bool enable_turboquant_storage = false;
    uint8_t turboquant_bits = 4;
    uint64_t turboquant_seed = 42;
    bool quantized_primary_storage = false;
};

struct VectorRecord {
    enum class QuantizedFormat : uint8_t {
        NONE = 0,
        TURBOquant_1 = 1,
    };

    struct QuantizedEmbedding {
        QuantizedFormat format = QuantizedFormat::NONE;
        uint8_t bits_per_channel = 0;
        uint64_t seed = 0;
        std::vector<uint8_t> packed_codes;
        std::vector<float> per_coord_scales;
    };

    std::string chunk_id;
    std::string document_hash;
    std::vector<float> embedding;
    std::string content;
    size_t start_offset = 0;
    size_t end_offset = 0;
    std::map<std::string, std::string> metadata;
    std::chrono::system_clock::time_point created_at;
    float relevance_score = 0.0f;
    std::string model_id;
    std::string model_version;
    uint32_t embedding_version = 1;
    std::string content_hash_at_embedding;
    std::chrono::system_clock::time_point embedded_at;
    bool is_stale = false;
    size_t embedding_dim = 0;
    QuantizedEmbedding quantized;
    EmbeddingLevel level = EmbeddingLevel::CHUNK;
    std::vector<std::string> source_chunk_ids;
    std::string parent_document_hash;
    std::vector<std::string> child_document_hashes;

    VectorRecord() = default;

    VectorRecord(std::string chunk_id_param, std::string document_hash_param,
                 std::vector<float> embedding_param, std::string content_param)
        : chunk_id(std::move(chunk_id_param)), document_hash(std::move(document_hash_param)),
          embedding(std::move(embedding_param)), content(std::move(content_param)),
          created_at(std::chrono::system_clock::now()) {}

    VectorRecord(const VectorRecord&) = default;
    VectorRecord(VectorRecord&&) = default;
    VectorRecord& operator=(const VectorRecord&) = default;
    VectorRecord& operator=(VectorRecord&&) = default;
};

struct EntityVectorRecord {
    int64_t rowid = 0;
    std::string node_key;
    EntityEmbeddingType embedding_type = EntityEmbeddingType::SIGNATURE;
    std::vector<float> embedding;
    std::string content;
    std::string model_id;
    std::string model_version;
    std::chrono::system_clock::time_point embedded_at;
    bool is_stale = false;
    float relevance_score = 0.0f;
    std::string node_type;
    std::string qualified_name;
    std::string file_path;
    std::string document_hash;

    EntityVectorRecord() = default;

    EntityVectorRecord(std::string node_key_param, EntityEmbeddingType type,
                       std::vector<float> embedding_param, std::string content_param)
        : node_key(std::move(node_key_param)), embedding_type(type),
          embedding(std::move(embedding_param)), content(std::move(content_param)),
          embedded_at(std::chrono::system_clock::now()) {}

    EntityVectorRecord(const EntityVectorRecord&) = default;
    EntityVectorRecord(EntityVectorRecord&&) = default;
    EntityVectorRecord& operator=(const EntityVectorRecord&) = default;
    EntityVectorRecord& operator=(EntityVectorRecord&&) = default;
};

struct EntitySearchParams {
    size_t k = 10;
    float similarity_threshold = 0.5f;
    std::optional<EntityEmbeddingType> embedding_type;
    std::optional<std::string> node_type;
    std::optional<std::string> document_hash;
    bool include_embeddings = false;
};

struct VectorSearchParams {
    size_t k = 10;
    float similarity_threshold = 0.7f;
    std::optional<std::string> document_hash;
    std::unordered_set<std::string> candidate_hashes;
    std::map<std::string, std::string> metadata_filters;
    bool include_embeddings = false;
};

struct VectorDatabaseStats {
    size_t total_vectors = 0;
    size_t total_documents = 0;
    double avg_embedding_magnitude = 0.0;
    size_t index_size_bytes = 0;
    std::chrono::system_clock::time_point last_optimized;
};

namespace utils {

[[nodiscard]] inline std::string entityEmbeddingTypeToString(EntityEmbeddingType type) {
    switch (type) {
        case EntityEmbeddingType::SIGNATURE:
            return "signature";
        case EntityEmbeddingType::DOCUMENTATION:
            return "documentation";
        case EntityEmbeddingType::ALIAS:
            return "alias";
        case EntityEmbeddingType::CONTEXT:
            return "context";
    }
    return "unknown";
}

[[nodiscard]] inline EntityEmbeddingType stringToEntityEmbeddingType(const std::string& str) {
    if (str == "signature") {
        return EntityEmbeddingType::SIGNATURE;
    }
    if (str == "documentation") {
        return EntityEmbeddingType::DOCUMENTATION;
    }
    if (str == "alias") {
        return EntityEmbeddingType::ALIAS;
    }
    if (str == "context") {
        return EntityEmbeddingType::CONTEXT;
    }
    return EntityEmbeddingType::SIGNATURE;
}

} // namespace utils

} // namespace yams::vector
