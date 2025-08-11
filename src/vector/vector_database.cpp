#include <yams/vector/vector_database.h>

#include <algorithm>
#include <cmath>
#include <sstream>
#include <iomanip>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <mutex>

// TODO: Replace with actual LanceDB includes when available
// #include <lancedb/lancedb.hpp>
// #include <arrow/api.h>

namespace yams::vector {

/**
 * Private implementation class (PIMPL pattern)
 * This will be replaced with actual LanceDB implementation
 */
class VectorDatabase::Impl {
public:
    explicit Impl(const VectorDatabaseConfig& config)
        : config_(config)
        , initialized_(false)
        , has_error_(false) {}

    bool initialize() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (initialized_) {
            return true;
        }

        try {
            // TODO: Initialize actual LanceDB connection
            // For now, use in-memory storage for testing
            vectors_.clear();
            document_index_.clear();
            
            // Create table if it doesn't exist
            if (!createTable()) {
                setError("Failed to create vector table");
                return false;
            }

            initialized_ = true;
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Initialization failed: " + std::string(e.what()));
            return false;
        }
    }

    bool isInitialized() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return initialized_;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        // TODO: Close LanceDB connection
        vectors_.clear();
        document_index_.clear();
        initialized_ = false;
        has_error_ = false;
        last_error_.clear();
    }

    bool createTable() {
        // TODO: Create actual LanceDB table with schema
        // For now, just ensure our in-memory storage is ready
        return true;
    }

    bool tableExists() const {
        // TODO: Check if LanceDB table exists
        return initialized_;
    }

    void dropTable() {
        std::lock_guard<std::mutex> lock(mutex_);
        // TODO: Drop actual LanceDB table
        vectors_.clear();
        document_index_.clear();
    }

    size_t getVectorCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return vectors_.size();
    }

    bool insertVector(const VectorRecord& record) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
            setError("Invalid vector record");
            return false;
        }

        try {
            // TODO: Insert into actual LanceDB
            vectors_[record.chunk_id] = record;
            document_index_[record.document_hash].insert(record.chunk_id);
            
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Insert failed: " + std::string(e.what()));
            return false;
        }
    }

    bool insertVectorsBatch(const std::vector<VectorRecord>& records) {
        if (records.empty()) {
            return true;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            // TODO: Use LanceDB batch insert for efficiency
            for (const auto& record : records) {
                if (!utils::validateVectorRecord(record, config_.embedding_dim)) {
                    setError("Invalid vector record in batch");
                    return false;
                }

                vectors_[record.chunk_id] = record;
                document_index_[record.document_hash].insert(record.chunk_id);
            }

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Batch insert failed: " + std::string(e.what()));
            return false;
        }
    }

    bool updateVector(const std::string& chunk_id, const VectorRecord& record) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        auto it = vectors_.find(chunk_id);
        if (it == vectors_.end()) {
            setError("Vector not found: " + chunk_id);
            return false;
        }

        try {
            // TODO: Update in actual LanceDB
            auto old_record = it->second;
            vectors_[chunk_id] = record;
            
            // Update document index if document hash changed
            if (old_record.document_hash != record.document_hash) {
                document_index_[old_record.document_hash].erase(chunk_id);
                document_index_[record.document_hash].insert(chunk_id);
            }

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Update failed: " + std::string(e.what()));
            return false;
        }
    }

    bool deleteVector(const std::string& chunk_id) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        auto it = vectors_.find(chunk_id);
        if (it == vectors_.end()) {
            setError("Vector not found: " + chunk_id);
            return false;
        }

        try {
            // TODO: Delete from actual LanceDB
            auto record = it->second;
            vectors_.erase(it);
            document_index_[record.document_hash].erase(chunk_id);

            // Clean up empty document entries
            if (document_index_[record.document_hash].empty()) {
                document_index_.erase(record.document_hash);
            }

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Delete failed: " + std::string(e.what()));
            return false;
        }
    }

    bool deleteVectorsByDocument(const std::string& document_hash) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        auto doc_it = document_index_.find(document_hash);
        if (doc_it == document_index_.end()) {
            // No vectors for this document - not an error
            return true;
        }

        try {
            // TODO: Use efficient batch delete in LanceDB
            for (const auto& chunk_id : doc_it->second) {
                vectors_.erase(chunk_id);
            }
            document_index_.erase(doc_it);

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Batch delete failed: " + std::string(e.what()));
            return false;
        }
    }

    std::vector<VectorRecord> searchSimilar(
        const std::vector<float>& query_embedding,
        const VectorSearchParams& params) const {
        
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return {};
        }

        if (query_embedding.size() != config_.embedding_dim) {
            return {};
        }

        try {
            // TODO: Use LanceDB's optimized vector search
            // For now, implement basic cosine similarity search
            
            std::vector<std::pair<double, std::string>> similarities;
            similarities.reserve(vectors_.size());

            for (const auto& [chunk_id, record] : vectors_) {
                // Apply filters
                if (params.document_hash && record.document_hash != *params.document_hash) {
                    continue;
                }

                bool metadata_match = true;
                for (const auto& [key, value] : params.metadata_filters) {
                    auto it = record.metadata.find(key);
                    if (it == record.metadata.end() || it->second != value) {
                        metadata_match = false;
                        break;
                    }
                }
                if (!metadata_match) {
                    continue;
                }

                // Compute cosine similarity
                double similarity = computeCosineSimilarity(query_embedding, record.embedding);
                
                if (similarity >= params.similarity_threshold) {
                    similarities.emplace_back(similarity, chunk_id);
                }
            }

            // Sort by similarity (descending)
            std::sort(similarities.begin(), similarities.end(), 
                     [](const auto& a, const auto& b) { return a.first > b.first; });

            // Take top k results
            size_t num_results = std::min(params.k, similarities.size());
            std::vector<VectorRecord> results;
            results.reserve(num_results);

            for (size_t i = 0; i < num_results; ++i) {
                auto it = vectors_.find(similarities[i].second);
                if (it != vectors_.end()) {
                    VectorRecord result = it->second;
                    result.relevance_score = static_cast<float>(similarities[i].first);
                    
                    // Optionally exclude embeddings to save memory
                    if (!params.include_embeddings) {
                        result.embedding.clear();
                    }
                    
                    results.push_back(std::move(result));
                }
            }

            return results;

        } catch (const std::exception& e) {
            // Can't modify has_error_ from const method
            return {};
        }
    }

    std::optional<VectorRecord> getVector(const std::string& chunk_id) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = vectors_.find(chunk_id);
        if (it == vectors_.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    std::vector<VectorRecord> getVectorsByDocument(const std::string& document_hash) const {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<VectorRecord> results;

        auto doc_it = document_index_.find(document_hash);
        if (doc_it == document_index_.end()) {
            return results;
        }

        results.reserve(doc_it->second.size());
        for (const auto& chunk_id : doc_it->second) {
            auto it = vectors_.find(chunk_id);
            if (it != vectors_.end()) {
                results.push_back(it->second);
            }
        }

        return results;
    }

    bool buildIndex() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            // TODO: Build actual LanceDB index (IVF_PQ)
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Index build failed: " + std::string(e.what()));
            return false;
        }
    }

    bool optimizeIndex() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (!initialized_) {
            setError("Database not initialized");
            return false;
        }

        try {
            // TODO: Optimize LanceDB index
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Index optimization failed: " + std::string(e.what()));
            return false;
        }
    }

    VectorDatabase::DatabaseStats getStats() const {
        std::lock_guard<std::mutex> lock(mutex_);

        VectorDatabase::DatabaseStats stats;
        stats.total_vectors = vectors_.size();
        stats.total_documents = document_index_.size();

        if (!vectors_.empty()) {
            double total_magnitude = 0.0;
            for (const auto& [_, record] : vectors_) {
                double magnitude = 0.0;
                for (float val : record.embedding) {
                    magnitude += val * val;
                }
                total_magnitude += std::sqrt(magnitude);
            }
            stats.avg_embedding_magnitude = total_magnitude / vectors_.size();
        }

        // TODO: Get actual index size from LanceDB
        stats.index_size_bytes = vectors_.size() * config_.embedding_dim * sizeof(float);
        stats.last_optimized = std::chrono::system_clock::now();

        return stats;
    }

    const VectorDatabaseConfig& getConfig() const {
        return config_;
    }

    std::string getLastError() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_error_;
    }

    bool hasError() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return has_error_;
    }

private:
    void setError(const std::string& error) const {
        last_error_ = error;
        has_error_ = true;
    }

    double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) const {
        if (a.size() != b.size()) {
            return 0.0;
        }

        double dot_product = 0.0;
        double norm_a = 0.0;
        double norm_b = 0.0;

        for (size_t i = 0; i < a.size(); ++i) {
            dot_product += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }

        norm_a = std::sqrt(norm_a);
        norm_b = std::sqrt(norm_b);

        if (norm_a == 0.0 || norm_b == 0.0) {
            return 0.0;
        }

        return dot_product / (norm_a * norm_b);
    }

    VectorDatabaseConfig config_;
    bool initialized_;
    mutable bool has_error_;
    mutable std::string last_error_;
    mutable std::mutex mutex_;

    // TODO: Replace with actual LanceDB storage
    std::unordered_map<std::string, VectorRecord> vectors_;
    std::unordered_map<std::string, std::unordered_set<std::string>> document_index_;
};

// VectorDatabase implementation

VectorDatabase::VectorDatabase(const VectorDatabaseConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

VectorDatabase::~VectorDatabase() = default;

VectorDatabase::VectorDatabase(VectorDatabase&&) noexcept = default;
VectorDatabase& VectorDatabase::operator=(VectorDatabase&&) noexcept = default;

bool VectorDatabase::initialize() {
    return pImpl->initialize();
}

bool VectorDatabase::isInitialized() const {
    return pImpl->isInitialized();
}

void VectorDatabase::close() {
    pImpl->close();
}

bool VectorDatabase::createTable() {
    return pImpl->createTable();
}

bool VectorDatabase::tableExists() const {
    return pImpl->tableExists();
}

void VectorDatabase::dropTable() {
    pImpl->dropTable();
}

size_t VectorDatabase::getVectorCount() const {
    return pImpl->getVectorCount();
}

bool VectorDatabase::insertVector(const VectorRecord& record) {
    return pImpl->insertVector(record);
}

bool VectorDatabase::insertVectorsBatch(const std::vector<VectorRecord>& records) {
    return pImpl->insertVectorsBatch(records);
}

bool VectorDatabase::updateVector(const std::string& chunk_id, const VectorRecord& record) {
    return pImpl->updateVector(chunk_id, record);
}

bool VectorDatabase::deleteVector(const std::string& chunk_id) {
    return pImpl->deleteVector(chunk_id);
}

bool VectorDatabase::deleteVectorsByDocument(const std::string& document_hash) {
    return pImpl->deleteVectorsByDocument(document_hash);
}

std::vector<VectorRecord> VectorDatabase::searchSimilar(
    const std::vector<float>& query_embedding,
    const VectorSearchParams& params) const {
    return pImpl->searchSimilar(query_embedding, params);
}

std::vector<VectorRecord> VectorDatabase::searchSimilarToDocument(
    const std::string& document_hash,
    const VectorSearchParams& params) const {
    
    auto document_vectors = pImpl->getVectorsByDocument(document_hash);
    if (document_vectors.empty()) {
        return {};
    }

    // Use the first chunk's embedding as the query
    // TODO: Could implement more sophisticated document-level embeddings
    return searchSimilar(document_vectors[0].embedding, params);
}

std::optional<VectorRecord> VectorDatabase::getVector(const std::string& chunk_id) const {
    return pImpl->getVector(chunk_id);
}

std::vector<VectorRecord> VectorDatabase::getVectorsByDocument(const std::string& document_hash) const {
    return pImpl->getVectorsByDocument(document_hash);
}

bool VectorDatabase::buildIndex() {
    return pImpl->buildIndex();
}

bool VectorDatabase::optimizeIndex() {
    return pImpl->optimizeIndex();
}

void VectorDatabase::compactDatabase() {
    pImpl->optimizeIndex(); // For now, optimization serves as compaction
}

bool VectorDatabase::rebuildIndex() {
    return pImpl->buildIndex();
}

VectorDatabase::DatabaseStats VectorDatabase::getStats() const {
    return pImpl->getStats();
}

const VectorDatabaseConfig& VectorDatabase::getConfig() const {
    return pImpl->getConfig();
}

std::string VectorDatabase::getLastError() const {
    return pImpl->getLastError();
}

bool VectorDatabase::hasError() const {
    return pImpl->hasError();
}

bool VectorDatabase::isValidEmbedding(const std::vector<float>& embedding, size_t expected_dim) {
    if (embedding.size() != expected_dim) {
        return false;
    }

    // Check for NaN or infinite values
    for (float val : embedding) {
        if (!std::isfinite(val)) {
            return false;
        }
    }

    return true;
}

double VectorDatabase::computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    if (a.size() != b.size()) {
        return 0.0;
    }

    double dot_product = 0.0;
    double norm_a = 0.0;
    double norm_b = 0.0;

    for (size_t i = 0; i < a.size(); ++i) {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    norm_a = std::sqrt(norm_a);
    norm_b = std::sqrt(norm_b);

    if (norm_a == 0.0 || norm_b == 0.0) {
        return 0.0;
    }

    return dot_product / (norm_a * norm_b);
}

// Factory function
std::unique_ptr<VectorDatabase> createVectorDatabase(const VectorDatabaseConfig& config) {
    auto db = std::make_unique<VectorDatabase>(config);
    if (!db->initialize()) {
        return nullptr;
    }
    return db;
}

// Utility functions
namespace utils {

std::vector<float> normalizeVector(const std::vector<float>& vec) {
    double norm = 0.0;
    for (float val : vec) {
        norm += val * val;
    }
    norm = std::sqrt(norm);

    if (norm == 0.0) {
        return vec; // Return original vector if zero
    }

    std::vector<float> normalized;
    normalized.reserve(vec.size());
    for (float val : vec) {
        normalized.push_back(static_cast<float>(val / norm));
    }

    return normalized;
}

std::string generateChunkId(const std::string& document_hash, size_t chunk_index) {
    // Generate a deterministic but unique chunk ID
    std::stringstream ss;
    ss << document_hash << "_chunk_" << std::setfill('0') << std::setw(6) << chunk_index;
    return ss.str();
}

bool validateVectorRecord(const VectorRecord& record, size_t expected_dim) {
    if (record.chunk_id.empty() || record.document_hash.empty()) {
        return false;
    }

    if (!VectorDatabase::isValidEmbedding(record.embedding, expected_dim)) {
        return false;
    }

    if (record.start_offset > record.end_offset && record.end_offset != 0) {
        return false;
    }

    return true;
}

double similarityToDistance(double similarity) {
    // Convert cosine similarity [-1, 1] to distance [0, 2]
    return 1.0 - similarity;
}

double distanceToSimilarity(double distance) {
    // Convert distance [0, 2] to cosine similarity [-1, 1]
    return 1.0 - distance;
}

} // namespace utils

} // namespace yams::vector