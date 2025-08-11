#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <fstream>
#include <sstream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <queue>
#include <set>
#include <limits>

namespace yams::vector {

// =============================================================================
// Flat Index Implementation (Brute Force)
// =============================================================================

class FlatIndex : public VectorIndex {
public:
    explicit FlatIndex(const IndexConfig& config) 
        : VectorIndex(config) {
        vectors_.reserve(config.max_elements);
    }
    
    Result<void> add(const std::string& id, const std::vector<float>& vector) override {
        if (vector.size() != config_.dimension) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Vector dimension mismatch"});
        }
        
        std::unique_lock lock(mutex_);
        
        if (id_to_index_.find(id) != id_to_index_.end()) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Vector with this ID already exists"});
        }
        
        size_t index = vectors_.size();
        vectors_.push_back(config_.normalize_vectors ? 
                          vector_utils::normalize(vector) : vector);
        ids_.push_back(id);
        id_to_index_[id] = index;
        
        stats_.num_vectors++;
        return Result<void>();
    }
    
    Result<void> update(const std::string& id, const std::vector<float>& vector) override {
        if (vector.size() != config_.dimension) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Vector dimension mismatch"});
        }
        
        std::unique_lock lock(mutex_);
        
        auto it = id_to_index_.find(id);
        if (it == id_to_index_.end()) {
            return Result<void>(Error{ErrorCode::NotFound, "Vector not found"});
        }
        
        vectors_[it->second] = config_.normalize_vectors ? 
                               vector_utils::normalize(vector) : vector;
        return Result<void>();
    }
    
    Result<void> remove(const std::string& id) override {
        std::unique_lock lock(mutex_);
        
        auto it = id_to_index_.find(id);
        if (it == id_to_index_.end()) {
            return Result<void>(Error{ErrorCode::NotFound, "Vector not found"});
        }
        
        size_t index = it->second;
        
        // Mark as deleted (don't actually remove to preserve indices)
        deleted_indices_.insert(index);
        id_to_index_.erase(it);
        stats_.num_vectors--;
        
        return Result<void>();
    }
    
    Result<std::vector<SearchResult>> search(
        const std::vector<float>& query, 
        size_t k,
        const SearchFilter* filter) override {
        
        if (query.size() != config_.dimension) {
            return Result<std::vector<SearchResult>>(Error{ErrorCode::InvalidArgument, "Query dimension mismatch"});
        }
        
        std::shared_lock lock(mutex_);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<float> normalized_query = config_.normalize_vectors ? 
                                              vector_utils::normalize(query) : query;
        
        // Calculate distances to all vectors
        std::vector<std::pair<float, size_t>> distances;
        distances.reserve(vectors_.size());
        
        for (size_t i = 0; i < vectors_.size(); ++i) {
            // Skip deleted vectors
            if (deleted_indices_.find(i) != deleted_indices_.end()) {
                continue;
            }
            
            // Apply filters
            if (filter && filter->hasFilters()) {
                if (!filter->exclude_ids.empty()) {
                    if (std::find(filter->exclude_ids.begin(), 
                                 filter->exclude_ids.end(), 
                                 ids_[i]) != filter->exclude_ids.end()) {
                        continue;
                    }
                }
                
                if (filter->custom_filter) {
                    if (!filter->custom_filter(ids_[i], {})) {
                        continue;
                    }
                }
            }
            
            float dist = vector_utils::calculateDistance(
                normalized_query, vectors_[i], config_.distance_metric);
            
            // Apply distance filter
            if (filter && filter->max_distance.has_value()) {
                if (dist > filter->max_distance.value()) {
                    continue;
                }
            }
            
            distances.emplace_back(dist, i);
        }
        
        // Sort by distance and take top k
        size_t result_size = std::min(k, distances.size());
        std::partial_sort(distances.begin(), 
                         distances.begin() + result_size,
                         distances.end());
        
        // Build results
        std::vector<SearchResult> results;
        results.reserve(result_size);
        
        for (size_t i = 0; i < result_size; ++i) {
            SearchResult result;
            result.id = ids_[distances[i].second];
            result.distance = distances[i].first;
            result.similarity = vector_utils::distanceToSimilarity(
                result.distance, config_.distance_metric);
            
            // Apply similarity filter
            if (filter && filter->min_similarity.has_value()) {
                if (result.similarity < filter->min_similarity.value()) {
                    break;  // Results are sorted, so we can stop
                }
            }
            
            results.push_back(std::move(result));
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Update statistics
        stats_.total_searches++;
        stats_.avg_search_time_ms = (stats_.avg_search_time_ms * (stats_.total_searches - 1) + 
                                     duration.count() / 1000.0) / stats_.total_searches;
        
        return Result<std::vector<SearchResult>>(results);
    }
    
    Result<void> addBatch(
        const std::vector<std::string>& ids,
        const std::vector<std::vector<float>>& vectors) override {
        
        if (ids.size() != vectors.size()) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "IDs and vectors size mismatch"});
        }
        
        std::unique_lock lock(mutex_);
        
        for (size_t i = 0; i < ids.size(); ++i) {
            if (vectors[i].size() != config_.dimension) {
                return Result<void>(Error{ErrorCode::InvalidArgument, "Vector dimension mismatch at index " + 
                                         std::to_string(i)});
            }
            
            if (id_to_index_.find(ids[i]) != id_to_index_.end()) {
                return Result<void>(Error{ErrorCode::InvalidArgument, "Duplicate ID: " + ids[i]});
            }
        }
        
        // Reserve space
        size_t current_size = vectors_.size();
        vectors_.reserve(current_size + ids.size());
        ids_.reserve(current_size + ids.size());
        
        // Add all vectors
        for (size_t i = 0; i < ids.size(); ++i) {
            size_t index = vectors_.size();
            vectors_.push_back(config_.normalize_vectors ? 
                             vector_utils::normalize(vectors[i]) : vectors[i]);
            ids_.push_back(ids[i]);
            id_to_index_[ids[i]] = index;
        }
        
        stats_.num_vectors += ids.size();
        return Result<void>();
    }
    
    Result<std::vector<std::vector<SearchResult>>> searchBatch(
        const std::vector<std::vector<float>>& queries,
        size_t k,
        const SearchFilter* filter) override {
        
        std::vector<std::vector<SearchResult>> results;
        results.reserve(queries.size());
        
        for (const auto& query : queries) {
            auto result = search(query, k, filter);
            if (!result.has_value()) {
                return Result<std::vector<std::vector<SearchResult>>>(result.error());
            }
            results.push_back(result.value());
        }
        
        return Result<std::vector<std::vector<SearchResult>>>(results);
    }
    
    Result<void> save(const std::string& path) override {
        std::shared_lock lock(mutex_);
        
        std::ofstream file(path, std::ios::binary);
        if (!file) {
            return Result<void>(Error{ErrorCode::InternalError, "Failed to open file for writing"});
        }
        
        // Write header
        size_t num_vectors = vectors_.size() - deleted_indices_.size();
        file.write(reinterpret_cast<const char*>(&num_vectors), sizeof(num_vectors));
        file.write(reinterpret_cast<const char*>(&config_.dimension), sizeof(config_.dimension));
        
        // Write vectors and IDs
        for (size_t i = 0; i < vectors_.size(); ++i) {
            if (deleted_indices_.find(i) != deleted_indices_.end()) {
                continue;
            }
            
            // Write ID length and ID
            size_t id_len = ids_[i].size();
            file.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
            file.write(ids_[i].data(), id_len);
            
            // Write vector
            file.write(reinterpret_cast<const char*>(vectors_[i].data()), 
                      config_.dimension * sizeof(float));
        }
        
        return Result<void>();
    }
    
    Result<void> load(const std::string& path) override {
        std::unique_lock lock(mutex_);
        
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            return Result<void>(Error{ErrorCode::InternalError, "Failed to open file for reading"});
        }
        
        // Read header
        size_t num_vectors, dimension;
        file.read(reinterpret_cast<char*>(&num_vectors), sizeof(num_vectors));
        file.read(reinterpret_cast<char*>(&dimension), sizeof(dimension));
        
        if (dimension != config_.dimension) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Dimension mismatch"});
        }
        
        // Clear existing data
        vectors_.clear();
        ids_.clear();
        id_to_index_.clear();
        deleted_indices_.clear();
        
        // Reserve space
        vectors_.reserve(num_vectors);
        ids_.reserve(num_vectors);
        
        // Read vectors and IDs
        for (size_t i = 0; i < num_vectors; ++i) {
            // Read ID
            size_t id_len;
            file.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));
            std::string id(id_len, '\0');
            file.read(&id[0], id_len);
            
            // Read vector
            std::vector<float> vector(dimension);
            file.read(reinterpret_cast<char*>(vector.data()), 
                     dimension * sizeof(float));
            
            // Add to index
            size_t index = vectors_.size();
            vectors_.push_back(std::move(vector));
            ids_.push_back(id);
            id_to_index_[id] = index;
        }
        
        stats_.num_vectors = num_vectors;
        return Result<void>();
    }
    
    Result<void> optimize() override {
        std::unique_lock lock(mutex_);
        
        if (deleted_indices_.empty()) {
            return Result<void>();
        }
        
        // Compact vectors by removing deleted entries
        std::vector<std::vector<float>> new_vectors;
        std::vector<std::string> new_ids;
        std::unordered_map<std::string, size_t> new_id_to_index;
        
        new_vectors.reserve(vectors_.size() - deleted_indices_.size());
        new_ids.reserve(vectors_.size() - deleted_indices_.size());
        
        for (size_t i = 0; i < vectors_.size(); ++i) {
            if (deleted_indices_.find(i) == deleted_indices_.end()) {
                size_t new_index = new_vectors.size();
                new_vectors.push_back(std::move(vectors_[i]));
                new_ids.push_back(std::move(ids_[i]));
                new_id_to_index[new_ids.back()] = new_index;
            }
        }
        
        vectors_ = std::move(new_vectors);
        ids_ = std::move(new_ids);
        id_to_index_ = std::move(new_id_to_index);
        deleted_indices_.clear();
        
        return Result<void>();
    }
    
    bool needsOptimization() const override {
        std::shared_lock lock(mutex_);
        // Optimize if more than 10% of vectors are deleted
        return deleted_indices_.size() > vectors_.size() * 0.1;
    }
    
    size_t size() const override {
        std::shared_lock lock(mutex_);
        return vectors_.size() - deleted_indices_.size();
    }
    
    size_t dimension() const override {
        return config_.dimension;
    }
    
    IndexType type() const override {
        return IndexType::FLAT;
    }
    
    IndexStats getStats() const override {
        std::shared_lock lock(mutex_);
        stats_.dimension = config_.dimension;
        stats_.type = IndexType::FLAT;
        stats_.metric = config_.distance_metric;
        stats_.memory_usage_bytes = vectors_.size() * config_.dimension * sizeof(float) +
                                   ids_.size() * sizeof(std::string);
        stats_.fragmentation_ratio = static_cast<double>(deleted_indices_.size()) / 
                                    std::max(size_t(1), vectors_.size());
        stats_.needs_optimization = needsOptimization();
        return stats_;
    }
    
private:
    mutable std::shared_mutex mutex_;
    std::vector<std::vector<float>> vectors_;
    std::vector<std::string> ids_;
    std::unordered_map<std::string, size_t> id_to_index_;
    std::set<size_t> deleted_indices_;
};

// =============================================================================
// HNSW Index Implementation (Placeholder)
// =============================================================================

class HNSWIndex : public VectorIndex {
public:
    explicit HNSWIndex(const IndexConfig& config) 
        : VectorIndex(config) {
        // TODO: Implement HNSW graph structure
    }
    
    Result<void> add(const std::string& id, const std::vector<float>& vector) override {
        // Placeholder - use flat index internally for now
        if (!flat_index_) {
            flat_index_ = std::make_unique<FlatIndex>(config_);
        }
        return flat_index_->add(id, vector);
    }
    
    Result<void> update(const std::string& id, const std::vector<float>& vector) override {
        if (!flat_index_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        return flat_index_->update(id, vector);
    }
    
    Result<void> remove(const std::string& id) override {
        if (!flat_index_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        return flat_index_->remove(id);
    }
    
    Result<std::vector<SearchResult>> search(
        const std::vector<float>& query, 
        size_t k,
        const SearchFilter* filter) override {
        if (!flat_index_) {
            return Result<std::vector<SearchResult>>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        return flat_index_->search(query, k, filter);
    }
    
    Result<void> addBatch(
        const std::vector<std::string>& ids,
        const std::vector<std::vector<float>>& vectors) override {
        if (!flat_index_) {
            flat_index_ = std::make_unique<FlatIndex>(config_);
        }
        return flat_index_->addBatch(ids, vectors);
    }
    
    Result<std::vector<std::vector<SearchResult>>> searchBatch(
        const std::vector<std::vector<float>>& queries,
        size_t k,
        const SearchFilter* filter) override {
        if (!flat_index_) {
            return Result<std::vector<std::vector<SearchResult>>>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        return flat_index_->searchBatch(queries, k, filter);
    }
    
    Result<void> save(const std::string& path) override {
        if (!flat_index_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        return flat_index_->save(path);
    }
    
    Result<void> load(const std::string& path) override {
        if (!flat_index_) {
            flat_index_ = std::make_unique<FlatIndex>(config_);
        }
        return flat_index_->load(path);
    }
    
    Result<void> optimize() override {
        if (!flat_index_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        return flat_index_->optimize();
    }
    
    bool needsOptimization() const override {
        return flat_index_ ? flat_index_->needsOptimization() : false;
    }
    
    size_t size() const override {
        return flat_index_ ? flat_index_->size() : 0;
    }
    
    size_t dimension() const override {
        return config_.dimension;
    }
    
    IndexType type() const override {
        return IndexType::HNSW;
    }
    
    IndexStats getStats() const override {
        if (flat_index_) {
            auto stats = flat_index_->getStats();
            stats.type = IndexType::HNSW;
            return stats;
        }
        return IndexStats{};
    }
    
private:
    std::unique_ptr<FlatIndex> flat_index_;  // Temporary fallback
    // TODO: Add HNSW graph structure
};

// =============================================================================
// Vector Index Manager Implementation
// =============================================================================

class VectorIndexManager::Impl {
public:
    explicit Impl(const IndexConfig& config) 
        : config_(config), initialized_(false) {}
    
    Result<void> initialize() {
        std::unique_lock lock(mutex_);
        
        if (initialized_) {
            return Result<void>();
        }
        
        // Create main index
        main_index_ = createVectorIndex(config_);
        if (!main_index_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Failed to create index"});
        }
        
        // Create delta index if enabled
        if (config_.enable_delta_index) {
            IndexConfig delta_config = config_;
            delta_config.type = IndexType::FLAT;  // Always use flat for delta
            delta_index_ = std::make_unique<FlatIndex>(delta_config);
        }
        
        initialized_ = true;
        return Result<void>();
    }
    
    bool isInitialized() const {
        std::shared_lock lock(mutex_);
        return initialized_;
    }
    
    void shutdown() {
        std::unique_lock lock(mutex_);
        main_index_.reset();
        delta_index_.reset();
        metadata_store_.clear();
        initialized_ = false;
    }
    
    Result<void> addVector(
        const std::string& id, 
        const std::vector<float>& vector,
        const std::map<std::string, std::string>& metadata) {
        
        if (!initialized_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        
        // Store metadata
        if (!metadata.empty()) {
            std::unique_lock lock(metadata_mutex_);
            metadata_store_[id] = metadata;
        }
        
        // Add to delta index if enabled, otherwise to main
        if (delta_index_) {
            auto result = delta_index_->add(id, vector);
            
            // Check if delta needs merging
            if (delta_index_->size() >= config_.delta_threshold) {
                auto merge_result = mergeDeltaIndex();
                if (!merge_result.has_value()) {
                    return merge_result;
                }
            }
            
            return result;
        } else {
            return main_index_->add(id, vector);
        }
    }
    
    Result<void> addVectors(
        const std::vector<std::string>& ids, 
        const std::vector<std::vector<float>>& vectors,
        const std::vector<std::map<std::string, std::string>>& metadata) {
        
        if (!initialized_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        
        // Store metadata
        if (!metadata.empty()) {
            std::unique_lock lock(metadata_mutex_);
            for (size_t i = 0; i < std::min(ids.size(), metadata.size()); ++i) {
                if (!metadata[i].empty()) {
                    metadata_store_[ids[i]] = metadata[i];
                }
            }
        }
        
        // Add to appropriate index
        if (delta_index_) {
            auto result = delta_index_->addBatch(ids, vectors);
            
            // Check if delta needs merging
            if (delta_index_->size() >= config_.delta_threshold) {
                auto merge_result = mergeDeltaIndex();
                if (!merge_result.has_value()) {
                    return merge_result;
                }
            }
            
            return result;
        } else {
            return main_index_->addBatch(ids, vectors);
        }
    }
    
    Result<std::vector<SearchResult>> search(
        const std::vector<float>& query,
        size_t k,
        const SearchFilter& filter) {
        
        if (!initialized_) {
            return Result<std::vector<SearchResult>>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        
        std::vector<SearchResult> all_results;
        
        // Search main index
        auto main_results = main_index_->search(query, k, &filter);
        if (!main_results.has_value()) {
            return main_results;
        }
        
        all_results = main_results.value();
        
        // Search delta index if present
        if (delta_index_ && delta_index_->size() > 0) {
            auto delta_results = delta_index_->search(query, k, &filter);
            if (delta_results.has_value()) {
                // Merge and re-sort results
                all_results.insert(all_results.end(), 
                                  delta_results.value().begin(), 
                                  delta_results.value().end());
                
                std::sort(all_results.begin(), all_results.end());
                
                // Keep only top k
                if (all_results.size() > k) {
                    all_results.resize(k);
                }
            }
        }
        
        // Add metadata to results
        if (!metadata_store_.empty()) {
            std::shared_lock lock(metadata_mutex_);
            for (auto& result : all_results) {
                auto it = metadata_store_.find(result.id);
                if (it != metadata_store_.end()) {
                    result.metadata = it->second;
                }
            }
        }
        
        return Result<std::vector<SearchResult>>(all_results);
    }
    
    Result<void> mergeDeltaIndex() {
        if (!delta_index_ || delta_index_->size() == 0) {
            return Result<void>();
        }
        
        // TODO: Implement actual merging
        // For now, just clear delta index
        delta_index_ = std::make_unique<FlatIndex>(config_);
        
        return Result<void>();
    }
    
    Result<void> optimizeIndex() {
        if (!initialized_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        
        auto result = main_index_->optimize();
        if (!result.has_value()) {
            return result;
        }
        
        if (delta_index_) {
            return delta_index_->optimize();
        }
        
        return Result<void>();
    }
    
    IndexStats getStats() const {
        if (!initialized_) {
            return IndexStats{};
        }
        
        auto stats = main_index_->getStats();
        
        if (delta_index_) {
            stats.delta_index_size = delta_index_->size();
        }
        
        return stats;
    }
    
    size_t size() const {
        if (!initialized_) {
            return 0;
        }
        
        size_t total = main_index_->size();
        if (delta_index_) {
            total += delta_index_->size();
        }
        return total;
    }
    
    Result<std::vector<std::string>> getAllVectorIds() const {
        if (!initialized_) {
            return Result<std::vector<std::string>>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }
        
        std::vector<std::string> ids;
        
        // Get IDs from metadata store (most reliable approach)
        {
            std::shared_lock lock(metadata_mutex_);
            ids.reserve(metadata_store_.size());
            for (const auto& [id, metadata] : metadata_store_) {
                ids.push_back(id);
            }
        }
        
        return Result<std::vector<std::string>>(std::move(ids));
    }
    
    IndexType getIndexType() const {
        return config_.type;
    }
    
private:
    mutable std::shared_mutex mutex_;
    mutable std::shared_mutex metadata_mutex_;
    
    IndexConfig config_;
    bool initialized_;
    
    std::unique_ptr<VectorIndex> main_index_;
    std::unique_ptr<VectorIndex> delta_index_;
    
    std::unordered_map<std::string, std::map<std::string, std::string>> metadata_store_;
};

// =============================================================================
// VectorIndexManager Public Interface
// =============================================================================

VectorIndexManager::VectorIndexManager(const IndexConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

VectorIndexManager::~VectorIndexManager() = default;
VectorIndexManager::VectorIndexManager(VectorIndexManager&&) noexcept = default;
VectorIndexManager& VectorIndexManager::operator=(VectorIndexManager&&) noexcept = default;

Result<void> VectorIndexManager::initialize() {
    return pImpl->initialize();
}

bool VectorIndexManager::isInitialized() const {
    return pImpl->isInitialized();
}

void VectorIndexManager::shutdown() {
    pImpl->shutdown();
}

Result<void> VectorIndexManager::addVector(
    const std::string& id, 
    const std::vector<float>& vector,
    const std::map<std::string, std::string>& metadata) {
    return pImpl->addVector(id, vector, metadata);
}

Result<void> VectorIndexManager::addVectors(
    const std::vector<std::string>& ids, 
    const std::vector<std::vector<float>>& vectors,
    const std::vector<std::map<std::string, std::string>>& metadata) {
    return pImpl->addVectors(ids, vectors, metadata);
}

Result<std::vector<SearchResult>> VectorIndexManager::search(
    const std::vector<float>& query,
    size_t k,
    const SearchFilter& filter) {
    return pImpl->search(query, k, filter);
}

Result<void> VectorIndexManager::optimizeIndex() {
    return pImpl->optimizeIndex();
}

Result<void> VectorIndexManager::mergeDeltaIndex() {
    return pImpl->mergeDeltaIndex();
}

IndexStats VectorIndexManager::getStats() const {
    return pImpl->getStats();
}

size_t VectorIndexManager::size() const {
    return pImpl->size();
}

IndexType VectorIndexManager::getIndexType() const {
    return pImpl->getIndexType();
}

Result<std::vector<std::string>> VectorIndexManager::getAllVectorIds() const {
    return pImpl->getAllVectorIds();
}

// Missing method implementations (stubs for now)
Result<void> VectorIndexManager::buildIndex() {
    // TODO: Implement index building logic
    return Result<void>();
}

Result<void> VectorIndexManager::removeVector(const std::string& id) {
    // TODO: Implement single vector removal
    return Error{ErrorCode::NotSupported, "removeVector not yet implemented"};
}

Result<void> VectorIndexManager::saveIndex(const std::string& path) {
    // TODO: Implement index persistence
    return Error{ErrorCode::NotSupported, "saveIndex not yet implemented"};
}

Result<void> VectorIndexManager::loadIndex(const std::string& path) {
    // TODO: Implement index loading
    return Error{ErrorCode::NotSupported, "loadIndex not yet implemented"};
}

// =============================================================================
// Factory Function
// =============================================================================

std::unique_ptr<VectorIndex> createVectorIndex(const IndexConfig& config) {
    switch (config.type) {
        case IndexType::FLAT:
            return std::make_unique<FlatIndex>(config);
        case IndexType::HNSW:
            return std::make_unique<HNSWIndex>(config);
        default:
            // Fall back to flat index for unsupported types
            return std::make_unique<FlatIndex>(config);
    }
}

// =============================================================================
// Utility Functions
// =============================================================================

namespace vector_utils {

float calculateDistance(
    const std::vector<float>& a,
    const std::vector<float>& b,
    DistanceMetric metric) {
    
    if (a.size() != b.size()) {
        return std::numeric_limits<float>::max();
    }
    
    switch (metric) {
        case DistanceMetric::COSINE: {
            float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
            for (size_t i = 0; i < a.size(); ++i) {
                dot += a[i] * b[i];
                norm_a += a[i] * a[i];
                norm_b += b[i] * b[i];
            }
            float similarity = dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
            return 1.0f - similarity;  // Convert to distance
        }
        
        case DistanceMetric::L2: {
            float sum = 0.0f;
            for (size_t i = 0; i < a.size(); ++i) {
                float diff = a[i] - b[i];
                sum += diff * diff;
            }
            return std::sqrt(sum);
        }
        
        case DistanceMetric::INNER_PRODUCT: {
            float dot = 0.0f;
            for (size_t i = 0; i < a.size(); ++i) {
                dot += a[i] * b[i];
            }
            return -dot;  // Negative because we want to minimize distance
        }
        
        default:
            return std::numeric_limits<float>::max();
    }
}

std::vector<float> normalize(const std::vector<float>& vector) {
    float norm = 0.0f;
    for (float val : vector) {
        norm += val * val;
    }
    norm = std::sqrt(norm);
    
    if (norm == 0.0f) {
        return vector;
    }
    
    std::vector<float> normalized;
    normalized.reserve(vector.size());
    for (float val : vector) {
        normalized.push_back(val / norm);
    }
    
    return normalized;
}

std::vector<std::vector<float>> normalizeVectors(
    const std::vector<std::vector<float>>& vectors) {
    
    std::vector<std::vector<float>> normalized;
    normalized.reserve(vectors.size());
    
    for (const auto& vector : vectors) {
        normalized.push_back(normalize(vector));
    }
    
    return normalized;
}

float distanceToSimilarity(float distance, DistanceMetric metric) {
    switch (metric) {
        case DistanceMetric::COSINE:
            return 1.0f - distance;  // Cosine distance is 1 - similarity
        
        case DistanceMetric::L2:
            return 1.0f / (1.0f + distance);  // Convert L2 to similarity
        
        case DistanceMetric::INNER_PRODUCT:
            return -distance;  // Inner product distance is negative of similarity
        
        default:
            return 0.0f;
    }
}

bool isValidDimension(const std::vector<float>& vector, size_t expected_dim) {
    return vector.size() == expected_dim;
}

std::vector<float> meanVector(const std::vector<std::vector<float>>& vectors) {
    if (vectors.empty()) {
        return {};
    }
    
    size_t dim = vectors[0].size();
    std::vector<float> mean(dim, 0.0f);
    
    for (const auto& vector : vectors) {
        for (size_t i = 0; i < dim; ++i) {
            mean[i] += vector[i];
        }
    }
    
    for (float& val : mean) {
        val /= vectors.size();
    }
    
    return mean;
}

std::vector<float> centroid(const std::vector<std::vector<float>>& vectors) {
    return meanVector(vectors);  // For now, same as mean
}

std::vector<uint8_t> quantizeVector(const std::vector<float>& vector) {
    std::vector<uint8_t> quantized;
    quantized.reserve(vector.size());
    
    for (float val : vector) {
        // Simple quantization to 8-bit
        int quantized_val = static_cast<int>((val + 1.0f) * 127.5f);
        quantized_val = std::max(0, std::min(255, quantized_val));
        quantized.push_back(static_cast<uint8_t>(quantized_val));
    }
    
    return quantized;
}

std::vector<float> dequantizeVector(const std::vector<uint8_t>& quantized, size_t dimension) {
    std::vector<float> vector;
    vector.reserve(dimension);
    
    for (size_t i = 0; i < std::min(quantized.size(), dimension); ++i) {
        float val = (quantized[i] / 127.5f) - 1.0f;
        vector.push_back(val);
    }
    
    // Pad with zeros if needed
    while (vector.size() < dimension) {
        vector.push_back(0.0f);
    }
    
    return vector;
}

} // namespace vector_utils

} // namespace yams::vector