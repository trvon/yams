#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <mutex>
#include <numeric>
#include <queue>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <thread>

// Include HNSWlib before namespace to avoid namespace conflicts
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-qual"
#endif
#include <hnswlib.h>
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
#include <unordered_map>

namespace yams::vector {

// =============================================================================
// Flat Index Implementation (Brute Force)
// =============================================================================

class FlatIndex : public VectorIndex {
public:
    explicit FlatIndex(const IndexConfig& config) : VectorIndex(config) {
        vectors_.reserve(config.max_elements);

        // Initialize stats
        stats_.type = IndexType::FLAT;
        stats_.dimension = config.dimension;
        stats_.metric = config.distance_metric;
        stats_.num_vectors = 0;
        stats_.memory_usage_bytes = 0;
        stats_.index_size_bytes = 0;
        stats_.needs_optimization = false;
        stats_.total_searches = 0;
        stats_.avg_search_time_ms = 0.0;
        stats_.fragmentation_ratio = 0.0;
        stats_.delta_index_size = 0;
    }

    Result<void> add(const std::string& id, const std::vector<float>& vector) override {
        if (vector.size() != config_.dimension) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Vector dimension mismatch"});
        }

        std::unique_lock lock(mutex_);

        if (id_to_index_.find(id) != id_to_index_.end()) {
            return Result<void>(
                Error{ErrorCode::InvalidArgument, "Vector with this ID already exists"});
        }

        size_t index = vectors_.size();
        vectors_.push_back(config_.normalize_vectors ? vector_utils::normalize(vector) : vector);
        ids_.push_back(id);
        id_to_index_[id] = index;

        stats_.num_vectors++;
        // Update memory usage stats
        stats_.memory_usage_bytes = vectors_.size() * config_.dimension * sizeof(float) +
                                    ids_.size() * sizeof(std::string) +
                                    id_to_index_.size() * (sizeof(std::string) + sizeof(size_t));
        stats_.index_size_bytes = stats_.memory_usage_bytes;
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

        vectors_[it->second] = config_.normalize_vectors ? vector_utils::normalize(vector) : vector;
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

        // Update memory usage stats
        stats_.memory_usage_bytes = vectors_.size() * config_.dimension * sizeof(float) +
                                    ids_.size() * sizeof(std::string) +
                                    id_to_index_.size() * (sizeof(std::string) + sizeof(size_t));
        stats_.index_size_bytes = stats_.memory_usage_bytes;

        return Result<void>();
    }

    Result<std::vector<SearchResult>> search(const std::vector<float>& query, size_t k,
                                             const SearchFilter* filter) override {
        if (query.size() != config_.dimension) {
            return Result<std::vector<SearchResult>>(
                Error{ErrorCode::InvalidArgument, "Query dimension mismatch"});
        }

        std::shared_lock lock(mutex_);

        auto start = std::chrono::high_resolution_clock::now();

        std::vector<float> normalized_query =
            config_.normalize_vectors ? vector_utils::normalize(query) : query;

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
                    if (std::find(filter->exclude_ids.begin(), filter->exclude_ids.end(),
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

            float dist = vector_utils::calculateDistance(normalized_query, vectors_[i],
                                                         config_.distance_metric);

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
        if (result_size > 0) {
            std::partial_sort(distances.begin(), distances.begin() + result_size, distances.end());
        }

        // Build results
        std::vector<SearchResult> results;
        results.reserve(result_size);

        for (size_t i = 0; i < result_size; ++i) {
            SearchResult result;
            result.id = ids_[distances[i].second];
            result.distance = distances[i].first;
            result.similarity =
                vector_utils::distanceToSimilarity(result.distance, config_.distance_metric);

            // Apply similarity filter
            if (filter && filter->min_similarity.has_value()) {
                if (result.similarity < filter->min_similarity.value()) {
                    break; // Results are sorted, so we can stop
                }
            }

            results.push_back(std::move(result));
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        // Update statistics
        stats_.total_searches++;
        stats_.avg_search_time_ms =
            (stats_.avg_search_time_ms * (stats_.total_searches - 1) + duration.count() / 1000.0) /
            stats_.total_searches;

        return Result<std::vector<SearchResult>>(results);
    }

    Result<void> addBatch(const std::vector<std::string>& ids,
                          const std::vector<std::vector<float>>& vectors) override {
        if (ids.size() != vectors.size()) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "IDs and vectors size mismatch"});
        }

        std::unique_lock lock(mutex_);

        for (size_t i = 0; i < ids.size(); ++i) {
            if (vectors[i].size() != config_.dimension) {
                return Result<void>(
                    Error{ErrorCode::InvalidArgument,
                          "Vector dimension mismatch at index " + std::to_string(i)});
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
            vectors_.push_back(config_.normalize_vectors ? vector_utils::normalize(vectors[i])
                                                         : vectors[i]);
            ids_.push_back(ids[i]);
            id_to_index_[ids[i]] = index;
        }

        stats_.num_vectors += ids.size();

        // Update memory usage stats
        stats_.memory_usage_bytes = vectors_.size() * config_.dimension * sizeof(float) +
                                    ids_.size() * sizeof(std::string) +
                                    id_to_index_.size() * (sizeof(std::string) + sizeof(size_t));
        stats_.index_size_bytes = stats_.memory_usage_bytes;

        return Result<void>();
    }

    Result<std::vector<std::vector<SearchResult>>>
    searchBatch(const std::vector<std::vector<float>>& queries, size_t k,
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
            file.read(reinterpret_cast<char*>(vector.data()), dimension * sizeof(float));

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

    size_t dimension() const override { return config_.dimension; }

    IndexType type() const override { return IndexType::FLAT; }

    IndexStats getStats() const override {
        std::shared_lock lock(mutex_);
        stats_.dimension = config_.dimension;
        stats_.type = IndexType::FLAT;
        stats_.metric = config_.distance_metric;
        // Ensure consistency with the updates in add/remove/addBatch
        stats_.memory_usage_bytes = vectors_.size() * config_.dimension * sizeof(float) +
                                    ids_.size() * sizeof(std::string) +
                                    id_to_index_.size() * (sizeof(std::string) + sizeof(size_t));
        stats_.index_size_bytes = stats_.memory_usage_bytes;
        stats_.fragmentation_ratio =
            static_cast<double>(deleted_indices_.size()) / std::max(size_t(1), vectors_.size());
        stats_.needs_optimization = needsOptimization();
        return stats_;
    }

    Result<void> serialize(std::ostream& out) const override {
        std::shared_lock lock(mutex_);

        try {
            // Write number of active vectors (excluding deleted)
            uint32_t num_active = vectors_.size() - deleted_indices_.size();
            out.write(reinterpret_cast<const char*>(&num_active), sizeof(num_active));

            // Write each vector (skip deleted ones)
            for (size_t i = 0; i < vectors_.size(); ++i) {
                if (deleted_indices_.find(i) != deleted_indices_.end()) {
                    continue; // Skip deleted vectors
                }

                // Write ID
                uint32_t id_len = ids_[i].size();
                out.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
                out.write(ids_[i].data(), id_len);

                // Write vector
                out.write(reinterpret_cast<const char*>(vectors_[i].data()),
                          config_.dimension * sizeof(float));
            }

            // Write deleted indices for potential recovery
            uint32_t num_deleted = deleted_indices_.size();
            out.write(reinterpret_cast<const char*>(&num_deleted), sizeof(num_deleted));
            for (size_t idx : deleted_indices_) {
                uint32_t index = static_cast<uint32_t>(idx);
                out.write(reinterpret_cast<const char*>(&index), sizeof(index));
            }

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::WriteError,
                         std::string("Failed to serialize index: ") + e.what()};
        }
    }

    Result<void> deserialize(std::istream& in) override {
        std::unique_lock lock(mutex_);

        try {
            // Clear existing data
            vectors_.clear();
            ids_.clear();
            id_to_index_.clear();
            deleted_indices_.clear();

            // Check stream state before reading
            if (!in.good()) {
                return Error{ErrorCode::InvalidData, "Input stream is not in good state"};
            }

            // Read number of vectors
            uint32_t num_vectors;
            in.read(reinterpret_cast<char*>(&num_vectors), sizeof(num_vectors));

            if (!in.good()) {
                return Error{ErrorCode::InvalidData, "Failed to read number of vectors"};
            }

            // Sanity check on num_vectors
            if (num_vectors > 1000000) { // Arbitrary large limit
                return Error{ErrorCode::InvalidData,
                             "Number of vectors seems invalid: " + std::to_string(num_vectors)};
            }

            // Reserve space
            vectors_.reserve(num_vectors);
            ids_.reserve(num_vectors);

            // Read each vector
            for (uint32_t i = 0; i < num_vectors; ++i) {
                // Read ID
                uint32_t id_len;
                in.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));

                if (!in.good()) {
                    return Error{ErrorCode::InvalidData,
                                 "Failed to read ID length for vector " + std::to_string(i)};
                }

                // Sanity check on ID length
                if (id_len > 10000) { // Arbitrary large limit
                    return Error{ErrorCode::InvalidData,
                                 "ID length seems invalid: " + std::to_string(id_len)};
                }

                std::string id(id_len, '\0');
                in.read(id.data(), id_len);

                if (!in.good()) {
                    return Error{ErrorCode::InvalidData,
                                 "Failed to read ID data for vector " + std::to_string(i)};
                }

                // Read vector
                std::vector<float> vector(config_.dimension);
                in.read(reinterpret_cast<char*>(vector.data()), config_.dimension * sizeof(float));

                if (!in.good()) {
                    return Error{ErrorCode::InvalidData,
                                 "Failed to read vector data for vector " + std::to_string(i)};
                }

                // Add to structures
                size_t index = vectors_.size();
                vectors_.push_back(std::move(vector));
                ids_.push_back(id);
                id_to_index_[id] = index;
            }

            // Read deleted indices (for information only, we don't restore deleted vectors)
            uint32_t num_deleted;
            in.read(reinterpret_cast<char*>(&num_deleted), sizeof(num_deleted));

            if (!in.good()) {
                return Error{ErrorCode::InvalidData, "Failed to read number of deleted indices"};
            }

            // Sanity check on num_deleted
            if (num_deleted > 1000000) { // Arbitrary large limit
                return Error{ErrorCode::InvalidData, "Number of deleted indices seems invalid: " +
                                                         std::to_string(num_deleted)};
            }

            for (uint32_t i = 0; i < num_deleted; ++i) {
                uint32_t index;
                in.read(reinterpret_cast<char*>(&index), sizeof(index));

                if (!in.good()) {
                    return Error{ErrorCode::InvalidData,
                                 "Failed to read deleted index " + std::to_string(i)};
                }
                // We don't restore deleted indices since we only saved active vectors
            }

            // Update stats
            stats_.num_vectors = vectors_.size();

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData,
                         std::string("Failed to deserialize index: ") + e.what()};
        }
    }

    std::vector<std::string> getAllIds() const override {
        std::shared_lock lock(mutex_);
        std::vector<std::string> result;
        result.reserve(vectors_.size() - deleted_indices_.size());

        for (size_t i = 0; i < ids_.size(); ++i) {
            if (deleted_indices_.find(i) == deleted_indices_.end()) {
                result.push_back(ids_[i]);
            }
        }

        return result;
    }

    Result<std::vector<float>> getVector(const std::string& id) const override {
        std::shared_lock lock(mutex_);

        auto it = id_to_index_.find(id);
        if (it == id_to_index_.end()) {
            return Error{ErrorCode::NotFound, "Vector not found: " + id};
        }

        size_t index = it->second;
        if (deleted_indices_.find(index) != deleted_indices_.end()) {
            return Error{ErrorCode::NotFound, "Vector was deleted: " + id};
        }

        return vectors_[index];
    }

private:
    mutable std::shared_mutex mutex_;
    std::vector<std::vector<float>> vectors_;
    std::vector<std::string> ids_;
    std::unordered_map<std::string, size_t> id_to_index_;
    std::set<size_t> deleted_indices_;
};

// =============================================================================
// HNSW Index Implementation using HNSWlib
// =============================================================================

class HNSWIndex : public VectorIndex {
public:
    explicit HNSWIndex(const IndexConfig& config) : VectorIndex(config), next_label_(0) {
        // Initialize stats
        stats_.type = IndexType::HNSW;
        stats_.dimension = config.dimension;
        stats_.metric = config.distance_metric;
        stats_.num_vectors = 0;
        stats_.memory_usage_bytes = 0;
        stats_.index_size_bytes = 0;
        stats_.needs_optimization = false;
        stats_.total_searches = 0;
        stats_.avg_search_time_ms = 0.0;
        stats_.fragmentation_ratio = 0.0;
        stats_.delta_index_size = 0;

        initializeIndex();
    }

    Result<void> add(const std::string& id, const std::vector<float>& vector) override {
        if (vector.size() != config_.dimension) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Vector dimension mismatch"});
        }

        std::unique_lock lock(mutex_);

        // Check if ID already exists
        if (id_to_label_.find(id) != id_to_label_.end()) {
            return Result<void>(
                Error{ErrorCode::InvalidArgument, "Vector with this ID already exists"});
        }

        // Check capacity
        if (next_label_ >= config_.max_elements) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index at maximum capacity"});
        }

        try {
            // Normalize vector if needed
            std::vector<float> normalized_vector =
                config_.normalize_vectors ? vector_utils::normalize(vector) : vector;

            // Map string ID to numeric label
            hnswlib::labeltype label = next_label_++;
            id_to_label_[id] = label;
            label_to_id_[label] = id;

            // Store the vector for later retrieval
            stored_vectors_[label] = normalized_vector;

            // Add to HNSW index
            hnsw_index_->addPoint(normalized_vector.data(), label);

            stats_.num_vectors++;
            updateMemoryStats();

            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(
                Error{ErrorCode::InternalError, std::string("Failed to add vector: ") + e.what()});
        }
    }

    Result<void> update(const std::string& id, const std::vector<float>& vector) override {
        if (vector.size() != config_.dimension) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Vector dimension mismatch"});
        }

        std::unique_lock lock(mutex_);

        auto it = id_to_label_.find(id);
        if (it == id_to_label_.end()) {
            return Result<void>(Error{ErrorCode::NotFound, "Vector not found"});
        }

        try {
            hnswlib::labeltype label = it->second;

            // Normalize vector if needed
            std::vector<float> normalized_vector =
                config_.normalize_vectors ? vector_utils::normalize(vector) : vector;

            // Update stored vector
            stored_vectors_[label] = normalized_vector;

            // Mark as deleted and re-add (HNSWlib's update mechanism)
            hnsw_index_->markDelete(label);
            hnsw_index_->addPoint(normalized_vector.data(), label, true); // replace_deleted = true

            // Update memory stats (vector count doesn't change but memory might)
            updateMemoryStats();

            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(Error{ErrorCode::InternalError,
                                      std::string("Failed to update vector: ") + e.what()});
        }
    }

    Result<void> remove(const std::string& id) override {
        std::unique_lock lock(mutex_);

        auto it = id_to_label_.find(id);
        if (it == id_to_label_.end()) {
            return Result<void>(Error{ErrorCode::NotFound, "Vector not found"});
        }

        try {
            hnswlib::labeltype label = it->second;

            // Mark as deleted in HNSW
            hnsw_index_->markDelete(label);

            // Remove from mappings
            stored_vectors_.erase(label);
            label_to_id_.erase(label);
            id_to_label_.erase(it);

            stats_.num_vectors--;
            updateMemoryStats();

            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(Error{ErrorCode::InternalError,
                                      std::string("Failed to remove vector: ") + e.what()});
        }
    }

    Result<std::vector<SearchResult>> search(const std::vector<float>& query, size_t k,
                                             const SearchFilter* filter) override {
        if (query.size() != config_.dimension) {
            return Result<std::vector<SearchResult>>(
                Error{ErrorCode::InvalidArgument, "Query dimension mismatch"});
        }

        std::shared_lock lock(mutex_);

        if (stats_.num_vectors == 0) {
            return Result<std::vector<SearchResult>>(std::vector<SearchResult>{});
        }

        auto start = std::chrono::high_resolution_clock::now();

        try {
            // Normalize query if needed
            std::vector<float> normalized_query =
                config_.normalize_vectors ? vector_utils::normalize(query) : query;

            // Set search parameters
            hnsw_index_->setEf(config_.hnsw_ef_search);

            // Perform k-NN search (search more if we have filters)
            size_t search_k =
                filter && filter->hasFilters() ? std::min(k * 10, stats_.num_vectors) : k;
            auto result_pairs = hnsw_index_->searchKnn(normalized_query.data(), search_k);

            // Convert results to our format and apply filters
            std::vector<SearchResult> results;
            results.reserve(k);

            while (!result_pairs.empty() && results.size() < k) {
                auto [dist, label] = result_pairs.top();
                result_pairs.pop();

                // Find the ID for this label
                auto id_it = label_to_id_.find(label);
                if (id_it == label_to_id_.end()) {
                    continue; // Deleted or invalid
                }

                const std::string& id = id_it->second;

                // Apply filters
                if (filter && filter->hasFilters()) {
                    // Check exclude list
                    if (!filter->exclude_ids.empty()) {
                        if (std::find(filter->exclude_ids.begin(), filter->exclude_ids.end(), id) !=
                            filter->exclude_ids.end()) {
                            continue;
                        }
                    }

                    // Check max distance
                    if (filter->max_distance.has_value() && dist > filter->max_distance.value()) {
                        continue;
                    }

                    // Check custom filter
                    if (filter->custom_filter && !filter->custom_filter(id, {})) {
                        continue;
                    }
                }

                SearchResult result;
                result.id = id;
                result.distance = dist;
                result.similarity =
                    vector_utils::distanceToSimilarity(dist, config_.distance_metric);

                // Apply similarity filter
                if (filter && filter->min_similarity.has_value()) {
                    if (result.similarity < filter->min_similarity.value()) {
                        continue;
                    }
                }

                results.push_back(std::move(result));
            }

            // Reverse to get best results first (priority queue gives worst first)
            std::reverse(results.begin(), results.end());

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

            // Update statistics
            stats_.total_searches++;
            stats_.avg_search_time_ms = (stats_.avg_search_time_ms * (stats_.total_searches - 1) +
                                         duration.count() / 1000.0) /
                                        stats_.total_searches;

            return Result<std::vector<SearchResult>>(results);
        } catch (const std::exception& e) {
            return Result<std::vector<SearchResult>>(
                Error{ErrorCode::InternalError, std::string("Search failed: ") + e.what()});
        }
    }

    Result<void> addBatch(const std::vector<std::string>& ids,
                          const std::vector<std::vector<float>>& vectors) override {
        if (ids.size() != vectors.size()) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "IDs and vectors size mismatch"});
        }

        std::unique_lock lock(mutex_);

        // Validate all vectors first
        for (size_t i = 0; i < vectors.size(); ++i) {
            if (vectors[i].size() != config_.dimension) {
                return Result<void>(
                    Error{ErrorCode::InvalidArgument,
                          "Vector dimension mismatch at index " + std::to_string(i)});
            }
            if (id_to_label_.find(ids[i]) != id_to_label_.end()) {
                return Result<void>(
                    Error{ErrorCode::InvalidArgument,
                          "Duplicate ID at index " + std::to_string(i) + ": " + ids[i]});
            }
        }

        try {
            // Add all vectors
            for (size_t i = 0; i < ids.size(); ++i) {
                // Normalize vector if needed
                std::vector<float> normalized_vector =
                    config_.normalize_vectors ? vector_utils::normalize(vectors[i]) : vectors[i];

                // Map string ID to numeric label
                hnswlib::labeltype label = next_label_++;
                id_to_label_[ids[i]] = label;
                label_to_id_[label] = ids[i];
                stored_vectors_[label] = normalized_vector;

                // Add to HNSW index
                hnsw_index_->addPoint(normalized_vector.data(), label);

                stats_.num_vectors++;
            }

            updateMemoryStats();
            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(
                Error{ErrorCode::InternalError, std::string("Batch add failed: ") + e.what()});
        }
    }

    Result<std::vector<std::vector<SearchResult>>>
    searchBatch(const std::vector<std::vector<float>>& queries, size_t k,
                const SearchFilter* filter) override {
        std::vector<std::vector<SearchResult>> results;
        results.reserve(queries.size());

        for (const auto& query : queries) {
            auto result = search(query, k, filter);
            if (!result.has_value()) {
                return Result<std::vector<std::vector<SearchResult>>>(result.error());
            }
            results.push_back(std::move(result.value()));
        }

        return Result<std::vector<std::vector<SearchResult>>>(results);
    }

    Result<void> save(const std::string& path) override {
        std::unique_lock lock(mutex_);

        try {
            // Save HNSW index
            hnsw_index_->saveIndex(path);

            // Save ID mappings to a separate file
            std::string mappings_path = path + ".mappings";
            std::ofstream mappings_file(mappings_path, std::ios::binary);
            if (!mappings_file) {
                return Result<void>(Error{ErrorCode::IOError, "Failed to create mappings file"});
            }

            // Write number of mappings
            size_t num_mappings = id_to_label_.size();
            mappings_file.write(reinterpret_cast<const char*>(&num_mappings), sizeof(num_mappings));

            // Write each mapping
            for (const auto& [id, label] : id_to_label_) {
                size_t id_len = id.length();
                mappings_file.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
                mappings_file.write(id.data(), id_len);
                mappings_file.write(reinterpret_cast<const char*>(&label), sizeof(label));

                // Write the stored vector
                const auto& vec = stored_vectors_[label];
                mappings_file.write(reinterpret_cast<const char*>(vec.data()),
                                    vec.size() * sizeof(float));
            }

            // Write next_label
            mappings_file.write(reinterpret_cast<const char*>(&next_label_), sizeof(next_label_));

            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(
                Error{ErrorCode::IOError, std::string("Failed to save index: ") + e.what()});
        }
    }

    Result<void> load(const std::string& path) override {
        std::unique_lock lock(mutex_);

        try {
            // Reinitialize index before loading
            initializeIndex();

            // Load HNSW index
            hnsw_index_->loadIndex(path, space_.get());

            // Load ID mappings
            std::string mappings_path = path + ".mappings";
            std::ifstream mappings_file(mappings_path, std::ios::binary);
            if (!mappings_file) {
                return Result<void>(Error{ErrorCode::IOError, "Failed to open mappings file"});
            }

            // Read number of mappings
            size_t num_mappings;
            mappings_file.read(reinterpret_cast<char*>(&num_mappings), sizeof(num_mappings));

            // Clear existing mappings
            id_to_label_.clear();
            label_to_id_.clear();
            stored_vectors_.clear();

            // Read each mapping
            for (size_t i = 0; i < num_mappings; ++i) {
                size_t id_len;
                mappings_file.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));

                std::string id(id_len, '\0');
                mappings_file.read(&id[0], id_len);

                hnswlib::labeltype label;
                mappings_file.read(reinterpret_cast<char*>(&label), sizeof(label));

                id_to_label_[id] = label;
                label_to_id_[label] = id;

                // Read the stored vector
                std::vector<float> vec(config_.dimension);
                mappings_file.read(reinterpret_cast<char*>(vec.data()),
                                   config_.dimension * sizeof(float));
                stored_vectors_[label] = vec;
            }

            // Read next_label
            mappings_file.read(reinterpret_cast<char*>(&next_label_), sizeof(next_label_));

            stats_.num_vectors = id_to_label_.size();
            updateMemoryStats();

            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(
                Error{ErrorCode::IOError, std::string("Failed to load index: ") + e.what()});
        }
    }

    Result<void> optimize() override {
        // HNSW doesn't need regular optimization like other indices
        // But we could rebuild if there are many deleted items
        return Result<void>();
    }

    bool needsOptimization() const override {
        // Could check for high deletion ratio
        return false;
    }

    size_t size() const override {
        std::shared_lock lock(mutex_);
        return stats_.num_vectors;
    }

    size_t dimension() const override { return config_.dimension; }

    IndexType type() const override { return IndexType::HNSW; }

    IndexStats getStats() const override {
        std::shared_lock lock(mutex_);
        stats_.type = IndexType::HNSW;
        stats_.dimension = config_.dimension;
        stats_.metric = config_.distance_metric;
        stats_.needs_optimization = needsOptimization();
        // memory_usage_bytes and index_size_bytes are already set by updateMemoryStats()
        // num_vectors is also already tracked
        return stats_;
    }

    Result<void> serialize(std::ostream& out) const override {
        std::shared_lock lock(mutex_);

        try {
            // Write version
            uint32_t version = 1;
            out.write(reinterpret_cast<const char*>(&version), sizeof(version));

            // Write number of vectors
            size_t num_vectors = id_to_label_.size();
            out.write(reinterpret_cast<const char*>(&num_vectors), sizeof(num_vectors));

            // Write each vector with its ID
            for (const auto& [id, label] : id_to_label_) {
                // Write ID length and ID
                size_t id_len = id.length();
                out.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
                out.write(id.data(), id_len);

                // Write vector
                const auto& vec = stored_vectors_.at(label);
                out.write(reinterpret_cast<const char*>(vec.data()),
                          config_.dimension * sizeof(float));
            }

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::IOError, std::string("Serialization failed: ") + e.what()};
        }
    }

    Result<void> deserialize(std::istream& in) override {
        std::unique_lock lock(mutex_);

        try {
            // Read version
            uint32_t version;
            in.read(reinterpret_cast<char*>(&version), sizeof(version));
            if (version != 1) {
                return Error{ErrorCode::InvalidArgument, "Unsupported serialization version"};
            }

            // Read number of vectors
            size_t num_vectors;
            in.read(reinterpret_cast<char*>(&num_vectors), sizeof(num_vectors));

            // Clear existing data
            id_to_label_.clear();
            label_to_id_.clear();
            stored_vectors_.clear();
            next_label_ = 0;
            stats_.num_vectors = 0;

            // Only initialize if index doesn't exist
            if (!hnsw_index_) {
                initializeIndex();
            }

            // Prepare batch of vectors for efficient loading
            std::vector<std::vector<float>> all_vectors;
            std::vector<hnswlib::labeltype> all_labels;
            all_vectors.reserve(num_vectors);
            all_labels.reserve(num_vectors);

            // Read all vectors first
            for (size_t i = 0; i < num_vectors; ++i) {
                // Read ID
                size_t id_len;
                in.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));
                std::string id(id_len, '\0');
                in.read(&id[0], id_len);

                // Read vector
                std::vector<float> vec(config_.dimension);
                in.read(reinterpret_cast<char*>(vec.data()), config_.dimension * sizeof(float));

                // Store mappings
                hnswlib::labeltype label = next_label_++;
                id_to_label_[id] = label;
                label_to_id_[label] = id;
                stored_vectors_[label] = vec;

                all_vectors.push_back(vec);
                all_labels.push_back(label);
            }

            // Add all points to index at once
            for (size_t i = 0; i < all_vectors.size(); ++i) {
                hnsw_index_->addPoint(all_vectors[i].data(), all_labels[i]);
            }

            stats_.num_vectors = num_vectors;
            updateMemoryStats();

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::IOError, std::string("Deserialization failed: ") + e.what()};
        }
    }

    std::vector<std::string> getAllIds() const override {
        std::shared_lock lock(mutex_);
        std::vector<std::string> ids;
        ids.reserve(id_to_label_.size());
        for (const auto& [id, label] : id_to_label_) {
            ids.push_back(id);
        }
        return ids;
    }

    Result<std::vector<float>> getVector(const std::string& id) const override {
        std::shared_lock lock(mutex_);

        auto it = id_to_label_.find(id);
        if (it == id_to_label_.end()) {
            return Error{ErrorCode::NotFound, "Vector not found"};
        }

        auto vec_it = stored_vectors_.find(it->second);
        if (vec_it == stored_vectors_.end()) {
            return Error{ErrorCode::InternalError, "Vector data not found"};
        }

        return Result<std::vector<float>>(vec_it->second);
    }

private:
    void initializeIndex() {
        // Create appropriate space based on distance metric
        switch (config_.distance_metric) {
            case DistanceMetric::L2:
                space_ = std::make_unique<hnswlib::L2Space>(config_.dimension);
                break;
            case DistanceMetric::INNER_PRODUCT:
                space_ = std::make_unique<hnswlib::InnerProductSpace>(config_.dimension);
                break;
            case DistanceMetric::COSINE:
                // For cosine similarity, we normalize vectors and use inner product
                space_ = std::make_unique<hnswlib::InnerProductSpace>(config_.dimension);
                config_.normalize_vectors = true; // Force normalization for cosine
                break;
            default:
                // Default to L2
                space_ = std::make_unique<hnswlib::L2Space>(config_.dimension);
        }

        // Create HNSW index
        hnsw_index_ = std::make_unique<hnswlib::HierarchicalNSW<float>>(
            space_.get(), config_.max_elements, config_.hnsw_m, config_.hnsw_ef_construction,
            config_.hnsw_seed);

        // Set the ef parameter for search
        hnsw_index_->setEf(config_.hnsw_ef_search);
    }

    void updateMemoryStats() {
        // Estimate memory usage
        // HNSW memory: ~(4 * dimension * N + 8 * M * N) bytes
        size_t hnsw_memory =
            4 * config_.dimension * stats_.num_vectors + 8 * config_.hnsw_m * stats_.num_vectors;

        // Add memory for ID mappings and stored vectors
        size_t mapping_memory =
            (sizeof(std::string) + sizeof(hnswlib::labeltype)) * stats_.num_vectors * 2;
        size_t vector_memory = config_.dimension * sizeof(float) * stats_.num_vectors;

        stats_.memory_usage_bytes = hnsw_memory + mapping_memory + vector_memory;
        stats_.index_size_bytes = stats_.memory_usage_bytes;
    }

    mutable std::shared_mutex mutex_;
    std::unique_ptr<hnswlib::SpaceInterface<float>> space_;
    std::unique_ptr<hnswlib::HierarchicalNSW<float>> hnsw_index_;

    // ID mappings (since HNSWlib uses numeric labels)
    std::unordered_map<std::string, hnswlib::labeltype> id_to_label_;
    std::unordered_map<hnswlib::labeltype, std::string> label_to_id_;
    std::unordered_map<hnswlib::labeltype, std::vector<float>> stored_vectors_;
    hnswlib::labeltype next_label_;
};

// =============================================================================
// Vector Index Manager Implementation
// =============================================================================

class VectorIndexManager::Impl {
public:
    explicit Impl(const IndexConfig& config) : config_(config), initialized_(false) {}

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
            delta_config.type = IndexType::FLAT; // Always use flat for delta
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

    Result<void> addVector(const std::string& id, const std::vector<float>& vector,
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

    Result<void> addVectors(const std::vector<std::string>& ids,
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

    Result<std::vector<SearchResult>> search(const std::vector<float>& query, size_t k,
                                             const SearchFilter& filter) {
        if (!initialized_) {
            return Result<std::vector<SearchResult>>(
                Error{ErrorCode::InvalidArgument, "Index not initialized"});
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
                all_results.insert(all_results.end(), delta_results.value().begin(),
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

        if (delta_index_ && delta_index_->size() > 0) {
            stats.delta_index_size = delta_index_->size();

            // Aggregate stats from delta index
            auto delta_stats = delta_index_->getStats();
            stats.num_vectors += delta_stats.num_vectors;
            stats.memory_usage_bytes += delta_stats.memory_usage_bytes;
            stats.index_size_bytes += delta_stats.index_size_bytes;
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
            return Result<std::vector<std::string>>(
                Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }

        std::vector<std::string> all_ids;
        std::set<std::string> unique_ids;

        // Get IDs from main index
        if (main_index_) {
            auto main_ids = main_index_->getAllIds();
            for (const auto& id : main_ids) {
                unique_ids.insert(id);
            }
        }

        // Get IDs from delta index
        if (delta_index_) {
            auto delta_ids = delta_index_->getAllIds();
            for (const auto& id : delta_ids) {
                unique_ids.insert(id);
            }
        }

        // Convert set to vector
        all_ids.reserve(unique_ids.size());
        for (const auto& id : unique_ids) {
            all_ids.push_back(id);
        }

        return Result<std::vector<std::string>>(std::move(all_ids));
    }

    IndexType getIndexType() const { return config_.type; }

    Result<void> updateVector(const std::string& id, const std::vector<float>& vector,
                              const std::map<std::string, std::string>& metadata) {
        if (!initialized_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }

        // Update metadata if provided
        if (!metadata.empty()) {
            std::unique_lock lock(metadata_mutex_);
            metadata_store_[id] = metadata;
        }

        // Try to update in delta index first if it exists
        if (delta_index_) {
            // Check if the vector exists in delta
            auto delta_ids = delta_index_->getAllIds();
            bool in_delta = std::find(delta_ids.begin(), delta_ids.end(), id) != delta_ids.end();

            if (in_delta) {
                return delta_index_->update(id, vector);
            }
        }

        // Update in main index
        return main_index_->update(id, vector);
    }

    Result<void> removeVector(const std::string& id) {
        if (!initialized_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }

        // Remove from metadata store
        {
            std::unique_lock lock(metadata_mutex_);
            metadata_store_.erase(id);
        }

        // Try to remove from delta index first if it exists
        if (delta_index_) {
            auto result = delta_index_->remove(id);
            if (result) {
                return result; // Successfully removed from delta
            }
        }

        // Remove from main index
        return main_index_->remove(id);
    }

    Result<void> saveIndex(const std::string& path) {
        if (!initialized_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Index not initialized"});
        }

        std::unique_lock lock(mutex_);

        try {
            std::ofstream file(path, std::ios::binary);
            if (!file) {
                return Result<void>(Error{ErrorCode::FileNotFound, "Cannot open file for writing"});
            }

            // Write magic number and version
            const uint32_t magic = 0x56494458; // "VIDX"
            const uint32_t version = 2;        // Version 2 with proper serialization
            file.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
            file.write(reinterpret_cast<const char*>(&version), sizeof(version));

            // Write configuration
            const uint32_t dim = config_.dimension;
            const int type = static_cast<int>(config_.type);
            file.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
            file.write(reinterpret_cast<const char*>(&type), sizeof(type));

            // Write main index
            const uint8_t has_main = main_index_ ? 1 : 0;
            file.write(reinterpret_cast<const char*>(&has_main), sizeof(has_main));
            if (main_index_) {
                auto result = main_index_->serialize(file);
                if (!result) {
                    return result;
                }
            }

            // Write delta index
            const uint8_t has_delta = delta_index_ ? 1 : 0;
            file.write(reinterpret_cast<const char*>(&has_delta), sizeof(has_delta));
            if (delta_index_) {
                auto result = delta_index_->serialize(file);
                if (!result) {
                    return result;
                }
            }

            // Write metadata
            const uint32_t metadata_count = metadata_store_.size();
            file.write(reinterpret_cast<const char*>(&metadata_count), sizeof(metadata_count));

            for (const auto& [id, meta] : metadata_store_) {
                // Write ID
                const uint32_t id_len = id.size();
                file.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
                file.write(id.data(), id_len);

                // Write metadata entries
                const uint32_t meta_count = meta.size();
                file.write(reinterpret_cast<const char*>(&meta_count), sizeof(meta_count));

                for (const auto& [key, value] : meta) {
                    const uint32_t key_len = key.size();
                    file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
                    file.write(key.data(), key_len);

                    const uint32_t val_len = value.size();
                    file.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
                    file.write(value.data(), val_len);
                }
            }

            file.close();
            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(
                Error{ErrorCode::InternalError, std::string("Failed to save index: ") + e.what()});
        }
    }

    Result<void> loadIndex(const std::string& path) {
        std::unique_lock lock(mutex_);

        try {
            std::ifstream file(path, std::ios::binary);
            if (!file) {
                return Result<void>(Error{ErrorCode::FileNotFound, "Cannot open index file"});
            }

            // Read and verify magic number
            uint32_t magic;
            file.read(reinterpret_cast<char*>(&magic), sizeof(magic));
            if (magic != 0x56494458) { // "VIDX"
                return Result<void>(Error{ErrorCode::InvalidData, "Invalid index file format"});
            }

            // Read version
            uint32_t version;
            file.read(reinterpret_cast<char*>(&version), sizeof(version));
            if (version != 2) {
                return Result<void>(Error{ErrorCode::InvalidData,
                                          "Unsupported index version: " + std::to_string(version) +
                                              " (expected 2)"});
            }

            // Read configuration
            uint32_t dim;
            int type;
            file.read(reinterpret_cast<char*>(&dim), sizeof(dim));
            file.read(reinterpret_cast<char*>(&type), sizeof(type));

            // Update configuration
            config_.dimension = dim;
            config_.type = static_cast<IndexType>(type);

            // Re-initialize index with loaded config
            initialized_ = false;

            lock.unlock(); // Release lock to avoid deadlock in initialize()

            auto init_result = initialize();
            if (!init_result) {
                return init_result;
            }

            lock.lock(); // Re-acquire lock for the rest of the operation

            // Read main index
            uint8_t has_main;
            file.read(reinterpret_cast<char*>(&has_main), sizeof(has_main));

            if (has_main && main_index_) {
                auto deserializeResult = main_index_->deserialize(file);
                if (!deserializeResult) {
                    return Result<void>(
                        Error{ErrorCode::IOError, "Failed to deserialize main index: " +
                                                      deserializeResult.error().message});
                }
            }

            // Read delta index
            uint8_t has_delta;
            file.read(reinterpret_cast<char*>(&has_delta), sizeof(has_delta));
            if (has_delta && delta_index_) {
                auto deserializeResult = delta_index_->deserialize(file);
                if (!deserializeResult) {
                    return Result<void>(
                        Error{ErrorCode::IOError, "Failed to deserialize delta index: " +
                                                      deserializeResult.error().message});
                }
            }

            // Read metadata
            uint32_t metadata_count;
            file.read(reinterpret_cast<char*>(&metadata_count), sizeof(metadata_count));

            metadata_store_.clear();
            for (uint32_t i = 0; i < metadata_count; ++i) {
                // Read ID
                uint32_t id_len;
                file.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));
                std::string id(id_len, '\0');
                file.read(id.data(), id_len);

                // Read metadata entries
                uint32_t meta_count;
                file.read(reinterpret_cast<char*>(&meta_count), sizeof(meta_count));

                std::map<std::string, std::string> meta;
                for (uint32_t j = 0; j < meta_count; ++j) {
                    uint32_t key_len;
                    file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
                    std::string key(key_len, '\0');
                    file.read(key.data(), key_len);

                    uint32_t val_len;
                    file.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
                    std::string value(val_len, '\0');
                    file.read(value.data(), val_len);

                    meta[key] = value;
                }

                metadata_store_[id] = meta;
            }

            file.close();
            return Result<void>();
        } catch (const std::exception& e) {
            return Result<void>(
                Error{ErrorCode::InternalError, std::string("Failed to load index: ") + e.what()});
        }
    }

    // Accessor methods for VectorIndexManager
    void setConfig(const IndexConfig& config) { config_ = config; }
    const IndexConfig& getConfig() const { return config_; }
    std::string getLastError() const { return lastError_; }
    void setLastError(const std::string& error) const { lastError_ = error; }

    Result<std::vector<float>> getVector(const std::string& id) const {
        std::shared_lock lock(mutex_);

        if (!main_index_) {
            return Result<std::vector<float>>(
                Error{ErrorCode::NotInitialized, "Index not initialized"});
        }

        // Try main index first
        auto result = main_index_->getVector(id);
        if (result) {
            return result;
        }

        // Try delta index if it exists
        if (delta_index_) {
            return delta_index_->getVector(id);
        }

        return Result<std::vector<float>>(Error{ErrorCode::NotFound, "Vector not found: " + id});
    }

private:
    mutable std::shared_mutex mutex_;
    mutable std::shared_mutex metadata_mutex_;

    IndexConfig config_;
    bool initialized_;
    mutable std::string lastError_;

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
    if (pImpl) {
        pImpl->shutdown();
    }
}

Result<void> VectorIndexManager::addVector(const std::string& id, const std::vector<float>& vector,
                                           const std::map<std::string, std::string>& metadata) {
    return pImpl->addVector(id, vector, metadata);
}

Result<void>
VectorIndexManager::addVectors(const std::vector<std::string>& ids,
                               const std::vector<std::vector<float>>& vectors,
                               const std::vector<std::map<std::string, std::string>>& metadata) {
    return pImpl->addVectors(ids, vectors, metadata);
}

Result<void> VectorIndexManager::updateVector(const std::string& id,
                                              const std::vector<float>& vector,
                                              const std::map<std::string, std::string>& metadata) {
    return pImpl->updateVector(id, vector, metadata);
}

Result<std::vector<SearchResult>> VectorIndexManager::search(const std::vector<float>& query,
                                                             size_t k, const SearchFilter& filter) {
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
    if (!pImpl) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // If we have a main index, rebuild it
    if (pImpl->isInitialized()) {
        auto result = pImpl->optimizeIndex();
        if (!result) {
            return result;
        }
    }

    // Merge delta index if it exists
    if (pImpl->isInitialized()) {
        auto mergeResult = mergeDeltaIndex();
        if (!mergeResult) {
            return mergeResult;
        }
    }

    return Result<void>();
}

Result<void> VectorIndexManager::rebuildIndex() {
    if (!pImpl) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // Rebuild index through the implementation
    if (!pImpl->isInitialized()) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // Get current configuration
    auto config = pImpl->getConfig();

    // Get all current vector IDs
    auto idsResult = pImpl->getAllVectorIds();
    if (!idsResult) {
        return Result<void>(
            Error{ErrorCode::InternalError, "Failed to get vector IDs for rebuild"});
    }

    // Store vectors temporarily
    std::vector<std::string> ids;
    std::vector<std::vector<float>> vectors;
    std::map<std::string, std::map<std::string, std::string>> metadata;

    for (const auto& id : idsResult.value()) {
        auto vectorResult = pImpl->getVector(id);
        if (vectorResult) {
            ids.push_back(id);
            vectors.push_back(vectorResult.value());
            // Note: metadata would need to be retrieved separately if we had a method for it
        }
    }

    // Shutdown and reinitialize
    pImpl->shutdown();
    auto initResult = pImpl->initialize();
    if (!initResult) {
        return Result<void>(
            Error{ErrorCode::InternalError, "Failed to reinitialize index during rebuild"});
    }

    // Re-add all vectors
    if (!ids.empty()) {
        auto addResult = pImpl->addVectors(ids, vectors, {});
        if (!addResult) {
            return Result<void>(
                Error{ErrorCode::InternalError, "Failed to re-add vectors during rebuild"});
        }
    }

    return Result<void>();
}

Result<void> VectorIndexManager::removeVector(const std::string& id) {
    return pImpl->removeVector(id);
}

Result<void> VectorIndexManager::saveIndex(const std::string& path) {
    return pImpl->saveIndex(path);
}

Result<void> VectorIndexManager::loadIndex(const std::string& path) {
    return pImpl->loadIndex(path);
}

Result<void> VectorIndexManager::removeVectors(const std::vector<std::string>& ids) {
    if (!pImpl) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // Remove vectors one by one
    for (const auto& id : ids) {
        auto result = removeVector(id);
        if (!result) {
            return result;
        }
    }

    return Result<void>();
}

Result<std::vector<std::vector<SearchResult>>>
VectorIndexManager::batchSearch(const std::vector<std::vector<float>>& queries, size_t k,
                                const SearchFilter& filter) {
    if (!pImpl) {
        return Result<std::vector<std::vector<SearchResult>>>(
            Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    std::vector<std::vector<SearchResult>> results;
    results.reserve(queries.size());

    for (const auto& query : queries) {
        auto result = search(query, k, filter);
        if (!result) {
            return Result<std::vector<std::vector<SearchResult>>>(result.error());
        }
        results.push_back(std::move(result.value()));
    }

    return results;
}

Result<std::vector<SearchResult>> VectorIndexManager::rangeSearch(const std::vector<float>& query,
                                                                  float max_distance,
                                                                  const SearchFilter& filter) {
    if (!pImpl) {
        return Result<std::vector<SearchResult>>(
            Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // Use regular search with a large k, then filter by distance
    auto searchResult = search(query, 1000, filter);
    if (!searchResult) {
        return searchResult;
    }

    std::vector<SearchResult> filtered;
    for (const auto& result : searchResult.value()) {
        if (result.distance <= max_distance) {
            filtered.push_back(result);
        }
    }

    return filtered;
}

Result<void> VectorIndexManager::exportIndex(const std::string& path, const std::string& format) {
    if (!pImpl) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    if (format != "json") {
        return Result<void>(
            Error{ErrorCode::InvalidArgument, "Unsupported export format: " + format});
    }

    // For now, just save the index in its native format
    return saveIndex(path);
}

Result<void> VectorIndexManager::changeIndexType(IndexType new_type) {
    if (!pImpl) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    if (pImpl->getConfig().type == new_type) {
        return Result<void>(); // Already the requested type
    }

    // Update config and rebuild index
    auto config = pImpl->getConfig();
    config.type = new_type;
    pImpl->setConfig(config);
    return rebuildIndex();
}

IndexType VectorIndexManager::getCurrentIndexType() const {
    if (!pImpl) {
        return IndexType::FLAT;
    }
    return pImpl->getIndexType();
}

void VectorIndexManager::clearCache() {
    // Currently no cache implementation
}

void VectorIndexManager::warmupCache(const std::vector<std::vector<float>>& frequent_queries) {
    // Currently no cache implementation
    (void)frequent_queries;
}

Result<bool> VectorIndexManager::validateIndex() const {
    if (!pImpl) {
        return Result<bool>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // Basic validation through the implementation
    return Result<bool>(pImpl->isInitialized());
}

Result<void> VectorIndexManager::repairIndex() {
    if (!pImpl) {
        return Result<void>(Error{ErrorCode::NotInitialized, "VectorIndexManager not initialized"});
    }

    // Simple repair: rebuild the index
    return rebuildIndex();
}

bool VectorIndexManager::hasError() const {
    if (!pImpl) {
        return true;
    }
    return !pImpl->getLastError().empty();
}

bool VectorIndexManager::needsOptimization() const {
    if (!pImpl || !pImpl->isInitialized()) {
        return false;
    }

    // Let implementation decide if optimization is needed
    auto stats = pImpl->getStats();
    return stats.needs_optimization;
}

size_t VectorIndexManager::dimension() const {
    if (!pImpl) {
        return 0;
    }
    return pImpl->getConfig().dimension;
}

// addVectors method already defined above

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

float calculateDistance(const std::vector<float>& a, const std::vector<float>& b,
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
            // Guard against zero-norm inputs which can occur when embeddings are unavailable
            // or degenerate. For cosine distance, treat zero-norm as maximal distance (1.0).
            if (norm_a == 0.0f || norm_b == 0.0f) {
                return 1.0f;
            }
            float similarity = dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
            return 1.0f - similarity; // Convert to distance
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
            return -dot; // Negative because we want to minimize distance
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

std::vector<std::vector<float>> normalizeVectors(const std::vector<std::vector<float>>& vectors) {
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
            return 1.0f - distance; // Cosine distance is 1 - similarity

        case DistanceMetric::L2:
            return 1.0f / (1.0f + distance); // Convert L2 to similarity

        case DistanceMetric::INNER_PRODUCT:
            return -distance; // Inner product distance is negative of similarity

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
    return meanVector(vectors); // For now, same as mean
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

// VectorIndexManager missing method implementations
void VectorIndexManager::setConfig(const IndexConfig& config) {
    pImpl->setConfig(config);
}

const IndexConfig& VectorIndexManager::getConfig() const {
    return pImpl->getConfig();
}

std::string VectorIndexManager::getLastError() const {
    return pImpl->getLastError();
}

// shutdown method already defined above

} // namespace yams::vector
