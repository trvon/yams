// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <yams/core/types.h>
#include <yams/vector/turboquant.h>

namespace yams {
namespace vector {

/**
 * @brief Compressed ANN index that operates entirely in the packed-code domain.
 *
 * This class builds a navigable-small-world (NSW) graph over packed TurboQuant
 * codes and performs greedy search using per-coordinate LUT scoring, avoiding
 * float decode until the top-k results are returned.
 *
 * Key design properties:
 * - Storage: only packed codes are stored (no float corpus)
 * - Search: scoreFromPacked() only, no decode during traversal
 * - Memory: ~dim*bits/8 bytes per vector vs dim*4 bytes for float
 * - Compatibility: uses the same TurboQuantMSE scorer from Milestones 1-6
 *
 * @note This is the Milestone 7 compressed-ANN prototype. The existing
 * HNSW+rerank pipeline remains the stable baseline; this module explores
 * whether ANN traversal in compressed space can match recall while improving
 * latency/memory.
 */
class CompressedANNIndex {
public:
    struct Config {
        /** Embedding dimension */
        size_t dimension = 384;
        /** Bits per TurboQuant channel */
        uint8_t bits_per_channel = 4;
        /** Random seed for reproducible graph construction */
        uint64_t seed = 42;
        /** Number of neighbors (M) per node in the NSW graph */
        size_t m = 16;
        /** Search ef parameter (candidates kept in search beam) */
        size_t ef_search = 50;
        /** Maximum corpus size */
        size_t max_elements = 100000;
    };

    struct SearchResult {
        size_t id;
        float score; // higher = better (inner product)
    };

    /** Runtime stats emitted per search for telemetry. */
    struct SearchStats {
        size_t candidate_count = 0;  // nodes scored during greedy descent
        float decode_escapes = 0.0f; // always 0 in compressed path
        std::vector<SearchResult> results;
    };

    /**
     * Graph-search with telemetry output.
     * @param out_candidate_count If non-null, receives the number of nodes scored
     * @param out_decode_escapes  If non-null, receives decode-escape count (always 0)
     */
    SearchStats searchWithStats(const std::vector<float>& query_embedding, size_t k,
                                size_t* out_candidate_count, float* out_decode_escapes);

    /**
     * Greedy NSW search using packed scoring only.
     *
     * Traverses the pre-built NSW graph, scoring candidates with
     * scoreFromPacked (per-coordinate LUT) — zero decode until results.
     *
     * @param query_embedding Float query (transformed via transformQuery internally)
     * @param k Number of results to return
     * @return Vector of top-k results (sorted descending by score)
     */
    std::vector<SearchResult> search(const std::vector<float>& query_embedding, size_t k);

    explicit CompressedANNIndex(const Config& config);

    // Non-copyable, movable
    CompressedANNIndex(const CompressedANNIndex&) = delete;
    CompressedANNIndex& operator=(const CompressedANNIndex&) = delete;
    CompressedANNIndex(CompressedANNIndex&&) noexcept;
    CompressedANNIndex& operator=(CompressedANNIndex&&) noexcept;
    ~CompressedANNIndex();

    /**
     * @brief Add a packed vector to the index.
     *
     * @param id Unique identifier for this vector
     * @param packed_code TurboQuant-packed bytes (must match dim/bits config)
     * @return Error if id already exists or dimensions mismatch
     */
    Result<void> add(size_t id, std::span<const uint8_t> packed_code);

    /**
     * @brief Pre-build the NSW navigation graph from all added vectors.
     *
     * Must be called after all insertions and before any search.
     * Uses compressed scoring (scoreFromPacked) to select neighbors.
     *
     * @return Error on failure
     */
    Result<void> build();

    /**
     * @brief Greedy NSW search using packed scoring only.
     *
     * Traverses the pre-built NSW graph, scoring candidates with
    /**
     * @brief Return total in-memory bytes for packed corpus + graph.
     */
    size_t memoryBytes() const;

    /**
     * @brief Return number of vectors in the index.
     */
    size_t size() const { return packed_codes_.size(); }

    /**
     * @brief Access the underlying TurboQuant scorer for transform/decode.
     */
    const TurboQuantMSE& scorer() const { return scorer_; }

private:
    struct NSWNode {
        size_t id;
        std::span<const uint8_t> packed_code;
        std::vector<size_t> neighbors; // neighbor node indices
    };

    /** Build NSW graph: connect each new node to its ef_search nearest existing neighbors */
    void connectNodeToGraph(size_t new_node_idx);

    /** Greedy descent: start from entry, iteratively move to better neighbor */
    std::vector<size_t> greedyDescent(std::span<const float> transformed_query, size_t entry_idx,
                                      size_t ef);

    Config config_;
    TurboQuantMSE scorer_;
    std::vector<NSWNode> nodes_;
    std::vector<uint8_t> packed_storage_;                // contiguous storage for all packed codes
    std::vector<std::span<const uint8_t>> packed_codes_; // views into packed_storage_
    std::mt19937 rng_;
    std::vector<float> transformed_query_cache_; // reused allocation for transformQuery output
};

} // namespace vector
} // namespace yams
