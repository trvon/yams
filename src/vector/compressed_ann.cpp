// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/vector/compressed_ann.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <random>
#include <stdexcept>
#include <tuple>
#include <unordered_set>

#include <spdlog/spdlog.h>

namespace yams {
namespace vector {

namespace {

/** Compute packed code byte length for a given dim and bits. */
constexpr size_t packedByteLen(size_t dim, uint8_t bits) {
    return (dim * bits + 7) / 8;
}

/** Compute score between transformed query and packed code using per-coord LUT. */
float packedScore(const TurboQuantMSE& scorer, std::span<const float> transformed_query,
                  std::span<const uint8_t> packed_code) {
    return scorer.scoreFromPacked(transformed_query, packed_code);
}

} // anonymous namespace

// ============================================================================
// CompressedANNIndex
// ============================================================================

CompressedANNIndex::~CompressedANNIndex() = default;

CompressedANNIndex::CompressedANNIndex(const Config& config)
    : config_(config), scorer_([&]() {
          TurboQuantConfig tq_cfg;
          tq_cfg.dimension = config.dimension;
          tq_cfg.bits_per_channel = config.bits_per_channel;
          tq_cfg.seed = config.seed;
          return TurboQuantMSE(tq_cfg);
      }()),
      rng_(config.seed), transformed_query_cache_(config.dimension) {
    packed_storage_.reserve(config.max_elements *
                            packedByteLen(config.dimension, config.bits_per_channel));
}

CompressedANNIndex::CompressedANNIndex(CompressedANNIndex&& other) noexcept
    : config_(other.config_), scorer_(std::move(other.scorer_)), nodes_(std::move(other.nodes_)),
      packed_storage_(std::move(other.packed_storage_)),
      packed_codes_(std::move(other.packed_codes_)), rng_(std::move(other.rng_)),
      transformed_query_cache_(std::move(other.transformed_query_cache_)) {
    // Rebuild spans after move since storage may have moved
    for (size_t i = 0; i < nodes_.size(); ++i) {
        nodes_[i].packed_code = packed_codes_[i];
    }
}

CompressedANNIndex& CompressedANNIndex::operator=(CompressedANNIndex&& other) noexcept {
    if (this != &other) {
        config_ = other.config_;
        scorer_ = std::move(other.scorer_);
        nodes_ = std::move(other.nodes_);
        packed_storage_ = std::move(other.packed_storage_);
        packed_codes_ = std::move(other.packed_codes_);
        rng_ = std::move(other.rng_);
        transformed_query_cache_ = std::move(other.transformed_query_cache_);
        for (size_t i = 0; i < nodes_.size(); ++i) {
            nodes_[i].packed_code = packed_codes_[i];
        }
    }
    return *this;
}

size_t CompressedANNIndex::memoryBytes() const {
    size_t bytes = packed_storage_.size(); // packed corpus
    for (const auto& node : nodes_) {
        bytes += node.neighbors.size() * sizeof(size_t); // graph edges
    }
    bytes += sizeof(NSWNode) * nodes_.size(); // node overhead
    return bytes;
}

Result<void> CompressedANNIndex::add(size_t id, std::span<const uint8_t> packed_code) {
    const size_t expected_len = packedByteLen(config_.dimension, config_.bits_per_channel);
    if (packed_code.size() != expected_len) {
        return Error{ErrorCode::InvalidArgument, fmt::format("Packed code size {} != expected {}",
                                                             packed_code.size(), expected_len)};
    }

    // Copy into contiguous storage
    size_t offset = packed_storage_.size();
    packed_storage_.insert(packed_storage_.end(), packed_code.begin(), packed_code.end());

    auto span = std::span<const uint8_t>(packed_storage_).subspan(offset, expected_len);
    packed_codes_.push_back(span);

    nodes_.push_back(NSWNode{id, span, {}});

    // No graph building during add() - build() will construct graph
    return {};
}

void CompressedANNIndex::connectNodeToGraph(size_t new_node_idx) {
    if (nodes_.empty())
        return;

    // For each existing node (in reverse to favor newer/more connected nodes),
    // compute score and keep top-M neighbors
    const size_t m = std::min(config_.m, nodes_.size() - 1);
    if (m == 0)
        return;

    const auto& new_packed = packed_codes_[new_node_idx];

    // Build candidate pool: sample up to ef_search existing nodes
    size_t pool_size = std::min(config_.ef_search, nodes_.size() - 1);
    std::vector<size_t> candidate_indices;
    candidate_indices.reserve(pool_size);

    // Deterministic sample: pick nodes at regular intervals
    for (size_t i = 0; i < pool_size; ++i) {
        size_t idx = (i * (nodes_.size() - 1)) / pool_size;
        if (idx != new_node_idx) {
            candidate_indices.push_back(idx);
        }
    }

    // Score all candidates (this is build-time so latency is acceptable)
    std::vector<std::pair<float, size_t>> scored;
    scored.reserve(candidate_indices.size());

    // Create a dummy transformed query from the packed code itself for build scoring
    // (we use the code as its own "query" for symmetric neighbor selection)
    std::vector<float> dummy_transformed(config_.dimension, 0.0f);
    for (size_t j = 0; j < config_.dimension; ++j) {
        // Score node vs new_node using simple XOR-like comparison in packed space
        // For symmetric NSW construction, we score from both sides
        dummy_transformed[j] = 0.0f; // placeholder
    }

    // Actually, use the packed code's centroid (precomputed per-code latent)
    // For simplicity in build, score by raw L2 on a zero query (distance from origin)
    // This is just for graph construction - search uses real transformed queries
    for (size_t cand_idx : candidate_indices) {
        // Symmetric score for NSW construction: XOR distance between packed codes
        float xor_dist = 0.0f;
        const auto& cand_code = packed_codes_[cand_idx];
        for (size_t b = 0; b < packedByteLen(config_.dimension, config_.bits_per_channel); ++b) {
            uint8_t x = new_packed[b] ^ cand_code[b];
            // Count set bits as rough distance proxy
            xor_dist += static_cast<float>(__builtin_popcount(x));
        }
        // Negative XOR = we want SMALL xor distance (close codes)
        scored.emplace_back(-xor_dist, cand_idx);
    }

    // Sort by score (higher = smaller XOR distance = closer)
    std::partial_sort(scored.begin(), scored.begin() + static_cast<long>(m), scored.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });

    // Add top-M as neighbors (bidirectional)
    size_t added = 0;
    for (size_t i = 0; i < m && added < m; ++i) {
        size_t neighbor_idx = scored[i].second;
        if (neighbor_idx >= nodes_.size())
            continue;

        // Add bidirectional edges
        nodes_[new_node_idx].neighbors.push_back(neighbor_idx);
        nodes_[neighbor_idx].neighbors.push_back(new_node_idx);
        ++added;
    }
}

Result<void> CompressedANNIndex::build() {
    // Build NSW-style graph: connect each node to nearest M neighbors
    // Uses XOR distance in packed space (O(n^2) build, fast for < 1000 vectors)
    const size_t max_neighbors = std::min(config_.m, nodes_.size() - 1);
    for (size_t i = 0; i < nodes_.size(); ++i) {
        std::vector<std::pair<float, size_t>> dists;
        dists.reserve(nodes_.size());

        for (size_t j = 0; j < nodes_.size(); ++j) {
            if (i == j)
                continue;
            float d = 0.0f;
            const auto& a = packed_codes_[i];
            const auto& b = packed_codes_[j];
            size_t byte_len = packedByteLen(config_.dimension, config_.bits_per_channel);
            for (size_t k = 0; k < byte_len; ++k) {
                d += static_cast<float>(__builtin_popcount(a[k] ^ b[k]));
            }
            dists.emplace_back(d, j);
        }

        // Sort by XOR distance (ascending = closer)
        std::partial_sort(dists.begin(), dists.begin() + static_cast<long>(max_neighbors),
                          dists.end(),
                          [](const auto& x, const auto& y) { return x.first < y.first; });

        for (size_t k = 0; k < max_neighbors; ++k) {
            nodes_[i].neighbors.push_back(dists[k].second);
        }
    }
    return {};
}

std::vector<size_t> CompressedANNIndex::greedyDescent(std::span<const float> transformed_query,
                                                      size_t entry_idx, size_t ef) {
    // Standard NSW greedy traversal:
    // 1. Maintain a max-heap of (score, node_idx)
    // 2. Pop best node, explore all its neighbors
    // 3. Stop when we've popped ef candidates OR heap is empty
    // 4. Return all popped node indices as the candidate set

    if (nodes_.empty())
        return {};

    // Max-heap: higher score = higher priority
    std::vector<std::pair<float, size_t>> heap; // (score, node_idx)
    heap.reserve(ef * 4);
    std::unordered_set<size_t> visited;
    visited.reserve(nodes_.size());

    // Initialize with entry point
    float entry_score = packedScore(scorer_, transformed_query, packed_codes_[entry_idx]);
    heap.emplace_back(entry_score, entry_idx);
    visited.insert(entry_idx);

    // Make max-heap
    std::make_heap(heap.begin(), heap.end(),
                   [](const auto& a, const auto& b) { return a.first < b.first; });

    std::vector<size_t> popped; // nodes popped from heap = candidate set
    popped.reserve(ef * 4);

    while (!heap.empty() && popped.size() < ef * 4) {
        // Extract max
        std::pop_heap(heap.begin(), heap.end(),
                      [](const auto& a, const auto& b) { return a.first < b.first; });
        auto [current_score, current_idx] = heap.back();
        heap.pop_back();
        popped.push_back(current_idx);

        // Explore neighbors
        for (size_t nb : nodes_[current_idx].neighbors) {
            if (visited.contains(nb))
                continue;
            visited.insert(nb);
            float nb_score = packedScore(scorer_, transformed_query, packed_codes_[nb]);
            heap.emplace_back(nb_score, nb);
            std::push_heap(heap.begin(), heap.end(),
                           [](const auto& a, const auto& b) { return a.first < b.first; });
        }
    }

    return popped;
}

std::vector<CompressedANNIndex::SearchResult>
CompressedANNIndex::search(const std::vector<float>& query_embedding, size_t k) {
    size_t cand_count = 0;
    float dec_escapes = 0.0f;
    auto stats = searchWithStats(query_embedding, k, &cand_count, &dec_escapes);
    return std::move(stats.results);
}

CompressedANNIndex::SearchStats
CompressedANNIndex::searchWithStats(const std::vector<float>& query_embedding, size_t k,
                                    size_t* out_candidate_count, float* out_decode_esapes) {
    SearchStats stats;
    stats.candidate_count = 0;
    stats.decode_escapes = 0.0f;

    if (nodes_.empty())
        return stats;
    if (k == 0)
        return stats;

    // Transform query once (allocation-free via reused cache)
    if (transformed_query_cache_.size() != config_.dimension) {
        transformed_query_cache_.resize(config_.dimension);
    }
    std::span<float> cache_span(transformed_query_cache_);
    scorer_.transformQueryInPlace(query_embedding, cache_span);
    std::span<const float> transformed_query(transformed_query_cache_);

    // Phase 1 (Milestone 9): entry-point heuristic — node with most neighbors (highest
    // connectivity)
    size_t entry_idx = 0;
    size_t best_conn = 0;
    for (size_t i = 0; i < nodes_.size(); ++i) {
        if (nodes_[i].neighbors.size() > best_conn) {
            best_conn = nodes_[i].neighbors.size();
            entry_idx = i;
        }
    }

    // Greedy descent in NSW graph — visits bounded frontier, not all nodes
    std::vector<size_t> top_candidates =
        greedyDescent(transformed_query, entry_idx, config_.ef_search);

    // Defensive: if greedyDescent returns empty, return empty results
    if (top_candidates.empty()) {
        stats.results.clear();
        return stats;
    }

    // If greedyDescent returned fewer candidates than requested k, that's fine
    // We sort and return what we have
    size_t top_cand_count = top_candidates.size(); // for logging below
    (void)top_cand_count;                          // suppress unused in non-debug

    stats.candidate_count = top_candidates.size();
    stats.decode_escapes = 0; // No decode in compressed path

    // Score only the frontier with scoreFromPacked — zero decode
    std::vector<std::pair<float, size_t>> scored;
    scored.reserve(top_candidates.size());
    for (size_t idx : top_candidates) {
        float s = packedScore(scorer_, transformed_query, packed_codes_[idx]);
        scored.emplace_back(s, nodes_[idx].id);
    }

    std::partial_sort(scored.begin(),
                      scored.begin() + static_cast<long>(std::min(k, scored.size())), scored.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<SearchResult> results;
    results.reserve(std::min(k, scored.size()));
    for (size_t i = 0; i < std::min(k, scored.size()); ++i) {
        results.push_back({scored[i].second, scored[i].first});
    }

    // Defensive: if scored is empty somehow, return empty
    if (results.empty()) {
        stats.results.clear();
        return stats;
    }

    stats.results = std::move(results);

    if (out_candidate_count)
        *out_candidate_count = stats.candidate_count;
    if (out_decode_esapes)
        *out_decode_esapes = stats.decode_escapes;

    return stats;
}

} // namespace vector
} // namespace yams
