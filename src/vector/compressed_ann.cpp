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
      transformed_query_cache_(std::move(other.transformed_query_cache_)),
      transformed_storage_(std::move(other.transformed_storage_)) {
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
        transformed_storage_ = std::move(other.transformed_storage_);
        for (size_t i = 0; i < nodes_.size(); ++i) {
            nodes_[i].packed_code = packed_codes_[i];
        }
    }
    return *this;
}

size_t CompressedANNIndex::memoryBytes() const {
    size_t bytes = packed_storage_.size();                // packed corpus
    bytes += transformed_storage_.size() * sizeof(float); // precomputed transformed vectors
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

    // For each existing node, compute metric-aligned packed score and keep top-M neighbors.
    // Uses the SAME scorer as search: scoreFromPacked(transformed_new, packed_cand).
    // This ensures new edges reflect the angular/cosine-like geometry used at search time.
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

    // Score all candidates using metric-aligned packed scoring
    std::vector<std::pair<float, size_t>> scored;
    scored.reserve(candidate_indices.size());

    // Precompute transformed vector for the new node (if not already in storage)
    // For incremental builds, ensure we have the transformed vector
    std::vector<float> new_transformed(config_.dimension);
    scorer_.transformPackedCode(new_packed,
                                std::span<float>(new_transformed.data(), config_.dimension));

    for (size_t cand_idx : candidate_indices) {
        // Metric-aligned score: same as search path
        float s = scorer_.scoreFromPacked(
            std::span<const float>(new_transformed.data(), config_.dimension),
            packed_codes_[cand_idx]);
        scored.emplace_back(s, cand_idx);
    }

    // Sort by score descending (higher = closer in packed-score space)
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

Result<void> CompressedANNIndex::buildMetricAligned() {
    // ========================================================================
    // METRIC-ALIGNED NSW GRAPH CONSTRUCTION
    // ========================================================================
    // Replaces XOR-distance neighbor selection with packed-score-aligned selection.
    // This fixes the graph-topology mismatch that caused poor recall at 10k+.
    //
    // The key insight: graph edges must reflect the SAME metric used at search time.
    // - Old: XOR distance between packed codes (popcount of a ^ b)
    // - New: scoreFromPacked(transformed_a, b) — the same LUT used during search
    //
    // Algorithm:
    // 1. Transform every node's packed code ONCE (O(n × dim) — done once)
    // 2. For each node i: score all j≠i using scoreFromPacked(transformed_i, packed_j)
    //    (O(n² × packed_bytes) using LUT — same cost as XOR but correct metric)
    // 3. Connect i to its top-M neighbors by packed score
    //
    // This is O(n²) scoring + O(n² × dim) for the initial transform pass.
    // Build time is dominated by the transform pass; scoring itself is fast
    // since scoreFromPacked is just LUT lookup + accumulation.
    // ========================================================================

    const size_t n = nodes_.size();
    const size_t dim = config_.dimension;
    const size_t max_neighbors = std::min(config_.m, n - 1);

    if (n == 0)
        return {};

    // --- Step 1: Precompute all transformed vectors (transform each node once) ---
    transformed_storage_.resize(n * dim);
    for (size_t i = 0; i < n; ++i) {
        scorer_.transformPackedCode(packed_codes_[i],
                                    std::span<float>(transformed_storage_.data() + i * dim, dim));
    }

    // --- Step 2: Build metric-aligned graph ---
    for (size_t i = 0; i < n; ++i) {
        std::vector<std::pair<float, size_t>> scored;
        scored.reserve(n - 1);

        std::span<const float> transformed_i(transformed_storage_.data() + i * dim, dim);

        for (size_t j = 0; j < n; ++j) {
            if (i == j)
                continue;
            // Use the SAME scorer as search: scoreFromPacked(transformed_query, packed_code)
            // Higher score = more similar = closer in angular space
            float s = scorer_.scoreFromPacked(transformed_i, packed_codes_[j]);
            scored.emplace_back(s, j);
        }

        // Sort by score descending (higher = closer in packed-score space)
        std::partial_sort(scored.begin(), scored.begin() + static_cast<long>(max_neighbors),
                          scored.end(),
                          [](const auto& x, const auto& y) { return x.first > y.first; });

        for (size_t k = 0; k < max_neighbors; ++k) {
            nodes_[i].neighbors.push_back(scored[k].second);
        }
    }

    return {};
}

Result<void> CompressedANNIndex::build() {
    // Build NSW-style graph: connect each node to nearest M neighbors.
    // Uses metric-aligned scoring (scoreFromPacked) — the same metric as search.
    // This replaces the old XOR-distance approach that caused topology mismatch.
    return buildMetricAligned();
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

    // Adaptive multi-start search: scale with corpus size.
    // Single-start is sufficient for n ≤ 1k; larger corpora need diversification.
    // Multi-start explores multiple graph regions, dramatically improving recall.
    size_t num_starts;
    if (nodes_.size() <= 1000) {
        num_starts = 1;
    } else if (nodes_.size() <= 10000) {
        num_starts = 4;
    } else if (nodes_.size() <= 50000) {
        num_starts = 8;
    } else {
        num_starts = 16;
    }

    // Collect all candidates from multi-start greedy descent
    std::vector<size_t> all_candidates;
    all_candidates.reserve(num_starts * config_.ef_search * 4);
    std::unordered_set<size_t> seen;
    seen.reserve(num_starts * config_.ef_search * 4);

    std::mt19937 local_rng(42); // deterministic per search
    for (size_t s = 0; s < num_starts; ++s) {
        // Reservoir-sampled entry point (fast, no full shuffle)
        size_t entry_idx = std::uniform_int_distribution<size_t>(0, nodes_.size() - 1)(local_rng);
        auto candidates = greedyDescent(transformed_query, entry_idx, config_.ef_search);
        for (size_t c : candidates) {
            if (seen.insert(c).second) {
                all_candidates.push_back(c);
            }
        }
    }

    // Defensive: if no candidates found, return empty
    if (all_candidates.empty()) {
        stats.results.clear();
        return stats;
    }

    stats.candidate_count = all_candidates.size();
    stats.decode_escapes = 0; // Zero decode in compressed path

    // Score all candidates with scoreFromPacked — zero decode
    std::vector<std::pair<float, size_t>> scored;
    scored.reserve(all_candidates.size());
    for (size_t idx : all_candidates) {
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
