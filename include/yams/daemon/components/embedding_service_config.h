// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstddef>
#include <optional>
#include <string>

namespace yams::daemon {

// Typed startup policy for embedding-worker safeguards. Callers that leave a field unset retain
// compatibility resolution; explicit values are immutable for the service lifecycle.
// [embeddings.semantic_graph] policy for the semantic_neighbor KG layer (formerly the
// YAMS_GRAPH_SEMANTIC_TOPK / _THRESHOLD / _USE_HNSW environment overlays).
struct SemanticNeighborGraphConfig {
    // At most 512 directed edges and 1024 ANN candidates per source; not a total-memory bound.
    static constexpr std::size_t kMaxTopK = 256;
    static constexpr bool validTopK(std::size_t value) noexcept {
        return value >= 1 && value <= kMaxTopK;
    }
    // Neighbors kept per source document. 4 left most docs at degree 0, so graph expansion at
    // search time fell back to medoid anchors; 8 stays sparse enough for component purity.
    std::size_t topK{8};
    // Minimum cosine similarity for an edge. Unset: adaptive, the K-th best similarity per
    // source (edge weights are clamped to that floor).
    std::optional<float> similarityThreshold;
    // Use the vector index for candidate search when a streaming batch exceeds the exact-pair
    // budget; the exact scan is always used for small batches.
    bool useHnsw{true};
};

struct EmbeddingServiceConfig {
    // Zero means unspecified; effective policy resolves a safe default or compatibility value.
    std::size_t coremlUnifiedConcurrency{0};
    std::string coremlUnifiedConcurrencySource;
    SemanticNeighborGraphConfig semanticGraph;
};

struct EffectiveEmbeddingServiceConfig {
    std::size_t coremlUnifiedConcurrency{1};
    std::string coremlUnifiedConcurrencySource{"default"};
};

} // namespace yams::daemon
