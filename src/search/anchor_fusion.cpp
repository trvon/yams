// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <yams/search/anchor_fusion.h>

#include <simeon/persistent_homology.hpp>

#include <algorithm>
#include <cmath>
#include <numeric>

namespace yams::search {

namespace {

float cosineSimilarity(std::span<const float> a, std::span<const float> b) noexcept {
    if (a.size() != b.size() || a.empty()) {
        return 0.0F;
    }
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        const double ai = static_cast<double>(a[i]);
        const double bi = static_cast<double>(b[i]);
        dot += ai * bi;
        normA += ai * ai;
        normB += bi * bi;
    }
    if (normA <= 0.0 || normB <= 0.0) {
        return 0.0F;
    }
    return static_cast<float>(dot / std::sqrt(normA * normB));
}

float clamp01(float x) noexcept {
    if (x < 0.0F)
        return 0.0F;
    if (x > 1.0F)
        return 1.0F;
    return x;
}

} // namespace

std::unordered_map<std::string, float>
computeAnchorScores(std::span<const float> queryEmbedding,
                    std::span<const AnchorCandidate> candidates) noexcept {
    std::unordered_map<std::string, float> out;
    out.reserve(candidates.size());
    for (const auto& cand : candidates) {
        if (cand.medoidEmbedding.empty() || cand.medoidEmbedding.size() != queryEmbedding.size()) {
            // Treat orphans + dimension mismatches as "no usable anchor" → 0.
            out[cand.documentHash] = 0.0F;
            continue;
        }
        const float cos = cosineSimilarity(queryEmbedding, cand.medoidEmbedding);
        // Map [-1, 1] -> [0, 1]; preserves rank ordering, no information loss.
        out[cand.documentHash] = clamp01((cos + 1.0F) * 0.5F);
    }
    return out;
}

float phssQueryConfidence(std::span<const float> queryEmbedding,
                          std::span<const std::vector<float>> topKEmbeddings) noexcept {
    // We treat the query as point 0 and the K docs as points 1..K, so the
    // PHSS graph has N = K + 1 vertices and N*(N-1)/2 upper-triangular edges.
    const std::size_t k = topKEmbeddings.size();
    if (k < 2 || queryEmbedding.empty()) {
        return 0.0F;
    }
    // Validate that all embeddings have the same dim as the query.
    for (const auto& e : topKEmbeddings) {
        if (e.size() != queryEmbedding.size()) {
            return 0.0F;
        }
    }
    const std::uint32_t n = static_cast<std::uint32_t>(k + 1);
    const std::size_t edgeCount = static_cast<std::size_t>(n) * (n - 1) / 2;
    std::vector<float> sims(edgeCount, 0.0F);

    auto pointSpan = [&](std::uint32_t idx) -> std::span<const float> {
        if (idx == 0) {
            return queryEmbedding;
        }
        return topKEmbeddings[idx - 1];
    };

    // Upper-triangular row-major: sims[i*(2n-i-1)/2 + (j - i - 1)] = sim(i, j)
    // for i < j.
    for (std::uint32_t i = 0; i < n; ++i) {
        for (std::uint32_t j = i + 1; j < n; ++j) {
            const std::size_t off = static_cast<std::size_t>(i) * (2 * n - i - 1) / 2 + (j - i - 1);
            sims[off] = cosineSimilarity(pointSpan(i), pointSpan(j));
        }
    }

    simeon::PhssConfig cfg{};
    cfg.criterion = simeon::PhssConfig::Criterion::LargestGap;
    cfg.output_diagram = false;
    auto result = simeon::phss_select_scale(std::span<const float>(sims), n, cfg);

    // Confidence = (selected_scale - mean_sim) / max(1e-6, std_sim), clamped to [0, 1].
    // High when the chosen threshold is meaningfully above average similarity →
    // there's a real "structural gap" the persistence diagram identified.
    if (sims.empty()) {
        return 0.0F;
    }
    double sum = 0.0;
    for (float s : sims) {
        sum += static_cast<double>(s);
    }
    const double mean = sum / static_cast<double>(sims.size());
    double sqDev = 0.0;
    for (float s : sims) {
        const double d = static_cast<double>(s) - mean;
        sqDev += d * d;
    }
    const double stdDev = std::sqrt(sqDev / static_cast<double>(sims.size()));
    if (stdDev < 1e-6) {
        return 0.0F;
    }
    const double zlike = (static_cast<double>(result.selected_scale) - mean) / stdDev;
    // Map z-like value via a soft sigmoid into [0, 1]; z = 0 → 0.5; z = 2 → ~0.88; z < 0 → < 0.5.
    const double conf = 1.0 / (1.0 + std::exp(-zlike));
    return clamp01(static_cast<float>(conf));
}

} // namespace yams::search
