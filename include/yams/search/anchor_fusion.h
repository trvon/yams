// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#pragma once

#include <cstddef>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search {

// One candidate document paired with the embedding of its community's medoid.
// Used as input to computeAnchorScores. If `medoidEmbedding` is empty, the
// doc has no anchor (orphan or singleton cluster) and gets a score of 0.
struct AnchorCandidate {
    std::string documentHash;
    std::vector<float> medoidEmbedding;
};

// Compute anchor-affinity score for each candidate document.
//
// For each candidate `c`:
//   raw = cosine(queryEmbedding, c.medoidEmbedding)
//   score = (raw + 1) / 2     // map [-1, 1] -> [0, 1] (no information loss)
// Empty medoid embeddings yield score = 0.
//
// The output map preserves all input doc hashes (orphans included) so callers
// can decide whether to fuse the score or skip it.
//
// Design note: this is a per-doc primary score, not a small boost. Callers
// should treat the return as a fusion leg comparable in magnitude to text/vector
// scores after the same per-leg normalization, NOT as an additive perturbation.
//
// Complexity: O(K * dim) where K = candidates.size().
[[nodiscard]] std::unordered_map<std::string, float>
computeAnchorScores(std::span<const float> queryEmbedding,
                    std::span<const AnchorCandidate> candidates) noexcept;

// PHSS query-confidence gate. Given the query embedding and the top-K
// retrieved document embeddings, build a pairwise cosine similarity matrix
// and run simeon::phss_select_scale to pick a similarity threshold.
//
// Returns a confidence value in [0, 1]:
//   - High when the persistence diagram has a clear largest gap (well-structured
//     neighborhood — anchor leg is reliable for this query).
//   - Low when persistence values cluster tightly (no clear scale — anchor
//     leg may add noise).
//
// Concretely: confidence = (selected_scale - mean_similarity) / max(1e-6, std_similarity)
// clamped to [0, 1]. A scale meaningfully above the mean indicates a real gap.
//
// Complexity: O(K^2 * dim) for similarity construction + O(K^2 log K) inside PHSS.
// Recommended K <= 100 to keep query latency under 10ms.
[[nodiscard]] float
phssQueryConfidence(std::span<const float> queryEmbedding,
                    std::span<const std::vector<float>> topKEmbeddings) noexcept;

} // namespace yams::search
