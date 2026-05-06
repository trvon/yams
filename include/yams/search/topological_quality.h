// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace yams::search {

// H_0 persistent homology via union-find on the Vietoris-Rips filtration's
// MST edges. Computes the sum of H_0 birth/death pair lifetimes (every point
// is born at r=0; each non-redundant merge produces a death at the edge's
// pairwise distance). The final "essential" feature is skipped (its lifetime
// would be infinite).
//
// Normalization: we divide by the 95th-percentile pairwise distance instead
// of the paper's max-pairwise distance. On L2-normalized text embeddings the
// max is near-constant (~sqrt(2)) across fixtures; the 95th percentile
// respects actual spread and is data-driven. This is a deliberate departure
// from Shestov et al. 2025 (arXiv 2512.15285) appropriate for our setting.
//
// Complexity: O(n^2 log n) dominated by sorting ~n^2/2 pairwise distances.
// Memory: O(n^2) for the distance list.
//
// `embeddings` is a contiguous row-major matrix of `count` rows x `dim`
// columns. Returns a scalar in [0, n-1] after normalization.
[[nodiscard]] double computePersistenceH0(std::span<const float> embeddings, std::size_t count,
                                          std::size_t dim) noexcept;

// Convenience overload: accepts a vector of vectors.
[[nodiscard]] double
computePersistenceH0(const std::vector<std::vector<float>>& embeddings) noexcept;

// Deterministic subsample helper. Produces a reproducible subset of
// `maxCount` row indices from [0, total) using a seeded mt19937. When
// total <= maxCount, returns the identity permutation [0, total).
[[nodiscard]] std::vector<std::size_t>
deterministicSubsample(std::size_t total, std::size_t maxCount, std::uint64_t seed);

} // namespace yams::search
