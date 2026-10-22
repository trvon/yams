// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <optional>
#include <span>

namespace yams::daemon::embed {

// Invalid/degenerate vectors have no semantic neighbors, just like zero-norm vectors.
inline float semanticInverseNorm(std::span<const float> values) noexcept {
    double squaredNorm = 0.0;
    for (const float value : values) {
        if (!std::isfinite(value)) {
            return 0.0f;
        }
        squaredNorm += static_cast<double>(value) * value;
    }
    if (!(squaredNorm > 0.0) || !std::isfinite(squaredNorm)) {
        return 0.0f;
    }
    const auto inverse = static_cast<float>(1.0 / std::sqrt(squaredNorm));
    return std::isfinite(inverse) ? inverse : 0.0f;
}

inline float semanticCosineSimilarity(std::span<const float> left, float inverseLeft,
                                      std::span<const float> right, float inverseRight) noexcept {
    if (left.empty() || left.size() != right.size() || !std::isfinite(inverseLeft) ||
        !std::isfinite(inverseRight) || inverseLeft <= 0.0f || inverseRight <= 0.0f) {
        return 0.0f;
    }
    double dot = 0.0;
    auto rightValue = right.begin();
    for (const float value : left) {
        dot += static_cast<double>(value) * *rightValue;
        ++rightValue;
    }
    const auto score = dot * inverseLeft * inverseRight;
    // Rounded inverse norms can produce cosine > 1 even for identical legal vectors.
    // Normalize BEFORE ranking/adaptive thresholds, not just when serializing the edge.
    return std::isfinite(score) ? static_cast<float>(std::clamp(score, -1.0, 1.0)) : 0.0f;
}

inline std::optional<float> semanticExplicitThreshold(float value) noexcept {
    if (!std::isfinite(value)) {
        return std::nullopt;
    }
    return std::clamp(value, 0.0f, 1.0f);
}

// Keep corpus cardinality, scored pairs, and retained output cardinality distinct.
struct SemanticGraphCandidateCounts {
    std::size_t sourceDocuments;
    std::size_t candidateDocuments;
    std::size_t candidateNeighbors;
    std::size_t retainedNeighbors;

    [[nodiscard]] bool hasGraphWork() const noexcept { return retainedNeighbors != 0; }
    [[nodiscard]] std::size_t nodeCapacity() const noexcept {
        return sourceDocuments + retainedNeighbors;
    }
};

} // namespace yams::daemon::embed
