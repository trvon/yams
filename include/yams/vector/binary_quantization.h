// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>

#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace yams::vector {

/// A 1-bit binary quantized vector representation.
/// For vector v in R^d, each bit b_i = (v_i >= 0.0f) ? 1 : 0.
/// 64 dimensions are packed into each uint64_t word.
struct BinaryVector {
    std::vector<std::uint64_t> words;
    std::size_t dimension{0};

    [[nodiscard]] bool empty() const noexcept { return dimension == 0; }
    [[nodiscard]] std::size_t sizeBytes() const noexcept {
        return words.size() * sizeof(std::uint64_t);
    }
};

/// 1-bit Binary Quantizer for vector embeddings.
class BinaryQuantizer {
public:
    /// Quantize a single float vector into a 1-bit BinaryVector.
    [[nodiscard]] static BinaryVector quantize(std::span<const float> vector);

    /// Compute Hamming distance (number of differing bits) between two binary vectors.
    [[nodiscard]] static std::size_t hammingDistance(const BinaryVector& a, const BinaryVector& b);

    /// Compute normalized Hamming distance in [0.0, 1.0].
    [[nodiscard]] static float normalizedHammingDistance(const BinaryVector& a,
                                                         const BinaryVector& b);

    /// Estimate cosine similarity from Hamming distance: cos(pi * D_H / d).
    /// Range is [-1.0, 1.0].
    [[nodiscard]] static float estimatedCosineSimilarity(const BinaryVector& a,
                                                         const BinaryVector& b);
};

struct BinaryQuantizedHit {
    std::size_t id{0};
    std::size_t hammingDistance{0};
    float estimatedSimilarity{0.0F};
};

/// Compact, immutable index of 1-bit binary quantized vectors for fast coarse filtering.
class BinaryQuantizedIndex final {
public:
    static Result<std::shared_ptr<const BinaryQuantizedIndex>>
    build(std::span<const std::size_t> ids, std::span<const std::vector<float>> vectors);

    [[nodiscard]] std::vector<BinaryQuantizedHit> search(std::span<const float> query,
                                                         std::size_t topK) const;

    [[nodiscard]] std::vector<BinaryQuantizedHit> search(const BinaryVector& queryBq,
                                                         std::size_t topK) const;

    [[nodiscard]] std::size_t size() const noexcept { return ids_.size(); }
    [[nodiscard]] std::size_t dimension() const noexcept { return dimension_; }
    [[nodiscard]] std::size_t memoryUsageBytes() const noexcept;

private:
    std::size_t dimension_{0};
    std::size_t wordsPerVector_{0};
    std::vector<std::size_t> ids_;
    std::vector<std::uint64_t> packedWords_;
};

} // namespace yams::vector
