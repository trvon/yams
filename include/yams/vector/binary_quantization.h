// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>

#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
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

/// Optional rotation applied before taking sign bits.
/// - None: sign bits of the raw coordinates (historical behaviour).
/// - Fwht: a fixed, seeded randomized Hadamard rotation x -> (1/sqrt(n)) H D [x; 0], where D is
///   a random +/-1 diagonal, H the Walsh-Hadamard matrix and n the next power of two >= dim.
///   Every output coordinate is a random +/-1 hyperplane projection, so cos(pi * h / d) becomes a
///   SimHash-style estimate even for anisotropic embeddings.
enum class BinaryRotation : std::uint8_t {
    None = 0,
    Fwht = 1,
};

/// Fixed seed shared by every centroid index and query rotated with BinaryRotation::Fwht.
inline constexpr std::uint64_t kDefaultBinaryRotationSeed = 0x59414D5342515231ULL; // "YAMSBQR1"

/// Config spelling (TOML `search.topology.bq_rotation`).
[[nodiscard]] constexpr std::string_view binaryRotationName(BinaryRotation rotation) noexcept {
    switch (rotation) {
        case BinaryRotation::None:
            return "none";
        case BinaryRotation::Fwht:
            return "fwht";
    }
    return "none";
}

/// Orthogonal randomized Hadamard rotation (the SRHT without row subsampling). Inputs are
/// zero-padded to outputDimension(), a power of two, so norms and inner products are preserved.
/// Signs come from a splitmix64 stream of the seed, identical on every platform.
class RandomizedHadamardRotation final {
public:
    RandomizedHadamardRotation(std::size_t inputDimension, std::uint64_t seed);

    [[nodiscard]] std::size_t inputDimension() const noexcept { return inputDimension_; }
    [[nodiscard]] std::size_t outputDimension() const noexcept { return signs_.size(); }
    [[nodiscard]] std::uint64_t seed() const noexcept { return seed_; }

    /// Rotate one vector. Returns an empty vector unless input.size() == inputDimension().
    [[nodiscard]] std::vector<float> apply(std::span<const float> input) const;

private:
    std::size_t inputDimension_{0};
    std::uint64_t seed_{0};
    std::vector<float> signs_;
};

/// 1-bit Binary Quantizer for vector embeddings.
class BinaryQuantizer {
public:
    /// Quantize a single float vector into a 1-bit BinaryVector.
    /// If maxDimensions > 0, quantize only the leading min(vector.size(), maxDimensions)
    /// coordinates.
    [[nodiscard]] static BinaryVector quantize(std::span<const float> vector,
                                               std::size_t maxDimensions = 0);

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
    /// Build index over vectors.
    /// If maxPrefixDimension > 0, store only the leading coordinates up to maxPrefixDimension
    /// (e.g. 64 dimensions for Matryoshka prefix shortlisting).
    /// With BinaryRotation::Fwht every vector is rotated before quantization (rotate first, then
    /// take the prefix), and search(span) applies the same rotation to the query, so stored
    /// vectors and queries always share one seed.
    static Result<std::shared_ptr<const BinaryQuantizedIndex>>
    build(std::span<const std::size_t> ids, std::span<const std::vector<float>> vectors,
          std::size_t maxPrefixDimension = 0, BinaryRotation rotation = BinaryRotation::None,
          std::uint64_t rotationSeed = kDefaultBinaryRotationSeed);

    [[nodiscard]] std::vector<BinaryQuantizedHit> search(std::span<const float> query,
                                                         std::size_t topK) const;

    [[nodiscard]] std::vector<BinaryQuantizedHit> search(const BinaryVector& queryBq,
                                                         std::size_t topK) const;

    [[nodiscard]] std::size_t size() const noexcept { return ids_.size(); }
    [[nodiscard]] std::size_t dimension() const noexcept { return dimension_; }
    [[nodiscard]] std::size_t memoryUsageBytes() const noexcept;
    [[nodiscard]] BinaryRotation rotation() const noexcept {
        return rotation_ ? BinaryRotation::Fwht : BinaryRotation::None;
    }
    /// Seed of the active rotation; zero when the index is unrotated.
    [[nodiscard]] std::uint64_t rotationSeed() const noexcept {
        return rotation_ ? rotation_->seed() : 0;
    }

private:
    std::optional<RandomizedHadamardRotation> rotation_;
    std::size_t dimension_{0};
    std::size_t wordsPerVector_{0};
    std::vector<std::size_t> ids_;
    std::vector<std::uint64_t> packedWords_;
};

} // namespace yams::vector
