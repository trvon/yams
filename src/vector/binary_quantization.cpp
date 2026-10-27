// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/profiling.h>
#include <yams/vector/binary_quantization.h>

#include <algorithm>
#include <cmath>
#include <numbers>
#include <unordered_set>

namespace yams::vector {

namespace {

std::uint64_t splitMix64(std::uint64_t& state) noexcept {
    std::uint64_t z = (state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

std::size_t nextPowerOfTwo(std::size_t value) noexcept {
    std::size_t power = 1;
    while (power < value) {
        power <<= 1U;
    }
    return power;
}

// In-place unnormalized Walsh-Hadamard transform; data.size() must be a power of two.
void fastWalshHadamard(std::span<float> data) noexcept {
    for (std::size_t half = 1; half < data.size(); half <<= 1U) {
        for (std::size_t block = 0; block < data.size(); block += half << 1U) {
            for (std::size_t i = block; i < block + half; ++i) {
                const float a = data[i];
                const float b = data[i + half];
                data[i] = a + b;
                data[i + half] = a - b;
            }
        }
    }
}

} // namespace

RandomizedHadamardRotation::RandomizedHadamardRotation(std::size_t inputDimension,
                                                       std::uint64_t seed)
    : inputDimension_(inputDimension), seed_(seed) {
    const auto padded = inputDimension == 0 ? std::size_t{0} : nextPowerOfTwo(inputDimension);
    signs_.resize(padded);
    std::uint64_t state = seed;
    std::uint64_t bits = 0;
    for (std::size_t i = 0; i < padded; ++i) {
        if (i % 64 == 0) {
            bits = splitMix64(state);
        }
        signs_[i] = ((bits >> (i % 64)) & 1ULL) != 0 ? 1.0F : -1.0F;
    }
}

std::vector<float> RandomizedHadamardRotation::apply(std::span<const float> input) const {
    if (input.size() != inputDimension_ || signs_.empty()) {
        return {};
    }
    std::vector<float> rotated(signs_.size(), 0.0F);
    for (std::size_t i = 0; i < input.size(); ++i) {
        rotated[i] = input[i] * signs_[i];
    }
    fastWalshHadamard(rotated);
    const float scale = 1.0F / std::sqrt(static_cast<float>(rotated.size()));
    for (float& value : rotated) {
        value *= scale;
    }
    return rotated;
}

BinaryVector BinaryQuantizer::quantize(std::span<const float> vector, std::size_t maxDimensions) {
    BinaryVector result;
    const auto dim =
        (maxDimensions > 0 && maxDimensions < vector.size()) ? maxDimensions : vector.size();
    result.dimension = dim;
    if (dim == 0) {
        return result;
    }
    const std::size_t numWords = (dim + 63) / 64;
    result.words.assign(numWords, 0ULL);
    for (std::size_t i = 0; i < dim; ++i) {
        if (vector[i] >= 0.0F) {
            result.words[i / 64] |= (1ULL << (i % 64));
        }
    }
    return result;
}

std::size_t BinaryQuantizer::hammingDistance(const BinaryVector& a, const BinaryVector& b) {
    if (a.dimension != b.dimension || a.words.size() != b.words.size()) {
        return static_cast<std::size_t>(-1);
    }
    std::size_t dist = 0;
    for (std::size_t i = 0; i < a.words.size(); ++i) {
        dist += static_cast<std::size_t>(std::popcount(a.words[i] ^ b.words[i]));
    }
    return dist;
}

float BinaryQuantizer::normalizedHammingDistance(const BinaryVector& a, const BinaryVector& b) {
    if (a.dimension == 0 || a.dimension != b.dimension) {
        return 1.0F;
    }
    return static_cast<float>(hammingDistance(a, b)) / static_cast<float>(a.dimension);
}

float BinaryQuantizer::estimatedCosineSimilarity(const BinaryVector& a, const BinaryVector& b) {
    const float normDist = normalizedHammingDistance(a, b);
    return std::cos(std::numbers::pi_v<float> * normDist);
}

Result<std::shared_ptr<const BinaryQuantizedIndex>> BinaryQuantizedIndex::build(
    std::span<const std::size_t> ids, std::span<const std::vector<float>> vectors,
    std::size_t maxPrefixDimension, BinaryRotation rotation, std::uint64_t rotationSeed) {
    if (ids.empty() || ids.size() != vectors.size()) {
        return Error{ErrorCode::InvalidArgument,
                     "BQ IDs and vectors must be non-empty and have equal sizes"};
    }
    const auto fullDimension = vectors.front().size();
    if (fullDimension == 0) {
        return Error{ErrorCode::InvalidArgument, "BQ vector dimension must be non-zero"};
    }
    std::optional<RandomizedHadamardRotation> hadamard;
    if (rotation == BinaryRotation::Fwht) {
        hadamard.emplace(fullDimension, rotationSeed);
    }
    // Rotate first, then take the prefix: the prefix selects leading rotated coordinates.
    const auto codeDimension = hadamard ? hadamard->outputDimension() : fullDimension;
    const auto dimension = (maxPrefixDimension > 0 && maxPrefixDimension < codeDimension)
                               ? maxPrefixDimension
                               : codeDimension;

    std::unordered_set<std::size_t> uniqueIds;
    uniqueIds.reserve(ids.size());
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        if (vectors[i].size() != fullDimension) {
            return Error{ErrorCode::InvalidArgument, "BQ vector dimensions must match"};
        }
        if (!uniqueIds.insert(ids[i]).second) {
            return Error{ErrorCode::InvalidArgument, "BQ IDs must be unique"};
        }
    }

    auto index = std::make_shared<BinaryQuantizedIndex>();
    index->rotation_ = std::move(hadamard);
    index->dimension_ = dimension;
    index->wordsPerVector_ = (dimension + 63) / 64;
    index->ids_.assign(ids.begin(), ids.end());
    index->packedWords_.assign(ids.size() * index->wordsPerVector_, 0ULL);

    std::vector<float> rotated;
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        std::span<const float> vec = vectors[i];
        if (index->rotation_) {
            rotated = index->rotation_->apply(vec);
            vec = rotated;
        }
        for (std::size_t j = 0; j < dimension; ++j) {
            if (vec[j] >= 0.0F) {
                index->packedWords_[i * index->wordsPerVector_ + (j / 64)] |= (1ULL << (j % 64));
            }
        }
    }

    return std::static_pointer_cast<const BinaryQuantizedIndex>(std::move(index));
}

std::vector<BinaryQuantizedHit> BinaryQuantizedIndex::search(std::span<const float> query,
                                                             std::size_t topK) const {
    if (rotation_) {
        // Queries must use the stored rotation; a different dimension cannot be scored.
        const auto rotated = rotation_->apply(query);
        if (rotated.empty()) {
            return {};
        }
        return search(
            BinaryQuantizer::quantize(std::span<const float>(rotated).subspan(0, dimension_)),
            topK);
    }
    if (query.size() < dimension_) {
        return {};
    }
    const auto queryBq = BinaryQuantizer::quantize(query.subspan(0, dimension_));
    return search(queryBq, topK);
}

std::vector<BinaryQuantizedHit> BinaryQuantizedIndex::search(const BinaryVector& queryBq,
                                                             std::size_t topK) const {
    YAMS_ZONE_SCOPED_N("vector::bq::search");
    if (topK == 0 || size() == 0 || queryBq.dimension != dimension_) {
        return {};
    }

    std::vector<BinaryQuantizedHit> candidates;
    candidates.reserve(ids_.size());
    const auto* queryWords = queryBq.words.data();
    const float invDim = 1.0F / static_cast<float>(dimension_);

    for (std::size_t i = 0; i < ids_.size(); ++i) {
        const auto* vecWords = &packedWords_[i * wordsPerVector_];
        std::size_t dist = 0;
        for (std::size_t w = 0; w < wordsPerVector_; ++w) {
            dist += static_cast<std::size_t>(std::popcount(queryWords[w] ^ vecWords[w]));
        }
        // cos(pi * h / d) is the SimHash estimate, exact in expectation only for random
        // hyperplane projections; for raw sign bits it is an ordering proxy, not a cosine.
        const float normDist = static_cast<float>(dist) * invDim;
        candidates.push_back(BinaryQuantizedHit{
            .id = ids_[i],
            .hammingDistance = dist,
            .estimatedSimilarity = std::cos(std::numbers::pi_v<float> * normDist),
        });
    }

    const auto take = std::min(topK, candidates.size());
    std::partial_sort(candidates.begin(), candidates.begin() + take, candidates.end(),
                      [](const BinaryQuantizedHit& lhs, const BinaryQuantizedHit& rhs) {
                          if (lhs.hammingDistance != rhs.hammingDistance) {
                              return lhs.hammingDistance < rhs.hammingDistance;
                          }
                          return lhs.id < rhs.id;
                      });
    candidates.resize(take);
    return candidates;
}

std::size_t BinaryQuantizedIndex::memoryUsageBytes() const noexcept {
    std::size_t bytes = sizeof(BinaryQuantizedIndex);
    bytes += ids_.capacity() * sizeof(std::size_t);
    bytes += packedWords_.capacity() * sizeof(std::uint64_t);
    if (rotation_) {
        bytes += rotation_->outputDimension() * sizeof(float);
    }
    return bytes;
}

} // namespace yams::vector
