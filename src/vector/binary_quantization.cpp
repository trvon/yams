// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/vector/binary_quantization.h>

#include <algorithm>
#include <cmath>
#include <numbers>
#include <unordered_set>

namespace yams::vector {

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

Result<std::shared_ptr<const BinaryQuantizedIndex>>
BinaryQuantizedIndex::build(std::span<const std::size_t> ids,
                            std::span<const std::vector<float>> vectors,
                            std::size_t maxPrefixDimension) {
    if (ids.empty() || ids.size() != vectors.size()) {
        return Error{ErrorCode::InvalidArgument,
                     "BQ IDs and vectors must be non-empty and have equal sizes"};
    }
    const auto fullDimension = vectors.front().size();
    if (fullDimension == 0) {
        return Error{ErrorCode::InvalidArgument, "BQ vector dimension must be non-zero"};
    }
    const auto dimension = (maxPrefixDimension > 0 && maxPrefixDimension < fullDimension)
                               ? maxPrefixDimension
                               : fullDimension;

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
    index->dimension_ = dimension;
    index->wordsPerVector_ = (dimension + 63) / 64;
    index->ids_.assign(ids.begin(), ids.end());
    index->packedWords_.assign(ids.size() * index->wordsPerVector_, 0ULL);

    for (std::size_t i = 0; i < vectors.size(); ++i) {
        const auto& vec = vectors[i];
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
    if (query.size() < dimension_) {
        return {};
    }
    const auto queryBq = BinaryQuantizer::quantize(query.subspan(0, dimension_));
    return search(queryBq, topK);
}

std::vector<BinaryQuantizedHit> BinaryQuantizedIndex::search(const BinaryVector& queryBq,
                                                             std::size_t topK) const {
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
    return bytes;
}

} // namespace yams::vector
