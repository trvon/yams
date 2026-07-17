#include <yams/vector/static_cosine_ann_index.h>

#include <sqlite-vec-cpp/distances/inner_product.hpp>
#include <sqlite-vec-cpp/index/hnsw.hpp>

#include <algorithm>
#include <cmath>
#include <exception>
#include <unordered_set>
#include <utility>

namespace yams::vector {

namespace {

thread_local std::size_t* activeDistanceEvaluationCounter = nullptr;

struct CountingNormalizedCosineDistance {
    [[nodiscard]] float operator()(std::span<const float> lhs, std::span<const float> rhs) const {
        if (activeDistanceEvaluationCounter != nullptr) {
            ++*activeDistanceEvaluationCounter;
        }
        return sqlite_vec_cpp::distances::inner_product_distance(lhs, rhs);
    }
};

class DistanceEvaluationScope final {
public:
    explicit DistanceEvaluationScope(std::size_t& counter) noexcept
        : previous_(std::exchange(activeDistanceEvaluationCounter, &counter)) {}

    ~DistanceEvaluationScope() { activeDistanceEvaluationCounter = previous_; }

private:
    std::size_t* previous_;
};

Result<std::vector<float>> normalized(std::span<const float> values) {
    double squaredNorm = 0.0;
    for (const auto value : values) {
        if (!std::isfinite(value)) {
            return Error{ErrorCode::InvalidArgument, "ANN vectors must contain finite values"};
        }
        squaredNorm += static_cast<double>(value) * static_cast<double>(value);
    }
    if (squaredNorm <= 1e-16) {
        return Error{ErrorCode::InvalidArgument, "ANN vectors must have non-zero norm"};
    }

    const auto inverseNorm = static_cast<float>(1.0 / std::sqrt(squaredNorm));
    std::vector<float> result(values.begin(), values.end());
    for (auto& value : result) {
        value *= inverseNorm;
    }
    return result;
}

} // namespace

class StaticCosineAnnIndex::Impl final {
public:
    using Index = sqlite_vec_cpp::index::HNSWIndex<float, CountingNormalizedCosineDistance>;

    Impl(std::size_t dimension, std::size_t size)
        : dimension_(dimension), index_(makeConfig(size, dimension)) {
        externalIds_.reserve(size);
        index_.reserve(size);
    }

    static Index::Config makeConfig(std::size_t size, std::size_t dimension) {
        auto config = Index::Config::for_corpus(size, dimension);
        config.random_seed = 42;
        return config;
    }

    std::size_t dimension_{0};
    std::vector<std::size_t> externalIds_;
    Index index_;
};

StaticCosineAnnIndex::StaticCosineAnnIndex(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

StaticCosineAnnIndex::~StaticCosineAnnIndex() = default;

Result<std::shared_ptr<const StaticCosineAnnIndex>>
StaticCosineAnnIndex::build(std::span<const std::size_t> ids,
                            std::span<const std::vector<float>> vectors) {
    if (ids.empty() || ids.size() != vectors.size()) {
        return Error{ErrorCode::InvalidArgument,
                     "ANN IDs and vectors must be non-empty and have equal sizes"};
    }
    const auto dimension = vectors.front().size();
    if (dimension == 0) {
        return Error{ErrorCode::InvalidArgument, "ANN vector dimension must be non-zero"};
    }

    std::unordered_set<std::size_t> uniqueIds;
    uniqueIds.reserve(ids.size());
    std::vector<std::vector<float>> normalizedVectors;
    normalizedVectors.reserve(vectors.size());
    for (std::size_t index = 0; index < vectors.size(); ++index) {
        if (vectors[index].size() != dimension) {
            return Error{ErrorCode::InvalidArgument, "ANN vector dimensions must match"};
        }
        if (!uniqueIds.insert(ids[index]).second) {
            return Error{ErrorCode::InvalidArgument, "ANN IDs must be unique"};
        }
        auto normalizedVector = normalized(vectors[index]);
        if (!normalizedVector) {
            return normalizedVector.error();
        }
        normalizedVectors.push_back(std::move(normalizedVector).value());
    }

    try {
        auto impl = std::make_unique<Impl>(dimension, vectors.size());
        impl->externalIds_.assign(ids.begin(), ids.end());
        for (std::size_t index = 0; index < normalizedVectors.size(); ++index) {
            impl->index_.insert_single_threaded(index, normalizedVectors[index]);
        }
        auto mutableIndex =
            std::shared_ptr<StaticCosineAnnIndex>(new StaticCosineAnnIndex(std::move(impl)));
        return std::static_pointer_cast<const StaticCosineAnnIndex>(std::move(mutableIndex));
    } catch (const std::exception& error) {
        return Error{ErrorCode::InternalError,
                     std::string{"failed to build static ANN index: "} + error.what()};
    }
}

Result<StaticCosineAnnSearchResult> StaticCosineAnnIndex::search(std::span<const float> query,
                                                                 std::size_t candidates,
                                                                 std::size_t efSearch) const {
    if (query.size() != dimension()) {
        return Error{ErrorCode::InvalidArgument, "ANN query dimension does not match index"};
    }
    if (candidates == 0 || size() == 0) {
        return StaticCosineAnnSearchResult{};
    }
    auto normalizedQuery = normalized(query);
    if (!normalizedQuery) {
        return normalizedQuery.error();
    }

    const auto take = std::min(candidates, size());
    const auto effectiveEf = std::min(size(), std::max({take, efSearch, std::size_t{32}}));
    StaticCosineAnnSearchResult result;
    try {
        DistanceEvaluationScope countDistances(result.distanceEvaluations);
        const auto hits =
            impl_->index_.search_read_mostly(normalizedQuery.value(), take, effectiveEf);
        result.hits.reserve(hits.size());
        for (const auto& [internalId, distance] : hits) {
            if (internalId < impl_->externalIds_.size()) {
                result.hits.push_back(StaticCosineAnnHit{.id = impl_->externalIds_[internalId],
                                                         .distance = distance});
            }
        }
    } catch (const std::exception& error) {
        return Error{ErrorCode::InternalError,
                     std::string{"static ANN search failed: "} + error.what()};
    }
    return result;
}

std::size_t StaticCosineAnnIndex::size() const noexcept {
    return impl_->externalIds_.size();
}

std::size_t StaticCosineAnnIndex::dimension() const noexcept {
    return impl_->dimension_;
}

} // namespace yams::vector
