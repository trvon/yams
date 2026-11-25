#include <yams/vector/static_cosine_ann_index.h>

#include <sqlite-vec-cpp/distances/inner_product.hpp>
#include <sqlite-vec-cpp/index/hnsw.hpp>

// sqlite-vec's inline distance kernels change body with SQLITE_VEC_ENABLE_*; building this TU
// without the flags its library uses is an ODR violation (see src/vector/meson.build).
#if defined(__aarch64__) && !defined(SQLITE_VEC_ENABLE_NEON)
#error "static_cosine_ann_index.cpp must be built with the sqlite-vec feature defines"
#endif

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

// Squared norm of a vector that can be normalized, or the reason it cannot.
Result<double> normalizableSquaredNorm(std::span<const float> values) {
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
    return squaredNorm;
}

Result<std::vector<float>> normalized(std::span<const float> values) {
    auto norm = normalizableSquaredNorm(values);
    if (!norm) {
        return norm.error();
    }
    const double squaredNorm = norm.value();

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
        // StaticCosineAnnIndex indexes compact centroid graphs (hundreds to a few thousand
        // clusters) during snapshot load. The corpus-scale defaults (M_max_0=64,
        // ef_construction=200) explore ~75% of a 2k-node graph per insert and trigger heavy
        // O(M_max_0^2) heuristic pruning on high-dimensional (1024-d) centroids. Scaling
        // connectivity and construction beam to graph size cuts build time ~10x while keeping
        // high top-k routing recall.
        if (size <= 8192) {
            config.M = 12;
            config.M_max = 16;
            config.M_max_0 = 32;
            config.ef_construction = 64;
        } else {
            config.ef_construction = std::min<std::size_t>(config.ef_construction, 128);
        }
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
    std::vector<std::span<const float>> views(vectors.begin(), vectors.end());
    return build(ids, std::span<const std::span<const float>>(views));
}

Result<std::shared_ptr<const StaticCosineAnnIndex>>
StaticCosineAnnIndex::build(std::span<const std::size_t> ids,
                            std::span<const std::span<const float>> vectors) {
    if (ids.empty() || ids.size() != vectors.size()) {
        return Error{ErrorCode::InvalidArgument,
                     "ANN IDs and vectors must be non-empty and have equal sizes"};
    }
    const auto dimension = vectors.front().size();
    if (dimension == 0) {
        return Error{ErrorCode::InvalidArgument, "ANN vector dimension must be non-zero"};
    }

    // Validate everything first, so a bad input fails before any work; then normalize and insert
    // one vector at a time. The index stores its own copy of each vector, so holding every
    // normalized vector at once only doubled the build's peak memory.
    std::unordered_set<std::size_t> uniqueIds;
    uniqueIds.reserve(ids.size());
    for (std::size_t index = 0; index < vectors.size(); ++index) {
        if (vectors[index].size() != dimension) {
            return Error{ErrorCode::InvalidArgument, "ANN vector dimensions must match"};
        }
        if (!uniqueIds.insert(ids[index]).second) {
            return Error{ErrorCode::InvalidArgument, "ANN IDs must be unique"};
        }
        // Check without building the normalized vector; it is built once, at insertion.
        if (auto checked = normalizableSquaredNorm(vectors[index]); !checked) {
            return checked.error();
        }
    }

    try {
        auto impl = std::make_unique<Impl>(dimension, vectors.size());
        impl->externalIds_.assign(ids.begin(), ids.end());
        std::vector<float> normalizedBuffer(dimension);
        for (std::size_t index = 0; index < vectors.size(); ++index) {
            const auto& src = vectors[index];
            double squaredNorm = 0.0;
            for (const float value : src) {
                squaredNorm += static_cast<double>(value) * static_cast<double>(value);
            }
            const auto inverseNorm = static_cast<float>(1.0 / std::sqrt(squaredNorm));
            for (std::size_t d = 0; d < dimension; ++d) {
                normalizedBuffer[d] = src[d] * inverseNorm;
            }
            impl->index_.insert_single_threaded(index, normalizedBuffer);
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
