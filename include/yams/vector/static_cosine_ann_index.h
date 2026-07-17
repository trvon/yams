#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace yams::vector {

struct StaticCosineAnnHit {
    std::size_t id{0};
    float distance{0.0F};
};

struct StaticCosineAnnSearchResult {
    std::vector<StaticCosineAnnHit> hits;
    std::size_t distanceEvaluations{0};
};

/// Immutable, in-memory cosine ANN index for small routing/vector-codebook surfaces.
/// IDs are caller-owned and need not be dense. Construction normalizes vectors once;
/// each query reports the graph traversal's actual distance evaluations.
class StaticCosineAnnIndex final {
public:
    static Result<std::shared_ptr<const StaticCosineAnnIndex>>
    build(std::span<const std::size_t> ids, std::span<const std::vector<float>> vectors);

    ~StaticCosineAnnIndex();
    StaticCosineAnnIndex(const StaticCosineAnnIndex&) = delete;
    StaticCosineAnnIndex& operator=(const StaticCosineAnnIndex&) = delete;

    [[nodiscard]] Result<StaticCosineAnnSearchResult>
    search(std::span<const float> query, std::size_t candidates, std::size_t efSearch = 0) const;

    [[nodiscard]] std::size_t size() const noexcept;
    [[nodiscard]] std::size_t dimension() const noexcept;

private:
    class Impl;
    explicit StaticCosineAnnIndex(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

} // namespace yams::vector
