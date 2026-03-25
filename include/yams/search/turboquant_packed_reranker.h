#pragma once

#include <yams/search/vector_reranker.h>
#include <yams/vector/turboquant.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace yams::search {

/**
 * @brief TurboQuant packed-code reranker configuration
 */
struct TurboQuantPackedRerankerConfig {
    size_t dimension = 384;
    uint8_t bits_per_channel = 4;
    uint64_t seed = 42;
    float rerank_weight = 0.5f;        // Weight for TurboQuant score in blended output
    bool require_packed_codes = false; // Skip candidates without packed codes entirely
};

/**
 * @brief TurboQuant packed-code reranker
 *
 * Implements IVectorReranker using TurboQuantMSE asymmetric scoring.
 * Candidates without packed codes receive initial_score as a fallback
 * and are sorted by initial_score descending within their group.
 *
 * Usage:
 *   TurboQuantPackedReranker reranker(config);
 *   auto output = reranker.rerank(input);
 */
class TurboQuantPackedReranker final : public IVectorReranker {
public:
    using Config = TurboQuantPackedRerankerConfig;

    explicit TurboQuantPackedReranker(Config config);

    bool isReady() const override { return ready_; }
    std::string name() const override { return "TurboQuantPackedReranker"; }
    size_t dimension() const override { return config_.dimension; }
    uint8_t bitsPerChannel() const override { return config_.bits_per_channel; }

    Result<VectorRerankOutput> rerank(const VectorRerankInput& input) override;

private:
    Config config_;
    yams::vector::TurboQuantMSE quantizer_;
    bool ready_ = false;

    static constexpr float kDefaultFallbackScore = 0.0f;

    static yams::vector::TurboQuantConfig buildTqConfig(const Config& cfg);
};

/**
 * @brief Create a TurboQuantPackedReranker
 *
 * @param config Reranker configuration
 * @return Unique pointer to the reranker
 */
std::unique_ptr<IVectorReranker>
createTurboQuantPackedReranker(TurboQuantPackedRerankerConfig config);

} // namespace yams::search
