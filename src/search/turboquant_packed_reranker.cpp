#include <yams/search/turboquant_packed_reranker.h>

#include <yams/vector/turboquant.h>

#include <algorithm>
#include <cstring>
#include <vector>

namespace yams::search {

yams::vector::TurboQuantConfig TurboQuantPackedReranker::buildTqConfig(const Config& cfg) {
    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = cfg.dimension;
    tq_cfg.bits_per_channel = cfg.bits_per_channel;
    tq_cfg.seed = cfg.seed;
    return tq_cfg;
}

TurboQuantPackedReranker::TurboQuantPackedReranker(Config config)
    : config_(std::move(config)), quantizer_(buildTqConfig(config_)) {
    ready_ = true;
}

Result<VectorRerankOutput> TurboQuantPackedReranker::rerank(const VectorRerankInput& input) {
    VectorRerankOutput output;

    if (!ready_) {
        return Error{ErrorCode::InvalidState, "TurboQuantPackedReranker: not ready"};
    }

    if (input.transformed_query.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "TurboQuantPackedReranker: transformed_query is empty"};
    }

    if (input.transformed_query.size() != config_.dimension) {
        return Error{ErrorCode::InvalidArgument,
                     "TurboQuantPackedReranker: transformed_query size " +
                         std::to_string(input.transformed_query.size()) +
                         " != configured dimension " + std::to_string(config_.dimension)};
    }

    // Separate candidates with and without packed codes
    std::vector<VectorRerankOutput::Candidate> with_packed;
    std::vector<VectorRerankOutput::Candidate> without_packed;

    with_packed.reserve(input.candidates.size());
    without_packed.reserve(input.candidates.size());

    for (const auto& [chunk_id, record] : input.candidates) {
        float initial_score = 0.0f;
        auto it = input.initial_scores.find(chunk_id);
        if (it != input.initial_scores.end()) {
            initial_score = it->second;
        }

        if (record.quantized.format == yams::vector::VectorRecord::QuantizedFormat::NONE ||
            record.quantized.packed_codes.empty()) {
            if (config_.require_packed_codes) {
                ++output.candidates_skipped;
                continue;
            }
            // Fallback: use initial score only
            without_packed.push_back({
                .chunk_id = chunk_id,
                .rerank_score = kDefaultFallbackScore,
                .initial_score = initial_score,
                .blended_score = initial_score,
                .has_packed_codes = false,
            });
            ++output.candidates_skipped;
        } else {
            // Verify dimension consistency
            if (record.embedding_dim != 0 && record.embedding_dim != config_.dimension) {
                ++output.candidates_skipped;
                continue;
            }

            // Asymmetric score from packed codes
            float rerank_score =
                quantizer_.scoreFromPacked(input.transformed_query, record.quantized.packed_codes);

            float blended_score = (config_.rerank_weight * rerank_score) +
                                  ((1.0f - config_.rerank_weight) * initial_score);

            with_packed.push_back({
                .chunk_id = chunk_id,
                .rerank_score = rerank_score,
                .initial_score = initial_score,
                .blended_score = blended_score,
                .has_packed_codes = true,
            });
            ++output.packed_candidates_scored;
        }
    }

    // Sort: TurboQuant-scored candidates by rerank_score desc,
    //       fallback candidates by initial_score desc
    std::sort(with_packed.begin(), with_packed.end(), [](const auto& a, const auto& b) {
        if (a.rerank_score != b.rerank_score) {
            return a.rerank_score > b.rerank_score;
        }
        return a.initial_score > b.initial_score;
    });

    std::sort(without_packed.begin(), without_packed.end(),
              [](const auto& a, const auto& b) { return a.initial_score > b.initial_score; });

    output.candidates.reserve(with_packed.size() + without_packed.size());
    output.candidates.insert(output.candidates.end(), std::make_move_iterator(with_packed.begin()),
                             std::make_move_iterator(with_packed.end()));
    output.candidates.insert(output.candidates.end(),
                             std::make_move_iterator(without_packed.begin()),
                             std::make_move_iterator(without_packed.end()));

    return output;
}

// =====================================================================
// Factory function (can be extended to return unique_ptr<IVectorReranker>)
// =====================================================================

std::unique_ptr<IVectorReranker>
createTurboQuantPackedReranker(TurboQuantPackedReranker::Config config) {
    return std::make_unique<TurboQuantPackedReranker>(std::move(config));
}

} // namespace yams::search
