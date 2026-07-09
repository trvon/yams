// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "cross_rerank_internal.h"

#include <algorithm>
#include <cmath>

namespace yams::search::detail {

namespace {

void appendRerankTextPart(std::string& out, const std::string& part) {
    if (part.empty()) {
        return;
    }
    if (!out.empty()) {
        out.push_back('\n');
    }
    out += part;
}

} // namespace

std::string buildCrossRerankText(const SearchResult& result,
                                 const std::shared_ptr<metadata::MetadataRepository>& metadataRepo,
                                 std::size_t textLimit) {
    std::string text;
    appendRerankTextPart(text, result.document.fileName);
    appendRerankTextPart(text, result.document.filePath);
    appendRerankTextPart(text, result.snippet);
    if (metadataRepo && result.document.id > 0 && text.size() < textLimit) {
        auto contentResult = metadataRepo->getContent(result.document.id);
        if (contentResult && contentResult.value()) {
            const auto& content = contentResult.value()->contentText;
            if (!content.empty()) {
                const size_t remaining =
                    textLimit > text.size() ? textLimit - text.size() : size_t{0};
                appendRerankTextPart(text, content.substr(0, remaining));
            }
        }
    }
    if (text.size() > textLimit) {
        text.resize(textLimit);
    }
    return text;
}

CrossRerankOutcome
applyCrossRerank(std::vector<SearchResult>& results, const std::string& query,
                 const SearchEngineConfig& config, std::size_t window,
                 const CrossRerankScorerFn& reranker,
                 const std::shared_ptr<metadata::MetadataRepository>& metadataRepo) {
    CrossRerankOutcome outcome;
    outcome.window = window;
    outcome.replaceScores = config.rerankReplaceScores;

    if (window == 0) {
        outcome.skipReason = "no_candidates";
        return outcome;
    }
    if (!reranker) {
        outcome.skipReason = "reranker_unavailable";
        return outcome;
    }

    const size_t textLimit = config.rerankSnippetMaxChars;
    std::vector<std::string> rerankTexts;
    rerankTexts.reserve(window);
    std::vector<double> originalScores;
    originalScores.reserve(window);
    for (size_t i = 0; i < window; ++i) {
        rerankTexts.push_back(buildCrossRerankText(results[i], metadataRepo, textLimit));
        originalScores.push_back(results[i].score);
    }

    outcome.attempted = true;
    auto scoreResult = reranker(query, rerankTexts);
    if (!scoreResult) {
        outcome.errorMessage = scoreResult.error().message;
        if (scoreResult.error().code == ErrorCode::NotImplemented ||
            scoreResult.error().code == ErrorCode::NotInitialized) {
            outcome.skipReason = "reranker_unavailable";
        } else {
            outcome.status = CrossRerankOutcome::Status::Failed;
        }
        return outcome;
    }
    const auto& scores = scoreResult.value();
    if (scores.size() != window) {
        outcome.status = CrossRerankOutcome::Status::Failed;
        outcome.errorMessage = "score_size_mismatch";
        return outcome;
    }

    auto [minIt, maxIt] = std::minmax_element(scores.begin(), scores.end());
    const float minScore = minIt != scores.end() && std::isfinite(*minIt) ? *minIt : 0.0F;
    const float maxScore = maxIt != scores.end() && std::isfinite(*maxIt) ? *maxIt : 0.0F;
    float secondScore = 0.0F;
    for (float score : scores) {
        if (!std::isfinite(score) || score >= maxScore) {
            continue;
        }
        secondScore = std::max(secondScore, score);
    }
    outcome.scoreGap = static_cast<double>(maxScore - secondScore);

    if (!(maxScore > minScore)) {
        outcome.skipReason = "no_score_variance";
        return outcome;
    }
    if (outcome.scoreGap < static_cast<double>(std::max(0.0F, config.rerankScoreGapThreshold))) {
        outcome.skipReason = "score_gap_below_threshold";
        return outcome;
    }

    auto [origMinIt, origMaxIt] = std::minmax_element(originalScores.begin(), originalScores.end());
    const double origMin = origMinIt != originalScores.end() ? *origMinIt : 0.0;
    const double origMax = origMaxIt != originalScores.end() ? *origMaxIt : 0.0;
    const double rerankRange = static_cast<double>(maxScore - minScore);
    const double originalRange = origMax - origMin;
    const double blend = std::clamp(static_cast<double>(config.rerankBlendWeight), 0.0, 1.0);
    outcome.blendWeight = blend;

    outcome.components.reserve(window);
    outcome.docTraces.reserve(window);
    for (size_t i = 0; i < window; ++i) {
        const double rerankNorm =
            std::clamp((static_cast<double>(scores[i]) - minScore) / rerankRange, 0.0, 1.0);
        const double originalNorm =
            originalRange > 0.0
                ? std::clamp((originalScores[i] - origMin) / originalRange, 0.0, 1.0)
                : 1.0;
        const double finalScore =
            config.rerankReplaceScores ? rerankNorm : blend * rerankNorm + (1.0 - blend) * originalNorm;

        outcome.docTraces.push_back(CrossRerankDocTrace{
            .filePath = results[i].document.filePath,
            .documentHash = results[i].document.sha256Hash,
            .originalScore = originalScores[i],
            .rerankScore = scores[i],
            .rerankNorm = rerankNorm,
            .finalScore = finalScore,
        });
        results[i].score = finalScore;

        outcome.components.push_back(ComponentResult{
            .documentHash = results[i].document.sha256Hash,
            .filePath = results[i].document.filePath,
            .score = static_cast<float>(rerankNorm),
            .source = ComponentResult::Source::Unknown,
            .rank = i,
            .snippet = results[i].snippet.empty()
                           ? std::nullopt
                           : std::optional<std::string>(results[i].snippet),
        });
    }

    std::stable_sort(results.begin(), results.begin() + static_cast<std::ptrdiff_t>(window),
                     [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    outcome.status = CrossRerankOutcome::Status::Applied;
    return outcome;
}

} // namespace yams::search::detail
