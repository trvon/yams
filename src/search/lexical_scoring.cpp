// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/lexical_scoring.h>

#include <yams/core/magic_numbers.hpp>
#include <yams/search/query_text_utils.h>

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <unordered_set>
#include <utility>

namespace yams::search::detail {

Bm25Range computeBm25Range(std::span<const metadata::SearchResult> candidates) {
    Bm25Range out;
    for (const auto& sr : candidates) {
        const double score = sr.score;
        if (!out.initialized) {
            out.minScore = score;
            out.maxScore = score;
            out.initialized = true;
        } else {
            out.minScore = std::min(out.minScore, score);
            out.maxScore = std::max(out.maxScore, score);
        }
    }
    return out;
}

float normalizedBm25Score(double rawScore, float divisor, double minScore, double maxScore) {
    if (maxScore > minScore) {
        const double norm = (rawScore - minScore) / (maxScore - minScore);
        return std::clamp(static_cast<float>(1.0 - norm), 0.0f, 1.0f);
    }
    if (!std::isfinite(divisor) || divisor <= 0.0f) {
        return std::clamp(static_cast<float>(-rawScore), 0.0f, 1.0f);
    }
    return std::clamp(static_cast<float>(-rawScore) / divisor, 0.0f, 1.0f);
}

float filenamePathBoost(const std::string& query, const std::string& filePath,
                        const std::string& fileName) {
    const auto queryTokens = tokenizeLower(query);
    if (queryTokens.empty()) {
        return 1.0f;
    }

    const auto nameTokens = tokenizeLower(fileName);
    const auto pathTokens = tokenizeLower(filePath);
    if (nameTokens.empty() && pathTokens.empty()) {
        return 1.0f;
    }

    std::unordered_set<std::string> nameSet(nameTokens.begin(), nameTokens.end());
    std::unordered_set<std::string> pathSet(pathTokens.begin(), pathTokens.end());

    std::size_t nameMatches = 0;
    std::size_t pathMatches = 0;
    for (const auto& tok : queryTokens) {
        if (nameSet.count(tok)) {
            nameMatches++;
        } else if (pathSet.count(tok)) {
            pathMatches++;
        } else {
            for (const auto& nameTok : nameTokens) {
                if (nameTok.rfind(tok, 0) == 0) {
                    nameMatches++;
                    break;
                }
            }
            if (nameMatches == 0) {
                for (const auto& pathTok : pathTokens) {
                    if (pathTok.rfind(tok, 0) == 0) {
                        pathMatches++;
                        break;
                    }
                }
            }
        }
    }

    if (nameMatches > 0) {
        return 1.0f + std::min(2.0f, 0.5f + static_cast<float>(nameMatches) * 0.5f);
    }
    if (pathMatches > 0) {
        return 1.0f + std::min(1.0f, 0.25f + static_cast<float>(pathMatches) * 0.25f);
    }
    return 1.0f;
}

namespace {

float intentAdaptiveNonCodeMultiplier(QueryIntent intent) {
    switch (intent) {
        case QueryIntent::Code:
            return 0.5f;
        case QueryIntent::Path:
            return 0.65f;
        case QueryIntent::Prose:
            return 1.0f;
        case QueryIntent::Mixed:
            return 0.80f;
    }
    return 1.0f;
}

} // namespace

LexicalScoringOutput scoreLexicalBatch(const LexicalScoringInputs& inputs) {
    LexicalScoringOutput out;
    const auto& rows = inputs.candidates;
    out.results.reserve(rows.size());

    const auto range = computeBm25Range(rows);

    const float nonCodeFileMultiplier =
        inputs.applyIntentAdaptiveWeighting ? intentAdaptiveNonCodeMultiplier(inputs.intent) : 1.0f;

    const std::string queryStr{inputs.query};

    for (size_t rank = 0; rank < rows.size(); ++rank) {
        const auto& sr = rows[rank];
        const auto& filePath = sr.document.filePath;
        const auto& fileName = sr.document.fileName;

        const auto pruneCategory = yams::magic::getPruneCategory(filePath);
        const bool isCodeFile = pruneCategory == yams::magic::PruneCategory::BuildObject ||
                                pruneCategory == yams::magic::PruneCategory::None;

        float scoreMultiplier = isCodeFile ? 1.0f : nonCodeFileMultiplier;
        if (inputs.applyFilenameBoost) {
            scoreMultiplier *= filenamePathBoost(queryStr, filePath, fileName);
        }
        scoreMultiplier *= inputs.penalty;

        const float rawScore = static_cast<float>(sr.score);
        const float normalized =
            normalizedBm25Score(rawScore, inputs.bm25NormDivisor, range.minScore, range.maxScore);
        const float finalScore = std::clamp(scoreMultiplier * normalized, 0.0f, 1.0f);

        if (inputs.admissionMinScore && finalScore < *inputs.admissionMinScore) {
            ++out.admissionDroppedCount;
            out.results.emplace_back(std::nullopt);
            continue;
        }

        ComponentResult cr;
        cr.documentHash = sr.document.sha256Hash;
        cr.filePath = filePath;
        cr.score = finalScore;
        cr.source = inputs.sourceTag;
        cr.rank = inputs.startRank + rank;
        cr.snippet = sr.snippet.empty() ? std::nullopt : std::optional<std::string>(sr.snippet);
        cr.debugInfo["score_multiplier"] = fmt::format("{:.3f}", scoreMultiplier);
        // Absolute scale: the single-hit branch of normalizedBm25Score (no batch range).
        cr.absoluteLexicalScore = std::clamp(
            scoreMultiplier * normalizedBm25Score(rawScore, inputs.bm25NormDivisor, 0.0, 0.0), 0.0f,
            1.0f);

        out.results.emplace_back(std::move(cr));
    }

    return out;
}

Tier1TextSignal classifyTier1TextSignal(std::span<const ComponentResult> components,
                                        size_t minTextHits, float minTopTextScore) {
    Tier1TextSignal signal;
    for (const auto& component : components) {
        if (component.source == ComponentResult::Source::Text ||
            component.source == ComponentResult::Source::SimeonText) {
            ++signal.textHits;
            signal.topTextScore = std::max(
                signal.topTextScore, component.absoluteLexicalScore.value_or(component.score));
        }
    }
    signal.strong = signal.textHits > 0 && signal.textHits >= minTextHits &&
                    signal.topTextScore >= minTopTextScore;
    return signal;
}

bool isStrongSemanticBudgetLexical(std::span<const ComponentResult> components) {
    return classifyTier1TextSignal(components, kSemanticBudgetMinTextHits,
                                   kSemanticBudgetMinTopTextScore)
        .strong;
}

} // namespace yams::search::detail
