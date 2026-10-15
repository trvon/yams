// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/metadata/document_metadata.h>
#include <yams/search/query_router.h>
#include <yams/search/search_models.h>

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace yams::search::detail {

struct Bm25Range {
    double minScore = 0.0;
    double maxScore = 0.0;
    bool initialized = false;
};

Bm25Range computeBm25Range(std::span<const metadata::SearchResult> candidates);

float normalizedBm25Score(double rawScore, float divisor, double minScore, double maxScore);

float filenamePathBoost(const std::string& query, const std::string& filePath,
                        const std::string& fileName);

struct LexicalScoringInputs {
    std::span<const metadata::SearchResult> candidates;
    std::string_view query;

    QueryIntent intent = QueryIntent::Mixed;
    bool applyIntentAdaptiveWeighting = false;
    bool applyFilenameBoost = true;

    float bm25NormDivisor = 300.0f;
    float penalty = 1.0f;

    ComponentResult::Source sourceTag = ComponentResult::Source::Text;
    size_t startRank = 0;

    std::optional<float> admissionMinScore;
};

struct LexicalScoringOutput {
    std::vector<std::optional<ComponentResult>> results;
    size_t admissionDroppedCount = 0;
};

LexicalScoringOutput scoreLexicalBatch(const LexicalScoringInputs& inputs);

// Tier-1 lexical evidence used to decide whether a query is weak (widens vector fanout and allows
// weak-query topology expansion).
struct Tier1TextSignal {
    size_t textHits = 0;
    float topTextScore = 0.0f;
    bool strong = false;
};

// Counts Text/SimeonText components and takes their top absoluteLexicalScore (falling back to
// score when unset). Strong means at least `minTextHits` (and at least one) hits with top score >=
// `minTopTextScore`. The batch-normalized score cannot be used here: its top is always 1.0.
// On the absolute scale the threshold is a raw bm25 cut: with bm25NormDivisor 25, 0.30 is
// bm25 <= -7.5 and 0.45 is bm25 <= -11.25.
Tier1TextSignal classifyTier1TextSignal(std::span<const ComponentResult> components,
                                        size_t minTextHits, float minTopTextScore);

} // namespace yams::search::detail
