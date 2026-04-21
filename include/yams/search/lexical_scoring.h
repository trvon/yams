// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/metadata/document_metadata.h>
#include <yams/search/query_router.h>
#include <yams/search/search_engine.h>

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

} // namespace yams::search::detail
