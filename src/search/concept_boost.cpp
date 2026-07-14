// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "concept_boost_internal.h"

#include <yams/app/services/simd_memmem.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <future>
#include <string>
#include <string_view>
#include <thread>

namespace yams::search::detail {

namespace {

bool containsFast(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    if (needle.size() > haystack.size()) {
        return false;
    }
    return yams::app::services::simdMemmem(haystack, needle) != yams::app::services::kMemmemNpos;
}

} // namespace

void applyConceptBoost(std::vector<SearchResult>& results,
                       const std::vector<QueryConcept>& concepts, const SearchEngineConfig& config) {
    if (concepts.empty() || config.conceptBoostWeight <= 0.0F || config.conceptMaxBoost <= 0.0F ||
        results.empty()) {
        return;
    }

    std::vector<std::string> conceptTerms;
    conceptTerms.reserve(concepts.size());
    for (const auto& conceptItem : concepts) {
        if (conceptItem.text.empty()) {
            continue;
        }
        std::string lowered = conceptItem.text;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        conceptTerms.push_back(std::move(lowered));
    }
    if (conceptTerms.empty()) {
        return;
    }
    std::sort(conceptTerms.begin(), conceptTerms.end());
    conceptTerms.erase(std::unique(conceptTerms.begin(), conceptTerms.end()), conceptTerms.end());

    const size_t totalResults = results.size();
    const size_t scanLimit = std::min(config.conceptMaxScanResults, totalResults);
    if (scanLimit == 0) {
        return;
    }

    std::vector<uint32_t> matchCounts(scanLimit, 0);
    const size_t minChunkSize = std::max<size_t>(1, config.minChunkSizeForParallel);
    const size_t maxThreads = std::max<size_t>(1, std::thread::hardware_concurrency());
    const size_t chunkTarget = (scanLimit + minChunkSize - 1) / minChunkSize;
    const size_t numThreads = std::min(maxThreads, std::max<size_t>(1, chunkTarget));
    const bool useParallelBoost = numThreads > 1;

    auto computeMatches = [&](size_t start, size_t end) {
        std::vector<uint32_t> matches;
        matches.reserve(end - start);
        for (size_t idx = start; idx < end; ++idx) {
            const auto& result = results[idx];
            uint32_t count = 0;
            for (const auto& term : conceptTerms) {
                if (!term.empty() && (containsFast(result.snippet, term) ||
                                      containsFast(result.document.fileName, term))) {
                    count++;
                }
            }
            matches.push_back(count);
        }
        return matches;
    };

    if (useParallelBoost) {
        const size_t chunkSize = (scanLimit + numThreads - 1) / numThreads;
        std::vector<std::future<std::vector<uint32_t>>> futures;
        futures.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            const size_t start = i * chunkSize;
            const size_t end = std::min(start + chunkSize, scanLimit);
            if (start >= end) {
                break;
            }
            futures.push_back(std::async(std::launch::async,
                                         [start, end, &computeMatches]() {
                                             return computeMatches(start, end);
                                         }));
        }
        size_t offset = 0;
        for (auto& future : futures) {
            auto matches = future.get();
            for (size_t i = 0; i < matches.size(); ++i) {
                matchCounts[offset + i] = matches[i];
            }
            offset += matches.size();
        }
    } else {
        auto matches = computeMatches(0, scanLimit);
        for (size_t i = 0; i < matches.size(); ++i) {
            matchCounts[i] = matches[i];
        }
    }

    bool boosted = false;
    float boostBudget = config.conceptMaxBoost;
    for (size_t i = 0; i < scanLimit; ++i) {
        if (boostBudget <= 0.0F) {
            break;
        }
        const uint32_t matchCount = matchCounts[i];
        if (matchCount == 0) {
            continue;
        }
        const float desiredBoost = config.conceptBoostWeight * static_cast<float>(matchCount);
        const float appliedBoost = std::min(desiredBoost, boostBudget);
        results[i].score *= (1.0F + appliedBoost);
        boostBudget -= appliedBoost;
        boosted = true;
    }

    if (boosted) {
        std::sort(results.begin(), results.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }
}

} // namespace yams::search::detail
