// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "search_models.h"
#include "search_tracing.h"

namespace yams::search {

// Centralizes the externally visible search-observability surfaces on SearchResponse:
// component state, component timing, degraded status, and stable trace/debug-stat payloads.
struct SearchObservability {
    std::vector<std::string> timedOutComponents;
    std::vector<std::string> failedComponents;
    std::vector<std::string> contributingComponents;
    std::vector<std::string> skippedComponents;
    std::map<std::string, std::int64_t> componentTimingMicros;
    bool includeComponentTiming = false;
    bool includeSimeonLexicalTiming = false;
    std::int64_t simeonLexicalScoreMicrosDelta = 0;

    void applyComponentStatus(SearchResponse& response) {
        response.timedOutComponents = std::move(timedOutComponents);
        response.failedComponents = std::move(failedComponents);
        response.contributingComponents = std::move(contributingComponents);
        response.skippedComponents = std::move(skippedComponents);
        response.usedEarlyTermination = false;

        if (includeComponentTiming) {
            componentTimingMicros.merge(response.componentTimingMicros);
            response.componentTimingMicros = std::move(componentTimingMicros);
            if (includeSimeonLexicalTiming) {
                response.componentTimingMicros[std::string(kSimeonLexicalScoreTimingKey)] =
                    simeonLexicalScoreMicrosDelta;
            }
        }

        // Lean contract SearchEngine.runSearch_degradedFlag: degraded iff any component timed out
        // or failed. Skipped components are expected source-gating information, not degradation.
        response.isDegraded =
            !response.timedOutComponents.empty() || !response.failedComponents.empty();
    }

    static void publishTraceSummaryDebugStats(
        SearchResponse& response, const SearchTraceCollector& traceCollector,
        const std::vector<ComponentResult>& allComponentResults, std::size_t userLimit) {
        response.debugStats["trace_stage_summary_json"] =
            traceCollector.buildStageSummaryJson().dump();
        response.debugStats["trace_fusion_source_summary_json"] =
            traceCollector
                .buildFusionSourceSummaryJson(allComponentResults, response.results,
                                              std::max<std::size_t>(userLimit, std::size_t{25}))
                .dump();
    }
};

} // namespace yams::search
