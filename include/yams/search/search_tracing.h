// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/search_engine.h>

namespace yams::search {

std::string deriveDocIdFromPath(const std::string& path);
std::string documentIdForTrace(const std::string& filePath, const std::string& documentHash);

std::vector<std::string>
collectUniqueComponentDocIds(const std::vector<ComponentResult>& componentResults);

std::vector<std::string>
collectRankedResultDocIds(const std::vector<SearchResult>& results,
                          size_t maxCount = std::numeric_limits<size_t>::max());

std::vector<std::string> setDifferenceIds(const std::vector<std::string>& lhs,
                                          const std::vector<std::string>& rhs);

std::string joinWithTab(const std::vector<std::string>& values);

nlohmann::json buildComponentHitSummaryJson(const std::vector<ComponentResult>& componentResults,
                                            size_t topPerComponent);

struct TraceStageSummary {
    bool enabled = false;
    bool attempted = false;
    bool contributed = false;
    bool timedOut = false;
    bool failed = false;
    bool skipped = false;
    std::string skipReason;
    std::size_t rawHitCount = 0;
    std::size_t uniqueDocCount = 0;
    std::vector<std::string> uniqueDocIds;
    std::int64_t durationMicros = 0;
    // Per-component score stats. Populated by markStageResult when results non-empty.
    // Vector stage: min/max are cosine similarity (relevance_score from HNSW). SearchTuner
    // reads these on the "vector" stage to adapt similarityThreshold at runtime.
    bool scoreStatsValid = false;
    double minScore = 0.0;
    double maxScore = 0.0;
    // Per-surface diagnostic counters populated via recordStageCounter.
    // Used by Phase F1 pool-pipeline attribution (vector_backend_hits, vector_shouldNarrow_applied,
    // vector_shouldSkipSemantic, vector_relaxed_retry_*, etc.) and surfaced under
    // stage["counters"] in buildStageSummaryJson.
    std::map<std::string, std::int64_t> extraCounters;
};

class SearchTraceCollector {
public:
    explicit SearchTraceCollector(const SearchEngineConfig& config);

    void markStageConfigured(const std::string& name, bool enabled);
    void markStageAttempted(const std::string& name);
    void markStageResult(const std::string& name, const std::vector<ComponentResult>& results,
                         std::int64_t durationMicros, bool contributed);
    void markStageTimeout(const std::string& name, std::int64_t durationMicros = 0);
    void markStageFailure(const std::string& name, std::int64_t durationMicros = 0);
    void markStageSkipped(const std::string& name, std::string reason);
    void recordStageCounter(const std::string& name, const std::string& key, std::int64_t value);

    nlohmann::json buildStageSummaryJson() const;
    nlohmann::json
    buildFusionSourceSummaryJson(const std::vector<ComponentResult>& componentResults,
                                 const std::vector<SearchResult>& finalResults,
                                 std::size_t topPerSource) const;

private:
    const SearchEngineConfig& config_;
    std::map<std::string, TraceStageSummary> stages_;
};

nlohmann::json buildFusionTopSummaryJson(const std::vector<SearchResult>& results, size_t maxDocs);

std::optional<std::int64_t>
resolveKgDocumentId(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                    const metadata::DocumentInfo& doc);

nlohmann::json buildGraphDocProbeJson(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                                      const std::vector<SearchResult>& results, size_t limit);

} // namespace yams::search
