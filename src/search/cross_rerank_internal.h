// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace yams::search::detail {

// Per-doc rerank data the orchestrator turns into a rerank_window_trace json entry.
struct CrossRerankDocTrace {
    std::string filePath;
    std::string documentHash;
    double originalScore = 0.0;
    float rerankScore = 0.0F;
    double rerankNorm = 0.0;
    double finalScore = 0.0;
};

// Result of the cross-rerank compute; the orchestrator emits all telemetry from this.
struct CrossRerankOutcome {
    enum class Status { Skipped, Failed, Applied };
    Status status = Status::Skipped;
    bool attempted = false; // reranker was actually invoked
    std::string skipReason;
    std::string errorMessage;
    std::size_t window = 0;
    double scoreGap = 0.0;
    double blendWeight = 0.0;
    bool replaceScores = false;
    std::vector<ComponentResult> components;
    std::vector<CrossRerankDocTrace> docTraces;
};

using CrossRerankScorerFn =
    std::function<Result<std::vector<float>>(const std::string&, const std::vector<std::string>&)>;

// Rerank input text for one candidate: fileName + filePath + snippet + (bounded) content.
std::string buildCrossRerankText(const SearchResult& result,
                                 const std::shared_ptr<metadata::MetadataRepository>& metadataRepo,
                                 std::size_t textLimit);

// Reranks the top-`window` results in place (min-max normalize → replace/blend → stable-sort) and
// returns the outcome. No telemetry side effects; the caller drives trace/debug from the result.
CrossRerankOutcome
applyCrossRerank(std::vector<SearchResult>& results, const std::string& query,
                 const SearchEngineConfig& config, std::size_t window,
                 const CrossRerankScorerFn& reranker,
                 const std::shared_ptr<metadata::MetadataRepository>& metadataRepo);

} // namespace yams::search::detail
