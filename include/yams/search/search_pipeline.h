// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <yams/search/search_models.h>
#include <yams/search/search_statistics.h>

#include <boost/asio/any_io_executor.hpp>

namespace yams::metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace yams::metadata

namespace yams::vector {
class EmbeddingGenerator;
class VectorDatabase;
} // namespace yams::vector

namespace yams::search {

class KGScorer;
class SearchTuner;
class SimeonLexicalBackend;

/**
 * Shared dependency bundle passed to concrete search pipelines.
 *
 * The first extraction keeps classic behavior in the existing implementation, but
 * this object makes future pipeline implementations receive dependencies
 * explicitly instead of reaching back into SearchEngine::Impl member state.
 */
using PipelineCrossRerankScorer = std::function<Result<std::vector<float>>(
    const std::string& query, const std::vector<std::string>& documents)>;

struct SearchPipelineContext {
    std::shared_ptr<metadata::MetadataRepository> metadataRepo;
    std::shared_ptr<vector::VectorDatabase> vectorDb;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGen;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore;
    std::shared_ptr<KGScorer> kgScorer;
    const boost::asio::any_io_executor* executor = nullptr;
    SearchEngineStatistics* statistics = nullptr;
    SearchTuner* tuner = nullptr;
    SimeonLexicalBackend* simeonLexical = nullptr;
    PipelineCrossRerankScorer* crossReranker = nullptr;
};

class SearchPipeline {
public:
    virtual ~SearchPipeline() = default;

    [[nodiscard]] virtual std::string_view name() const noexcept = 0;

    virtual Result<SearchResponse> execute(std::string_view query, const SearchParams& params,
                                           const SearchEngineConfig& config,
                                           SearchPipelineContext& context) = 0;
};

} // namespace yams::search
