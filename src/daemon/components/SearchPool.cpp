#include <yams/daemon/components/SearchPool.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <unordered_map>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon {

SearchPool::SearchPool(std::shared_ptr<metadata::MetadataRepository> meta,
                       std::shared_ptr<vector::VectorDatabase> vdb, WorkCoordinator* coordinator)
    : meta_(std::move(meta)), vdb_(std::move(vdb)), coordinator_(coordinator),
      strand_(coordinator_->makeStrand()) {}

void SearchPool::setModelProvider(std::function<std::shared_ptr<IModelProvider>()> providerGetter,
                                  std::function<std::string()> modelNameGetter) {
    getModelProvider_ = std::move(providerGetter);
    getPreferredModel_ = std::move(modelNameGetter);
}

boost::asio::awaitable<std::vector<HybridSearchResult>>
SearchPool::executeHybridSearch(const std::string& query, std::size_t limit) {
    using namespace boost::asio::experimental;

    auto ex = co_await boost::asio::this_coro::executor;

    auto [order, e1, fts5Results, e2, vectorResults] =
        co_await make_parallel_group(
            co_spawn(ex, executeFTS5Query(query, limit), boost::asio::deferred),
            co_spawn(ex, executeVectorQuery(query, limit), boost::asio::deferred))
            .async_wait(wait_for_all(), boost::asio::use_awaitable);

    if (e1) {
        try {
            std::rethrow_exception(e1);
        } catch (const std::exception& e) {
            spdlog::error("[SearchPool] FTS5 query exception: {}", e.what());
        }
    }

    if (e2) {
        try {
            std::rethrow_exception(e2);
        } catch (const std::exception& e) {
            spdlog::error("[SearchPool] Vector query exception: {}", e.what());
        }
    }

    auto fusedResults = fuseResultsRRF(fts5Results, vectorResults, limit);

    spdlog::debug("[SearchPool] Hybrid search complete: {} FTS5 + {} vector → {} fused",
                  fts5Results.size(), vectorResults.size(), fusedResults.size());

    co_return fusedResults;
}

boost::asio::awaitable<std::vector<HybridSearchResult>>
SearchPool::executeFTS5Query(const std::string& query, std::size_t limit) {
    std::vector<HybridSearchResult> results;

    if (!meta_) {
        co_return results;
    }

    try {
        auto searchRes = meta_->search(query, static_cast<int>(limit));
        if (!searchRes) {
            spdlog::warn("[SearchPool] FTS5 search failed: {}", searchRes.error().message);
            co_return results;
        }

        for (const auto& doc : searchRes.value().results) {
            HybridSearchResult sr;
            sr.hash = doc.document.sha256Hash;
            sr.fileName = doc.document.fileName;
            sr.filePath = doc.document.filePath;
            sr.score = doc.score;
            sr.snippet = doc.snippet;
            results.push_back(std::move(sr));
        }

        spdlog::debug("[SearchPool] FTS5 query returned {} results", results.size());
    } catch (const std::exception& e) {
        spdlog::error("[SearchPool] FTS5 query exception: {}", e.what());
    }

    co_return results;
}

boost::asio::awaitable<std::vector<HybridSearchResult>>
SearchPool::executeVectorQuery(const std::string& query, std::size_t limit) {
    std::vector<HybridSearchResult> results;

    if (!vdb_) {
        co_return results;
    }

    std::shared_ptr<IModelProvider> provider;
    std::string modelName;

    if (getModelProvider_)
        provider = getModelProvider_();
    if (getPreferredModel_)
        modelName = getPreferredModel_();

    if (!provider || modelName.empty()) {
        spdlog::warn("[SearchPool] Model provider unavailable for vector query");
        co_return results;
    }

    try {
        auto embeddingRes = provider->generateEmbeddingFor(modelName, query);
        if (!embeddingRes) {
            spdlog::warn("[SearchPool] Failed to generate query embedding: {}",
                         embeddingRes.error().message);
            co_return results;
        }

        vector::VectorSearchParams params;
        params.k = static_cast<int>(limit);

        auto searchRes = vdb_->searchSimilar(embeddingRes.value(), params);

        for (const auto& match : searchRes) {
            HybridSearchResult sr;
            sr.hash = match.document_hash;
            sr.fileName = "";
            sr.filePath = "";
            sr.score = match.relevance_score;
            sr.snippet = match.content;
            results.push_back(std::move(sr));
        }

        spdlog::debug("[SearchPool] Vector query returned {} results", results.size());
    } catch (const std::exception& e) {
        spdlog::error("[SearchPool] Vector query exception: {}", e.what());
    }

    co_return results;
}

std::vector<HybridSearchResult>
SearchPool::fuseResultsRRF(const std::vector<HybridSearchResult>& fts5Results,
                           const std::vector<HybridSearchResult>& vectorResults,
                           std::size_t limit) {
    constexpr double k = 60.0;

    std::unordered_map<std::string, double> rrfScores;
    std::unordered_map<std::string, HybridSearchResult> resultMap;

    for (std::size_t i = 0; i < fts5Results.size(); ++i) {
        const auto& sr = fts5Results[i];
        double rrfScore = 1.0 / (k + static_cast<double>(i + 1));
        rrfScores[sr.hash] += rrfScore;
        resultMap[sr.hash] = sr;
    }

    for (std::size_t i = 0; i < vectorResults.size(); ++i) {
        const auto& sr = vectorResults[i];
        double rrfScore = 1.0 / (k + static_cast<double>(i + 1));
        rrfScores[sr.hash] += rrfScore;

        if (resultMap.find(sr.hash) == resultMap.end()) {
            resultMap[sr.hash] = sr;
        }
    }

    std::vector<std::pair<std::string, double>> sortedScores;
    sortedScores.reserve(rrfScores.size());
    for (const auto& [hash, score] : rrfScores) {
        sortedScores.emplace_back(hash, score);
    }

    std::sort(sortedScores.begin(), sortedScores.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    std::vector<HybridSearchResult> fusedResults;
    fusedResults.reserve(std::min(limit, sortedScores.size()));

    for (std::size_t i = 0; i < std::min(limit, sortedScores.size()); ++i) {
        const auto& hash = sortedScores[i].first;
        auto it = resultMap.find(hash);
        if (it != resultMap.end()) {
            auto sr = it->second;
            sr.score = sortedScores[i].second;
            fusedResults.push_back(std::move(sr));
        }
    }

    spdlog::debug("[SearchPool] RRF fusion: {} FTS5 + {} vector → {} fused", fts5Results.size(),
                  vectorResults.size(), fusedResults.size());

    return fusedResults;
}

} // namespace yams::daemon
