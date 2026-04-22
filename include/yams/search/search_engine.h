#pragma once

#include <yams/core/types.h>
#include <yams/search/query_concept_extractor.h>
#include <yams/search/search_result_fusion.h>

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/any_io_executor.hpp>

namespace yams::metadata {
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::vector {
class EmbeddingGenerator;
class VectorDatabase;
} // namespace yams::vector

namespace yams::search {

class SearchTuner;
class SimeonLexicalBackend;

class SearchEngine {
public:
    explicit SearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                          std::shared_ptr<vector::VectorDatabase> vectorDb,
                          std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                          std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                          const SearchEngineConfig& config = {});

    ~SearchEngine();

    SearchEngine(const SearchEngine&) = delete;
    SearchEngine& operator=(const SearchEngine&) = delete;
    SearchEngine(SearchEngine&&) noexcept;
    SearchEngine& operator=(SearchEngine&&) noexcept;

    Result<std::vector<SearchResult>> search(const std::string& query,
                                             const SearchParams& params = {});
    Result<SearchResponse> searchWithResponse(const std::string& query,
                                              const SearchParams& params = {});

    void setConfig(const SearchEngineConfig& config);
    const SearchEngineConfig& getConfig() const;

    struct Statistics {
        std::atomic<uint64_t> totalQueries{0};
        std::atomic<uint64_t> successfulQueries{0};
        std::atomic<uint64_t> failedQueries{0};
        std::atomic<uint64_t> timedOutQueries{0};
        std::atomic<uint64_t> textQueries{0};
        std::atomic<uint64_t> pathTreeQueries{0};
        std::atomic<uint64_t> kgQueries{0};
        std::atomic<uint64_t> vectorQueries{0};
        std::atomic<uint64_t> entityVectorQueries{0};
        std::atomic<uint64_t> tagQueries{0};
        std::atomic<uint64_t> metadataQueries{0};
        std::atomic<uint64_t> totalQueryTimeMicros{0};
        std::atomic<uint64_t> avgQueryTimeMicros{0};
        std::atomic<uint64_t> avgTextTimeMicros{0};
        std::atomic<uint64_t> avgPathTreeTimeMicros{0};
        std::atomic<uint64_t> avgKgTimeMicros{0};
        std::atomic<uint64_t> avgVectorTimeMicros{0};
        std::atomic<uint64_t> avgEntityVectorTimeMicros{0};
        std::atomic<uint64_t> avgTagTimeMicros{0};
        std::atomic<uint64_t> avgMetadataTimeMicros{0};
        std::atomic<uint64_t> avgResultsPerQuery{0};
        std::atomic<uint64_t> avgComponentsPerResult{0};
    };

    const Statistics& getStatistics() const;
    void resetStatistics();
    Result<void> healthCheck();
    void setExecutor(std::optional<boost::asio::any_io_executor> executor);
    void setConceptExtractor(EntityExtractionFunc extractor);
    void setSearchTuner(std::shared_ptr<SearchTuner> tuner);
    void setSimeonLexicalBackend(std::unique_ptr<SimeonLexicalBackend> backend);
    std::shared_ptr<SearchTuner> getSearchTuner() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
};

std::unique_ptr<SearchEngine>
createSearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                   std::shared_ptr<vector::VectorDatabase> vectorDb,
                   std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                   std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                   const SearchEngineConfig& config = {});

} // namespace yams::search
