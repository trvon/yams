#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

namespace yams {
namespace metadata {
class MetadataRepository;
}
namespace vector {
class VectorDatabase;
}
namespace daemon {

class IModelProvider;
class WorkCoordinator;

struct HybridSearchResult {
    std::string hash;
    std::string fileName;
    std::string filePath;
    double score;
    std::string snippet;
};

class SearchPool {
public:
    SearchPool(std::shared_ptr<metadata::MetadataRepository> meta,
               std::shared_ptr<vector::VectorDatabase> vdb, WorkCoordinator* coordinator);
    ~SearchPool() = default;

    void setModelProvider(std::function<std::shared_ptr<IModelProvider>()> providerGetter,
                          std::function<std::string()> modelNameGetter);

    boost::asio::awaitable<std::vector<HybridSearchResult>>
    executeHybridSearch(const std::string& query, std::size_t limit = 20);

private:
    boost::asio::awaitable<std::vector<HybridSearchResult>>
    executeFTS5Query(const std::string& query, std::size_t limit);

    boost::asio::awaitable<std::vector<HybridSearchResult>>
    executeVectorQuery(const std::string& query, std::size_t limit);

    std::vector<HybridSearchResult>
    fuseResultsRRF(const std::vector<HybridSearchResult>& fts5Results,
                   const std::vector<HybridSearchResult>& vectorResults, std::size_t limit);

    std::shared_ptr<metadata::MetadataRepository> meta_;
    std::shared_ptr<vector::VectorDatabase> vdb_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    std::function<std::shared_ptr<IModelProvider>()> getModelProvider_;
    std::function<std::string()> getPreferredModel_;
};

} // namespace daemon
} // namespace yams
