#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/vector/vector_types.h>

#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::vector {
class VectorDatabase;
}

namespace yams::search::detail {

Result<std::vector<ComponentResult>>
queryVectorIndexPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                         const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                         const std::vector<float>& embedding, const SearchEngineConfig& config,
                         size_t limit, vector::VectorSearchDiagnostics* diagnostics = nullptr);

Result<std::vector<ComponentResult>>
queryVectorIndexPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                         const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                         const std::vector<float>& embedding, const SearchEngineConfig& config,
                         size_t limit, const std::unordered_set<std::string>& candidates,
                         vector::CandidateFilterMode candidateFilterMode,
                         vector::VectorSearchDiagnostics* diagnostics = nullptr);

Result<std::vector<ComponentResult>>
queryEntityVectorsPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                           const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                           const std::vector<float>& embedding, const SearchEngineConfig& config,
                           size_t limit);

struct RoutedVectorFilterResult {
    std::vector<ComponentResult> results;
    bool applied{false};
    bool fellBackToGlobal{false};
    std::size_t matched{0};
    std::size_t removed{0};
};

/// Retain global vector hits belonging to a confident topology route. When the ANN result has no
/// route overlap, preserve it unchanged so routing cannot erase the vector fallback.
[[nodiscard]] RoutedVectorFilterResult
filterVectorResultsByAllowedDocuments(std::vector<ComponentResult> globalResults,
                                      const std::unordered_set<std::string>& allowedDocuments);

#ifdef YAMS_TESTING
size_t testingVectorRawCandidateLimit(const SearchEngineConfig& config, size_t limit,
                                      bool narrowedSearch) noexcept;
#endif

} // namespace yams::search::detail
