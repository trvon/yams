#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/vector/vector_types.h>

#include <memory>
#include <optional>
#include <string>
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

struct CandidateRescueMergeResult {
    std::vector<ComponentResult> results;
    std::vector<std::string> addedDocumentHashes;
    std::vector<std::string> addedDocumentIds;
    std::vector<std::string> novelDocumentHashes;
    std::vector<std::string> novelDocumentIds;
    std::vector<std::string> evidenceRescueDocumentHashes;
    std::vector<std::string> evidenceRescueDocumentIds;
    std::size_t added{0};
    std::size_t novelDocuments{0};
    std::size_t evidenceRescues{0};
    std::size_t duplicates{0};
};

/// Union query-scored relation-expansion hits with the global vector candidates. Baseline vector
/// candidates are never removed. A routed hit already present in another component is retained as
/// new vector evidence and classified separately from a novel document.
[[nodiscard]] CandidateRescueMergeResult
mergeVectorCandidateRescues(std::vector<ComponentResult> baseline,
                            std::vector<ComponentResult> expansion,
                            const std::unordered_set<std::string>& existingCandidates = {});

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
