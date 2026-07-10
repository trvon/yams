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
                         vector::VectorSearchDiagnostics* diagnostics = nullptr);

Result<std::vector<ComponentResult>>
queryEntityVectorsPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                           const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                           const std::vector<float>& embedding, const SearchEngineConfig& config,
                           size_t limit);

/// Reuse exact/ANN scores already computed while selecting routed members.
/// Returns nullopt unless every allowed document has a precomputed score.
[[nodiscard]] std::optional<std::vector<ComponentResult>> reusePrecomputedVectorResults(
    const std::vector<ComponentResult>& precomputed,
    const std::unordered_set<std::string>& allowedDocuments, size_t limit);

#ifdef YAMS_TESTING
size_t testingVectorRawCandidateLimit(const SearchEngineConfig& config, size_t limit,
                                      bool narrowedSearch) noexcept;
#endif

} // namespace yams::search::detail
