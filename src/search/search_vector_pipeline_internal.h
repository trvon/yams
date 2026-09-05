#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/vector/vector_types.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::vector {
class VectorDatabase;
}

namespace yams::search::detail {

void mergeVectorSearchDiagnostics(vector::VectorSearchDiagnostics& total,
                                  const vector::VectorSearchDiagnostics& sample);

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

/// Resolve raw document hashes to the metadata primary keys expected by backend-owned indexes.
/// Trace IDs are deliberately excluded: a filename-derived ID is presentation data and need not
/// equal the metadata row ID.
[[nodiscard]] Result<std::vector<std::pair<std::string, std::int64_t>>>
resolveMetadataDocumentIdsByHash(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    std::span<const std::string> documentHashes);

struct RoutedVectorFilterResult {
    std::vector<ComponentResult> results;
    bool applied{false};
    bool fellBackToGlobal{false};
    std::size_t matched{0};
    std::size_t removed{0};
};

struct AuxiliaryVectorCandidateBatch {
    std::string query;
    std::size_t queryIndex{0};
    float weight{1.0F};
    std::vector<ComponentResult> candidates;
};

struct AuxiliaryVectorMergeStats {
    std::size_t baseVectorCount{0};
    std::size_t multiVectorRawHitCount{0};
    std::size_t multiVectorAddedNewCount{0};
    std::size_t multiVectorReplacedBaseCount{0};
    std::size_t multiVectorMergedCount{0};
    std::size_t graphVectorRawHitCount{0};
    std::size_t graphVectorAddedNewCount{0};
    std::size_t graphVectorReplacedBaseCount{0};
    std::size_t graphVectorBlockedUncorroboratedCount{0};
    std::size_t graphVectorBlockedMissingTextAnchorCount{0};
    std::size_t graphVectorBlockedMissingBaselineTextAnchorCount{0};
};

struct AuxiliaryVectorMergeResult {
    std::vector<ComponentResult> components;
    AuxiliaryVectorMergeStats stats;
};

/// Merge already-scored sub-phrase and graph-term vector candidates into component results.
/// Embedding, vector queries, timing, tracing, exception handling, and I/O remain caller-owned.
[[nodiscard]] AuxiliaryVectorMergeResult
mergeAuxiliaryVectorCandidates(std::vector<ComponentResult> components,
                               std::vector<AuxiliaryVectorCandidateBatch> subPhraseCandidates,
                               std::vector<AuxiliaryVectorCandidateBatch> graphTermCandidates,
                               const SearchEngineConfig& config);

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
