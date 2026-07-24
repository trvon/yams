#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <yams/metadata/document_metadata.h>

namespace yams::metadata {

class IMetadataRepository;
class MetadataRepository;

/// Retains the newest versions in each document series. A value of one always
/// preserves the current version, even if version metadata is malformed.
struct VersionRetentionPolicy {
    std::size_t keepLatest{1};
    std::optional<std::string> seriesKey;
};

struct VersionRetentionCandidate {
    DocumentInfo document;
    int64_t version{0};
    std::string seriesKey;
};

/// Returns superseded document versions that are safe to remove. This is
/// selection only; callers must delete through DocumentService so all derived
/// indexes and content references are cleaned up consistently.
Result<std::vector<VersionRetentionCandidate>>
selectVersionRetentionCandidates(IMetadataRepository& repo, const VersionRetentionPolicy& policy);

int64_t applyPathSeriesVersioning(MetadataRepository& repo, const std::string& filePath,
                                  int64_t newDocumentId,
                                  std::optional<DocumentInfo> prevLatestHint = std::nullopt);

} // namespace yams::metadata
