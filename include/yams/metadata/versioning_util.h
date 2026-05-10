#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <yams/metadata/document_metadata.h>

namespace yams::metadata {

class MetadataRepository;

int64_t applyPathSeriesVersioning(MetadataRepository& repo, const std::string& filePath,
                                  int64_t newDocumentId,
                                  std::optional<DocumentInfo> prevLatestHint = std::nullopt);

} // namespace yams::metadata
