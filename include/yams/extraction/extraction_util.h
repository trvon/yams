#pragma once

#include <optional>
#include <string>
#include <vector>

#include <yams/api/content_store.h>
#include <yams/extraction/content_extractor.h>

namespace yams::extraction::util {

// Best-effort text extraction using plugins and built-ins.
// Returns std::nullopt when no text could be extracted.
std::optional<std::string> extractDocumentText(std::shared_ptr<yams::api::IContentStore> store,
                                               const std::string& hash, const std::string& mime,
                                               const std::string& extension,
                                               const ContentExtractorList& extractors);

} // namespace yams::extraction::util
