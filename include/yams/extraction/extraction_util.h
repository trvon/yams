#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/api/content_store.h>
#include <yams/extraction/content_extractor.h>

namespace yams::extraction::util {

struct ExtractedTextAndBytes {
    std::string text;
    std::shared_ptr<std::vector<std::byte>> bytes;
};

// Best-effort text extraction using plugins and built-ins.
// Returns std::nullopt when no text could be extracted.
std::optional<std::string> extractDocumentText(std::shared_ptr<yams::api::IContentStore> store,
                                               const std::string& hash, const std::string& mime,
                                               const std::string& extension,
                                               const ContentExtractorList& extractors);

// Like extractDocumentText(), but also returns the raw document bytes so callers can fan-out
// downstream processing without re-reading from the content store.
std::optional<ExtractedTextAndBytes>
extractDocumentTextAndBytes(std::shared_ptr<yams::api::IContentStore> store,
                            const std::string& hash, const std::string& mime,
                            const std::string& extension,
                            const ContentExtractorList& extractors);

} // namespace yams::extraction::util
