#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <yams/api/content_store.h>
#include <yams/extraction/content_extractor.h>

namespace yams::extraction::util {

struct ExtractedTextAndBytes {
    std::string text;
    std::shared_ptr<std::vector<std::byte>> bytes;
};

struct ExtractedTextBytesAndMetadata {
    std::string text;
    std::shared_ptr<std::vector<std::byte>> bytes;
    std::unordered_map<std::string, std::string> metadata;
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
                            const std::string& extension, const ContentExtractorList& extractors);

// Like extractDocumentTextAndBytes(), but also returns any metadata extracted by the plugin
// (e.g. PDF title from zyp). Callers can use this metadata to skip ML-based enrichment.
std::optional<ExtractedTextBytesAndMetadata>
extractDocumentContent(std::shared_ptr<yams::api::IContentStore> store, const std::string& hash,
                       const std::string& mime, const std::string& extension,
                       const ContentExtractorList& extractors);

} // namespace yams::extraction::util
