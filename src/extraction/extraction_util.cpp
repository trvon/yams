#include <yams/extraction/extraction_util.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <memory>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction::util {

namespace {

std::optional<std::string> extractTextFromBytes(const std::vector<std::byte>& bytes,
                                                const std::string& mime,
                                                const std::string& extension,
                                                const ContentExtractorList& extractors) {
    // Normalize extension for factory lookup
    std::string normalizedExt = extension;
    if (!normalizedExt.empty() && normalizedExt[0] != '.') {
        normalizedExt = "." + normalizedExt;
    }
    std::transform(normalizedExt.begin(), normalizedExt.end(), normalizedExt.begin(), ::tolower);

    // 1) Try built-in extractors via TextExtractorFactory first
    //    This handles PlainTextExtractor (.txt, .md, source code, etc.) and HtmlTextExtractor
    if (!normalizedExt.empty()) {
        auto& factory = TextExtractorFactory::instance();
        if (factory.isSupported(normalizedExt)) {
            try {
                auto extractor = factory.create(normalizedExt);
                if (extractor) {
                    ExtractionConfig cfg{};
                    cfg.preserveFormatting = true;
                    auto result = extractor->extractFromBuffer(
                        std::span<const std::byte>(bytes.data(), bytes.size()), cfg);
                    if (result && result.value().isSuccess() && !result.value().text.empty()) {
                        spdlog::debug("Extracted {} bytes via {} for extension {}",
                                      result.value().text.size(), extractor->name(), normalizedExt);
                        return result.value().text;
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("TextExtractorFactory error for {}: {}", normalizedExt, e.what());
            } catch (...) {
                spdlog::debug("TextExtractorFactory unknown error for {}", normalizedExt);
            }
        }
    }

    // 2) Try plugin extractors (IContentExtractor) for formats not handled by built-ins
    //    e.g., PDF, Office docs, etc. provided by external plugins
    for (const auto& ext : extractors) {
        try {
            if (ext && ext->supports(mime, extension)) {
                auto text = ext->extractText(bytes, mime, extension);
                if (text && !text->empty()) {
                    return text;
                }
            }
        } catch (const std::exception& e) {
            spdlog::debug("Plugin extractor threw exception for {}: {}", extension, e.what());
        } catch (...) {
            spdlog::debug("Plugin extractor threw unknown exception for {}", extension);
        }
    }

    // 3) Fallback: raw bytes for text MIME types not handled above
    auto& detector = yams::detection::FileTypeDetector::instance();
    if (!mime.empty() && detector.isTextMimeType(mime)) {
        spdlog::debug("Fallback to raw bytes for MIME type {}", mime);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    // 4) Last resort: extension implies text but wasn't in factory
    try {
        auto detectedMime =
            yams::detection::FileTypeDetector::getMimeTypeFromExtension(normalizedExt);
        if (!detectedMime.empty() && detector.isTextMimeType(detectedMime)) {
            spdlog::debug("Fallback to raw bytes for extension {} (detected MIME: {})",
                          normalizedExt, detectedMime);
            return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        }
    } catch (const std::exception& e) {
        spdlog::debug("FileTypeDetector error for extension {}: {}", extension, e.what());
    } catch (...) {
        spdlog::debug("FileTypeDetector unknown error for extension {}", extension);
    }

    return std::nullopt;
}

} // namespace

std::optional<std::string> extractDocumentText(std::shared_ptr<yams::api::IContentStore> store,
                                               const std::string& hash, const std::string& mime,
                                               const std::string& extension,
                                               const ContentExtractorList& extractors) {
    if (!store)
        return std::nullopt;

    auto bytesRes = store->retrieveBytes(hash);
    if (!bytesRes)
        return std::nullopt;

    const auto& bytes = bytesRes.value();
    return extractTextFromBytes(bytes, mime, extension, extractors);
}

std::optional<ExtractedTextAndBytes>
extractDocumentTextAndBytes(std::shared_ptr<yams::api::IContentStore> store,
                            const std::string& hash, const std::string& mime,
                            const std::string& extension, const ContentExtractorList& extractors) {
    if (!store) {
        return std::nullopt;
    }

    auto bytesRes = store->retrieveBytes(hash);
    if (!bytesRes) {
        return std::nullopt;
    }

    std::vector<std::byte> ownedBytes = std::move(bytesRes.value());
    auto textOpt = extractTextFromBytes(ownedBytes, mime, extension, extractors);
    if (!textOpt || textOpt->empty()) {
        return std::nullopt;
    }

    ExtractedTextAndBytes out;
    out.text = std::move(*textOpt);
    out.bytes = std::make_shared<std::vector<std::byte>>(std::move(ownedBytes));
    return out;
}

std::optional<yams::extraction::util::ExtractedTextBytesAndMetadata>
extractDocumentContent(std::shared_ptr<yams::api::IContentStore> store, const std::string& hash,
                       const std::string& mime, const std::string& extension,
                       const ContentExtractorList& extractors) {
    if (!store)
        return std::nullopt;

    auto bytesRes = store->retrieveBytes(hash);
    if (!bytesRes)
        return std::nullopt;

    std::vector<std::byte> ownedBytes = std::move(bytesRes.value());

    // Normalize extension for factory lookup
    std::string normalizedExt = extension;
    if (!normalizedExt.empty() && normalizedExt[0] != '.')
        normalizedExt = "." + normalizedExt;
    std::transform(normalizedExt.begin(), normalizedExt.end(), normalizedExt.begin(), ::tolower);

    std::string text;
    std::unordered_map<std::string, std::string> metadata;

    // 1) Try built-in extractors
    if (!normalizedExt.empty()) {
        auto& factory = TextExtractorFactory::instance();
        if (factory.isSupported(normalizedExt)) {
            try {
                auto extractor = factory.create(normalizedExt);
                if (extractor) {
                    ExtractionConfig cfg{};
                    cfg.preserveFormatting = true;
                    auto result = extractor->extractFromBuffer(
                        std::span<const std::byte>(ownedBytes.data(), ownedBytes.size()), cfg);
                    if (result && result.value().isSuccess() && !result.value().text.empty()) {
                        text = std::move(result.value().text);
                    }
                }
            } catch (...) {
                spdlog::debug("Built-in extractor failed for extension {}", normalizedExt);
            }
        }
    }

    // 2) Try plugin extractors — prefer the combined text+metadata path
    if (text.empty()) {
        for (const auto& ext : extractors) {
            try {
                if (ext && ext->supports(mime, extension)) {
                    auto content = ext->extractTextAndMetadata(ownedBytes, mime, extension);
                    if (content && !content->text.empty()) {
                        text = std::move(content->text);
                        metadata = std::move(content->metadata);
                        break;
                    }
                }
            } catch (...) {
                spdlog::debug("Plugin extractor failed for extension {}", extension);
            }
        }
    }

    // 3) Fallback: raw bytes for text MIME types
    auto& detector = yams::detection::FileTypeDetector::instance();
    if (text.empty()) {
        if (!mime.empty() && detector.isTextMimeType(mime)) {
            text = std::string(reinterpret_cast<const char*>(ownedBytes.data()), ownedBytes.size());
        }
    }

    // 4) Last resort
    if (text.empty()) {
        try {
            auto detectedMime =
                yams::detection::FileTypeDetector::getMimeTypeFromExtension(normalizedExt);
            if (!detectedMime.empty() && detector.isTextMimeType(detectedMime)) {
                text = std::string(reinterpret_cast<const char*>(ownedBytes.data()),
                                   ownedBytes.size());
            }
        } catch (...) {
            spdlog::debug("FileTypeDetector error for extension {} in extractDocumentContent",
                          extension);
        }
    }

    if (text.empty())
        return std::nullopt;

    ExtractedTextBytesAndMetadata out;
    out.text = std::move(text);
    out.bytes = std::make_shared<std::vector<std::byte>>(std::move(ownedBytes));
    out.metadata = std::move(metadata);
    return out;
}

} // namespace yams::extraction::util
