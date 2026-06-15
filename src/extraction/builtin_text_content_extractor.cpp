#include <yams/extraction/builtin_text_content_extractor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction {

namespace {

std::string normalizeExtension(const std::string& extension) {
    std::string normalized = extension;
    if (!normalized.empty() && normalized[0] != '.') {
        normalized = "." + normalized;
    }
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
    return normalized;
}

} // namespace

bool BuiltinTextContentExtractor::supports(const std::string& mime,
                                           const std::string& extension) const {
    const std::string normalizedExt = normalizeExtension(extension);
    if (!normalizedExt.empty() && TextExtractorFactory::instance().isSupported(normalizedExt)) {
        return true;
    }
    if (!mime.empty()) {
        return detection::FileTypeDetector::instance().isTextMimeType(mime);
    }
    return false;
}

std::optional<std::string>
BuiltinTextContentExtractor::extractText(const std::vector<std::byte>& bytes,
                                         const std::string& mime, const std::string& extension) {
    const std::string normalizedExt = normalizeExtension(extension);

    if (!normalizedExt.empty()) {
        auto& factory = TextExtractorFactory::instance();
        if (factory.isSupported(normalizedExt)) {
            try {
                if (auto extractor = factory.create(normalizedExt)) {
                    ExtractionConfig cfg{};
                    cfg.preserveFormatting = true;
                    auto result = extractor->extractFromBuffer(
                        std::span<const std::byte>(bytes.data(), bytes.size()), cfg);
                    if (result && result.value().isSuccess() && !result.value().text.empty()) {
                        return result.value().text;
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("BuiltinTextContentExtractor error for {}: {}", normalizedExt,
                              e.what());
            } catch (...) {
                spdlog::debug("BuiltinTextContentExtractor unknown error for {}", normalizedExt);
            }
        }
    }

    auto& detector = detection::FileTypeDetector::instance();
    if (!mime.empty() && detector.isTextMimeType(mime)) {
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    return std::nullopt;
}

} // namespace yams::extraction
