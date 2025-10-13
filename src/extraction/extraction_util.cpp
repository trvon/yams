#include <yams/extraction/extraction_util.h>

#include <spdlog/spdlog.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/html_text_extractor.h>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction::util {

static inline bool is_text_like(const std::string& mime) {
    if (mime.rfind("text/", 0) == 0)
        return true;
    if (mime == "application/json" || mime == "application/xml" || mime == "application/x-yaml" ||
        mime == "application/yaml") {
        return true;
    }
    return false;
}

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

    // 1) Try plugin extractors first (best-match)
    for (const auto& ext : extractors) {
        try {
            if (ext && ext->supports(mime, extension)) {
                auto text = ext->extractText(bytes, mime, extension);
                if (text && !text->empty())
                    return text;
            }
        } catch (const std::exception& e) {
            spdlog::debug("Extractor threw exception for extension {}: {}", extension, e.what());
        } catch (...) {
            spdlog::debug("Extractor threw unknown exception for extension {}", extension);
        }
    }

    // 2) Built-in HTML-aware fallback: if content is HTML and still unhandled, extract readable
    // text
    auto is_html_ext = [&]() {
        return extension == ".html" || extension == ".htm" || extension == ".xhtml";
    };
    if (mime == "text/html" || mime == "application/xhtml+xml" || is_html_ext()) {
        try {
            HtmlTextExtractor html;
            ExtractionConfig cfg{};
            if (auto res = html.extractFromBuffer(bytes, cfg);
                res && res.value().isSuccess() && !res.value().text.empty()) {
                return std::optional<std::string>(res.value().text);
            }
        } catch (...) {
            // fall through to generic fallback
        }
    }

    // 3) Built-in fallbacks for clearly text-like MIME types
    if (is_text_like(mime)) {
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    // 4) As a last resort, if extension implies text
    try {
        auto mt = yams::detection::FileTypeDetector::getMimeTypeFromExtension(extension);
        if (!mt.empty() && is_text_like(mt)) {
            return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        }
    } catch (const std::exception& e) {
        spdlog::debug("FileTypeDetector error for extension {}: {}", extension, e.what());
    } catch (...) {
        // ignore
    }

    return std::nullopt;
}

} // namespace yams::extraction::util
