#include <spdlog/spdlog.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

namespace yams::ingest {

namespace {
constexpr size_t kMaxTextToPersistInMetadataBytes = 16 * 1024 * 1024; // 16 MiB (best-effort)
}

Result<void> persist_content_and_index(metadata::IMetadataRepository& meta, int64_t docId,
                                       const std::string& title, const std::string& text,
                                       const std::string& mime, const std::string& method) {
    YAMS_ZONE_SCOPED_N("ingest::persist_content_and_index");
    try {
        metadata::DocumentContent contentRow;
        contentRow.documentId = docId;
        if (text.size() > kMaxTextToPersistInMetadataBytes) {
            contentRow.contentText = text.substr(0, kMaxTextToPersistInMetadataBytes);
        } else {
            contentRow.contentText = text;
        }
        contentRow.contentLength = static_cast<int64_t>(contentRow.contentText.size());
        contentRow.extractionMethod = method;
        double langConfidence = 0.0;
        contentRow.language = yams::extraction::LanguageDetector::detectLanguage(
            contentRow.contentText, &langConfidence);

        if (const auto up = meta.insertContent(contentRow); !up) {
            return Result<void>(Error{up.error()});
        }
        if (auto r = meta.indexDocumentContentTrusted(docId, title, text, mime); !r) {
            return Result<void>(Error{r.error()});
        }

        (void)meta.updateDocumentExtractionStatus(docId, true, metadata::ExtractionStatus::Success);
        return Result<void>();
    } catch (const std::exception& e) {
        return Result<void>(Error{ErrorCode::InternalError, e.what()});
    } catch (...) {
        return Result<void>(Error{ErrorCode::InternalError, "persist_content_and_index: unknown"});
    }
}

} // namespace yams::ingest
