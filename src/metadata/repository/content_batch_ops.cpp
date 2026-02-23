#include <algorithm>
#include <yams/metadata/metadata_repository.h>

namespace yams::metadata {

namespace {

std::string buildPlaceholderList(size_t count) {
    std::string list;
    list.reserve(count * 2 + 2);
    list += '(';
    for (size_t i = 0; i < count; ++i) {
        if (i)
            list += ',';
        list += '?';
    }
    list += ')';
    return list;
}

Result<void> bindDocumentIdList(Statement& stmt, int startIndex, const std::vector<int64_t>& ids) {
    for (size_t i = 0; i < ids.size(); ++i) {
        if (auto bindResult = stmt.bind(static_cast<int>(i + startIndex), ids[i]); !bindResult) {
            return bindResult.error();
        }
    }
    return {};
}

} // namespace

Result<std::unordered_map<int64_t, DocumentContent>>
MetadataRepository::batchGetContent(const std::vector<int64_t>& documentIds) {
    if (documentIds.empty()) {
        return std::unordered_map<int64_t, DocumentContent>{};
    }

    return executeReadQuery<std::unordered_map<int64_t, DocumentContent>>(
        [&](Database& db) -> Result<std::unordered_map<int64_t, DocumentContent>> {
            std::string sql =
                "SELECT document_id, content_text, content_length, extraction_method, language "
                "FROM document_content WHERE document_id IN ";
            sql += buildPlaceholderList(documentIds.size());

            auto stmtResult = db.prepare(sql);
            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();

            if (auto bindResult = bindDocumentIdList(stmt, 1, documentIds); !bindResult) {
                return bindResult.error();
            }

            std::unordered_map<int64_t, DocumentContent> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    return stepResult.error();
                }

                if (!stepResult.value()) {
                    break;
                }

                DocumentContent content;
                int64_t docId = stmt.getInt64(0);
                content.documentId = docId;
                content.contentText = stmt.getString(1);
                content.contentLength = stmt.getInt64(2);
                content.extractionMethod = stmt.getString(3);
                content.language = stmt.getString(4);
                result[docId] = std::move(content);
            }

            return result;
        });
}

Result<std::unordered_map<int64_t, std::string>>
MetadataRepository::batchGetContentPreview(const std::vector<int64_t>& documentIds, int maxChars,
                                           int maxDocs) {
    if (documentIds.empty()) {
        return std::unordered_map<int64_t, std::string>{};
    }

    const int effectiveMaxChars = std::max(1, maxChars);
    (void)maxDocs;
    const std::vector<int64_t>& effectiveIds = documentIds;

    return executeReadQuery<std::unordered_map<int64_t, std::string>>(
        [&](Database& db) -> Result<std::unordered_map<int64_t, std::string>> {
            std::string sql = "SELECT document_id, substr(content_text, 1, ?) "
                              "FROM document_content WHERE document_id IN ";
            sql += buildPlaceholderList(effectiveIds.size());

            auto stmtResult = db.prepare(sql);
            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();

            if (auto bindResult = stmt.bind(1, effectiveMaxChars); !bindResult) {
                return bindResult.error();
            }

            if (auto bindResult = bindDocumentIdList(stmt, 2, effectiveIds); !bindResult) {
                return bindResult.error();
            }

            std::unordered_map<int64_t, std::string> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    return stepResult.error();
                }

                if (!stepResult.value()) {
                    break;
                }

                int64_t docId = stmt.getInt64(0);
                result[docId] = stmt.getString(1);
            }

            return result;
        });
}

} // namespace yams::metadata
