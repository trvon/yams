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

Result<std::unordered_map<std::string, std::pair<DocumentInfo, std::string>>>
MetadataRepository::batchGetDocumentsWithContentPreview(const std::vector<std::string>& hashes,
                                                        int maxPreviewChars) {
    if (hashes.empty()) {
        return std::unordered_map<std::string, std::pair<DocumentInfo, std::string>>{};
    }

    const int effectiveMaxChars = std::max(1, maxPreviewChars);

    return executeReadQuery<std::unordered_map<std::string, std::pair<DocumentInfo, std::string>>>(
        [&](Database& db)
            -> Result<std::unordered_map<std::string, std::pair<DocumentInfo, std::string>>> {
            std::string sql =
                "SELECT d.id, d.file_path, d.file_name, d.file_extension, d.file_size, "
                "d.sha256_hash, d.mime_type, d.created_time, d.modified_time, d.indexed_time, "
                "d.content_extracted, d.extraction_status, d.extraction_error, "
                "COALESCE(substr(dc.content_text, 1, ?), '') "
                "FROM documents d "
                "LEFT JOIN document_content dc ON dc.document_id = d.id "
                "WHERE d.sha256_hash IN ";
            sql += buildPlaceholderList(hashes.size());

            auto stmtResult = db.prepare(sql);
            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();

            if (auto bindResult = stmt.bind(1, effectiveMaxChars); !bindResult) {
                return bindResult.error();
            }
            for (size_t i = 0; i < hashes.size(); ++i) {
                if (auto bindResult = stmt.bind(static_cast<int>(i + 2), hashes[i]); !bindResult) {
                    return bindResult.error();
                }
            }

            std::unordered_map<std::string, std::pair<DocumentInfo, std::string>> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    return stepResult.error();
                }
                if (!stepResult.value()) {
                    break;
                }

                DocumentInfo info;
                info.id = stmt.getInt64(0);
                info.filePath = stmt.getString(1);
                info.fileName = stmt.getString(2);
                info.fileExtension = stmt.getString(3);
                info.fileSize = stmt.getInt64(4);
                info.sha256Hash = stmt.getString(5);
                info.mimeType = stmt.getString(6);
                info.setCreatedTime(stmt.getInt64(7));
                info.setModifiedTime(stmt.getInt64(8));
                info.setIndexedTime(stmt.getInt64(9));
                info.contentExtracted = stmt.getInt(10) != 0;
                info.extractionStatus = ExtractionStatusUtils::fromString(stmt.getString(11));
                info.extractionError = stmt.getString(12);

                std::string preview = stmt.getString(13);
                std::string hashKey = info.sha256Hash;

                std::string hashKey = info.sha256Hash;
                result[hashKey] = std::make_pair(std::move(info), std::move(preview));
            }

            return result;
        });
}

} // namespace yams::metadata
