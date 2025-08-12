#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/document_metadata.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <iostream>

namespace yams::metadata {

MetadataRepository::MetadataRepository(ConnectionPool& pool) : pool_(pool) {
}

// Document operations
Result<int64_t> MetadataRepository::insertDocument(const DocumentInfo& info) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO documents (
                file_path, file_name, file_extension, file_size, sha256_hash,
                mime_type, created_time, modified_time, indexed_time,
                content_extracted, extraction_status, extraction_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            info.filePath, info.fileName, info.fileExtension, info.fileSize,
            info.sha256Hash, info.mimeType, info.createdTimeUnix(),
            info.modifiedTimeUnix(), info.indexedTimeUnix(),
            info.contentExtracted ? 1 : 0,
            ExtractionStatusUtils::toString(info.extractionStatus),
            info.extractionError
        );
        
        if (!bindResult) return bindResult.error();
        
        auto execResult = stmt.execute();
        if (!execResult) return execResult.error();
        
        return db.lastInsertRowId();
    });
}

Result<std::optional<DocumentInfo>> MetadataRepository::getDocument(int64_t id) {
    return executeQuery<std::optional<DocumentInfo>>([&](Database& db) -> Result<std::optional<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, file_path, file_name, file_extension, file_size,
                   sha256_hash, mime_type, created_time, modified_time,
                   indexed_time, content_extracted, extraction_status,
                   extraction_error
            FROM documents WHERE id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, id);
        if (!bindResult) return bindResult.error();
        
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        if (!stepResult.value()) {
            return std::optional<DocumentInfo>{};
        }
        
        return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
    });
}

Result<std::optional<DocumentInfo>> MetadataRepository::getDocumentByHash(const std::string& hash) {
    return executeQuery<std::optional<DocumentInfo>>([&](Database& db) -> Result<std::optional<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, file_path, file_name, file_extension, file_size,
                   sha256_hash, mime_type, created_time, modified_time,
                   indexed_time, content_extracted, extraction_status,
                   extraction_error
            FROM documents WHERE sha256_hash = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, hash);
        if (!bindResult) return bindResult.error();
        
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        if (!stepResult.value()) {
            return std::optional<DocumentInfo>{};
        }
        
        return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
    });
}

Result<void> MetadataRepository::updateDocument(const DocumentInfo& info) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            UPDATE documents SET
                file_path = ?, file_name = ?, file_extension = ?,
                file_size = ?, sha256_hash = ?, mime_type = ?,
                created_time = ?, modified_time = ?, indexed_time = ?,
                content_extracted = ?, extraction_status = ?,
                extraction_error = ?
            WHERE id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            info.filePath, info.fileName, info.fileExtension, info.fileSize,
            info.sha256Hash, info.mimeType, info.createdTimeUnix(),
            info.modifiedTimeUnix(), info.indexedTimeUnix(),
            info.contentExtracted ? 1 : 0,
            ExtractionStatusUtils::toString(info.extractionStatus),
            info.extractionError, info.id
        );
        
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

Result<void> MetadataRepository::deleteDocument(int64_t id) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Foreign key constraints will handle cascading deletes
        auto stmtResult = db.prepare("DELETE FROM documents WHERE id = ?");
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, id);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

// Content operations
Result<void> MetadataRepository::insertContent(const DocumentContent& content) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO document_content (
                document_id, content_text, content_length,
                extraction_method, language
            ) VALUES (?, ?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            content.documentId, content.contentText, content.contentLength,
            content.extractionMethod, content.language
        );
        
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

Result<std::optional<DocumentContent>> MetadataRepository::getContent(int64_t documentId) {
    return executeQuery<std::optional<DocumentContent>>([&](Database& db) -> Result<std::optional<DocumentContent>> {
        auto stmtResult = db.prepare(R"(
            SELECT document_id, content_text, content_length,
                   extraction_method, language
            FROM document_content WHERE document_id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult) return bindResult.error();
        
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        if (!stepResult.value()) {
            return std::optional<DocumentContent>{};
        }
        
        return std::optional<DocumentContent>{mapContentRow(stmt)};
    });
}

Result<void> MetadataRepository::updateContent(const DocumentContent& content) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            UPDATE document_content SET
                content_text = ?, content_length = ?,
                extraction_method = ?, language = ?
            WHERE document_id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            content.contentText, content.contentLength,
            content.extractionMethod, content.language,
            content.documentId
        );
        
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

Result<void> MetadataRepository::deleteContent(int64_t documentId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare("DELETE FROM document_content WHERE document_id = ?");
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

// Metadata operations
Result<void> MetadataRepository::setMetadata(int64_t documentId, const std::string& key,
                                            const MetadataValue& value) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Use INSERT OR REPLACE to handle both insert and update
        auto stmtResult = db.prepare(R"(
            INSERT OR REPLACE INTO metadata (document_id, key, value, value_type)
            VALUES (?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            documentId, key, value.value,
            MetadataValueTypeUtils::toString(value.type)
        );
        
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

Result<std::optional<MetadataValue>> MetadataRepository::getMetadata(int64_t documentId,
                                                                   const std::string& key) {
    return executeQuery<std::optional<MetadataValue>>([&](Database& db) -> Result<std::optional<MetadataValue>> {
        auto stmtResult = db.prepare(R"(
            SELECT value, value_type FROM metadata
            WHERE document_id = ? AND key = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(documentId, key);
        if (!bindResult) return bindResult.error();
        
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        if (!stepResult.value()) {
            return std::optional<MetadataValue>{};
        }
        
        MetadataValue value;
        value.value = stmt.getString(0);
        value.type = MetadataValueTypeUtils::fromString(stmt.getString(1));
        
        return std::optional<MetadataValue>{value};
    });
}

Result<std::unordered_map<std::string, MetadataValue>> MetadataRepository::getAllMetadata(
    int64_t documentId) {
    return executeQuery<std::unordered_map<std::string, MetadataValue>>([&](Database& db) 
        -> Result<std::unordered_map<std::string, MetadataValue>> {
        
        auto stmtResult = db.prepare(R"(
            SELECT key, value, value_type FROM metadata
            WHERE document_id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult) return bindResult.error();
        
        std::unordered_map<std::string, MetadataValue> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            MetadataValue value;
            value.value = stmt.getString(1);
            value.type = MetadataValueTypeUtils::fromString(stmt.getString(2));
            
            result[stmt.getString(0)] = value;
        }
        
        return result;
    });
}

Result<void> MetadataRepository::removeMetadata(int64_t documentId, const std::string& key) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            DELETE FROM metadata WHERE document_id = ? AND key = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(documentId, key);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

// Relationship operations
Result<int64_t> MetadataRepository::insertRelationship(const DocumentRelationship& relationship) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO document_relationships (
                parent_id, child_id, relationship_type, created_time
            ) VALUES (?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        
        Result<void> bindResult;
        if (relationship.parentId > 0) {
            bindResult = stmt.bindAll(
                relationship.parentId,
                relationship.childId,
                relationship.getRelationshipTypeString(),
                relationship.createdTimeUnix()
            );
        } else {
            bindResult = stmt.bind(1, nullptr);
            if (bindResult.has_value()) {
                bindResult = stmt.bind(2, relationship.childId);
            }
            if (bindResult.has_value()) {
                bindResult = stmt.bind(3, relationship.getRelationshipTypeString());
            }
            if (bindResult.has_value()) {
                bindResult = stmt.bind(4, relationship.createdTimeUnix());
            }
        }
        
        if (!bindResult) return bindResult.error();
        
        auto execResult = stmt.execute();
        if (!execResult) return execResult.error();
        
        return db.lastInsertRowId();
    });
}

Result<std::vector<DocumentRelationship>> MetadataRepository::getRelationships(int64_t documentId) {
    return executeQuery<std::vector<DocumentRelationship>>([&](Database& db) -> Result<std::vector<DocumentRelationship>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, parent_id, child_id, relationship_type, created_time
            FROM document_relationships
            WHERE parent_id = ? OR child_id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(documentId, documentId);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentRelationship> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result.push_back(mapRelationshipRow(stmt));
        }
        
        return result;
    });
}

Result<void> MetadataRepository::deleteRelationship(int64_t relationshipId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            DELETE FROM document_relationships WHERE id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, relationshipId);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

// Search history operations
Result<int64_t> MetadataRepository::insertSearchHistory(const SearchHistoryEntry& entry) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO search_history (
                query, query_time, results_count, execution_time_ms, user_context
            ) VALUES (?, ?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            entry.query, entry.queryTimeUnix(), entry.resultsCount,
            entry.executionTimeMs, entry.userContext
        );
        
        if (!bindResult) return bindResult.error();
        
        auto execResult = stmt.execute();
        if (!execResult) return execResult.error();
        
        return db.lastInsertRowId();
    });
}

Result<std::vector<SearchHistoryEntry>> MetadataRepository::getRecentSearches(int limit) {
    return executeQuery<std::vector<SearchHistoryEntry>>([&](Database& db) -> Result<std::vector<SearchHistoryEntry>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, query, query_time, results_count, execution_time_ms, user_context
            FROM search_history
            ORDER BY query_time DESC
            LIMIT ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, limit);
        if (!bindResult) return bindResult.error();
        
        std::vector<SearchHistoryEntry> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result.push_back(mapSearchHistoryRow(stmt));
        }
        
        return result;
    });
}

// Saved queries operations
Result<int64_t> MetadataRepository::insertSavedQuery(const SavedQuery& query) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO saved_queries (
                name, query, description, created_time, last_used, use_count
            ) VALUES (?, ?, ?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            query.name, query.query, query.description,
            query.createdTimeUnix(), query.lastUsedUnix(), query.useCount
        );
        
        if (!bindResult) return bindResult.error();
        
        auto execResult = stmt.execute();
        if (!execResult) return execResult.error();
        
        return db.lastInsertRowId();
    });
}

Result<std::optional<SavedQuery>> MetadataRepository::getSavedQuery(int64_t id) {
    return executeQuery<std::optional<SavedQuery>>([&](Database& db) -> Result<std::optional<SavedQuery>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, name, query, description, created_time, last_used, use_count
            FROM saved_queries WHERE id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, id);
        if (!bindResult) return bindResult.error();
        
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        if (!stepResult.value()) {
            return std::optional<SavedQuery>{};
        }
        
        return std::optional<SavedQuery>{mapSavedQueryRow(stmt)};
    });
}

Result<std::vector<SavedQuery>> MetadataRepository::getAllSavedQueries() {
    return executeQuery<std::vector<SavedQuery>>([&](Database& db) -> Result<std::vector<SavedQuery>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, name, query, description, created_time, last_used, use_count
            FROM saved_queries
            ORDER BY use_count DESC, last_used DESC
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        std::vector<SavedQuery> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result.push_back(mapSavedQueryRow(stmt));
        }
        
        return result;
    });
}

Result<void> MetadataRepository::updateSavedQuery(const SavedQuery& query) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            UPDATE saved_queries SET
                name = ?, query = ?, description = ?,
                created_time = ?, last_used = ?, use_count = ?
            WHERE id = ?
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            query.name, query.query, query.description,
            query.createdTimeUnix(), query.lastUsedUnix(),
            query.useCount, query.id
        );
        
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

Result<void> MetadataRepository::deleteSavedQuery(int64_t id) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare("DELETE FROM saved_queries WHERE id = ?");
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, id);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

// Full-text search operations
Result<void> MetadataRepository::indexDocumentContent(int64_t documentId, 
                                                    const std::string& title,
                                                    const std::string& content,
                                                    const std::string& contentType) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // First check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result) return fts5Result.error();
        
        if (!fts5Result.value()) {
            spdlog::warn("FTS5 not available, skipping content indexing");
            return {};
        }
        
        // Delete existing entry first (FTS5 doesn't support ON CONFLICT well)
        auto deleteResult = db.execute("DELETE FROM documents_fts WHERE rowid = " + 
                                      std::to_string(documentId));
        if (!deleteResult) return deleteResult.error();
        
        // Insert new FTS entry
        auto stmtResult = db.prepare(R"(
            INSERT INTO documents_fts (rowid, content, title, content_type)
            VALUES (?, ?, ?, ?)
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(documentId, content, title, contentType);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

Result<void> MetadataRepository::removeFromIndex(int64_t documentId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // First check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result) return fts5Result.error();
        
        if (!fts5Result.value()) {
            return {};
        }
        
        auto stmtResult = db.prepare("DELETE FROM documents_fts WHERE rowid = ?");
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult) return bindResult.error();
        
        return stmt.execute();
    });
}

// Helper function to sanitize FTS5 query strings
std::string sanitizeFTS5Query(const std::string& query) {
    // Trim whitespace from both ends
    std::string trimmed = query;
    trimmed.erase(0, trimmed.find_first_not_of(" \t\n\r"));
    trimmed.erase(trimmed.find_last_not_of(" \t\n\r") + 1);
    
    // If empty after trimming, return empty phrase
    if (trimmed.empty()) {
        return "\"\"";
    }
    
    // Check if the query might be using advanced FTS5 operators
    // (AND, OR, NOT, NEAR) - if so, leave it as-is for power users
    bool hasAdvancedOperators = false;
    if (trimmed.find(" AND ") != std::string::npos ||
        trimmed.find(" OR ") != std::string::npos ||
        trimmed.find(" NOT ") != std::string::npos ||
        trimmed.find("NEAR(") != std::string::npos) {
        hasAdvancedOperators = true;
    }
    
    // If using advanced operators, do minimal sanitization
    if (hasAdvancedOperators) {
        // Just remove trailing operators that would cause syntax errors
        while (!trimmed.empty()) {
            char lastChar = trimmed.back();
            if (lastChar == '-' || lastChar == '+' || lastChar == '*' || 
                lastChar == '(' || lastChar == ')') {
                trimmed.pop_back();
            } else {
                break;
            }
        }
        return trimmed.empty() ? "\"\"" : trimmed;
    }
    
    // For regular queries, wrap in quotes to treat as literal phrase
    // This handles all special characters safely
    std::string escaped;
    for (char c : trimmed) {
        if (c == '"') {
            // Escape quotes by doubling them (FTS5 syntax)
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    
    // Wrap in quotes to make it a phrase search
    // This prevents FTS5 from interpreting special characters as operators
    return "\"" + escaped + "\"";
}

Result<SearchResults> MetadataRepository::search(const std::string& query, int limit, int offset) {
    return executeQuery<SearchResults>([&](Database& db) -> Result<SearchResults> {
        SearchResults results;
        results.query = query;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // First check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result) {
            results.errorMessage = fts5Result.error().message;
            return results;
        }
        
        if (!fts5Result.value()) {
            results.errorMessage = "FTS5 not available";
            return results;
        }
        
        // Sanitize the query to prevent FTS5 syntax errors
        std::string sanitizedQuery = sanitizeFTS5Query(query);
        
        // Try FTS search first
        bool ftsSearchSucceeded = false;
        auto stmtResult = db.prepare(R"(
            SELECT 
                fts.rowid,
                fts.title,
                snippet(documents_fts, 0, '<b>', '</b>', '...', 32) as snippet,
                bm25(documents_fts) as score,
                d.file_path, d.file_name, d.file_extension, d.file_size,
                d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                d.indexed_time, d.content_extracted, d.extraction_status,
                d.extraction_error
            FROM documents_fts fts
            JOIN documents d ON d.id = fts.rowid
            WHERE documents_fts MATCH ?
            ORDER BY score
            LIMIT ? OFFSET ?
        )");
        
        if (stmtResult) {
            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bindAll(sanitizedQuery, limit, offset);
            if (bindResult) {
                // Try to execute the FTS5 search
                while (true) {
                    auto stepResult = stmt.step();
                    if (!stepResult) {
                        // FTS5 search failed - log and break to fall back to fuzzy search
                        spdlog::debug("FTS5 search execution failed: {}, will fall back to fuzzy search", 
                                     stepResult.error().message);
                        break;
                    }
                    if (!stepResult.value()) {
                        // Successfully completed FTS5 search
                        ftsSearchSucceeded = true;
                        break;
                    }
                    
                    SearchResult result;
                    
                    // Map document info
                    result.document.id = stmt.getInt64(0);
                    result.document.filePath = stmt.getString(4);
                    result.document.fileName = stmt.getString(5);
                    result.document.fileExtension = stmt.getString(6);
                    result.document.fileSize = stmt.getInt64(7);
                    result.document.sha256Hash = stmt.getString(8);
                    result.document.mimeType = stmt.getString(9);
                    result.document.setCreatedTime(stmt.getInt64(10));
                    result.document.setModifiedTime(stmt.getInt64(11));
                    result.document.setIndexedTime(stmt.getInt64(12));
                    result.document.contentExtracted = stmt.getInt(13) != 0;
                    result.document.extractionStatus = ExtractionStatusUtils::fromString(stmt.getString(14));
                    result.document.extractionError = stmt.getString(15);
                    
                    // Search-specific fields
                    result.snippet = stmt.getString(2);
                    result.score = stmt.getDouble(3);
                    
                    results.results.push_back(result);
                    ftsSearchSucceeded = true;
                }
                
                // Get total count for FTS5 results
                if (ftsSearchSucceeded) {
                    auto countStmtResult = db.prepare(R"(
                        SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH ?
                    )");
                    
                    if (countStmtResult) {
                        Statement countStmt = std::move(countStmtResult).value();
                        auto bindRes = countStmt.bind(1, sanitizedQuery);
                        if (bindRes.has_value()) {
                            auto stepRes = countStmt.step();
                            if (stepRes.has_value() && stepRes.value()) {
                                results.totalCount = countStmt.getInt64(0);
                            }
                        }
                    }
                }
            } else {
                spdlog::debug("FTS5 search bind failed: {}", bindResult.error().message);
            }
        } else {
            spdlog::debug("FTS5 search prepare failed: {}", stmtResult.error().message);
        }
        
        // If FTS5 search failed, fall back to fuzzy search
        if (!ftsSearchSucceeded) {
            spdlog::info("FTS5 search failed for query '{}', falling back to fuzzy search", query);
            
            // Use fuzzy search as fallback (note: fuzzy search doesn't support offset)
            auto fuzzyResults = fuzzySearch(query, 0.3, limit);
            if (fuzzyResults) {
                results = fuzzyResults.value();
                // Add a note in the error message that we fell back to fuzzy search
                // but don't treat it as a failure since we have results
                spdlog::debug("Successfully fell back to fuzzy search for query '{}'", query);
            } else {
                results.errorMessage = "Both FTS5 and fuzzy search failed: " + fuzzyResults.error().message;
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        results.executionTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            end - start).count();
        
        return results;
    });
}

// Bulk operations
Result<std::vector<DocumentInfo>> MetadataRepository::findDocumentsByPath(
    const std::string& pathPattern) {
    return executeQuery<std::vector<DocumentInfo>>([&](Database& db) -> Result<std::vector<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, file_path, file_name, file_extension, file_size,
                   sha256_hash, mime_type, created_time, modified_time,
                   indexed_time, content_extracted, extraction_status,
                   extraction_error
            FROM documents WHERE file_path LIKE ?
            ORDER BY file_path
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, pathPattern);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentInfo> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result.push_back(mapDocumentRow(stmt));
        }
        
        return result;
    });
}

Result<std::vector<DocumentInfo>> MetadataRepository::findDocumentsByExtension(
    const std::string& extension) {
    return executeQuery<std::vector<DocumentInfo>>([&](Database& db) -> Result<std::vector<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT id, file_path, file_name, file_extension, file_size,
                   sha256_hash, mime_type, created_time, modified_time,
                   indexed_time, content_extracted, extraction_status,
                   extraction_error
            FROM documents WHERE file_extension = ?
            ORDER BY file_name
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, extension);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentInfo> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result.push_back(mapDocumentRow(stmt));
        }
        
        return result;
    });
}

Result<std::vector<DocumentInfo>> MetadataRepository::findDocumentsModifiedSince(
    std::chrono::system_clock::time_point since) {
    return executeQuery<std::vector<DocumentInfo>>([&](Database& db) -> Result<std::vector<DocumentInfo>> {
        auto sinceUnix = std::chrono::duration_cast<std::chrono::seconds>(
            since.time_since_epoch()).count();
        
        auto stmtResult = db.prepare(R"(
            SELECT id, file_path, file_name, file_extension, file_size,
                   sha256_hash, mime_type, created_time, modified_time,
                   indexed_time, content_extracted, extraction_status,
                   extraction_error
            FROM documents WHERE modified_time >= ?
            ORDER BY modified_time DESC
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, sinceUnix);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentInfo> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result.push_back(mapDocumentRow(stmt));
        }
        
        return result;
    });
}

// Statistics
Result<int64_t> MetadataRepository::getDocumentCount() {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare("SELECT COUNT(*) FROM documents");
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        return stmt.getInt64(0);
    });
}

Result<int64_t> MetadataRepository::getIndexedDocumentCount() {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            SELECT COUNT(*) FROM documents WHERE content_extracted = 1
        )");
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        
        return stmt.getInt64(0);
    });
}

Result<std::unordered_map<std::string, int64_t>> MetadataRepository::getDocumentCountsByExtension() {
    return executeQuery<std::unordered_map<std::string, int64_t>>([&](Database& db) 
        -> Result<std::unordered_map<std::string, int64_t>> {
        
        auto stmtResult = db.prepare(R"(
            SELECT file_extension, COUNT(*) as count
            FROM documents
            GROUP BY file_extension
            ORDER BY count DESC
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        std::unordered_map<std::string, int64_t> result;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            result[stmt.getString(0)] = stmt.getInt64(1);
        }
        
        return result;
    });
}

// Helper methods for row mapping
DocumentInfo MetadataRepository::mapDocumentRow(Statement& stmt) {
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
    return info;
}

DocumentContent MetadataRepository::mapContentRow(Statement& stmt) {
    DocumentContent content;
    content.documentId = stmt.getInt64(0);
    content.contentText = stmt.getString(1);
    content.contentLength = stmt.getInt64(2);
    content.extractionMethod = stmt.getString(3);
    content.language = stmt.getString(4);
    return content;
}

DocumentRelationship MetadataRepository::mapRelationshipRow(Statement& stmt) {
    DocumentRelationship rel;
    rel.id = stmt.getInt64(0);
    if (!stmt.isNull(1)) {
        rel.parentId = stmt.getInt64(1);
    }
    rel.childId = stmt.getInt64(2);
    rel.setRelationshipTypeFromString(stmt.getString(3));
    rel.setCreatedTime(stmt.getInt64(4));
    return rel;
}

SearchHistoryEntry MetadataRepository::mapSearchHistoryRow(Statement& stmt) {
    SearchHistoryEntry entry;
    entry.id = stmt.getInt64(0);
    entry.query = stmt.getString(1);
    entry.setQueryTime(stmt.getInt64(2));
    entry.resultsCount = stmt.getInt64(3);
    entry.executionTimeMs = stmt.getInt64(4);
    entry.userContext = stmt.getString(5);
    return entry;
}

SavedQuery MetadataRepository::mapSavedQueryRow(Statement& stmt) {
    SavedQuery query;
    query.id = stmt.getInt64(0);
    query.name = stmt.getString(1);
    query.query = stmt.getString(2);
    query.description = stmt.getString(3);
    query.setCreatedTime(stmt.getInt64(4));
    if (!stmt.isNull(5)) {
        query.setLastUsed(stmt.getInt64(5));
    }
    query.useCount = stmt.getInt64(6);
    return query;
}

// MetadataQueryBuilder implementation
MetadataQueryBuilder& MetadataQueryBuilder::withExtension(const std::string& extension) {
    conditions_.push_back("file_extension = ?");
    parameters_.push_back(extension);
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withMimeType(const std::string& mimeType) {
    conditions_.push_back("mime_type = ?");
    parameters_.push_back(mimeType);
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withPathContaining(const std::string& pathFragment) {
    conditions_.push_back("file_path LIKE ?");
    parameters_.push_back("%" + pathFragment + "%");
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::modifiedAfter(
    std::chrono::system_clock::time_point time) {
    conditions_.push_back("modified_time >= ?");
    auto unixTime = std::chrono::duration_cast<std::chrono::seconds>(
        time.time_since_epoch()).count();
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::modifiedBefore(
    std::chrono::system_clock::time_point time) {
    conditions_.push_back("modified_time <= ?");
    auto unixTime = std::chrono::duration_cast<std::chrono::seconds>(
        time.time_since_epoch()).count();
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::indexedAfter(
    std::chrono::system_clock::time_point time) {
    conditions_.push_back("indexed_time >= ?");
    auto unixTime = std::chrono::duration_cast<std::chrono::seconds>(
        time.time_since_epoch()).count();
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withContentExtracted(bool extracted) {
    conditions_.push_back("content_extracted = ?");
    parameters_.push_back(extracted ? "1" : "0");
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withExtractionStatus(ExtractionStatus status) {
    conditions_.push_back("extraction_status = ?");
    parameters_.push_back(ExtractionStatusUtils::toString(status));
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withMetadata(const std::string& key,
                                                        const std::string& value) {
    conditions_.push_back(R"(
        EXISTS (
            SELECT 1 FROM metadata m 
            WHERE m.document_id = documents.id 
            AND m.key = ? AND m.value = ?
        )
    )");
    parameters_.push_back(key);
    parameters_.push_back(value);
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withMetadataKey(const std::string& key) {
    conditions_.push_back(R"(
        EXISTS (
            SELECT 1 FROM metadata m 
            WHERE m.document_id = documents.id 
            AND m.key = ?
        )
    )");
    parameters_.push_back(key);
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withContentLanguage(const std::string& language) {
    conditions_.push_back(R"(
        EXISTS (
            SELECT 1 FROM document_content dc 
            WHERE dc.document_id = documents.id 
            AND dc.language = ?
        )
    )");
    parameters_.push_back(language);
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withMinContentLength(int64_t minLength) {
    conditions_.push_back(R"(
        EXISTS (
            SELECT 1 FROM document_content dc 
            WHERE dc.document_id = documents.id 
            AND dc.content_length >= ?
        )
    )");
    parameters_.push_back(std::to_string(minLength));
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::withMaxContentLength(int64_t maxLength) {
    conditions_.push_back(R"(
        EXISTS (
            SELECT 1 FROM document_content dc 
            WHERE dc.document_id = documents.id 
            AND dc.content_length <= ?
        )
    )");
    parameters_.push_back(std::to_string(maxLength));
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::orderByModified(bool ascending) {
    orderBy_ = "ORDER BY modified_time " + std::string(ascending ? "ASC" : "DESC");
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::orderByIndexed(bool ascending) {
    orderBy_ = "ORDER BY indexed_time " + std::string(ascending ? "ASC" : "DESC");
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::orderBySize(bool ascending) {
    orderBy_ = "ORDER BY file_size " + std::string(ascending ? "ASC" : "DESC");
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::limit(int count) {
    limit_ = count;
    return *this;
}

MetadataQueryBuilder& MetadataQueryBuilder::offset(int count) {
    offset_ = count;
    return *this;
}

std::string MetadataQueryBuilder::buildQuery() const {
    std::string query = R"(
        SELECT id, file_path, file_name, file_extension, file_size,
               sha256_hash, mime_type, created_time, modified_time,
               indexed_time, content_extracted, extraction_status,
               extraction_error
        FROM documents
    )";
    
    if (!conditions_.empty()) {
        query += " WHERE ";
        for (size_t i = 0; i < conditions_.size(); ++i) {
            if (i > 0) query += " AND ";
            query += conditions_[i];
        }
    }
    
    if (!orderBy_.empty()) {
        query += " " + orderBy_;
    }
    
    if (limit_ > 0) {
        query += " LIMIT " + std::to_string(limit_);
    }
    
    if (offset_ > 0) {
        query += " OFFSET " + std::to_string(offset_);
    }
    
    return query;
}

std::vector<std::string> MetadataQueryBuilder::getParameters() const {
    return parameters_;
}

// MetadataTransaction implementation
MetadataTransaction::MetadataTransaction(MetadataRepository& repo) 
    : repo_(repo) {
}

MetadataTransaction::~MetadataTransaction() = default;

// Fuzzy search implementation
Result<SearchResults> MetadataRepository::fuzzySearch(const std::string& query, 
                                                     float minSimilarity,
                                                     int limit) {
    return executeQuery<SearchResults>([&]([[maybe_unused]] Database& db) -> Result<SearchResults> {
        SearchResults results;
        results.query = query;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Ensure fuzzy index is built
        {
            std::lock_guard<std::mutex> lock(fuzzyIndexMutex_);
            if (!fuzzySearchIndex_) {
                auto buildResult = buildFuzzyIndex();
                if (!buildResult) {
                    results.errorMessage = "Failed to build fuzzy index: " + buildResult.error().message;
                    return results;
                }
            }
        }
        
        // Perform fuzzy search
        search::HybridFuzzySearch::SearchOptions options;
        options.minSimilarity = minSimilarity;
        options.maxEditDistance = 3;
        options.useTrigramPrefilter = true;
        options.useBKTree = true;
        
        auto fuzzyResults = fuzzySearchIndex_->search(query, static_cast<size_t>(limit), options);
        
        // Debug output (disabled)
        // std::cerr << "[DEBUG] Fuzzy search returned " << fuzzyResults.size() << " results" << std::endl;
        
        // Convert fuzzy results to SearchResults
        for (const auto& fuzzyResult : fuzzyResults) {
            // Parse document ID from fuzzy result ID
            int64_t docId = 0;
            try {
                docId = std::stoll(fuzzyResult.id);
            } catch (...) {
                continue; // Skip invalid IDs
            }
            
            // Fetch document info
            auto docResult = getDocument(docId);
            if (!docResult || !docResult.value().has_value()) {
                continue;
            }
            
            SearchResult result;
            result.document = docResult.value().value();
            result.snippet = "Match type: " + fuzzyResult.matchType + 
                           " (similarity: " + std::to_string(fuzzyResult.score) + ")";
            result.score = static_cast<double>(fuzzyResult.score);
            
            results.results.push_back(result);
        }
        
        results.totalCount = static_cast<int64_t>(results.results.size());
        
        auto end = std::chrono::high_resolution_clock::now();
        results.executionTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            end - start).count();
        
        return results;
    });
}

Result<void> MetadataRepository::buildFuzzyIndex() {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // NOTE: Mutex is already locked by the caller - don't lock again!
        
        // Create new fuzzy search index
        fuzzySearchIndex_ = std::make_unique<search::HybridFuzzySearch>();
        
        // Query all documents
        auto stmtResult = db.prepare(R"(
            SELECT id, file_name, file_path
            FROM documents
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            int64_t id = stmt.getInt64(0);
            std::string fileName = stmt.getString(1);
            std::string filePath = stmt.getString(2);
            
            // Extract keywords from file path
            std::vector<std::string> keywords;
            
            // Split path into components as keywords
            size_t pos = 0;
            std::string path = filePath;
            while ((pos = path.find('/')) != std::string::npos) {
                std::string component = path.substr(0, pos);
                if (!component.empty() && component != "." && component != "..") {
                    keywords.push_back(component);
                }
                path.erase(0, pos + 1);
            }
            if (!path.empty()) {
                keywords.push_back(path);
            }
            
            // Add document to fuzzy index with both filename and content
            // First add with filename as title
            fuzzySearchIndex_->addDocument(std::to_string(id), fileName, keywords);
            // std::cerr << "[DEBUG] Added doc " << id << " with title '" << fileName << "' and " << keywords.size() << " keywords" << std::endl;
            
            // Also get and index document content for fuzzy search
            auto contentResult = getContent(id);
            if (contentResult && contentResult.value().has_value()) {
                auto content = contentResult.value().value();
                
                // Extract more comprehensive keywords from content
                std::vector<std::string> contentKeywords;
                
                // Process larger portion of content (up to 5000 chars instead of 500)
                size_t maxContentLength = std::min(size_t(5000), content.contentText.length());
                std::string contentToIndex = content.contentText.substr(0, maxContentLength);
                
                // Convert to lowercase for better matching
                std::transform(contentToIndex.begin(), contentToIndex.end(), contentToIndex.begin(), ::tolower);
                
                // Extract words and multi-word phrases
                std::istringstream iss(contentToIndex);
                std::string word;
                std::string previousWord;
                
                while (iss >> word) {
                    // Clean up word - remove common punctuation
                    word.erase(std::remove_if(word.begin(), word.end(), 
                        [](char c) { return !std::isalnum(c) && c != '-' && c != '_'; }), word.end());
                    
                    if (!word.empty() && word.length() > 2) {  // Skip very short words
                        contentKeywords.push_back(word);
                        
                        // Also add two-word phrases for better phrase matching
                        if (!previousWord.empty()) {
                            std::string phrase = previousWord + " " + word;
                            contentKeywords.push_back(phrase);
                        }
                        
                        previousWord = word;
                    }
                    
                    // Limit total keywords to prevent memory issues
                    if (contentKeywords.size() >= 100) {
                        break;
                    }
                }
                
                // Add content-based entry with more content preview
                std::string contentPreview = content.contentText.substr(0, std::min(size_t(200), content.contentText.length()));
                fuzzySearchIndex_->addDocument(std::to_string(id) + "_content", 
                                              contentPreview, 
                                              contentKeywords);
            }
        }
        
        // Also query metadata tags
        auto tagStmtResult = db.prepare(R"(
            SELECT document_id, value
            FROM metadata
            WHERE key = 'tag'
        )");
        
        if (tagStmtResult) {
            Statement tagStmt = std::move(tagStmtResult).value();
            
            std::unordered_map<int64_t, std::vector<std::string>> docTags;
            
            while (true) {
                auto stepResult = tagStmt.step();
                if (!stepResult || !stepResult.value()) break;
                
                int64_t docId = tagStmt.getInt64(0);
                std::string tag = tagStmt.getString(1);
                docTags[docId].push_back(tag);
            }
            
            // Update documents with tags
            for (const auto& [docId, tags] : docTags) {
                auto docResult = getDocument(docId);
                if (docResult && docResult.value().has_value()) {
                    auto doc = docResult.value().value();
                    fuzzySearchIndex_->addDocument(std::to_string(docId), 
                                                  doc.fileName, tags);
                }
            }
        }
        
        spdlog::info("Built fuzzy index with {} documents", 
                    fuzzySearchIndex_->getStats().documentCount);
        
        return Result<void>();
    });
}

Result<void> MetadataRepository::updateFuzzyIndex(int64_t documentId) {
    return executeQuery<void>([&]([[maybe_unused]] Database& db) -> Result<void> {
        std::lock_guard<std::mutex> lock(fuzzyIndexMutex_);
        
        if (!fuzzySearchIndex_) {
            // Index not built yet, will be built on first search
            return Result<void>();
        }
        
        // Get document info
        auto docResult = getDocument(documentId);
        if (!docResult || !docResult.value().has_value()) {
            return Error{ErrorCode::NotFound, "Document not found"};
        }
        
        auto doc = docResult.value().value();
        
        // Get document tags
        std::vector<std::string> keywords;
        auto metadataResult = getAllMetadata(documentId);
        if (metadataResult) {
            for (const auto& [key, value] : metadataResult.value()) {
                if (key == "tag" && value.type == MetadataValueType::String) {
                    keywords.push_back(value.value);
                }
            }
        }
        
        // Update fuzzy index
        fuzzySearchIndex_->addDocument(std::to_string(documentId), 
                                      doc.fileName, keywords);
        
        return Result<void>();
    });
}

// Collection and snapshot operations
Result<std::vector<DocumentInfo>> MetadataRepository::findDocumentsByCollection(
    const std::string& collection) {
    return executeQuery<std::vector<DocumentInfo>>([&](Database& db) -> Result<std::vector<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'collection' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, collection);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentInfo> documents;
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            documents.push_back(mapDocumentRow(stmt));
        }
        
        return documents;
    });
}

Result<std::vector<DocumentInfo>> MetadataRepository::findDocumentsBySnapshot(
    const std::string& snapshotId) {
    return executeQuery<std::vector<DocumentInfo>>([&](Database& db) -> Result<std::vector<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'snapshot_id' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, snapshotId);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentInfo> documents;
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            documents.push_back(mapDocumentRow(stmt));
        }
        
        return documents;
    });
}

Result<std::vector<DocumentInfo>> MetadataRepository::findDocumentsBySnapshotLabel(
    const std::string& snapshotLabel) {
    return executeQuery<std::vector<DocumentInfo>>([&](Database& db) -> Result<std::vector<DocumentInfo>> {
        auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'snapshot_label' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, snapshotLabel);
        if (!bindResult) return bindResult.error();
        
        std::vector<DocumentInfo> documents;
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            documents.push_back(mapDocumentRow(stmt));
        }
        
        return documents;
    });
}

Result<std::vector<std::string>> MetadataRepository::getCollections() {
    return executeQuery<std::vector<std::string>>([&](Database& db) -> Result<std::vector<std::string>> {
        auto stmtResult = db.prepare(R"(
            SELECT DISTINCT value
            FROM metadata
            WHERE key = 'collection'
            ORDER BY value
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        std::vector<std::string> collections;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            collections.push_back(stmt.getString(0));
        }
        
        return collections;
    });
}

Result<std::vector<std::string>> MetadataRepository::getSnapshots() {
    return executeQuery<std::vector<std::string>>([&](Database& db) -> Result<std::vector<std::string>> {
        auto stmtResult = db.prepare(R"(
            SELECT DISTINCT value
            FROM metadata
            WHERE key = 'snapshot_id'
            ORDER BY value
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        std::vector<std::string> snapshots;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            snapshots.push_back(stmt.getString(0));
        }
        
        return snapshots;
    });
}

Result<std::vector<std::string>> MetadataRepository::getSnapshotLabels() {
    return executeQuery<std::vector<std::string>>([&](Database& db) -> Result<std::vector<std::string>> {
        auto stmtResult = db.prepare(R"(
            SELECT DISTINCT value
            FROM metadata
            WHERE key = 'snapshot_label'
            ORDER BY value
        )");
        
        if (!stmtResult) return stmtResult.error();
        
        Statement stmt = std::move(stmtResult).value();
        std::vector<std::string> labels;
        
        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();
            if (!stepResult.value()) break;
            
            labels.push_back(stmt.getString(0));
        }
        
        return labels;
    });
}

} // namespace yams::metadata