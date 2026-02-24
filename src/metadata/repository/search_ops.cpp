#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <sstream>

#include <yams/common/utf8_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>
#include <yams/search/symspell_search.h>

#include "document_query_filters.hpp"
#include "search_query_helpers.hpp"

namespace yams::metadata {

Result<SearchResults>
MetadataRepository::search(const std::string& query, int limit, int offset,
                           const std::optional<std::vector<int64_t>>& docIds) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::search");
    return executeReadQuery<SearchResults>([&](Database& db) -> Result<SearchResults> {
        YAMS_ZONE_SCOPED_N("MetadataRepo::search::FTS5Query");
        SearchResults results;
        results.query = query;

        constexpr int kMaxSearchLimit = 10000;
        const int effectiveLimit = std::min(limit > 0 ? limit : 100, kMaxSearchLimit);
        if (limit > kMaxSearchLimit) {
            spdlog::debug("Search limit {} exceeds max {}, capping", limit, kMaxSearchLimit);
        }

        const bool cacheable = !docIds.has_value() || docIds->empty();
        std::string cacheKey;
        if (cacheable) {
            cacheKey.reserve(query.size() + 32);
            cacheKey.append(query);
            cacheKey.push_back(':');
            cacheKey.append(std::to_string(limit));
            cacheKey.push_back(':');
            cacheKey.append(std::to_string(offset));

            auto snapshot =
                std::atomic_load_explicit(&queryCacheSnapshot_, std::memory_order_acquire);
            if (snapshot) {
                auto it = snapshot->find(cacheKey);
                if (it != snapshot->end()) {
                    auto age = std::chrono::steady_clock::now() - it->second.timestamp;
                    if (age <= kQueryCacheTtl) {
                        return it->second.results;
                    }
                }
            }
        }

        auto start = std::chrono::high_resolution_clock::now();

        std::string sanitizedQuery = sanitizeFTS5Query(query);
        const auto ftsMode = parseFts5ModeEnv();
        auto parseEnvFlag = [](const char* key, bool defaultValue) {
            const char* env = std::getenv(key);
            if (!env || !*env) {
                return defaultValue;
            }

            std::string value(env);
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return !(value == "0" || value == "false" || value == "off" || value == "no");
        };

        const bool nlAutoPhrase = parseEnvFlag("YAMS_FTS_NL_AUTO_PHRASE", true);
        const bool nlAutoPrefix = parseEnvFlag("YAMS_FTS_NL_PREFIX", true);
        int nlMinResults = 3;
        if (const char* s = std::getenv("YAMS_FTS_NL_MIN_RESULTS"); s && *s) {
            nlMinResults = std::max(0, std::atoi(s));
        }
        const bool useNlFallback = parseEnvFlag("YAMS_FTS_NL_OR_FALLBACK", false);
        const bool isAdvancedQuery = hasAdvancedFts5Operators(query);
        bool usedNaturalLanguageQuery = false;
        if (!isAdvancedQuery) {
            if (ftsMode == Fts5QueryMode::Natural ||
                (ftsMode == Fts5QueryMode::Smart && isLikelyNaturalLanguageQuery(query))) {
                sanitizedQuery =
                    buildNaturalLanguageFts5Query(query, false, nlAutoPrefix, nlAutoPhrase);
                usedNaturalLanguageQuery = true;
            }
        }

        bool ftsDebug = false;
        if (const char* env = std::getenv("YAMS_FTS_DEBUG_QUERY"); env && std::string(env) == "1") {
            ftsDebug = true;
            spdlog::warn("[FTS5] raw='{}' sanitized='{}' limit={} offset={} docIds={}", query,
                         sanitizedQuery, limit, offset, (docIds ? docIds->size() : 0));
        }

        std::optional<size_t> diagAltHitCount;
        std::string diagAltQuery;
        if (ftsDebug && !hasAdvancedFts5Operators(query)) {
            std::vector<std::string> diagTokens = splitFTS5Terms(query);
            diagAltQuery = buildDiagnosticAltOrQuery(diagTokens);
            if (!diagAltQuery.empty()) {
                auto diagStmtResult =
                    db.prepare("SELECT 1 FROM documents_fts WHERE documents_fts MATCH ? LIMIT ?");
                if (diagStmtResult) {
                    Statement diagStmt = std::move(diagStmtResult).value();
                    auto bindRes1 = diagStmt.bind(1, diagAltQuery);
                    auto bindRes2 = diagStmt.bind(2, effectiveLimit);
                    if (bindRes1 && bindRes2) {
                        size_t rowCount = 0;
                        while (true) {
                            auto stepRes = diagStmt.step();
                            if (!stepRes) {
                                spdlog::debug("FTS5 diag alt query step failed: {}",
                                              stepRes.error().message);
                                break;
                            }
                            if (!stepRes.value())
                                break;
                            ++rowCount;
                        }
                        diagAltHitCount = rowCount;
                    } else {
                        spdlog::debug("FTS5 diag alt query bind failed");
                    }
                } else {
                    spdlog::debug("FTS5 diag alt query prepare failed: {}",
                                  diagStmtResult.error().message);
                }
            }
        }

        if (ftsDebug && diagAltHitCount.has_value()) {
            spdlog::warn("[FTS5 diag_alt_exec] raw='{}' alt_or='{}' text_hits={}", query,
                         diagAltQuery, *diagAltHitCount);
        }

        auto runFtsQuery = [&](const std::string& queryText, SearchResults& out) -> Result<bool> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "documents_fts";
            spec.from = std::optional<std::string>{"documents_fts fts JOIN documents d ON d.id = "
                                                   "fts.rowid"};
            const bool includeSnippet = includeSearchSnippets();
            spec.columns = {"d.id",
                            "bm25(documents_fts, 1.0, 10.0) as score",
                            "d.file_path",
                            "d.file_name",
                            "d.file_extension",
                            "d.file_size",
                            "d.sha256_hash",
                            "d.mime_type",
                            "d.created_time",
                            "d.modified_time",
                            "d.indexed_time",
                            "d.content_extracted",
                            "d.extraction_status",
                            "d.extraction_error"};
            if (includeSnippet) {
                spec.columns.insert(spec.columns.begin() + 1,
                                    "snippet(documents_fts, 0, '<b>', '</b>', '...', 16) as "
                                    "snippet");
            }
            spec.conditions.emplace_back("documents_fts MATCH ?");
            std::string idIn;
            if (docIds && !docIds->empty()) {
                idIn = "d.id IN (";
                for (size_t i = 0; i < docIds->size(); ++i) {
                    if (i > 0)
                        idIn += ',';
                    idIn += '?';
                }
                idIn += ')';
                spec.conditions.push_back(idIn);
            }
            spec.orderBy = std::optional<std::string>{"score"};
            spec.limit = effectiveLimit;
            spec.offset = offset;
            auto sql = yams::metadata::sql::buildSelect(spec);

            auto stmtResult = db.prepare(sql);
            if (!stmtResult) {
                spdlog::debug("FTS5 search prepare failed: {}", stmtResult.error().message);
                return false;
            }

            Statement stmt = std::move(stmtResult).value();
            int bindIndex = 1;
            auto b1 = stmt.bind(bindIndex++, queryText);
            if (!b1)
                return b1.error();
            if (docIds && !docIds->empty()) {
                for (auto id : *docIds) {
                    auto b = stmt.bind(bindIndex++, static_cast<int64_t>(id));
                    if (!b)
                        return b.error();
                }
            }

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    spdlog::error("FTS5 search step() failed: {}", stepResult.error().message);
                    break;
                }
                if (!stepResult.value()) {
                    break;
                }

                SearchResult result;

                const bool includeSnippet = includeSearchSnippets();
                const int scoreIndex = includeSnippet ? 2 : 1;
                const int docStartIndex = includeSnippet ? 3 : 2;

                result.document.id = stmt.getInt64(0);
                result.document.filePath = stmt.getString(docStartIndex + 0);
                result.document.fileName = stmt.getString(docStartIndex + 1);
                result.document.fileExtension = stmt.getString(docStartIndex + 2);
                result.document.fileSize = stmt.getInt64(docStartIndex + 3);
                result.document.sha256Hash = stmt.getString(docStartIndex + 4);
                result.document.mimeType = stmt.getString(docStartIndex + 5);
                result.document.createdTime = stmt.getTime(docStartIndex + 6);
                result.document.modifiedTime = stmt.getTime(docStartIndex + 7);
                result.document.indexedTime = stmt.getTime(docStartIndex + 8);
                result.document.contentExtracted = stmt.getInt(docStartIndex + 9) != 0;
                result.document.extractionStatus =
                    ExtractionStatusUtils::fromString(stmt.getString(docStartIndex + 10));
                result.document.extractionError = stmt.getString(docStartIndex + 11);

                result.snippet =
                    includeSnippet ? common::sanitizeUtf8(stmt.getString(1)) : std::string{};
                result.score = stmt.getDouble(scoreIndex);

                out.results.push_back(result);
            }

            if (!out.results.empty()) {
                const size_t resultSize = out.results.size();
                const size_t requestedLimit = static_cast<size_t>(effectiveLimit);

                if (resultSize < requestedLimit) {
                    out.totalCount = resultSize;
                } else {
                    constexpr int64_t kMaxCountLimit = 10000;
                    std::string countSql = R"(
                            SELECT COUNT(*) FROM (
                                SELECT 1
                                FROM documents_fts fts
                                JOIN documents d ON d.id = fts.rowid
                                WHERE documents_fts MATCH ?
                        )";
                    if (docIds && !docIds->empty()) {
                        countSql += " AND d.id IN (";
                        for (size_t i = 0; i < docIds->size(); ++i) {
                            if (i > 0) {
                                countSql += ',';
                            }
                            countSql += '?';
                        }
                        countSql += ')';
                    }
                    countSql += " LIMIT ? )";

                    auto countStmtResult = db.prepare(countSql);

                    if (countStmtResult) {
                        Statement countStmt = std::move(countStmtResult).value();
                        int countBindIndex = 1;
                        auto bindRes1 = countStmt.bind(countBindIndex++, queryText);
                        bool bindOk = bindRes1.has_value();
                        if (bindOk && docIds && !docIds->empty()) {
                            for (auto id : *docIds) {
                                auto bindDoc =
                                    countStmt.bind(countBindIndex++, static_cast<int64_t>(id));
                                if (!bindDoc.has_value()) {
                                    bindOk = false;
                                    break;
                                }
                            }
                        }
                        auto bindRes2 = countStmt.bind(countBindIndex++, kMaxCountLimit);
                        if (bindOk && bindRes2.has_value()) {
                            auto stepRes = countStmt.step();
                            if (stepRes.has_value() && stepRes.value()) {
                                int64_t boundedCount = countStmt.getInt64(0);
                                out.totalCount = boundedCount;
                                if (boundedCount >= kMaxCountLimit) {
                                    spdlog::debug(
                                        "Search matched >{} results, using approximate count",
                                        kMaxCountLimit);
                                }
                            }
                        }
                    } else {
                        out.totalCount = resultSize;
                        spdlog::debug("Count query failed, using result size as count");
                    }
                }
            }

            return !out.results.empty();
        };

        bool ftsSearchSucceeded = false;
        auto ftsRun = runFtsQuery(sanitizedQuery, results);
        if (!ftsRun) {
            return ftsRun.error();
        }
        ftsSearchSucceeded = ftsRun.value();

        if (usedNaturalLanguageQuery && useNlFallback &&
            static_cast<int>(results.results.size()) < nlMinResults) {
            SearchResults orResults;
            orResults.query = query;
            std::string orQuery =
                buildNaturalLanguageFts5Query(query, true, nlAutoPrefix, nlAutoPhrase);
            auto orRun = runFtsQuery(orQuery, orResults);
            if (!orRun) {
                return orRun.error();
            }
            if (orRun.value()) {
                results = std::move(orResults);
                sanitizedQuery = std::move(orQuery);
                ftsSearchSucceeded = true;
            }
        }

        spdlog::info("FTS5 search for '{}': succeeded={} results={}", query.substr(0, 50),
                     ftsSearchSucceeded, results.results.size());

        if (!ftsSearchSucceeded) {
            spdlog::debug("FTS5 search failed for query '{}', falling back to fuzzy search", query);
            auto fuzzyResults = fuzzySearch(query, 0.3, limit, docIds);
            if (fuzzyResults) {
                results = fuzzyResults.value();
                spdlog::debug("Successfully fell back to fuzzy search for query '{}'", query);
            } else {
                results.errorMessage =
                    "Both FTS5 and fuzzy search failed: " + fuzzyResults.error().message;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        results.executionTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        if (cacheable) {
            updateQueryCache(cacheKey, results);
        }

        return results;
    });
}

Result<SearchResults>
MetadataRepository::fuzzySearch(const std::string& query, float minSimilarity, int limit,
                                const std::optional<std::vector<int64_t>>& docIds) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::fuzzySearch");
    (void)minSimilarity;

    return executeReadQuery<SearchResults>([&](Database& db) -> Result<SearchResults> {
        SearchResults results;
        results.query = query;

        spdlog::debug("[FUZZY] fuzzySearch starting for query='{}' limit={}", query, limit);
        auto totalStart = std::chrono::high_resolution_clock::now();

        auto initResult = ensureSymSpellInitialized();
        if (!initResult || !symspellIndex_) {
            results.errorMessage = "SymSpell index not available";
            return results;
        }

        std::string ftsQuery;
        const bool advancedQuery = hasAdvancedFts5Operators(query);
        if (!advancedQuery) {
            std::vector<std::string> expandedTerms;
            std::istringstream iss(query);
            std::string term;
            while (iss >> term) {
                std::transform(term.begin(), term.end(), term.begin(), ::tolower);
                term = stripPunctuation(std::move(term));
                if (term.empty())
                    continue;
                if (term.length() < 2)
                    continue;

                search::SymSpellSearch::SearchOptions opts;
                opts.maxEditDistance = 2;
                opts.returnAll = true;
                opts.maxResults = 5;

                auto suggestions = symspellIndex_->search(term, opts);
                if (!suggestions.empty()) {
                    for (const auto& s : suggestions) {
                        std::string cleaned = stripPunctuation(std::string(s.term));
                        if (!cleaned.empty()) {
                            expandedTerms.push_back(std::move(cleaned));
                        }
                    }
                } else {
                    expandedTerms.push_back(term);
                }
            }

            auto symspellEnd = std::chrono::high_resolution_clock::now();
            auto symspellMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(symspellEnd - totalStart)
                    .count();
            spdlog::debug("[FUZZY] SymSpell term expansion took {}ms, expanded to {} terms",
                          symspellMs, expandedTerms.size());

            if (expandedTerms.empty()) {
                results.totalCount = 0;
                results.executionTimeMs = symspellMs;
                return results;
            }

            for (size_t i = 0; i < expandedTerms.size(); ++i) {
                if (i > 0)
                    ftsQuery += " OR ";
                ftsQuery += renderFts5Token(expandedTerms[i], false);
            }
        } else {
            ftsQuery = sanitizeFts5UserQuery(query);
        }

        spdlog::debug("[FUZZY] FTS5 query: {}", ftsQuery);

        constexpr int kMaxSearchLimit = 10000;
        const int effectiveLimit = std::min(limit > 0 ? limit : 100, kMaxSearchLimit);

        const bool includeSnippet = includeSearchSnippets();
        std::string sql = R"(
                 SELECT d.id,
                     bm25(documents_fts, 1.0, 10.0) as score,
                   d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status, d.extraction_error
            FROM documents_fts fts
            JOIN documents d ON d.id = fts.rowid
            WHERE documents_fts MATCH ?
        )";
        if (includeSnippet) {
            sql = R"(
                 SELECT d.id,
                     snippet(documents_fts, 0, '<b>', '</b>', '...', 16) as snippet,
                     bm25(documents_fts, 1.0, 10.0) as score,
                   d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status, d.extraction_error
            FROM documents_fts fts
            JOIN documents d ON d.id = fts.rowid
            WHERE documents_fts MATCH ?
        )";
        }

        if (docIds && !docIds->empty()) {
            sql += " AND d.id IN (";
            for (size_t i = 0; i < docIds->size(); ++i) {
                if (i > 0)
                    sql += ',';
                sql += '?';
            }
            sql += ')';
        }
        sql += " ORDER BY score LIMIT ?";

        auto stmtResult = db.prepare(sql);
        if (!stmtResult) {
            spdlog::warn("[FUZZY] FTS5 prepare failed: {}", stmtResult.error().message);
            results.errorMessage = "FTS5 query failed: " + stmtResult.error().message;
            auto end = std::chrono::high_resolution_clock::now();
            results.executionTimeMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - totalStart).count();
            return results;
        }

        Statement stmt = std::move(stmtResult).value();
        int bindIndex = 1;

        auto b1 = stmt.bind(bindIndex++, ftsQuery);
        if (!b1) {
            results.errorMessage = "Bind failed: " + b1.error().message;
            return results;
        }

        if (docIds && !docIds->empty()) {
            for (auto id : *docIds) {
                auto b = stmt.bind(bindIndex++, static_cast<int64_t>(id));
                if (!b) {
                    results.errorMessage = "Bind doc ID failed: " + b.error().message;
                    return results;
                }
            }
        }

        auto bLimit = stmt.bind(bindIndex++, effectiveLimit);
        if (!bLimit) {
            results.errorMessage = "Bind limit failed: " + bLimit.error().message;
            return results;
        }

        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) {
                spdlog::warn("[FUZZY] FTS5 step failed: {}", stepResult.error().message);
                break;
            }
            if (!stepResult.value())
                break;

            SearchResult result;
            const int scoreIndex = includeSnippet ? 2 : 1;
            const int docStartIndex = includeSnippet ? 3 : 2;
            result.document.id = stmt.getInt64(0);
            result.document.filePath = stmt.getString(docStartIndex + 0);
            result.document.fileName = stmt.getString(docStartIndex + 1);
            result.document.fileExtension = stmt.getString(docStartIndex + 2);
            result.document.fileSize = stmt.getInt64(docStartIndex + 3);
            result.document.sha256Hash = stmt.getString(docStartIndex + 4);
            result.document.mimeType = stmt.getString(docStartIndex + 5);
            result.document.createdTime = stmt.getTime(docStartIndex + 6);
            result.document.modifiedTime = stmt.getTime(docStartIndex + 7);
            result.document.indexedTime = stmt.getTime(docStartIndex + 8);
            result.document.contentExtracted = stmt.getInt(docStartIndex + 9) != 0;
            result.document.extractionStatus =
                ExtractionStatusUtils::fromString(stmt.getString(docStartIndex + 10));
            result.document.extractionError = stmt.getString(docStartIndex + 11);
            result.snippet =
                includeSnippet ? common::sanitizeUtf8(stmt.getString(1)) : std::string{};
            result.score = stmt.getDouble(scoreIndex);

            results.results.push_back(result);
        }

        results.totalCount = results.results.size();

        auto end = std::chrono::high_resolution_clock::now();
        results.executionTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - totalStart).count();
        spdlog::debug("[FUZZY] SymSpell+FTS5 returned {} results in {}ms", results.results.size(),
                      results.executionTimeMs);

        return results;
    });
}

} // namespace yams::metadata
