// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>

#include "crud_ops.hpp"
#include "result_helpers.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransactionWithRetry;
using repository::scope_exit;

namespace {
bool isTagMetadataKey(std::string_view key) {
    return key == "tag" || key.starts_with("tag:");
}
} // namespace

// Metadata operations
Result<void> MetadataRepository::setMetadata(int64_t documentId, const std::string& key,
                                             const MetadataValue& value) {
    uint64_t tagCountDelta = 0;
    uint64_t docsWithTagsDelta = 0;
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        if (isTagMetadataKey(key)) {
            auto countStmt = db.prepareCached("SELECT COUNT(*) FROM metadata WHERE document_id = ? "
                                              "AND (key = 'tag' OR key LIKE 'tag:%')");
            if (countStmt) {
                auto& stmt = *countStmt.value();
                YAMS_TRY(stmt.reset());
                YAMS_TRY(stmt.bind(1, documentId));
                YAMS_TRY_UNWRAP(hasRow, stmt.step());
                const auto priorTagCount = hasRow ? stmt.getInt64(0) : 0;

                auto existsStmt = db.prepareCached(
                    "SELECT COUNT(*) FROM metadata WHERE document_id = ? AND key = ?");
                if (existsStmt) {
                    auto& estmt = *existsStmt.value();
                    YAMS_TRY(estmt.reset());
                    YAMS_TRY(estmt.bind(1, documentId));
                    YAMS_TRY(estmt.bind(2, key));
                    YAMS_TRY_UNWRAP(existsRow, estmt.step());
                    const auto priorKeyCount = existsRow ? estmt.getInt64(0) : 0;
                    if (priorKeyCount == 0) {
                        tagCountDelta = 1;
                        if (priorTagCount == 0) {
                            docsWithTagsDelta = 1;
                        }
                    }
                }
            }
        }

        // Single-row fast path: avoid batch scaffolding + explicit transaction.
        // Use ON CONFLICT to avoid DELETE+INSERT semantics of OR REPLACE (less write
        // amplification).
        static const std::string sql =
            "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(document_id, key) DO UPDATE SET value = excluded.value, "
            "value_type = excluded.value_type";
        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));
        YAMS_TRY(stmt->bind(1, documentId));
        YAMS_TRY(stmt->bind(2, key));
        YAMS_TRY(stmt->bind(3, value.value));
        YAMS_TRY(stmt->bind(4, MetadataValueTypeUtils::toStringView(value.type)));
        YAMS_TRY(stmt->execute());
        return {};
    });

    if (result) {
        if (tagCountDelta > 0) {
            cachedTagCount_.fetch_add(tagCountDelta, std::memory_order_relaxed);
        }
        if (docsWithTagsDelta > 0) {
            cachedDocsWithTags_.fetch_add(docsWithTagsDelta, std::memory_order_relaxed);
        }
        // Signal enumeration cache invalidation
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    }
    return result;
}

Result<void> MetadataRepository::setMetadataBatch(
    const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) {
    if (entries.empty()) {
        return Result<void>();
    }

    // Deduplicate entries by (document_id, key) within the batch.
    // Duplicate pairs cause SQLite ON CONFLICT DO UPDATE to fail.
    // Keep the last entry for each key (latest write wins).
    std::vector<std::tuple<int64_t, std::string, MetadataValue>> deduped;
    deduped.reserve(entries.size());
    std::unordered_map<uint64_t, size_t> seen;
    for (const auto& entry : entries) {
        auto docId = std::get<0>(entry);
        auto& key = std::get<1>(entry);
        uint64_t hash = static_cast<uint64_t>(docId) * 31 + std::hash<std::string>{}(key);
        auto it = seen.find(hash);
        if (it != seen.end()) {
            deduped[it->second] = entry;
        } else {
            seen[hash] = deduped.size();
            deduped.push_back(entry);
        }
    }

    uint64_t tagCountDelta = 0;
    uint64_t docsWithTagsDelta = 0;
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        // Chunked multi-row upsert:
        // - Avoid INSERT OR REPLACE delete+insert semantics (less write amplification)
        // - Avoid per-entry MetadataEntry allocations/copies
        // - Reuse cached statement for the common "full chunk" shape
        constexpr int kColumnsPerRow = 4; // document_id, key, value, value_type
        constexpr int kSqliteParamLimit = 999;
        constexpr int kMaxRowsPerChunk = kSqliteParamLimit / kColumnsPerRow; // 249

        auto buildUpsertSql = [](int rows) -> std::string {
            std::string sql;
            sql.reserve(static_cast<size_t>(rows) * 20 + 200);
            sql += "INSERT INTO metadata (document_id, key, value, value_type) VALUES ";
            for (int i = 0; i < rows; ++i) {
                if (i > 0)
                    sql += ',';
                sql += "(?, ?, ?, ?)";
            }
            sql += " ON CONFLICT(document_id, key) DO UPDATE SET value = excluded.value, "
                   "value_type = excluded.value_type";
            return sql;
        };

        const std::string fullChunkSql = buildUpsertSql(kMaxRowsPerChunk);

        // Wrap the full operation in a single transaction.
        // beginTransactionWithRetry() uses BEGIN IMMEDIATE on SQLite to avoid mid-loop lock
        // surprises; commit/rollback uses Database::execute.
        YAMS_TRY(beginTransactionWithRetry(db));
        auto rollback = scope_exit([&] { db.execute("ROLLBACK"); });

        std::unordered_map<int64_t, std::vector<std::string>> pendingTagKeysByDoc;
        for (const auto& [documentId, key, _value] : deduped) {
            if (isTagMetadataKey(key)) {
                pendingTagKeysByDoc[documentId].push_back(key);
            }
        }

        if (!pendingTagKeysByDoc.empty()) {
            auto tagCountStmt = db.prepareCached("SELECT COUNT(*) FROM metadata WHERE document_id "
                                                 "= ? AND (key = 'tag' OR key LIKE 'tag:%')");
            auto keyExistsStmt =
                db.prepareCached("SELECT COUNT(*) FROM metadata WHERE document_id = ? AND key = ?");
            if (tagCountStmt && keyExistsStmt) {
                for (auto& [documentId, keys] : pendingTagKeysByDoc) {
                    auto& tcStmt = *tagCountStmt.value();
                    YAMS_TRY(tcStmt.reset());
                    YAMS_TRY(tcStmt.bind(1, documentId));
                    YAMS_TRY_UNWRAP(tagRow, tcStmt.step());
                    const auto priorTagCount = tagRow ? tcStmt.getInt64(0) : 0;
                    bool docWillGainFirstTag = priorTagCount == 0;
                    std::unordered_set<std::string> seenKeys;
                    for (const auto& tagKey : keys) {
                        if (!seenKeys.insert(tagKey).second) {
                            continue;
                        }
                        auto& keStmt = *keyExistsStmt.value();
                        YAMS_TRY(keStmt.reset());
                        YAMS_TRY(keStmt.bind(1, documentId));
                        YAMS_TRY(keStmt.bind(2, tagKey));
                        YAMS_TRY_UNWRAP(keyRow, keStmt.step());
                        const auto priorKeyCount = keyRow ? keStmt.getInt64(0) : 0;
                        if (priorKeyCount == 0) {
                            ++tagCountDelta;
                            if (docWillGainFirstTag) {
                                ++docsWithTagsDelta;
                                docWillGainFirstTag = false;
                            }
                        }
                    }
                }
            }
        }

        for (size_t offset = 0; offset < deduped.size(); offset += kMaxRowsPerChunk) {
            const int rows = static_cast<int>(
                std::min(deduped.size() - offset, static_cast<size_t>(kMaxRowsPerChunk)));

            if (rows == kMaxRowsPerChunk) {
                YAMS_TRY_UNWRAP(stmt, db.prepareCached(fullChunkSql));
                int bindIndex = 1;
                for (int i = 0; i < rows; ++i) {
                    const auto& [documentId, key, value] = deduped[offset + static_cast<size_t>(i)];
                    YAMS_TRY(stmt->bind(bindIndex++, documentId));
                    YAMS_TRY(stmt->bind(bindIndex++, key));
                    YAMS_TRY(stmt->bind(bindIndex++, value.value));
                    YAMS_TRY(
                        stmt->bind(bindIndex++, MetadataValueTypeUtils::toStringView(value.type)));
                }
                YAMS_TRY(stmt->execute());
            } else {
                const std::string tailSql = buildUpsertSql(rows);
                YAMS_TRY_UNWRAP(stmt, db.prepare(tailSql));
                int bindIndex = 1;
                for (int i = 0; i < rows; ++i) {
                    const auto& [documentId, key, value] = deduped[offset + static_cast<size_t>(i)];
                    YAMS_TRY(stmt.bind(bindIndex++, documentId));
                    YAMS_TRY(stmt.bind(bindIndex++, key));
                    YAMS_TRY(stmt.bind(bindIndex++, value.value));
                    YAMS_TRY(
                        stmt.bind(bindIndex++, MetadataValueTypeUtils::toStringView(value.type)));
                }
                YAMS_TRY(stmt.execute());
            }
        }

        YAMS_TRY(db.execute("COMMIT"));
        rollback.dismiss();
        return {};
    });

    if (result) {
        // Signal corpus stats stale - metadata batch may affect corpus statistics
        signalCorpusStatsStale();
        if (tagCountDelta > 0) {
            cachedTagCount_.fetch_add(tagCountDelta, std::memory_order_relaxed);
        }
        if (docsWithTagsDelta > 0) {
            cachedDocsWithTags_.fetch_add(docsWithTagsDelta, std::memory_order_relaxed);
        }
        // Signal enumeration cache invalidation
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    }
    return result;
}

Result<std::optional<MetadataValue>> MetadataRepository::getMetadata(int64_t documentId,
                                                                     const std::string& key) {
    return executeReadQuery<std::optional<MetadataValue>>(
        [&](Database& db) -> Result<std::optional<MetadataValue>> {
            repository::CrudOps<repository::MetadataEntry> ops;
            YAMS_TRY_UNWRAP(entry,
                            ops.queryOne(db, "document_id = ? AND key = ?", documentId, key));
            if (!entry) {
                return std::optional<MetadataValue>{};
            }
            MetadataValue value;
            value.value = entry->value;
            value.type = MetadataValueTypeUtils::fromString(entry->valueType);
            return std::optional<MetadataValue>{std::move(value)};
        });
}

Result<std::unordered_map<std::string, MetadataValue>>
MetadataRepository::getAllMetadata(int64_t documentId) {
    return executeReadQuery<std::unordered_map<std::string, MetadataValue>>(
        [&](Database& db) -> Result<std::unordered_map<std::string, MetadataValue>> {
            repository::CrudOps<repository::MetadataEntry> ops;
            YAMS_TRY_UNWRAP(entries, ops.query(db, "document_id = ?", documentId));

            std::unordered_map<std::string, MetadataValue> result;
            for (const auto& entry : entries) {
                MetadataValue value;
                value.value = entry.value;
                value.type = MetadataValueTypeUtils::fromString(entry.valueType);
                result[entry.key] = value;
            }
            return result;
        });
}

Result<std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>>
MetadataRepository::getMetadataForDocuments(std::span<const int64_t> documentIds) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getMetadataForDocuments");
    YAMS_PLOT("metadata_repo::metadata_batch_requested", static_cast<int64_t>(documentIds.size()));
    if (documentIds.empty())
        return std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>{};

    auto result = executeReadQuery<
        std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>>(
        [&](Database& db)
            -> Result<std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>> {
            using yams::metadata::sql::QuerySpec;
            std::string inList;
            inList.reserve(documentIds.size() * 2);
            inList += '(';
            for (size_t i = 0; i < documentIds.size(); ++i) {
                if (i > 0)
                    inList += ',';
                inList += '?';
            }
            inList += ')';
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"document_id", "key", "value", "value_type"};
            spec.conditions = {"document_id IN " + inList};

            YAMS_TRY_UNWRAP(stmt, db.prepare(yams::metadata::sql::buildSelect(spec)));
            int index = 1;
            for (auto id : documentIds) {
                YAMS_TRY(stmt.bind(index++, id));
            }

            std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>> result;
            while (true) {
                YAMS_TRY_UNWRAP(hasRow, stmt.step());
                if (!hasRow)
                    break;

                int64_t docId = stmt.getInt64(0);
                MetadataValue value;
                value.value = stmt.getString(2);
                value.type = MetadataValueTypeUtils::fromString(stmt.getString(3));
                result[docId][stmt.getString(1)] = value;
            }

            return result;
        });
    if (result) {
        YAMS_PLOT("metadata_repo::metadata_batch_docs",
                  static_cast<int64_t>(result.value().size()));
    }
    return result;
}

Result<void> MetadataRepository::removeMetadata(int64_t documentId, const std::string& key) {
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<repository::MetadataEntry> ops;
        ops.deleteWhere(db, "document_id = ? AND key = ?", documentId, key);
        return {};
    });

    if (result) {
        // Signal enumeration cache invalidation
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    }
    return result;
}
} // namespace yams::metadata
