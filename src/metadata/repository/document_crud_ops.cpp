// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>

#include "corpus_stats_ops.hpp"
#include "metadata_write_helpers.hpp"
#include "result_helpers.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransaction;
using repository::beginTransactionWithRetry;
using repository::classifyExtensionBucket;
using repository::commitOrRollback;
using repository::ExtensionBucket;
using repository::MetadataTagDelta;
using repository::rollbackIgnoringErrors;
using repository::scope_exit;

namespace {
void applyExtensionStatsDelta(std::atomic<uint64_t>& codeCounter,
                              std::atomic<uint64_t>& proseCounter,
                              std::atomic<uint64_t>& binaryCounter, std::string_view ext,
                              std::int64_t delta) {
    auto apply = [&](std::atomic<uint64_t>& counter) {
        if (delta >= 0) {
            counter.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed);
        } else {
            auto current = counter.load(std::memory_order_relaxed);
            const auto subtract = static_cast<uint64_t>(-delta);
            while (true) {
                const auto next = current > subtract ? current - subtract : 0;
                if (counter.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                                  std::memory_order_relaxed)) {
                    return;
                }
            }
        }
    };
    switch (classifyExtensionBucket(ext)) {
        case ExtensionBucket::Code:
            apply(codeCounter);
            break;
        case ExtensionBucket::Prose:
            apply(proseCounter);
            break;
        case ExtensionBucket::Binary:
            apply(binaryCounter);
            break;
        case ExtensionBucket::Other:
            break;
    }
}

void updateExtensionCountMap(std::unordered_map<std::string, int64_t>& counts, std::string_view ext,
                             std::int64_t delta) {
    if (ext.empty() || delta == 0) {
        return;
    }
    auto key = std::string(ext);
    auto& entry = counts[key];
    entry += delta;
    if (entry <= 0) {
        counts.erase(key);
    }
}

void saturatingSubBytes(std::atomic<uint64_t>& counter, uint64_t bytes) {
    auto current = counter.load(std::memory_order_relaxed);
    while (true) {
        const auto next = current > bytes ? current - bytes : 0;
        if (counter.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
            return;
        }
    }
}

void updateCachedPathDepthMax(std::atomic<uint64_t>& cachedPathDepthMax, uint64_t nextDepth) {
    auto currentDepthMax = cachedPathDepthMax.load(std::memory_order_relaxed);
    while (nextDepth > currentDepthMax &&
           !cachedPathDepthMax.compare_exchange_weak(
               currentDepthMax, nextDepth, std::memory_order_acq_rel, std::memory_order_relaxed)) {
    }
}

void reconcileCachedPathDepthMax(std::atomic<uint64_t>& cachedPathDepthMax,
                                 uint64_t expectedBeforeMutation, uint64_t recomputedPathDepthMax) {
    auto currentDepthMax = cachedPathDepthMax.load(std::memory_order_relaxed);
    while (true) {
        if (currentDepthMax == expectedBeforeMutation || currentDepthMax < recomputedPathDepthMax) {
            if (cachedPathDepthMax.compare_exchange_weak(currentDepthMax, recomputedPathDepthMax,
                                                         std::memory_order_acq_rel,
                                                         std::memory_order_relaxed)) {
                return;
            }
            continue;
        }
        return;
    }
}

Result<uint64_t> queryCurrentPathDepthMaxInDb(Database& db) {
    YAMS_TRY_UNWRAP(stmt, db.prepare("SELECT COALESCE(MAX(path_depth), 0) FROM documents"));
    YAMS_TRY_UNWRAP(hasRow, stmt.step());
    if (!hasRow) {
        return uint64_t{0};
    }
    return static_cast<uint64_t>(std::max<int64_t>(stmt.getInt64(0), 0));
}

void applyMetadataTagDelta(std::atomic<uint64_t>& cachedTagCount,
                           std::atomic<uint64_t>& cachedDocsWithTags,
                           const std::atomic<uint64_t>& cachedDocumentCount,
                           const MetadataTagDelta& delta) {
    if (delta.tagCountDelta > 0) {
        cachedTagCount.fetch_add(static_cast<uint64_t>(delta.tagCountDelta),
                                 std::memory_order_relaxed);
    } else if (delta.tagCountDelta < 0) {
        core::saturating_sub(cachedTagCount, static_cast<uint64_t>(-delta.tagCountDelta));
    }

    if (delta.docsWithTagsDelta > 0) {
        cachedDocsWithTags.fetch_add(static_cast<uint64_t>(delta.docsWithTagsDelta),
                                     std::memory_order_relaxed);
    } else if (delta.docsWithTagsDelta < 0) {
        core::saturating_sub(cachedDocsWithTags, static_cast<uint64_t>(-delta.docsWithTagsDelta));
    }

    const auto docsWithTags = cachedDocsWithTags.load(std::memory_order_relaxed);
    const auto docCount = cachedDocumentCount.load(std::memory_order_relaxed);
    const auto tagCount = cachedTagCount.load(std::memory_order_relaxed);
    YAMS_DCHECK(docsWithTags <= docCount,
                "metadata tag counter invariant violated: docsWithTags <= docCount");
    YAMS_DCHECK(tagCount >= docsWithTags,
                "metadata tag counter invariant violated: tagCount >= docsWithTags");
}

Result<MetadataTagDelta> calculateDocumentDeleteTagDelta(Database& db, int64_t documentId) {
    YAMS_TRY_UNWRAP(tagCountStmt, db.prepareCached("SELECT COUNT(*) FROM metadata WHERE "
                                                   "document_id = ? AND (key = 'tag' OR key "
                                                   "LIKE 'tag:%')"));
    YAMS_TRY(tagCountStmt->reset());
    YAMS_TRY(tagCountStmt->bind(1, documentId));
    YAMS_TRY_UNWRAP(hasRow, tagCountStmt->step());

    MetadataTagDelta delta;
    if (!hasRow) {
        return delta;
    }

    const auto priorTagCount = tagCountStmt->getInt64(0);
    if (priorTagCount > 0) {
        delta.tagCountDelta = -priorTagCount;
        delta.docsWithTagsDelta = -1;
    }
    return delta;
}

struct InsertDocumentWithMetadataResult {
    int64_t docId{0};
    bool insertedNewDocument{false};
    MetadataTagDelta metadataTagDelta{};
    uint64_t metadataWriteCount{0};
};

struct DeleteDocumentResult {
    bool deleted{false};
    uint64_t totalSizeBytesRemoved{0};
    uint64_t pathDepthSumRemoved{0};
    bool extractedRemoved{false};
    bool indexedRemoved{false};
    bool embeddedRemoved{false};
    std::string removedExtension;
    std::string removedFilePath;
    MetadataTagDelta metadataTagDelta{};
};

struct DeleteDocumentsBatchResult {
    size_t deletedCount{0};
    uint64_t totalSizeBytesRemoved{0};
    uint64_t pathDepthSumRemoved{0};
    uint64_t extractedRemoved{0};
    uint64_t indexedRemoved{0};
    uint64_t embeddedRemoved{0};
    std::unordered_map<std::string, int64_t> removedExtensionCounts;
    std::vector<std::string> removedFilePaths;
    MetadataTagDelta metadataTagDelta{};
};

Result<std::pair<int64_t, bool>> insertOrLookupDocumentRow(Database& db, const DocumentInfo& info,
                                                           bool hasPathIndexing) {
    std::string sql =
        "INSERT OR IGNORE INTO documents (file_path, file_name, file_extension, "
        "file_size, sha256_hash, mime_type, created_time, modified_time, "
        "indexed_time, content_extracted, extraction_status, extraction_error";
    if (hasPathIndexing) {
        sql += ", path_prefix, reverse_path, path_hash, parent_hash, path_depth";
    }
    sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";
    if (hasPathIndexing) {
        sql += ", ?, ?, ?, ?, ?";
    }
    sql += ")";

    YAMS_TRY_UNWRAP(stmtResult, db.prepareCached(sql));
    auto& stmt = *stmtResult;

    if (hasPathIndexing) {
        YAMS_TRY(stmt.bindAll(info.filePath, info.fileName, info.fileExtension, info.fileSize,
                              info.sha256Hash, info.mimeType, info.createdTime, info.modifiedTime,
                              info.indexedTime, info.contentExtracted ? 1 : 0,
                              ExtractionStatusUtils::toString(info.extractionStatus),
                              info.extractionError, info.pathPrefix, info.reversePath,
                              info.pathHash, info.parentHash, info.pathDepth));
    } else {
        YAMS_TRY(stmt.bindAll(info.filePath, info.fileName, info.fileExtension, info.fileSize,
                              info.sha256Hash, info.mimeType, info.createdTime, info.modifiedTime,
                              info.indexedTime, info.contentExtracted ? 1 : 0,
                              ExtractionStatusUtils::toString(info.extractionStatus),
                              info.extractionError));
    }

    YAMS_TRY(stmt.execute());

    if (db.changes() > 0) {
        const int64_t docId = db.lastInsertRowId();
        spdlog::debug("insertDocumentWithMetadata: inserted hash={} id={}", info.sha256Hash, docId);
        return std::pair<int64_t, bool>{docId, true};
    }

    YAMS_TRY_UNWRAP(checkStmtResult,
                    db.prepareCached("SELECT id FROM documents WHERE sha256_hash = ?"));
    auto& checkStmt = *checkStmtResult;
    YAMS_TRY(checkStmt.reset());
    YAMS_TRY(checkStmt.clearBindings());
    YAMS_TRY(checkStmt.bind(1, info.sha256Hash));
    YAMS_TRY_UNWRAP(stepRes, checkStmt.step());
    if (!stepRes) {
        return Error{ErrorCode::DatabaseError,
                     "Document insert ignored but existing record not found"};
    }
    const int64_t docId = checkStmt.getInt64(0);
    spdlog::debug("insertDocumentWithMetadata: existing hash={} id={}", info.sha256Hash, docId);
    return std::pair<int64_t, bool>{docId, false};
}

Result<MetadataTagDelta>
applyDocumentMetadataWrites(Database& db,
                            const std::vector<repository::MetadataWriteEntry>& dedupedWrites,
                            int64_t docId) {
    if (dedupedWrites.empty()) {
        return MetadataTagDelta{};
    }
    auto writesForDoc = dedupedWrites;
    for (auto& [writeDocId, key, value] : writesForDoc) {
        (void)key;
        (void)value;
        writeDocId = docId;
    }
    return repository::upsertMetadataWritesWithTagDelta(db, writesForDoc);
}

Result<void> upsertTreeSnapshotRow(Database& db, const TreeSnapshotRecord& snapshot) {
    auto metaOrEmpty = [&](const char* key) -> std::string {
        auto it = snapshot.metadata.find(key);
        return it != snapshot.metadata.end() ? it->second : std::string{};
    };

    YAMS_TRY_UNWRAP(snapStmtResult, db.prepareCached(R"(
                INSERT INTO tree_snapshots (
                    snapshot_id, created_at, directory_path, tree_root_hash,
                    snapshot_label, git_commit, git_branch, git_remote, files_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id) DO UPDATE SET
                    created_at = excluded.created_at,
                    directory_path = excluded.directory_path,
                    tree_root_hash = excluded.tree_root_hash,
                    snapshot_label = excluded.snapshot_label,
                    git_commit = excluded.git_commit,
                    git_branch = excluded.git_branch,
                    git_remote = excluded.git_remote,
                    files_count = excluded.files_count
            )"));
    auto& snapStmt = *snapStmtResult;
    YAMS_TRY(snapStmt.reset());
    YAMS_TRY(snapStmt.clearBindings());

    auto bindTextOrNull = [&](int index, const std::string& value) -> Result<void> {
        if (value.empty()) {
            return snapStmt.bind(index, nullptr);
        }
        return snapStmt.bind(index, value);
    };

    YAMS_TRY(snapStmt.bind(1, snapshot.snapshotId));
    YAMS_TRY(snapStmt.bind(2, static_cast<int64_t>(snapshot.createdTime)));
    YAMS_TRY(bindTextOrNull(3, metaOrEmpty("directory_path")));
    YAMS_TRY(bindTextOrNull(4, snapshot.rootTreeHash));
    YAMS_TRY(bindTextOrNull(5, metaOrEmpty("snapshot_label")));
    YAMS_TRY(bindTextOrNull(6, metaOrEmpty("git_commit")));
    YAMS_TRY(bindTextOrNull(7, metaOrEmpty("git_branch")));
    YAMS_TRY(bindTextOrNull(8, metaOrEmpty("git_remote")));
    YAMS_TRY(snapStmt.bind(9, static_cast<int64_t>(snapshot.fileCount)));
    return snapStmt.execute();
}
} // namespace

// Document operations
Result<int64_t> MetadataRepository::insertDocument(const DocumentInfo& info) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::insertDocument");
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        // Build INSERT OR IGNORE SQL based on whether path indexing columns exist
        std::string sql = "INSERT OR IGNORE INTO documents (file_path, file_name, file_extension, "
                          "file_size, sha256_hash, mime_type, created_time, modified_time, "
                          "indexed_time, content_extracted, extraction_status, extraction_error";

        if (hasPathIndexing_) {
            sql += ", path_prefix, reverse_path, path_hash, parent_hash, path_depth";
        }

        sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";

        if (hasPathIndexing_) {
            sql += ", ?, ?, ?, ?, ?";
        }

        sql += ")";

        auto stmtResult = db.prepare(sql);

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();

        // Bind common columns first
        if (hasPathIndexing_) {
            auto bindResult = stmt.bindAll(
                info.filePath, info.fileName, info.fileExtension, info.fileSize, info.sha256Hash,
                info.mimeType, info.createdTime, info.modifiedTime, info.indexedTime,
                info.contentExtracted ? 1 : 0,
                ExtractionStatusUtils::toString(info.extractionStatus), info.extractionError,
                info.pathPrefix, info.reversePath, info.pathHash, info.parentHash, info.pathDepth);

            if (!bindResult)
                return bindResult.error();
        } else {
            // Compat mode: only bind the 12 columns that exist
            auto bindResult = stmt.bindAll(
                info.filePath, info.fileName, info.fileExtension, info.fileSize, info.sha256Hash,
                info.mimeType, info.createdTime, info.modifiedTime, info.indexedTime,
                info.contentExtracted ? 1 : 0,
                ExtractionStatusUtils::toString(info.extractionStatus), info.extractionError);

            if (!bindResult)
                return bindResult.error();
        }

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        // Check if a row was actually inserted (changes() returns 0 if INSERT was ignored)
        int changes = db.changes();
        int64_t docId;

        if (changes > 0) {
            // New document inserted
            docId = db.lastInsertRowId();

            // Update component-owned metrics
            cachedDocumentCount_.fetch_add(1, std::memory_order_relaxed);
            cachedTotalSizeBytes_.fetch_add(
                static_cast<uint64_t>(std::max<int64_t>(info.fileSize, 0)),
                std::memory_order_relaxed);
            applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_,
                                     cachedBinaryDocCount_, info.fileExtension, 1);
            cachedPathDepthSum_.fetch_add(static_cast<uint64_t>(std::max(info.pathDepth, 0)),
                                          std::memory_order_relaxed);
            updateCachedPathDepthMax(cachedPathDepthMax_,
                                     static_cast<uint64_t>(std::max(info.pathDepth, 0)));
            {
                std::lock_guard<std::mutex> lock(extensionStatsMutex_);
                updateExtensionCountMap(cachedExtensionCounts_, info.fileExtension, 1);
            }
            if (info.contentExtracted) {
                cachedExtractedCount_.fetch_add(1, std::memory_order_relaxed);
            }

            spdlog::debug("Inserted new document with hash {} (id={})", info.sha256Hash, docId);
        } else {
            // Document already exists (INSERT was ignored), retrieve existing ID
            auto checkStmt = db.prepare("SELECT id FROM documents WHERE sha256_hash = ?");
            if (!checkStmt)
                return checkStmt.error();

            auto& stmt2 = checkStmt.value();
            if (auto bindRes = stmt2.bind(1, info.sha256Hash); !bindRes)
                return bindRes.error();

            auto stepRes = stmt2.step();
            if (!stepRes)
                return stepRes.error();

            if (!stepRes.value())
                return Error{ErrorCode::DatabaseError,
                             "Document insert was ignored but could not find existing document"};

            docId = stmt2.getInt64(0);
            spdlog::debug("Document with hash {} already exists (id={}), using existing",
                          info.sha256Hash, docId);
        }

        return docId;
    });
}

Result<int64_t> MetadataRepository::insertDocumentWithMetadata(
    const DocumentInfo& info, const std::vector<std::pair<std::string, MetadataValue>>& tags,
    TreeSnapshotRecord* snapshot) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::insertDocumentWithMetadata");

    std::vector<repository::MetadataWriteEntry> metadataWrites;
    metadataWrites.reserve(tags.size());
    for (const auto& [key, value] : tags) {
        metadataWrites.emplace_back(0, key, value);
    }
    const auto dedupedMetadataWrites = repository::deduplicateMetadataWrites(metadataWrites);

    auto result = executeQuery<InsertDocumentWithMetadataResult>(
        [&](Database& db) -> Result<InsertDocumentWithMetadataResult> {
            // Keep retry ownership at executeQueryOnPool and avoid stacking inner BEGIN retries.
            YAMS_TRY(beginTransaction(db));
            bool committed = false;
            auto rollback = scope_exit([&] {
                if (!committed) {
                    rollbackIgnoringErrors(db);
                }
            });

            YAMS_TRY_UNWRAP(inserted,
                            insertOrLookupDocumentRow(db, info, hasPathIndexing_));
            const auto [docId, insertedNewDocument] = inserted;

            YAMS_TRY_UNWRAP(metadataTagDelta,
                            applyDocumentMetadataWrites(db, dedupedMetadataWrites, docId));
            const uint64_t metadataWriteCount = dedupedMetadataWrites.size();

            if (snapshot) {
                snapshot->ingestDocumentId = docId;
                YAMS_TRY(upsertTreeSnapshotRow(db, *snapshot));
            }

            YAMS_TRY(commitOrRollback(db));
            committed = true;

            return InsertDocumentWithMetadataResult{docId, insertedNewDocument, metadataTagDelta,
                                                    metadataWriteCount};
        });

    if (!result) {
        return result.error();
    }

    const auto& update = result.value();
    if (update.insertedNewDocument) {
        cachedDocumentCount_.fetch_add(1, std::memory_order_relaxed);
        cachedTotalSizeBytes_.fetch_add(static_cast<uint64_t>(std::max<int64_t>(info.fileSize, 0)),
                                        std::memory_order_relaxed);
        applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_, cachedBinaryDocCount_,
                                 info.fileExtension, 1);
        cachedPathDepthSum_.fetch_add(static_cast<uint64_t>(std::max(info.pathDepth, 0)),
                                      std::memory_order_relaxed);
        updateCachedPathDepthMax(cachedPathDepthMax_,
                                 static_cast<uint64_t>(std::max(info.pathDepth, 0)));
        {
            std::lock_guard<std::mutex> lock(extensionStatsMutex_);
            updateExtensionCountMap(cachedExtensionCounts_, info.fileExtension, 1);
        }
        if (info.contentExtracted) {
            cachedExtractedCount_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    if (update.metadataWriteCount > 0) {
        signalCorpusStatsStale();
        if (update.metadataTagDelta.tagCountDelta > 0) {
            cachedTagCount_.fetch_add(static_cast<uint64_t>(update.metadataTagDelta.tagCountDelta),
                                      std::memory_order_relaxed);
        }
        if (update.metadataTagDelta.docsWithTagsDelta > 0) {
            cachedDocsWithTags_.fetch_add(
                static_cast<uint64_t>(update.metadataTagDelta.docsWithTagsDelta),
                std::memory_order_relaxed);
        }
        const auto docsWithTags = cachedDocsWithTags_.load(std::memory_order_relaxed);
        const auto docCount = cachedDocumentCount_.load(std::memory_order_relaxed);
        const auto tagCount = cachedTagCount_.load(std::memory_order_relaxed);
        YAMS_DCHECK(docsWithTags <= docCount,
                    "metadata tag counter invariant violated: docsWithTags <= docCount");
        YAMS_DCHECK(tagCount >= docsWithTags,
                    "metadata tag counter invariant violated: tagCount >= docsWithTags");
        metadataChangeCounter_.fetch_add(update.metadataWriteCount, std::memory_order_release);
    }
    return update.docId;
}

Result<std::optional<DocumentInfo>> MetadataRepository::getDocument(int64_t id) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getDocument");
    return executeReadQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            return getDocumentByCondition(db, "id = ?",
                                          [&](Statement& stmt) { return stmt.bind(1, id); });
        });
}

// Internal helper that uses an existing connection to avoid nested connection acquisition deadlock
Result<std::optional<DocumentInfo>> MetadataRepository::getDocumentInternal(Database& db,
                                                                            int64_t id) {
    return getDocumentByCondition(db, "id = ?", [&](Statement& stmt) { return stmt.bind(1, id); });
}

// Internal helper that uses an existing connection to avoid nested connection acquisition deadlock
Result<std::unordered_map<std::string, MetadataValue>>
MetadataRepository::getAllMetadataInternal(Database& db, int64_t documentId) {
    using yams::metadata::sql::QuerySpec;
    QuerySpec spec{};
    spec.table = "metadata";
    spec.columns = {"key", "value", "value_type"};
    spec.conditions = {"document_id = ?"};

    YAMS_TRY_UNWRAP(stmt, db.prepare(yams::metadata::sql::buildSelect(spec)));
    YAMS_TRY(stmt.bind(1, documentId));

    std::unordered_map<std::string, MetadataValue> result;
    while (true) {
        YAMS_TRY_UNWRAP(hasRow, stmt.step());
        if (!hasRow)
            break;

        std::string key = stmt.getString(0);
        MetadataValue value;
        value.value = stmt.getString(1);
        value.type = MetadataValueTypeUtils::fromString(stmt.getString(2));
        result[key] = value;
    }

    return result;
}

Result<std::optional<DocumentInfo>> MetadataRepository::getDocumentByHash(const std::string& hash) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getDocumentByHash");
    return executeReadQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            return getDocumentByCondition(db, "sha256_hash = ?",
                                          [&](Statement& stmt) { return stmt.bind(1, hash); });
        });
}

Result<void> MetadataRepository::updateDocument(const DocumentInfo& info) {
    const auto cachedPathDepthMaxBeforeMutation =
        cachedPathDepthMax_.load(std::memory_order_relaxed);
    bool shouldRefreshPathDepthMax = false;
    uint64_t recomputedPathDepthMax = 0;
    std::string priorFilePath;

    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        int64_t priorFileSize = info.fileSize;
        std::string priorExtension = info.fileExtension;
        int priorPathDepth = info.pathDepth;
        bool priorContentExtracted = info.contentExtracted;

        auto priorStmt = db.prepareCached(
            "SELECT file_size, file_extension, path_depth, content_extracted, file_path "
            "FROM documents WHERE id = ?");
        if (priorStmt) {
            auto& stmt = *priorStmt.value();
            YAMS_TRY(stmt.bind(1, info.id));
            if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                priorFileSize = stmt.getInt64(0);
                priorExtension = stmt.getString(1);
                priorPathDepth = stmt.getInt(2);
                priorContentExtracted = stmt.getInt(3) != 0;
                priorFilePath = stmt.getString(4);
            }
        }

        // Use prepareCached for better performance on repeated updates
        YAMS_TRY_UNWRAP(cachedStmt, db.prepareCached(R"(
            UPDATE documents SET
                file_path = ?, file_name = ?, file_extension = ?,
                file_size = ?, sha256_hash = ?, mime_type = ?,
                created_time = ?, modified_time = ?, indexed_time = ?,
                content_extracted = ?, extraction_status = ?,
                extraction_error = ?, path_prefix = ?, reverse_path = ?,
                path_hash = ?, parent_hash = ?, path_depth = ?
            WHERE id = ?
        )"));

        auto& stmt = *cachedStmt;
        YAMS_TRY(stmt.bindAll(info.filePath, info.fileName, info.fileExtension, info.fileSize,
                              info.sha256Hash, info.mimeType, info.createdTime, info.modifiedTime,
                              info.indexedTime, info.contentExtracted ? 1 : 0,
                              ExtractionStatusUtils::toString(info.extractionStatus),
                              info.extractionError, info.pathPrefix, info.reversePath,
                              info.pathHash, info.parentHash, info.pathDepth, info.id));

        YAMS_TRY(stmt.execute());
        if (db.changes() > 0) {
            const auto nextSize = static_cast<uint64_t>(std::max<int64_t>(info.fileSize, 0));
            const auto prevSize = static_cast<uint64_t>(std::max<int64_t>(priorFileSize, 0));
            if (nextSize >= prevSize) {
                cachedTotalSizeBytes_.fetch_add(nextSize - prevSize, std::memory_order_relaxed);
            } else {
                saturatingSubBytes(cachedTotalSizeBytes_, prevSize - nextSize);
            }
            applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_,
                                     cachedBinaryDocCount_, priorExtension, -1);
            applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_,
                                     cachedBinaryDocCount_, info.fileExtension, 1);
            const auto priorDepth = static_cast<uint64_t>(std::max(priorPathDepth, 0));
            const auto nextDepth = static_cast<uint64_t>(std::max(info.pathDepth, 0));
            if (priorDepth == cachedPathDepthMaxBeforeMutation && nextDepth < priorDepth) {
                YAMS_TRY_UNWRAP(recomputedMax, queryCurrentPathDepthMaxInDb(db));
                recomputedPathDepthMax = recomputedMax;
                shouldRefreshPathDepthMax = true;
            }
            saturatingSubBytes(cachedPathDepthSum_, priorDepth);
            cachedPathDepthSum_.fetch_add(nextDepth, std::memory_order_relaxed);
            updateCachedPathDepthMax(cachedPathDepthMax_, nextDepth);
            {
                std::lock_guard<std::mutex> lock(extensionStatsMutex_);
                updateExtensionCountMap(cachedExtensionCounts_, priorExtension, -1);
                updateExtensionCountMap(cachedExtensionCounts_, info.fileExtension, 1);
            }
            if (info.contentExtracted && !priorContentExtracted) {
                cachedExtractedCount_.fetch_add(1, std::memory_order_relaxed);
            } else if (!info.contentExtracted && priorContentExtracted) {
                core::saturating_sub(cachedExtractedCount_, uint64_t{1});
            }

            YAMS_DCHECK(cachedExtractedCount_.load() <= cachedDocumentCount_.load(),
                        "metadata: extracted count must not exceed total document count after "
                        "update");
            if (shouldRefreshPathDepthMax) {
                reconcileCachedPathDepthMax(cachedPathDepthMax_, cachedPathDepthMaxBeforeMutation,
                                            recomputedPathDepthMax);
            }
            signalCorpusStatsStale();
        }
        return Result<void>();
    });
    if (result) {
        std::vector<std::string> stale;
        if (!priorFilePath.empty()) {
            stale.push_back(priorFilePath);
        }
        if (info.filePath != priorFilePath) {
            stale.push_back(info.filePath);
        }
        invalidatePathCache(stale);
    }
    return result;
}

Result<void> MetadataRepository::deleteDocument(int64_t id) {
    const auto cachedPathDepthMaxBeforeMutation =
        cachedPathDepthMax_.load(std::memory_order_relaxed);
    bool shouldRefreshPathDepthMax = false;
    uint64_t recomputedPathDepthMax = 0;

    auto result =
        executeQuery<DeleteDocumentResult>([&](Database& db) -> Result<DeleteDocumentResult> {
            YAMS_TRY(beginTransactionWithRetry(db));
            bool committed = false;
            auto rollback = scope_exit([&] {
                if (!committed) {
                    repository::rollbackIgnoringErrors(db);
                }
            });

            // Query document flags before deletion to update counters
            bool wasExtracted = false;
            bool wasIndexed = false;
            bool wasEmbedded = false;
            uint64_t priorFileSize = 0;
            std::string priorExtension;
            std::string deletedFilePath;
            int priorPathDepth = 0;
            YAMS_TRY_UNWRAP(metadataTagDelta, calculateDocumentDeleteTagDelta(db, id));
            {
                // Use prepareCached for better performance on repeated deletes.
                // wasIndexed checks actual FTS row presence; wasEmbedded checks
                // document_embeddings_status.has_embedding.
                auto checkStmt = db.prepareCached(R"(
                SELECT d.content_extracted,
                       d.file_size,
                       d.file_extension,
                       d.path_depth,
                       CASE WHEN EXISTS(
                           SELECT 1 FROM documents_fts WHERE rowid = d.id
                       ) THEN 1 ELSE 0 END,
                       COALESCE(des.has_embedding, 0),
                       d.file_path
                FROM documents d
                LEFT JOIN document_embeddings_status des ON des.document_id = d.id
                WHERE d.id = ?
            )");
                if (checkStmt) {
                    auto& stmt = *checkStmt.value();
                    stmt.bind(1, id);
                    if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                        wasExtracted = stmt.getInt(0) != 0;
                        priorFileSize =
                            static_cast<uint64_t>(std::max<int64_t>(stmt.getInt64(1), 0));
                        priorExtension = stmt.getString(2);
                        priorPathDepth = stmt.getInt(3);
                        wasIndexed = stmt.getInt(4) != 0;
                        wasEmbedded = stmt.getInt(5) != 0;
                        deletedFilePath = stmt.getString(6);
                    }
                }
            }

            // Foreign key constraints will handle cascading deletes
            // Use prepareCached for better performance
            auto stmtResult = db.prepareCached("DELETE FROM documents WHERE id = ?");
            if (!stmtResult)
                return stmtResult.error();

            auto& stmt = *stmtResult.value();
            auto bindResult = stmt.bind(1, id);
            if (!bindResult)
                return bindResult.error();

            auto execResult = stmt.execute();
            if (!execResult)
                return execResult.error();

            DeleteDocumentResult deleteResult;
            if (db.changes() > 0) {
                const auto priorDepth = static_cast<uint64_t>(std::max(priorPathDepth, 0));
                deleteResult.deleted = true;
                deleteResult.totalSizeBytesRemoved = priorFileSize;
                deleteResult.pathDepthSumRemoved = priorDepth;
                deleteResult.extractedRemoved = wasExtracted;
                deleteResult.indexedRemoved = wasIndexed;
                deleteResult.embeddedRemoved = wasEmbedded;
                deleteResult.removedExtension = std::move(priorExtension);
                deleteResult.removedFilePath = std::move(deletedFilePath);
                deleteResult.metadataTagDelta = metadataTagDelta;
                if (priorDepth == cachedPathDepthMaxBeforeMutation) {
                    YAMS_TRY_UNWRAP(recomputedMax, queryCurrentPathDepthMaxInDb(db));
                    recomputedPathDepthMax = recomputedMax;
                    shouldRefreshPathDepthMax = true;
                }
            }

            YAMS_TRY(repository::commitOrRollback(db));
            committed = true;
            return deleteResult;
        });

    if (!result) {
        return result.error();
    }

    const auto& deleteResult = result.value();
    if (!deleteResult.deleted) {
        return Result<void>();
    }

    core::saturating_sub(cachedDocumentCount_, uint64_t{1});
    saturatingSubBytes(cachedTotalSizeBytes_, deleteResult.totalSizeBytesRemoved);
    applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_, cachedBinaryDocCount_,
                             deleteResult.removedExtension, -1);
    saturatingSubBytes(cachedPathDepthSum_, deleteResult.pathDepthSumRemoved);
    if (shouldRefreshPathDepthMax) {
        reconcileCachedPathDepthMax(cachedPathDepthMax_, cachedPathDepthMaxBeforeMutation,
                                    recomputedPathDepthMax);
    }
    {
        std::lock_guard<std::mutex> lock(extensionStatsMutex_);
        updateExtensionCountMap(cachedExtensionCounts_, deleteResult.removedExtension, -1);
    }
    if (deleteResult.extractedRemoved) {
        core::saturating_sub(cachedExtractedCount_, uint64_t{1});
    }
    if (deleteResult.indexedRemoved) {
        core::saturating_sub(cachedIndexedCount_, uint64_t{1});
    }
    if (deleteResult.embeddedRemoved) {
        core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
    }
    applyMetadataTagDelta(cachedTagCount_, cachedDocsWithTags_, cachedDocumentCount_,
                          deleteResult.metadataTagDelta);

    if (!deleteResult.removedFilePath.empty()) {
        invalidatePathCache(deleteResult.removedFilePath);
    }

    // Signal enumeration cache invalidation (document deletion cascades to metadata)
    metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    return Result<void>();
}

Result<size_t> MetadataRepository::deleteDocumentsBatch(const std::vector<int64_t>& ids) {
    if (ids.empty()) {
        return size_t{0};
    }

    const auto cachedPathDepthMaxBeforeMutation =
        cachedPathDepthMax_.load(std::memory_order_relaxed);
    bool shouldRefreshPathDepthMax = false;
    uint64_t recomputedPathDepthMax = 0;

    auto result = executeQuery<DeleteDocumentsBatchResult>(
        [&](Database& db) -> Result<DeleteDocumentsBatchResult> {
            // Begin transaction for batch operation
            auto beginResult = beginTransactionWithRetry(db);
            if (!beginResult) {
                return beginResult.error();
            }

            DeleteDocumentsBatchResult batchResult;

            // Prepare statement for checking document flags.
            // wasIndexed checks actual FTS row presence; wasEmbedded checks
            // document_embeddings_status.has_embedding.
            auto checkStmtResult = db.prepareCached(R"(
            SELECT d.id, d.content_extracted, d.file_size, d.file_extension, d.path_depth,
                   CASE WHEN EXISTS(
                       SELECT 1 FROM documents_fts WHERE rowid = d.id
                   ) THEN 1 ELSE 0 END,
                   COALESCE(des.has_embedding, 0),
                   d.file_path
            FROM documents d
            LEFT JOIN document_embeddings_status des ON des.document_id = d.id
            WHERE d.id = ?
        )");
            if (!checkStmtResult) {
                db.execute("ROLLBACK");
                return checkStmtResult.error();
            }
            auto& checkStmt = *checkStmtResult.value();

            // Prepare statement for deletion
            auto deleteStmtResult = db.prepareCached("DELETE FROM documents WHERE id = ?");
            if (!deleteStmtResult) {
                db.execute("ROLLBACK");
                return deleteStmtResult.error();
            }
            auto& deleteStmt = *deleteStmtResult.value();

            // Process each document
            for (int64_t id : ids) {
                // Check flags before deletion
                bool wasExtracted = false;
                bool wasIndexed = false;
                bool wasEmbedded = false;
                uint64_t priorFileSize = 0;
                std::string priorExtension;
                std::string priorFilePath;
                int priorPathDepth = 0;
                YAMS_TRY_UNWRAP(metadataTagDelta, calculateDocumentDeleteTagDelta(db, id));

                if (auto r = checkStmt.reset(); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto r = checkStmt.bind(1, id); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                auto stepRes = checkStmt.step();
                if (!stepRes) {
                    db.execute("ROLLBACK");
                    return stepRes.error();
                }
                if (stepRes.value()) {
                    wasExtracted = checkStmt.getInt(1) != 0;
                    priorFileSize =
                        static_cast<uint64_t>(std::max<int64_t>(checkStmt.getInt64(2), 0));
                    priorExtension = checkStmt.getString(3);
                    priorPathDepth = checkStmt.getInt(4);
                    wasIndexed = checkStmt.getInt(5) != 0;
                    wasEmbedded = checkStmt.getInt(6) != 0;
                    priorFilePath = checkStmt.getString(7);
                }

                // Delete document
                if (auto r = deleteStmt.reset(); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto r = deleteStmt.bind(1, id); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto execRes = deleteStmt.execute(); !execRes) {
                    db.execute("ROLLBACK");
                    return execRes.error();
                }

                // Update metrics
                if (db.changes() > 0) {
                    const auto priorDepth = static_cast<uint64_t>(std::max(priorPathDepth, 0));
                    batchResult.deletedCount++;
                    batchResult.totalSizeBytesRemoved += priorFileSize;
                    batchResult.pathDepthSumRemoved += priorDepth;
                    batchResult.metadataTagDelta.tagCountDelta += metadataTagDelta.tagCountDelta;
                    batchResult.metadataTagDelta.docsWithTagsDelta +=
                        metadataTagDelta.docsWithTagsDelta;
                    if (!priorExtension.empty()) {
                        batchResult.removedExtensionCounts[priorExtension] += 1;
                    }
                    if (!priorFilePath.empty()) {
                        batchResult.removedFilePaths.push_back(std::move(priorFilePath));
                    }
                    if (wasExtracted) {
                        batchResult.extractedRemoved += 1;
                    }
                    if (wasIndexed) {
                        batchResult.indexedRemoved += 1;
                    }
                    if (wasEmbedded) {
                        batchResult.embeddedRemoved += 1;
                    }
                    if (priorDepth == cachedPathDepthMaxBeforeMutation) {
                        shouldRefreshPathDepthMax = true;
                    }
                }
            }

            if (shouldRefreshPathDepthMax) {
                YAMS_TRY_UNWRAP(recomputedMax, queryCurrentPathDepthMaxInDb(db));
                recomputedPathDepthMax = recomputedMax;
            }

            // Commit transaction
            auto commitResult = db.execute("COMMIT");
            if (!commitResult) {
                db.execute("ROLLBACK");
                return commitResult.error();
            }

            return batchResult;
        });

    if (result) {
        const auto& batchResult = result.value();
        core::saturating_sub(cachedDocumentCount_, static_cast<uint64_t>(batchResult.deletedCount));
        saturatingSubBytes(cachedTotalSizeBytes_, batchResult.totalSizeBytesRemoved);
        saturatingSubBytes(cachedPathDepthSum_, batchResult.pathDepthSumRemoved);
        if (shouldRefreshPathDepthMax) {
            reconcileCachedPathDepthMax(cachedPathDepthMax_, cachedPathDepthMaxBeforeMutation,
                                        recomputedPathDepthMax);
        }
        for (const auto& [extension, count] : batchResult.removedExtensionCounts) {
            applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_,
                                     cachedBinaryDocCount_, extension, -count);
        }
        {
            std::lock_guard<std::mutex> lock(extensionStatsMutex_);
            for (const auto& [extension, count] : batchResult.removedExtensionCounts) {
                updateExtensionCountMap(cachedExtensionCounts_, extension, -count);
            }
        }
        core::saturating_sub(cachedExtractedCount_, batchResult.extractedRemoved);
        core::saturating_sub(cachedIndexedCount_, batchResult.indexedRemoved);
        core::saturating_sub(cachedEmbeddedCount_, batchResult.embeddedRemoved);
        applyMetadataTagDelta(cachedTagCount_, cachedDocsWithTags_, cachedDocumentCount_,
                              batchResult.metadataTagDelta);

        invalidatePathCache(batchResult.removedFilePaths);

        // Signal enumeration cache invalidation
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
        return batchResult.deletedCount;
    }
    return result.error();
}

Result<size_t> MetadataRepository::updateDocumentsMimeBatch(
    const std::vector<std::pair<int64_t, std::string>>& idMimePairs) {
    if (idMimePairs.empty()) {
        return size_t{0};
    }

    return executeQuery<size_t>([&](Database& db) -> Result<size_t> {
        // Begin transaction for batch operation
        auto beginResult = beginTransactionWithRetry(db);
        if (!beginResult) {
            return beginResult.error();
        }

        // Prepare cached statement for updates
        auto updateStmtResult = db.prepareCached("UPDATE documents SET mime_type = ? WHERE id = ?");
        if (!updateStmtResult) {
            db.execute("ROLLBACK");
            return updateStmtResult.error();
        }
        auto& updateStmt = *updateStmtResult.value();

        size_t updatedCount = 0;

        for (const auto& [id, mimeType] : idMimePairs) {
            if (auto r = updateStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = updateStmt.bind(1, mimeType); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = updateStmt.bind(2, id); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto execRes = updateStmt.execute(); !execRes) {
                db.execute("ROLLBACK");
                return execRes.error();
            }

            if (db.changes() > 0) {
                updatedCount++;
            }
        }

        // Commit transaction
        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
            db.execute("ROLLBACK");
            return commitResult.error();
        }

        return updatedCount;
    });
}
} // namespace yams::metadata
