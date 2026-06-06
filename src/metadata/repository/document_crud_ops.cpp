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

using repository::beginTransactionWithRetry;
using repository::classifyExtensionBucket;
using repository::ExtensionBucket;
using repository::MetadataTagDelta;
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
    MetadataTagDelta metadataTagDelta{};
};
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
            auto currentDepthMax = cachedPathDepthMax_.load(std::memory_order_relaxed);
            const auto nextDepth = static_cast<uint64_t>(std::max(info.pathDepth, 0));
            while (nextDepth > currentDepthMax &&
                   !cachedPathDepthMax_.compare_exchange_weak(currentDepthMax, nextDepth,
                                                              std::memory_order_acq_rel,
                                                              std::memory_order_relaxed)) {
            }
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
    auto result = executeQuery<InsertDocumentWithMetadataResult>(
        [&](Database& db) -> Result<InsertDocumentWithMetadataResult> {
            // Wrap everything in a single BEGIN IMMEDIATE to reduce lock acquisitions
            // from ~15-20 per document down to 1.
            YAMS_TRY(beginTransactionWithRetry(db));
            auto rollback = scope_exit([&] { db.execute("ROLLBACK"); });

            // --- 1. INSERT document ---
            std::string sql =
                "INSERT OR IGNORE INTO documents (file_path, file_name, file_extension, "
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

            if (hasPathIndexing_) {
                auto bindResult =
                    stmt.bindAll(info.filePath, info.fileName, info.fileExtension, info.fileSize,
                                 info.sha256Hash, info.mimeType, info.createdTime,
                                 info.modifiedTime, info.indexedTime, info.contentExtracted ? 1 : 0,
                                 ExtractionStatusUtils::toString(info.extractionStatus),
                                 info.extractionError, info.pathPrefix, info.reversePath,
                                 info.pathHash, info.parentHash, info.pathDepth);
                if (!bindResult)
                    return bindResult.error();
            } else {
                auto bindResult = stmt.bindAll(
                    info.filePath, info.fileName, info.fileExtension, info.fileSize,
                    info.sha256Hash, info.mimeType, info.createdTime, info.modifiedTime,
                    info.indexedTime, info.contentExtracted ? 1 : 0,
                    ExtractionStatusUtils::toString(info.extractionStatus), info.extractionError);
                if (!bindResult)
                    return bindResult.error();
            }

            auto execResult = stmt.execute();
            if (!execResult)
                return execResult.error();

            int changes = db.changes();
            int64_t docId;
            bool insertedNewDocument = false;

            if (changes > 0) {
                docId = db.lastInsertRowId();
                insertedNewDocument = true;
                spdlog::debug("insertDocumentWithMetadata: inserted hash={} id={}", info.sha256Hash,
                              docId);
            } else {
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
                                 "Document insert ignored but existing record not found"};
                docId = stmt2.getInt64(0);
                spdlog::debug("insertDocumentWithMetadata: existing hash={} id={}", info.sha256Hash,
                              docId);
            }

            MetadataTagDelta metadataTagDelta{};
            uint64_t metadataWriteCount = 0;

            // --- 2. Batch upsert metadata (if any) ---
            if (!tags.empty()) {
                std::vector<repository::MetadataWriteEntry> metadataWrites;
                metadataWrites.reserve(tags.size());
                for (const auto& [key, value] : tags) {
                    metadataWrites.emplace_back(docId, key, value);
                }

                const auto dedupedMetadataWrites =
                    repository::deduplicateMetadataWrites(metadataWrites);
                YAMS_TRY_UNWRAP(
                    delta, repository::upsertMetadataWritesWithTagDelta(db, dedupedMetadataWrites));
                metadataTagDelta = delta;
                metadataWriteCount = dedupedMetadataWrites.size();
            }

            // --- 3. Upsert tree snapshot (if provided) ---
            if (snapshot) {
                snapshot->ingestDocumentId = docId;

                std::string directoryPath = snapshot->metadata.count("directory_path")
                                                ? snapshot->metadata.at("directory_path")
                                                : "";
                std::string snapshotLabel = snapshot->metadata.count("snapshot_label")
                                                ? snapshot->metadata.at("snapshot_label")
                                                : "";
                std::string gitCommit = snapshot->metadata.count("git_commit")
                                            ? snapshot->metadata.at("git_commit")
                                            : "";
                std::string gitBranch = snapshot->metadata.count("git_branch")
                                            ? snapshot->metadata.at("git_branch")
                                            : "";
                std::string gitRemote;
                if (auto it = snapshot->metadata.find("git_remote");
                    it != snapshot->metadata.end()) {
                    gitRemote.append(it->second);
                }

                auto snapStmtResult = db.prepare(R"(
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
            )");
                if (!snapStmtResult)
                    return snapStmtResult.error();

                Statement snapStmt = std::move(snapStmtResult).value();
                snapStmt.bind(1, snapshot->snapshotId);
                snapStmt.bind(2, static_cast<int64_t>(snapshot->createdTime));
                snapStmt.bind(3, directoryPath);
                if (snapshot->rootTreeHash.empty())
                    snapStmt.bind(4, nullptr);
                else
                    snapStmt.bind(4, snapshot->rootTreeHash);
                if (snapshotLabel.empty())
                    snapStmt.bind(5, nullptr);
                else
                    snapStmt.bind(5, snapshotLabel);
                if (gitCommit.empty())
                    snapStmt.bind(6, nullptr);
                else
                    snapStmt.bind(6, gitCommit);
                if (gitBranch.empty())
                    snapStmt.bind(7, nullptr);
                else
                    snapStmt.bind(7, gitBranch);
                if (gitRemote.empty())
                    snapStmt.bind(8, nullptr);
                else
                    snapStmt.bind(8, gitRemote);
                snapStmt.bind(9, static_cast<int64_t>(snapshot->fileCount));

                auto snapExecResult = snapStmt.execute();
                if (!snapExecResult)
                    return snapExecResult.error();
            }

            // --- COMMIT ---
            YAMS_TRY(db.execute("COMMIT"));
            rollback.dismiss();

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
        auto currentDepthMax = cachedPathDepthMax_.load(std::memory_order_relaxed);
        const auto nextDepth = static_cast<uint64_t>(std::max(info.pathDepth, 0));
        while (nextDepth > currentDepthMax &&
               !cachedPathDepthMax_.compare_exchange_weak(currentDepthMax, nextDepth,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_relaxed)) {
        }
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
    return executeQuery<void>([&](Database& db) -> Result<void> {
        int64_t priorFileSize = info.fileSize;
        std::string priorExtension = info.fileExtension;
        int priorPathDepth = info.pathDepth;
        bool priorContentExtracted = info.contentExtracted;

        auto priorStmt = db.prepareCached(
            "SELECT file_size, file_extension, path_depth, content_extracted FROM documents "
            "WHERE id = ?");
        if (priorStmt) {
            auto& stmt = *priorStmt.value();
            YAMS_TRY(stmt.bind(1, info.id));
            if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                priorFileSize = stmt.getInt64(0);
                priorExtension = stmt.getString(1);
                priorPathDepth = stmt.getInt(2);
                priorContentExtracted = stmt.getInt(3) != 0;
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
            saturatingSubBytes(cachedPathDepthSum_,
                               static_cast<uint64_t>(std::max(priorPathDepth, 0)));
            cachedPathDepthSum_.fetch_add(static_cast<uint64_t>(std::max(info.pathDepth, 0)),
                                          std::memory_order_relaxed);
            auto currentDepthMax = cachedPathDepthMax_.load(std::memory_order_relaxed);
            const auto nextDepth = static_cast<uint64_t>(std::max(info.pathDepth, 0));
            while (nextDepth > currentDepthMax &&
                   !cachedPathDepthMax_.compare_exchange_weak(currentDepthMax, nextDepth,
                                                              std::memory_order_acq_rel,
                                                              std::memory_order_relaxed)) {
            }
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
            signalCorpusStatsStale();
        }
        return Result<void>();
    });
}

Result<void> MetadataRepository::deleteDocument(int64_t id) {
    auto result = executeQuery<DeleteDocumentResult>([&](Database& db) -> Result<DeleteDocumentResult> {
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
                       COALESCE(des.has_embedding, 0)
                FROM documents d
                LEFT JOIN document_embeddings_status des ON des.document_id = d.id
                WHERE d.id = ?
            )");
            if (checkStmt) {
                auto& stmt = *checkStmt.value();
                stmt.bind(1, id);
                if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                    wasExtracted = stmt.getInt(0) != 0;
                    priorFileSize = static_cast<uint64_t>(std::max<int64_t>(stmt.getInt64(1), 0));
                    priorExtension = stmt.getString(2);
                    priorPathDepth = stmt.getInt(3);
                    wasIndexed = stmt.getInt(4) != 0;
                    wasEmbedded = stmt.getInt(5) != 0;
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
            deleteResult.deleted = true;
            deleteResult.totalSizeBytesRemoved = priorFileSize;
            deleteResult.pathDepthSumRemoved = static_cast<uint64_t>(std::max(priorPathDepth, 0));
            deleteResult.extractedRemoved = wasExtracted;
            deleteResult.indexedRemoved = wasIndexed;
            deleteResult.embeddedRemoved = wasEmbedded;
            deleteResult.removedExtension = std::move(priorExtension);
            deleteResult.metadataTagDelta = metadataTagDelta;
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

    YAMS_DCHECK(cachedIndexedCount_.load() <= cachedDocumentCount_.load(),
                "metadata: indexed count must not exceed total document count after delete");
    YAMS_DCHECK(cachedExtractedCount_.load() <= cachedDocumentCount_.load(),
                "metadata: extracted count must not exceed total document count after delete");
    YAMS_DCHECK(cachedEmbeddedCount_.load() <= cachedDocumentCount_.load(),
                "metadata: embedded count must not exceed total document count after delete");

    // Signal enumeration cache invalidation (document deletion cascades to metadata)
    metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    return Result<void>();
}

Result<size_t> MetadataRepository::deleteDocumentsBatch(const std::vector<int64_t>& ids) {
    if (ids.empty()) {
        return size_t{0};
    }

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
                   COALESCE(des.has_embedding, 0)
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
                    batchResult.deletedCount++;
                    batchResult.totalSizeBytesRemoved += priorFileSize;
                    batchResult.pathDepthSumRemoved +=
                        static_cast<uint64_t>(std::max(priorPathDepth, 0));
                    batchResult.metadataTagDelta.tagCountDelta += metadataTagDelta.tagCountDelta;
                    batchResult.metadataTagDelta.docsWithTagsDelta +=
                        metadataTagDelta.docsWithTagsDelta;
                    if (!priorExtension.empty()) {
                        batchResult.removedExtensionCounts[priorExtension] += 1;
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
                }
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

        YAMS_DCHECK(cachedIndexedCount_.load() <= cachedDocumentCount_.load(),
                    "metadata: indexed count must not exceed total document count after batch "
                    "delete");
        YAMS_DCHECK(cachedExtractedCount_.load() <= cachedDocumentCount_.load(),
                    "metadata: extracted count must not exceed total document count after batch "
                    "delete");
        YAMS_DCHECK(cachedEmbeddedCount_.load() <= cachedDocumentCount_.load(),
                    "metadata: embedded count must not exceed total document count after batch "
                    "delete");
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
