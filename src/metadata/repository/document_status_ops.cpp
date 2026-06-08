// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/sqlite_retry.h>

#include "crud_ops.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransaction;
using repository::beginTransactionWithRetry;
using repository::commitOrRollback;
using repository::rollbackIgnoringErrors;
using repository::scope_exit;

Result<int64_t> MetadataRepository::getEmbeddedDocumentCount() {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult =
            db.prepare("SELECT COUNT(*) FROM document_embeddings_status WHERE has_embedding = 1");
        if (!stmtResult)
            return stmtResult.error();
        auto& stmt = stmtResult.value();
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        return stepResult.value() ? stmt.getInt64(0) : int64_t{0};
    });
}

Result<int64_t> MetadataRepository::getDocumentCountByExtractionStatus(ExtractionStatus status) {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<DocumentInfo> ops;
        return ops.count(db, "extraction_status = ?", ExtractionStatusUtils::toString(status));
    });
}

Result<void> MetadataRepository::updateDocumentEmbeddingStatus(int64_t documentId,
                                                               bool hasEmbedding,
                                                               const std::string& modelId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Check current embedding status to track changes
        bool hadEmbedding = false;
        {
            auto checkStmt = db.prepare("SELECT COALESCE(has_embedding, 0) FROM "
                                        "document_embeddings_status WHERE document_id = ?");
            if (checkStmt) {
                auto& stmt = checkStmt.value();
                stmt.bind(1, documentId);
                if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                    hadEmbedding = stmt.getInt(0) != 0;
                }
            }
        }

        // Ensure the document exists
        auto docCheckStmt = db.prepare("SELECT 1 FROM documents WHERE id = ?");
        if (!docCheckStmt)
            return docCheckStmt.error();

        auto& stmt = docCheckStmt.value();
        if (auto r = stmt.bind(1, documentId); !r)
            return r.error();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value()) {
            return Error{ErrorCode::NotFound, "Document not found"};
        }

        // Ensure model_id exists in vector_models (FK constraint)
        if (!modelId.empty()) {
            auto ensureModelStmt = db.prepare(R"(
                INSERT OR IGNORE INTO vector_models (model_id, model_name, embedding_dim)
                VALUES (?, ?, 0)
            )");
            if (ensureModelStmt) {
                auto& mstmt = ensureModelStmt.value();
                mstmt.bind(1, modelId);
                mstmt.bind(2, modelId); // Use model_id as name if not registered
                (void)mstmt.execute();
            }
        }

        // Insert or update the embedding status
        auto updateStmt = db.prepare(R"(
                INSERT INTO document_embeddings_status (document_id, has_embedding, model_id, updated_at)
                VALUES (?, ?, ?, unixepoch())
                ON CONFLICT(document_id) DO UPDATE SET
                    has_embedding = excluded.has_embedding,
                    model_id = excluded.model_id,
                    updated_at = excluded.updated_at
            )");
        if (!updateStmt)
            return updateStmt.error();

        auto& ustmt = updateStmt.value();
        if (auto r = ustmt.bind(1, documentId); !r)
            return r.error();
        if (auto r = ustmt.bind(2, hasEmbedding ? 1 : 0); !r)
            return r.error();
        if (auto r = ustmt.bind(3, modelId.empty() ? nullptr : modelId.c_str()); !r)
            return r.error();

        auto execResult = ustmt.execute();
        if (!execResult)
            return execResult.error();

        // Note: cachedIndexedCount_ now tracks actual FTS5 rows, not extraction success or
        // embedding status. Embedding status is tracked separately via
        // VectorDatabase::getVectorCount()

        // Update component-owned embedded-doc counter only on state transition.
        if (!hadEmbedding && hasEmbedding) {
            cachedEmbeddedCount_.fetch_add(1, std::memory_order_relaxed);
            signalCorpusStatsStale();
        } else if (hadEmbedding && !hasEmbedding) {
            core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
            signalCorpusStatsStale();
        }

        YAMS_DCHECK(cachedEmbeddedCount_.load(std::memory_order_relaxed) <=
                        cachedDocumentCount_.load(std::memory_order_relaxed),
                    "metadata: embedded count must not exceed total document count after "
                    "embedding status update");
        return Result<void>();
    });
}

Result<void> MetadataRepository::updateDocumentEmbeddingStatusByHash(const std::string& hash,
                                                                     bool hasEmbedding,
                                                                     const std::string& modelId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Get document ID and previous embedding status in one query.
        auto getIdStmt = db.prepare(R"(
            SELECT d.id, COALESCE(des.has_embedding, 0)
            FROM documents d
            LEFT JOIN document_embeddings_status des ON d.id = des.document_id
            WHERE d.sha256_hash = ?
        )");
        if (!getIdStmt)
            return getIdStmt.error();

        auto& stmt = getIdStmt.value();
        if (auto r = stmt.bind(1, hash); !r)
            return r.error();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value()) {
            return Error{ErrorCode::NotFound, "Document with hash not found"};
        }

        int64_t documentId = stmt.getInt64(0);
        bool hadEmbedding = stmt.getInt(1) != 0;

        // Ensure model_id exists in vector_models (FK constraint)
        if (!modelId.empty()) {
            auto ensureModelStmt = db.prepare(R"(
                INSERT OR IGNORE INTO vector_models (model_id, model_name, embedding_dim)
                VALUES (?, ?, 0)
            )");
            if (ensureModelStmt) {
                auto& mstmt = ensureModelStmt.value();
                mstmt.bind(1, modelId);
                mstmt.bind(2, modelId); // Use model_id as name if not registered
                (void)mstmt.execute();
            }
        }

        // Insert or update the embedding status
        auto updateStmt = db.prepare(R"(
                INSERT INTO document_embeddings_status (document_id, has_embedding, model_id, updated_at)
                VALUES (?, ?, ?, unixepoch())
                ON CONFLICT(document_id) DO UPDATE SET
                    has_embedding = excluded.has_embedding,
                    model_id = excluded.model_id,
                    updated_at = excluded.updated_at
            )");
        if (!updateStmt)
            return updateStmt.error();

        auto& ustmt = updateStmt.value();
        if (auto r = ustmt.bind(1, documentId); !r)
            return r.error();
        if (auto r = ustmt.bind(2, hasEmbedding ? 1 : 0); !r)
            return r.error();
        if (auto r = ustmt.bind(3, modelId.empty() ? nullptr : modelId.c_str()); !r)
            return r.error();

        auto execResult = ustmt.execute();
        if (!execResult)
            return execResult.error();

        // Update component-owned embedded-doc counter only on state transition.
        if (!hadEmbedding && hasEmbedding) {
            cachedEmbeddedCount_.fetch_add(1, std::memory_order_relaxed);
            signalCorpusStatsStale();
        } else if (hadEmbedding && !hasEmbedding) {
            core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
            signalCorpusStatsStale();
        }

        YAMS_DCHECK(cachedEmbeddedCount_.load(std::memory_order_relaxed) <=
                        cachedDocumentCount_.load(std::memory_order_relaxed),
                    "metadata: embedded count must not exceed total document count after "
                    "embedding hash update");
        return Result<void>();
    });
}

Result<void> MetadataRepository::batchUpdateDocumentEmbeddingStatusByHashes(
    const std::vector<std::string>& hashes, bool hasEmbedding, const std::string& modelId) {
    if (hashes.empty())
        return Result<void>();

    constexpr int kMaxRetries = 7; // Increased for heavy concurrent load
    constexpr int kBaseDelayMs = 50;

    // Thread-local RNG for jitter to avoid thundering herd
    thread_local std::mt19937 rng(std::random_device{}());

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        auto result = executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
            YAMS_TRY(beginTransactionWithRetry(db));
            auto rollback = scope_exit([&] { rollbackIgnoringErrors(db); });

            // Ensure model_id exists in vector_models (FK constraint) - once per batch
            if (!modelId.empty()) {
                auto ensureModelStmt = db.prepare(R"(
                    INSERT OR IGNORE INTO vector_models (model_id, model_name, embedding_dim)
                    VALUES (?, ?, 0)
                )");
                if (ensureModelStmt) {
                    auto& mstmt = ensureModelStmt.value();
                    mstmt.bind(1, modelId);
                    mstmt.bind(2, modelId); // Use model_id as name if not registered
                    (void)mstmt.execute();
                }
            }

            auto lookupStmt = db.prepare(R"(
                SELECT d.id, COALESCE(des.has_embedding, 0)
                FROM documents d
                LEFT JOIN document_embeddings_status des ON d.id = des.document_id
                WHERE d.sha256_hash = ?
            )");
            if (!lookupStmt) {
                return lookupStmt.error();
            }

            auto updateStmt = db.prepare(R"(
                INSERT INTO document_embeddings_status (document_id, has_embedding, model_id, updated_at)
                VALUES (?, ?, ?, unixepoch())
                ON CONFLICT(document_id) DO UPDATE SET
                    has_embedding = excluded.has_embedding,
                    model_id = excluded.model_id,
                    updated_at = excluded.updated_at
            )");
            if (!updateStmt) {
                return updateStmt.error();
            }

            auto& lstmt = lookupStmt.value();
            auto& ustmt = updateStmt.value();
            int64_t embeddedDelta = 0;

            for (const auto& hash : hashes) {
                lstmt.reset();
                if (auto r = lstmt.bind(1, hash); !r) {
                    return r.error();
                }

                auto stepResult = lstmt.step();
                if (!stepResult) {
                    return stepResult.error();
                }
                if (!stepResult.value())
                    continue;

                int64_t documentId = lstmt.getInt64(0);
                bool hadEmbedding = lstmt.getInt(1) != 0;

                ustmt.reset();
                if (auto r = ustmt.bind(1, documentId); !r) {
                    return r.error();
                }
                if (auto r = ustmt.bind(2, hasEmbedding ? 1 : 0); !r) {
                    return r.error();
                }
                if (auto r = ustmt.bind(3, modelId.empty() ? nullptr : modelId.c_str()); !r) {
                    return r.error();
                }

                auto execResult = ustmt.execute();
                if (!execResult) {
                    return execResult.error();
                }

                if (!hadEmbedding && hasEmbedding) {
                    ++embeddedDelta;
                } else if (hadEmbedding && !hasEmbedding) {
                    --embeddedDelta;
                }
            }

            YAMS_TRY(commitOrRollback(db));
            rollback.dismiss();
            return embeddedDelta;
        });

        if (result) {
            if (result.value() > 0) {
                cachedEmbeddedCount_.fetch_add(static_cast<uint64_t>(result.value()),
                                               std::memory_order_relaxed);
            } else if (result.value() < 0) {
                core::saturating_sub(cachedEmbeddedCount_, static_cast<uint64_t>(-result.value()));
            }
            signalCorpusStatsStale();
            YAMS_DCHECK(cachedEmbeddedCount_.load(std::memory_order_relaxed) <=
                            cachedDocumentCount_.load(std::memory_order_relaxed),
                        "metadata: embedded count must not exceed total document count after "
                        "embedding batch update");
            return Result<void>();
        }

        if (!storage::sqlite_retry::isBusyOrLockedMessage(result.error().message))
            return result.error();

        // Exponential backoff with jitter (±25%) to prevent thundering herd
        int baseDelayMs = kBaseDelayMs * (1 << attempt);
        int jitter = static_cast<int>(baseDelayMs * 0.25);
        std::uniform_int_distribution<int> dist(-jitter, jitter);
        int delayMs = baseDelayMs + dist(rng);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }

    daemon::TuneAdvisor::reportDbLockError();
    return Error{ErrorCode::DatabaseError,
                 "batchUpdateDocumentEmbeddingStatusByHashes: max retries exceeded"};
}

Result<void> MetadataRepository::reconcileDocumentEmbeddingStatusByHashes(
    const std::vector<std::string>& embeddedHashes, const std::string& modelId) {
    constexpr int kMaxRetries = 5;
    constexpr int kBaseDelayMs = 50;

    std::vector<std::string> uniqueHashes = embeddedHashes;
    std::sort(uniqueHashes.begin(), uniqueHashes.end());
    uniqueHashes.erase(std::unique(uniqueHashes.begin(), uniqueHashes.end()), uniqueHashes.end());

    thread_local std::mt19937 rng(std::random_device{}());

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        auto result = executeQuery<std::size_t>([&](Database& db) -> Result<std::size_t> {
            YAMS_TRY(beginTransactionWithRetry(db));
            auto rollback = scope_exit([&] { rollbackIgnoringErrors(db); });

            if (auto createTemp =
                    db.execute("CREATE TEMP TABLE IF NOT EXISTS temp_embedding_reconcile_hashes ("
                               "sha256_hash TEXT PRIMARY KEY)");
                !createTemp) {
                return createTemp.error();
            }
            if (auto clearTemp = db.execute("DELETE FROM temp_embedding_reconcile_hashes");
                !clearTemp) {
                return clearTemp.error();
            }

            if (!modelId.empty()) {
                auto ensureModelStmt = db.prepare(R"(
                    INSERT OR IGNORE INTO vector_models (model_id, model_name, embedding_dim)
                    VALUES (?, ?, 0)
                )");
                if (!ensureModelStmt) {
                    return ensureModelStmt.error();
                }
                auto& mstmt = ensureModelStmt.value();
                if (auto r = mstmt.bind(1, modelId); !r) {
                    return r.error();
                }
                if (auto r = mstmt.bind(2, modelId); !r) {
                    return r.error();
                }
                if (auto exec = mstmt.execute(); !exec) {
                    return exec.error();
                }
            }

            if (!uniqueHashes.empty()) {
                auto insertHashStmt = db.prepare(
                    "INSERT OR IGNORE INTO temp_embedding_reconcile_hashes (sha256_hash) "
                    "VALUES (?)");
                if (!insertHashStmt) {
                    return insertHashStmt.error();
                }
                auto& hstmt = insertHashStmt.value();
                for (const auto& hash : uniqueHashes) {
                    hstmt.reset();
                    if (auto r = hstmt.bind(1, hash); !r) {
                        return r.error();
                    }
                    if (auto exec = hstmt.execute(); !exec) {
                        return exec.error();
                    }
                }
            }

            std::size_t reconciledEmbeddedDocs = 0;
            auto countStmt = db.prepare(R"(
                SELECT COUNT(*)
                FROM documents d
                JOIN temp_embedding_reconcile_hashes th ON th.sha256_hash = d.sha256_hash
            )");
            if (!countStmt) {
                return countStmt.error();
            }
            if (auto step = countStmt.value().step(); !step) {
                return step.error();
            } else if (step.value()) {
                reconciledEmbeddedDocs = static_cast<std::size_t>(countStmt.value().getInt64(0));
            }

            auto reconcileStmt = db.prepare(R"(
                INSERT INTO document_embeddings_status (
                    document_id, has_embedding, model_id, chunk_count, updated_at
                )
                SELECT d.id,
                       CASE WHEN th.sha256_hash IS NOT NULL THEN 1 ELSE 0 END,
                       CASE WHEN th.sha256_hash IS NOT NULL THEN ? ELSE NULL END,
                       CASE WHEN th.sha256_hash IS NOT NULL THEN 1 ELSE 0 END,
                       unixepoch()
                FROM documents d
                LEFT JOIN temp_embedding_reconcile_hashes th ON th.sha256_hash = d.sha256_hash
                ON CONFLICT(document_id) DO UPDATE SET
                    has_embedding = excluded.has_embedding,
                    model_id = CASE
                        WHEN excluded.has_embedding = 0 THEN NULL
                        WHEN excluded.model_id IS NOT NULL THEN excluded.model_id
                        ELSE document_embeddings_status.model_id
                    END,
                    chunk_count = CASE
                        WHEN excluded.has_embedding = 0 THEN 0
                        WHEN document_embeddings_status.chunk_count > 0
                            THEN document_embeddings_status.chunk_count
                        ELSE excluded.chunk_count
                    END,
                    updated_at = excluded.updated_at
            )");
            if (!reconcileStmt) {
                return reconcileStmt.error();
            }
            auto& rstmt = reconcileStmt.value();
            if (auto r = rstmt.bind(1, modelId.empty() ? nullptr : modelId.c_str()); !r) {
                return r.error();
            }
            if (auto exec = rstmt.execute(); !exec) {
                return exec.error();
            }

            if (auto clearTemp = db.execute("DELETE FROM temp_embedding_reconcile_hashes");
                !clearTemp) {
                return clearTemp.error();
            }

            YAMS_TRY(commitOrRollback(db));
            rollback.dismiss();
            return reconciledEmbeddedDocs;
        });

        if (result) {
            cachedEmbeddedCount_.store(static_cast<uint64_t>(result.value()),
                                       std::memory_order_relaxed);
            signalCorpusStatsStale();
            YAMS_DCHECK(cachedEmbeddedCount_.load(std::memory_order_relaxed) <=
                            cachedDocumentCount_.load(std::memory_order_relaxed),
                        "metadata: embedded count must not exceed total document count after "
                        "embedding reconciliation");
            return Result<void>();
        }

        if (!storage::sqlite_retry::isBusyOrLockedMessage(result.error().message)) {
            return result.error();
        }

        int baseDelayMs = kBaseDelayMs * (1 << attempt);
        int jitter = static_cast<int>(baseDelayMs * 0.25);
        std::uniform_int_distribution<int> dist(-jitter, jitter);
        int delayMs = baseDelayMs + dist(rng);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }

    daemon::TuneAdvisor::reportDbLockError();
    return Error{ErrorCode::DatabaseError,
                 "reconcileDocumentEmbeddingStatusByHashes: max retries exceeded"};
}

Result<bool> MetadataRepository::hasDocumentEmbeddingByHash(const std::string& hash) {
    return executeReadQuery<bool>([&](Database& db) -> Result<bool> {
        auto stmtResult = db.prepare(R"(
            SELECT COALESCE(des.has_embedding, 0)
            FROM documents d
            LEFT JOIN document_embeddings_status des ON d.id = des.document_id
            WHERE d.sha256_hash = ?
        )");
        if (!stmtResult)
            return stmtResult.error();

        auto& stmt = stmtResult.value();
        if (auto r = stmt.bind(1, hash); !r)
            return r.error();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();

        if (!stepResult.value()) {
            // Document not found - treat as no embedding
            return false;
        }

        return stmt.getInt(0) != 0;
    });
}

Result<void> MetadataRepository::updateDocumentExtractionStatus(int64_t documentId,
                                                                bool contentExtracted,
                                                                ExtractionStatus status,
                                                                const std::string& error) {
    YAMS_DCHECK(documentId != 0, "extraction status update should not target document id 0");
    if (documentId < 0) {
        return Result<void>();
    }

    struct ExtractionDelta {
        std::uint64_t newlyExtracted = 0;
        std::uint64_t newlyUnextracted = 0;
        std::uint64_t rowsUpdated = 0;
    };

    const std::string statusStr = ExtractionStatusUtils::toString(status);
    auto result = executeQuery<ExtractionDelta>([&](Database& db) -> Result<ExtractionDelta> {
        // Keep retry ownership at executeQueryOnPool so singleton extraction updates do not
        // stack inner BEGIN retry/backoff with the outer write retry loop.
        YAMS_TRY(beginTransaction(db));
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        ExtractionDelta delta;

        YAMS_TRY_UNWRAP(deltaStmtResult, db.prepareCached(R"(
            SELECT
                COALESCE(SUM(CASE WHEN COALESCE(content_extracted, 0) = 0 AND ? = 1 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN COALESCE(content_extracted, 0) = 1 AND ? = 0 THEN 1 ELSE 0 END), 0),
                COUNT(*)
            FROM documents
            WHERE id = ?
        )"));
        auto& deltaStmt = *deltaStmtResult;
        YAMS_TRY(deltaStmt.reset());
        YAMS_TRY(deltaStmt.clearBindings());
        YAMS_TRY(deltaStmt.bind(1, contentExtracted ? 1 : 0));
        YAMS_TRY(deltaStmt.bind(2, contentExtracted ? 1 : 0));
        YAMS_TRY(deltaStmt.bind(3, documentId));
        YAMS_TRY_UNWRAP(deltaStep, deltaStmt.step());
        YAMS_DCHECK(deltaStep, "singleton extraction delta query should always return one row");
        if (deltaStep) {
            delta.newlyExtracted = static_cast<std::uint64_t>(deltaStmt.getInt64(0));
            delta.newlyUnextracted = static_cast<std::uint64_t>(deltaStmt.getInt64(1));
            delta.rowsUpdated = static_cast<std::uint64_t>(deltaStmt.getInt64(2));
        }

        if (delta.rowsUpdated > 0) {
            YAMS_TRY_UNWRAP(updateStmtResult, db.prepareCached(R"(
                UPDATE documents
                SET content_extracted = ?,
                    extraction_status = ?,
                    extraction_error = ?
                WHERE id = ?
            )"));
            auto& updateStmt = *updateStmtResult;
            YAMS_TRY(updateStmt.reset());
            YAMS_TRY(updateStmt.clearBindings());
            YAMS_TRY(updateStmt.bind(1, contentExtracted ? 1 : 0));
            YAMS_TRY(updateStmt.bind(2, statusStr));
            if (error.empty()) {
                YAMS_TRY(updateStmt.bind(3, nullptr));
            } else {
                YAMS_TRY(updateStmt.bind(3, error));
            }
            YAMS_TRY(updateStmt.bind(4, documentId));
            YAMS_TRY(updateStmt.execute());
        }

        YAMS_TRY(commitOrRollback(db));
        committed = true;
        return delta;
    });

    if (!result) {
        return result.error();
    }

    const auto& delta = result.value();
    if (delta.newlyExtracted > 0) {
        cachedExtractedCount_.fetch_add(delta.newlyExtracted, std::memory_order_relaxed);
    }
    if (delta.newlyUnextracted > 0) {
        core::saturating_sub(cachedExtractedCount_, delta.newlyUnextracted);
    }
    YAMS_DCHECK(cachedExtractedCount_.load(std::memory_order_relaxed) <=
                    cachedDocumentCount_.load(std::memory_order_relaxed),
                "metadata: extracted count must not exceed total document count after "
                "extraction status update");
    if (delta.rowsUpdated > 0) {
        signalCorpusStatsStale();
    }

    return Result<void>();
}

Result<void> MetadataRepository::batchUpdateDocumentExtractionStatuses(
    const std::vector<ExtractionStatusUpdate>& updates) {
    if (updates.empty())
        return Result<void>();

    struct EffectiveExtractionStatusUpdate {
        std::int64_t documentId = 0;
        bool contentExtracted = false;
        std::string status;
        std::string error;
    };
    struct ExtractionDelta {
        std::uint64_t newlyExtracted = 0;
        std::uint64_t newlyUnextracted = 0;
        std::uint64_t rowsUpdated = 0;
    };

    std::vector<EffectiveExtractionStatusUpdate> effectiveUpdates;
    effectiveUpdates.reserve(updates.size());
    std::unordered_map<std::int64_t, std::size_t> updateIndexByDocumentId;
    updateIndexByDocumentId.reserve(updates.size());

    for (const auto& item : updates) {
        YAMS_DCHECK(item.documentId != 0,
                    "extraction status batch should not contain document id 0");
        if (item.documentId < 0) {
            continue;
        }

        const auto it = updateIndexByDocumentId.find(item.documentId);
        if (it == updateIndexByDocumentId.end()) {
            updateIndexByDocumentId.emplace(item.documentId, effectiveUpdates.size());
            effectiveUpdates.push_back(EffectiveExtractionStatusUpdate{
                .documentId = item.documentId,
                .contentExtracted = item.contentExtracted,
                .status = ExtractionStatusUtils::toString(item.status),
                .error = item.error,
            });
            continue;
        }

        auto& effective = effectiveUpdates[it->second];
        effective.contentExtracted = item.contentExtracted;
        effective.status = ExtractionStatusUtils::toString(item.status);
        effective.error = item.error;
    }

    if (effectiveUpdates.empty()) {
        return Result<void>();
    }

    constexpr std::size_t kMaxBatchRows = 200;
    auto makeBatchValuesSql = [](std::size_t rowCount) {
        std::string sql;
        sql.reserve(rowCount * 18);
        for (std::size_t i = 0; i < rowCount; ++i) {
            if (i > 0) {
                sql += ',';
            }
            sql += "(?, ?, ?, ?)";
        }
        return sql;
    };
    auto bindBatchParams =
        [](Statement& stmt,
           std::span<const EffectiveExtractionStatusUpdate> chunk) -> Result<void> {
        int bindIndex = 1;
        for (const auto& item : chunk) {
            YAMS_TRY(stmt.bind(bindIndex++, item.documentId));
            YAMS_TRY(stmt.bind(bindIndex++, item.contentExtracted ? 1 : 0));
            YAMS_TRY(stmt.bind(bindIndex++, item.status));
            if (item.error.empty()) {
                YAMS_TRY(stmt.bind(bindIndex++, nullptr));
            } else {
                YAMS_TRY(stmt.bind(bindIndex++, item.error));
            }
        }
        return Result<void>();
    };

    auto result = executeQuery<ExtractionDelta>([&](Database& db) -> Result<ExtractionDelta> {
        YAMS_TRY(beginTransactionWithRetry(db));
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        ExtractionDelta delta;
        for (std::size_t offset = 0; offset < effectiveUpdates.size(); offset += kMaxBatchRows) {
            const auto remaining = effectiveUpdates.size() - offset;
            const auto chunkSize = std::min(kMaxBatchRows, remaining);
            std::span<const EffectiveExtractionStatusUpdate> chunk{effectiveUpdates.data() + offset,
                                                                   chunkSize};
            const std::string valuesSql = makeBatchValuesSql(chunk.size());

            auto deltaStmtResult =
                db.prepare("WITH batch(document_id, content_extracted, extraction_status, "
                           "extraction_error) AS "
                           "(VALUES " +
                           valuesSql +
                           ") "
                           "SELECT "
                           "COALESCE(SUM(CASE WHEN COALESCE(d.content_extracted, 0) = 0 AND "
                           "b.content_extracted = 1 THEN 1 ELSE 0 END), 0), "
                           "COALESCE(SUM(CASE WHEN COALESCE(d.content_extracted, 0) = 1 AND "
                           "b.content_extracted = 0 THEN 1 ELSE 0 END), 0), "
                           "COUNT(*) "
                           "FROM documents d "
                           "JOIN batch b ON d.id = b.document_id");
            if (!deltaStmtResult) {
                return deltaStmtResult.error();
            }
            Statement deltaStmt = std::move(deltaStmtResult).value();
            YAMS_TRY(bindBatchParams(deltaStmt, chunk));
            auto deltaStep = deltaStmt.step();
            if (!deltaStep) {
                return deltaStep.error();
            }
            YAMS_DCHECK(deltaStep.value(),
                        "extraction status aggregate query should always return one row");
            if (deltaStep.value()) {
                delta.newlyExtracted += static_cast<std::uint64_t>(deltaStmt.getInt64(0));
                delta.newlyUnextracted += static_cast<std::uint64_t>(deltaStmt.getInt64(1));
                delta.rowsUpdated += static_cast<std::uint64_t>(deltaStmt.getInt64(2));
            }

            auto updateStmtResult = db.prepare("WITH batch(document_id, content_extracted, "
                                               "extraction_status, extraction_error) AS "
                                               "(VALUES " +
                                               valuesSql +
                                               ") "
                                               "UPDATE documents "
                                               "SET content_extracted = batch.content_extracted, "
                                               "extraction_status = batch.extraction_status, "
                                               "extraction_error = batch.extraction_error "
                                               "FROM batch "
                                               "WHERE documents.id = batch.document_id");
            if (!updateStmtResult) {
                return updateStmtResult.error();
            }
            Statement updateStmt = std::move(updateStmtResult).value();
            YAMS_TRY(bindBatchParams(updateStmt, chunk));
            YAMS_TRY(updateStmt.execute());
        }

        YAMS_TRY(commitOrRollback(db));
        committed = true;
        return delta;
    });

    if (!result) {
        return result.error();
    }

    const auto& delta = result.value();
    if (delta.newlyExtracted > 0) {
        cachedExtractedCount_.fetch_add(delta.newlyExtracted, std::memory_order_relaxed);
    }
    if (delta.newlyUnextracted > 0) {
        core::saturating_sub(cachedExtractedCount_, delta.newlyUnextracted);
    }
    YAMS_DCHECK(cachedExtractedCount_.load(std::memory_order_relaxed) <=
                    cachedDocumentCount_.load(std::memory_order_relaxed),
                "metadata: extracted count must not exceed total document count after "
                "extraction status update");
    if (delta.rowsUpdated > 0) {
        signalCorpusStatsStale();
    }

    return Result<void>();
}

Result<void> MetadataRepository::updateDocumentRepairStatus(const std::string& hash,
                                                            RepairStatus status) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto updateStmt = db.prepare(R"(
            UPDATE documents
            SET repair_status = ?, repair_attempted_at = unixepoch(), repair_attempts = repair_attempts + 1
            WHERE sha256_hash = ?
        )");
        if (!updateStmt)
            return updateStmt.error();

        auto& stmt = updateStmt.value();
        if (auto r = stmt.bind(1, RepairStatusUtils::toString(status)); !r)
            return r.error();
        if (auto r = stmt.bind(2, hash); !r)
            return r.error();

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        return Result<void>();
    });
}

Result<void>
MetadataRepository::batchUpdateDocumentRepairStatuses(const std::vector<std::string>& hashes,
                                                      RepairStatus status) {
    if (hashes.empty())
        return Result<void>();

    const std::string statusStr = RepairStatusUtils::toString(status);
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        // Keep retry ownership at executeQueryOnPool so one repair batch does not
        // sleep both inside BEGIN retry/backoff and again in the outer write retry loop.
        YAMS_TRY(beginTransaction(db));
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        auto updateStmt = db.prepareCached(R"(
            UPDATE documents
            SET repair_status = ?, repair_attempted_at = unixepoch(), repair_attempts = repair_attempts + 1
            WHERE sha256_hash = ?
        )");
        if (!updateStmt) {
            return updateStmt.error();
        }

        auto& stmt = *updateStmt.value();
        for (const auto& hash : hashes) {
            YAMS_DCHECK(!hash.empty(), "repair status batch should not contain empty hashes");
            if (hash.empty()) {
                continue;
            }
            YAMS_TRY(stmt.reset());
            if (auto r = stmt.bind(1, statusStr); !r) {
                return r.error();
            }
            if (auto r = stmt.bind(2, hash); !r) {
                return r.error();
            }

            auto execResult = stmt.execute();
            if (!execResult) {
                return execResult.error();
            }
        }

        YAMS_TRY(commitOrRollback(db));
        committed = true;
        rollback.dismiss();
        return Result<void>();
    });

    if (!result && storage::sqlite_retry::isBusyOrLockedMessage(result.error().message)) {
        daemon::TuneAdvisor::reportDbLockError();
    }
    return result;
}

} // namespace yams::metadata
