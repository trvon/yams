// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <cstdint>
#include <optional>

#include <yams/common/utf8_utils.h>
#include <yams/core/atomic_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

#include "crud_ops.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {
namespace {

// Observe the trigger's readiness change under the same write transaction as the
// mutation. Identical writes and rollbacks must not decrement the advisory counter.
template <typename Mutation>
Result<bool> mutateContent(Database& db, int64_t documentId, Mutation&& mutation) {
    YAMS_TRY(repository::beginTransaction(db));
    auto rollback = repository::scope_exit([&] { repository::rollbackIgnoringErrors(db); });
    const auto ready = [&]() -> Result<bool> {
        YAMS_TRY_UNWRAP(
            stmt,
            db.prepare(
                "SELECT has_embedding FROM document_embeddings_status WHERE document_id = ?"));
        YAMS_TRY(stmt.bind(1, documentId));
        YAMS_TRY_UNWRAP(found, stmt.step());
        return found && stmt.getInt(0) != 0;
    };
    YAMS_TRY_UNWRAP(wasReady, ready());
    YAMS_TRY(mutation());
    YAMS_TRY_UNWRAP(isReady, ready());
    YAMS_TRY(repository::commitOrRollback(db));
    rollback.dismiss();
    return wasReady && !isReady;
}

} // namespace

// Content operations
Result<void> MetadataRepository::insertContent(const DocumentContent& content) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::insertContent");
    YAMS_PLOT("metadata_repo::insert_content_bytes",
              static_cast<int64_t>(content.contentText.size()));
    auto result = executeWriteQuery<bool>([&](Database& db) -> Result<bool> {
        DocumentContent sanitized = content;
        sanitized.contentText = common::sanitizeUtf8(content.contentText);
        sanitized.contentLength = static_cast<int64_t>(sanitized.contentText.length());
        return mutateContent(db, content.documentId, [&]() {
            repository::CrudOps<DocumentContent> ops;
            return ops.upsertOnConflict(db, sanitized, "document_id");
        });
    });
    if (!result) {
        return result.error();
    }
    if (result.value()) {
        core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
        signalCorpusStatsStale();
    }
    return {};
}

Result<std::optional<DocumentContent>> MetadataRepository::getContent(int64_t documentId) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getContent");
    auto result = executeReadQuery<std::optional<DocumentContent>>(
        [&](Database& db) -> Result<std::optional<DocumentContent>> {
            repository::CrudOps<DocumentContent> ops;
            return ops.getById(db, documentId);
        });
    if (result && result.value().has_value()) {
        YAMS_PLOT("metadata_repo::get_content_bytes",
                  static_cast<int64_t>(result.value()->contentText.size()));
    }
    return result;
}

Result<std::unordered_set<int64_t>>
MetadataRepository::batchHasContent(std::span<const int64_t> documentIds) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::batchHasContent");
    if (documentIds.empty()) {
        return std::unordered_set<int64_t>{};
    }
    return executeReadQuery<std::unordered_set<int64_t>>(
        [&](Database& db) -> Result<std::unordered_set<int64_t>> {
            std::unordered_set<int64_t> present;
            present.reserve(documentIds.size());
            constexpr std::size_t kChunkSize = 500;
            for (std::size_t offset = 0; offset < documentIds.size(); offset += kChunkSize) {
                const auto chunkSize = std::min(kChunkSize, documentIds.size() - offset);
                std::string sql = "SELECT document_id FROM document_content WHERE document_id IN (";
                sql.reserve(sql.size() + chunkSize * 2 + 2);
                for (std::size_t i = 0; i < chunkSize; ++i) {
                    if (i > 0) {
                        sql += ',';
                    }
                    sql += '?';
                }
                sql += ')';
                YAMS_TRY_UNWRAP(stmt, db.prepare(sql));
                for (std::size_t i = 0; i < chunkSize; ++i) {
                    YAMS_TRY(stmt.bind(static_cast<int>(i + 1), documentIds[offset + i]));
                }
                while (true) {
                    YAMS_TRY_UNWRAP(hasRow, stmt.step());
                    if (!hasRow) {
                        break;
                    }
                    present.insert(stmt.getInt64(0));
                }
            }
            return present;
        });
}

Result<void> MetadataRepository::updateContent(const DocumentContent& content) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::updateContent");
    YAMS_PLOT("metadata_repo::update_content_bytes",
              static_cast<int64_t>(content.contentText.size()));
    auto result = executeWriteQuery<bool>([&](Database& db) -> Result<bool> {
        DocumentContent sanitized = content;
        sanitized.contentText = common::sanitizeUtf8(content.contentText);
        sanitized.contentLength = static_cast<int64_t>(sanitized.contentText.length());
        return mutateContent(db, content.documentId, [&]() {
            repository::CrudOps<DocumentContent> ops;
            return ops.update(db, sanitized);
        });
    });
    if (!result) {
        return result.error();
    }
    if (result.value()) {
        core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
        signalCorpusStatsStale();
    }
    return {};
}

Result<void> MetadataRepository::deleteContent(int64_t documentId) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::deleteContent");
    auto result = executeWriteQuery<bool>([&](Database& db) -> Result<bool> {
        return mutateContent(db, documentId, [&]() {
            repository::CrudOps<DocumentContent> ops;
            return ops.deleteById(db, documentId);
        });
    });
    if (!result) {
        return result.error();
    }
    if (result.value()) {
        core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
        signalCorpusStatsStale();
    }
    return {};
}
} // namespace yams::metadata
