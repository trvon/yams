// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <atomic>
#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>

#include "crud_ops.hpp"
#include "metadata_write_helpers.hpp"
#include "result_helpers.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransactionWithRetry;
using repository::commitOrRollback;
using repository::MetadataTagDelta;
using repository::rollbackIgnoringErrors;
using repository::scope_exit;

namespace {
constexpr std::size_t kMaxMetadataBatchQueryIds = 900;

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
} // namespace

// Metadata operations
Result<void> MetadataRepository::setMetadata(int64_t documentId, const std::string& key,
                                             const MetadataValue& value) {
    std::vector<repository::MetadataWriteEntry> entries;
    entries.emplace_back(documentId, key, value);
    const auto pendingTagKeysByDoc = repository::collectPendingTagKeysByDoc(entries);

    auto result = executeQuery<MetadataTagDelta>([&](Database& db) -> Result<MetadataTagDelta> {
        YAMS_TRY(beginTransactionWithRetry(db));
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        YAMS_TRY_UNWRAP(
            delta, repository::upsertMetadataWritesWithTagDelta(db, entries, pendingTagKeysByDoc));
        YAMS_TRY(commitOrRollback(db));
        committed = true;
        return delta;
    });

    if (!result) {
        return result.error();
    }

    signalCorpusStatsStale();
    applyMetadataTagDelta(cachedTagCount_, cachedDocsWithTags_, cachedDocumentCount_,
                          result.value());
    metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    return {};
}

Result<void> MetadataRepository::setMetadataBatch(
    const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::setMetadataBatch");
    YAMS_PLOT("metadata_repo::set_metadata_batch_entries", static_cast<int64_t>(entries.size()));
    if (entries.empty()) {
        return Result<void>();
    }

    const bool traceMetadataBatch = metadata_trace_enabled();
    const auto overallStart = std::chrono::steady_clock::now();
    auto phaseStart = overallStart;
    const auto dedupedEntries = repository::deduplicateMetadataWrites(entries);
    const auto dedupUs = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - phaseStart)
                             .count();
    phaseStart = std::chrono::steady_clock::now();
    const auto pendingTagKeysByDoc = repository::collectPendingTagKeysByDoc(dedupedEntries);
    const auto collectTagsUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::steady_clock::now() - phaseStart)
                                   .count();

    int64_t beginUs = 0;
    int64_t upsertUs = 0;
    int64_t commitUs = 0;
    const auto executeStart = std::chrono::steady_clock::now();
    auto result = executeQuery<MetadataTagDelta>([&](Database& db) -> Result<MetadataTagDelta> {
        auto txnPhaseStart = std::chrono::steady_clock::now();
        YAMS_TRY(repository::beginTransaction(db));
        beginUs = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - txnPhaseStart)
                      .count();
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                repository::rollbackIgnoringErrors(db);
            }
        });

        txnPhaseStart = std::chrono::steady_clock::now();
        YAMS_TRY_UNWRAP(delta, repository::upsertMetadataWritesWithTagDelta(db, dedupedEntries,
                                                                            pendingTagKeysByDoc));
        upsertUs = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - txnPhaseStart)
                       .count();
        txnPhaseStart = std::chrono::steady_clock::now();
        YAMS_TRY(repository::commitOrRollback(db));
        commitUs = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - txnPhaseStart)
                       .count();
        committed = true;
        return delta;
    });
    const auto executeUs = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::steady_clock::now() - executeStart)
                               .count();

    if (!result) {
        return result.error();
    }

    signalCorpusStatsStale();
    applyMetadataTagDelta(cachedTagCount_, cachedDocsWithTags_, cachedDocumentCount_,
                          result.value());
    metadataChangeCounter_.fetch_add(dedupedEntries.size(), std::memory_order_release);
    if (traceMetadataBatch) {
        const auto totalUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::steady_clock::now() - overallStart)
                                 .count();
        spdlog::info("MetadataRepository::setMetadataBatch phases entries={} deduped={} tags={} "
                     "dedup_us={} collect_tags_us={} execute_us={} begin_us={} upsert_us={} "
                     "commit_us={} total_us={}",
                     entries.size(), dedupedEntries.size(), pendingTagKeysByDoc.size(), dedupUs,
                     collectTagsUs, executeUs, beginUs, upsertUs, commitUs, totalUs);
    }
    return {};
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
            std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>> result;

            for (std::size_t offset = 0; offset < documentIds.size();
                 offset += kMaxMetadataBatchQueryIds) {
                const auto chunkSize =
                    std::min(kMaxMetadataBatchQueryIds, documentIds.size() - offset);
                const auto chunkIds = documentIds.subspan(offset, chunkSize);

                std::string inList;
                inList.reserve(chunkIds.size() * 2);
                inList += '(';
                for (std::size_t i = 0; i < chunkIds.size(); ++i) {
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
                for (auto id : chunkIds) {
                    YAMS_TRY(stmt.bind(index++, id));
                }

                while (true) {
                    YAMS_TRY_UNWRAP(hasRow, stmt.step());
                    if (!hasRow)
                        break;

                    const int64_t docId = stmt.getInt64(0);
                    MetadataValue value;
                    value.value = stmt.getString(2);
                    value.type = MetadataValueTypeUtils::fromString(stmt.getString(3));
                    result[docId][stmt.getString(1)] = value;
                }
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
    auto result = executeQuery<MetadataTagDelta>([&](Database& db) -> Result<MetadataTagDelta> {
        YAMS_TRY(beginTransactionWithRetry(db));
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        YAMS_TRY_UNWRAP(delta, repository::calculateMetadataTagDeltaForDelete(db, documentId, key));

        repository::CrudOps<repository::MetadataEntry> ops;
        YAMS_TRY_UNWRAP(deletedRows,
                        ops.deleteWhere(db, "document_id = ? AND key = ?", documentId, key));
        if (deletedRows == 0) {
            YAMS_TRY(commitOrRollback(db));
            committed = true;
            return MetadataTagDelta{};
        }

        YAMS_TRY(commitOrRollback(db));
        committed = true;
        return delta;
    });

    if (!result) {
        return result.error();
    }

    signalCorpusStatsStale();
    applyMetadataTagDelta(cachedTagCount_, cachedDocsWithTags_, cachedDocumentCount_,
                          result.value());
    metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    return {};
}
} // namespace yams::metadata
