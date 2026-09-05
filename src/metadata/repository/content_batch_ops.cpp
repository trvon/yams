// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <spdlog/spdlog.h>

#include <yams/common/utf8_utils.h>
#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "metadata_write_helpers.hpp"
#include "result_helpers.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransaction;
using repository::rollbackIgnoringErrors;
using repository::scope_exit;

class MetadataRepository::ContentBatchOps {
public:
    struct Delta {
        uint64_t newlyExtracted{0};
        uint64_t newlyIndexed{0};
        uint64_t invalidatedEmbeddings{0};
        uint64_t metadataWrites{0};
        std::vector<int64_t> indexedDocIds;
    };

    struct PreparedEntry {
        int64_t documentId = 0;
        std::string sanitizedTitle;
        std::string sanitizedContent;
        std::string boostedContent;
        std::string extractionMethod;
        std::string language;
        std::string metadataTitle;
    };

    struct PhaseTimings {
        int64_t prepareInputUs{0};
        int64_t transactionUs{0};
        int64_t beginUs{0};
        int64_t detectFtsUs{0};
        int64_t prepareStatementsUs{0};
        int64_t precheckUs{0};
        int64_t contentUpsertUs{0};
        int64_t ftsUpsertUs{0};
        int64_t ftsUpsertMaxUs{0};
        std::size_t ftsUpsertSlowCount{0};
        std::size_t ftsUpsertBoostedBytes{0};
        int64_t statusUpdateUs{0};
        int64_t commitUs{0};
        int64_t executeQueryUs{0};
        int64_t postResultUs{0};
        int64_t invalidateQueryCacheUs{0};
        int64_t signalCorpusStatsStaleUs{0};
        int64_t counterUpdateUs{0};
        int64_t noteFtsIndexedUs{0};
        std::size_t prechecks{0};
        std::size_t ftsPrecheckCacheHits{0};
        std::size_t ftsPrecheckCacheMisses{0};
        uint64_t corpusStatsSignals{0};
        uint64_t corpusStatsRedundantSignals{0};
        std::size_t contentUpserts{0};
        std::size_t ftsUpserts{0};
        std::size_t statusUpdates{0};
        std::size_t staleSkipped{0};
    };

    struct ExistingContentState {
        bool contentExtracted{false};
        bool ftsIndexed{false};
    };
    using ExistingStateMap = std::unordered_map<int64_t, ExistingContentState>;

    struct PendingWrites {
        Delta delta;
        std::vector<int64_t> statusUpdateIds;
        bool hasDuplicateStatusIds{false};
    };

    static void addElapsedUs(int64_t& target, std::chrono::steady_clock::time_point start,
                             bool collectTiming) {
        if (collectTiming) {
            target += std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
        }
    }

    static std::vector<PreparedEntry> prepareEntries(const std::vector<BatchContentEntry>& entries,
                                                     PhaseTimings& phaseTimings,
                                                     bool collectTiming) {
        constexpr size_t kMaxTextBytes = size_t{16} * 1024 * 1024;
        constexpr std::size_t kMaxBoostedBytes = kMaxTextBytes;
        std::vector<PreparedEntry> preparedEntries;
        preparedEntries.reserve(entries.size());
        const auto prepareStart = std::chrono::steady_clock::now();
        YAMS_ZONE_SCOPED_N("MetadataRepo::batchContentPrepare");
        for (const auto& entry : entries) {
            YAMS_DCHECK(entry.documentId != 0,
                        "batch content entry should target a persisted document");

            std::string_view contentView = entry.contentText;
            if (contentView.size() > kMaxTextBytes) {
                contentView = contentView.substr(0, kMaxTextBytes);
            }

            preparedEntries.emplace_back();
            auto& prepared = preparedEntries.back();
            prepared.documentId = entry.documentId;
            prepared.extractionMethod = entry.extractionMethod;
            prepared.language = entry.language;
            prepared.metadataTitle = entry.metadataTitle;

            std::string contentStorage;
            prepared.sanitizedContent = common::ensureValidUtf8(contentView, contentStorage);
            std::string titleStorage;
            prepared.sanitizedTitle = common::ensureValidUtf8(entry.title, titleStorage);

            prepared.boostedContent.reserve(
                std::min<std::size_t>(prepared.sanitizedContent.size() + 512, kMaxBoostedBytes));
            auto appendRepeated = [&](std::string_view text, int times) {
                if (text.empty()) {
                    return;
                }
                for (int repeat = 0; repeat < times; ++repeat) {
                    if (prepared.boostedContent.size() >= kMaxBoostedBytes) {
                        break;
                    }
                    if (!prepared.boostedContent.empty() && prepared.boostedContent.back() != ' ') {
                        prepared.boostedContent.push_back(' ');
                    }
                    const auto available = kMaxBoostedBytes - prepared.boostedContent.size();
                    if (available < text.size() + 1) {
                        prepared.boostedContent.append(
                            text.substr(0, std::min(text.size(), available)));
                        break;
                    }
                    prepared.boostedContent.append(text);
                }
            };

            appendRepeated(prepared.sanitizedTitle, 3);
            if (!entry.abstract.empty()) {
                std::string abstractStorage;
                const auto sanitizedAbstract =
                    common::ensureValidUtf8(entry.abstract, abstractStorage);
                appendRepeated(sanitizedAbstract, 2);
            }
            if (!prepared.boostedContent.empty() && prepared.boostedContent.back() != ' ') {
                prepared.boostedContent.push_back(' ');
            }
            const auto bodySpace = kMaxBoostedBytes > prepared.boostedContent.size()
                                       ? kMaxBoostedBytes - prepared.boostedContent.size()
                                       : 0;
            prepared.boostedContent.append(prepared.sanitizedContent.substr(
                0, std::min(bodySpace, prepared.sanitizedContent.size())));
        }
        addElapsedUs(phaseTimings.prepareInputUs, prepareStart, collectTiming);
        return preparedEntries;
    }

    static Result<ExistingStateMap> precheckExistingState(Database& db,
                                                          std::span<const PreparedEntry> entries,
                                                          bool hasFts5, PhaseTimings& phaseTimings,
                                                          bool collectTiming) {
        ExistingStateMap existingStates;
        if (entries.empty()) {
            return existingStates;
        }

        // The database is authoritative inside this transaction. A warmed negative cache can lag
        // a concurrent commit until that writer publishes its post-commit delta.
        const bool queryFtsState = hasFts5;
        YAMS_ZONE_SCOPED_N("MetadataRepo::batchContentPrecheckBatch");
        const auto phaseStart = std::chrono::steady_clock::now();
        std::string placeholders;
        placeholders.reserve(entries.size() * 2);
        std::unordered_map<int64_t, std::size_t> firstPreparedIndexById;
        firstPreparedIndexById.reserve(entries.size());
        for (std::size_t i = 0; i < entries.size(); ++i) {
            if (i > 0) {
                placeholders += ',';
            }
            placeholders += '?';
            firstPreparedIndexById.emplace(entries[i].documentId, i);
        }

        std::string sql = "SELECT id, COALESCE(content_extracted, 0)";
        if (queryFtsState) {
            sql += ", CASE WHEN EXISTS(SELECT 1 FROM documents_fts WHERE rowid = documents.id) "
                   "THEN 1 ELSE 0 END";
        }
        sql += " FROM documents WHERE id IN (";
        sql += placeholders;
        sql += ')';
        YAMS_TRY_UNWRAP(checkStmt, db.prepare(sql));
        for (std::size_t i = 0; i < entries.size(); ++i) {
            YAMS_TRY(checkStmt.bind(static_cast<int>(i + 1), entries[i].documentId));
        }

        existingStates.reserve(entries.size());
        while (true) {
            YAMS_TRY_UNWRAP(hasRow, checkStmt.step());
            if (!hasRow) {
                break;
            }
            const auto documentId = checkStmt.getInt64(0);
            const auto indexIt = firstPreparedIndexById.find(documentId);
            if (indexIt == firstPreparedIndexById.end()) {
                continue;
            }
            int column = 1;
            ExistingContentState state;
            state.contentExtracted = checkStmt.getInt(column++) == 1;
            state.ftsIndexed = hasFts5 ? checkStmt.getInt(column++) == 1 : true;
            existingStates.emplace(documentId, state);
            ++phaseTimings.prechecks;
        }
        addElapsedUs(phaseTimings.precheckUs, phaseStart, collectTiming);
        return existingStates;
    }

    static Result<PendingWrites> upsertContentAndFts(Database& db,
                                                     std::span<const PreparedEntry> entries,
                                                     const ExistingStateMap& existingStates,
                                                     bool hasFts5, PhaseTimings& phaseTimings,
                                                     bool collectTiming) {
        const auto prepareStatementsStart = std::chrono::steady_clock::now();
        YAMS_TRY_UNWRAP(contentStmtResult, db.prepareCached(R"(
                INSERT INTO document_content (
                    document_id, content_text, content_length,
                    extraction_method, language
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(document_id) DO UPDATE SET
                    content_text = excluded.content_text,
                    content_length = excluded.content_length,
                    extraction_method = excluded.extraction_method,
                    language = excluded.language
            )"));
        auto& contentStmt = *contentStmtResult;

        // Compare the persisted input before replacing it, in the same transaction.
        // This prevents skipExisting from reusing embeddings of an older extraction.
        // It does not fence completion of an already-running embedding job.
        YAMS_TRY_UNWRAP(invalidateStmtResult, db.prepareCached(R"(
            UPDATE document_embeddings_status
            SET has_embedding = 0, model_id = NULL, updated_at = unixepoch()
            WHERE document_id = ? AND has_embedding = 1
              AND NOT EXISTS (
                SELECT 1 FROM document_content
                WHERE document_id = document_embeddings_status.document_id
                  AND content_text IS ? AND extraction_method IS ? AND language IS ?
              )
        )"));
        auto& invalidateStmt = *invalidateStmtResult;

        std::optional<CachedStatement> ftsStmtOpt;
        if (hasFts5) {
            YAMS_TRY_UNWRAP(ftsStmtResult, db.prepareCached(R"(
                    INSERT OR REPLACE INTO documents_fts (rowid, content, title)
                    VALUES (?, ?, ?)
                )"));
            ftsStmtOpt = std::move(ftsStmtResult);
        }
        addElapsedUs(phaseTimings.prepareStatementsUs, prepareStatementsStart, collectTiming);

        PendingWrites writes;
        writes.delta.indexedDocIds.reserve(entries.size());
        writes.statusUpdateIds.reserve(entries.size());
        std::unordered_set<int64_t> uniqueStatusIds;
        std::unordered_set<int64_t> newlyExtractedIds;
        std::unordered_set<int64_t> newlyIndexedIds;
        uniqueStatusIds.reserve(entries.size());
        newlyExtractedIds.reserve(entries.size());
        newlyIndexedIds.reserve(entries.size());
        std::vector<repository::MetadataWriteEntry> metadataWrites;
        metadataWrites.reserve(entries.size());

        for (const auto& entry : entries) {
            const auto stateIt = existingStates.find(entry.documentId);
            if (stateIt == existingStates.end()) {
                spdlog::debug("MetadataRepository: skipping stale batch content entry for "
                              "missing document id {}",
                              entry.documentId);
                ++phaseTimings.staleSkipped;
                continue;
            }

            {
                YAMS_ZONE_SCOPED_N("MetadataRepo::batchContentUpsertContent");
                const auto phaseStart = std::chrono::steady_clock::now();
                YAMS_TRY(invalidateStmt.reset());
                YAMS_TRY(invalidateStmt.clearBindings());
                YAMS_TRY(invalidateStmt.bindAll(entry.documentId, entry.sanitizedContent,
                                                entry.extractionMethod, entry.language));
                YAMS_TRY(invalidateStmt.execute());
                writes.delta.invalidatedEmbeddings += static_cast<uint64_t>(db.changes());
                YAMS_TRY(contentStmt.reset());
                YAMS_TRY(contentStmt.clearBindings());
                YAMS_TRY(contentStmt.bindAll(entry.documentId, entry.sanitizedContent,
                                             static_cast<int64_t>(entry.sanitizedContent.length()),
                                             entry.extractionMethod, entry.language));
                YAMS_TRY(contentStmt.execute());
                ++phaseTimings.contentUpserts;
                addElapsedUs(phaseTimings.contentUpsertUs, phaseStart, collectTiming);
            }

            if (hasFts5 && ftsStmtOpt) {
                YAMS_ZONE_SCOPED_N("MetadataRepo::batchContentUpsertFts");
                const auto phaseStart = std::chrono::steady_clock::now();
                auto& ftsStmt = **ftsStmtOpt;
                YAMS_TRY(ftsStmt.reset());
                YAMS_TRY(ftsStmt.clearBindings());
                YAMS_TRY(
                    ftsStmt.bindAll(entry.documentId, entry.boostedContent, entry.sanitizedTitle));
                YAMS_TRY(ftsStmt.execute());
                const auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::steady_clock::now() - phaseStart)
                                           .count();
                const auto boundedUs = std::max<int64_t>(0, elapsedUs);
                writes.delta.indexedDocIds.push_back(entry.documentId);
                ++phaseTimings.ftsUpserts;
                phaseTimings.ftsUpsertUs += boundedUs;
                phaseTimings.ftsUpsertMaxUs = std::max(phaseTimings.ftsUpsertMaxUs, boundedUs);
                phaseTimings.ftsUpsertBoostedBytes += entry.boostedContent.size();
                constexpr int64_t kSlowFtsUpsertUs = 1000;
                if (boundedUs >= kSlowFtsUpsertUs) {
                    ++phaseTimings.ftsUpsertSlowCount;
                }
            }

            writes.statusUpdateIds.push_back(entry.documentId);
            if (!uniqueStatusIds.insert(entry.documentId).second) {
                writes.hasDuplicateStatusIds = true;
            }
            if (!stateIt->second.contentExtracted) {
                newlyExtractedIds.insert(entry.documentId);
            }
            if (!stateIt->second.ftsIndexed) {
                newlyIndexedIds.insert(entry.documentId);
            }
            if (!entry.metadataTitle.empty()) {
                metadataWrites.emplace_back(entry.documentId, "title",
                                            MetadataValue(entry.metadataTitle));
            }
        }

        writes.delta.newlyExtracted = newlyExtractedIds.size();
        writes.delta.newlyIndexed = newlyIndexedIds.size();
        writes.delta.metadataWrites = metadataWrites.size();
        YAMS_TRY(repository::upsertMetadataWrites(db, metadataWrites));
        return writes;
    }

    static Result<void> updateSharedStatus(Database& db, const std::vector<int64_t>& documentIds,
                                           bool hasDuplicateIds, PhaseTimings& phaseTimings,
                                           bool collectTiming) {
        if (documentIds.empty()) {
            return {};
        }
        YAMS_ZONE_SCOPED_N("MetadataRepo::batchContentStatusUpdate");
        const auto phaseStart = std::chrono::steady_clock::now();
        if (hasDuplicateIds) {
            YAMS_TRY_UNWRAP(statusStmtResult, db.prepareCached(R"(
                    UPDATE documents
                    SET content_extracted = 1,
                        extraction_status = 'success',
                        extraction_error = NULL,
                        repair_status = 'completed',
                        repair_attempted_at = unixepoch(),
                        repair_attempts = repair_attempts + 1
                    WHERE id = ?
                )"));
            auto& statusStmt = *statusStmtResult;
            for (const auto documentId : documentIds) {
                YAMS_TRY(statusStmt.reset());
                YAMS_TRY(statusStmt.clearBindings());
                YAMS_TRY(statusStmt.bind(1, documentId));
                YAMS_TRY(statusStmt.execute());
            }
        } else {
            constexpr std::size_t kMaxStatusIdsPerUpdate = 400;
            for (std::size_t offset = 0; offset < documentIds.size();
                 offset += kMaxStatusIdsPerUpdate) {
                const auto chunkSize =
                    std::min(kMaxStatusIdsPerUpdate, documentIds.size() - offset);
                std::string placeholders;
                placeholders.reserve(chunkSize * 2);
                for (std::size_t i = 0; i < chunkSize; ++i) {
                    if (i > 0) {
                        placeholders += ',';
                    }
                    placeholders += '?';
                }
                std::string sql;
                sql.reserve(placeholders.size() + 256);
                sql += "UPDATE documents SET content_extracted = 1, ";
                sql += "extraction_status = 'success', extraction_error = NULL, ";
                sql += "repair_status = 'completed', repair_attempted_at = unixepoch(), ";
                sql += "repair_attempts = repair_attempts + 1 WHERE id IN (";
                sql += placeholders;
                sql += ')';

                YAMS_TRY_UNWRAP(statusStmt, db.prepare(sql));
                YAMS_TRY(statusStmt.reset());
                YAMS_TRY(statusStmt.clearBindings());
                for (std::size_t i = 0; i < chunkSize; ++i) {
                    YAMS_TRY(statusStmt.bind(static_cast<int>(i + 1), documentIds[offset + i]));
                }
                YAMS_TRY(statusStmt.execute());
            }
        }
        phaseTimings.statusUpdates += documentIds.size();
        addElapsedUs(phaseTimings.statusUpdateUs, phaseStart, collectTiming);
        return {};
    }

    // Commit only: the transaction-owner scope guard is the sole rollback authority.
    static Result<void> commitTransaction(Database& db) { return db.execute("COMMIT"); }

    static void applyPostCommit(MetadataRepository& repository, const Delta& delta,
                                PhaseTimings& phaseTimings, bool collectTiming) {
        const auto phaseStart = collectTiming ? std::chrono::steady_clock::now()
                                              : std::chrono::steady_clock::time_point{};
        repository.invalidateQueryCache();
        addElapsedUs(phaseTimings.invalidateQueryCacheUs, phaseStart, collectTiming);

        const auto signalCountBefore =
            repository.corpusStatsStaleSignals_.load(std::memory_order_relaxed);
        const auto redundantCountBefore =
            repository.corpusStatsStaleRedundantSignals_.load(std::memory_order_relaxed);
        const auto signalStart = collectTiming ? std::chrono::steady_clock::now()
                                               : std::chrono::steady_clock::time_point{};
        repository.signalCorpusStatsStale();
        addElapsedUs(phaseTimings.signalCorpusStatsStaleUs, signalStart, collectTiming);
        phaseTimings.corpusStatsSignals =
            repository.corpusStatsStaleSignals_.load(std::memory_order_relaxed) - signalCountBefore;
        phaseTimings.corpusStatsRedundantSignals =
            repository.corpusStatsStaleRedundantSignals_.load(std::memory_order_relaxed) -
            redundantCountBefore;

        const auto counterStart = collectTiming ? std::chrono::steady_clock::now()
                                                : std::chrono::steady_clock::time_point{};
        if (delta.newlyExtracted > 0) {
            repository.cachedExtractedCount_.fetch_add(delta.newlyExtracted,
                                                       std::memory_order_relaxed);
        }
        if (delta.newlyIndexed > 0) {
            repository.cachedIndexedCount_.fetch_add(delta.newlyIndexed, std::memory_order_relaxed);
        }
        if (delta.invalidatedEmbeddings > 0) {
            core::saturating_sub(repository.cachedEmbeddedCount_, delta.invalidatedEmbeddings);
        }
        if (delta.metadataWrites > 0) {
            repository.metadataChangeCounter_.fetch_add(delta.metadataWrites,
                                                        std::memory_order_release);
        }
        addElapsedUs(phaseTimings.counterUpdateUs, counterStart, collectTiming);

        const auto ftsCacheStart = collectTiming ? std::chrono::steady_clock::now()
                                                 : std::chrono::steady_clock::time_point{};
        for (const auto documentId : delta.indexedDocIds) {
            repository.noteFtsIndexedId(documentId);
        }
        addElapsedUs(phaseTimings.noteFtsIndexedUs, ftsCacheStart, collectTiming);
    }
};

// Document operations
Result<void>
MetadataRepository::batchInsertContentAndIndex(const std::vector<BatchContentEntry>& entries) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::batchInsertContentAndIndex");
    YAMS_PLOT("metadata_repo::batch_content_entries", static_cast<int64_t>(entries.size()));
    if (entries.empty()) {
        return Result<void>();
    }

    using BatchContentDelta = ContentBatchOps::Delta;
    using BatchContentPhaseTimings = ContentBatchOps::PhaseTimings;

    const bool traceBatchContent = metadata_trace_enabled();
#ifdef TRACY_ENABLE
    constexpr bool kCollectBatchContentTimingForTracy = true;
#else
    constexpr bool kCollectBatchContentTimingForTracy = false;
#endif
    const bool collectBatchContentTiming = traceBatchContent || kCollectBatchContentTimingForTracy;
    BatchContentPhaseTimings phaseTimings;
    auto addElapsedUs = [&](int64_t& target, std::chrono::steady_clock::time_point start) {
        ContentBatchOps::addElapsedUs(target, start, collectBatchContentTiming);
    };
    auto nowForBatchTiming = [&]() {
        return collectBatchContentTiming ? std::chrono::steady_clock::now()
                                         : std::chrono::steady_clock::time_point{};
    };

    // Expensive UTF-8 cleanup and FTS payload construction stay outside the write transaction.
    auto preparedEntries =
        ContentBatchOps::prepareEntries(entries, phaseTimings, collectBatchContentTiming);

    const auto executeQueryStart = nowForBatchTiming();
    auto result = executeQuery<BatchContentDelta>([&](Database& db) -> Result<BatchContentDelta> {
        YAMS_ZONE_SCOPED_N("MetadataRepo::batchContentTransaction");
        const auto transactionStart = std::chrono::steady_clock::now();
        // Track counter deltas per attempt. executeQueryOnPool may retry the lambda
        // after lock/constraint races; deltas from failed attempts must not leak into
        // the final cache update.
        BatchContentDelta delta;
        delta.indexedDocIds.reserve(preparedEntries.size());
        const auto beginStart = std::chrono::steady_clock::now();
        YAMS_TRY(beginTransaction(db));
        addElapsedUs(phaseTimings.beginUs, beginStart);
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        // Check if FTS5 is available once
        const auto ftsDetectStart = std::chrono::steady_clock::now();
        YAMS_TRY_UNWRAP(hasFts5, db.hasFTS5());
        addElapsedUs(phaseTimings.detectFtsUs, ftsDetectStart);

        if (hasFts5) {
            // Write-side correctness requires a transactional DB read. Keep the existing metric
            // explicit: these entries intentionally bypass the advisory warmed-ID cache.
            phaseTimings.ftsPrecheckCacheMisses += preparedEntries.size();
        }

        // Every retry re-reads committed state inside its new transaction before deriving deltas.
        YAMS_TRY_UNWRAP(existingStates,
                        ContentBatchOps::precheckExistingState(
                            db, preparedEntries, hasFts5, phaseTimings, collectBatchContentTiming));
        YAMS_TRY_UNWRAP(pendingWrites, ContentBatchOps::upsertContentAndFts(
                                           db, preparedEntries, existingStates, hasFts5,
                                           phaseTimings, collectBatchContentTiming));
        YAMS_TRY(ContentBatchOps::updateSharedStatus(db, pendingWrites.statusUpdateIds,
                                                     pendingWrites.hasDuplicateStatusIds,
                                                     phaseTimings, collectBatchContentTiming));
        delta = std::move(pendingWrites.delta);

        const auto commitStart = std::chrono::steady_clock::now();
        YAMS_TRY(ContentBatchOps::commitTransaction(db));
        addElapsedUs(phaseTimings.commitUs, commitStart);
        committed = true;
        rollback.dismiss();
        addElapsedUs(phaseTimings.transactionUs, transactionStart);

        return delta;
    });

    addElapsedUs(phaseTimings.executeQueryUs, executeQueryStart);

    if (result) {
        const auto postResultStart = nowForBatchTiming();
        const auto& delta = result.value();
        ContentBatchOps::applyPostCommit(*this, delta, phaseTimings, collectBatchContentTiming);
        addElapsedUs(phaseTimings.postResultUs, postResultStart);
        YAMS_PLOT("metadata_repo::batch_content_newly_extracted",
                  static_cast<int64_t>(delta.newlyExtracted));
        YAMS_PLOT("metadata_repo::batch_content_newly_indexed",
                  static_cast<int64_t>(delta.newlyIndexed));
        YAMS_PLOT("metadata_repo::batch_content_prepare_us", phaseTimings.prepareInputUs);
        YAMS_PLOT("metadata_repo::batch_content_transaction_us", phaseTimings.transactionUs);
        YAMS_PLOT("metadata_repo::batch_content_precheck_us", phaseTimings.precheckUs);
        YAMS_PLOT("metadata_repo::batch_content_content_upsert_us", phaseTimings.contentUpsertUs);
        YAMS_PLOT("metadata_repo::batch_content_fts_upsert_us", phaseTimings.ftsUpsertUs);
        YAMS_PLOT("metadata_repo::batch_content_fts_upsert_max_us", phaseTimings.ftsUpsertMaxUs);
        YAMS_PLOT("metadata_repo::batch_content_fts_upsert_slow_count",
                  static_cast<int64_t>(phaseTimings.ftsUpsertSlowCount));
        YAMS_PLOT("metadata_repo::batch_content_fts_upsert_boosted_bytes",
                  static_cast<int64_t>(phaseTimings.ftsUpsertBoostedBytes));
        YAMS_PLOT("metadata_repo::batch_content_status_update_us", phaseTimings.statusUpdateUs);
        YAMS_PLOT("metadata_repo::batch_content_execute_query_us", phaseTimings.executeQueryUs);
        YAMS_PLOT("metadata_repo::batch_content_post_result_us", phaseTimings.postResultUs);
        YAMS_PLOT("metadata_repo::batch_content_invalidate_query_cache_us",
                  phaseTimings.invalidateQueryCacheUs);
        YAMS_PLOT("metadata_repo::batch_content_signal_corpus_stats_stale_us",
                  phaseTimings.signalCorpusStatsStaleUs);
        YAMS_PLOT("metadata_repo::batch_content_counter_update_us", phaseTimings.counterUpdateUs);
        YAMS_PLOT("metadata_repo::batch_content_note_fts_indexed_us",
                  phaseTimings.noteFtsIndexedUs);
        YAMS_PLOT("metadata_repo::batch_content_fts_precheck_cache_hits",
                  static_cast<int64_t>(phaseTimings.ftsPrecheckCacheHits));
        YAMS_PLOT("metadata_repo::batch_content_fts_precheck_cache_misses",
                  static_cast<int64_t>(phaseTimings.ftsPrecheckCacheMisses));
        YAMS_PLOT("metadata_repo::batch_content_corpus_stats_signals",
                  static_cast<int64_t>(phaseTimings.corpusStatsSignals));
        YAMS_PLOT("metadata_repo::batch_content_corpus_stats_redundant_signals",
                  static_cast<int64_t>(phaseTimings.corpusStatsRedundantSignals));
        if (traceBatchContent) {
            spdlog::info(
                "MetadataRepository::batchInsertContentAndIndex phases entries={} "
                "prepare_us={} transaction_us={} begin_us={} fts_detect_us={} "
                "stmt_prepare_us={} precheck_us={} content_upsert_us={} "
                "fts_upsert_us={} fts_upsert_max_us={} fts_upsert_slow_count={} "
                "fts_upsert_boosted_bytes={} status_update_us={} commit_us={} execute_query_us={} "
                "post_result_us={} invalidate_query_cache_us={} "
                "signal_corpus_stats_stale_us={} counter_update_us={} "
                "note_fts_indexed_us={} prechecks={} fts_precheck_cache_hits={} "
                "fts_precheck_cache_misses={} corpus_stats_signals={} "
                "corpus_stats_redundant_signals={} content_upserts={} fts_upserts={} "
                "status_updates={} stale_skipped={}",
                entries.size(), phaseTimings.prepareInputUs, phaseTimings.transactionUs,
                phaseTimings.beginUs, phaseTimings.detectFtsUs, phaseTimings.prepareStatementsUs,
                phaseTimings.precheckUs, phaseTimings.contentUpsertUs, phaseTimings.ftsUpsertUs,
                phaseTimings.ftsUpsertMaxUs, phaseTimings.ftsUpsertSlowCount,
                phaseTimings.ftsUpsertBoostedBytes, phaseTimings.statusUpdateUs,
                phaseTimings.commitUs, phaseTimings.executeQueryUs, phaseTimings.postResultUs,
                phaseTimings.invalidateQueryCacheUs, phaseTimings.signalCorpusStatsStaleUs,
                phaseTimings.counterUpdateUs, phaseTimings.noteFtsIndexedUs, phaseTimings.prechecks,
                phaseTimings.ftsPrecheckCacheHits, phaseTimings.ftsPrecheckCacheMisses,
                phaseTimings.corpusStatsSignals, phaseTimings.corpusStatsRedundantSignals,
                phaseTimings.contentUpserts, phaseTimings.ftsUpserts, phaseTimings.statusUpdates,
                phaseTimings.staleSkipped);
        }

        const auto cachedIndexed = cachedIndexedCount_.load(std::memory_order_relaxed);
        const auto cachedDocuments = cachedDocumentCount_.load(std::memory_order_relaxed);
        if (cachedIndexed > cachedDocuments) {
            spdlog::warn("MetadataRepository: batch counter drift docs={} indexed={} "
                         "delta_extracted={} delta_indexed={} entries={}",
                         cachedDocuments, cachedIndexed, delta.newlyExtracted, delta.newlyIndexed,
                         entries.size());
        }
        YAMS_DCHECK(
            cachedIndexed <= cachedDocuments,
            "metadata: indexed count must not exceed total document count after batch insert");
        YAMS_DCHECK(
            cachedExtractedCount_.load() <= cachedDocumentCount_.load(),
            "metadata: extracted count must not exceed total document count after batch insert");
    }
    if (!result) {
        return result.error();
    }
    return Result<void>();
}

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
                result[hashKey] = std::make_pair(std::move(info), std::move(preview));
            }

            return result;
        });
}

} // namespace yams::metadata
