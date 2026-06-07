#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <span>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <yams/common/time_utils.h>
#include <yams/common/utf8_utils.h>
#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>
#include <yams/search/symspell_search.h>
#include <yams/storage/corpus_stats.h>
#include <yams/storage/sqlite_retry.h>

// Phase 2: MetadataRepository refactor - ADR-0004
// Using result helpers for reduced error handling boilerplate
#include "repository/result_helpers.hpp"

// Phase 5: CrudOps for generic CRUD operations (ADR-0004)
#include "repository/corpus_stats_ops.hpp"
#include "repository/crud_ops.hpp"
#include "repository/document_query_filters.hpp"
#include "repository/metadata_value_count_ops.hpp"
#include "repository/transaction_helpers.hpp"

namespace yams::metadata {

// Import result helpers for cleaner error handling (ADR-0004 Phase 2)
using repository::addIntParam;
using repository::appendDocumentQueryFilters;
using repository::applyCorpusStatsOnlineOverlay;
using repository::beginTransaction;
using repository::beginTransactionWithRetry;
using repository::BindParam;
using repository::calculateExtensionBucketCounts;
using repository::classifyExtensionBucket;
using repository::commitOrRollback;
using repository::CorpusStatsOverlayInput;
using repository::ExtensionBucket;
using repository::rollbackIgnoringErrors;
using repository::runMetadataValueCountQuery;
using repository::scope_exit;

namespace {
std::chrono::sys_seconds nowSysSeconds() {
    return std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
}

Result<int64_t> queryExactFtsIndexedDocumentCount(Database& db) {
    auto stmtResult = db.prepare("SELECT COUNT(*) FROM documents_fts_docsize");
    if (!stmtResult) {
        spdlog::debug("MetadataRepository: documents_fts_docsize unavailable, falling back to "
                      "documents_fts COUNT(*): {}",
                      stmtResult.error().message);
        stmtResult = db.prepare("SELECT COUNT(*) FROM documents_fts");
        if (!stmtResult) {
            return stmtResult.error();
        }
    }

    auto stmt = std::move(stmtResult).value();
    auto stepResult = stmt.step();
    if (!stepResult) {
        return stepResult.error();
    }
    if (!stepResult.value()) {
        return int64_t{0};
    }
    return stmt.getInt64(0);
}

SemanticDuplicateGroup mapSemanticDuplicateGroupRow(const Statement& stmt) {
    SemanticDuplicateGroup group;
    group.id = stmt.getInt64(0);
    group.groupKey = stmt.getString(1);
    group.algorithmVersion = stmt.getString(2);
    group.status = stmt.getString(3);
    group.reviewState = stmt.getString(4);
    if (!stmt.isNull(5))
        group.canonicalDocumentId = stmt.getInt64(5);
    group.memberCount = stmt.getInt64(6);
    group.maxPairScore = stmt.getDouble(7);
    if (!stmt.isNull(8))
        group.threshold = stmt.getDouble(8);
    group.evidenceJson = stmt.isNull(9) ? "" : stmt.getString(9);
    group.setCreatedAt(stmt.getInt64(10));
    group.setUpdatedAt(stmt.getInt64(11));
    group.setLastComputedAt(stmt.getInt64(12));
    return group;
}

SemanticDuplicateGroupMember mapSemanticDuplicateGroupMemberRow(const Statement& stmt) {
    SemanticDuplicateGroupMember member;
    member.id = stmt.getInt64(0);
    member.groupId = stmt.getInt64(1);
    member.documentId = stmt.getInt64(2);
    member.role = stmt.getString(3);
    if (!stmt.isNull(4))
        member.similarityToCanonical = stmt.getDouble(4);
    if (!stmt.isNull(5))
        member.titleOverlap = stmt.getDouble(5);
    if (!stmt.isNull(6))
        member.pathOverlap = stmt.getDouble(6);
    if (!stmt.isNull(7))
        member.pairScore = stmt.getDouble(7);
    member.decision = stmt.getString(8);
    member.reason = stmt.isNull(9) ? "" : stmt.getString(9);
    member.setCreatedAt(stmt.getInt64(10));
    member.setUpdatedAt(stmt.getInt64(11));
    return member;
}

Result<void> bindOptionalInt64(Statement& stmt, int index, const std::optional<int64_t>& value) {
    if (value.has_value()) {
        return stmt.bind(index, *value);
    }
    return stmt.bind(index, nullptr);
}

Result<void> bindOptionalDouble(Statement& stmt, int index, const std::optional<double>& value) {
    if (value.has_value()) {
        return stmt.bind(index, *value);
    }
    return stmt.bind(index, nullptr);
}

Result<void> bindNullableText(Statement& stmt, int index, std::string_view value) {
    if (value.empty()) {
        return stmt.bind(index, nullptr);
    }
    return stmt.bind(index, value);
}

Result<void> bindSemanticDuplicateGroupMemberInsert(Statement& stmt, int64_t groupId,
                                                    const SemanticDuplicateGroupMember& member) {
    YAMS_TRY(stmt.bind(1, groupId));
    YAMS_TRY(stmt.bind(2, member.documentId));
    YAMS_TRY(stmt.bind(3, member.role));
    YAMS_TRY(bindOptionalDouble(stmt, 4, member.similarityToCanonical));
    YAMS_TRY(bindOptionalDouble(stmt, 5, member.titleOverlap));
    YAMS_TRY(bindOptionalDouble(stmt, 6, member.pathOverlap));
    YAMS_TRY(bindOptionalDouble(stmt, 7, member.pairScore));
    YAMS_TRY(stmt.bind(8, member.decision));
    YAMS_TRY(bindNullableText(stmt, 9, member.reason));
    YAMS_TRY(stmt.bind(10, member.createdAt));
    YAMS_TRY(stmt.bind(11, member.updatedAt));
    return Result<void>();
}

constexpr const char* kDocumentColumnListNew =
    "id, file_path, file_name, file_extension, file_size, sha256_hash, mime_type, "
    "created_time, modified_time, indexed_time, content_extracted, extraction_status, "
    "extraction_error, path_prefix, reverse_path, path_hash, parent_hash, path_depth, "
    "repair_status, repair_attempted_at, repair_attempts";

constexpr const char* kDocumentColumnListCompat =
    "id, file_path, file_name, file_extension, file_size, sha256_hash, mime_type, "
    "created_time, modified_time, indexed_time, content_extracted, extraction_status, "
    "extraction_error, NULL as path_prefix, '' as reverse_path, '' as path_hash, '' as "
    "parent_hash, 0 as path_depth, 'pending' as repair_status, NULL as repair_attempted_at, "
    "0 as repair_attempts";

constexpr const char* kDocumentColumnListNewQualified =
    "documents.id, documents.file_path, documents.file_name, documents.file_extension, "
    "documents.file_size, documents.sha256_hash, documents.mime_type, documents.created_time, "
    "documents.modified_time, documents.indexed_time, documents.content_extracted, "
    "documents.extraction_status, documents.extraction_error, documents.path_prefix, "
    "documents.reverse_path, documents.path_hash, documents.parent_hash, documents.path_depth, "
    "documents.repair_status, documents.repair_attempted_at, documents.repair_attempts";

constexpr const char* kDocumentColumnListCompatQualified =
    "documents.id, documents.file_path, documents.file_name, documents.file_extension, "
    "documents.file_size, documents.sha256_hash, documents.mime_type, documents.created_time, "
    "documents.modified_time, documents.indexed_time, documents.content_extracted, "
    "documents.extraction_status, documents.extraction_error, NULL as path_prefix, '' as "
    "reverse_path, '' as path_hash, '' as parent_hash, 0 as path_depth, 'pending' as "
    "repair_status, NULL as repair_attempted_at, 0 as repair_attempts";

// Qualified with alias "d" for JOIN queries that alias documents as d
constexpr const char* kDocumentColumnListAliasD =
    "d.id, d.file_path, d.file_name, d.file_extension, d.file_size, d.sha256_hash, "
    "d.mime_type, d.created_time, d.modified_time, d.indexed_time, d.content_extracted, "
    "d.extraction_status, d.extraction_error, d.path_prefix, d.reverse_path, d.path_hash, "
    "d.parent_hash, d.path_depth, d.repair_status, d.repair_attempted_at, d.repair_attempts";

std::string buildInList(size_t count) {
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

template <typename RowT, typename RowMapper, typename RowVisitor>
Result<std::vector<RowT>> executePreparedVectorQuery(Database& db, const std::string& sql,
                                                     const std::vector<BindParam>& params,
                                                     RowMapper&& mapRow, RowVisitor&& onRow) {
    auto stmtResult = db.prepare(sql);
    if (!stmtResult) {
        return stmtResult.error();
    }

    Statement stmt = std::move(stmtResult).value();
    int index = 1;
    for (const auto& param : params) {
        Result<void> bindResult;
        if (param.type == BindParam::Type::Text) {
            bindResult = stmt.bind(index, param.text);
        } else {
            bindResult = stmt.bind(index, param.integer);
        }
        if (!bindResult) {
            return bindResult.error();
        }
        ++index;
    }

    std::vector<RowT> rows;
    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult) {
            return stepResult.error();
        }
        if (!stepResult.value())
            break;

        RowT row = mapRow(stmt);
        onRow(row);
        rows.push_back(std::move(row));
    }

    return rows;
}

template <typename RowT, typename RowMapper>
Result<std::vector<RowT>> executePreparedVectorQuery(Database& db, const std::string& sql,
                                                     const std::vector<BindParam>& params,
                                                     RowMapper&& mapRow) {
    return executePreparedVectorQuery<RowT>(db, sql, params, std::forward<RowMapper>(mapRow),
                                            [](RowT&) {});
}

uint64_t clampNonNegativeCount(int64_t value) {
    return static_cast<uint64_t>(std::max<int64_t>(value, 0));
}

void storeNonNegativeCount(std::atomic<uint64_t>& target, int64_t value) {
    target.store(clampNonNegativeCount(value), std::memory_order_release);
}

} // namespace

const char* MetadataRepository::documentColumnList(bool qualified) const {
    if (qualified) {
        return hasPathIndexing_ ? kDocumentColumnListNewQualified
                                : kDocumentColumnListCompatQualified;
    }
    return hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
}

Result<std::optional<DocumentInfo>> MetadataRepository::getDocumentByCondition(
    Database& db, std::string_view condition,
    const std::function<Result<void>(Statement&)>& binder) const {
    using yams::metadata::sql::QuerySpec;
    QuerySpec spec{};
    spec.table = "documents";
    spec.columns = {documentColumnList(false)};
    spec.conditions = {std::string(condition)};

    YAMS_TRY_UNWRAP(cachedStmt, db.prepareCached(yams::metadata::sql::buildSelect(spec)));
    auto& stmt = *cachedStmt;
    if (binder) {
        YAMS_TRY(binder(stmt));
    }
    YAMS_TRY_UNWRAP(hasRow, stmt.step());

    if (!hasRow) {
        return std::optional<DocumentInfo>{};
    }
    return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
}

Result<std::vector<DocumentInfo>> MetadataRepository::queryDocumentsBySpec(
    Database& db, const sql::QuerySpec& spec,
    const std::function<Result<void>(Statement&)>& binder) const {
    auto stmtResult = db.prepare(sql::buildSelect(spec));
    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    if (binder) {
        auto bindResult = binder(stmt);
        if (!bindResult)
            return bindResult.error();
    }

    std::vector<DocumentInfo> result;
    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            break;

        result.push_back(mapDocumentRow(stmt));
    }

    return result;
}

std::optional<DocumentInfo>
MetadataRepository::lookupPathCache(const std::string& normalizedPath) const {
    YAMS_ZONE_SCOPED_N("MetadataRepo::lookupPathCache");
    flushPathCacheBuffer();
    auto snap = std::atomic_load_explicit(&pathCacheSnapshot_, std::memory_order_acquire);
    if (!snap)
        return std::nullopt;
    if (auto it = snap->data.find(normalizedPath); it != snap->data.end()) {
        // Record a hit approximately (lock-free ring), then return copy of doc
        recordPathHit(normalizedPath);
        return it->second.doc;
    }
    return std::nullopt;
}

void MetadataRepository::storePathCache(const DocumentInfo& info) const {
    {
        std::lock_guard<std::mutex> lock(pathCacheWriteBuffer_.mutex);
        pathCacheWriteBuffer_.pending.push_back(info);
    }
    auto pending = pathCacheWriteBuffer_.size.fetch_add(1, std::memory_order_relaxed) + 1;
    if (pending >= kPathCacheFlushThreshold) {
        flushPathCacheBuffer();
    }
}

void MetadataRepository::recordPathHit(const std::string& normalizedPath) const {
    if (!hitRing_)
        return; // not initialized until first store
    uint64_t h = std::hash<std::string>{}(normalizedPath);
    auto idx = hitSeq_.fetch_add(1, std::memory_order_relaxed) & hitRingMask_;
    hitRing_[idx].store(h, std::memory_order_relaxed);
}

void MetadataRepository::flushPathCacheBuffer() const {
    YAMS_ZONE_SCOPED_N("MetadataRepo::flushPathCacheBuffer");
    std::vector<DocumentInfo> batch;
    {
        std::lock_guard<std::mutex> lock(pathCacheWriteBuffer_.mutex);
        if (pathCacheWriteBuffer_.pending.empty())
            return;
        batch = std::move(pathCacheWriteBuffer_.pending);
        pathCacheWriteBuffer_.pending.clear();
        pathCacheWriteBuffer_.size.store(0, std::memory_order_relaxed);
    }

    if (!hitRing_) {
        const std::size_t ringSize = 4096; // power of two
        hitRing_.reset(new std::atomic<uint64_t>[ringSize]);
        hitRingSize_ = ringSize;
        for (std::size_t i = 0; i < ringSize; ++i) {
            hitRing_[i].store(0, std::memory_order_relaxed);
        }
        hitRingMask_ = ringSize - 1;
    }

    auto old = std::atomic_load_explicit(&pathCacheSnapshot_, std::memory_order_acquire);
    auto updated = std::make_shared<PathCacheSnapshot>(*old);

    const auto endSeq = hitSeq_.load(std::memory_order_relaxed);
    const std::size_t toFold = static_cast<std::size_t>(std::min<uint64_t>(hitRingSize_, endSeq));
    for (std::size_t i = 0; i < toFold; ++i) {
        const uint64_t h =
            hitRing_[(endSeq - 1 - i) & hitRingMask_].load(std::memory_order_relaxed);
        if (h == 0)
            continue;
        for (auto& kv : updated->data) {
            const uint64_t kh = std::hash<std::string>{}(kv.first);
            if (kh == h) {
                kv.second.lastHitSeq = endSeq - i;
            }
        }
    }

    uint64_t lastSeq = updated->buildSeq;
    for (const auto& info : batch) {
        const auto gseq = globalSeq_.fetch_add(1, std::memory_order_relaxed);
        lastSeq = gseq;
        auto& entry = updated->data[info.filePath];
        entry.doc = info;
        if (entry.insertedSeq == 0)
            entry.insertedSeq = gseq;
    }

    if (updated->data.size() > pathCacheCapacity_) {
        std::vector<std::pair<std::string, PathCacheSnapshot::Entry*>> entries;
        entries.reserve(updated->data.size());
        for (auto& kv : updated->data) {
            entries.emplace_back(kv.first, &kv.second);
        }
        std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
            if (a.second->lastHitSeq != b.second->lastHitSeq)
                return a.second->lastHitSeq < b.second->lastHitSeq;
            return a.second->insertedSeq < b.second->insertedSeq;
        });
        const auto need = updated->data.size() - pathCacheCapacity_;
        for (std::size_t i = 0; i < need && i < entries.size(); ++i) {
            updated->data.erase(entries[i].first);
        }
    }

    updated->timestamp = std::chrono::steady_clock::now();
    if (!batch.empty())
        updated->buildSeq = lastSeq;
    std::atomic_store_explicit(&pathCacheSnapshot_, std::move(updated), std::memory_order_release);
}

void MetadataRepository::invalidateQueryCache() const {
    std::lock_guard<std::mutex> lock(queryCacheMutex_);
    std::atomic_store_explicit(&queryCacheSnapshot_, std::make_shared<QueryCacheMap>(),
                               std::memory_order_release);
}

std::optional<std::vector<int64_t>> MetadataRepository::getCachedFtsIndexedIds() const {
    if (!ftsIndexedIdsCacheReady_.load(std::memory_order_acquire)) {
        return std::nullopt;
    }

    std::shared_lock<std::shared_mutex> lock(ftsIndexedIdsCacheMutex_);
    std::vector<int64_t> ids;
    ids.reserve(ftsIndexedIdsCache_.size());
    for (int64_t id : ftsIndexedIdsCache_) {
        ids.push_back(id);
    }
    return ids;
}

void MetadataRepository::setCachedFtsIndexedIds(const std::vector<int64_t>& docIds) const {
    std::unique_lock<std::shared_mutex> lock(ftsIndexedIdsCacheMutex_);
    ftsIndexedIdsCache_.clear();
    ftsIndexedIdsCache_.reserve(docIds.size());
    for (int64_t id : docIds) {
        ftsIndexedIdsCache_.insert(id);
    }
    ftsIndexedIdsCacheReady_.store(true, std::memory_order_release);
}

void MetadataRepository::noteFtsIndexedId(int64_t docId) const {
    if (!ftsIndexedIdsCacheReady_.load(std::memory_order_acquire)) {
        return;
    }
    std::unique_lock<std::shared_mutex> lock(ftsIndexedIdsCacheMutex_);
    ftsIndexedIdsCache_.insert(docId);
}

void MetadataRepository::eraseFtsIndexedId(int64_t docId) const {
    if (!ftsIndexedIdsCacheReady_.load(std::memory_order_acquire)) {
        return;
    }
    std::unique_lock<std::shared_mutex> lock(ftsIndexedIdsCacheMutex_);
    ftsIndexedIdsCache_.erase(docId);
}

void MetadataRepository::updateQueryCache(const std::string& key,
                                          const SearchResults& results) const {
    std::lock_guard<std::mutex> lock(queryCacheMutex_);
    auto snapshot = std::atomic_load_explicit(&queryCacheSnapshot_, std::memory_order_acquire);
    if (!snapshot) {
        snapshot = std::make_shared<QueryCacheMap>();
    }
    auto updated = std::make_shared<QueryCacheMap>(*snapshot);
    QueryCacheEntry entry;
    entry.results = results;
    entry.timestamp = std::chrono::steady_clock::now();
    entry.hits = 0;

    (*updated)[key] = std::move(entry);
    if (updated->size() > kQueryCacheCapacity) {
        auto victim = std::min_element(updated->begin(), updated->end(),
                                       [](const auto& lhs, const auto& rhs) {
                                           auto lhsHits = lhs.second.hits;
                                           auto rhsHits = rhs.second.hits;
                                           if (lhsHits != rhsHits)
                                               return lhsHits < rhsHits;
                                           return lhs.second.timestamp < rhs.second.timestamp;
                                       });
        if (victim != updated->end()) {
            updated->erase(victim);
        }
    }
    std::atomic_store_explicit(&queryCacheSnapshot_, std::move(updated), std::memory_order_release);
}

MetadataRepository::MetadataRepository(ConnectionPool& pool, ConnectionPool* readPool)
    : pool_(pool), readPool_(readPool) {
    // Ensure database schema is initialized
    auto initResult = pool_.withConnection([](Database& db) -> Result<void> {
        // Create migration manager and apply all migrations
        MigrationManager manager(db);
        auto initResult = manager.initialize();
        if (!initResult) {
            spdlog::error("Failed to initialize migration system: {}", initResult.error().message);
            return initResult;
        }

        // Register all YAMS metadata migrations
        manager.registerMigrations(YamsMetadataMigrations::getAllMigrations());

        auto currentVersionRes = manager.getCurrentVersion();
        if (!currentVersionRes) {
            spdlog::error("Failed to get current DB version: {}",
                          currentVersionRes.error().message);
            return currentVersionRes.error();
        }
        int currentVersion = currentVersionRes.value();
        int latestVersion = manager.getLatestVersion();
        spdlog::info("Database schema version: {}, latest available: {}", currentVersion,
                     latestVersion);

        if (currentVersion < latestVersion) {
            spdlog::info("Pending migrations found. Attempting to upgrade...");
        }

        // Apply migrations
        auto migrateResult = manager.migrate();
        if (!migrateResult) {
            spdlog::error("Failed to run database migrations: {}", migrateResult.error().message);
            return Error{migrateResult.error()};
        }

        auto newVersionRes = manager.getCurrentVersion();
        if (newVersionRes && newVersionRes.value() > currentVersion) {
            spdlog::info("Database successfully migrated to version {}", newVersionRes.value());
        } else if (newVersionRes) {
            spdlog::info("Database schema is up to date at version {}", newVersionRes.value());
        }

        spdlog::info("Database schema initialized successfully");
        return Result<void>();
    });

    if (!initResult.has_value()) {
        spdlog::warn("Failed to initialize database schema: {}", initResult.error().message);
        // Continue anyway - the error will be caught when operations are attempted
    }

    auto featureResult = pool_.withConnection([this](Database& db) -> Result<void> {
        std::unordered_set<std::string> columns;
        if (auto tableInfo = db.prepare("PRAGMA table_info(documents)"); tableInfo) {
            Statement stmt = std::move(tableInfo).value();
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                columns.insert(stmt.getString(1));
            }
        } else {
            return tableInfo.error();
        }

        const bool hasPrefix = columns.count("path_prefix") > 0;
        const bool hasReverse = columns.count("reverse_path") > 0;
        const bool hasHash = columns.count("path_hash") > 0;
        const bool hasParent = columns.count("parent_hash") > 0;
        const bool hasDepth = columns.count("path_depth") > 0;
        hasPathIndexing_ = hasPrefix && hasReverse && hasHash && hasParent && hasDepth;

        auto ftsResult = db.tableExists("documents_path_fts");
        if (ftsResult) {
            pathFtsAvailable_ = ftsResult.value();
        } else {
            pathFtsAvailable_ = false;
            spdlog::debug("MetadataRepository: failed to detect documents_path_fts table: {}",
                          ftsResult.error().message);
        }

        return Result<void>();
    });

    if (!featureResult) {
        spdlog::warn("MetadataRepository: failed to detect path indexing features: {}",
                     featureResult.error().message);
    }
    spdlog::info("MetadataRepository: hasPathIndexing={} pathFtsAvailable={}", hasPathIndexing_,
                 pathFtsAvailable_);
}

// Destructor must be defined in cpp file because of forward-declared CorpusStats in unique_ptr
MetadataRepository::~MetadataRepository() {
    shutdown();
}

void MetadataRepository::shutdown() {
    std::lock_guard<std::mutex> lock(symspellInitMutex_);
    symspellIndex_.reset();
    symspellDb_.reset();
    symspellInitialized_.store(false, std::memory_order_release);
    graphComponent_.reset();
    kgStore_.reset();
}

// Document operations
Result<void>
MetadataRepository::batchInsertContentAndIndex(const std::vector<BatchContentEntry>& entries) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::batchInsertContentAndIndex");
    YAMS_PLOT("metadata_repo::batch_content_entries", static_cast<int64_t>(entries.size()));
    if (entries.empty()) {
        return Result<void>();
    }

    struct BatchContentDelta {
        uint64_t newlyExtracted{0};
        uint64_t newlyIndexed{0};
        std::vector<int64_t> indexedDocIds;
    };

    auto result = executeQuery<BatchContentDelta>([&](Database& db) -> Result<BatchContentDelta> {
        // Track counter deltas per attempt. executeQueryOnPool may retry the lambda
        // after lock/constraint races; deltas from failed attempts must not leak into
        // the final cache update.
        BatchContentDelta delta;
        delta.indexedDocIds.reserve(entries.size());
        YAMS_TRY(beginTransaction(db));
        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        // Check if FTS5 is available once
        YAMS_TRY_UNWRAP(hasFts5, db.hasFTS5());

        // Prepare cached statements for reuse
        YAMS_TRY_UNWRAP(contentStmtResult,
                        db.prepareCached(R"(
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

        std::optional<CachedStatement> ftsStmtOpt;
        if (hasFts5) {
            YAMS_TRY_UNWRAP(ftsStmtResult,
                            db.prepareCached(R"(
                INSERT OR REPLACE INTO documents_fts (rowid, content, title)
                VALUES (?, ?, ?)
            )"));
            ftsStmtOpt = std::move(ftsStmtResult);
        }

        // Pre-check statement to determine counter increments before the combined UPDATE.
        YAMS_TRY_UNWRAP(checkStmtResult, db.prepareCached(hasFts5 ? R"(
            SELECT COALESCE(content_extracted, 0),
                   CASE WHEN EXISTS(
                       SELECT 1 FROM documents_fts WHERE rowid = documents.id
                   ) THEN 1 ELSE 0 END
            FROM documents WHERE id = ?
        )"
                                                           : R"(
            SELECT COALESCE(content_extracted, 0), 0
            FROM documents WHERE id = ?
        )"));
        auto& checkStmt = *checkStmtResult;

        // Combined UPDATE: sets all extraction fields in a single statement
        // (replaces 3 separate conditional UPDATEs per document).
        YAMS_TRY_UNWRAP(combinedStmtResult,
                        db.prepareCached(R"(
            UPDATE documents
            SET content_extracted = 1,
                extraction_status = 'Success',
                extraction_error = NULL,
                repair_status = 'completed',
                repair_attempted_at = unixepoch(),
                repair_attempts = repair_attempts + 1
            WHERE id = ?
        )"));
        auto& combinedStmt = *combinedStmtResult;

        // Process each entry
        constexpr size_t kMaxTextBytes = size_t{16} * 1024 * 1024; // 16 MiB
        std::string contentStorage, titleStorage;
        for (const auto& entry : entries) {
            std::string_view contentView = entry.contentText;
            if (contentView.size() > kMaxTextBytes) {
                contentView = contentView.substr(0, kMaxTextBytes);
            }
            contentStorage.clear();
            titleStorage.clear();
            const auto sanitizedContent = common::ensureValidUtf8(contentView, contentStorage);
            const auto sanitizedTitle = common::ensureValidUtf8(entry.title, titleStorage);

            bool wasExtracted = entry.priorContentExtracted;
            // Without FTS5 there is no documents_fts state to reconcile, so avoid
            // double-counting "indexed" documents in non-FTS builds.
            bool wasIndexed = !hasFts5;
            if (!entry.priorStateKnown || hasFts5) {
                YAMS_TRY(checkStmt.reset());
                YAMS_TRY(checkStmt.clearBindings());
                YAMS_TRY(checkStmt.bind(1, entry.documentId));
                auto checkStep = checkStmt.step();
                if (!checkStep) {
                    return checkStep.error();
                }
                if (checkStep.value()) {
                    wasExtracted = checkStmt.getInt(0) == 1;
                    wasIndexed = checkStmt.getInt(1) == 1;
                } else {
                    // Post-ingest work can race with document deletion or duplicate-content
                    // resolution. Do not create orphan content/FTS rows or advance counters
                    // for a document row that no longer exists.
                    spdlog::debug("MetadataRepository: skipping stale batch content entry for "
                                  "missing document id {}",
                                  entry.documentId);
                    continue;
                }
            }

            // 1. Insert content
            YAMS_TRY(contentStmt.reset());
            YAMS_TRY(contentStmt.clearBindings());
            YAMS_TRY(contentStmt.bindAll(entry.documentId, sanitizedContent,
                                         static_cast<int64_t>(sanitizedContent.length()),
                                         entry.extractionMethod, entry.language));
            YAMS_TRY(contentStmt.execute());

            // 2. Insert FTS index with field-weighted content
            // Title tokens repeated 3x, abstract tokens 2x, body tokens 1x.
            // This gives BM25 natural field weighting without schema changes.
            if (hasFts5 && ftsStmtOpt) {
                auto& ftsStmt = **ftsStmtOpt;
                YAMS_TRY(ftsStmt.reset());
                YAMS_TRY(ftsStmt.clearBindings());

                // Build boosted content: title (3x) + abstract (2x) + body
                std::string boosted;
                constexpr std::size_t kMaxBoostedBytes = kMaxTextBytes;
                boosted.reserve(
                    std::min<std::size_t>(sanitizedContent.size() + 512, kMaxBoostedBytes));

                auto append_repeated = [&](std::string_view text, int times) {
                    if (text.empty())
                        return;
                    for (int t = 0; t < times; ++t) {
                        if (boosted.size() >= kMaxBoostedBytes)
                            break;
                        if (!boosted.empty() && boosted.back() != ' ')
                            boosted.push_back(' ');
                        std::size_t avail = kMaxBoostedBytes - boosted.size();
                        if (avail < text.size() + 1) {
                            boosted.append(text.substr(0, std::min(text.size(), avail)));
                            break;
                        }
                        boosted.append(text);
                    }
                };

                append_repeated(sanitizedTitle, 3);
                if (!entry.abstract.empty()) {
                    std::string absStorage;
                    auto sanitizedAbstract = common::ensureValidUtf8(entry.abstract, absStorage);
                    append_repeated(sanitizedAbstract, 2);
                }
                if (!boosted.empty() && boosted.back() != ' ')
                    boosted.push_back(' ');
                std::size_t bodySpace =
                    kMaxBoostedBytes > boosted.size() ? kMaxBoostedBytes - boosted.size() : 0;
                boosted.append(
                    sanitizedContent.substr(0, std::min(bodySpace, sanitizedContent.size())));

                YAMS_TRY(ftsStmt.bindAll(entry.documentId, boosted, sanitizedTitle));
                YAMS_TRY(ftsStmt.execute());
                delta.indexedDocIds.push_back(entry.documentId);
            }

            YAMS_TRY(combinedStmt.reset());
            YAMS_TRY(combinedStmt.clearBindings());
            YAMS_TRY(combinedStmt.bind(1, entry.documentId));
            YAMS_TRY(combinedStmt.execute());
            if (!wasExtracted) {
                delta.newlyExtracted++;
            }
            if (!wasIndexed) {
                delta.newlyIndexed++;
            }
        }

        YAMS_TRY(commitOrRollback(db));
        committed = true;
        rollback.dismiss();

        return delta;
    });

    if (result) {
        invalidateQueryCache();
        // Signal corpus stats stale - batch content affects extractionCoverage stats
        signalCorpusStatsStale();

        const auto& delta = result.value();
        if (delta.newlyExtracted > 0) {
            cachedExtractedCount_.fetch_add(delta.newlyExtracted, std::memory_order_relaxed);
        }
        if (delta.newlyIndexed > 0) {
            cachedIndexedCount_.fetch_add(delta.newlyIndexed, std::memory_order_relaxed);
        }
        for (const auto docId : delta.indexedDocIds) {
            noteFtsIndexedId(docId);
        }
        YAMS_PLOT("metadata_repo::batch_content_newly_extracted",
                  static_cast<int64_t>(delta.newlyExtracted));
        YAMS_PLOT("metadata_repo::batch_content_newly_indexed",
                  static_cast<int64_t>(delta.newlyIndexed));

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

std::string
MetadataRepository::generateValueCountsCacheKey(const std::vector<std::string>& keys,
                                                const DocumentQueryOptions& options) const {
    std::string cacheKey;
    cacheKey.reserve(256);

    // Add keys (sorted for consistency)
    std::vector<std::string> sortedKeys = keys;
    std::sort(sortedKeys.begin(), sortedKeys.end());
    for (const auto& k : sortedKeys) {
        cacheKey += k;
        cacheKey += ';';
    }

    // Add relevant filter params that affect the query
    if (options.exactPath) {
        cacheKey += "ep:";
        cacheKey += *options.exactPath;
        cacheKey += ';';
    }
    if (options.pathPrefix) {
        cacheKey += "pp:";
        cacheKey += *options.pathPrefix;
        cacheKey += ';';
    }
    if (options.containsFragment) {
        cacheKey += "cf:";
        cacheKey += *options.containsFragment;
        cacheKey += ';';
    }
    if (options.fileName) {
        cacheKey += "fn:";
        cacheKey += *options.fileName;
        cacheKey += ';';
    }
    if (options.extension) {
        cacheKey += "ext:";
        cacheKey += *options.extension;
        cacheKey += ';';
    }
    if (!options.extensions.empty()) {
        cacheKey += "exts:";
        for (const auto& ext : options.extensions) {
            cacheKey += ext;
            cacheKey += ',';
        }
        cacheKey += ';';
    }
    if (options.mimeType) {
        cacheKey += "mt:";
        cacheKey += *options.mimeType;
        cacheKey += ';';
    }
    if (options.textOnly)
        cacheKey += "to;";
    if (options.binaryOnly)
        cacheKey += "bo;";
    if (options.createdAfter)
        cacheKey += "ca:" + std::to_string(*options.createdAfter) + ";";
    if (options.createdBefore)
        cacheKey += "cb:" + std::to_string(*options.createdBefore) + ";";
    if (options.modifiedAfter)
        cacheKey += "ma:" + std::to_string(*options.modifiedAfter) + ";";
    if (options.modifiedBefore)
        cacheKey += "mb:" + std::to_string(*options.modifiedBefore) + ";";
    if (options.indexedAfter)
        cacheKey += "ia:" + std::to_string(*options.indexedAfter) + ";";
    if (options.indexedBefore)
        cacheKey += "ib:" + std::to_string(*options.indexedBefore) + ";";
    if (options.changedSince)
        cacheKey += "cs:" + std::to_string(*options.changedSince) + ";";
    if (!options.tags.empty()) {
        cacheKey += "tags:";
        for (const auto& tag : options.tags) {
            cacheKey += tag;
            cacheKey += ',';
        }
        cacheKey += ';';
    }
    if (options.likePattern) {
        cacheKey += "lp:";
        cacheKey += *options.likePattern;
        cacheKey += ';';
    }
    if (options.prefixIsDirectory)
        cacheKey += "pid;";
    if (!options.includeSubdirectories)
        cacheKey += "nosd;";
    if (options.containsUsesFts)
        cacheKey += "fts;";

    return cacheKey;
}

// Helper to check if any document-level filter is set (requires JOIN with documents table)
static bool requiresDocumentJoin(const DocumentQueryOptions& options) {
    return options.exactPath.has_value() || options.pathPrefix.has_value() ||
           options.containsFragment.has_value() || options.likePattern.has_value() ||
           options.fileName.has_value() || options.extension.has_value() ||
           !options.extensions.empty() || options.mimeType.has_value() || options.textOnly ||
           options.binaryOnly || options.createdAfter.has_value() ||
           options.createdBefore.has_value() || options.modifiedAfter.has_value() ||
           options.modifiedBefore.has_value() || options.indexedAfter.has_value() ||
           options.indexedBefore.has_value() || options.changedSince.has_value() ||
           !options.tags.empty();
}

Result<std::unordered_map<std::string, std::vector<MetadataValueCount>>>
MetadataRepository::getMetadataValueCounts(const std::vector<std::string>& keys,
                                           const DocumentQueryOptions& options) {
    if (keys.empty()) {
        return std::unordered_map<std::string, std::vector<MetadataValueCount>>{};
    }

    // Generate cache key from parameters
    std::string cacheKey = generateValueCountsCacheKey(keys, options);

    // Check cache under shared lock
    {
        std::shared_lock<std::shared_mutex> lock(valueCountsCacheMutex_);
        if (cachedValueCounts_) {
            auto changeCount = metadataChangeCounter_.load(std::memory_order_acquire);
            if (cachedValueCounts_->metadataChangeCount == changeCount) {
                auto it = cachedValueCounts_->entries.find(cacheKey);
                if (it != cachedValueCounts_->entries.end()) {
                    auto& [cachedAt, cachedResult] = it->second;
                    if (std::chrono::steady_clock::now() - cachedAt < kValueCountsCacheTtl) {
                        spdlog::debug(
                            "MetadataRepository::getMetadataValueCounts cache hit for {} keys",
                            keys.size());
                        return cachedResult; // Cache hit
                    }
                }
            }
        }
    }

    // Cache miss - execute query
    auto queryResult =
        executeReadQuery<std::unordered_map<std::string, std::vector<MetadataValueCount>>>(
            [&](Database& db)
                -> Result<std::unordered_map<std::string, std::vector<MetadataValueCount>>> {
                const bool needsDocumentJoin = requiresDocumentJoin(options);
                const bool joinFtsForContains =
                    needsDocumentJoin && options.containsFragment && options.containsUsesFts &&
                    !options.containsFragment->empty() && pathFtsAvailable_;

                std::string sql;
                auto runResult =
                    runMetadataValueCountQuery(db, keys, options, needsDocumentJoin,
                                               joinFtsForContains, hasPathIndexing_, &sql);
                if (!runResult) {
                    if (joinFtsForContains && options.containsUsesFts) {
                        spdlog::debug("MetadataRepository::getMetadataValueCounts query failed "
                                      "(falling back to without FTS): {}\nSQL: {}",
                                      runResult.error().message, sql);
                        auto fallbackOpts = options;
                        fallbackOpts.containsUsesFts = false;
                        return getMetadataValueCounts(keys, fallbackOpts);
                    }
                    spdlog::error("MetadataRepository::getMetadataValueCounts query failed: "
                                  "{}\nSQL: {}",
                                  runResult.error().message, sql);
                }
                return runResult;
            });

    // Update cache on success
    if (queryResult) {
        std::unique_lock<std::shared_mutex> lock(valueCountsCacheMutex_);
        if (!cachedValueCounts_) {
            cachedValueCounts_ = std::make_unique<MetadataValueCountsCache>();
        }
        // Clear cache if metadataChangeCounter changed (data was modified)
        auto currentChangeCount = metadataChangeCounter_.load(std::memory_order_acquire);
        if (cachedValueCounts_->metadataChangeCount != currentChangeCount) {
            cachedValueCounts_->entries.clear();
            cachedValueCounts_->metadataChangeCount = currentChangeCount;
        }
        cachedValueCounts_->entries[cacheKey] = {std::chrono::steady_clock::now(),
                                                 queryResult.value()};
        spdlog::debug("MetadataRepository::getMetadataValueCounts cached result for {} keys",
                      keys.size());
    }

    return queryResult;
}

Result<int64_t>
MetadataRepository::upsertSemanticDuplicateGroup(const SemanticDuplicateGroup& group) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO semantic_duplicate_groups (
                group_key, algorithm_version, status, review_state,
                canonical_document_id, member_count, max_pair_score, threshold,
                evidence_json, created_at, updated_at, last_computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_key) DO UPDATE SET
                algorithm_version = excluded.algorithm_version,
                status = excluded.status,
                review_state = excluded.review_state,
                canonical_document_id = excluded.canonical_document_id,
                member_count = excluded.member_count,
                max_pair_score = excluded.max_pair_score,
                threshold = excluded.threshold,
                evidence_json = excluded.evidence_json,
                updated_at = excluded.updated_at,
                last_computed_at = excluded.last_computed_at
        )");
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        YAMS_TRY(stmt.bind(1, group.groupKey));
        YAMS_TRY(stmt.bind(2, group.algorithmVersion));
        YAMS_TRY(stmt.bind(3, group.status));
        YAMS_TRY(stmt.bind(4, group.reviewState));
        YAMS_TRY(bindOptionalInt64(stmt, 5, group.canonicalDocumentId));
        YAMS_TRY(stmt.bind(6, group.memberCount));
        YAMS_TRY(stmt.bind(7, group.maxPairScore));
        YAMS_TRY(bindOptionalDouble(stmt, 8, group.threshold));
        YAMS_TRY(bindNullableText(stmt, 9, group.evidenceJson));
        YAMS_TRY(stmt.bind(10, group.createdAt));
        YAMS_TRY(stmt.bind(11, group.updatedAt));
        YAMS_TRY(stmt.bind(12, group.lastComputedAt));
        YAMS_TRY(stmt.execute());

        auto selectResult =
            db.prepare("SELECT id FROM semantic_duplicate_groups WHERE group_key = ?");
        if (!selectResult)
            return selectResult.error();
        Statement selectStmt = std::move(selectResult).value();
        YAMS_TRY(selectStmt.bind(1, group.groupKey));
        YAMS_TRY_UNWRAP(hasRow, selectStmt.step());
        if (!hasRow) {
            return Error{ErrorCode::NotFound, "Failed to resolve semantic duplicate group id"};
        }
        return selectStmt.getInt64(0);
    });
}

Result<void> MetadataRepository::replaceSemanticDuplicateGroupMembers(
    int64_t groupId, const std::vector<SemanticDuplicateGroupMember>& members) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        YAMS_TRY(beginTransactionWithRetry(db));
        auto rollback = scope_exit([&] { rollbackIgnoringErrors(db); });

        YAMS_TRY_UNWRAP(
            deleteStmt,
            db.prepare("DELETE FROM semantic_duplicate_group_members WHERE group_id = ?"));
        YAMS_TRY(deleteStmt.bind(1, groupId));
        YAMS_TRY(deleteStmt.execute());

        YAMS_TRY_UNWRAP(insertStmt, db.prepare(R"(
            INSERT INTO semantic_duplicate_group_members (
                group_id, document_id, role, similarity_to_canonical,
                title_overlap, path_overlap, pair_score, decision, reason,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )"));

        for (const auto& member : members) {
            YAMS_TRY(insertStmt.reset());
            YAMS_TRY(bindSemanticDuplicateGroupMemberInsert(insertStmt, groupId, member));
            YAMS_TRY(insertStmt.execute());
        }

        YAMS_TRY(commitOrRollback(db));
        rollback.dismiss();
        return Result<void>();
    });
}

Result<std::optional<SemanticDuplicateGroup>>
MetadataRepository::getSemanticDuplicateGroupByKey(const std::string& groupKey) {
    return executeReadQuery<std::optional<SemanticDuplicateGroup>>(
        [&](Database& db) -> Result<std::optional<SemanticDuplicateGroup>> {
            auto stmtResult = db.prepare(R"(
                SELECT id, group_key, algorithm_version, status, review_state,
                       canonical_document_id, member_count, max_pair_score, threshold,
                       evidence_json, created_at, updated_at, last_computed_at
                FROM semantic_duplicate_groups
                WHERE group_key = ?
            )");
            if (!stmtResult)
                return stmtResult.error();
            Statement stmt = std::move(stmtResult).value();
            YAMS_TRY(stmt.bind(1, groupKey));
            YAMS_TRY_UNWRAP(hasRow, stmt.step());
            if (!hasRow)
                return std::optional<SemanticDuplicateGroup>{};
            return std::optional<SemanticDuplicateGroup>{mapSemanticDuplicateGroupRow(stmt)};
        });
}

Result<std::vector<SemanticDuplicateGroup>>
MetadataRepository::listSemanticDuplicateGroups(int limit) {
    return executeReadQuery<std::vector<SemanticDuplicateGroup>>(
        [&](Database& db) -> Result<std::vector<SemanticDuplicateGroup>> {
            auto stmtResult = db.prepare(R"(
                SELECT id, group_key, algorithm_version, status, review_state,
                       canonical_document_id, member_count, max_pair_score, threshold,
                       evidence_json, created_at, updated_at, last_computed_at
                FROM semantic_duplicate_groups
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
            )");
            if (!stmtResult)
                return stmtResult.error();
            Statement stmt = std::move(stmtResult).value();
            YAMS_TRY(stmt.bind(1, limit));

            std::vector<SemanticDuplicateGroup> groups;
            while (true) {
                YAMS_TRY_UNWRAP(hasRow, stmt.step());
                if (!hasRow)
                    break;
                groups.push_back(mapSemanticDuplicateGroupRow(stmt));
            }
            return groups;
        });
}

Result<std::unordered_map<int64_t, SemanticDuplicateGroupDetail>>
MetadataRepository::getSemanticDuplicateGroupsForDocuments(std::span<const int64_t> documentIds) {
    if (documentIds.empty()) {
        return std::unordered_map<int64_t, SemanticDuplicateGroupDetail>{};
    }

    return executeReadQuery<std::unordered_map<int64_t, SemanticDuplicateGroupDetail>>(
        [&](Database& db) -> Result<std::unordered_map<int64_t, SemanticDuplicateGroupDetail>> {
            std::string placeholders;
            placeholders.reserve(documentIds.size() * 2);
            for (size_t i = 0; i < documentIds.size(); ++i) {
                if (i > 0)
                    placeholders += ',';
                placeholders += '?';
            }

            auto groupStmtResult = db.prepare(
                "SELECT DISTINCT g.id, g.group_key, g.algorithm_version, g.status, g.review_state, "
                "g.canonical_document_id, g.member_count, g.max_pair_score, g.threshold, "
                "g.evidence_json, g.created_at, g.updated_at, g.last_computed_at "
                "FROM semantic_duplicate_groups g "
                "JOIN semantic_duplicate_group_members m ON m.group_id = g.id "
                "WHERE m.document_id IN (" +
                placeholders + ") ORDER BY g.updated_at DESC, g.id DESC");
            if (!groupStmtResult)
                return groupStmtResult.error();
            Statement groupStmt = std::move(groupStmtResult).value();
            for (size_t i = 0; i < documentIds.size(); ++i) {
                YAMS_TRY(groupStmt.bind(static_cast<int>(i + 1), documentIds[i]));
            }

            std::unordered_map<int64_t, SemanticDuplicateGroupDetail> groupsById;
            while (true) {
                YAMS_TRY_UNWRAP(hasRow, groupStmt.step());
                if (!hasRow)
                    break;
                auto group = mapSemanticDuplicateGroupRow(groupStmt);
                groupsById.emplace(group.id, SemanticDuplicateGroupDetail{std::move(group), {}});
            }

            if (groupsById.empty()) {
                return groupsById;
            }

            std::string memberPlaceholders;
            memberPlaceholders.reserve(groupsById.size() * 2);
            std::vector<int64_t> groupIds;
            groupIds.reserve(groupsById.size());
            for (const auto& [groupId, _] : groupsById) {
                if (!memberPlaceholders.empty())
                    memberPlaceholders += ',';
                memberPlaceholders += '?';
                groupIds.push_back(groupId);
            }

            auto memberStmtResult = db.prepare(
                "SELECT id, group_id, document_id, role, similarity_to_canonical, title_overlap, "
                "path_overlap, pair_score, decision, reason, created_at, updated_at "
                "FROM semantic_duplicate_group_members WHERE group_id IN (" +
                memberPlaceholders + ") ORDER BY group_id ASC, id ASC");
            if (!memberStmtResult)
                return memberStmtResult.error();
            Statement memberStmt = std::move(memberStmtResult).value();
            for (size_t i = 0; i < groupIds.size(); ++i) {
                YAMS_TRY(memberStmt.bind(static_cast<int>(i + 1), groupIds[i]));
            }

            while (true) {
                YAMS_TRY_UNWRAP(hasRow, memberStmt.step());
                if (!hasRow)
                    break;
                auto member = mapSemanticDuplicateGroupMemberRow(memberStmt);
                auto it = groupsById.find(member.groupId);
                if (it != groupsById.end()) {
                    it->second.members.push_back(std::move(member));
                }
            }

            std::unordered_map<int64_t, SemanticDuplicateGroupDetail> byDocumentId;
            for (auto& [groupId, detail] : groupsById) {
                (void)groupId;
                for (const auto& member : detail.members) {
                    byDocumentId.emplace(member.documentId, detail);
                }
            }
            return byDocumentId;
        });
}

Result<void> MetadataRepository::updateSemanticDuplicateGroupStatus(const std::string& groupKey,
                                                                    const std::string& status) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(
            "UPDATE semantic_duplicate_groups SET status = ?, updated_at = ? WHERE group_key = ?");
        if (!stmtResult)
            return stmtResult.error();
        Statement stmt = std::move(stmtResult).value();
        YAMS_TRY(stmt.bind(1, status));
        YAMS_TRY(stmt.bind(2, nowSysSeconds()));
        YAMS_TRY(stmt.bind(3, groupKey));
        YAMS_TRY(stmt.execute());
        return Result<void>();
    });
}

namespace {
struct FtsIndexDelta {
    bool indexed{false};
    bool newlyIndexed{false};
};

Result<void> indexDocumentContentImpl(Database& db, int64_t documentId, const std::string& title,
                                      const std::string& content,
                                      [[maybe_unused]] const std::string& contentType,
                                      bool verifyDocumentExists) {
    // First check if FTS5 is available
    auto fts5Result = db.hasFTS5();
    if (!fts5Result)
        return fts5Result.error();

    if (!fts5Result.value()) {
        spdlog::warn("FTS5 not available, skipping content indexing");
        return {};
    }

    if (verifyDocumentExists) {
        // Verify document exists before indexing (FTS5 doesn't enforce foreign keys)
        auto checkStmt = db.prepare("SELECT COUNT(*) FROM documents WHERE id = ?");
        if (!checkStmt)
            return checkStmt.error();
        Statement checkS = std::move(checkStmt).value();
        auto checkBind = checkS.bind(1, documentId);
        if (!checkBind)
            return checkBind.error();
        auto checkStep = checkS.step();
        if (!checkStep)
            return checkStep.error();
        if (checkS.getInt(0) == 0) {
            return Error{ErrorCode::NotFound,
                         "Document ID " + std::to_string(documentId) +
                             " not found - cannot index content for non-existent document"};
        }
    }

    std::string contentStorage, titleStorage;
    const auto sanitizedContent = common::ensureValidUtf8(content, contentStorage);
    const auto sanitizedTitle = common::ensureValidUtf8(title, titleStorage);

    // Use INSERT OR REPLACE for atomic upsert (avoids race condition in delete-then-insert)
    // Note: content_type removed from FTS5 in migration v18 - never used in MATCH queries
    auto stmtResult = db.prepare(R"(
             INSERT OR REPLACE INTO documents_fts (rowid, content, title)
             VALUES (?, ?, ?)
         )");

    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto bindResult = stmt.bindAll(documentId, sanitizedContent, sanitizedTitle);
    if (!bindResult)
        return bindResult.error();

    auto execResult = stmt.execute();
    if (execResult) {
        if (spdlog::should_log(spdlog::level::debug)) {
            spdlog::debug("[FTS5 Index] Inserted rowid={} title='{}' contentLen={}", documentId,
                          sanitizedTitle.substr(0, 30), sanitizedContent.size());
        }
    } else {
        spdlog::warn("[FTS5 Index] Insert failed for rowid={}: {}", documentId,
                     execResult.error().message);
    }
    return execResult;
}

Result<FtsIndexDelta> indexDocumentContentWithDelta(Database& db, int64_t documentId,
                                                    const std::string& title,
                                                    const std::string& content,
                                                    const std::string& contentType,
                                                    bool verifyDocumentExists) {
    YAMS_TRY(beginTransactionWithRetry(db));

    auto fts5Result = db.hasFTS5();
    if (!fts5Result) {
        db.execute("ROLLBACK");
        return fts5Result.error();
    }
    if (!fts5Result.value()) {
        db.execute("ROLLBACK");
        return FtsIndexDelta{};
    }

    bool hadFtsEntry = false;
    auto checkStmt = db.prepare("SELECT 1 FROM documents_fts WHERE rowid = ?");
    if (!checkStmt) {
        db.execute("ROLLBACK");
        return checkStmt.error();
    }
    auto& stmt = checkStmt.value();
    if (auto bind = stmt.bind(1, documentId); !bind) {
        db.execute("ROLLBACK");
        return bind.error();
    }
    auto step = stmt.step();
    if (!step) {
        db.execute("ROLLBACK");
        return step.error();
    }
    hadFtsEntry = step.value();

    auto indexResult =
        indexDocumentContentImpl(db, documentId, title, content, contentType, verifyDocumentExists);
    if (!indexResult) {
        db.execute("ROLLBACK");
        return indexResult.error();
    }

    auto commit = db.execute("COMMIT");
    if (!commit) {
        db.execute("ROLLBACK");
        return commit.error();
    }

    return FtsIndexDelta{.indexed = true, .newlyIndexed = !hadFtsEntry};
}
} // namespace

Result<void> MetadataRepository::indexDocumentContent(int64_t documentId, const std::string& title,
                                                      const std::string& content,
                                                      const std::string& contentType) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::indexDocumentContent");
    auto result = executeQuery<FtsIndexDelta>([&](Database& db) -> Result<FtsIndexDelta> {
        return indexDocumentContentWithDelta(db, documentId, title, content, contentType,
                                             /*verifyDocumentExists=*/true);
    });

    if (result) {
        const auto& delta = result.value();
        if (delta.newlyIndexed) {
            cachedIndexedCount_.fetch_add(1, std::memory_order_relaxed);
        }
        if (delta.indexed) {
            noteFtsIndexedId(documentId);
        }
        invalidateQueryCache();
        return Result<void>();
    }
    return result.error();
}

Result<void> MetadataRepository::indexDocumentContentTrusted(int64_t documentId,
                                                             const std::string& title,
                                                             const std::string& content,
                                                             const std::string& contentType) {
    auto result = executeQuery<FtsIndexDelta>([&](Database& db) -> Result<FtsIndexDelta> {
        return indexDocumentContentWithDelta(db, documentId, title, content, contentType,
                                             /*verifyDocumentExists=*/false);
    });

    if (result) {
        const auto& delta = result.value();
        if (delta.newlyIndexed) {
            cachedIndexedCount_.fetch_add(1, std::memory_order_relaxed);
        }
        if (delta.indexed) {
            noteFtsIndexedId(documentId);
        }
        invalidateQueryCache();
        return Result<void>();
    }
    return result.error();
}

Result<bool> MetadataRepository::hasFtsEntry(int64_t documentId) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::hasFtsEntry");
    return executeReadQuery<bool>([&](Database& db) -> Result<bool> {
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();
        if (!fts5Result.value())
            return false; // FTS5 not available, no entry possible

        auto stmtResult = db.prepare("SELECT rowid FROM documents_fts WHERE rowid = ?");
        if (!stmtResult)
            return stmtResult.error();
        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult)
            return bindResult.error();
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        return stepResult.value(); // true if row was found
    });
}

Result<std::unordered_set<int64_t>> MetadataRepository::getFts5IndexedRowIdSet() {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getFts5IndexedRowIdSet");
    return executeReadQuery<std::unordered_set<int64_t>>(
        [&](Database& db) -> Result<std::unordered_set<int64_t>> {
            auto fts5Result = db.hasFTS5();
            if (!fts5Result)
                return fts5Result.error();
            if (!fts5Result.value())
                return std::unordered_set<int64_t>{}; // FTS5 not available

            auto stmtResult = db.prepare("SELECT rowid FROM documents_fts");
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::unordered_set<int64_t> ids;

            for (;;) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;
                ids.insert(stmt.getInt64(0));
            }

            return ids;
        });
}

Result<void> MetadataRepository::removeFromIndex(int64_t documentId) {
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        // First check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();

        if (!fts5Result.value()) {
            return {};
        }

        auto stmtResult = db.prepare("DELETE FROM documents_fts WHERE rowid = ?");
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });

    if (result) {
        eraseFtsIndexedId(documentId);
        invalidateQueryCache();
    }
    return result;
}

Result<void> MetadataRepository::removeFromIndexByHash(const std::string& hash) {
    int64_t removedDocId = 0;
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        // First check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();

        if (!fts5Result.value()) {
            return {};
        }

        // Get document ID from hash first
        auto stmtResult = db.prepare("SELECT id FROM documents WHERE sha256_hash = ?");
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, hash);
        if (!bindResult)
            return bindResult.error();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();

        if (!stepResult.value()) {
            // Document not found - treat as success (already removed/doesn't exist)
            return {};
        }

        int64_t docId = stmt.getInt64(0);

        // Now remove from FTS5 index
        auto delStmtResult = db.prepare("DELETE FROM documents_fts WHERE rowid = ?");
        if (!delStmtResult)
            return delStmtResult.error();

        Statement delStmt = std::move(delStmtResult).value();
        auto delBindResult = delStmt.bind(1, docId);
        if (!delBindResult)
            return delBindResult.error();

        auto execResult = delStmt.execute();
        if (execResult) {
            removedDocId = docId;
        }
        return execResult;
    });

    if (result) {
        if (removedDocId > 0) {
            eraseFtsIndexedId(removedDocId);
        }
        invalidateQueryCache();
    }
    return result;
}

Result<size_t>
MetadataRepository::removeFromIndexByHashBatch(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return Result<size_t>(0);
    }

    std::vector<int64_t> removedDocIds;
    auto result = executeQuery<size_t>([&](Database& db) -> Result<size_t> {
        // Begin transaction for batch operation
        auto beginResult = beginTransactionWithRetry(db);
        if (!beginResult) {
            return beginResult.error();
        }

        // Check if FTS5 is available once
        auto fts5Result = db.hasFTS5();
        if (!fts5Result) {
            rollbackIgnoringErrors(db);
            return fts5Result.error();
        }
        const bool hasFts5 = fts5Result.value();

        if (!hasFts5) {
            rollbackIgnoringErrors(db);
            return Result<size_t>(0);
        }

        // Prepare cached statements for reuse
        auto selectStmtResult = db.prepareCached("SELECT id FROM documents WHERE sha256_hash = ?");
        if (!selectStmtResult) {
            rollbackIgnoringErrors(db);
            return selectStmtResult.error();
        }
        auto& selectStmt = *selectStmtResult.value();

        auto deleteStmtResult = db.prepareCached("DELETE FROM documents_fts WHERE rowid = ?");
        if (!deleteStmtResult) {
            rollbackIgnoringErrors(db);
            return deleteStmtResult.error();
        }
        auto& deleteStmt = *deleteStmtResult.value();

        size_t removed = 0;
        for (const auto& hash : hashes) {
            // Reset and rebind select statement
            if (auto r = selectStmt.reset(); !r) {
                rollbackIgnoringErrors(db);
                return r.error();
            }
            auto bindResult = selectStmt.bind(1, hash);
            if (!bindResult) {
                rollbackIgnoringErrors(db);
                return bindResult.error();
            }

            auto stepResult = selectStmt.step();
            if (!stepResult) {
                rollbackIgnoringErrors(db);
                return stepResult.error();
            }

            if (!stepResult.value()) {
                // Document not found - skip
                continue;
            }

            int64_t docId = selectStmt.getInt64(0);

            // Reset and rebind delete statement
            if (auto r = deleteStmt.reset(); !r) {
                rollbackIgnoringErrors(db);
                return r.error();
            }
            auto delBindResult = deleteStmt.bind(1, docId);
            if (!delBindResult) {
                rollbackIgnoringErrors(db);
                return delBindResult.error();
            }

            auto execResult = deleteStmt.execute();
            if (!execResult) {
                rollbackIgnoringErrors(db);
                return execResult.error();
            }

            if (db.changes() > 0) {
                removedDocIds.push_back(docId);
                ++removed;
            }
        }

        // Commit the transaction
        if (auto commitResult = commitOrRollback(db); !commitResult) {
            return commitResult.error();
        }

        return Result<size_t>(removed);
    });

    if (result) {
        for (int64_t docId : removedDocIds) {
            eraseFtsIndexedId(docId);
        }
        invalidateQueryCache();
    }
    return result;
}

Result<std::vector<int64_t>> MetadataRepository::getAllFts5IndexedDocumentIds() {
    if (auto cached = getCachedFtsIndexedIds(); cached.has_value()) {
        return cached.value();
    }

    bool allowBackfill = true;
    {
        static std::mutex envMutex;
        std::lock_guard<std::mutex> lock(envMutex);
        const char* env =
            std::getenv("YAMS_FTS5_BACKFILL_INDEX_CACHE"); // NOLINT(concurrency-mt-unsafe)
        if (env && *env) {
            std::string_view value(env);
            allowBackfill = (value != "0" && value != "false" && value != "off" && value != "no");
        }
    }
    if (!allowBackfill) {
        setCachedFtsIndexedIds({});
        return std::vector<int64_t>{};
    }

    return executeReadQuery<std::vector<int64_t>>(
        [&](Database& db) -> Result<std::vector<int64_t>> {
            // First check if FTS5 is available
            auto fts5Result = db.hasFTS5();
            if (!fts5Result)
                return fts5Result.error();

            if (!fts5Result.value()) {
                return std::vector<int64_t>{}; // FTS5 not available, return empty
            }

            // Query all rowids from FTS5 index (rowid corresponds to document.id).
            // DISTINCT is unnecessary for FTS5 rowid and can trigger expensive scan/sort work.
            auto stmtResult = db.prepare("SELECT rowid FROM documents_fts");
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::vector<int64_t> docIds;

            for (;;) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();

                if (!stepResult.value())
                    break;

                int64_t docId = stmt.getInt64(0);
                docIds.push_back(docId);
            }

            setCachedFtsIndexedIds(docIds);
            return docIds;
        });
}

// Bulk operations
Result<std::optional<DocumentInfo>>
MetadataRepository::findDocumentByExactPath(const std::string& path) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::findDocumentByExactPath");
    auto derived = computePathDerivedValues(path);
    if (auto cached = lookupPathCache(derived.normalizedPath))
        return cached;

    return executeReadQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {documentColumnList(false)};
            spec.conditions = {"path_hash = ?"};
            spec.orderBy = std::nullopt;
            spec.groupBy = std::nullopt;
            spec.having = std::nullopt;
            spec.limit = 1;
            spec.offset = std::nullopt;
            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            if (auto bind = stmt.bind(1, derived.pathHash); !bind)
                return bind.error();

            auto stepResult = stmt.step();
            if (!stepResult)
                return stepResult.error();
            if (!stepResult.value())
                return std::optional<DocumentInfo>{};

            auto doc = mapDocumentRow(stmt);
            storePathCache(doc);
            return std::optional<DocumentInfo>{std::move(doc)};
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::queryDocuments(const DocumentQueryOptions& options) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::queryDocuments");
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            const bool joinFtsForContains = options.containsFragment && options.containsUsesFts &&
                                            !options.containsFragment->empty() && pathFtsAvailable_;

            std::string sql = "SELECT ";
            if (joinFtsForContains) {
                sql += documentColumnList(true);
            } else {
                sql += documentColumnList(false);
            }
            sql += " FROM documents";
            if (joinFtsForContains)
                sql += " JOIN documents_path_fts ON documents.id = documents_path_fts.rowid";

            std::vector<std::string> conditions;
            std::vector<BindParam> params;
            appendDocumentQueryFilters(options, joinFtsForContains, hasPathIndexing_, conditions,
                                       params, true);

            if (!conditions.empty()) {
                sql += " WHERE ";
                for (size_t i = 0; i < conditions.size(); ++i) {
                    if (i > 0)
                        sql += " AND ";
                    sql += conditions[i];
                }
            }

            if (options.orderByIdAsc) {
                sql += " ORDER BY documents.id ASC";
            } else if (options.orderByNameAsc) {
                sql += " ORDER BY file_name ASC";
            } else if (options.orderByIndexedDesc) {
                sql += " ORDER BY indexed_time DESC";
            }

            if (options.limit > 0) {
                sql += " LIMIT ?";
                addIntParam(params, options.limit);
            }
            if (options.offset > 0) {
                sql += " OFFSET ?";
                addIntParam(params, options.offset);
            }

            auto execResult = executePreparedVectorQuery<DocumentInfo>(
                db, sql, params, [&](const Statement& stmt) { return mapDocumentRow(stmt); },
                [&](const DocumentInfo& doc) { storePathCache(doc); });
            if (!execResult) {
                if (joinFtsForContains && options.containsUsesFts) {
                    spdlog::debug("MetadataRepository::queryDocuments prepare failed (falling back "
                                  "to without FTS): {}\nSQL: {}",
                                  execResult.error().message, sql);
                    auto fallbackOpts = options;
                    fallbackOpts.containsUsesFts = false;
                    return queryDocuments(fallbackOpts);
                }
                spdlog::error("MetadataRepository::queryDocuments prepare failed: {}\nSQL: {}",
                              execResult.error().message, sql);
                return execResult.error();
            }

            return execResult;
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByHashPrefix(const std::string& hashPrefix, std::size_t limit) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            if (hashPrefix.empty()) {
                return std::vector<DocumentInfo>{};
            }

            std::string lowered = hashPrefix;
            std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

            // Build query via helper
            // Use sha256_hash LIKE ? instead of lower(sha256_hash) LIKE ?
            // The lower() call defeats the idx_documents_hash index causing a full table scan.
            // Hashes are stored lowercase, so a case-insensitive prefix match via LIKE is safe.
            sql::QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {documentColumnList(false)};
            spec.conditions = {"sha256_hash LIKE ?"};
            spec.orderBy = std::optional<std::string>("indexed_time DESC");
            spec.limit = static_cast<int>(limit);
            spec.offset = std::nullopt;
            return queryDocumentsBySpec(
                db, spec, [&](Statement& stmt) { return stmt.bind(1, lowered + "%"); });
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByExtension(const std::string& extension) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            sql::QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {documentColumnList(false)};
            spec.conditions = {"file_extension = ?"};
            spec.orderBy = std::optional<std::string>("file_name");
            spec.limit = std::nullopt;
            spec.offset = std::nullopt;
            return queryDocumentsBySpec(db, spec,
                                        [&](Statement& stmt) { return stmt.bind(1, extension); });
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsModifiedSince(std::chrono::system_clock::time_point since) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            using yams::metadata::sql::QuerySpec;
            const auto sinceUnix = yams::common::timePointToEpochSeconds(since);

            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {documentColumnList(false)};
            spec.conditions = {"modified_time >= ?"};
            spec.orderBy = std::optional<std::string>("modified_time DESC");
            return queryDocumentsBySpec(db, spec, [&](Statement& stmt) {
                return stmt.bind(1, static_cast<int64_t>(sinceUnix));
            });
        });
}

// Statistics
Result<int64_t> MetadataRepository::getDocumentCount() {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<DocumentInfo> ops;
        return ops.count(db);
    });
}

Result<int64_t> MetadataRepository::getIndexedDocumentCount() {
    return executeReadQuery<int64_t>(
        [&](Database& db) -> Result<int64_t> { return queryExactFtsIndexedDocumentCount(db); });
}

Result<int64_t> MetadataRepository::getContentExtractedDocumentCount() {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<DocumentInfo> ops;
        return ops.count(db, "content_extracted = 1");
    });
}

Result<int64_t> MetadataRepository::queryTotalSizeBytesForInitialization() {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare("SELECT COALESCE(SUM(file_size), 0) FROM documents");
        if (!stmtResult) {
            return stmtResult.error();
        }
        auto& stmt = stmtResult.value();
        YAMS_TRY_UNWRAP(hasRow, stmt.step());
        if (!hasRow) {
            return int64_t{0};
        }
        return stmt.getInt64(0);
    });
}

Result<std::unordered_map<std::string, int64_t>>
MetadataRepository::queryExtensionCountsForInitialization() {
    return executeReadQuery<std::unordered_map<std::string, int64_t>>(
        [&](Database& db) -> Result<std::unordered_map<std::string, int64_t>> {
            auto stmtResult = db.prepare(
                "SELECT file_extension, COUNT(*) FROM documents GROUP BY file_extension");
            if (!stmtResult) {
                return stmtResult.error();
            }
            auto stmt = std::move(stmtResult).value();
            std::unordered_map<std::string, int64_t> counts;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    return stepResult.error();
                }
                if (!stepResult.value()) {
                    break;
                }
                counts[stmt.getString(0)] = stmt.getInt64(1);
            }
            return counts;
        });
}

Result<std::pair<int64_t, int64_t>> MetadataRepository::queryPathDepthStatsForInitialization() {
    return executeReadQuery<std::pair<int64_t, int64_t>>(
        [&](Database& db) -> Result<std::pair<int64_t, int64_t>> {
            auto stmtResult = db.prepare(
                "SELECT COALESCE(SUM(path_depth), 0), COALESCE(MAX(path_depth), 0) FROM documents");
            if (!stmtResult) {
                return stmtResult.error();
            }
            auto stmt = std::move(stmtResult).value();
            YAMS_TRY_UNWRAP(hasRow, stmt.step());
            if (!hasRow) {
                return std::pair<int64_t, int64_t>{0, 0};
            }
            return std::pair<int64_t, int64_t>{stmt.getInt64(0), stmt.getInt64(1)};
        });
}

Result<std::pair<int64_t, int64_t>> MetadataRepository::queryTagStatsForInitialization() {
    return executeReadQuery<std::pair<int64_t, int64_t>>(
        [&](Database& db) -> Result<std::pair<int64_t, int64_t>> {
            auto stmtResult = db.prepare(R"(
                SELECT COUNT(DISTINCT document_id), COUNT(*)
                FROM metadata
                WHERE key = 'tag' OR key LIKE 'tag:%'
            )");
            if (!stmtResult) {
                return stmtResult.error();
            }
            auto& stmt = stmtResult.value();
            YAMS_TRY_UNWRAP(hasRow, stmt.step());
            if (!hasRow) {
                return std::pair<int64_t, int64_t>{0, 0};
            }
            return std::pair<int64_t, int64_t>{stmt.getInt64(0), stmt.getInt64(1)};
        });
}

void MetadataRepository::applyInitializedExtensionCounts(
    std::unordered_map<std::string, int64_t> counts) {
    std::lock_guard<std::mutex> lock(extensionStatsMutex_);
    cachedExtensionCounts_ = std::move(counts);
    const auto bucketCounts = calculateExtensionBucketCounts(cachedExtensionCounts_);
    cachedCodeDocCount_.store(bucketCounts.code, std::memory_order_release);
    cachedProseDocCount_.store(bucketCounts.prose, std::memory_order_release);
    cachedBinaryDocCount_.store(bucketCounts.binary, std::memory_order_release);
}

void MetadataRepository::logInitializedCounters() const {
    const auto total = cachedDocumentCount_.load(std::memory_order_relaxed);
    const auto totalBytes = cachedTotalSizeBytes_.load(std::memory_order_relaxed);
    const auto indexed = cachedIndexedCount_.load(std::memory_order_relaxed);
    const auto extracted = cachedExtractedCount_.load(std::memory_order_relaxed);
    const auto embedded = cachedEmbeddedCount_.load(std::memory_order_relaxed);
    const auto docsWithTags = cachedDocsWithTags_.load(std::memory_order_relaxed);
    const auto tagCount = cachedTagCount_.load(std::memory_order_relaxed);
    const auto pathDepthSum = cachedPathDepthSum_.load(std::memory_order_relaxed);
    const auto pathDepthMax = cachedPathDepthMax_.load(std::memory_order_relaxed);
    std::size_t extensionCount = 0;
    {
        std::lock_guard<std::mutex> lock(extensionStatsMutex_);
        extensionCount = cachedExtensionCounts_.size();
    }

    spdlog::info("MetadataRepository: initialized counters - total={}, bytes={}, indexed={}, "
                 "extracted={}, embedded={}, docs_with_tags={}, tag_count={}, exts={}, "
                 "path_sum={}, path_max={}",
                 total, totalBytes, indexed, extracted, embedded, docsWithTags, tagCount,
                 extensionCount, pathDepthSum, pathDepthMax);
}

void MetadataRepository::debugCheckInitializedCounters() const {
    const auto total = cachedDocumentCount_.load(std::memory_order_relaxed);
    const auto indexed = cachedIndexedCount_.load(std::memory_order_relaxed);
    const auto extracted = cachedExtractedCount_.load(std::memory_order_relaxed);
    const auto embedded = cachedEmbeddedCount_.load(std::memory_order_relaxed);
    const auto docsWithTags = cachedDocsWithTags_.load(std::memory_order_relaxed);
    const auto tagCount = cachedTagCount_.load(std::memory_order_relaxed);
    const auto pathDepthSum = cachedPathDepthSum_.load(std::memory_order_relaxed);
    const auto pathDepthMax = cachedPathDepthMax_.load(std::memory_order_relaxed);

    YAMS_DCHECK(indexed <= total,
                "metadata: indexed count must not exceed total after initialization");
    YAMS_DCHECK(extracted <= total,
                "metadata: extracted count must not exceed total after initialization");
    YAMS_DCHECK(embedded <= total,
                "metadata: embedded count must not exceed total after initialization");
    YAMS_DCHECK(docsWithTags <= total,
                "metadata: tagged document count must not exceed total after initialization");
    YAMS_DCHECK(tagCount >= docsWithTags,
                "metadata: tag entry count must cover each tagged document after initialization");
    YAMS_DCHECK(pathDepthMax <= pathDepthSum,
                "metadata: max path depth must not exceed depth sum after initialization");
}

void MetadataRepository::initializeCounters() {
    if (countersInitialized_.exchange(true, std::memory_order_acquire)) {
        return; // Already initialized
    }

    try {
        // Query actual counts from DB once at startup
        if (auto totalResult = getDocumentCount(); totalResult) {
            storeNonNegativeCount(cachedDocumentCount_, totalResult.value());
        }
        if (auto totalSizeResult = queryTotalSizeBytesForInitialization(); totalSizeResult) {
            storeNonNegativeCount(cachedTotalSizeBytes_, totalSizeResult.value());
        }
        if (auto indexedResult = getIndexedDocumentCount(); indexedResult) {
            storeNonNegativeCount(cachedIndexedCount_, indexedResult.value());
        }
        if (auto extractedResult = getContentExtractedDocumentCount(); extractedResult) {
            storeNonNegativeCount(cachedExtractedCount_, extractedResult.value());
        }
        if (auto embeddedResult = getEmbeddedDocumentCount(); embeddedResult) {
            storeNonNegativeCount(cachedEmbeddedCount_, embeddedResult.value());
        }
        if (auto extensionStatsResult = queryExtensionCountsForInitialization();
            extensionStatsResult) {
            applyInitializedExtensionCounts(std::move(extensionStatsResult.value()));
        }
        if (auto pathStatsResult = queryPathDepthStatsForInitialization(); pathStatsResult) {
            storeNonNegativeCount(cachedPathDepthSum_, pathStatsResult.value().first);
            storeNonNegativeCount(cachedPathDepthMax_, pathStatsResult.value().second);
        }
        if (auto tagStatsResult = queryTagStatsForInitialization(); tagStatsResult) {
            storeNonNegativeCount(cachedDocsWithTags_, tagStatsResult.value().first);
            storeNonNegativeCount(cachedTagCount_, tagStatsResult.value().second);
        }

        logInitializedCounters();
        debugCheckInitializedCounters();
    } catch (const std::exception& e) {
        spdlog::warn("MetadataRepository: failed to initialize counters: {}", e.what());
    }
}

void MetadataRepository::warmValueCountsCache() {
    // Common metadata keys to pre-warm (no filters = baseline cache)
    static const std::vector<std::string> kCommonKeys = {"pbi"};

    DocumentQueryOptions defaultOpts{}; // No filters

    auto result = getMetadataValueCounts(kCommonKeys, defaultOpts);
    if (result) {
        spdlog::info("MetadataRepository: warmed value counts cache for {} keys, {} values",
                     kCommonKeys.size(), result.value().size());
    } else {
        spdlog::warn("MetadataRepository: failed to warm value counts cache: {}",
                     result.error().message);
    }
}

Result<std::unordered_map<std::string, DocumentInfo>>
MetadataRepository::batchGetDocumentsByHash(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return std::unordered_map<std::string, DocumentInfo>{};
    }

    return executeReadQuery<std::unordered_map<std::string, DocumentInfo>>(
        [&](Database& db) -> Result<std::unordered_map<std::string, DocumentInfo>> {
            std::string sql = "SELECT id, file_path, file_name, file_extension, file_size, "
                              "sha256_hash, mime_type, created_time, modified_time, indexed_time, "
                              "content_extracted, extraction_status, extraction_error "
                              "FROM documents WHERE sha256_hash IN (";
            for (size_t i = 0; i < hashes.size(); ++i) {
                if (i > 0)
                    sql += ",";
                sql += "?";
            }
            sql += ")";

            auto stmtResult = db.prepare(sql);
            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();

            // Bind hashes
            for (size_t i = 0; i < hashes.size(); ++i) {
                if (auto bindResult = stmt.bind(static_cast<int>(i + 1), hashes[i]); !bindResult) {
                    return bindResult.error();
                }
            }

            std::unordered_map<std::string, DocumentInfo> result;

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

                result[info.sha256Hash] = std::move(info);
            }

            return result;
        });
}

Result<std::unordered_set<std::string>>
MetadataRepository::getExistingDocumentHashes(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return std::unordered_set<std::string>{};
    }

    std::vector<std::string> uniqueHashes = hashes;
    std::sort(uniqueHashes.begin(), uniqueHashes.end());
    uniqueHashes.erase(std::unique(uniqueHashes.begin(), uniqueHashes.end()), uniqueHashes.end());

    return executeQuery<std::unordered_set<std::string>>(
        [&](Database& db) -> Result<std::unordered_set<std::string>> {
            auto beginResult = db.execute("BEGIN");
            if (!beginResult) {
                return beginResult.error();
            }

            auto rollback = [&db](const Error& err) -> Result<std::unordered_set<std::string>> {
                db.execute("ROLLBACK");
                return err;
            };

            if (auto createTemp =
                    db.execute("CREATE TEMP TABLE IF NOT EXISTS temp_existing_document_hashes ("
                               "sha256_hash TEXT PRIMARY KEY)");
                !createTemp) {
                return rollback(createTemp.error());
            }
            if (auto clearTemp = db.execute("DELETE FROM temp_existing_document_hashes");
                !clearTemp) {
                return rollback(clearTemp.error());
            }

            auto insertStmt = db.prepare(
                "INSERT OR IGNORE INTO temp_existing_document_hashes (sha256_hash) VALUES (?)");
            if (!insertStmt) {
                return rollback(insertStmt.error());
            }

            auto& istmt = insertStmt.value();
            for (const auto& hash : uniqueHashes) {
                if (auto reset = istmt.reset(); !reset) {
                    return rollback(reset.error());
                }
                if (auto bind = istmt.bind(1, hash); !bind) {
                    return rollback(bind.error());
                }
                if (auto exec = istmt.execute(); !exec) {
                    return rollback(exec.error());
                }
            }

            auto selectStmt = db.prepare(R"(
                SELECT d.sha256_hash
                FROM documents d
                JOIN temp_existing_document_hashes th ON th.sha256_hash = d.sha256_hash
            )");
            if (!selectStmt) {
                return rollback(selectStmt.error());
            }

            std::unordered_set<std::string> existing;
            auto& sstmt = selectStmt.value();
            while (true) {
                auto step = sstmt.step();
                if (!step) {
                    return rollback(step.error());
                }
                if (!step.value()) {
                    break;
                }
                existing.insert(sstmt.getString(0));
            }

            if (auto clearTemp = db.execute("DELETE FROM temp_existing_document_hashes");
                !clearTemp) {
                return rollback(clearTemp.error());
            }

            auto commitResult = db.execute("COMMIT");
            if (!commitResult) {
                db.execute("ROLLBACK");
                return commitResult.error();
            }

            return existing;
        });
}

Result<std::vector<ListDocumentProjection>>
MetadataRepository::queryDocumentsForListProjection(const DocumentQueryOptions& options) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::queryDocumentsForListProjection");
    YAMS_PLOT("metadata_repo::list_limit", static_cast<int64_t>(options.limit));
    YAMS_PLOT("metadata_repo::list_offset", static_cast<int64_t>(options.offset));
    auto result = executeReadQuery<std::vector<ListDocumentProjection>>(
        [&](Database& db) -> Result<std::vector<ListDocumentProjection>> {
            const bool joinFtsForContains = options.containsFragment && options.containsUsesFts &&
                                            !options.containsFragment->empty() && pathFtsAvailable_;

            std::string sql =
                "SELECT documents.id, documents.file_path, documents.file_name, "
                "documents.file_extension, documents.file_size, documents.sha256_hash, "
                "documents.mime_type, documents.created_time, documents.modified_time, "
                "documents.indexed_time, documents.extraction_status FROM documents";
            if (joinFtsForContains)
                sql += " JOIN documents_path_fts ON documents.id = documents_path_fts.rowid";

            std::vector<std::string> conditions;
            std::vector<BindParam> params;
            appendDocumentQueryFilters(options, joinFtsForContains, hasPathIndexing_, conditions,
                                       params, false);

            if (!conditions.empty()) {
                sql += " WHERE ";
                for (size_t i = 0; i < conditions.size(); ++i) {
                    if (i > 0)
                        sql += " AND ";
                    sql += conditions[i];
                }
            }

            if (options.orderByNameAsc) {
                sql += " ORDER BY file_name ASC";
            } else if (options.orderByIndexedDesc) {
                sql += " ORDER BY indexed_time DESC, id DESC";
            }

            if (options.limit > 0) {
                sql += " LIMIT ?";
                addIntParam(params, options.limit);
            }
            if (options.offset > 0) {
                sql += " OFFSET ?";
                addIntParam(params, options.offset);
            }

            auto execResult = executePreparedVectorQuery<ListDocumentProjection>(
                db, sql, params, [&](const Statement& stmt) { return mapListProjectionRow(stmt); });
            if (!execResult) {
                if (joinFtsForContains && options.containsUsesFts) {
                    auto fallbackOpts = options;
                    fallbackOpts.containsUsesFts = false;
                    return queryDocumentsForListProjection(fallbackOpts);
                }
                return execResult.error();
            }
            return execResult;
        });
    if (result) {
        YAMS_PLOT("metadata_repo::list_projection_results",
                  static_cast<int64_t>(result.value().size()));
    }
    return result;
}

Result<std::vector<GrepCandidateProjection>>
MetadataRepository::queryDocumentsForGrepCandidates(const DocumentQueryOptions& options) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::queryDocumentsForGrepCandidates");
    YAMS_PLOT("metadata_repo::grep_candidates_limit", static_cast<int64_t>(options.limit));
    YAMS_PLOT("metadata_repo::grep_candidates_offset", static_cast<int64_t>(options.offset));
    auto result = executeReadQuery<std::vector<GrepCandidateProjection>>(
        [&](Database& db) -> Result<std::vector<GrepCandidateProjection>> {
            const bool joinFtsForContains = options.containsFragment && options.containsUsesFts &&
                                            !options.containsFragment->empty() && pathFtsAvailable_;

            // Lightweight 6-column projection: only what grep needs for candidate
            // discovery and hot/cold classification.
            std::string sql =
                "SELECT documents.id, documents.file_path, documents.file_size, "
                "documents.sha256_hash, documents.mime_type, documents.content_extracted "
                "FROM documents";
            if (joinFtsForContains)
                sql += " JOIN documents_path_fts ON documents.id = documents_path_fts.rowid";

            std::vector<std::string> conditions;
            std::vector<BindParam> params;
            appendDocumentQueryFilters(options, joinFtsForContains, hasPathIndexing_, conditions,
                                       params, false);

            if (!conditions.empty()) {
                sql += " WHERE ";
                for (size_t i = 0; i < conditions.size(); ++i) {
                    if (i > 0)
                        sql += " AND ";
                    sql += conditions[i];
                }
            }

            // Grep should prefer the freshest indexed snapshot when candidate caps apply,
            // especially for path-scoped queries that may have multiple historical versions.
            sql += " ORDER BY documents.indexed_time DESC, documents.id DESC";

            if (options.limit > 0) {
                sql += " LIMIT ?";
                addIntParam(params, options.limit);
            }
            if (options.offset > 0) {
                sql += " OFFSET ?";
                addIntParam(params, options.offset);
            }

            auto execResult = executePreparedVectorQuery<GrepCandidateProjection>(
                db, sql, params, [&](const Statement& stmt) {
                    GrepCandidateProjection p;
                    p.id = stmt.getInt64(0);
                    p.filePath = stmt.getString(1);
                    p.fileSize = stmt.getInt64(2);
                    p.sha256Hash = stmt.getString(3);
                    p.mimeType = stmt.getString(4);
                    p.contentExtracted = stmt.getInt(5) != 0;
                    return p;
                });
            if (!execResult) {
                if (joinFtsForContains && options.containsUsesFts) {
                    auto fallbackOpts = options;
                    fallbackOpts.containsUsesFts = false;
                    return queryDocumentsForGrepCandidates(fallbackOpts);
                }
                return execResult.error();
            }
            return execResult;
        });
    if (result) {
        YAMS_PLOT("metadata_repo::grep_candidates_results",
                  static_cast<int64_t>(result.value().size()));
    }
    return result;
}

Result<std::unordered_map<std::string, int64_t>>
MetadataRepository::getDocumentCountsByExtension() {
    return executeReadQuery<std::unordered_map<std::string, int64_t>>(
        [&](Database& db) -> Result<std::unordered_map<std::string, int64_t>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {"file_extension", "COUNT(*) as count"};
            spec.orderBy = std::optional<std::string>("count DESC");
            spec.groupBy = std::optional<std::string>("file_extension");
            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::unordered_map<std::string, int64_t> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                result[stmt.getString(0)] = stmt.getInt64(1);
            }

            return result;
        });
}

Result<storage::CorpusStats> MetadataRepository::getCorpusStats() {
    constexpr auto kCorpusStatsOverlayTtl = std::chrono::minutes(5);
    const bool countersPrimedForOverlay = countersInitialized_.load(std::memory_order_acquire);
    auto makeOnlineOverlayInput = [&]() {
        CorpusStatsOverlayInput input;
        input.docCount = static_cast<int64_t>(cachedDocumentCount_.load(std::memory_order_relaxed));
        input.embeddedCount =
            static_cast<int64_t>(cachedEmbeddedCount_.load(std::memory_order_relaxed));
        input.totalSizeBytes =
            static_cast<int64_t>(cachedTotalSizeBytes_.load(std::memory_order_relaxed));
        input.docsWithTags =
            static_cast<int64_t>(cachedDocsWithTags_.load(std::memory_order_relaxed));
        input.tagCount = static_cast<int64_t>(cachedTagCount_.load(std::memory_order_relaxed));
        input.codeDocCount =
            static_cast<int64_t>(cachedCodeDocCount_.load(std::memory_order_relaxed));
        input.proseDocCount =
            static_cast<int64_t>(cachedProseDocCount_.load(std::memory_order_relaxed));
        input.binaryDocCount =
            static_cast<int64_t>(cachedBinaryDocCount_.load(std::memory_order_relaxed));
        input.pathDepthSum =
            static_cast<int64_t>(cachedPathDepthSum_.load(std::memory_order_relaxed));
        input.pathDepthMax =
            static_cast<int64_t>(cachedPathDepthMax_.load(std::memory_order_relaxed));
        input.contentExtractedCount =
            static_cast<int64_t>(cachedExtractedCount_.load(std::memory_order_relaxed));
        input.ftsIndexedCount =
            static_cast<int64_t>(cachedIndexedCount_.load(std::memory_order_relaxed));

        const auto kgCounts =
            kgStore_ ? kgStore_->getEntityCountSnapshot() : KGEntityCountSnapshot{};
        input.symbolCount = kgCounts.totalCount;
        input.nativeSymbolCount = kgCounts.nativeSymbolCount;
        input.nerEntityCount = kgCounts.nerEntityCount;
        input.kgEdgeCount = kgCounts.edgeCount;
        input.kgAliasCount = kgCounts.aliasCount;

        {
            std::lock_guard<std::mutex> lock(extensionStatsMutex_);
            input.extensionCounts = cachedExtensionCounts_;
        }
        return input;
    };

    auto mergeOnlineOverlay = [&](storage::CorpusStats stats) {
        return applyCorpusStatsOnlineOverlay(std::move(stats), makeOnlineOverlayInput());
    };

    // Check cache first (reader lock)
    {
        std::shared_lock<std::shared_mutex> readLock(corpusStatsMutex_);
        if (cachedCorpusStats_) {
            auto now = std::chrono::steady_clock::now();
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - corpusStatsCachedAt_);

            // Check if cache is still valid:
            // 1. Not explicitly marked stale via signalCorpusStatsStale()
            // 2. Within TTL
            // 3. Document count hasn't changed significantly
            bool isStale = corpusStatsStale_.load(std::memory_order_acquire);
            auto currentDocCount = cachedDocumentCount_.load(std::memory_order_relaxed);
            bool docCountUnchanged = (currentDocCount == corpusStatsDocCount_);

            if (!isStale && age < kCorpusStatsTtl && docCountUnchanged) {
                return *cachedCorpusStats_;
            }

            if (countersPrimedForOverlay && age < kCorpusStatsOverlayTtl) {
                auto merged = mergeOnlineOverlay(*cachedCorpusStats_);
                if (isStale || !docCountUnchanged) {
                    return merged;
                }
            }
        }
    }

    // Cold-start shortcut: if live counters are primed (initializeCounters
    // ran during startup) but the cache hasn't been populated yet, synthesize
    // a stats object from the in-memory atomics instead of running the full
    // documents-table reconcile below. On a 32k-doc corpus the reconcile costs
    // ~5s and was previously gating the Search Engine build critical path.
    // The synthesized stats are marked stale so the next call after
    // kCorpusStatsOverlayTtl (5 min) promotes to a reconciled snapshot.
    if (countersPrimedForOverlay && cachedDocumentCount_.load(std::memory_order_relaxed) > 0) {
        storage::CorpusStats baseline;
        auto merged = mergeOnlineOverlay(baseline);
        {
            std::unique_lock<std::shared_mutex> writeLock(corpusStatsMutex_);
            if (!cachedCorpusStats_) {
                cachedCorpusStats_ = std::make_unique<storage::CorpusStats>(merged);
                corpusStatsCachedAt_ = std::chrono::steady_clock::now();
                corpusStatsDocCount_ = cachedDocumentCount_.load(std::memory_order_relaxed);
                corpusStatsStale_.store(true, std::memory_order_release);
            }
        }
        return merged;
    }

    // Cache miss or stale - compute fresh stats
    auto result =
        executeReadQuery<storage::CorpusStats>([&](Database& db) -> Result<storage::CorpusStats> {
            storage::CorpusStats stats;

            // 1. Basic document metrics: count, total size, avg size, path depth
            {
                auto stmtResult = db.prepare(R"(
                    SELECT
                        COUNT(*) as doc_count,
                        COALESCE(SUM(file_size), 0) as total_size,
                        COALESCE(AVG(file_size), 0) as avg_size,
                        COALESCE(AVG(path_depth), 0) as avg_depth,
                        COALESCE(MAX(path_depth), 0) as max_depth,
                        COALESCE(AVG(path_depth) - MIN(path_depth), 0) as relative_depth_avg
                    FROM documents
                )");
                if (!stmtResult)
                    return stmtResult.error();

                auto& stmt = stmtResult.value();
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (stepResult.value()) {
                    stats.docCount = stmt.getInt64(0);
                    stats.totalSizeBytes = stmt.getInt64(1);
                    stats.avgDocLengthBytes = stmt.getDouble(2);
                    stats.pathDepthAvg = stmt.getDouble(3);
                    stats.pathDepthMax = stmt.getDouble(4);
                    stats.pathRelativeDepthAvg = stmt.getDouble(5);
                }
            }

            // Early return if no documents
            if (stats.docCount == 0) {
                stats.computedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count();
                return stats;
            }

            // 2. Extension counts and content type classification
            {
                auto stmtResult = db.prepare(R"(
                    SELECT file_extension, COUNT(*) as count
                    FROM documents
                    GROUP BY file_extension
                    ORDER BY count DESC
                )");
                if (!stmtResult)
                    return stmtResult.error();

                auto& stmt = stmtResult.value();
                int64_t codeCount = 0;
                int64_t proseCount = 0;
                int64_t binaryCount = 0;

                while (true) {
                    auto stepResult = stmt.step();
                    if (!stepResult)
                        return stepResult.error();
                    if (!stepResult.value())
                        break;

                    std::string ext = stmt.getString(0);
                    int64_t count = stmt.getInt64(1);
                    stats.extensionCounts[ext] = count;

                    // Normalize extension to lowercase with leading dot
                    std::string extLower = ext;
                    std::transform(extLower.begin(), extLower.end(), extLower.begin(),
                                   [](unsigned char c) { return std::tolower(c); });
                    if (!extLower.empty() && extLower[0] != '.') {
                        extLower = "." + extLower;
                    }

                    // Classify by extension
                    if (storage::detail::kCodeExtensions.contains(extLower)) {
                        codeCount += count;
                    } else if (storage::detail::kProseExtensions.contains(extLower)) {
                        proseCount += count;
                    } else if (storage::detail::kBinaryExtensions.contains(extLower)) {
                        binaryCount += count;
                    }
                    // Unknown extensions are not counted (could be either)
                }

                // Calculate ratios
                double total = static_cast<double>(stats.docCount);
                stats.codeRatio = static_cast<double>(codeCount) / total;
                stats.proseRatio = static_cast<double>(proseCount) / total;
                stats.binaryRatio = static_cast<double>(binaryCount) / total;
            }

            // 3. Embedding coverage
            {
                auto stmtResult = db.prepare(R"(
                    SELECT COUNT(*) FROM document_embeddings_status WHERE has_embedding = 1
                )");
                if (stmtResult) {
                    auto& stmt = stmtResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        stats.embeddingCount = stmt.getInt64(0);
                        stats.embeddingCoverage = static_cast<double>(stats.embeddingCount) /
                                                  static_cast<double>(stats.docCount);
                    }
                }
                // If table doesn't exist or query fails, counts stay at 0
            }

            // 4. Tag coverage (metadata keys starting with 'tag')
            {
                auto stmtResult = db.prepare(R"(
                    SELECT COUNT(DISTINCT document_id), COUNT(*)
                    FROM metadata
                    WHERE key = 'tag' OR key LIKE 'tag:%'
                )");
                if (stmtResult) {
                    auto& stmt = stmtResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        stats.docsWithTags = stmt.getInt64(0);
                        stats.tagCount = stmt.getInt64(1);
                        stats.tagCoverage = static_cast<double>(stats.docsWithTags) /
                                            static_cast<double>(stats.docCount);
                    }
                }
            }

            // 5. KG symbol count (check if kg_doc_entities table exists first)
            {
                // Check if table exists
                auto checkResult = db.prepare(R"(
                    SELECT COUNT(*) FROM sqlite_master
                    WHERE type='table' AND name='kg_doc_entities'
                )");
                bool tableExists = false;
                if (checkResult) {
                    auto& stmt = checkResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        tableExists = stmt.getInt64(0) > 0;
                    }
                }

                if (tableExists) {
                    // Total count (all extractors) — preserves hasKnowledgeGraph() behaviour.
                    auto stmtResult = db.prepare("SELECT COUNT(*) FROM kg_doc_entities");
                    if (stmtResult) {
                        auto& stmt = stmtResult.value();
                        auto stepResult = stmt.step();
                        if (stepResult && stepResult.value()) {
                            stats.symbolCount = stmt.getInt64(0);
                            stats.symbolDensity = static_cast<double>(stats.symbolCount) /
                                                  static_cast<double>(stats.docCount);
                        }
                    }

                    // Native code symbols (treesitter).
                    auto nativeResult = db.prepare("SELECT COUNT(*) FROM kg_doc_entities"
                                                   " WHERE extractor = 'symbol_extractor_v1'");
                    if (nativeResult) {
                        auto& stmt = nativeResult.value();
                        auto stepResult = stmt.step();
                        if (stepResult && stepResult.value()) {
                            stats.nativeSymbolCount = stmt.getInt64(0);
                            stats.nativeSymbolDensity =
                                static_cast<double>(stats.nativeSymbolCount) /
                                static_cast<double>(stats.docCount);
                        }
                    }

                    // GLiNER NER annotations.
                    auto nerResult = db.prepare("SELECT COUNT(*) FROM kg_doc_entities"
                                                " WHERE extractor LIKE 'gliner%'");
                    if (nerResult) {
                        auto& stmt = nerResult.value();
                        auto stepResult = stmt.step();
                        if (stepResult && stepResult.value()) {
                            stats.nerEntityCount = stmt.getInt64(0);
                            stats.nerEntityDensity = static_cast<double>(stats.nerEntityCount) /
                                                     static_cast<double>(stats.docCount);
                        }
                    }
                }
            }

            // 5a. Content extracted count
            {
                auto stmtResult =
                    db.prepare("SELECT COUNT(*) FROM documents WHERE content_extracted = 1");
                if (stmtResult) {
                    auto& stmt = stmtResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        stats.contentExtractedCount = stmt.getInt64(0);
                        stats.contentExtractedCoverage =
                            static_cast<double>(stats.contentExtractedCount) /
                            static_cast<double>(stats.docCount);
                    }
                }
            }

            // 5b. FTS indexed count (documents with an FTS row)
            {
                auto stmtResult = db.prepare("SELECT COUNT(*) FROM documents_fts_docsize");
                if (!stmtResult) {
                    stmtResult = db.prepare("SELECT COUNT(*) FROM documents_fts");
                }
                if (stmtResult) {
                    auto& stmt = stmtResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        stats.ftsIndexedCount = stmt.getInt64(0);
                        stats.ftsIndexedCoverage = static_cast<double>(stats.ftsIndexedCount) /
                                                   static_cast<double>(stats.docCount);
                    }
                }
            }

            // 5c. Title coverage (metadata key = 'title')
            {
                auto stmtResult = db.prepare(
                    "SELECT COUNT(DISTINCT document_id) FROM metadata WHERE key = 'title'");
                if (stmtResult) {
                    auto& stmt = stmtResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        stats.titleCount = stmt.getInt64(0);
                        stats.titleCoverage = static_cast<double>(stats.titleCount) /
                                              static_cast<double>(stats.docCount);
                    }
                }
            }

            // 5d. Language coverage (document_content.language is non-empty)
            {
                auto stmtResult = db.prepare("SELECT COUNT(*) FROM document_content "
                                             "WHERE language IS NOT NULL AND language != ''");
                if (stmtResult) {
                    auto& stmt = stmtResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        stats.docsWithLanguage = stmt.getInt64(0);
                        stats.languageCoverage = static_cast<double>(stats.docsWithLanguage) /
                                                 static_cast<double>(stats.docCount);
                    }
                }
            }

            // 5e. KG edge density (check if kg_edges table exists first)
            {
                auto checkResult = db.prepare(R"(
                    SELECT COUNT(*) FROM sqlite_master
                    WHERE type='table' AND name='kg_edges'
                )");
                bool edgeTableExists = false;
                if (checkResult) {
                    auto& stmt = checkResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        edgeTableExists = stmt.getInt64(0) > 0;
                    }
                }
                if (edgeTableExists) {
                    auto stmtResult = db.prepare("SELECT COUNT(*) FROM kg_edges");
                    if (stmtResult) {
                        auto& stmt = stmtResult.value();
                        auto stepResult = stmt.step();
                        if (stepResult && stepResult.value()) {
                            stats.kgEdgeCount = stmt.getInt64(0);
                            stats.kgEdgeDensity = static_cast<double>(stats.kgEdgeCount) /
                                                  static_cast<double>(stats.docCount);
                        }
                    }
                }
            }

            // 5f. KG alias density (check if kg_aliases table exists)
            {
                auto checkResult = db.prepare(R"(
                    SELECT COUNT(*) FROM sqlite_master
                    WHERE type='table' AND name='kg_aliases'
                )");
                bool aliasTableExists = false;
                if (checkResult) {
                    auto& stmt = checkResult.value();
                    auto stepResult = stmt.step();
                    if (stepResult && stepResult.value()) {
                        aliasTableExists = stmt.getInt64(0) > 0;
                    }
                }
                if (aliasTableExists) {
                    auto stmtResult = db.prepare("SELECT COUNT(*) FROM kg_aliases");
                    if (stmtResult) {
                        auto& stmt = stmtResult.value();
                        auto stepResult = stmt.step();
                        if (stepResult && stepResult.value()) {
                            stats.kgAliasCount = stmt.getInt64(0);
                            stats.kgAliasDensity = static_cast<double>(stats.kgAliasCount) /
                                                   static_cast<double>(stats.docCount);
                        }
                    }
                }
            }

            // 6. Set timestamp
            stats.computedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
            stats.usedOnlineOverlay = false;
            stats.reconciledComputedAtMs = stats.computedAtMs;
            stats.pathDepthMaxApproximate = false;

            return stats;
        });

    // Cache the result if successful
    if (result.has_value()) {
        std::unique_lock<std::shared_mutex> writeLock(corpusStatsMutex_);
        cachedCorpusStats_ = std::make_unique<storage::CorpusStats>(result.value());
        corpusStatsCachedAt_ = std::chrono::steady_clock::now();
        corpusStatsDocCount_ = cachedDocumentCount_.load(std::memory_order_relaxed);
        // Clear stale flag after successful recomputation
        corpusStatsStale_.store(false, std::memory_order_release);
    }

    return result;
}

void MetadataRepository::signalCorpusStatsStale() {
    // Lightweight signal - just set the flag, don't recompute
    // Actual recomputation is deferred to next getCorpusStats() call
    corpusStatsStale_.store(true, std::memory_order_release);
}

// -----------------------------------------------------------------------------
// SymSpell fuzzy search (SQLite-backed, incremental)
// -----------------------------------------------------------------------------

Result<void> MetadataRepository::ensureSymSpellInitialized() {
    if (symspellInitialized_.load(std::memory_order_acquire)) {
        return {};
    }

    std::lock_guard<std::mutex> lock(symspellInitMutex_);
    if (symspellInitialized_.load(std::memory_order_relaxed)) {
        return {};
    }

    // SymSpellSearch stores a raw sqlite3* pointer and assumes it remains valid
    // for its entire lifetime. Do not satisfy that lifetime by permanently
    // leasing a pooled connection: single-connection pools would be starved for
    // the rest of the process. Instead open a small dedicated handle to the same
    // metadata DB.
    auto db = std::make_unique<Database>();
    auto openResult = db->open(pool_.dbPath(), ConnectionMode::Create);
    if (!openResult) {
        return openResult.error();
    }

    sqlite3* rawDb = db->rawHandle();
    if (!rawDb) {
        return Error{ErrorCode::DatabaseError, "Failed to get raw SQLite handle"};
    }

    // Initialize schema (idempotent - creates tables if not exist)
    auto schemaResult = search::SymSpellSearch::initializeSchema(rawDb);
    if (!schemaResult) {
        spdlog::error("SymSpell schema initialization failed: {}", schemaResult.error().message);
        return schemaResult;
    }

    // Create the search index instance
    symspellDb_ = std::move(db);
    symspellIndex_ = std::make_unique<search::SymSpellSearch>(rawDb);
    symspellInitialized_.store(true, std::memory_order_release);

    spdlog::info("SymSpell fuzzy search index initialized");
    return {};
}

void MetadataRepository::addSymSpellTerm(std::string_view term, int64_t frequency) {
    if (term.empty()) {
        return;
    }

    // Ensure initialized (lazy init on first term add)
    auto initResult = ensureSymSpellInitialized();
    if (!initResult) {
        spdlog::warn("SymSpell not initialized, skipping term '{}': {}", term,
                     initResult.error().message);
        return;
    }

    // Serialize use of the raw sqlite-backed SymSpell instance with initialization/shutdown.
    std::lock_guard<std::mutex> lock(symspellInitMutex_);
    if (symspellIndex_) {
        symspellIndex_->addTerm(term, frequency);
    }
}

// =============================================================================
// Term Statistics for IDF (Dense-First Retrieval)
// =============================================================================

Result<float> MetadataRepository::getTermIDF(const std::string& term) {
    return executeReadQuery<float>([&](Database& db) -> Result<float> {
        // Get total document count from corpus stats
        auto corpusStmt = db.prepare("SELECT total_documents FROM corpus_term_stats WHERE id = 1");
        if (!corpusStmt)
            return corpusStmt.error();

        auto& cs = corpusStmt.value();
        auto corpusStep = cs.step();
        if (!corpusStep)
            return corpusStep.error();

        int64_t totalDocs = 0;
        if (corpusStep.value()) {
            totalDocs = cs.getInt64(0);
        }

        if (totalDocs <= 0) {
            return 0.0f; // Empty corpus
        }

        // Get document frequency for term
        auto termStmt = db.prepare("SELECT document_frequency FROM term_stats WHERE term = ?");
        if (!termStmt)
            return termStmt.error();

        auto& ts = termStmt.value();
        auto bindResult = ts.bind(1, term);
        if (!bindResult)
            return bindResult.error();

        auto termStep = ts.step();
        if (!termStep)
            return termStep.error();

        int64_t docFreq = 0;
        if (termStep.value()) {
            docFreq = ts.getInt64(0);
        }

        if (docFreq <= 0) {
            return 0.0f; // Term not found
        }

        // IDF = log(N / df)
        return static_cast<float>(std::log(static_cast<double>(totalDocs) / docFreq));
    });
}

Result<std::unordered_map<std::string, float>>
MetadataRepository::getTermIDFBatch(const std::vector<std::string>& terms) {
    if (terms.empty()) {
        return std::unordered_map<std::string, float>{};
    }

    return executeReadQuery<std::unordered_map<std::string, float>>(
        [&](Database& db) -> Result<std::unordered_map<std::string, float>> {
            std::unordered_map<std::string, float> result;

            // Get total document count
            auto corpusStmt =
                db.prepare("SELECT total_documents FROM corpus_term_stats WHERE id = 1");
            if (!corpusStmt)
                return corpusStmt.error();

            auto& cs = corpusStmt.value();
            auto corpusStep = cs.step();
            if (!corpusStep)
                return corpusStep.error();

            int64_t totalDocs = 0;
            if (corpusStep.value()) {
                totalDocs = cs.getInt64(0);
            }

            if (totalDocs <= 0) {
                // Empty corpus - return zeros for all terms
                for (const auto& term : terms) {
                    result[term] = 0.0f;
                }
                return result;
            }

            // Build IN clause for batch lookup
            std::string sql = "SELECT term, document_frequency FROM term_stats WHERE term IN (";
            for (size_t i = 0; i < terms.size(); ++i) {
                if (i > 0)
                    sql += ',';
                sql += '?';
            }
            sql += ')';

            auto termStmt = db.prepare(sql);
            if (!termStmt)
                return termStmt.error();

            auto& ts = termStmt.value();
            for (size_t i = 0; i < terms.size(); ++i) {
                auto bindResult = ts.bind(static_cast<int>(i + 1), terms[i]);
                if (!bindResult)
                    return bindResult.error();
            }

            // Initialize all terms with 0 (not found)
            for (const auto& term : terms) {
                result[term] = 0.0f;
            }

            // Process results
            double logTotalDocs = std::log(static_cast<double>(totalDocs));
            while (true) {
                auto stepResult = ts.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                std::string term = ts.getString(0);
                int64_t docFreq = ts.getInt64(1);

                if (docFreq > 0) {
                    // IDF = log(N / df) = log(N) - log(df)
                    result[term] =
                        static_cast<float>(logTotalDocs - std::log(static_cast<double>(docFreq)));
                }
            }

            return result;
        });
}

Result<void>
MetadataRepository::updateTermStats(const std::unordered_map<std::string, int64_t>& terms) {
    if (terms.empty()) {
        return {};
    }

    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Use UPSERT pattern to update term statistics
        auto stmt = db.prepare(R"(
            INSERT INTO term_stats (term, document_frequency, collection_frequency, last_updated)
            VALUES (?, 1, ?, unixepoch())
            ON CONFLICT(term) DO UPDATE SET
                document_frequency = document_frequency + 1,
                collection_frequency = collection_frequency + excluded.collection_frequency,
                last_updated = unixepoch()
        )");
        if (!stmt)
            return stmt.error();

        auto& s = stmt.value();
        for (const auto& [term, count] : terms) {
            auto b1 = s.bind(1, term);
            if (!b1)
                return b1.error();
            auto b2 = s.bind(2, count);
            if (!b2)
                return b2.error();
            auto execResult = s.execute();
            if (!execResult)
                return execResult.error();
            s.reset();
        }

        return {};
    });
}

Result<void> MetadataRepository::updateCorpusTermStats() {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Update corpus-level statistics from documents table
        auto result = db.execute(R"(
            UPDATE corpus_term_stats SET
                total_documents = (SELECT COUNT(*) FROM documents WHERE content_extracted = 1),
                total_terms = (SELECT COUNT(*) FROM term_stats),
                avg_document_length = COALESCE(
                    (SELECT AVG(content_length) FROM document_content WHERE content_length > 0),
                    0.0
                ),
                last_updated = unixepoch()
            WHERE id = 1
        )");
        return result;
    });
}

Result<int64_t> MetadataRepository::getCorpusDocumentCount() {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmt = db.prepare("SELECT total_documents FROM corpus_term_stats WHERE id = 1");
        if (!stmt)
            return stmt.error();

        auto& s = stmt.value();
        auto stepResult = s.step();
        if (!stepResult)
            return stepResult.error();

        if (stepResult.value()) {
            return s.getInt64(0);
        }
        return int64_t{0};
    });
}

Result<void> MetadataRepository::checkpointWal() {
    return executeQuery<void>([](Database& db) -> Result<void> {
        // Use PASSIVE checkpoint — non-blocking, only checkpoints pages not
        // held by active readers. When readers hold pages open, PASSIVE silently
        // checkpoints zero frames; we read the return counts and log a warning
        // so the periodic CheckpointManager can escalate to TRUNCATE.
        int nLog = 0;
        int nCkpt = 0;
        int rc = sqlite3_wal_checkpoint_v2(db.rawHandle(), nullptr, SQLITE_CHECKPOINT_PASSIVE,
                                           &nLog, &nCkpt);
        if (rc != SQLITE_OK) {
            return Error{ErrorCode::DatabaseError, std::string("wal_checkpoint(PASSIVE) failed: ") +
                                                       sqlite3_errmsg(db.rawHandle())};
        }

        if (nCkpt == 0 && nLog > 0) {
            spdlog::warn("[MetadataRepo] PASSIVE checkpoint checked 0 of {} WAL frames "
                         "(readers holding locks — TRUNCATE needed)",
                         nLog);
        } else if (nCkpt > 0) {
            spdlog::debug("[MetadataRepo] PASSIVE checkpoint: {}/{} frames", nCkpt, nLog);
        }

        return db.execute("PRAGMA optimize");
    });
}

// Path 1b: TRUNCATE-mode checkpoint for watermark-triggered or shutdown flushes.
// Unlike the PASSIVE variant, this takes exclusive access — readers/writers
// block until the checkpoint finishes. It returns the WAL file to zero bytes.
// Call sparingly: (a) when WAL size exceeds a watermark (unbounded growth
// stalls query latency anyway, so a brief block is better than the alternative),
// (b) at shutdown when no readers are expected.
Result<void> MetadataRepository::checkpointWalTruncate() {
    return executeQuery<void>([](Database& db) -> Result<void> {
        auto result = db.execute("PRAGMA wal_checkpoint(TRUNCATE)");
        if (!result) {
            return result;
        }
        return db.execute("PRAGMA optimize");
    });
}

void MetadataRepository::refreshAllConnections() {
    pool_.refreshAll();
}

// -----------------------------------------------------------------------------
// Tree diff persistence stubs (PBI-043)
// -----------------------------------------------------------------------------

Result<void> MetadataRepository::upsertTreeSnapshot(const TreeSnapshotRecord& record) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Extract metadata fields from record.metadata map
        static const std::string kEmpty;
        auto field = [&](const char* key) -> const std::string& {
            auto it = record.metadata.find(key);
            return it != record.metadata.end() ? it->second : kEmpty;
        };
        const auto& directoryPath = field("directory_path");
        const auto& snapshotLabel = field("snapshot_label");
        const auto& gitCommit = field("git_commit");
        const auto& gitBranch = field("git_branch");
        const auto& gitRemote = field("git_remote");

        auto stmtResult = db.prepare(R"(
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

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();

        // Bind parameters
        stmt.bind(1, record.snapshotId);
        stmt.bind(2, static_cast<int64_t>(record.createdTime));
        stmt.bind(3, directoryPath);
        if (record.rootTreeHash.empty())
            stmt.bind(4, nullptr);
        else
            stmt.bind(4, record.rootTreeHash);
        if (snapshotLabel.empty())
            stmt.bind(5, nullptr);
        else
            stmt.bind(5, snapshotLabel);
        if (gitCommit.empty())
            stmt.bind(6, nullptr);
        else
            stmt.bind(6, gitCommit);
        if (gitBranch.empty())
            stmt.bind(7, nullptr);
        else
            stmt.bind(7, gitBranch);
        if (gitRemote.empty())
            stmt.bind(8, nullptr);
        else
            stmt.bind(8, gitRemote);
        stmt.bind(9, static_cast<int64_t>(record.fileCount));

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        return Result<void>();
    });
}

Result<std::optional<TreeSnapshotRecord>>
MetadataRepository::getTreeSnapshot(std::string_view snapshotId) {
    (void)snapshotId;
    return Error{ErrorCode::NotImplemented, "Tree diff snapshot lookup not implemented"};
}

Result<std::vector<TreeSnapshotRecord>> MetadataRepository::listTreeSnapshots(int limit) {
    return executeReadQuery<std::vector<TreeSnapshotRecord>>(
        [limit](Database& db) -> Result<std::vector<TreeSnapshotRecord>> {
            const char* sql = R"(
            SELECT snapshot_id, directory_path, snapshot_label, 
                   git_commit, git_branch, git_remote, files_count, created_at
            FROM tree_snapshots
                 ORDER BY created_at DESC, snapshot_id DESC
            LIMIT ?
        )";

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            stmt.bind(1, limit);

            std::vector<TreeSnapshotRecord> snapshots;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break; // No more rows

                TreeSnapshotRecord record;
                record.snapshotId = stmt.getString(0);

                // Store fields in metadata map to match existing structure
                record.metadata["directory_path"] = stmt.getString(1);
                record.metadata["snapshot_label"] = stmt.isNull(2) ? "" : stmt.getString(2);
                record.metadata["git_commit"] = stmt.isNull(3) ? "" : stmt.getString(3);
                record.metadata["git_branch"] = stmt.isNull(4) ? "" : stmt.getString(4);
                record.metadata["git_remote"] = stmt.isNull(5) ? "" : stmt.getString(5);

                record.fileCount = stmt.getInt64(6);
                record.createdTime = stmt.getInt64(7);

                snapshots.push_back(std::move(record));
            }

            return snapshots;
        });
}

namespace {
// Helper to convert TreeChangeType to database TEXT representation
std::string changeTypeToString(TreeChangeType type) {
    switch (type) {
        case TreeChangeType::Added:
            return "added";
        case TreeChangeType::Deleted:
            return "deleted";
        case TreeChangeType::Modified:
            return "modified";
        case TreeChangeType::Renamed:
            return "renamed";
        case TreeChangeType::Moved:
            return "moved";
        default:
            return "unknown";
    }
}

// Helper to convert database TEXT to TreeChangeType
TreeChangeType stringToChangeType(const std::string& str) {
    if (str == "added")
        return TreeChangeType::Added;
    if (str == "deleted")
        return TreeChangeType::Deleted;
    if (str == "modified")
        return TreeChangeType::Modified;
    if (str == "renamed")
        return TreeChangeType::Renamed;
    if (str == "moved")
        return TreeChangeType::Moved;
    return TreeChangeType::Modified; // default fallback
}
} // namespace

Result<int64_t> MetadataRepository::beginTreeDiff(const TreeDiffDescriptor& descriptor) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO tree_diffs (
                base_snapshot_id, target_snapshot_id, computed_at, status
            ) VALUES (?, ?, ?, ?)
        )");

        if (!stmtResult) {
            return stmtResult.error();
        }

        Statement stmt = std::move(stmtResult).value();
        stmt.bind(1, descriptor.baseSnapshotId);
        stmt.bind(2, descriptor.targetSnapshotId);
        stmt.bind(3, descriptor.computedAt);
        stmt.bind(4, descriptor.status);

        auto execResult = stmt.execute();
        if (!execResult) {
            return execResult.error();
        }

        int64_t diffId = db.lastInsertRowId();
        spdlog::debug("Created tree diff: id={}, base={}, target={}", diffId,
                      descriptor.baseSnapshotId, descriptor.targetSnapshotId);

        return diffId;
    });
}

Result<void> MetadataRepository::appendTreeChanges(int64_t diffId,
                                                   const std::vector<TreeChangeRecord>& changes) {
    auto sqlResult = executeQuery<void>([&](Database& db) -> Result<void> {
        // Use a transaction for batch inserts
        auto txnResult = db.execute("BEGIN TRANSACTION");
        if (!txnResult) {
            return txnResult.error();
        }

        auto stmtResult = db.prepare(R"(
            INSERT INTO tree_changes (
                diff_id, change_type, old_path, new_path,
                old_hash, new_hash, old_mode, new_mode, is_directory, file_size,
                content_delta_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )");

        if (!stmtResult) {
            db.execute("ROLLBACK");
            return stmtResult.error();
        }

        Statement stmt = std::move(stmtResult).value();

        for (const auto& change : changes) {
            stmt.reset();
            stmt.bind(1, diffId);
            stmt.bind(2, changeTypeToString(change.type));
            stmt.bind(3, change.oldPath);
            stmt.bind(4, change.newPath);
            stmt.bind(5, change.oldHash);
            stmt.bind(6, change.newHash);

            if (change.mode.has_value()) {
                stmt.bind(7, static_cast<int64_t>(*change.mode));
                stmt.bind(8, static_cast<int64_t>(*change.mode));
            } else {
                stmt.bind(7, nullptr);
                stmt.bind(8, nullptr);
            }

            stmt.bind(9, change.isDirectory ? 1 : 0);
            stmt.bind(10, static_cast<int64_t>(0));

            if (change.contentDeltaHash.has_value()) {
                stmt.bind(11, *change.contentDeltaHash);
            } else {
                stmt.bind(11, nullptr);
            }

            auto execResult = stmt.execute();
            if (!execResult) {
                db.execute("ROLLBACK");
                return Error{execResult.error()};
            }
        }

        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
            db.execute("ROLLBACK");
            return Error{commitResult.error()};
        }

        spdlog::debug("Appended {} tree changes to diff_id={}", changes.size(), diffId);
        return Result<void>();
    });

    if (!sqlResult) {
        return sqlResult;
    }

    if (graphComponent_) {
        auto kgResult = graphComponent_->onTreeDiffApplied(diffId, changes);
        if (!kgResult) {
            spdlog::warn("GraphComponent tree diff processing failed: {}",
                         kgResult.error().message);
        }
    }

    return Result<void>();
}

Result<std::vector<TreeChangeRecord>>
MetadataRepository::listTreeChanges(const TreeDiffQuery& query) {
    return executeReadQuery<std::vector<TreeChangeRecord>>(
        [&](Database& db) -> Result<std::vector<TreeChangeRecord>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.from = std::optional<std::string>{
                "tree_changes tc JOIN tree_diffs td ON tc.diff_id = td.diff_id"};
            spec.table = "tree_changes"; // not used when from is set
            spec.columns = {"change_type",  "old_path",          "new_path",
                            "old_hash",     "new_hash",          "COALESCE(new_mode, old_mode)",
                            "is_directory", "content_delta_hash"};
            spec.conditions = {"td.base_snapshot_id = ?", "td.target_snapshot_id = ?"};
            if (query.pathPrefix.has_value()) {
                spec.conditions.emplace_back("(old_path LIKE ? OR new_path LIKE ?)");
            }
            if (query.typeFilter.has_value()) {
                spec.conditions.emplace_back("change_type = ?");
            }
            spec.orderBy = std::optional<std::string>{"tc.change_id"};
            if (query.limit > 0) {
                spec.limit = static_cast<int>(query.limit);
            }
            if (query.offset > 0) {
                spec.offset = static_cast<int>(query.offset);
            }

            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));
            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();
            int paramIdx = 1;

            stmt.bind(paramIdx++, query.baseSnapshotId);
            stmt.bind(paramIdx++, query.targetSnapshotId);

            if (query.pathPrefix.has_value()) {
                std::string pattern = *query.pathPrefix + "%";
                stmt.bind(paramIdx++, pattern);
                stmt.bind(paramIdx++, pattern);
            }

            if (query.typeFilter.has_value()) {
                stmt.bind(paramIdx, changeTypeToString(*query.typeFilter));
            }

            std::vector<TreeChangeRecord> results;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    return stepResult.error();
                }
                if (!stepResult.value()) {
                    break;
                }

                TreeChangeRecord record;
                record.type = stringToChangeType(stmt.getString(0));
                record.oldPath = stmt.getString(1);
                record.newPath = stmt.getString(2);
                record.oldHash = stmt.getString(3);
                record.newHash = stmt.getString(4);

                if (!stmt.isNull(5)) {
                    record.mode = static_cast<int>(stmt.getInt64(5));
                }

                record.isDirectory = stmt.getInt(6) != 0;

                if (!stmt.isNull(7)) {
                    record.contentDeltaHash = stmt.getString(7);
                }

                results.push_back(std::move(record));
            }

            spdlog::debug("Listed {} tree changes for base={}, target={}", results.size(),
                          query.baseSnapshotId, query.targetSnapshotId);

            return results;
        });
}

Result<void> MetadataRepository::finalizeTreeDiff(int64_t diffId, std::size_t changeCount,
                                                  std::string_view status) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Single-scan aggregation using SUM(CASE ...) instead of 4 correlated subqueries.
        // Each subquery previously scanned tree_changes independently (4x I/O).
        auto stmtResult = db.prepare(R"(
            UPDATE tree_diffs
            SET files_added = (
                    SELECT SUM(CASE WHEN change_type = 'added' THEN 1 ELSE 0 END)
                    FROM tree_changes WHERE diff_id = ? AND is_directory = 0
                ),
                files_deleted = (
                    SELECT SUM(CASE WHEN change_type = 'deleted' THEN 1 ELSE 0 END)
                    FROM tree_changes WHERE diff_id = ? AND is_directory = 0
                ),
                files_modified = (
                    SELECT SUM(CASE WHEN change_type = 'modified' THEN 1 ELSE 0 END)
                    FROM tree_changes WHERE diff_id = ? AND is_directory = 0
                ),
                files_renamed = (
                    SELECT SUM(CASE WHEN change_type IN ('renamed', 'moved') THEN 1 ELSE 0 END)
                    FROM tree_changes WHERE diff_id = ? AND is_directory = 0
                ),
                status = ?
            WHERE diff_id = ?
        )");

        if (!stmtResult) {
            return stmtResult.error();
        }

        Statement stmt = std::move(stmtResult).value();
        stmt.bind(1, diffId);
        stmt.bind(2, diffId);
        stmt.bind(3, diffId);
        stmt.bind(4, diffId);
        stmt.bind(5, std::string(status));
        stmt.bind(6, diffId);

        auto execResult = stmt.execute();
        if (!execResult) {
            return Error{execResult.error()};
        }

        spdlog::debug("Finalized tree diff: id={}, changes={}, status={}", diffId, changeCount,
                      status);

        return Result<void>();
    });
}

// Helper methods for row mapping
DocumentInfo MetadataRepository::mapDocumentRow(const Statement& stmt) const {
    DocumentInfo info;
    info.id = stmt.getInt64(0);
    info.filePath = stmt.getString(1);
    info.fileName = stmt.getString(2);
    info.fileExtension = stmt.getString(3);
    info.fileSize = stmt.getInt64(4);
    info.sha256Hash = stmt.getString(5);
    info.mimeType = stmt.getString(6);
    info.createdTime = stmt.getTime(7);
    info.modifiedTime = stmt.getTime(8);
    info.indexedTime = stmt.getTime(9);
    info.contentExtracted = stmt.getInt(10) != 0;
    info.extractionStatus = ExtractionStatusUtils::fromString(stmt.getString(11));
    info.extractionError = stmt.getString(12);
    info.pathPrefix = stmt.getString(13);
    info.reversePath = stmt.getString(14);
    info.pathHash = stmt.getString(15);
    info.parentHash = stmt.getString(16);
    info.pathDepth = stmt.getInt(17);
    info.repairStatus = RepairStatusUtils::fromString(stmt.getString(18));
    if (!stmt.isNull(19)) {
        info.repairAttemptedAt = std::chrono::sys_seconds{std::chrono::seconds{stmt.getInt64(19)}};
    }
    info.repairAttempts = stmt.getInt(20);
    return info;
}

ListDocumentProjection MetadataRepository::mapListProjectionRow(const Statement& stmt) const {
    ListDocumentProjection info;
    info.id = stmt.getInt64(0);
    info.filePath = stmt.getString(1);
    info.fileName = stmt.getString(2);
    info.fileExtension = stmt.getString(3);
    info.fileSize = stmt.getInt64(4);
    info.sha256Hash = stmt.getString(5);
    info.mimeType = stmt.getString(6);
    info.createdTime = stmt.getTime(7);
    info.modifiedTime = stmt.getTime(8);
    info.indexedTime = stmt.getTime(9);
    info.extractionStatus = ExtractionStatusUtils::fromString(stmt.getString(10));
    return info;
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

MetadataQueryBuilder&
MetadataQueryBuilder::modifiedAfter(std::chrono::system_clock::time_point time) {
    conditions_.push_back("modified_time >= ?");
    const auto unixTime = yams::common::timePointToEpochSeconds(time);
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder&
MetadataQueryBuilder::modifiedBefore(std::chrono::system_clock::time_point time) {
    conditions_.push_back("modified_time <= ?");
    const auto unixTime = yams::common::timePointToEpochSeconds(time);
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder&
MetadataQueryBuilder::indexedAfter(std::chrono::system_clock::time_point time) {
    conditions_.push_back("indexed_time >= ?");
    const auto unixTime = yams::common::timePointToEpochSeconds(time);
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
    std::string query = "SELECT ";
    // MetadataQueryBuilder is used primarily by tools that execute via
    // MetadataRepository::queryDocuments. To keep compatibility with pre-v13
    // schemas, prefer the newer column set and let the repository path adjust
    // when preparing statements. Here, use the superset to keep ordinal mapping
    // consistent; older schemas will be projected via aliases.
    query += kDocumentColumnListNew;
    query += " FROM documents";

    if (!conditions_.empty()) {
        query += " WHERE ";
        for (size_t i = 0; i < conditions_.size(); ++i) {
            if (i > 0)
                query += " AND ";
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

const std::vector<std::string>& MetadataQueryBuilder::getParameters() const {
    return parameters_;
}

// MetadataTransaction implementation
MetadataTransaction::MetadataTransaction(MetadataRepository& repo) : repo_(repo) {}

MetadataTransaction::~MetadataTransaction() = default;

// Snapshot operations (collections use generic metadata query via getMetadataValueCounts)
Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsBySnapshot(const std::string& snapshotId) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            // Use the full 21-column list so mapDocumentRow() can populate all fields
            std::string sql = "SELECT DISTINCT ";
            sql += kDocumentColumnListAliasD;
            sql += " FROM documents d"
                   " JOIN metadata m ON d.id = m.document_id"
                   " WHERE m.key = 'snapshot_id' AND m.value = ?"
                   " ORDER BY d.indexed_time DESC, d.id DESC";
            auto stmtResult = db.prepare(sql);

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, snapshotId);
            if (!bindResult)
                return bindResult.error();

            std::vector<DocumentInfo> documents;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                documents.push_back(mapDocumentRow(stmt));
            }

            return documents;
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsBySnapshotLabel(const std::string& snapshotLabel) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            // Use the full 21-column list so mapDocumentRow() can populate all fields
            std::string sql = "SELECT DISTINCT ";
            sql += kDocumentColumnListAliasD;
            sql += " FROM documents d"
                   " JOIN metadata m ON d.id = m.document_id"
                   " WHERE m.key = 'snapshot_label' AND m.value = ?"
                   " ORDER BY d.indexed_time DESC, d.id DESC";
            auto stmtResult = db.prepare(sql);

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, snapshotLabel);
            if (!bindResult)
                return bindResult.error();

            std::vector<DocumentInfo> documents;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                documents.push_back(mapDocumentRow(stmt));
            }

            return documents;
        });
}

Result<std::vector<std::string>> MetadataRepository::getSnapshots() {
    // Check cache under shared lock first
    {
        std::shared_lock<std::shared_mutex> lock(enumerationCacheMutex_);
        if (cachedEnumerations_) {
            auto now = std::chrono::steady_clock::now();
            auto changeCount = metadataChangeCounter_.load(std::memory_order_acquire);
            bool cacheValid = (now - cachedEnumerations_->cachedAt < kEnumerationCacheTtl) &&
                              (cachedEnumerations_->metadataChangeCount == changeCount);
            if (cacheValid && !cachedEnumerations_->snapshots.empty()) {
                return cachedEnumerations_->snapshots;
            }
        }
    }

    // Cache miss - query database
    auto result = executeReadQuery<std::vector<std::string>>(
        [&](Database& db) -> Result<std::vector<std::string>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"DISTINCT value"};
            spec.conditions = {"key = 'snapshot_id'"};
            spec.orderBy = std::optional<std::string>{"value"};

            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::vector<std::string> snapshots;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                snapshots.push_back(stmt.getString(0));
            }

            return snapshots;
        });

    // Update cache on success
    if (result) {
        std::unique_lock<std::shared_mutex> lock(enumerationCacheMutex_);
        if (!cachedEnumerations_) {
            cachedEnumerations_ = std::make_unique<EnumerationCache>();
        }
        cachedEnumerations_->snapshots = result.value();
        cachedEnumerations_->cachedAt = std::chrono::steady_clock::now();
        cachedEnumerations_->metadataChangeCount =
            metadataChangeCounter_.load(std::memory_order_acquire);
    }

    return result;
}

Result<std::vector<std::string>> MetadataRepository::getSnapshotLabels() {
    // Check cache under shared lock first
    {
        std::shared_lock<std::shared_mutex> lock(enumerationCacheMutex_);
        if (cachedEnumerations_) {
            auto now = std::chrono::steady_clock::now();
            auto changeCount = metadataChangeCounter_.load(std::memory_order_acquire);
            bool cacheValid = (now - cachedEnumerations_->cachedAt < kEnumerationCacheTtl) &&
                              (cachedEnumerations_->metadataChangeCount == changeCount);
            if (cacheValid && !cachedEnumerations_->snapshotLabels.empty()) {
                return cachedEnumerations_->snapshotLabels;
            }
        }
    }

    // Cache miss - query database
    auto result = executeReadQuery<std::vector<std::string>>(
        [&](Database& db) -> Result<std::vector<std::string>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"DISTINCT value"};
            spec.conditions = {"key = 'snapshot_label'"};
            spec.orderBy = std::optional<std::string>{"value"};

            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::vector<std::string> labels;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                labels.push_back(stmt.getString(0));
            }

            return labels;
        });

    // Update cache on success
    if (result) {
        std::unique_lock<std::shared_mutex> lock(enumerationCacheMutex_);
        if (!cachedEnumerations_) {
            cachedEnumerations_ = std::make_unique<EnumerationCache>();
        }
        cachedEnumerations_->snapshotLabels = result.value();
        cachedEnumerations_->cachedAt = std::chrono::steady_clock::now();
        cachedEnumerations_->metadataChangeCount =
            metadataChangeCounter_.load(std::memory_order_acquire);
    }

    return result;
}

Result<SnapshotInfo> MetadataRepository::getSnapshotInfo(const std::string& snapshotId) {
    return executeReadQuery<SnapshotInfo>([&snapshotId](Database& db) -> Result<SnapshotInfo> {
        // Query documents with this snapshot_id and aggregate info
        const char* sql = R"(
            SELECT
                COUNT(*) as file_count,
                MIN(d.indexed_time) as created_time,
                GROUP_CONCAT(DISTINCT SUBSTR(d.file_path, 1, INSTR(d.file_path || '/', '/') - 1)) as root_dirs
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'snapshot_id' AND m.value = ?
        )";

        auto stmtResult = db.prepare(sql);
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, snapshotId);
        if (!bindResult)
            return bindResult.error();

        SnapshotInfo info;
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (stepResult.value()) {
            info.fileCount = stmt.getInt64(0);
            info.createdTime = stmt.isNull(1) ? 0 : stmt.getInt64(1);
            // Derive directory path from common root (simplified - just take first path component)
            if (!stmt.isNull(2)) {
                std::string roots = stmt.getString(2);
                // If there are multiple roots, just use the first one
                auto commaPos = roots.find(',');
                info.directoryPath =
                    commaPos != std::string::npos ? roots.substr(0, commaPos) : roots;
            }
        }

        // Get label from metadata if present
        const char* labelSql = R"(
            SELECT DISTINCT m2.value
            FROM metadata m1
            JOIN metadata m2 ON m1.document_id = m2.document_id
            WHERE m1.key = 'snapshot_id' AND m1.value = ?
              AND m2.key = 'snapshot_label'
            LIMIT 1
        )";

        auto labelStmtResult = db.prepare(labelSql);
        if (labelStmtResult) {
            Statement labelStmt = std::move(labelStmtResult).value();
            auto labelBindResult = labelStmt.bind(1, snapshotId);
            if (labelBindResult) {
                auto labelStepResult = labelStmt.step();
                if (labelStepResult && labelStepResult.value() && !labelStmt.isNull(0)) {
                    info.label = labelStmt.getString(0);
                }
            }
        }

        // Get git commit from metadata if present
        const char* commitSql = R"(
            SELECT DISTINCT m2.value
            FROM metadata m1
            JOIN metadata m2 ON m1.document_id = m2.document_id
            WHERE m1.key = 'snapshot_id' AND m1.value = ?
              AND m2.key = 'git_commit'
            LIMIT 1
        )";

        auto commitStmtResult = db.prepare(commitSql);
        if (commitStmtResult) {
            Statement commitStmt = std::move(commitStmtResult).value();
            auto commitBindResult = commitStmt.bind(1, snapshotId);
            if (commitBindResult) {
                auto commitStepResult = commitStmt.step();
                if (commitStepResult && commitStepResult.value() && !commitStmt.isNull(0)) {
                    info.gitCommit = commitStmt.getString(0);
                }
            }
        }

        return info;
    });
}

Result<std::unordered_map<std::string, SnapshotInfo>>
MetadataRepository::batchGetSnapshotInfo(const std::vector<std::string>& snapshotIds) {
    if (snapshotIds.empty()) {
        return std::unordered_map<std::string, SnapshotInfo>{};
    }

    return executeReadQuery<std::unordered_map<std::string, SnapshotInfo>>(
        [&snapshotIds](Database& db) -> Result<std::unordered_map<std::string, SnapshotInfo>> {
            // Build placeholders for IN clause
            std::string placeholders;
            placeholders.reserve(snapshotIds.size() * 2);
            for (size_t i = 0; i < snapshotIds.size(); ++i) {
                if (i > 0)
                    placeholders += ',';
                placeholders += '?';
            }

            // Single query with GROUP BY to get all snapshot info in one round-trip
            // Uses conditional aggregation to extract label and git_commit in same query
            std::string sql = R"(
                SELECT
                    m_snap.value as snapshot_id,
                    COUNT(DISTINCT d.id) as file_count,
                    MIN(d.indexed_time) as created_time,
                    MAX(CASE WHEN m2.key = 'snapshot_label' THEN m2.value END) as label,
                    MAX(CASE WHEN m2.key = 'git_commit' THEN m2.value END) as git_commit,
                    GROUP_CONCAT(DISTINCT SUBSTR(d.file_path, 1, INSTR(d.file_path || '/', '/') - 1)) as root_dirs
                FROM documents d
                JOIN metadata m_snap ON d.id = m_snap.document_id AND m_snap.key = 'snapshot_id'
                LEFT JOIN metadata m2 ON d.id = m2.document_id AND m2.key IN ('snapshot_label', 'git_commit')
                WHERE m_snap.value IN ()" +
                              placeholders + R"()
                GROUP BY m_snap.value
            )";

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();

            // Bind all snapshot IDs
            int paramIndex = 1;
            for (const auto& snapshotId : snapshotIds) {
                auto bindResult = stmt.bind(paramIndex++, snapshotId);
                if (!bindResult)
                    return bindResult.error();
            }

            std::unordered_map<std::string, SnapshotInfo> results;
            results.reserve(snapshotIds.size());

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                SnapshotInfo info;
                std::string snapshotId = stmt.getString(0);
                info.fileCount = stmt.getInt64(1);
                info.createdTime = stmt.isNull(2) ? 0 : stmt.getInt64(2);
                info.label = stmt.isNull(3) ? "" : stmt.getString(3);
                info.gitCommit = stmt.isNull(4) ? "" : stmt.getString(4);

                // Derive directory path from root dirs
                if (!stmt.isNull(5)) {
                    std::string roots = stmt.getString(5);
                    auto commaPos = roots.find(',');
                    info.directoryPath =
                        commaPos != std::string::npos ? roots.substr(0, commaPos) : roots;
                }

                results[snapshotId] = std::move(info);
            }

            return results;
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsBySessionId(const std::string& sessionId) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            // Use the full 21-column list so mapDocumentRow() can populate all fields
            std::string sql = "SELECT DISTINCT ";
            sql += kDocumentColumnListAliasD;
            sql += " FROM documents d"
                   " JOIN metadata m ON d.id = m.document_id"
                   " WHERE m.key = 'session_id' AND m.value = ?"
                   " ORDER BY d.indexed_time DESC";
            auto stmtResult = db.prepare(sql);

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, sessionId);
            if (!bindResult)
                return bindResult.error();

            std::vector<DocumentInfo> documents;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                documents.push_back(mapDocumentRow(stmt));
            }

            return documents;
        });
}

Result<int64_t> MetadataRepository::countDocumentsBySessionId(const std::string& sessionId) {
    return executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            SELECT COUNT(DISTINCT document_id)
            FROM metadata
            WHERE key = 'session_id' AND value = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, sessionId);
        if (!bindResult)
            return bindResult.error();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();

        return stmt.getInt64(0);
    });
}

Result<void> MetadataRepository::removeSessionIdFromDocuments(const std::string& sessionId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<repository::MetadataEntry> ops;
        ops.deleteWhere(db, "key = 'session_id' AND value = ?", sessionId);
        return {};
    });
}

Result<int64_t> MetadataRepository::deleteDocumentsBySessionId(const std::string& sessionId) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto countResult = db.prepare(R"(
            SELECT COUNT(DISTINCT document_id)
            FROM metadata
            WHERE key = 'session_id' AND value = ?
        )");
        if (!countResult)
            return countResult.error();

        Statement countStmt = std::move(countResult).value();
        auto bindCount = countStmt.bind(1, sessionId);
        if (!bindCount)
            return bindCount.error();

        auto stepCount = countStmt.step();
        if (!stepCount)
            return stepCount.error();

        int64_t count = countStmt.getInt64(0);

        auto deleteResult = db.prepare(R"(
            DELETE FROM documents
            WHERE id IN (
                SELECT DISTINCT document_id
                FROM metadata
                WHERE key = 'session_id' AND value = ?
            )
        )");
        if (!deleteResult)
            return deleteResult.error();

        Statement deleteStmt = std::move(deleteResult).value();
        auto bindDelete = deleteStmt.bind(1, sessionId);
        if (!bindDelete)
            return bindDelete.error();

        auto execResult = deleteStmt.execute();
        if (!execResult)
            return execResult.error();

        return count;
    });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByTags(const std::vector<std::string>& tags, bool matchAll) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::findDocumentsByTags");
    YAMS_PLOT("metadata_repo::find_by_tags_count", static_cast<int64_t>(tags.size()));
    if (tags.empty()) {
        return std::vector<DocumentInfo>();
    }

    auto result = executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            // Support both legacy tag storage (key="tag", value="<name>")
            // and normalized storage (key="tag:<name>").
            std::unordered_set<std::string> tagSet;
            for (const auto& t : tags) {
                if (!t.empty())
                    tagSet.insert(t);
            }
            std::vector<std::string> uniqueTags(tagSet.begin(), tagSet.end());
            std::sort(uniqueTags.begin(), uniqueTags.end());
            if (uniqueTags.empty()) {
                return std::vector<DocumentInfo>();
            }

            std::vector<std::string> tagKeys;
            tagKeys.reserve(uniqueTags.size());
            for (const auto& t : uniqueTags) {
                tagKeys.push_back(std::string("tag:") + t);
            }
            std::string inKeys = buildInList(tagKeys.size());
            std::string inTags = buildInList(uniqueTags.size());

            // Use explicit column list instead of d.* to match mapDocumentRow() expectations
            std::string colList = kDocumentColumnListAliasD;
            std::string sql;
            if (matchAll) {
                // Match-all across both "tag:<name>" keys and legacy key="tag" values.
                // Normalize to tag names via CASE so count reflects actual tags.
                sql = "SELECT " + colList +
                      " FROM documents d WHERE d.id IN ("
                      "SELECT document_id FROM metadata WHERE "
                      "(key IN " +
                      inKeys + " OR (key = 'tag' AND value IN " + inTags +
                      ")) "
                      "GROUP BY document_id "
                      "HAVING COUNT(DISTINCT CASE "
                      "WHEN key = 'tag' THEN value "
                      "WHEN key LIKE 'tag:%' THEN substr(key, 5) "
                      "END) = ?"
                      ") ORDER BY d.indexed_time DESC";
            } else {
                sql = "SELECT DISTINCT " + colList +
                      " FROM documents d "
                      "JOIN metadata m ON d.id = m.document_id "
                      "WHERE (m.key IN " +
                      inKeys + " OR (m.key = 'tag' AND m.value IN " + inTags +
                      ")) "
                      "ORDER BY d.indexed_time DESC";
            }

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            int paramIndex = 1;
            // Bind keys
            for (const auto& k : tagKeys) {
                auto b = stmt.bind(paramIndex++, k);
                if (!b)
                    return b.error();
            }
            // Bind legacy tag values
            for (const auto& t : uniqueTags) {
                auto b = stmt.bind(paramIndex++, t);
                if (!b)
                    return b.error();
            }
            // Bind N for matchAll HAVING
            if (matchAll) {
                auto b = stmt.bind(paramIndex, static_cast<int64_t>(uniqueTags.size()));
                if (!b)
                    return b.error();
            }

            std::vector<DocumentInfo> documents;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                documents.push_back(mapDocumentRow(stmt));
            }

            return documents;
        });
    if (result) {
        YAMS_PLOT("metadata_repo::find_by_tags_results",
                  static_cast<int64_t>(result.value().size()));
    }
    return result;
}

Result<std::vector<std::string>> MetadataRepository::getDocumentTags(int64_t documentId) {
    return executeReadQuery<std::vector<std::string>>(
        [&](Database& db) -> Result<std::vector<std::string>> {
            auto stmtResult = db.prepare(R"(
                SELECT key
                FROM metadata
                WHERE document_id = ?
                AND key LIKE 'tag:%'
                ORDER BY key
            )");

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, documentId);
            if (!bindResult)
                return Error{bindResult.error()};

            std::vector<std::string> tags;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                std::string fullKey = stmt.getString(0);
                // Remove "tag:" prefix
                if (fullKey.starts_with("tag:")) {
                    tags.push_back(fullKey.substr(4));
                }
            }

            return tags;
        });
}

Result<std::unordered_map<int64_t, std::vector<std::string>>>
MetadataRepository::batchGetDocumentTags(std::span<const int64_t> documentIds) {
    if (documentIds.empty()) {
        return std::unordered_map<int64_t, std::vector<std::string>>{};
    }

    return executeReadQuery<std::unordered_map<int64_t, std::vector<std::string>>>(
        [&](Database& db) -> Result<std::unordered_map<int64_t, std::vector<std::string>>> {
            std::string query =
                "SELECT document_id, key FROM metadata WHERE key LIKE 'tag:%' AND document_id IN (";
            for (std::size_t i = 0; i < documentIds.size(); ++i) {
                if (i)
                    query += ",";
                query += "?";
            }
            query += ") ORDER BY document_id, key";

            auto stmtResult = db.prepare(query);
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            int bindIndex = 1;
            for (auto id : documentIds) {
                auto b = stmt.bind(bindIndex++, id);
                if (!b)
                    return b.error();
            }

            std::unordered_map<int64_t, std::vector<std::string>> out;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                std::string fullKey = stmt.getString(1);
                if (fullKey.starts_with("tag:")) {
                    out[stmt.getInt64(0)].push_back(fullKey.substr(4));
                }
            }

            return out;
        });
}

Result<std::vector<std::string>> MetadataRepository::getAllTags() {
    // Check cache under shared lock first
    {
        std::shared_lock<std::shared_mutex> lock(enumerationCacheMutex_);
        if (cachedEnumerations_) {
            auto now = std::chrono::steady_clock::now();
            auto changeCount = metadataChangeCounter_.load(std::memory_order_acquire);
            bool cacheValid = (now - cachedEnumerations_->cachedAt < kEnumerationCacheTtl) &&
                              (cachedEnumerations_->metadataChangeCount == changeCount);
            if (cacheValid && !cachedEnumerations_->tags.empty()) {
                return cachedEnumerations_->tags;
            }
        }
    }

    // Cache miss - query database
    auto result = executeReadQuery<std::vector<std::string>>(
        [&](Database& db) -> Result<std::vector<std::string>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"DISTINCT key"};
            spec.conditions = {"key LIKE 'tag:%'"};
            spec.orderBy = std::optional<std::string>{"key"};

            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::vector<std::string> tags;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                std::string fullKey = stmt.getString(0);
                // Remove "tag:" prefix
                if (fullKey.starts_with("tag:")) {
                    tags.push_back(fullKey.substr(4));
                }
            }

            return tags;
        });

    // Update cache on success
    if (result) {
        std::unique_lock<std::shared_mutex> lock(enumerationCacheMutex_);
        if (!cachedEnumerations_) {
            cachedEnumerations_ = std::make_unique<EnumerationCache>();
        }
        cachedEnumerations_->tags = result.value();
        cachedEnumerations_->cachedAt = std::chrono::steady_clock::now();
        cachedEnumerations_->metadataChangeCount =
            metadataChangeCounter_.load(std::memory_order_acquire);
    }

    return result;
}

} // namespace yams::metadata
