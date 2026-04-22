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
#include <yams/common/utf8_utils.h>
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

// Phase 2: MetadataRepository refactor - ADR-0004
// Using result helpers for reduced error handling boilerplate
#include "repository/result_helpers.hpp"

// Phase 5: CrudOps for generic CRUD operations (ADR-0004)
#include "repository/crud_ops.hpp"
#include "repository/document_query_filters.hpp"
#include "repository/metadata_value_count_ops.hpp"

namespace yams::metadata {

// Import result helpers for cleaner error handling (ADR-0004 Phase 2)
using repository::addIntParam;
using repository::appendDocumentQueryFilters;
using repository::BindParam;
using repository::runMetadataValueCountQuery;
using repository::scope_exit;

namespace {
std::chrono::sys_seconds nowSysSeconds() {
    return std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
}

bool isTagMetadataKey(std::string_view key) {
    return key == "tag" || key.starts_with("tag:");
}

enum class ExtensionBucket { Other, Code, Prose, Binary };

ExtensionBucket classifyExtensionBucket(std::string_view ext) {
    const std::string normalized{ext};
    if (storage::detail::kCodeExtensions.contains(normalized)) {
        return ExtensionBucket::Code;
    }
    if (storage::detail::kProseExtensions.contains(normalized)) {
        return ExtensionBucket::Prose;
    }
    if (storage::detail::kBinaryExtensions.contains(normalized)) {
        return ExtensionBucket::Binary;
    }
    return ExtensionBucket::Other;
}

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

constexpr int64_t kPathTreeNullParent = PathTreeNode::kNullParent;

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

// Transaction begin helper with backend-appropriate semantics.
// - libsql (MVCC): Uses regular BEGIN since concurrent writers are supported.
// - SQLite: Uses BEGIN IMMEDIATE with retry/backoff for lock contention.
Result<void>
beginTransactionWithRetry(Database& db, int maxRetries = 5,
                          std::chrono::milliseconds initialBackoff = std::chrono::milliseconds(5)) {
#if YAMS_LIBSQL_BACKEND
    // libsql supports MVCC - concurrent writers don't block each other.
    // Use regular BEGIN (deferred) for better concurrency.
    (void)maxRetries;     // unused in libsql mode
    (void)initialBackoff; // unused in libsql mode
    return db.execute("BEGIN");
#else
    // Standard SQLite: single-writer model. BEGIN IMMEDIATE acquires write lock
    // immediately but fails fast when another writer holds a lock.
    // Retry with exponential backoff to handle transient lock contention.
    auto backoff = initialBackoff;
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = db.execute("BEGIN IMMEDIATE");
        if (result) {
            return result;
        }
        // Check if it's a lock error (worth retrying)
        const auto& errMsg = result.error().message;
        if (errMsg.find("locked") == std::string::npos &&
            errMsg.find("busy") == std::string::npos) {
            // Not a lock error, don't retry.
            return Error{result.error().code, result.error().message};
        }
        if (attempt + 1 < maxRetries) {
            std::this_thread::sleep_for(backoff);
            backoff *= 2; // Exponential backoff
        }
    }
    return Error{ErrorCode::DatabaseError, "BEGIN IMMEDIATE failed after retries: database locked"};
#endif
}

std::vector<float> blobToFloatVector(const std::vector<std::byte>& blob) {
    if (blob.empty() || (blob.size() % sizeof(float)) != 0)
        return {};
    std::vector<float> out(blob.size() / sizeof(float));
    std::memcpy(out.data(), blob.data(), blob.size());
    return out;
}

PathTreeNode mapPathTreeNodeRow(const Statement& stmt) {
    PathTreeNode node;
    node.id = stmt.getInt64(0);
    node.parentId = stmt.isNull(1) ? kPathTreeNullParent : stmt.getInt64(1);
    node.pathSegment = stmt.getString(2);
    node.fullPath = stmt.getString(3);
    node.docCount = stmt.getInt64(4);
    node.centroidWeight = stmt.getInt64(5);
    if (!stmt.isNull(6)) {
        node.centroid = blobToFloatVector(stmt.getBlob(6));
    }
    return node;
}

Result<void> bindParentId(Statement& stmt, int index, int64_t parentId) {
    if (parentId == kPathTreeNullParent) {
        return stmt.bind(index, nullptr);
    }
    return stmt.bind(index, parentId);
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
    symspellConn_.reset();
    symspellInitialized_.store(false, std::memory_order_release);
    graphComponent_.reset();
    kgStore_.reset();
}

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
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        // Wrap everything in a single BEGIN IMMEDIATE to reduce lock acquisitions
        // from ~15-20 per document down to 1.
        YAMS_TRY(beginTransactionWithRetry(db));
        auto rollback = scope_exit([&] { db.execute("ROLLBACK"); });

        // --- 1. INSERT document ---
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

        int changes = db.changes();
        int64_t docId;

        if (changes > 0) {
            docId = db.lastInsertRowId();
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

        // --- 2. Batch upsert metadata (if any) ---
        if (!tags.empty()) {
            // Use ON CONFLICT upsert to avoid DELETE+INSERT write amplification.
            // Prepare once, bind+execute+reset per tag pair.
            static const std::string metaUpsertSql =
                "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(document_id, key) DO UPDATE SET value = excluded.value, "
                "value_type = excluded.value_type";

            YAMS_TRY_UNWRAP(metaStmt, db.prepareCached(metaUpsertSql));
            for (const auto& [key, value] : tags) {
                YAMS_TRY(metaStmt->bind(1, docId));
                YAMS_TRY(metaStmt->bind(2, key));
                YAMS_TRY(metaStmt->bind(3, value.value));
                YAMS_TRY(metaStmt->bind(4, MetadataValueTypeUtils::toStringView(value.type)));
                YAMS_TRY(metaStmt->execute());
                YAMS_TRY(metaStmt->reset()); // Reset for next iteration
            }
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
            std::string gitCommit =
                snapshot->metadata.count("git_commit") ? snapshot->metadata.at("git_commit") : "";
            std::string gitBranch =
                snapshot->metadata.count("git_branch") ? snapshot->metadata.at("git_branch") : "";
            std::string gitRemote;
            if (auto it = snapshot->metadata.find("git_remote"); it != snapshot->metadata.end()) {
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

        return docId;
    });
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
        auto priorStmt = db.prepareCached("SELECT file_size FROM documents WHERE id = ?");
        if (priorStmt) {
            auto& stmt = *priorStmt.value();
            YAMS_TRY(stmt.bind(1, info.id));
            if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                priorFileSize = stmt.getInt64(0);
            }
        }
        auto priorAttrsStmt =
            db.prepareCached("SELECT file_extension, path_depth FROM documents WHERE id = ?");
        if (priorAttrsStmt) {
            auto& stmt = *priorAttrsStmt.value();
            YAMS_TRY(stmt.bind(1, info.id));
            if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                priorExtension = stmt.getString(0);
                priorPathDepth = stmt.getInt(1);
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
        }
        return Result<void>();
    });
}

Result<void> MetadataRepository::deleteDocument(int64_t id) {
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        // Query document flags before deletion to update counters
        bool wasExtracted = false;
        bool wasIndexed = false;
        bool wasEmbedded = false;
        uint64_t priorFileSize = 0;
        std::string priorExtension;
        int priorPathDepth = 0;
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

        // Update component-owned metrics (using saturating subtraction to prevent underflow)
        if (db.changes() > 0) {
            core::saturating_sub(cachedDocumentCount_, uint64_t{1});
            saturatingSubBytes(cachedTotalSizeBytes_, priorFileSize);
            applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_,
                                     cachedBinaryDocCount_, priorExtension, -1);
            saturatingSubBytes(cachedPathDepthSum_,
                               static_cast<uint64_t>(std::max(priorPathDepth, 0)));
            {
                std::lock_guard<std::mutex> lock(extensionStatsMutex_);
                updateExtensionCountMap(cachedExtensionCounts_, priorExtension, -1);
            }
            if (wasExtracted) {
                core::saturating_sub(cachedExtractedCount_, uint64_t{1});
            }
            if (wasIndexed) {
                core::saturating_sub(cachedIndexedCount_, uint64_t{1});
            }
            if (wasEmbedded) {
                core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
            }
        }

        return Result<void>();
    });

    if (result) {
        // Signal enumeration cache invalidation (document deletion cascades to metadata)
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    }
    return result;
}

Result<size_t> MetadataRepository::deleteDocumentsBatch(const std::vector<int64_t>& ids) {
    if (ids.empty()) {
        return size_t{0};
    }

    auto result = executeQuery<size_t>([&](Database& db) -> Result<size_t> {
        // Begin transaction for batch operation
        auto beginResult = beginTransactionWithRetry(db);
        if (!beginResult) {
            return beginResult.error();
        }

        size_t deletedCount = 0;

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

            if (auto r = checkStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = checkStmt.bind(1, id); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto stepRes = checkStmt.step(); stepRes && stepRes.value()) {
                wasExtracted = checkStmt.getInt(1) != 0;
                priorFileSize = static_cast<uint64_t>(std::max<int64_t>(checkStmt.getInt64(2), 0));
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
                deletedCount++;
                core::saturating_sub(cachedDocumentCount_, uint64_t{1});
                saturatingSubBytes(cachedTotalSizeBytes_, priorFileSize);
                applyExtensionStatsDelta(cachedCodeDocCount_, cachedProseDocCount_,
                                         cachedBinaryDocCount_, priorExtension, -1);
                saturatingSubBytes(cachedPathDepthSum_,
                                   static_cast<uint64_t>(std::max(priorPathDepth, 0)));
                {
                    std::lock_guard<std::mutex> lock(extensionStatsMutex_);
                    updateExtensionCountMap(cachedExtensionCounts_, priorExtension, -1);
                }
                if (wasExtracted) {
                    core::saturating_sub(cachedExtractedCount_, uint64_t{1});
                }
                if (wasIndexed) {
                    core::saturating_sub(cachedIndexedCount_, uint64_t{1});
                }
                if (wasEmbedded) {
                    core::saturating_sub(cachedEmbeddedCount_, uint64_t{1});
                }
            }
        }

        // Commit transaction
        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
            db.execute("ROLLBACK");
            return commitResult.error();
        }

        return deletedCount;
    });

    if (result) {
        // Signal enumeration cache invalidation
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    }
    return result;
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

// Content operations
Result<void> MetadataRepository::insertContent(const DocumentContent& content) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::insertContent");
    YAMS_PLOT("metadata_repo::insert_content_bytes",
              static_cast<int64_t>(content.contentText.size()));
    return executeQuery<void>([&](Database& db) -> Result<void> {
        DocumentContent sanitized = content;
        sanitized.contentText = common::sanitizeUtf8(content.contentText);
        sanitized.contentLength = static_cast<int64_t>(sanitized.contentText.length());

        repository::CrudOps<DocumentContent> ops;
        return ops.upsertOnConflict(db, sanitized, "document_id");
    });
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

Result<void> MetadataRepository::updateContent(const DocumentContent& content) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::updateContent");
    YAMS_PLOT("metadata_repo::update_content_bytes",
              static_cast<int64_t>(content.contentText.size()));
    return executeQuery<void>([&](Database& db) -> Result<void> {
        DocumentContent sanitized = content;
        sanitized.contentText = common::sanitizeUtf8(content.contentText);
        sanitized.contentLength = static_cast<int64_t>(sanitized.contentText.length());

        repository::CrudOps<DocumentContent> ops;
        return ops.update(db, sanitized);
    });
}

Result<void> MetadataRepository::deleteContent(int64_t documentId) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::deleteContent");
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<DocumentContent> ops;
        return ops.deleteById(db, documentId);
    });
}

Result<void>
MetadataRepository::batchInsertContentAndIndex(const std::vector<BatchContentEntry>& entries) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::batchInsertContentAndIndex");
    YAMS_PLOT("metadata_repo::batch_content_entries", static_cast<int64_t>(entries.size()));
    if (entries.empty()) {
        return Result<void>();
    }

    // Track counter deltas for component-owned cached metrics.
    // These counters are intentionally maintained without live DB queries on hot paths.
    uint64_t newlyExtracted = 0;
    uint64_t newlyIndexed = 0;

    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        // Begin transaction for batch operation
        auto beginResult = beginTransactionWithRetry(db);
        if (!beginResult) {
            return beginResult.error();
        }

        // Check if FTS5 is available once
        auto fts5Result = db.hasFTS5();
        if (!fts5Result) {
            db.execute("ROLLBACK");
            return fts5Result.error();
        }
        const bool hasFts5 = fts5Result.value();

        // Prepare cached statements for reuse
        auto contentStmtResult = db.prepareCached(R"(
            INSERT INTO document_content (
                document_id, content_text, content_length,
                extraction_method, language
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(document_id) DO UPDATE SET
                content_text = excluded.content_text,
                content_length = excluded.content_length,
                extraction_method = excluded.extraction_method,
                language = excluded.language
        )");
        if (!contentStmtResult) {
            db.execute("ROLLBACK");
            return contentStmtResult.error();
        }
        auto& contentStmt = *contentStmtResult.value();

        std::optional<CachedStatement> ftsStmtOpt;
        if (hasFts5) {
            auto ftsStmtResult = db.prepareCached(R"(
                INSERT OR REPLACE INTO documents_fts (rowid, content, title)
                VALUES (?, ?, ?)
            )");
            if (!ftsStmtResult) {
                db.execute("ROLLBACK");
                return ftsStmtResult.error();
            }
            ftsStmtOpt = std::move(ftsStmtResult.value());
        }

        // Pre-check statement to determine counter increments before the combined UPDATE.
        auto checkStmtResult = db.prepareCached(R"(
            SELECT COALESCE(content_extracted, 0), extraction_status
            FROM documents WHERE id = ?
        )");
        if (!checkStmtResult) {
            db.execute("ROLLBACK");
            return checkStmtResult.error();
        }
        auto& checkStmt = *checkStmtResult.value();

        // Combined UPDATE: sets all extraction fields in a single statement
        // (replaces 3 separate conditional UPDATEs per document).
        auto combinedStmtResult = db.prepareCached(R"(
            UPDATE documents
            SET content_extracted = 1,
                extraction_status = 'Success',
                extraction_error = NULL
            WHERE id = ?
        )");
        if (!combinedStmtResult) {
            db.execute("ROLLBACK");
            return combinedStmtResult.error();
        }
        auto& combinedStmt = *combinedStmtResult.value();

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

            // 1. Insert content
            if (auto r = contentStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = contentStmt.clearBindings(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            auto bindResult = contentStmt.bindAll(entry.documentId, sanitizedContent,
                                                  static_cast<int64_t>(sanitizedContent.length()),
                                                  entry.extractionMethod, entry.language);
            if (!bindResult) {
                db.execute("ROLLBACK");
                return bindResult.error();
            }
            auto execResult = contentStmt.execute();
            if (!execResult) {
                db.execute("ROLLBACK");
                return execResult.error();
            }

            // 2. Insert FTS index
            if (hasFts5 && ftsStmtOpt) {
                auto& ftsStmt = **ftsStmtOpt;
                if (auto r = ftsStmt.reset(); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto r = ftsStmt.clearBindings(); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                auto ftsBind = ftsStmt.bindAll(entry.documentId, sanitizedContent, sanitizedTitle);
                if (!ftsBind) {
                    db.execute("ROLLBACK");
                    return ftsBind.error();
                }
                auto ftsExec = ftsStmt.execute();
                if (!ftsExec) {
                    db.execute("ROLLBACK");
                    return ftsExec.error();
                }
            }

            // 3. Update extracted/indexed flags with a single combined UPDATE.
            // Pre-check current state to determine counter increments.
            if (auto r = checkStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = checkStmt.clearBindings(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = checkStmt.bind(1, entry.documentId); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            auto checkStep = checkStmt.step();
            if (!checkStep) {
                db.execute("ROLLBACK");
                return checkStep.error();
            }
            bool wasExtracted = checkStep.value() && checkStmt.getInt(0) == 1;
            std::string priorStatus;
            if (checkStep.value()) {
                priorStatus = checkStmt.getString(1);
            }

            if (auto r = combinedStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = combinedStmt.clearBindings(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = combinedStmt.bind(1, entry.documentId); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            auto combExec = combinedStmt.execute();
            if (!combExec) {
                db.execute("ROLLBACK");
                return combExec.error();
            }
            if (!wasExtracted) {
                newlyExtracted++;
            }
            if (priorStatus != "Success") {
                newlyIndexed++;
            }
        }

        // Commit transaction
        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
            db.execute("ROLLBACK");
            return commitResult.error();
        }

        return Result<void>();
    });

    if (result) {
        invalidateQueryCache();
        // Signal corpus stats stale - batch content affects extractionCoverage stats
        signalCorpusStatsStale();

        if (newlyExtracted > 0) {
            cachedExtractedCount_.fetch_add(newlyExtracted, std::memory_order_relaxed);
        }
        if (newlyIndexed > 0) {
            cachedIndexedCount_.fetch_add(newlyIndexed, std::memory_order_relaxed);
        }
        YAMS_PLOT("metadata_repo::batch_content_newly_extracted",
                  static_cast<int64_t>(newlyExtracted));
        YAMS_PLOT("metadata_repo::batch_content_newly_indexed", static_cast<int64_t>(newlyIndexed));
    }
    return result;
}

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
        for (const auto& [documentId, key, _value] : entries) {
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

        for (size_t offset = 0; offset < entries.size(); offset += kMaxRowsPerChunk) {
            const int rows = static_cast<int>(
                std::min(entries.size() - offset, static_cast<size_t>(kMaxRowsPerChunk)));

            if (rows == kMaxRowsPerChunk) {
                YAMS_TRY_UNWRAP(stmt, db.prepareCached(fullChunkSql));
                int bindIndex = 1;
                for (int i = 0; i < rows; ++i) {
                    const auto& [documentId, key, value] = entries[offset + static_cast<size_t>(i)];
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
                    const auto& [documentId, key, value] = entries[offset + static_cast<size_t>(i)];
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

// Relationship operations
Result<int64_t> MetadataRepository::insertRelationship(const DocumentRelationship& relationship) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        YAMS_TRY_UNWRAP(stmt, db.prepare(R"(
            INSERT INTO document_relationships (
                parent_id, child_id, relationship_type, created_time
            ) VALUES (?, ?, ?, ?)
        )"));

        if (relationship.parentId > 0) {
            YAMS_TRY(stmt.bindAll(relationship.parentId, relationship.childId,
                                  relationship.getRelationshipTypeString(),
                                  relationship.createdTime));
        } else {
            YAMS_TRY(stmt.bind(1, nullptr));
            YAMS_TRY(stmt.bind(2, relationship.childId));
            YAMS_TRY(stmt.bind(3, relationship.getRelationshipTypeString()));
            YAMS_TRY(stmt.bind(4, relationship.createdTime));
        }

        YAMS_TRY(stmt.execute());
        return db.lastInsertRowId();
    });
}

Result<std::vector<DocumentRelationship>> MetadataRepository::getRelationships(int64_t documentId) {
    return executeReadQuery<std::vector<DocumentRelationship>>(
        [&](Database& db) -> Result<std::vector<DocumentRelationship>> {
            repository::CrudOps<DocumentRelationship> ops;
            return ops.query(db, "parent_id = ? OR child_id = ?", documentId, documentId);
        });
}

Result<void> MetadataRepository::deleteRelationship(int64_t relationshipId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<DocumentRelationship> ops;
        return ops.deleteById(db, relationshipId);
    });
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
        if (group.canonicalDocumentId.has_value())
            YAMS_TRY(stmt.bind(5, *group.canonicalDocumentId));
        else
            YAMS_TRY(stmt.bind(5, nullptr));
        YAMS_TRY(stmt.bind(6, group.memberCount));
        YAMS_TRY(stmt.bind(7, group.maxPairScore));
        if (group.threshold.has_value())
            YAMS_TRY(stmt.bind(8, *group.threshold));
        else
            YAMS_TRY(stmt.bind(8, nullptr));
        if (group.evidenceJson.empty())
            YAMS_TRY(stmt.bind(9, nullptr));
        else
            YAMS_TRY(stmt.bind(9, group.evidenceJson));
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
        YAMS_TRY(db.execute("BEGIN IMMEDIATE"));

        auto deleteResult =
            db.prepare("DELETE FROM semantic_duplicate_group_members WHERE group_id = ?");
        if (!deleteResult) {
            db.execute("ROLLBACK");
            return deleteResult.error();
        }

        Statement deleteStmt = std::move(deleteResult).value();
        if (auto r = deleteStmt.bind(1, groupId); !r) {
            db.execute("ROLLBACK");
            return r.error();
        }
        if (auto r = deleteStmt.execute(); !r) {
            db.execute("ROLLBACK");
            return r.error();
        }

        auto insertResult = db.prepare(R"(
            INSERT INTO semantic_duplicate_group_members (
                group_id, document_id, role, similarity_to_canonical,
                title_overlap, path_overlap, pair_score, decision, reason,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )");
        if (!insertResult) {
            db.execute("ROLLBACK");
            return insertResult.error();
        }

        Statement insertStmt = std::move(insertResult).value();
        for (const auto& member : members) {
            insertStmt.reset();
            if (auto r = insertStmt.bind(1, groupId); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = insertStmt.bind(2, member.documentId); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = insertStmt.bind(3, member.role); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (member.similarityToCanonical.has_value()) {
                if (auto r = insertStmt.bind(4, *member.similarityToCanonical); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
            } else if (auto r = insertStmt.bind(4, nullptr); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (member.titleOverlap.has_value()) {
                if (auto r = insertStmt.bind(5, *member.titleOverlap); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
            } else if (auto r = insertStmt.bind(5, nullptr); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (member.pathOverlap.has_value()) {
                if (auto r = insertStmt.bind(6, *member.pathOverlap); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
            } else if (auto r = insertStmt.bind(6, nullptr); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (member.pairScore.has_value()) {
                if (auto r = insertStmt.bind(7, *member.pairScore); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
            } else if (auto r = insertStmt.bind(7, nullptr); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = insertStmt.bind(8, member.decision); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (member.reason.empty()) {
                if (auto r = insertStmt.bind(9, nullptr); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
            } else if (auto r = insertStmt.bind(9, member.reason); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = insertStmt.bind(10, member.createdAt); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = insertStmt.bind(11, member.updatedAt); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            if (auto r = insertStmt.execute(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
        }

        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
            db.execute("ROLLBACK");
            return commitResult.error();
        }
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

// Search history operations (refactored with YAMS_TRY - ADR-0004 Phase 2)
Result<int64_t> MetadataRepository::insertSearchHistory(const SearchHistoryEntry& entry) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<SearchHistoryEntry> ops;
        return ops.insert(db, entry);
    });
}

Result<std::vector<SearchHistoryEntry>> MetadataRepository::getRecentSearches(int limit) {
    return executeReadQuery<std::vector<SearchHistoryEntry>>(
        [&](Database& db) -> Result<std::vector<SearchHistoryEntry>> {
            repository::CrudOps<SearchHistoryEntry> ops;
            return ops.getAllOrdered(db, "query_time DESC", limit);
        });
}

Result<int64_t> MetadataRepository::insertFeedbackEvent(const FeedbackEvent& event) {
    // Best-effort: feedback events are telemetry. If the DB is locked by concurrent
    // writes, drop the event rather than blocking the worker thread for 15+ seconds
    // in busy_timeout + retry loops, which causes system-wide starvation.
    return executeBestEffortWrite<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<FeedbackEvent> ops;
        return ops.insert(db, event);
    });
}

Result<std::vector<FeedbackEvent>>
MetadataRepository::getFeedbackEventsByTrace(const std::string& traceId, int limit) {
    return executeReadQuery<std::vector<FeedbackEvent>>(
        [&](Database& db) -> Result<std::vector<FeedbackEvent>> {
            repository::CrudOps<FeedbackEvent> ops;
            return ops.query(db, "trace_id = ? ORDER BY created_at DESC", traceId, limit);
        });
}

Result<std::vector<FeedbackEvent>> MetadataRepository::getRecentFeedbackEvents(int limit) {
    return executeReadQuery<std::vector<FeedbackEvent>>(
        [&](Database& db) -> Result<std::vector<FeedbackEvent>> {
            repository::CrudOps<FeedbackEvent> ops;
            return ops.getAllOrdered(db, "created_at DESC", limit);
        });
}

// Saved queries operations (refactored with YAMS_TRY - ADR-0004 Phase 2)
Result<int64_t> MetadataRepository::insertSavedQuery(const SavedQuery& query) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<SavedQuery> ops;
        return ops.insert(db, query);
    });
}

Result<std::optional<SavedQuery>> MetadataRepository::getSavedQuery(int64_t id) {
    return executeReadQuery<std::optional<SavedQuery>>(
        [&](Database& db) -> Result<std::optional<SavedQuery>> {
            repository::CrudOps<SavedQuery> ops;
            return ops.getById(db, id);
        });
}

Result<std::vector<SavedQuery>> MetadataRepository::getAllSavedQueries() {
    return executeReadQuery<std::vector<SavedQuery>>(
        [&](Database& db) -> Result<std::vector<SavedQuery>> {
            repository::CrudOps<SavedQuery> ops;
            return ops.getAllOrdered(db, "use_count DESC, last_used DESC");
        });
}

Result<void> MetadataRepository::updateSavedQuery(const SavedQuery& query) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<SavedQuery> ops;
        return ops.update(db, query);
    });
}

Result<void> MetadataRepository::deleteSavedQuery(int64_t id) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<SavedQuery> ops;
        return ops.deleteById(db, id);
    });
}

// Full-text search operations
namespace {
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
} // namespace

Result<void> MetadataRepository::indexDocumentContent(int64_t documentId, const std::string& title,
                                                      const std::string& content,
                                                      const std::string& contentType) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::indexDocumentContent");
    bool hadFtsEntry = false;
    if (auto hasFtsBefore = hasFtsEntry(documentId); hasFtsBefore) {
        hadFtsEntry = hasFtsBefore.value();
    }
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        return indexDocumentContentImpl(db, documentId, title, content, contentType,
                                        /*verifyDocumentExists=*/true);
    });

    if (result) {
        if (!hadFtsEntry) {
            cachedIndexedCount_.fetch_add(1, std::memory_order_relaxed);
        }
        noteFtsIndexedId(documentId);
        invalidateQueryCache();
    }
    return result;
}

Result<void> MetadataRepository::indexDocumentContentTrusted(int64_t documentId,
                                                             const std::string& title,
                                                             const std::string& content,
                                                             const std::string& contentType) {
    bool hadFtsEntry = false;
    if (auto hasFtsBefore = hasFtsEntry(documentId); hasFtsBefore) {
        hadFtsEntry = hasFtsBefore.value();
    }
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        return indexDocumentContentImpl(db, documentId, title, content, contentType,
                                        /*verifyDocumentExists=*/false);
    });

    if (result) {
        if (!hadFtsEntry) {
            cachedIndexedCount_.fetch_add(1, std::memory_order_relaxed);
        }
        noteFtsIndexedId(documentId);
        invalidateQueryCache();
    }
    return result;
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
            db.execute("ROLLBACK");
            return fts5Result.error();
        }
        const bool hasFts5 = fts5Result.value();

        if (!hasFts5) {
            return Result<size_t>(0);
        }

        // Prepare cached statements for reuse
        auto selectStmtResult = db.prepareCached("SELECT id FROM documents WHERE sha256_hash = ?");
        if (!selectStmtResult) {
            db.execute("ROLLBACK");
            return selectStmtResult.error();
        }
        auto& selectStmt = *selectStmtResult.value();

        auto deleteStmtResult = db.prepareCached("DELETE FROM documents_fts WHERE rowid = ?");
        if (!deleteStmtResult) {
            db.execute("ROLLBACK");
            return deleteStmtResult.error();
        }
        auto& deleteStmt = *deleteStmtResult.value();

        size_t removed = 0;
        for (const auto& hash : hashes) {
            // Reset and rebind select statement
            if (auto r = selectStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            auto bindResult = selectStmt.bind(1, hash);
            if (!bindResult) {
                db.execute("ROLLBACK");
                return bindResult.error();
            }

            auto stepResult = selectStmt.step();
            if (!stepResult) {
                db.execute("ROLLBACK");
                return stepResult.error();
            }

            if (!stepResult.value()) {
                // Document not found - skip
                continue;
            }

            int64_t docId = selectStmt.getInt64(0);

            // Reset and rebind delete statement
            if (auto r = deleteStmt.reset(); !r) {
                db.execute("ROLLBACK");
                return r.error();
            }
            auto delBindResult = deleteStmt.bind(1, docId);
            if (!delBindResult) {
                db.execute("ROLLBACK");
                return delBindResult.error();
            }

            auto execResult = deleteStmt.execute();
            if (!execResult) {
                db.execute("ROLLBACK");
                return execResult.error();
            }

            removedDocIds.push_back(docId);
            ++removed;
        }

        // Commit the transaction
        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
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
    if (const char* env = std::getenv("YAMS_FTS5_BACKFILL_INDEX_CACHE"); env && *env) {
        std::string_view value(env);
        allowBackfill = (value != "0" && value != "false" && value != "off" && value != "no");
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

            if (options.orderByNameAsc) {
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
            auto sinceUnix =
                std::chrono::duration_cast<std::chrono::seconds>(since.time_since_epoch()).count();

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

void MetadataRepository::initializeCounters() {
    if (countersInitialized_.exchange(true, std::memory_order_acquire)) {
        return; // Already initialized
    }

    try {
        // Query actual counts from DB once at startup
        if (auto totalResult = getDocumentCount(); totalResult) {
            cachedDocumentCount_.store(static_cast<uint64_t>(totalResult.value()),
                                       std::memory_order_release);
        }
        auto totalSizeResult = executeReadQuery<int64_t>([&](Database& db) -> Result<int64_t> {
            auto stmtResult = db.prepare("SELECT COALESCE(SUM(file_size), 0) FROM documents");
            if (!stmtResult) {
                return stmtResult.error();
            }
            auto& stmt = stmtResult.value();
            auto stepResult = stmt.step();
            if (!stepResult) {
                return stepResult.error();
            }
            if (!stepResult.value()) {
                return int64_t{0};
            }
            return stmt.getInt64(0);
        });
        if (totalSizeResult) {
            cachedTotalSizeBytes_.store(
                static_cast<uint64_t>(std::max<int64_t>(totalSizeResult.value(), 0)),
                std::memory_order_release);
        }
        if (auto indexedResult = getIndexedDocumentCount(); indexedResult) {
            cachedIndexedCount_.store(static_cast<uint64_t>(indexedResult.value()),
                                      std::memory_order_release);
        }
        if (auto extractedResult = getContentExtractedDocumentCount(); extractedResult) {
            cachedExtractedCount_.store(static_cast<uint64_t>(extractedResult.value()),
                                        std::memory_order_release);
        }
        if (auto embeddedResult = getEmbeddedDocumentCount(); embeddedResult) {
            cachedEmbeddedCount_.store(static_cast<uint64_t>(embeddedResult.value()),
                                       std::memory_order_release);
        }
        auto extensionStatsResult = executeReadQuery<std::unordered_map<std::string, int64_t>>(
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
        if (extensionStatsResult) {
            std::lock_guard<std::mutex> lock(extensionStatsMutex_);
            cachedExtensionCounts_ = std::move(extensionStatsResult.value());
            uint64_t codeCount = 0;
            uint64_t proseCount = 0;
            uint64_t binaryCount = 0;
            for (const auto& [ext, count] : cachedExtensionCounts_) {
                switch (classifyExtensionBucket(ext)) {
                    case ExtensionBucket::Code:
                        codeCount += static_cast<uint64_t>(std::max<int64_t>(count, 0));
                        break;
                    case ExtensionBucket::Prose:
                        proseCount += static_cast<uint64_t>(std::max<int64_t>(count, 0));
                        break;
                    case ExtensionBucket::Binary:
                        binaryCount += static_cast<uint64_t>(std::max<int64_t>(count, 0));
                        break;
                    case ExtensionBucket::Other:
                        break;
                }
            }
            cachedCodeDocCount_.store(codeCount, std::memory_order_release);
            cachedProseDocCount_.store(proseCount, std::memory_order_release);
            cachedBinaryDocCount_.store(binaryCount, std::memory_order_release);
        }
        auto pathStatsResult = executeReadQuery<
            std::pair<int64_t, int64_t>>([&](Database& db) -> Result<std::pair<int64_t, int64_t>> {
            auto stmtResult = db.prepare(
                "SELECT COALESCE(SUM(path_depth), 0), COALESCE(MAX(path_depth), 0) FROM documents");
            if (!stmtResult) {
                return stmtResult.error();
            }
            auto stmt = std::move(stmtResult).value();
            auto stepResult = stmt.step();
            if (!stepResult) {
                return stepResult.error();
            }
            if (!stepResult.value()) {
                return std::pair<int64_t, int64_t>{0, 0};
            }
            return std::pair<int64_t, int64_t>{stmt.getInt64(0), stmt.getInt64(1)};
        });
        if (pathStatsResult) {
            cachedPathDepthSum_.store(
                static_cast<uint64_t>(std::max<int64_t>(pathStatsResult.value().first, 0)),
                std::memory_order_release);
            cachedPathDepthMax_.store(
                static_cast<uint64_t>(std::max<int64_t>(pathStatsResult.value().second, 0)),
                std::memory_order_release);
        }
        auto tagStatsResult = executeReadQuery<std::pair<int64_t, int64_t>>(
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
        if (tagStatsResult) {
            cachedDocsWithTags_.store(
                static_cast<uint64_t>(std::max<int64_t>(tagStatsResult.value().first, 0)),
                std::memory_order_release);
            cachedTagCount_.store(
                static_cast<uint64_t>(std::max<int64_t>(tagStatsResult.value().second, 0)),
                std::memory_order_release);
        }
        spdlog::info("MetadataRepository: initialized counters - total={}, bytes={}, indexed={}, "
                     "extracted={}, embedded={}, docs_with_tags={}, tag_count={}, exts={}, "
                     "path_sum={}, path_max={}",
                     cachedDocumentCount_.load(), cachedTotalSizeBytes_.load(),
                     cachedIndexedCount_.load(), cachedExtractedCount_.load(),
                     cachedEmbeddedCount_.load(), cachedDocsWithTags_.load(),
                     cachedTagCount_.load(), cachedExtensionCounts_.size(),
                     cachedPathDepthSum_.load(), cachedPathDepthMax_.load());
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
    auto mergeOnlineOverlay = [&](storage::CorpusStats stats) {
        const auto reconciledAtMs = stats.computedAtMs;
        const double reconciledMinDepth = stats.pathDepthAvg - stats.pathRelativeDepthAvg;
        const auto liveDocCount =
            static_cast<int64_t>(cachedDocumentCount_.load(std::memory_order_relaxed));
        const auto liveEmbeddedCount =
            static_cast<int64_t>(cachedEmbeddedCount_.load(std::memory_order_relaxed));
        const auto liveTotalSizeBytes =
            static_cast<int64_t>(cachedTotalSizeBytes_.load(std::memory_order_relaxed));
        const auto liveDocsWithTags =
            static_cast<int64_t>(cachedDocsWithTags_.load(std::memory_order_relaxed));
        const auto liveTagCount =
            static_cast<int64_t>(cachedTagCount_.load(std::memory_order_relaxed));
        const auto liveKgCounts =
            kgStore_ ? kgStore_->getEntityCountSnapshot() : KGEntityCountSnapshot{};
        const auto liveCodeCount =
            static_cast<int64_t>(cachedCodeDocCount_.load(std::memory_order_relaxed));
        const auto liveProseCount =
            static_cast<int64_t>(cachedProseDocCount_.load(std::memory_order_relaxed));
        const auto liveBinaryCount =
            static_cast<int64_t>(cachedBinaryDocCount_.load(std::memory_order_relaxed));
        const auto livePathDepthSum =
            static_cast<int64_t>(cachedPathDepthSum_.load(std::memory_order_relaxed));
        const auto livePathDepthMax =
            static_cast<int64_t>(cachedPathDepthMax_.load(std::memory_order_relaxed));

        if (liveDocCount > 0) {
            stats.docCount = liveDocCount;
            stats.totalSizeBytes = std::max<int64_t>(liveTotalSizeBytes, 0);
            stats.embeddingCount = std::clamp<int64_t>(liveEmbeddedCount, 0, liveDocCount);
            stats.embeddingCoverage =
                static_cast<double>(stats.embeddingCount) / static_cast<double>(stats.docCount);
            stats.docsWithTags = std::clamp<int64_t>(liveDocsWithTags, 0, liveDocCount);
            stats.tagCount = std::max<int64_t>(liveTagCount, 0);
            stats.symbolCount = std::max<int64_t>(liveKgCounts.totalCount, 0);
            stats.nativeSymbolCount = std::max<int64_t>(liveKgCounts.nativeSymbolCount, 0);
            stats.nerEntityCount = std::max<int64_t>(liveKgCounts.nerEntityCount, 0);
            stats.tagCoverage =
                static_cast<double>(stats.docsWithTags) / static_cast<double>(stats.docCount);
            stats.codeRatio = static_cast<double>(std::max<int64_t>(liveCodeCount, 0)) /
                              static_cast<double>(stats.docCount);
            stats.proseRatio = static_cast<double>(std::max<int64_t>(liveProseCount, 0)) /
                               static_cast<double>(stats.docCount);
            stats.binaryRatio = static_cast<double>(std::max<int64_t>(liveBinaryCount, 0)) /
                                static_cast<double>(stats.docCount);
            stats.symbolDensity =
                static_cast<double>(stats.symbolCount) / static_cast<double>(stats.docCount);
            stats.nativeSymbolDensity =
                static_cast<double>(stats.nativeSymbolCount) / static_cast<double>(stats.docCount);
            stats.nerEntityDensity =
                static_cast<double>(stats.nerEntityCount) / static_cast<double>(stats.docCount);
            stats.pathDepthAvg = static_cast<double>(std::max<int64_t>(livePathDepthSum, 0)) /
                                 static_cast<double>(stats.docCount);
            stats.pathDepthMax = static_cast<double>(std::max<int64_t>(livePathDepthMax, 0));
            // Carry the reconciled MIN(path_depth) forward. We can't track MIN as a single
            // atomic (delete of the current-min doc would need a scan), but any insert of a
            // path shallower than the reconciled min only lowers the true min, so clamping
            // the relative average at zero keeps it within a valid range.
            const double relativeDepth = stats.pathDepthAvg - reconciledMinDepth;
            stats.pathRelativeDepthAvg = relativeDepth > 0.0 ? relativeDepth : 0.0;
            if (stats.totalSizeBytes > 0) {
                stats.avgDocLengthBytes =
                    static_cast<double>(stats.totalSizeBytes) / static_cast<double>(stats.docCount);
            }
            {
                std::lock_guard<std::mutex> lock(extensionStatsMutex_);
                stats.extensionCounts = cachedExtensionCounts_;
            }
        } else {
            stats.docCount = 0;
            stats.totalSizeBytes = 0;
            stats.embeddingCount = 0;
            stats.embeddingCoverage = 0.0;
            stats.docsWithTags = 0;
            stats.tagCount = 0;
            stats.symbolCount = 0;
            stats.nativeSymbolCount = 0;
            stats.nerEntityCount = 0;
            stats.codeRatio = 0.0;
            stats.proseRatio = 0.0;
            stats.binaryRatio = 0.0;
            stats.tagCoverage = 0.0;
            stats.symbolDensity = 0.0;
            stats.nativeSymbolDensity = 0.0;
            stats.nerEntityDensity = 0.0;
            stats.avgDocLengthBytes = 0.0;
            stats.pathDepthAvg = 0.0;
            stats.pathDepthMax = 0.0;
            stats.extensionCounts.clear();
        }

        stats.computedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        stats.usedOnlineOverlay = true;
        stats.reconciledComputedAtMs = reconciledAtMs;
        stats.pathDepthMaxApproximate = true;
        return stats;
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

    // Keep a dedicated pooled connection alive for SymSpell. SymSpellSearch stores
    // a raw sqlite3* pointer and assumes it remains valid for its entire lifetime.
    auto connResult = pool_.acquire(std::chrono::milliseconds(30000), ConnectionPriority::Normal);
    if (!connResult) {
        return Error{ErrorCode::ResourceExhausted,
                     "Failed to acquire database connection for SymSpell"};
    }

    symspellConn_ = std::move(connResult).value();
    if (!symspellConn_ || !symspellConn_->isValid()) {
        symspellConn_.reset();
        return Error{ErrorCode::DatabaseError, "Invalid database connection for SymSpell"};
    }

    sqlite3* rawDb = (*symspellConn_)->rawHandle();
    if (!rawDb) {
        symspellConn_.reset();
        return Error{ErrorCode::DatabaseError, "Failed to get raw SQLite handle"};
    }

    // Initialize schema (idempotent - creates tables if not exist)
    auto schemaResult = search::SymSpellSearch::initializeSchema(rawDb);
    if (!schemaResult) {
        spdlog::error("SymSpell schema initialization failed: {}", schemaResult.error().message);
        symspellConn_.reset();
        return schemaResult;
    }

    // Create the search index instance
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
        auto result = executeQuery<void>([&](Database& db) -> Result<void> {
#if YAMS_LIBSQL_BACKEND
            auto beginResult = db.execute("BEGIN");
#else
            auto beginResult = db.execute("BEGIN IMMEDIATE");
#endif
            if (!beginResult)
                return beginResult.error();

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
                db.execute("ROLLBACK");
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
                db.execute("ROLLBACK");
                return updateStmt.error();
            }

            auto& lstmt = lookupStmt.value();
            auto& ustmt = updateStmt.value();
            int64_t embeddedDelta = 0;

            for (const auto& hash : hashes) {
                lstmt.reset();
                if (auto r = lstmt.bind(1, hash); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }

                auto stepResult = lstmt.step();
                if (!stepResult) {
                    db.execute("ROLLBACK");
                    return stepResult.error();
                }
                if (!stepResult.value())
                    continue;

                int64_t documentId = lstmt.getInt64(0);
                bool hadEmbedding = lstmt.getInt(1) != 0;

                ustmt.reset();
                if (auto r = ustmt.bind(1, documentId); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto r = ustmt.bind(2, hasEmbedding ? 1 : 0); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto r = ustmt.bind(3, modelId.empty() ? nullptr : modelId.c_str()); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }

                auto execResult = ustmt.execute();
                if (!execResult) {
                    db.execute("ROLLBACK");
                    return execResult.error();
                }

                if (!hadEmbedding && hasEmbedding) {
                    ++embeddedDelta;
                } else if (hadEmbedding && !hasEmbedding) {
                    --embeddedDelta;
                }
            }

            auto commitResult = db.execute("COMMIT");
            if (!commitResult)
                return commitResult.error();

            if (embeddedDelta > 0) {
                cachedEmbeddedCount_.fetch_add(static_cast<uint64_t>(embeddedDelta),
                                               std::memory_order_relaxed);
            } else if (embeddedDelta < 0) {
                core::saturating_sub(cachedEmbeddedCount_, static_cast<uint64_t>(-embeddedDelta));
            }

            signalCorpusStatsStale();
            return Result<void>();
        });

        if (result)
            return result;

        if (result.error().message.find("database is locked") == std::string::npos)
            return result;

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
    std::size_t reconciledEmbeddedDocs = 0;

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        auto result = executeQuery<void>([&](Database& db) -> Result<void> {
#if YAMS_LIBSQL_BACKEND
            auto beginResult = db.execute("BEGIN");
#else
            auto beginResult = db.execute("BEGIN IMMEDIATE");
#endif
            if (!beginResult)
                return beginResult.error();

            auto rollback = [&db](const Result<void>& err) -> Result<void> {
                db.execute("ROLLBACK");
                return err;
            };

            if (auto createTemp =
                    db.execute("CREATE TEMP TABLE IF NOT EXISTS temp_embedding_reconcile_hashes ("
                               "sha256_hash TEXT PRIMARY KEY)");
                !createTemp) {
                return rollback(createTemp.error());
            }
            if (auto clearTemp = db.execute("DELETE FROM temp_embedding_reconcile_hashes");
                !clearTemp) {
                return rollback(clearTemp.error());
            }

            if (!modelId.empty()) {
                auto ensureModelStmt = db.prepare(R"(
                    INSERT OR IGNORE INTO vector_models (model_id, model_name, embedding_dim)
                    VALUES (?, ?, 0)
                )");
                if (!ensureModelStmt) {
                    return rollback(ensureModelStmt.error());
                }
                auto& mstmt = ensureModelStmt.value();
                if (auto r = mstmt.bind(1, modelId); !r) {
                    return rollback(r.error());
                }
                if (auto r = mstmt.bind(2, modelId); !r) {
                    return rollback(r.error());
                }
                if (auto exec = mstmt.execute(); !exec) {
                    return rollback(exec.error());
                }
            }

            if (!uniqueHashes.empty()) {
                auto insertHashStmt = db.prepare(
                    "INSERT OR IGNORE INTO temp_embedding_reconcile_hashes (sha256_hash) "
                    "VALUES (?)");
                if (!insertHashStmt) {
                    return rollback(insertHashStmt.error());
                }
                auto& hstmt = insertHashStmt.value();
                for (const auto& hash : uniqueHashes) {
                    hstmt.reset();
                    if (auto r = hstmt.bind(1, hash); !r) {
                        return rollback(r.error());
                    }
                    if (auto exec = hstmt.execute(); !exec) {
                        return rollback(exec.error());
                    }
                }
            }

            auto countStmt = db.prepare(R"(
                SELECT COUNT(*)
                FROM documents d
                JOIN temp_embedding_reconcile_hashes th ON th.sha256_hash = d.sha256_hash
            )");
            if (!countStmt) {
                return rollback(countStmt.error());
            }
            if (auto step = countStmt.value().step(); !step) {
                return rollback(step.error());
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
                return rollback(reconcileStmt.error());
            }
            auto& rstmt = reconcileStmt.value();
            if (auto r = rstmt.bind(1, modelId.empty() ? nullptr : modelId.c_str()); !r) {
                return rollback(r.error());
            }
            if (auto exec = rstmt.execute(); !exec) {
                return rollback(exec.error());
            }

            if (auto clearTemp = db.execute("DELETE FROM temp_embedding_reconcile_hashes");
                !clearTemp) {
                return rollback(clearTemp.error());
            }

            auto commitResult = db.execute("COMMIT");
            if (!commitResult) {
                db.execute("ROLLBACK");
                return commitResult.error();
            }
            return Result<void>();
        });

        if (result) {
            cachedEmbeddedCount_.store(static_cast<uint64_t>(reconciledEmbeddedDocs),
                                       std::memory_order_relaxed);
            signalCorpusStatsStale();
            return result;
        }

        if (result.error().message.find("database is locked") == std::string::npos) {
            return result;
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
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Check previous extracted state for counter updates. Indexed/FTS counters are managed
        // only when an FTS row is inserted or removed.
        bool extractedBefore = false;
        {
            auto checkStmt = db.prepareCached(R"(
                SELECT COALESCE(content_extracted, 0)
                FROM documents WHERE id = ?
            )");
            if (checkStmt) {
                auto& stmt = *checkStmt.value();
                if (auto bindRes = stmt.bind(1, documentId); bindRes) {
                    if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                        extractedBefore = stmt.getInt(0) != 0;
                    }
                }
            }
        }

        auto updateStmt = db.prepare(R"(
            UPDATE documents
            SET content_extracted = ?, extraction_status = ?, extraction_error = ?
            WHERE id = ?
        )");
        if (!updateStmt)
            return updateStmt.error();

        auto& stmt = updateStmt.value();
        if (auto r = stmt.bind(1, contentExtracted ? 1 : 0); !r)
            return r.error();
        if (auto r = stmt.bind(2, ExtractionStatusUtils::toString(status)); !r)
            return r.error();
        if (auto r = stmt.bind(3, error.empty() ? nullptr : error.c_str()); !r)
            return r.error();
        if (auto r = stmt.bind(4, documentId); !r)
            return r.error();

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        if (!extractedBefore && contentExtracted) {
            cachedExtractedCount_.fetch_add(1, std::memory_order_relaxed);
        } else if (extractedBefore && !contentExtracted) {
            core::saturating_sub(cachedExtractedCount_, uint64_t{1});
        }

        return Result<void>{};
    });
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

    constexpr int kMaxRetries = 7; // Increased for heavy concurrent load
    constexpr int kBaseDelayMs = 50;

    // Thread-local RNG for jitter to avoid thundering herd
    thread_local std::mt19937 rng(std::random_device{}());

    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        auto result = executeQuery<void>([&](Database& db) -> Result<void> {
#if YAMS_LIBSQL_BACKEND
            auto beginResult = db.execute("BEGIN");
#else
            auto beginResult = db.execute("BEGIN IMMEDIATE");
#endif
            if (!beginResult)
                return beginResult.error();

            auto updateStmt = db.prepare(R"(
                UPDATE documents
                SET repair_status = ?, repair_attempted_at = unixepoch(), repair_attempts = repair_attempts + 1
                WHERE sha256_hash = ?
            )");
            if (!updateStmt) {
                db.execute("ROLLBACK");
                return updateStmt.error();
            }

            auto& stmt = updateStmt.value();
            std::string statusStr = RepairStatusUtils::toString(status);

            for (const auto& hash : hashes) {
                stmt.reset();
                if (auto r = stmt.bind(1, statusStr.c_str()); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }
                if (auto r = stmt.bind(2, hash); !r) {
                    db.execute("ROLLBACK");
                    return r.error();
                }

                auto execResult = stmt.execute();
                if (!execResult) {
                    db.execute("ROLLBACK");
                    return execResult.error();
                }
            }

            auto commitResult = db.execute("COMMIT");
            if (!commitResult)
                return commitResult.error();

            return Result<void>();
        });

        if (result)
            return result;

        if (result.error().message.find("database is locked") == std::string::npos)
            return result;

        // Exponential backoff with jitter (±25%) to prevent thundering herd
        int baseDelayMs = kBaseDelayMs * (1 << attempt);
        int jitter = static_cast<int>(baseDelayMs * 0.25);
        std::uniform_int_distribution<int> dist(-jitter, jitter);
        int delayMs = baseDelayMs + dist(rng);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }

    daemon::TuneAdvisor::reportDbLockError();
    return Error{ErrorCode::DatabaseError,
                 "batchUpdateDocumentRepairStatuses: max retries exceeded"};
}

Result<void> MetadataRepository::checkpointWal() {
    return executeQuery<void>([](Database& db) -> Result<void> {
        // Use PASSIVE checkpoint instead of TRUNCATE. TRUNCATE requires exclusive access
        // and blocks all readers/writers for the entire duration, causing pipeline stalls.
        // PASSIVE checkpoints only pages that are not currently in use by any reader,
        // allowing concurrent queries to continue uninterrupted.
        auto result = db.execute("PRAGMA wal_checkpoint(PASSIVE)");
        if (!result) {
            return result;
        }

        return db.execute("PRAGMA optimize");
    });
}

void MetadataRepository::refreshAllConnections() {
    pool_.refreshAll();
}

Result<std::optional<PathTreeNode>>
MetadataRepository::findPathTreeNode(int64_t parentId, std::string_view pathSegment) {
    return executeReadQuery<std::optional<PathTreeNode>>(
        [&](Database& db) -> Result<std::optional<PathTreeNode>> {
            const bool parentIsNull = parentId == kPathTreeNullParent;
            const char* sql =
                parentIsNull ? "SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                               "centroid_weight, centroid FROM path_tree_nodes "
                               "WHERE parent_id IS NULL AND path_segment = ?"
                             : "SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                               "centroid_weight, centroid FROM path_tree_nodes "
                               "WHERE parent_id = ? AND path_segment = ?";

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            int bindIndex = 1;

            if (!parentIsNull) {
                if (auto bindResult = stmt.bind(bindIndex++, parentId); !bindResult)
                    return bindResult.error();
            }
            if (auto bindResult = stmt.bind(bindIndex, pathSegment); !bindResult)
                return bindResult.error();

            auto stepResult = stmt.step();
            if (!stepResult)
                return stepResult.error();
            if (!stepResult.value())
                return std::optional<PathTreeNode>{};

            return std::optional<PathTreeNode>{mapPathTreeNodeRow(stmt)};
        });
}

Result<PathTreeNode> MetadataRepository::insertPathTreeNode(int64_t parentId,
                                                            std::string_view pathSegment,
                                                            std::string_view fullPath) {
    return executeQuery<PathTreeNode>([&](Database& db) -> Result<PathTreeNode> {
        auto insertStmtResult = db.prepare("INSERT OR IGNORE INTO path_tree_nodes "
                                           "(parent_id, path_segment, full_path) VALUES (?, ?, ?)");
        if (!insertStmtResult)
            return insertStmtResult.error();

        auto insertStmt = std::move(insertStmtResult).value();
        if (auto bindParent = bindParentId(insertStmt, 1, parentId); !bindParent)
            return bindParent.error();
        if (auto bindSeg = insertStmt.bind(2, pathSegment); !bindSeg)
            return bindSeg.error();
        if (auto bindFull = insertStmt.bind(3, fullPath); !bindFull)
            return bindFull.error();

        if (auto execResult = insertStmt.execute(); !execResult)
            return execResult.error();

        auto selectStmtResult = db.prepare(
            "SELECT node_id, parent_id, path_segment, full_path, doc_count, centroid_weight "
            "FROM path_tree_nodes WHERE full_path = ?");
        if (!selectStmtResult)
            return selectStmtResult.error();

        auto selectStmt = std::move(selectStmtResult).value();
        if (auto bindPath = selectStmt.bind(1, fullPath); !bindPath)
            return bindPath.error();

        auto stepResult = selectStmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            return Error{ErrorCode::Unknown, "Failed to fetch inserted path tree node"};

        return mapPathTreeNodeRow(selectStmt);
    });
}

Result<void> MetadataRepository::incrementPathTreeDocCount(int64_t nodeId, int64_t documentId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Use INSERT OR IGNORE to handle concurrent inserts atomically.
        // This avoids the race condition in check-then-insert pattern.
        auto insertStmtResult = db.prepare(
            "INSERT OR IGNORE INTO path_tree_node_documents (node_id, document_id) VALUES (?, ?)");
        if (!insertStmtResult)
            return insertStmtResult.error();
        auto insertStmt = std::move(insertStmtResult).value();
        if (auto bindNode = insertStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();
        if (auto bindDoc = insertStmt.bind(2, documentId); !bindDoc)
            return bindDoc.error();
        if (auto execResult = insertStmt.execute(); !execResult)
            return execResult.error();

        // Only increment doc_count if a new row was actually inserted
        // (changes() returns 0 if INSERT was ignored due to conflict)
        if (db.changes() == 0) {
            return Result<void>(); // Already associated, nothing to update
        }

        auto updateStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET doc_count = doc_count + 1, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!updateStmtResult)
            return updateStmtResult.error();
        auto updateStmt = std::move(updateStmtResult).value();
        if (auto bindNode = updateStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();
        return updateStmt.execute();
    });
}

Result<void>
MetadataRepository::accumulatePathTreeCentroid(int64_t nodeId,
                                               std::span<const float> embeddingValues) {
    if (embeddingValues.empty())
        return Result<void>();

    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto selectStmtResult =
            db.prepare("SELECT centroid, centroid_weight FROM path_tree_nodes WHERE node_id = ?");
        if (!selectStmtResult)
            return selectStmtResult.error();

        auto selectStmt = std::move(selectStmtResult).value();
        if (auto bindNode = selectStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();

        auto stepResult = selectStmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            return Error{ErrorCode::NotFound, "Path tree node not found"};

        std::vector<float> centroid;
        centroid.reserve(embeddingValues.size());
        int64_t currentWeight = selectStmt.getInt64(1);

        auto existingBlob = selectStmt.isNull(0) ? std::vector<std::byte>() : selectStmt.getBlob(0);
        if (!existingBlob.empty() &&
            existingBlob.size() == embeddingValues.size() * sizeof(float) && currentWeight > 0) {
            centroid.resize(embeddingValues.size());
            std::memcpy(centroid.data(), existingBlob.data(), existingBlob.size());
        } else {
            centroid.assign(embeddingValues.begin(), embeddingValues.end());
            currentWeight = 0;
        }

        int64_t newWeight = currentWeight + 1;
        if (currentWeight > 0) {
            const double weightFactor = static_cast<double>(currentWeight);
            for (std::size_t i = 0; i < embeddingValues.size(); ++i) {
                double updated = (centroid[i] * weightFactor + embeddingValues[i]) /
                                 static_cast<double>(newWeight);
                centroid[i] = static_cast<float>(updated);
            }
        } else {
            centroid.assign(embeddingValues.begin(), embeddingValues.end());
        }

        auto updateStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET centroid = ?, centroid_weight = ?, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!updateStmtResult)
            return updateStmtResult.error();

        auto updateStmt = std::move(updateStmtResult).value();

        std::span<const std::byte> blob(reinterpret_cast<const std::byte*>(centroid.data()),
                                        centroid.size() * sizeof(float));
        if (auto bindBlob = updateStmt.bind(1, blob); !bindBlob)
            return bindBlob.error();
        if (auto bindWeight = updateStmt.bind(2, newWeight); !bindWeight)
            return bindWeight.error();
        if (auto bindNode = updateStmt.bind(3, nodeId); !bindNode)
            return bindNode.error();

        return updateStmt.execute();
    });
}

Result<std::optional<PathTreeNode>>
MetadataRepository::findPathTreeNodeByFullPath(std::string_view fullPath) {
    if (fullPath.empty())
        return std::optional<PathTreeNode>{};

    return executeReadQuery<std::optional<PathTreeNode>>(
        [&](Database& db) -> Result<std::optional<PathTreeNode>> {
            auto stmtResult = db.prepare("SELECT node_id, parent_id, path_segment, full_path, "
                                         "doc_count, centroid_weight, centroid "
                                         "FROM path_tree_nodes WHERE full_path = ?");
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            if (auto bindRes = stmt.bind(1, fullPath); !bindRes)
                return bindRes.error();

            auto stepRes = stmt.step();
            if (!stepRes)
                return stepRes.error();
            if (!stepRes.value())
                return std::optional<PathTreeNode>{};

            PathTreeNode node;
            node.id = stmt.getInt64(0);
            node.parentId = stmt.isNull(1) ? kPathTreeNullParent : stmt.getInt64(1);
            node.pathSegment = stmt.getString(2);
            node.fullPath = stmt.getString(3);
            node.docCount = stmt.getInt64(4);
            node.centroidWeight = stmt.getInt64(5);
            if (!stmt.isNull(6)) {
                node.centroid = blobToFloatVector(stmt.getBlob(6));
            }
            return std::optional<PathTreeNode>{std::move(node)};
        });
}

// ---------------------------------------------------------------------------
// Repair helpers (concrete-class only)
// ---------------------------------------------------------------------------

Result<uint64_t> MetadataRepository::countDocsMissingPathTree() {
    return executeReadQuery<uint64_t>([&](Database& db) -> Result<uint64_t> {
        auto stmtResult = db.prepare("SELECT COUNT(*) FROM documents d "
                                     "LEFT JOIN path_tree_nodes p ON p.full_path = d.file_path "
                                     "WHERE d.file_path != '' AND p.node_id IS NULL");
        if (!stmtResult)
            return stmtResult.error();

        auto stmt = std::move(stmtResult).value();
        auto stepRes = stmt.step();
        if (!stepRes)
            return stepRes.error();
        if (!stepRes.value())
            return uint64_t(0);

        return static_cast<uint64_t>(stmt.getInt64(0));
    });
}

Result<std::vector<DocumentInfo>> MetadataRepository::findDocsMissingPathTree(int limit) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            std::string sql = "SELECT ";
            sql += documentColumnList(false);
            sql += " FROM documents d "
                   "LEFT JOIN path_tree_nodes p ON p.full_path = d.file_path "
                   "WHERE d.file_path != '' AND p.node_id IS NULL";
            if (limit > 0) {
                sql += " LIMIT ?";
            }

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            if (limit > 0) {
                if (auto r = stmt.bind(1, static_cast<int64_t>(limit)); !r)
                    return r.error();
            }

            std::vector<DocumentInfo> docs;
            while (true) {
                auto stepRes = stmt.step();
                if (!stepRes)
                    return stepRes.error();
                if (!stepRes.value())
                    break;
                docs.push_back(mapDocumentRow(stmt));
            }
            return docs;
        });
}

Result<std::vector<PathTreeNode>>
MetadataRepository::listPathTreeChildren(std::string_view fullPath, std::size_t limit) {
    return executeReadQuery<std::vector<PathTreeNode>>(
        [&](Database& db) -> Result<std::vector<PathTreeNode>> {
            bool isRoot = fullPath.empty() || fullPath == "/";
            int64_t parentId = kPathTreeNullParent;

            if (!isRoot) {
                auto parentRes = findPathTreeNodeByFullPath(fullPath);
                if (!parentRes)
                    return parentRes.error();
                const auto& parentOpt = parentRes.value();
                if (!parentOpt)
                    return std::vector<PathTreeNode>{};
                parentId = parentOpt->id;
            }

            std::string sql = "SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                              "centroid_weight, centroid "
                              "FROM path_tree_nodes ";
            if (isRoot) {
                sql += "WHERE parent_id IS NULL ";
            } else {
                sql += "WHERE parent_id = ? ";
            }
            sql += "ORDER BY doc_count DESC, path_segment ASC ";
            if (limit > 0)
                sql += "LIMIT ?";

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            int bindIndex = 1;
            if (!isRoot) {
                if (auto bindParent = stmt.bind(bindIndex++, parentId); !bindParent)
                    return bindParent.error();
            }
            if (limit > 0) {
                if (auto bindLimit = stmt.bind(bindIndex, static_cast<int64_t>(limit)); !bindLimit)
                    return bindLimit.error();
            }

            std::vector<PathTreeNode> children;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                children.push_back(mapPathTreeNodeRow(stmt));
            }
            return children;
        });
}

namespace {
std::vector<std::string> splitPathSegments(const std::string& normalizedPath) {
    std::vector<std::string> segments;
    std::string segment;
    for (char ch : normalizedPath) {
        if (ch == '/') {
            if (!segment.empty()) {
                segments.push_back(segment);
                segment.clear();
            }
        } else {
            segment.push_back(ch);
        }
    }
    if (!segment.empty())
        segments.push_back(segment);
    return segments;
}
} // namespace

Result<void> MetadataRepository::upsertPathTreeForDocument(const DocumentInfo& info,
                                                           int64_t documentId, bool isNewDocument,
                                                           std::span<const float> embeddingValues) {
    if (info.filePath.empty())
        return Result<void>();

    auto segments = splitPathSegments(info.filePath);
    if (segments.empty())
        return Result<void>();

    const bool isAbsolute = !info.filePath.empty() && info.filePath.front() == '/';

    int64_t parentNodeId = kPathTreeNullParent;
    std::string currentPath = isAbsolute ? std::string("/") : std::string{};

    for (const auto& part : segments) {
        if (!currentPath.empty() && currentPath.back() != '/')
            currentPath.push_back('/');
        currentPath += part;

        auto nodeResult = findPathTreeNode(parentNodeId, part);
        if (!nodeResult)
            return nodeResult.error();

        PathTreeNode node;
        if (nodeResult.value()) {
            node = *nodeResult.value();
        } else {
            auto insertResult = insertPathTreeNode(parentNodeId, part, currentPath);
            if (!insertResult)
                return insertResult.error();
            node = insertResult.value();
        }

        if (isNewDocument) {
            auto inc = incrementPathTreeDocCount(node.id, documentId);
            if (!inc)
                return inc.error();
        }

        if (!embeddingValues.empty()) {
            auto acc = accumulatePathTreeCentroid(node.id, embeddingValues);
            if (!acc)
                return acc.error();
        }

        parentNodeId = node.id;
    }

    return Result<void>();
}

Result<void> MetadataRepository::removePathTreeForDocument(const DocumentInfo& info,
                                                           int64_t documentId,
                                                           std::span<const float> embeddingValues) {
    if (info.filePath.empty())
        return Result<void>();

    auto segments = splitPathSegments(info.filePath);
    if (segments.empty())
        return Result<void>();

    const bool isAbsolute = !info.filePath.empty() && info.filePath.front() == '/';
    int64_t parentNodeId = kPathTreeNullParent;
    std::string currentPath = isAbsolute ? std::string("/") : std::string{};

    // Collect all node IDs along the path for later cleanup
    std::vector<int64_t> pathNodeIds;

    for (const auto& part : segments) {
        if (!currentPath.empty() && currentPath.back() != '/')
            currentPath.push_back('/');
        currentPath += part;

        auto nodeResult = findPathTreeNode(parentNodeId, part);
        if (!nodeResult)
            return nodeResult.error();

        if (!nodeResult.value()) {
            // Node doesn't exist, nothing to remove
            return Result<void>();
        }

        pathNodeIds.push_back(nodeResult.value()->id);
        parentNodeId = nodeResult.value()->id;
    }

    // First pass: Remove document associations and decrement counts from leaf to root
    for (auto it = pathNodeIds.rbegin(); it != pathNodeIds.rend(); ++it) {
        int64_t nodeId = *it;

        // Remove the document-node association
        auto removeAssoc = executeQuery<void>([&](Database& db) -> Result<void> {
            auto deleteStmt = db.prepare(
                "DELETE FROM path_tree_node_documents WHERE node_id = ? AND document_id = ?");
            if (!deleteStmt)
                return deleteStmt.error();

            auto stmt = std::move(deleteStmt).value();
            if (auto bindNode = stmt.bind(1, nodeId); !bindNode)
                return bindNode.error();
            if (auto bindDoc = stmt.bind(2, documentId); !bindDoc)
                return bindDoc.error();

            return stmt.execute();
        });
        if (!removeAssoc)
            return removeAssoc.error();

        // Decrement doc_count
        auto decrementCount = executeQuery<void>([&](Database& db) -> Result<void> {
            auto updateStmt =
                db.prepare("UPDATE path_tree_nodes "
                           "SET doc_count = MAX(0, doc_count - 1), last_updated = unixepoch() "
                           "WHERE node_id = ?");
            if (!updateStmt)
                return updateStmt.error();

            auto stmt = std::move(updateStmt).value();
            if (auto bindNode = stmt.bind(1, nodeId); !bindNode)
                return bindNode.error();

            return stmt.execute();
        });
        if (!decrementCount)
            return decrementCount.error();

        // Recalculate centroid if embedding was provided
        if (!embeddingValues.empty()) {
            auto recalc = executeQuery<void>([&](Database& db) -> Result<void> {
                // Get current centroid and weight
                auto selectStmt =
                    db.prepare("SELECT centroid, centroid_weight FROM path_tree_nodes "
                               "WHERE node_id = ?");
                if (!selectStmt)
                    return selectStmt.error();

                auto stmt = std::move(selectStmt).value();
                if (auto bindNode = stmt.bind(1, nodeId); !bindNode)
                    return bindNode.error();

                auto stepRes = stmt.step();
                if (!stepRes)
                    return stepRes.error();
                if (!stepRes.value())
                    return Result<void>(); // Node doesn't exist anymore

                std::vector<float> centroid;
                int64_t currentWeight = stmt.getInt64(1);

                if (!stmt.isNull(0)) {
                    centroid = blobToFloatVector(stmt.getBlob(0));
                }

                if (currentWeight <= 1) {
                    // Reset centroid when no documents remain
                    auto updateStmt = db.prepare("UPDATE path_tree_nodes "
                                                 "SET centroid = NULL, centroid_weight = 0, "
                                                 "last_updated = unixepoch() "
                                                 "WHERE node_id = ?");
                    if (!updateStmt)
                        return updateStmt.error();

                    auto update = std::move(updateStmt).value();
                    if (auto bindNode = update.bind(1, nodeId); !bindNode)
                        return bindNode.error();

                    return update.execute();
                }

                if (centroid.size() == embeddingValues.size()) {
                    const double oldWeightFactor = static_cast<double>(currentWeight);
                    const int64_t newWeight = currentWeight - 1;
                    const double newWeightFactor = static_cast<double>(newWeight);
                    for (std::size_t i = 0; i < embeddingValues.size(); ++i) {
                        double updated =
                            (centroid[i] * oldWeightFactor - embeddingValues[i]) / newWeightFactor;
                        centroid[i] = static_cast<float>(updated);
                    }

                    auto updateStmt = db.prepare("UPDATE path_tree_nodes "
                                                 "SET centroid = ?, centroid_weight = ?, "
                                                 "last_updated = unixepoch() "
                                                 "WHERE node_id = ?");
                    if (!updateStmt)
                        return updateStmt.error();

                    auto update = std::move(updateStmt).value();

                    std::span<const std::byte> blob(
                        reinterpret_cast<const std::byte*>(centroid.data()),
                        centroid.size() * sizeof(float));
                    if (auto bindBlob = update.bind(1, blob); !bindBlob)
                        return bindBlob.error();
                    if (auto bindWeight = update.bind(2, newWeight); !bindWeight)
                        return bindWeight.error();
                    if (auto bindNode = update.bind(3, nodeId); !bindNode)
                        return bindNode.error();

                    return update.execute();
                }

                return Result<void>();
            });
            if (!recalc)
                return recalc.error();
        }
    }

    // Second pass: Delete empty nodes from leaf to root, but keep top-level directories
    // This ensures parent nodes are only checked after all their children have been processed
    for (size_t i = 0; i < pathNodeIds.size(); ++i) {
        size_t reverseIdx = pathNodeIds.size() - 1 - i;
        int64_t nodeId = pathNodeIds[reverseIdx];

        // Keep first-level directories (direct children of root) even if empty
        bool isFirstLevel = (reverseIdx == 0);
        if (isFirstLevel) {
            continue; // Don't delete top-level directories
        }

        // Check if node is now empty and has no children
        auto shouldDelete = executeQuery<bool>([&](Database& db) -> Result<bool> {
            auto checkStmt = db.prepare(
                "SELECT doc_count, "
                "(SELECT COUNT(*) FROM path_tree_nodes WHERE parent_id = ?) as child_count "
                "FROM path_tree_nodes WHERE node_id = ?");
            if (!checkStmt)
                return checkStmt.error();

            auto stmt = std::move(checkStmt).value();
            if (auto bind1 = stmt.bind(1, nodeId); !bind1)
                return bind1.error();
            if (auto bind2 = stmt.bind(2, nodeId); !bind2)
                return bind2.error();

            auto stepRes = stmt.step();
            if (!stepRes)
                return stepRes.error();
            if (!stepRes.value())
                return false; // Node doesn't exist

            int64_t docCount = stmt.getInt64(0);
            int64_t childCount = stmt.getInt64(1);

            return (docCount == 0 && childCount == 0);
        });

        if (!shouldDelete)
            return shouldDelete.error();

        if (shouldDelete.value()) {
            auto deleteNode = executeQuery<void>([&](Database& db) -> Result<void> {
                auto deleteStmt = db.prepare("DELETE FROM path_tree_nodes WHERE node_id = ?");
                if (!deleteStmt)
                    return deleteStmt.error();

                auto stmt = std::move(deleteStmt).value();
                if (auto bindNode = stmt.bind(1, nodeId); !bindNode)
                    return bindNode.error();

                return stmt.execute();
            });
            if (!deleteNode)
                return deleteNode.error();
        }
    }

    return Result<void>();
}

// -----------------------------------------------------------------------------
// Tree-based document queries (PBI-043 integration)
// -----------------------------------------------------------------------------

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByPathTreePrefix(std::string_view pathPrefix,
                                                  bool includeSubdirectories, int limit) {
    DocumentQueryOptions opts;
    if (!pathPrefix.empty())
        opts.pathPrefix = std::string(pathPrefix);
    opts.includeSubdirectories = includeSubdirectories;
    opts.limit = limit;
    return queryDocuments(opts);
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
    auto unixTime =
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count();
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder&
MetadataQueryBuilder::modifiedBefore(std::chrono::system_clock::time_point time) {
    conditions_.push_back("modified_time <= ?");
    auto unixTime =
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count();
    parameters_.push_back(std::to_string(unixTime));
    return *this;
}

MetadataQueryBuilder&
MetadataQueryBuilder::indexedAfter(std::chrono::system_clock::time_point time) {
    conditions_.push_back("indexed_time >= ?");
    auto unixTime =
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count();
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
