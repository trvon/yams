#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <sstream>
#include <string_view>
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
#include <yams/storage/corpus_stats.h>

namespace yams::metadata {

namespace {
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

constexpr int64_t kPathTreeNullParent = PathTreeNode::kNullParent;

std::vector<float> blobToFloatVector(const std::vector<std::byte>& blob) {
    if (blob.empty() || (blob.size() % sizeof(float)) != 0)
        return {};
    std::vector<float> out(blob.size() / sizeof(float));
    std::memcpy(out.data(), blob.data(), blob.size());
    return out;
}

PathTreeNode mapPathTreeNodeRow(Statement& stmt) {
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

MetadataRepository::MetadataRepository(ConnectionPool& pool) : pool_(pool) {
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
MetadataRepository::~MetadataRepository() = default;

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

Result<std::optional<DocumentInfo>> MetadataRepository::getDocument(int64_t id) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getDocument");
    return executeQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            using yams::metadata::sql::QuerySpec;
            const char* cols =
                hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {cols};
            spec.conditions = {"id = ?"};
            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, id);
            if (!bindResult)
                return bindResult.error();

            auto stepResult = stmt.step();
            if (!stepResult)
                return stepResult.error();

            if (!stepResult.value()) {
                return std::optional<DocumentInfo>{};
            }

            return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
        });
}

// Internal helper that uses an existing connection to avoid nested connection acquisition deadlock
Result<std::optional<DocumentInfo>> MetadataRepository::getDocumentInternal(Database& db,
                                                                            int64_t id) {
    using yams::metadata::sql::QuerySpec;
    const char* cols = hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
    QuerySpec spec{};
    spec.table = "documents";
    spec.columns = {cols};
    spec.conditions = {"id = ?"};
    auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto bindResult = stmt.bind(1, id);
    if (!bindResult)
        return bindResult.error();

    auto stepResult = stmt.step();
    if (!stepResult)
        return stepResult.error();

    if (!stepResult.value()) {
        return std::optional<DocumentInfo>{};
    }

    return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
}

// Internal helper that uses an existing connection to avoid nested connection acquisition deadlock
Result<std::unordered_map<std::string, MetadataValue>>
MetadataRepository::getAllMetadataInternal(Database& db, int64_t documentId) {
    using yams::metadata::sql::QuerySpec;
    QuerySpec spec{};
    spec.table = "metadata";
    spec.columns = {"key", "value", "value_type"};
    spec.conditions = {"document_id = ?"};
    auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

    if (!stmtResult)
        return stmtResult.error();

    Statement stmt = std::move(stmtResult).value();
    auto bindResult = stmt.bind(1, documentId);
    if (!bindResult)
        return bindResult.error();

    std::unordered_map<std::string, MetadataValue> result;

    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
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
    return executeQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            using yams::metadata::sql::QuerySpec;
            const char* cols =
                hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {cols};
            spec.conditions = {"sha256_hash = ?"};
            auto sql = yams::metadata::sql::buildSelect(spec);
            auto stmtResult = db.prepare(sql);

            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, hash);
            if (!bindResult) {
                return bindResult.error();
            }

            auto stepResult = stmt.step();
            if (!stepResult) {
                return stepResult.error();
            }

            if (!stepResult.value()) {
                return std::optional<DocumentInfo>{};
            }

            return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
        });
}

Result<void> MetadataRepository::updateDocument(const DocumentInfo& info) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            UPDATE documents SET
                file_path = ?, file_name = ?, file_extension = ?,
                file_size = ?, sha256_hash = ?, mime_type = ?,
                created_time = ?, modified_time = ?, indexed_time = ?,
                content_extracted = ?, extraction_status = ?,
                extraction_error = ?, path_prefix = ?, reverse_path = ?,
                path_hash = ?, parent_hash = ?, path_depth = ?
            WHERE id = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(
            info.filePath, info.fileName, info.fileExtension, info.fileSize, info.sha256Hash,
            info.mimeType, info.createdTime, info.modifiedTime, info.indexedTime,
            info.contentExtracted ? 1 : 0, ExtractionStatusUtils::toString(info.extractionStatus),
            info.extractionError, info.pathPrefix, info.reversePath, info.pathHash, info.parentHash,
            info.pathDepth, info.id);

        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

Result<void> MetadataRepository::deleteDocument(int64_t id) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Query document flags before deletion to update counters
        bool wasExtracted = false;
        bool wasIndexed = false;
        {
            auto checkStmt = db.prepare(R"(
                SELECT d.content_extracted, COALESCE(des.has_embedding, 0)
                FROM documents d
                LEFT JOIN document_embeddings_status des ON d.id = des.document_id
                WHERE d.id = ?
            )");
            if (checkStmt) {
                auto& stmt = checkStmt.value();
                stmt.bind(1, id);
                if (auto stepRes = stmt.step(); stepRes && stepRes.value()) {
                    wasExtracted = stmt.getInt(0) != 0;
                    wasIndexed = stmt.getInt(1) != 0;
                }
            }
        }

        // Foreign key constraints will handle cascading deletes
        auto stmtResult = db.prepare("DELETE FROM documents WHERE id = ?");
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, id);
        if (!bindResult)
            return bindResult.error();

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        // Update component-owned metrics (using saturating subtraction to prevent underflow)
        if (db.changes() > 0) {
            core::saturating_sub(cachedDocumentCount_, uint64_t{1});
            if (wasExtracted) {
                core::saturating_sub(cachedExtractedCount_, uint64_t{1});
            }
            if (wasIndexed) {
                core::saturating_sub(cachedIndexedCount_, uint64_t{1});
            }
        }

        return Result<void>();
    });
}

// Content operations
Result<void> MetadataRepository::insertContent(const DocumentContent& content) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
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

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        const std::string sanitizedContent = common::sanitizeUtf8(content.contentText);
        auto bindResult = stmt.bindAll(content.documentId, sanitizedContent,
                                       static_cast<int64_t>(sanitizedContent.length()),
                                       content.extractionMethod, content.language);

        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

Result<std::optional<DocumentContent>> MetadataRepository::getContent(int64_t documentId) {
    return executeQuery<std::optional<DocumentContent>>(
        [&](Database& db) -> Result<std::optional<DocumentContent>> {
            std::string sql = R"(
            SELECT document_id, content_text, content_length,
                   extraction_method, language
            FROM document_content WHERE document_id = ?
        )";

            auto stmtResult = db.prepare(sql);

            if (!stmtResult) {
                return stmtResult.error();
            }

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, documentId);
            if (!bindResult) {
                return bindResult.error();
            }

            auto stepResult = stmt.step();
            if (!stepResult) {
                return stepResult.error();
            }

            if (!stepResult.value()) {
                return std::optional<DocumentContent>{};
            }

            auto content = mapContentRow(stmt);
            return std::optional<DocumentContent>{content};
        });
}

Result<void> MetadataRepository::updateContent(const DocumentContent& content) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            UPDATE document_content SET
                content_text = ?, content_length = ?,
                extraction_method = ?, language = ?
            WHERE document_id = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        const std::string sanitizedContent = common::sanitizeUtf8(content.contentText);
        auto bindResult =
            stmt.bindAll(sanitizedContent, static_cast<int64_t>(sanitizedContent.length()),
                         content.extractionMethod, content.language, content.documentId);

        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

Result<void> MetadataRepository::deleteContent(int64_t documentId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare("DELETE FROM document_content WHERE document_id = ?");
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, documentId);
        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

// Metadata operations
Result<void> MetadataRepository::setMetadata(int64_t documentId, const std::string& key,
                                             const MetadataValue& value) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Use INSERT OR REPLACE to handle both insert and update
        auto stmtResult = db.prepare(R"(
            INSERT OR REPLACE INTO metadata (document_id, key, value, value_type)
            VALUES (?, ?, ?, ?)
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(documentId, key, value.value,
                                       MetadataValueTypeUtils::toString(value.type));

        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

Result<void> MetadataRepository::setMetadataBatch(
    const std::vector<std::tuple<int64_t, std::string, MetadataValue>>& entries) {
    if (entries.empty()) {
        return Result<void>();
    }
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Begin transaction for batch operation
        auto beginResult = db.execute("BEGIN IMMEDIATE");
        if (!beginResult) {
            return beginResult.error();
        }

        // Prepare statement once, reuse for all entries
        auto stmtResult = db.prepare(R"(
            INSERT OR REPLACE INTO metadata (document_id, key, value, value_type)
            VALUES (?, ?, ?, ?)
        )");

        if (!stmtResult) {
            db.execute("ROLLBACK");
            return stmtResult.error();
        }

        Statement stmt = std::move(stmtResult).value();

        for (const auto& [documentId, key, value] : entries) {
            stmt.reset();
            auto bindResult = stmt.bindAll(documentId, key, value.value,
                                           MetadataValueTypeUtils::toString(value.type));
            if (!bindResult) {
                db.execute("ROLLBACK");
                return bindResult.error();
            }

            auto execResult = stmt.execute();
            if (!execResult) {
                db.execute("ROLLBACK");
                return execResult.error();
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

Result<std::optional<MetadataValue>> MetadataRepository::getMetadata(int64_t documentId,
                                                                     const std::string& key) {
    return executeQuery<std::optional<MetadataValue>>(
        [&](Database& db) -> Result<std::optional<MetadataValue>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"value", "value_type"};
            spec.conditions = {"document_id = ?", "key = ?"};
            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bindAll(documentId, key);
            if (!bindResult)
                return bindResult.error();

            auto stepResult = stmt.step();
            if (!stepResult)
                return stepResult.error();

            if (!stepResult.value()) {
                return std::optional<MetadataValue>{};
            }

            MetadataValue value;
            value.value = stmt.getString(0);
            value.type = MetadataValueTypeUtils::fromString(stmt.getString(1));

            return std::optional<MetadataValue>{value};
        });
}

Result<std::unordered_map<std::string, MetadataValue>>
MetadataRepository::getAllMetadata(int64_t documentId) {
    return executeQuery<std::unordered_map<std::string, MetadataValue>>(
        [&](Database& db) -> Result<std::unordered_map<std::string, MetadataValue>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"key", "value", "value_type"};
            spec.conditions = {"document_id = ?"};
            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, documentId);
            if (!bindResult)
                return bindResult.error();

            std::unordered_map<std::string, MetadataValue> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                MetadataValue value;
                value.value = stmt.getString(1);
                value.type = MetadataValueTypeUtils::fromString(stmt.getString(2));

                result[stmt.getString(0)] = value;
            }

            return result;
        });
}

Result<std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>>
MetadataRepository::getMetadataForDocuments(std::span<const int64_t> documentIds) {
    if (documentIds.empty())
        return std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>>{};

    return executeQuery<
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
            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            int index = 1;
            for (auto id : documentIds) {
                if (auto bindRes = stmt.bind(index++, id); !bindRes)
                    return bindRes.error();
            }

            std::unordered_map<int64_t, std::unordered_map<std::string, MetadataValue>> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                int64_t docId = stmt.getInt64(0);
                MetadataValue value;
                value.value = stmt.getString(2);
                value.type = MetadataValueTypeUtils::fromString(stmt.getString(3));
                result[docId][stmt.getString(1)] = value;
            }

            return result;
        });
}

Result<void> MetadataRepository::removeMetadata(int64_t documentId, const std::string& key) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            DELETE FROM metadata WHERE document_id = ? AND key = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(documentId, key);
        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

// Relationship operations
Result<int64_t> MetadataRepository::insertRelationship(const DocumentRelationship& relationship) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO document_relationships (
                parent_id, child_id, relationship_type, created_time
            ) VALUES (?, ?, ?, ?)
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();

        Result<void> bindResult;
        if (relationship.parentId > 0) {
            bindResult =
                stmt.bindAll(relationship.parentId, relationship.childId,
                             relationship.getRelationshipTypeString(), relationship.createdTime);
        } else {
            bindResult = stmt.bind(1, nullptr);
            if (bindResult.has_value()) {
                bindResult = stmt.bind(2, relationship.childId);
            }
            if (bindResult.has_value()) {
                bindResult = stmt.bind(3, relationship.getRelationshipTypeString());
            }
            if (bindResult.has_value()) {
                bindResult = stmt.bind(4, relationship.createdTime);
            }
        }

        if (!bindResult)
            return bindResult.error();

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        return db.lastInsertRowId();
    });
}

Result<std::vector<DocumentRelationship>> MetadataRepository::getRelationships(int64_t documentId) {
    return executeQuery<std::vector<DocumentRelationship>>(
        [&](Database& db) -> Result<std::vector<DocumentRelationship>> {
            auto stmtResult = db.prepare(R"(
            SELECT id, parent_id, child_id, relationship_type, created_time
            FROM document_relationships
            WHERE parent_id = ? OR child_id = ?
        )");

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bindAll(documentId, documentId);
            if (!bindResult)
                return bindResult.error();

            std::vector<DocumentRelationship> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                result.push_back(mapRelationshipRow(stmt));
            }

            return result;
        });
}

Result<void> MetadataRepository::deleteRelationship(int64_t relationshipId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            DELETE FROM document_relationships WHERE id = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, relationshipId);
        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

// Search history operations
Result<int64_t> MetadataRepository::insertSearchHistory(const SearchHistoryEntry& entry) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO search_history (
                query, query_time, results_count, execution_time_ms, user_context
            ) VALUES (?, ?, ?, ?, ?)
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(entry.query, entry.queryTime, entry.resultsCount,
                                       entry.executionTimeMs, entry.userContext);

        if (!bindResult)
            return bindResult.error();

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        return db.lastInsertRowId();
    });
}

Result<std::vector<SearchHistoryEntry>> MetadataRepository::getRecentSearches(int limit) {
    return executeQuery<std::vector<SearchHistoryEntry>>(
        [&](Database& db) -> Result<std::vector<SearchHistoryEntry>> {
            auto stmtResult = db.prepare(R"(
            SELECT id, query, query_time, results_count, execution_time_ms, user_context
            FROM search_history
            ORDER BY query_time DESC
            LIMIT ?
        )");

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, limit);
            if (!bindResult)
                return bindResult.error();

            std::vector<SearchHistoryEntry> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                result.push_back(mapSearchHistoryRow(stmt));
            }

            return result;
        });
}

// Saved queries operations
Result<int64_t> MetadataRepository::insertSavedQuery(const SavedQuery& query) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            INSERT INTO saved_queries (
                name, query, description, created_time, last_used, use_count
            ) VALUES (?, ?, ?, ?, ?, ?)
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(query.name, query.query, query.description,
                                       query.createdTime, query.lastUsed, query.useCount);

        if (!bindResult)
            return bindResult.error();

        auto execResult = stmt.execute();
        if (!execResult)
            return execResult.error();

        return db.lastInsertRowId();
    });
}

Result<std::optional<SavedQuery>> MetadataRepository::getSavedQuery(int64_t id) {
    return executeQuery<std::optional<SavedQuery>>(
        [&](Database& db) -> Result<std::optional<SavedQuery>> {
            auto stmtResult = db.prepare(R"(
            SELECT id, name, query, description, created_time, last_used, use_count
            FROM saved_queries WHERE id = ?
        )");

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, id);
            if (!bindResult)
                return bindResult.error();

            auto stepResult = stmt.step();
            if (!stepResult)
                return stepResult.error();

            if (!stepResult.value()) {
                return std::optional<SavedQuery>{};
            }

            return std::optional<SavedQuery>{mapSavedQueryRow(stmt)};
        });
}

Result<std::vector<SavedQuery>> MetadataRepository::getAllSavedQueries() {
    return executeQuery<std::vector<SavedQuery>>(
        [&](Database& db) -> Result<std::vector<SavedQuery>> {
            auto stmtResult = db.prepare(R"(
            SELECT id, name, query, description, created_time, last_used, use_count
            FROM saved_queries
            ORDER BY use_count DESC, last_used DESC
        )");

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::vector<SavedQuery> result;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                result.push_back(mapSavedQueryRow(stmt));
            }

            return result;
        });
}

Result<void> MetadataRepository::updateSavedQuery(const SavedQuery& query) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            UPDATE saved_queries SET
                name = ?, query = ?, description = ?,
                created_time = ?, last_used = ?, use_count = ?
            WHERE id = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bindAll(query.name, query.query, query.description,
                                       query.createdTime, query.lastUsed, query.useCount, query.id);

        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
    });
}

Result<void> MetadataRepository::deleteSavedQuery(int64_t id) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare("DELETE FROM saved_queries WHERE id = ?");
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, id);
        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
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

    // Delete existing entry first (FTS5 doesn't support ON CONFLICT well)
    auto deleteStmt = db.prepare("DELETE FROM documents_fts WHERE rowid = ?");
    if (!deleteStmt)
        return deleteStmt.error();

    Statement deleteS = std::move(deleteStmt).value();
    auto deleteBind = deleteS.bind(1, documentId);
    if (!deleteBind)
        return deleteBind.error();
    auto deleteResult = deleteS.execute();
    if (!deleteResult)
        return deleteResult.error();

    const std::string sanitizedContent = common::sanitizeUtf8(content);
    const std::string sanitizedTitle = common::sanitizeUtf8(title);

    // Note: content_type removed from FTS5 in migration v18 - never used in MATCH queries
    auto stmtResult = db.prepare(R"(
             INSERT INTO documents_fts (rowid, content, title)
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
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        return indexDocumentContentImpl(db, documentId, title, content, contentType,
                                        /*verifyDocumentExists=*/true);
    });

    if (result)
        invalidateQueryCache();
    return result;
}

Result<void> MetadataRepository::indexDocumentContentTrusted(int64_t documentId,
                                                             const std::string& title,
                                                             const std::string& content,
                                                             const std::string& contentType) {
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        return indexDocumentContentImpl(db, documentId, title, content, contentType,
                                        /*verifyDocumentExists=*/false);
    });

    if (result)
        invalidateQueryCache();
    return result;
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

    if (result)
        invalidateQueryCache();
    return result;
}

Result<void> MetadataRepository::removeFromIndexByHash(const std::string& hash) {
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

        return delStmt.execute();
    });

    if (result)
        invalidateQueryCache();
    return result;
}

Result<std::vector<int64_t>> MetadataRepository::getAllFts5IndexedDocumentIds() {
    return executeQuery<std::vector<int64_t>>([&](Database& db) -> Result<std::vector<int64_t>> {
        // First check if FTS5 is available
        auto fts5Result = db.hasFTS5();
        if (!fts5Result)
            return fts5Result.error();

        if (!fts5Result.value()) {
            return std::vector<int64_t>{}; // FTS5 not available, return empty
        }

        // Query all rowids from FTS5 index (rowid corresponds to document.id)
        auto stmtResult = db.prepare("SELECT DISTINCT rowid FROM documents_fts");
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

        return docIds;
    });
}

// Helper: escape a single term for FTS5 by wrapping in quotes
// Per SQLite docs: replace " with "" and wrap in double quotes
static std::string quoteFTS5Term(const std::string& term) {
    std::string escaped;
    escaped.reserve(term.size() + 4);
    escaped += '"';
    for (char c : term) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

// Strip leading and trailing punctuation from a term.
// Preserves internal punctuation (e.g., hyphens in "sugar-sweetened").
// This is critical for FTS5 matching: "India." won't match "India" in the index.
static std::string stripPunctuation(std::string term) {
    // Strip trailing punctuation
    while (!term.empty()) {
        char c = term.back();
        if (std::isalnum(static_cast<unsigned char>(c)))
            break;
        term.pop_back();
    }
    // Strip leading punctuation (e.g., "(HSC)" -> "HSC")
    while (!term.empty()) {
        char c = term.front();
        if (std::isalnum(static_cast<unsigned char>(c)))
            break;
        term.erase(term.begin());
    }
    return term;
}

// Backwards compatibility alias
static std::string stripTrailingPunctuation(std::string term) {
    return stripPunctuation(std::move(term));
}

static std::vector<std::string> splitFTS5Terms(const std::string& trimmed) {
    std::vector<std::string> terms;
    std::string current;
    for (char c : trimmed) {
        if (c == ' ' || c == '\t') {
            if (!current.empty()) {
                terms.push_back(current);
                current.clear();
            }
        } else {
            current += c;
        }
    }
    if (!current.empty()) {
        terms.push_back(current);
    }
    return terms;
}

static std::vector<std::string> stripPunctuationTokens(const std::vector<std::string>& tokens) {
    std::vector<std::string> stripped;
    stripped.reserve(tokens.size());
    for (const auto& tok : tokens) {
        auto s = stripTrailingPunctuation(tok);
        if (!s.empty()) {
            stripped.push_back(s);
        }
    }
    return stripped;
}

static std::string joinPreview(const std::vector<std::string>& tokens) {
    std::string joined;
    joined.reserve(tokens.size() * 8);
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (i > 0)
            joined += ", ";
        if (tokens[i].size() > 64) {
            joined += tokens[i].substr(0, 64);
            joined += "…";
        } else {
            joined += tokens[i];
        }
    }
    return joined;
}

static std::string buildDiagnosticAltOrQuery(const std::vector<std::string>& tokens) {
    std::vector<std::string> stripped = stripPunctuationTokens(tokens);

    std::vector<std::string> altTerms;
    for (const auto& t : stripped) {
        if (t.size() >= 4) {
            altTerms.push_back(t);
        }
        if (altTerms.size() >= 5)
            break;
    }

    std::string altQuery;
    if (!altTerms.empty()) {
        for (size_t i = 0; i < altTerms.size(); ++i) {
            if (i > 0)
                altQuery += " OR ";
            altQuery += quoteFTS5Term(altTerms[i]);
        }
    }
    return altQuery;
}

static void logFtsTokensIfEnabled(const std::string& rawQuery,
                                  const std::vector<std::string>& tokens) {
    if (const char* env = std::getenv("YAMS_FTS_DEBUG_QUERY"); env && std::string(env) == "1") {
        std::vector<std::string> stripped = stripPunctuationTokens(tokens);
        std::string previewRaw = joinPreview(tokens);
        std::string previewStripped = joinPreview(stripped);
        std::string altQuery = buildDiagnosticAltOrQuery(tokens);

        spdlog::warn(
            "[FTS5] tokens count={} raw='{}' tokens=[{}] stripped=[{}] stripped_dropped={} "
            "diag_alt_or=\"{}\"",
            tokens.size(), rawQuery, previewRaw, previewStripped, tokens.size() - stripped.size(),
            altQuery);
    }
}

// Common English stopwords that add noise to FTS5 AND queries.
// These are filtered out for natural language queries to improve recall.
static const std::unordered_set<std::string>& getStopwords() {
    static const std::unordered_set<std::string> stopwords = {
        "a",    "an",   "and",   "are",   "as",   "at",   "be",   "by",    "for", "from",
        "had",  "has",  "have",  "he",    "her",  "his",  "i",    "in",    "is",  "it",
        "its",  "no",   "not",   "of",    "on",   "or",   "she",  "that",  "the", "their",
        "them", "then", "there", "these", "they", "this", "to",   "was",   "we",  "were",
        "what", "when", "where", "which", "who",  "will", "with", "would", "you", "your",
        "very", "can",  "could", "do",    "does", "did",  "but",  "if",    "so",  "than",
        "too",  "only", "just",  "also"};
    return stopwords;
}

// Check if a term is a stopword (case-insensitive)
static bool isStopword(const std::string& term) {
    std::string lower;
    lower.reserve(term.size());
    for (char c : term) {
        lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return getStopwords().count(lower) > 0;
}

// Helper function to sanitize FTS5 query strings
// Best practice: split into terms and wrap each in quotes to preserve AND behavior
// while safely escaping all FTS5 special characters (~, -, +, *, ^, :, etc.)
// See: https://sqlite.org/forum/forumpost/576d6cc2d2
//
// For natural language (sentence-like) queries with many terms:
// - Strip punctuation from tokens
// - Filter out stopwords
// - Use OR semantics if many terms remain (improves recall for NL queries)
std::string sanitizeFTS5Query(const std::string& query) {
    // Trim whitespace from both ends
    std::string trimmed = query;
    trimmed.erase(0, trimmed.find_first_not_of(" \t\n\r"));
    if (!trimmed.empty()) {
        auto lastNonWs = trimmed.find_last_not_of(" \t\n\r");
        if (lastNonWs != std::string::npos) {
            trimmed.erase(lastNonWs + 1);
        }
    }

    if (trimmed.empty()) {
        return "\"\"";
    }

    // Check if the query uses advanced FTS5 operators (AND, OR, NOT, NEAR, ~ for NOT)
    // Power users can use these directly - do minimal sanitization
    bool hasAdvancedOperators =
        trimmed.find(" AND ") != std::string::npos || trimmed.find(" OR ") != std::string::npos ||
        trimmed.find(" NOT ") != std::string::npos || trimmed.find("NEAR(") != std::string::npos ||
        trimmed.find('~') != std::string::npos;

    if (hasAdvancedOperators) {
        // For advanced queries, just escape quotes
        std::string escaped;
        for (char c : trimmed) {
            if (c == '"') {
                escaped += "\"\"";
            } else {
                escaped += c;
            }
        }
        return escaped;
    }

    std::vector<std::string> terms = splitFTS5Terms(trimmed);
    if (terms.empty()) {
        return "\"\"";
    }

    logFtsTokensIfEnabled(query, terms);

    // Strip punctuation from terms before quoting to improve matching.
    // FTS5 requires exact matches for quoted terms, so "India." won't match "India".
    // By stripping punctuation, natural language queries match corpus text better.
    std::vector<std::string> cleanedTerms;
    cleanedTerms.reserve(terms.size());
    for (const auto& t : terms) {
        std::string cleaned = stripPunctuation(t);
        if (!cleaned.empty()) {
            cleanedTerms.push_back(std::move(cleaned));
        }
    }

    if (cleanedTerms.empty()) {
        return "\"\"";
    }

    // For natural language queries (many terms), filter stopwords and use OR semantics.
    // This dramatically improves recall for sentence-like queries in BEIR/SciFact benchmarks.
    // Heuristic: if query has 5+ terms, treat as natural language and use OR on content terms.
    constexpr size_t kNaturalLanguageThreshold = 5;
    const bool isNaturalLanguageQuery = cleanedTerms.size() >= kNaturalLanguageThreshold;

    if (isNaturalLanguageQuery) {
        // Filter out stopwords and keep only significant content terms
        std::vector<std::string> contentTerms;
        contentTerms.reserve(cleanedTerms.size());
        for (const auto& t : cleanedTerms) {
            if (!isStopword(t) && t.size() >= 2) {
                contentTerms.push_back(t);
            }
        }

        // If we have content terms, use OR semantics for better recall
        if (!contentTerms.empty()) {
            std::string result;
            for (size_t i = 0; i < contentTerms.size(); ++i) {
                if (i > 0)
                    result += " OR ";
                result += quoteFTS5Term(contentTerms[i]);
            }
            return result;
        }
        // Fall through to AND if no content terms remain
    }

    // For short queries (< 5 terms), use AND semantics (more precise)
    std::string result;
    for (size_t i = 0; i < cleanedTerms.size(); ++i) {
        if (i > 0)
            result += ' ';
        result += quoteFTS5Term(cleanedTerms[i]);
    }
    return result;
}

Result<SearchResults>
MetadataRepository::search(const std::string& query, int limit, int offset,
                           const std::optional<std::vector<int64_t>>& docIds) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::search");
    return executeQuery<SearchResults>([&](Database& db) -> Result<SearchResults> {
        YAMS_ZONE_SCOPED_N("MetadataRepo::search::FTS5Query");
        SearchResults results;
        results.query = query;

        // PERFORMANCE FIX: Cap limit to prevent massive result processing
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
                        ++it->second.hits;
                        return it->second.results;
                    }
                }
            }
        }

        auto start = std::chrono::high_resolution_clock::now();

        // FTS5 availability is verified at startup, skip check for performance
        // Sanitize the query to prevent FTS5 syntax errors
        std::string sanitizedQuery = sanitizeFTS5Query(query);

        // Optional debug: log raw vs sanitized query.
        // This is intentionally opt-in because it can be noisy and may contain user text.
        bool ftsDebug = false;
        if (const char* env = std::getenv("YAMS_FTS_DEBUG_QUERY"); env && std::string(env) == "1") {
            ftsDebug = true;
            spdlog::warn("[FTS5] raw='{}' sanitized='{}' limit={} offset={} docIds={}", query,
                         sanitizedQuery, limit, offset, (docIds ? docIds->size() : 0));
        }

        // Diagnostic-only: execute an alternative OR-style query built from stripped tokens to
        // gauge strictness of AND behavior. This must not affect normal results.
        std::optional<size_t> diagAltHitCount;
        std::string diagAltQuery;
        if (ftsDebug) {
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

        using yams::metadata::sql::QuerySpec;
        QuerySpec spec{};
        spec.table = "documents_fts";
        spec.from =
            std::optional<std::string>{"documents_fts fts JOIN documents d ON d.id = fts.rowid"};
        spec.columns = {"fts.rowid",
                        "fts.title",
                        "snippet(documents_fts, 0, '<b>', '</b>', '...', 16) as snippet",
                        "bm25(documents_fts) as score",
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
        spec.conditions.emplace_back("documents_fts MATCH ?");
        // Optional ID filter (dynamic IN placeholder list)
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

        bool ftsSearchSucceeded = false;
        auto stmtResult = db.prepare(sql);

        if (stmtResult) {
            Statement stmt = std::move(stmtResult).value();
            // Bind: MATCH term, optional id list, limit, offset
            int bindIndex = 1;
            auto b1 = stmt.bind(bindIndex++, sanitizedQuery);
            if (!b1)
                return b1.error();
            if (docIds && !docIds->empty()) {
                for (auto id : *docIds) {
                    auto b = stmt.bind(bindIndex++, static_cast<int64_t>(id));
                    if (!b)
                        return b.error();
                }
            }
            {
                // Execute the FTS5 search
                size_t rowCount = 0;
                while (true) {
                    auto stepResult = stmt.step();
                    if (!stepResult) {
                        break;
                    }
                    if (!stepResult.value()) {
                        ftsSearchSucceeded = true;
                        break;
                    }

                    rowCount++;

                    SearchResult result;

                    // Map document info
                    result.document.id = stmt.getInt64(0);
                    result.document.filePath = stmt.getString(4);
                    result.document.fileName = stmt.getString(5);
                    result.document.fileExtension = stmt.getString(6);
                    result.document.fileSize = stmt.getInt64(7);
                    result.document.sha256Hash = stmt.getString(8);
                    result.document.mimeType = stmt.getString(9);
                    result.document.createdTime = stmt.getTime(10);
                    result.document.modifiedTime = stmt.getTime(11);
                    result.document.indexedTime = stmt.getTime(12);
                    result.document.contentExtracted = stmt.getInt(13) != 0;
                    result.document.extractionStatus =
                        ExtractionStatusUtils::fromString(stmt.getString(14));
                    result.document.extractionError = stmt.getString(15);

                    // Search-specific fields
                    result.snippet = common::sanitizeUtf8(stmt.getString(2));
                    result.score = stmt.getDouble(3);

                    results.results.push_back(result);
                    ftsSearchSucceeded = true;
                }

                // Get total count for FTS5 results
                // PERFORMANCE FIX: Skip expensive COUNT(*) for large result sets
                // If we got back fewer results than the limit, that's the total count
                if (ftsSearchSucceeded) {
                    const size_t resultSize = results.results.size();
                    const size_t requestedLimit = static_cast<size_t>(limit);

                    // Fast path: if we got fewer than limit, that's the exact count
                    if (resultSize < requestedLimit) {
                        results.totalCount = resultSize;
                    } else {
                        // We hit the limit - need to count, but with timeout protection
                        // Use a fast heuristic: try counting with a LIMIT to avoid full scans
                        constexpr int64_t kMaxCountLimit = 10000;
                        auto countStmtResult = db.prepare(R"(
                            SELECT COUNT(*) FROM (
                                SELECT 1 FROM documents_fts 
                                WHERE documents_fts MATCH ? 
                                LIMIT ?
                            )
                        )");

                        if (countStmtResult) {
                            Statement countStmt = std::move(countStmtResult).value();
                            auto bindRes1 = countStmt.bind(1, sanitizedQuery);
                            auto bindRes2 = countStmt.bind(2, kMaxCountLimit);
                            if (bindRes1.has_value() && bindRes2.has_value()) {
                                auto stepRes = countStmt.step();
                                if (stepRes.has_value() && stepRes.value()) {
                                    int64_t boundedCount = countStmt.getInt64(0);
                                    results.totalCount = boundedCount;
                                    // If we hit the limit, indicate there are "many more"
                                    if (boundedCount >= kMaxCountLimit) {
                                        spdlog::debug(
                                            "Search matched >{} results, using approximate count",
                                            kMaxCountLimit);
                                    }
                                }
                            }
                        } else {
                            // Fallback: just use result size as lower bound
                            results.totalCount = resultSize;
                            spdlog::debug("Count query failed, using result size as count");
                        }
                    }
                }
            }
        } else {
            spdlog::debug("FTS5 search prepare failed: {}", stmtResult.error().message);
        }

        // If FTS5 search failed, fall back to fuzzy search (noise-reduced to debug)
        if (!ftsSearchSucceeded) {
            spdlog::debug("FTS5 search failed for query '{}', falling back to fuzzy search", query);

            // Use fuzzy search as fallback (note: fuzzy search doesn't support offset)
            auto fuzzyResults = fuzzySearch(query, 0.3, limit);
            if (fuzzyResults) {
                results = fuzzyResults.value();
                // Add a note in the error message that we fell back to fuzzy search
                // but don't treat it as a failure since we have results
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

// Bulk operations
Result<std::optional<DocumentInfo>>
MetadataRepository::findDocumentByExactPath(const std::string& path) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::findDocumentByExactPath");
    auto derived = computePathDerivedValues(path);
    if (auto cached = lookupPathCache(derived.normalizedPath))
        return cached;

    return executeQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            using yams::metadata::sql::QuerySpec;
            const char* cols =
                hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {cols};
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
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            const bool joinFtsForContains = options.containsFragment && options.containsUsesFts &&
                                            !options.containsFragment->empty() && pathFtsAvailable_;

            std::string sql = "SELECT ";
            if (joinFtsForContains) {
                sql += hasPathIndexing_ ? kDocumentColumnListNewQualified
                                        : kDocumentColumnListCompatQualified;
            } else {
                sql += hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            }
            sql += " FROM documents";
            if (joinFtsForContains)
                sql += " JOIN documents_path_fts ON documents.id = documents_path_fts.rowid";

            std::vector<std::string> conditions;
            struct BindParam {
                enum class Type { Text, Int } type;
                std::string text;
                int64_t integer{0};
            };
            std::vector<BindParam> params;

            auto addText = [&](std::string value) {
                params.push_back(BindParam{BindParam::Type::Text, std::move(value), 0});
            };
            auto addInt = [&](int64_t value) {
                params.push_back(BindParam{BindParam::Type::Int, {}, value});
            };

            if (options.exactPath) {
                auto derived = computePathDerivedValues(*options.exactPath);
                const bool pathsDiffer = derived.normalizedPath != *options.exactPath;
                if (hasPathIndexing_) {
                    std::string clause = "(path_hash = ? OR file_path = ?";
                    addText(derived.pathHash);
                    addText(derived.normalizedPath);
                    if (pathsDiffer) {
                        clause += " OR file_path = ?";
                        addText(*options.exactPath);
                    }
                    clause += ')';
                    conditions.emplace_back(std::move(clause));
                } else {
                    if (pathsDiffer) {
                        conditions.emplace_back("(file_path = ? OR file_path = ?)");
                        addText(derived.normalizedPath);
                        addText(*options.exactPath);
                    } else {
                        conditions.emplace_back("file_path = ?");
                        addText(derived.normalizedPath);
                    }
                }
                spdlog::info(
                    "[MetadataRepository] exactPath query path='{}' normalized='{}' hash={}",
                    *options.exactPath, derived.normalizedPath, derived.pathHash);
            }

            if (options.pathPrefix && !options.pathPrefix->empty()) {
                const std::string& originalPrefix = *options.pathPrefix;
                bool treatAsDirectory = options.prefixIsDirectory;
                if (!treatAsDirectory) {
                    char tail = originalPrefix.back();
                    treatAsDirectory = (tail == '/' || tail == '\\');
                }

                auto derived = computePathDerivedValues(originalPrefix);
                std::string normalized = derived.normalizedPath;

                if (treatAsDirectory) {
                    // Ensure we operate on directory component without trailing slash
                    if (!normalized.empty() && normalized.back() == '/')
                        normalized.pop_back();

                    if (!normalized.empty()) {
                        if (hasPathIndexing_) {
                            std::string clause = "(path_prefix = ?";
                            addText(normalized);
                            if (options.includeSubdirectories) {
                                clause += " OR path_prefix LIKE ?";
                                std::string likeValue = normalized;
                                likeValue.append("/%");
                                addText(likeValue);
                            }
                            clause += ')';
                            conditions.emplace_back(std::move(clause));
                        } else {
                            std::string likeValue = normalized;
                            likeValue.append("/%");
                            conditions.emplace_back("file_path LIKE ?");
                            addText(likeValue);
                        }
                    } else {
                        if (!options.includeSubdirectories) {
                            if (hasPathIndexing_) {
                                conditions.emplace_back("path_prefix = ''");
                            } else {
                                // Root-only without subdirectories doesn't have a strict
                                // equivalent; approximate with file_path NOT LIKE '%/%'
                                // which filters to top-level entries.
                                conditions.emplace_back("file_path NOT LIKE '%/%'");
                            }
                        }
                        // When querying from repository root with includeSubdirectories,
                        // the prefix condition would match everything; omit predicate.
                    }
                } else {
                    std::string lower = normalized;
                    std::string upper = normalized;
                    upper.push_back(static_cast<char>(0xFF));
                    conditions.emplace_back("(file_path >= ? AND file_path < ?)");
                    addText(lower);
                    addText(upper);
                }
            }

            if (options.containsFragment && !options.containsFragment->empty()) {
                std::string fragment = *options.containsFragment;
                std::replace(fragment.begin(), fragment.end(), '\\', '/');

                if (joinFtsForContains) {
                    std::string ftsToken = fragment;
                    auto slashPos = ftsToken.find_last_of('/');
                    if (slashPos != std::string::npos)
                        ftsToken = ftsToken.substr(slashPos + 1);
                    std::replace(ftsToken.begin(), ftsToken.end(), '"', ' ');
                    if (!ftsToken.empty() && ftsToken.back() != '*')
                        ftsToken.push_back('*');
                    conditions.emplace_back("documents_path_fts MATCH ?");
                    addText(ftsToken);
                }

                if (hasPathIndexing_) {
                    std::string reversed(fragment.rbegin(), fragment.rend());
                    conditions.emplace_back("reverse_path LIKE ?");
                    addText(reversed + "%");
                } else {
                    conditions.emplace_back("file_path LIKE ?");
                    addText("%" + fragment + "%");
                }
            }

            if (options.likePattern && !options.likePattern->empty()) {
                conditions.emplace_back("file_path LIKE ?");
                addText(*options.likePattern);
            }

            if (options.fileName && !options.fileName->empty()) {
                conditions.emplace_back("file_name = ?");
                addText(*options.fileName);
            }

            if (options.extension && !options.extension->empty()) {
                conditions.emplace_back("file_extension = ?");
                addText(*options.extension);
            }

            if (options.mimeType && !options.mimeType->empty()) {
                conditions.emplace_back("mime_type = ?");
                addText(*options.mimeType);
            }

            if (options.textOnly) {
                conditions.emplace_back("mime_type LIKE 'text/%'");
            } else if (options.binaryOnly) {
                conditions.emplace_back("mime_type NOT LIKE 'text/%'");
            }

            if (options.modifiedAfter) {
                conditions.emplace_back("modified_time >= ?");
                addInt(*options.modifiedAfter);
            }

            if (options.modifiedBefore) {
                conditions.emplace_back("modified_time <= ?");
                addInt(*options.modifiedBefore);
            }

            if (options.indexedAfter) {
                conditions.emplace_back("indexed_time >= ?");
                addInt(*options.indexedAfter);
            }

            if (options.indexedBefore) {
                conditions.emplace_back("indexed_time <= ?");
                addInt(*options.indexedBefore);
            }

            for (const auto& tag : options.tags) {
                conditions.emplace_back(
                    "EXISTS (SELECT 1 FROM metadata m WHERE m.document_id = documents.id "
                    "AND m.key = ? AND m.value = ?)");
                addText(std::string("tag:") + tag);
                addText(tag);
            }

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
                addInt(options.limit);
            }
            if (options.offset > 0) {
                sql += " OFFSET ?";
                addInt(options.offset);
            }

            auto stmtResult = db.prepare(sql);
            if (!stmtResult) {
                if (joinFtsForContains && options.containsUsesFts) {
                    spdlog::debug("MetadataRepository::queryDocuments prepare failed (falling back "
                                  "to without FTS): {}\nSQL: {}",
                                  stmtResult.error().message, sql);
                    auto fallbackOpts = options;
                    fallbackOpts.containsUsesFts = false;
                    return queryDocuments(fallbackOpts);
                }
                spdlog::error("MetadataRepository::queryDocuments prepare failed: {}\nSQL: {}",
                              stmtResult.error().message, sql);
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
                    spdlog::error(
                        "MetadataRepository::queryDocuments bind failed (index={}): {}\nSQL: {}",
                        index, bindResult.error().message, sql);
                    return bindResult.error();
                }
                ++index;
            }

            std::vector<DocumentInfo> docs;
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult) {
                    if (joinFtsForContains && options.containsUsesFts) {
                        spdlog::debug("MetadataRepository::queryDocuments step failed (falling "
                                      "back to without FTS): {}\nSQL: {}",
                                      stepResult.error().message, sql);
                        auto fallbackOpts = options;
                        fallbackOpts.containsUsesFts = false;
                        return queryDocuments(fallbackOpts);
                    }
                    spdlog::error("MetadataRepository::queryDocuments step failed: {}\nSQL: {}",
                                  stepResult.error().message, sql);
                    return stepResult.error();
                }
                if (!stepResult.value())
                    break;

                auto doc = mapDocumentRow(stmt);
                storePathCache(doc);
                docs.push_back(std::move(doc));
            }

            return docs;
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByHashPrefix(const std::string& hashPrefix, std::size_t limit) {
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            if (hashPrefix.empty()) {
                return std::vector<DocumentInfo>{};
            }

            std::string lowered = hashPrefix;
            std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

            // Build query via helper
            const char* cols =
                hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            sql::QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {cols};
            spec.conditions = {"lower(sha256_hash) LIKE ?"};
            spec.orderBy = std::optional<std::string>("indexed_time DESC");
            spec.limit = static_cast<int>(limit);
            spec.offset = std::nullopt;
            auto stmtResult = db.prepare(sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindPrefix = stmt.bind(1, lowered + "%");
            if (!bindPrefix)
                return bindPrefix.error();
            // Limit is embedded in SQL

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
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByExtension(const std::string& extension) {
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            const char* cols =
                hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            sql::QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {cols};
            spec.conditions = {"file_extension = ?"};
            spec.orderBy = std::optional<std::string>("file_name");
            spec.limit = std::nullopt;
            spec.offset = std::nullopt;

            auto stmtResult = db.prepare(sql::buildSelect(spec));
            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, extension);
            if (!bindResult)
                return bindResult.error();

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
        });
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsModifiedSince(std::chrono::system_clock::time_point since) {
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            using yams::metadata::sql::QuerySpec;
            auto sinceUnix =
                std::chrono::duration_cast<std::chrono::seconds>(since.time_since_epoch()).count();

            const char* cols =
                hasPathIndexing_ ? kDocumentColumnListNew : kDocumentColumnListCompat;
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {cols};
            spec.conditions = {"modified_time >= ?"};
            spec.orderBy = std::optional<std::string>("modified_time DESC");

            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, static_cast<int64_t>(sinceUnix));
            if (!bindResult)
                return bindResult.error();

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
        });
}

// Statistics
Result<int64_t> MetadataRepository::getDocumentCount() {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        using yams::metadata::sql::QuerySpec;
        QuerySpec spec{};
        spec.table = "documents";
        spec.columns = {"COUNT(*)"};
        auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();

        return stmt.getInt64(0);
    });
}

Result<int64_t> MetadataRepository::getIndexedDocumentCount() {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        auto stmtResult = db.prepare(R"(
            SELECT COUNT(DISTINCT d.id) 
            FROM documents d
            LEFT JOIN document_embeddings_status des ON d.id = des.document_id
            WHERE des.has_embedding = 1
        )");
        if (!stmtResult)
            return stmtResult.error();
        Statement stmt = std::move(stmtResult).value();
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        return stmt.getInt64(0);
    });
}

Result<int64_t> MetadataRepository::getContentExtractedDocumentCount() {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        using yams::metadata::sql::QuerySpec;
        QuerySpec spec{};
        spec.table = "documents";
        spec.columns = {"COUNT(*)"};
        spec.conditions = {"content_extracted = 1"};
        auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));
        if (!stmtResult)
            return stmtResult.error();
        Statement stmt = std::move(stmtResult).value();
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        return stmt.getInt64(0);
    });
}

Result<int64_t> MetadataRepository::getDocumentCountByExtractionStatus(ExtractionStatus status) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        using yams::metadata::sql::QuerySpec;
        QuerySpec spec{};
        spec.table = "documents";
        spec.columns = {"COUNT(*)"};
        spec.conditions = {"extraction_status = ?"};
        auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));
        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, ExtractionStatusUtils::toString(status));
        if (!bindResult)
            return bindResult.error();

        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();

        return stmt.getInt64(0);
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
        if (auto indexedResult = getIndexedDocumentCount(); indexedResult) {
            cachedIndexedCount_.store(static_cast<uint64_t>(indexedResult.value()),
                                      std::memory_order_release);
        }
        if (auto extractedResult = getContentExtractedDocumentCount(); extractedResult) {
            cachedExtractedCount_.store(static_cast<uint64_t>(extractedResult.value()),
                                        std::memory_order_release);
        }
        spdlog::info(
            "MetadataRepository: initialized counters - total={}, indexed={}, extracted={}",
            cachedDocumentCount_.load(), cachedIndexedCount_.load(), cachedExtractedCount_.load());
    } catch (const std::exception& e) {
        spdlog::warn("MetadataRepository: failed to initialize counters: {}", e.what());
    }
}

Result<std::unordered_map<std::string, DocumentInfo>>
MetadataRepository::batchGetDocumentsByHash(const std::vector<std::string>& hashes) {
    if (hashes.empty()) {
        return std::unordered_map<std::string, DocumentInfo>{};
    }

    return executeQuery<std::unordered_map<std::string, DocumentInfo>>(
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

Result<std::unordered_map<int64_t, DocumentContent>>
MetadataRepository::batchGetContent(const std::vector<int64_t>& documentIds) {
    if (documentIds.empty()) {
        return std::unordered_map<int64_t, DocumentContent>{};
    }

    return executeQuery<std::unordered_map<int64_t, DocumentContent>>(
        [&](Database& db) -> Result<std::unordered_map<int64_t, DocumentContent>> {
            std::string sql =
                "SELECT document_id, content_text, content_length, extraction_method, language "
                "FROM document_content WHERE document_id IN (";
            for (size_t i = 0; i < documentIds.size(); ++i) {
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

            // Bind document IDs
            for (size_t i = 0; i < documentIds.size(); ++i) {
                if (auto bindResult = stmt.bind(static_cast<int>(i + 1), documentIds[i]);
                    !bindResult) {
                    return bindResult.error();
                }
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

Result<std::unordered_map<std::string, int64_t>>
MetadataRepository::getDocumentCountsByExtension() {
    return executeQuery<std::unordered_map<std::string, int64_t>>(
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
    // Check cache first (reader lock)
    {
        std::shared_lock<std::shared_mutex> readLock(corpusStatsMutex_);
        if (cachedCorpusStats_) {
            auto now = std::chrono::steady_clock::now();
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - corpusStatsCachedAt_);

            // Check if cache is still valid:
            // 1. Within TTL
            // 2. Document count hasn't changed significantly
            auto currentDocCount = cachedDocumentCount_.load(std::memory_order_relaxed);
            bool docCountUnchanged = (currentDocCount == corpusStatsDocCount_);

            if (age < kCorpusStatsTtl && docCountUnchanged) {
                return *cachedCorpusStats_;
            }
        }
    }

    // Cache miss or stale - compute fresh stats
    auto result =
        executeQuery<storage::CorpusStats>([&](Database& db) -> Result<storage::CorpusStats> {
            storage::CorpusStats stats;

            // 1. Basic document metrics: count, total size, avg size, path depth
            {
                auto stmtResult = db.prepare(R"(
                    SELECT 
                        COUNT(*) as doc_count,
                        COALESCE(SUM(file_size), 0) as total_size,
                        COALESCE(AVG(file_size), 0) as avg_size,
                        COALESCE(AVG(path_depth), 0) as avg_depth,
                        COALESCE(MAX(path_depth), 0) as max_depth
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
                }
            }

            // 6. Set timestamp
            stats.computedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();

            return stats;
        });

    // Cache the result if successful
    if (result.has_value()) {
        std::unique_lock<std::shared_mutex> writeLock(corpusStatsMutex_);
        cachedCorpusStats_ = std::make_unique<storage::CorpusStats>(result.value());
        corpusStatsCachedAt_ = std::chrono::steady_clock::now();
        corpusStatsDocCount_ = cachedDocumentCount_.load(std::memory_order_relaxed);
    }

    return result;
}

Result<void> MetadataRepository::ensureFuzzyIndexInitialized() {
    {
        std::shared_lock<std::shared_mutex> readLock(fuzzyIndexMutex_);
        if (fuzzySearchIndex_) {
            return Result<void>();
        }
    }
    return buildFuzzyIndex();
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

        // Update component-owned metrics (using saturating subtraction to prevent underflow)
        if (!hadEmbedding && hasEmbedding) {
            cachedIndexedCount_.fetch_add(1, std::memory_order_relaxed);
        } else if (hadEmbedding && !hasEmbedding) {
            core::saturating_sub(cachedIndexedCount_, uint64_t{1});
        }

        return Result<void>();
    });
}

Result<void> MetadataRepository::updateDocumentEmbeddingStatusByHash(const std::string& hash,
                                                                     bool hasEmbedding,
                                                                     const std::string& modelId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // First, get the document ID from the hash
        auto getIdStmt = db.prepare("SELECT id FROM documents WHERE sha256_hash = ?");
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

        return Result<void>();
    });
}

Result<void> MetadataRepository::updateDocumentExtractionStatus(int64_t documentId,
                                                                bool contentExtracted,
                                                                ExtractionStatus status,
                                                                const std::string& error) {
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
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

        return stmt.execute();
    });

    if (result)
        invalidateQueryCache();
    return result;
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

Result<void> MetadataRepository::checkpointWal() {
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

Result<std::optional<PathTreeNode>>
MetadataRepository::findPathTreeNode(int64_t parentId, std::string_view pathSegment) {
    return executeQuery<std::optional<PathTreeNode>>(
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
        // Check whether the (node, document) pair already exists.
        auto checkStmtResult = db.prepare(
            "SELECT 1 FROM path_tree_node_documents WHERE node_id = ? AND document_id = ?");
        if (!checkStmtResult)
            return checkStmtResult.error();
        auto checkStmt = std::move(checkStmtResult).value();
        if (auto bindNode = checkStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();
        if (auto bindDoc = checkStmt.bind(2, documentId); !bindDoc)
            return bindDoc.error();

        auto stepResult = checkStmt.step();
        if (!stepResult)
            return stepResult.error();
        if (stepResult.value()) {
            // Already associated; nothing to update.
            return Result<void>();
        }

        // Insert relationship and increment doc count.
        auto insertStmtResult =
            db.prepare("INSERT INTO path_tree_node_documents (node_id, document_id) VALUES (?, ?)");
        if (!insertStmtResult)
            return insertStmtResult.error();
        auto insertStmt = std::move(insertStmtResult).value();
        if (auto bindNode = insertStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();
        if (auto bindDoc = insertStmt.bind(2, documentId); !bindDoc)
            return bindDoc.error();
        if (auto execResult = insertStmt.execute(); !execResult)
            return execResult.error();

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

    return executeQuery<std::optional<PathTreeNode>>(
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

Result<std::vector<PathTreeNode>>
MetadataRepository::listPathTreeChildren(std::string_view fullPath, std::size_t limit) {
    return executeQuery<std::vector<PathTreeNode>>(
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

                // Recalculate centroid: subtract the removed embedding
                int64_t newWeight = currentWeight - 1;
                if (centroid.size() == embeddingValues.size()) {
                    const double oldWeightFactor = static_cast<double>(currentWeight);
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
        std::string directoryPath =
            record.metadata.count("directory_path") ? record.metadata.at("directory_path") : "";
        std::string snapshotLabel =
            record.metadata.count("snapshot_label") ? record.metadata.at("snapshot_label") : "";
        std::string gitCommit =
            record.metadata.count("git_commit") ? record.metadata.at("git_commit") : "";
        std::string gitBranch =
            record.metadata.count("git_branch") ? record.metadata.at("git_branch") : "";
        std::string gitRemote =
            record.metadata.count("git_remote") ? record.metadata.at("git_remote") : "";

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
    return executeQuery<std::vector<TreeSnapshotRecord>>(
        [limit](Database& db) -> Result<std::vector<TreeSnapshotRecord>> {
            const char* sql = R"(
            SELECT snapshot_id, directory_path, snapshot_label, 
                   git_commit, git_branch, git_remote, files_count, created_at
            FROM tree_snapshots
            ORDER BY created_at DESC
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
                old_hash, new_hash, mode, is_directory
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
            } else {
                stmt.bind(7, nullptr);
            }

            stmt.bind(8, change.isDirectory ? 1 : 0);

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
    return executeQuery<std::vector<TreeChangeRecord>>(
        [&](Database& db) -> Result<std::vector<TreeChangeRecord>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.from = std::optional<std::string>{
                "tree_changes tc JOIN tree_diffs td ON tc.diff_id = td.diff_id"};
            spec.table = "tree_changes"; // not used when from is set
            spec.columns = {"change_type", "old_path", "new_path",    "old_hash",
                            "new_hash",    "mode",     "is_directory"};
            spec.conditions = {"td.base_snapshot_id = ?", "td.target_snapshot_id = ?"};
            if (query.pathPrefix.has_value()) {
                spec.conditions.emplace_back("(old_path LIKE ? OR new_path LIKE ?)");
            }
            if (query.typeFilter.has_value()) {
                spec.conditions.emplace_back("change_type = ?");
            }
            spec.orderBy = std::optional<std::string>{"tc.change_id"};
            spec.limit = static_cast<int>(query.limit);
            spec.offset = static_cast<int>(query.offset);

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
                stmt.bind(paramIdx++, changeTypeToString(*query.typeFilter));
            }

            stmt.bind(paramIdx++, static_cast<int64_t>(query.limit));
            stmt.bind(paramIdx++, static_cast<int64_t>(query.offset));

            std::vector<TreeChangeRecord> results;

            while (stmt.step()) {
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
        auto stmtResult = db.prepare(R"(
            UPDATE tree_diffs 
            SET files_added = (
                    SELECT COUNT(*) FROM tree_changes 
                    WHERE diff_id = ? AND change_type = 'added' AND is_directory = 0
                ),
                files_deleted = (
                    SELECT COUNT(*) FROM tree_changes 
                    WHERE diff_id = ? AND change_type = 'deleted' AND is_directory = 0
                ),
                files_modified = (
                    SELECT COUNT(*) FROM tree_changes 
                    WHERE diff_id = ? AND change_type = 'modified' AND is_directory = 0
                ),
                files_renamed = (
                    SELECT COUNT(*) FROM tree_changes 
                    WHERE diff_id = ? AND change_type IN ('renamed', 'moved') AND is_directory = 0
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
DocumentInfo MetadataRepository::mapDocumentRow(Statement& stmt) {
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

DocumentContent MetadataRepository::mapContentRow(Statement& stmt) {
    DocumentContent content;
    content.documentId = stmt.getInt64(0);
    content.contentText = stmt.getString(1);
    content.contentLength = stmt.getInt64(2);
    content.extractionMethod = stmt.getString(3);
    content.language = stmt.getString(4);
    return content;
}

DocumentRelationship MetadataRepository::mapRelationshipRow(Statement& stmt) {
    DocumentRelationship rel;
    rel.id = stmt.getInt64(0);
    if (!stmt.isNull(1)) {
        rel.parentId = stmt.getInt64(1);
    }
    rel.childId = stmt.getInt64(2);
    rel.setRelationshipTypeFromString(stmt.getString(3));
    rel.createdTime = stmt.getTime(4);
    return rel;
}

SearchHistoryEntry MetadataRepository::mapSearchHistoryRow(Statement& stmt) {
    SearchHistoryEntry entry;
    entry.id = stmt.getInt64(0);
    entry.query = stmt.getString(1);
    entry.queryTime = stmt.getTime(2);
    entry.resultsCount = stmt.getInt64(3);
    entry.executionTimeMs = stmt.getInt64(4);
    entry.userContext = stmt.getString(5);
    return entry;
}

SavedQuery MetadataRepository::mapSavedQueryRow(Statement& stmt) {
    SavedQuery query;
    query.id = stmt.getInt64(0);
    query.name = stmt.getString(1);
    query.query = stmt.getString(2);
    query.description = stmt.getString(3);
    query.createdTime = stmt.getTime(4);
    if (!stmt.isNull(5)) {
        query.lastUsed = stmt.getTime(5);
    }
    query.useCount = stmt.getInt64(6);
    return query;
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

std::vector<std::string> MetadataQueryBuilder::getParameters() const {
    return parameters_;
}

// MetadataTransaction implementation
MetadataTransaction::MetadataTransaction(MetadataRepository& repo) : repo_(repo) {}

MetadataTransaction::~MetadataTransaction() = default;

// Fuzzy search implementation
Result<SearchResults>
MetadataRepository::fuzzySearch(const std::string& query, float minSimilarity, int limit,
                                const std::optional<std::vector<int64_t>>& docIds) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::fuzzySearch");
    return executeQuery<SearchResults>([&]([[maybe_unused]] Database& db) -> Result<SearchResults> {
        SearchResults results;
        results.query = query;

        spdlog::info("[FUZZY_PERF] fuzzySearch starting for query='{}' limit={}", query, limit);
        auto totalStart = std::chrono::high_resolution_clock::now();

        auto ensureStart = std::chrono::high_resolution_clock::now();
        auto ensureResult = ensureFuzzyIndexInitialized();
        auto ensureEnd = std::chrono::high_resolution_clock::now();
        auto ensureMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(ensureEnd - ensureStart).count();
        spdlog::info("[FUZZY_PERF] ensureFuzzyIndexInitialized took {}ms", ensureMs);

        if (!ensureResult) {
            results.errorMessage = "Failed to build fuzzy index: " + ensureResult.error().message;
            return results;
        }

        std::vector<search::HybridFuzzySearch::SearchResult> fuzzyResults;
        {
            std::shared_lock<std::shared_mutex> readLock(fuzzyIndexMutex_);
            auto* indexPtr = fuzzySearchIndex_.get();
            if (!indexPtr) {
                results.errorMessage = "Fuzzy index unavailable";
                return results;
            }

            search::HybridFuzzySearch::SearchOptions options;
            options.minSimilarity = minSimilarity;
            options.maxEditDistance = 3;
            options.useTrigramPrefilter = true;
            options.useBKTree = true;

            auto searchStart = std::chrono::high_resolution_clock::now();
            fuzzyResults = indexPtr->search(query, static_cast<size_t>(limit), options);
            auto searchEnd = std::chrono::high_resolution_clock::now();
            auto searchMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(searchEnd - searchStart)
                    .count();
            spdlog::info("[FUZZY_PERF] HybridFuzzySearch::search took {}ms, returned {} results",
                         searchMs, fuzzyResults.size());
        }

        auto start = std::chrono::high_resolution_clock::now();

        auto parseDocId = [](const std::string& token) -> std::optional<int64_t> {
            if (token.empty()) {
                return std::nullopt;
            }
            std::size_t len = 0;
            while (len < token.size() && std::isdigit(static_cast<unsigned char>(token[len]))) {
                ++len;
            }
            if (len == 0) {
                return std::nullopt;
            }
            try {
                return std::stoll(token.substr(0, len));
            } catch (...) {
                return std::nullopt;
            }
        };

        std::unordered_set<int64_t> seenDocIds;
        const size_t effectiveLimit = limit > 0 ? static_cast<size_t>(limit) : SIZE_MAX;

        for (const auto& fuzzyResult : fuzzyResults) {
            // Early termination once we have enough results
            if (results.results.size() >= effectiveLimit) {
                break;
            }

            auto docIdOpt = parseDocId(fuzzyResult.id);
            if (!docIdOpt.has_value()) {
                continue;
            }
            int64_t docId = *docIdOpt;

            if (!seenDocIds.insert(docId).second) {
                continue;
            }

            if (docIds && std::find(docIds->begin(), docIds->end(), docId) == docIds->end()) {
                continue;
            }

            auto docResult = getDocument(docId);
            if (!docResult || !docResult.value().has_value()) {
                continue;
            }

            SearchResult result;
            result.document = docResult.value().value();
            // Snippet will be populated by the service layer if needed
            // Store match metadata in matchedTerms for debugging if needed
            result.matchedTerms.push_back(fuzzyResult.matchType);
            result.score = static_cast<double>(fuzzyResult.score);

            results.results.push_back(result);
        }

        results.totalCount = static_cast<int64_t>(results.results.size());

        auto end = std::chrono::high_resolution_clock::now();
        results.executionTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        return results;
    });
}

Result<void> MetadataRepository::buildFuzzyIndex() {
    YAMS_ZONE_SCOPED_N("MetadataRepo::buildFuzzyIndex");
    return executeQuery<void>([&](Database& db) -> Result<void> {
        std::unique_lock<std::shared_mutex> lock(fuzzyIndexMutex_);

        // Create new fuzzy search index
        fuzzySearchIndex_ = std::make_unique<search::HybridFuzzySearch>();

        // Smart filtering: use metadata and KG to prioritize relevant documents
        // This keeps memory bounded while indexing the most important documents
        auto stmtResult = db.prepare(R"(
            WITH ranked_docs AS (
                SELECT 
                    d.id,
                    d.file_name,
                    d.file_path,
                    d.indexed_time,
                    -- Prioritize documents with rich metadata
                    COALESCE((SELECT COUNT(*) FROM metadata m WHERE m.document_id = d.id AND m.key = 'tag'), 0) as tag_count,
                    -- Prioritize documents in knowledge graph (have symbols/entities)
                    COALESCE((SELECT COUNT(DISTINCT kde.id) 
                              FROM kg_doc_entities kde 
                              WHERE kde.document_id = d.id), 0) as entity_count,
                    -- Recency score
                    CASE 
                        WHEN d.indexed_time > datetime('now', '-7 days') THEN 4
                        WHEN d.indexed_time > datetime('now', '-30 days') THEN 3
                        WHEN d.indexed_time > datetime('now', '-90 days') THEN 2
                        WHEN d.indexed_time > datetime('now', '-180 days') THEN 1
                        ELSE 0
                    END as recency_score,
                    -- Boost code files (likely to have symbols)
                    CASE 
                        WHEN d.mime_type LIKE 'text/x-%' OR 
                             d.mime_type IN ('application/javascript', 'application/typescript') OR
                             d.file_name LIKE '%.py' OR d.file_name LIKE '%.cpp' OR 
                             d.file_name LIKE '%.h' OR d.file_name LIKE '%.hpp' OR
                             d.file_name LIKE '%.rs' OR d.file_name LIKE '%.go' THEN 2
                        ELSE 0
                    END as code_boost
                FROM documents d
                WHERE d.indexed_time IS NOT NULL
            )
            SELECT id, file_name, file_path
            FROM ranked_docs
            -- Prioritize: tagged > KG-connected > recent > code files > rest
            ORDER BY (tag_count * 3 + entity_count * 2 + recency_score + code_boost) DESC, 
                     indexed_time DESC
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();

        constexpr size_t kSafetyBufferEntries = 2048;
        size_t docsAdded = 0;
        size_t guardLimit = std::numeric_limits<size_t>::max();
        std::optional<size_t> configuredLimit;

        if (const char* envLimit = std::getenv("YAMS_FUZZY_INDEX_LIMIT"); envLimit && *envLimit) {
            try {
                auto parsed = static_cast<size_t>(std::stoull(envLimit));
                if (parsed > 0) {
                    configuredLimit = parsed;
                    if (parsed > std::numeric_limits<size_t>::max() - kSafetyBufferEntries) {
                        guardLimit = std::numeric_limits<size_t>::max();
                    } else {
                        guardLimit = parsed + kSafetyBufferEntries;
                    }
                }
            } catch (...) {
                spdlog::warn("Invalid YAMS_FUZZY_INDEX_LIMIT '{}', ignoring", envLimit);
            }
        }

        auto guardTriggered = [&](size_t count) {
            return guardLimit != std::numeric_limits<size_t>::max() && count >= guardLimit;
        };

        bool guardHit = false;
        auto stopIfGuarded = [&](const char* phase) {
            if (guardHit || !guardTriggered(docsAdded)) {
                return guardHit;
            }
            guardHit = true;
            if (configuredLimit) {
                spdlog::warn("Fuzzy index stopped after {} entries while {} (configured limit {} + "
                             "{} entry safety buffer)",
                             docsAdded, phase, configuredLimit.value(), kSafetyBufferEntries);
            } else {
                spdlog::warn("Fuzzy index stopped after {} entries while {} to protect memory",
                             docsAdded, phase);
            }
            return true;
        };

        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult)
                return stepResult.error();
            if (!stepResult.value())
                break;

            if (stopIfGuarded("scanning ranked documents")) {
                break;
            }

            int64_t id = stmt.getInt64(0);
            std::string fileName = stmt.getString(1);
            std::string filePath = stmt.getString(2);

            // Extract keywords from file path
            std::vector<std::string> keywords;

            // Split path into components as keywords
            size_t pos = 0;
            std::string path = filePath;
            while ((pos = path.find('/')) != std::string::npos) {
                std::string component = path.substr(0, pos);
                if (!component.empty() && component != "." && component != "..") {
                    keywords.push_back(component);
                }
                path.erase(0, pos + 1);
            }
            if (!path.empty()) {
                keywords.push_back(path);
            }

            // Add document to fuzzy index with both filename and content
            // First add with filename as title
            try {
                fuzzySearchIndex_->addDocument(std::to_string(id), fileName, keywords);
                docsAdded++;
                if (stopIfGuarded("indexing document metadata")) {
                    break;
                }
            } catch (const std::bad_alloc& e) {
                spdlog::error("Memory exhausted building fuzzy index at {} documents, stopping",
                              docsAdded);
                guardHit = true;
                break;
            }
            // std::cerr << "[DEBUG] Added doc " << id << " with title '" << fileName << "' and " <<
            // keywords.size() << " keywords" << std::endl;

            // Also get and index document content for fuzzy search
            auto contentResult = getContent(id);
            if (contentResult && contentResult.value().has_value()) {
                auto content = contentResult.value().value();

                // Extract more comprehensive keywords from content
                std::vector<std::string> contentKeywords;

                // Process larger portion of content (up to 5000 chars instead of 500)
                size_t maxContentLength = std::min(size_t(5000), content.contentText.length());
                std::string contentToIndex = content.contentText.substr(0, maxContentLength);

                // Convert to lowercase for better matching
                std::transform(contentToIndex.begin(), contentToIndex.end(), contentToIndex.begin(),
                               ::tolower);

                // Extract words and multi-word phrases
                std::istringstream iss(contentToIndex);
                std::string word;
                std::string previousWord;

                while (iss >> word) {
                    // Clean up word - remove common punctuation
                    word.erase(std::remove_if(
                                   word.begin(), word.end(),
                                   [](char c) { return !std::isalnum(c) && c != '-' && c != '_'; }),
                               word.end());

                    if (!word.empty() && word.length() > 2) { // Skip very short words
                        contentKeywords.push_back(word);

                        // Also add two-word phrases for better phrase matching
                        if (!previousWord.empty()) {
                            std::string phrase = previousWord + " " + word;
                            contentKeywords.push_back(phrase);
                        }

                        previousWord = word;
                    }

                    // Limit total keywords to prevent memory issues
                    if (contentKeywords.size() >= 100) {
                        break;
                    }
                }

                // Add content-based entry with more content preview
                std::string contentPreview = content.contentText.substr(
                    0, std::min(size_t(200), content.contentText.length()));
                try {
                    fuzzySearchIndex_->addDocument(std::to_string(id) + "_content", contentPreview,
                                                   contentKeywords);
                    docsAdded++;
                    if (stopIfGuarded("indexing document content")) {
                        break;
                    }
                } catch (const std::bad_alloc& e) {
                    spdlog::warn("Memory limit reached adding content entries, skipping remaining");
                    guardHit = true;
                    break;
                }
            }

            // Check if we've hit the limit
            if (stopIfGuarded("indexing ranked documents")) {
                break;
            }
        }

        if (!guardHit) {
            // Also query metadata tags
            auto tagStmtResult = db.prepare(R"(
                SELECT document_id, value
                FROM metadata
                WHERE key = 'tag'
            )");

            if (tagStmtResult) {
                Statement tagStmt = std::move(tagStmtResult).value();

                std::unordered_map<int64_t, std::vector<std::string>> docTags;

                while (true) {
                    auto stepResult = tagStmt.step();
                    if (!stepResult || !stepResult.value())
                        break;

                    int64_t docId = tagStmt.getInt64(0);
                    std::string tag = tagStmt.getString(1);
                    docTags[docId].push_back(tag);
                }

                // Update documents with tags (respecting limit)
                for (const auto& [docId, tags] : docTags) {
                    if (stopIfGuarded("enriching metadata tags")) {
                        break;
                    }

                    auto docResult = getDocument(docId);
                    if (docResult && docResult.value().has_value()) {
                        auto doc = docResult.value().value();
                        try {
                            fuzzySearchIndex_->addDocument(std::to_string(docId), doc.fileName,
                                                           tags);
                            docsAdded++;
                            if (stopIfGuarded("enriching metadata tags")) {
                                break;
                            }
                        } catch (const std::bad_alloc& e) {
                            spdlog::error("Memory exhausted adding tagged documents at {} docs",
                                          docsAdded);
                            guardHit = true;
                            break;
                        }
                    }
                }
            }
        } else {
            spdlog::debug("Skipping tag enrichment after fuzzy index guard triggered");
        }

        const size_t totalDocs = fuzzySearchIndex_->getStats().documentCount;
        if (configuredLimit) {
            if (guardTriggered(totalDocs)) {
                spdlog::info("Built fuzzy index with {} entries (reached configured limit {} plus "
                             "{} entry safety buffer)",
                             totalDocs, configuredLimit.value(), kSafetyBufferEntries);
            } else {
                spdlog::info("Built fuzzy index with {} entries (configured limit {})", totalDocs,
                             configuredLimit.value());
            }
        } else {
            spdlog::info("Built fuzzy index with {} entries", totalDocs);
        }

        return Result<void>();
    });
}

Result<void> MetadataRepository::updateFuzzyIndex(int64_t documentId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        std::unique_lock<std::shared_mutex> lock(fuzzyIndexMutex_);

        if (!fuzzySearchIndex_) {
            // Index not built yet, will be built on first search
            return Result<void>();
        }

        // Get document info using internal helper to avoid nested connection acquisition deadlock
        // Previously this called getDocument() which would try to acquire another connection
        // from the pool while we already hold one, causing "resource deadlock would occur"
        auto docResult = getDocumentInternal(db, documentId);
        if (!docResult || !docResult.value().has_value()) {
            return Error{ErrorCode::NotFound, "Document not found"};
        }

        auto doc = docResult.value().value();

        // Get document tags using internal helper to avoid nested connection acquisition
        std::vector<std::string> keywords;
        auto metadataResult = getAllMetadataInternal(db, documentId);
        if (metadataResult) {
            for (const auto& [key, value] : metadataResult.value()) {
                if (key == "tag" && value.type == MetadataValueType::String) {
                    keywords.push_back(value.value);
                }
            }
        }

        // Update fuzzy index
        fuzzySearchIndex_->addDocument(std::to_string(documentId), doc.fileName, keywords);

        return Result<void>();
    });
}

// Collection and snapshot operations
Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByCollection(const std::string& collection) {
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'collection' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            auto bindResult = stmt.bind(1, collection);
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
MetadataRepository::findDocumentsBySnapshot(const std::string& snapshotId) {
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'snapshot_id' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");

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
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'snapshot_label' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");

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

Result<std::vector<std::string>> MetadataRepository::getCollections() {
    return executeQuery<std::vector<std::string>>(
        [&](Database& db) -> Result<std::vector<std::string>> {
            using yams::metadata::sql::QuerySpec;
            QuerySpec spec{};
            spec.table = "metadata";
            spec.columns = {"DISTINCT value"};
            spec.conditions = {"key = 'collection'"};
            spec.orderBy = std::optional<std::string>{"value"};

            auto stmtResult = db.prepare(yams::metadata::sql::buildSelect(spec));

            if (!stmtResult)
                return stmtResult.error();

            Statement stmt = std::move(stmtResult).value();
            std::vector<std::string> collections;

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult)
                    return stepResult.error();
                if (!stepResult.value())
                    break;

                collections.push_back(stmt.getString(0));
            }

            return collections;
        });
}

Result<std::vector<std::string>> MetadataRepository::getSnapshots() {
    return executeQuery<std::vector<std::string>>(
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
}

Result<std::vector<std::string>> MetadataRepository::getSnapshotLabels() {
    return executeQuery<std::vector<std::string>>(
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
}

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsBySessionId(const std::string& sessionId) {
    return executeQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            auto stmtResult = db.prepare(R"(
            SELECT DISTINCT d.id, d.file_path, d.file_name, d.file_extension, d.file_size,
                   d.sha256_hash, d.mime_type, d.created_time, d.modified_time,
                   d.indexed_time, d.content_extracted, d.extraction_status,
                   d.extraction_error
            FROM documents d
            JOIN metadata m ON d.id = m.document_id
            WHERE m.key = 'session_id' AND m.value = ?
            ORDER BY d.indexed_time DESC
        )");

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
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
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
        auto stmtResult = db.prepare(R"(
            DELETE FROM metadata
            WHERE key = 'session_id' AND value = ?
        )");

        if (!stmtResult)
            return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, sessionId);
        if (!bindResult)
            return bindResult.error();

        return stmt.execute();
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
    if (tags.empty()) {
        return std::vector<DocumentInfo>();
    }

    return executeQuery<std::vector<DocumentInfo>>(
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

            // Build IN list
            auto buildInList = [](size_t count) {
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
            };
            std::string inKeys = buildInList(tagKeys.size());
            std::string inTags = buildInList(uniqueTags.size());

            std::string sql;
            if (matchAll) {
                // Match-all across both "tag:<name>" keys and legacy key="tag" values.
                // Normalize to tag names via CASE so count reflects actual tags.
                sql = "SELECT d.* FROM documents d WHERE d.id IN ("
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
                sql = "SELECT DISTINCT d.* FROM documents d "
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
                auto b = stmt.bind(paramIndex++, static_cast<int64_t>(uniqueTags.size()));
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
}

Result<std::vector<std::string>> MetadataRepository::getDocumentTags(int64_t documentId) {
    return executeQuery<std::vector<std::string>>(
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

    return executeQuery<std::unordered_map<int64_t, std::vector<std::string>>>(
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

                int64_t docId = stmt.getInt64(0);
                std::string fullKey = stmt.getString(1);
                if (fullKey.starts_with("tag:")) {
                    out[docId].push_back(fullKey.substr(4));
                }
            }

            return out;
        });
}

Result<std::vector<std::string>> MetadataRepository::getAllTags() {
    return executeQuery<std::vector<std::string>>(
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
}

} // namespace yams::metadata
