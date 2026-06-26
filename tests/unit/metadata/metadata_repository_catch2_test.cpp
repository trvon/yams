// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <future>
#include <optional>
#include <thread>
#include <variant>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <boost/asio/io_context.hpp>
#include <yams/common/utf8_utils.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/connection_pool.h>

#include "../../common/metadata_test_db.h"
#include <yams/metadata/content_index_writer.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_insert_writer.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>
#include <yams/storage/corpus_stats.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

// Catch2's expression-decomposition macros intentionally overload operator<=,
// which triggers false positives from chained-comparison linters on ordinary
// CHECK/REQUIRE assertions in this file.
// NOLINTBEGIN(bugprone-chained-comparison)
namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    static const bool kSuppressInfoLogs = [] {
        spdlog::set_level(spdlog::level::warn);
        return true;
    }();
    (void)kSuppressInfoLogs;
    return yams::test::migrated_metadata_db_template().clone(prefix);
}

DocumentInfo makeDocumentWithPath(const std::string& path, const std::string& hash,
                                  const std::string& mime = "text/plain") {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 1234;
    info.sha256Hash = hash;
    info.mimeType = mime;
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    populatePathDerivedFields(info);
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    return info;
}

BatchContentEntry makeBatchContentEntry(int64_t documentId, const std::string& title,
                                        const std::string& contentText,
                                        const std::string& mimeType = "text/plain",
                                        const std::string& extractionMethod = "test",
                                        const std::string& language = "en") {
    BatchContentEntry entry;
    entry.documentId = documentId;
    entry.title = title;
    entry.contentText = contentText;
    entry.mimeType = mimeType;
    entry.extractionMethod = extractionMethod;
    entry.language = language;
    return entry;
}

std::unique_ptr<yams::daemon::WriteBatch> makeSymSpellBatch(const std::string& source,
                                                            const std::string& indexedTerm) {
    auto batch = std::make_unique<yams::daemon::WriteBatch>();
    batch->source = source;
    batch->ops.emplace_back(yams::daemon::AddSymSpellTermsOp{{indexedTerm}});
    return batch;
}

BatchDocumentInsert makeIndexedBatchInsert(int index) {
    BatchDocumentInsert item;
    item.info = makeDocumentWithPath("repo/writer/doc-" + std::to_string(index) + ".txt",
                                     "writer-hash-" + std::to_string(index));
    item.tags.emplace_back("idx", MetadataValue(std::to_string(index)));
    return item;
}

TEST_CASE("populatePathDerivedFields fills DocumentInfo path index fields", "[metadata][path]") {
    DocumentInfo info;
    info.filePath = "/tmp/../tmp/yams/path-note.md";

    const auto expected = computePathDerivedValues(info.filePath);
    populatePathDerivedFields(info);

    CHECK((info.filePath == expected.normalizedPath));
    CHECK((info.pathPrefix == expected.pathPrefix));
    CHECK((info.reversePath == expected.reversePath));
    CHECK((info.pathHash == expected.pathHash));
    CHECK((info.parentHash == expected.parentHash));
    CHECK((info.pathDepth == expected.pathDepth));
}

struct MetadataRepositoryFixture {
    MetadataRepositoryFixture() {
        dbPath_ = tempDbPath("metadata_repo_catch2_test_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE((initResult.has_value()));

        repository_ = std::make_unique<MetadataRepository>(
            *pool_, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~MetadataRepositoryFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        yams::test::remove_sqlite_artifacts(dbPath_);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

struct EmbeddingStatusRow {
    bool hasEmbedding = false;
    std::optional<std::string> modelId;
    int64_t chunkCount = 0;
};

Result<std::optional<EmbeddingStatusRow>> getEmbeddingStatusRow(ConnectionPool& pool,
                                                                const std::string& hash) {
    auto connResult = pool.acquire();
    if (!connResult) {
        return connResult.error();
    }

    auto& db = **connResult.value();
    auto stmtResult = db.prepare(R"(
        SELECT COALESCE(des.has_embedding, 0), des.model_id, COALESCE(des.chunk_count, 0)
        FROM documents d
        LEFT JOIN document_embeddings_status des ON d.id = des.document_id
        WHERE d.sha256_hash = ?
    )");
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto& stmt = stmtResult.value();
    if (auto bindResult = stmt.bind(1, hash); !bindResult) {
        return bindResult.error();
    }

    auto stepResult = stmt.step();
    if (!stepResult) {
        return stepResult.error();
    }
    if (!stepResult.value()) {
        return std::optional<EmbeddingStatusRow>{};
    }

    EmbeddingStatusRow row;
    row.hasEmbedding = stmt.getInt(0) != 0;
    if (!stmt.isNull(1)) {
        row.modelId = stmt.getString(1);
    }
    row.chunkCount = stmt.getInt64(2);
    return std::optional<EmbeddingStatusRow>{row};
}

Result<void> setEmbeddingChunkCount(ConnectionPool& pool, const std::string& hash,
                                    int64_t chunkCount) {
    auto connResult = pool.acquire();
    if (!connResult) {
        return connResult.error();
    }

    auto& db = **connResult.value();
    auto stmtResult = db.prepare(R"(
        UPDATE document_embeddings_status
        SET chunk_count = ?
        WHERE document_id = (SELECT id FROM documents WHERE sha256_hash = ?)
    )");
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto& stmt = stmtResult.value();
    if (auto bindCount = stmt.bind(1, chunkCount); !bindCount) {
        return bindCount.error();
    }
    if (auto bindHash = stmt.bind(2, hash); !bindHash) {
        return bindHash.error();
    }
    return stmt.execute();
}

Result<void> overwritePathTreeCentroid(ConnectionPool& pool, std::string_view fullPath,
                                       std::span<const std::byte> blob, int64_t centroidWeight) {
    auto connResult = pool.acquire();
    if (!connResult) {
        return connResult.error();
    }

    auto& db = **connResult.value();
    auto stmtResult = db.prepare(R"(
        UPDATE path_tree_nodes
        SET centroid = ?, centroid_weight = ?
        WHERE full_path = ?
    )");
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto& stmt = stmtResult.value();
    if (auto bindBlob = stmt.bind(1, blob); !bindBlob) {
        return bindBlob.error();
    }
    if (auto bindWeight = stmt.bind(2, centroidWeight); !bindWeight) {
        return bindWeight.error();
    }
    if (auto bindPath = stmt.bind(3, fullPath); !bindPath) {
        return bindPath.error();
    }
    return stmt.execute();
}

} // namespace

TEST_CASE("MetadataRepository: path-derived fields support exact path lookup",
          "[unit][metadata][repository][path]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath("/tmp/../tmp/yams/path-index.md", "path-index-hash");
    REQUIRE((!docInfo.pathHash.empty()));
    REQUIRE((!docInfo.reversePath.empty()));
    REQUIRE((docInfo.pathDepth > 0));

    auto insert = fix.repository_->insertDocument(docInfo);
    REQUIRE((insert.has_value()));

    auto found = fix.repository_->findDocumentByExactPath(docInfo.filePath);
    REQUIRE((found.has_value()));
    REQUIRE((found.value().has_value()));
    CHECK((found.value()->pathHash == docInfo.pathHash));
    CHECK((found.value()->pathPrefix == docInfo.pathPrefix));
    CHECK((found.value()->parentHash == docInfo.parentHash));
    CHECK((found.value()->pathDepth == docInfo.pathDepth));
}

TEST_CASE("MetadataRepository: insert and get document", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash123";
    docInfo.fileName = "test.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto result = fix.repository_->insertDocument(docInfo);
    REQUIRE((result.has_value()));

    auto docId = result.value();
    CHECK((docId > 0));

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE((getResult.has_value()));
    REQUIRE((getResult.value().has_value()));

    auto retrievedDoc = getResult.value().value();
    CHECK((retrievedDoc.id == docId));
    CHECK((retrievedDoc.sha256Hash == docInfo.sha256Hash));
    CHECK((retrievedDoc.fileName == docInfo.fileName));
    CHECK((retrievedDoc.fileSize == docInfo.fileSize));
    CHECK((retrievedDoc.mimeType == docInfo.mimeType));
}

TEST_CASE("MetadataRepository: dual-pool read/write routing behavior",
          "[unit][metadata][repository][dual-pool]") {
    auto dbPath = tempDbPath("metadata_repo_dual_pool_");

    ConnectionPoolConfig writeCfg;
    writeCfg.minConnections = 1;
    writeCfg.maxConnections = 2;

    ConnectionPoolConfig readCfg;
    readCfg.minConnections = 1;
    readCfg.maxConnections = 2;

    auto writePool = std::make_unique<ConnectionPool>(dbPath.string(), writeCfg);
    auto readPool = std::make_unique<ConnectionPool>(dbPath.string(), readCfg);

    REQUIRE((writePool->initialize().has_value()));
    REQUIRE((readPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *writePool, readPool.get(), MetadataRepository::SchemaBootstrapMode::AssumeReady);

    auto doc = makeDocumentWithPath("/tmp/dual-pool.txt", "dual-pool-hash");
    auto insertResult = repository->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    auto readOk = repository->getDocumentByHash("dual-pool-hash");
    REQUIRE((readOk.has_value()));
    REQUIRE((readOk.value().has_value()));

    readPool->shutdown();

    auto readAfterShutdown = repository->getDocumentByHash("dual-pool-hash");
    REQUIRE_FALSE(readAfterShutdown.has_value());

    MetadataValue value;
    value.value = "still-writeable";
    value.type = MetadataValueType::String;
    auto writeAfterReadPoolDown = repository->setMetadata(docId, "dual_pool_test", value);
    REQUIRE((writeAfterReadPoolDown.has_value()));

    repository.reset();
    writePool->shutdown();
    writePool.reset();
    readPool.reset();

    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: dual-pool session count read route isolation",
          "[unit][metadata][repository][dual-pool]") {
    auto dbPath = tempDbPath("metadata_repo_dual_pool_session_count_");

    ConnectionPoolConfig writeCfg;
    writeCfg.minConnections = 1;
    writeCfg.maxConnections = 2;

    ConnectionPoolConfig readCfg;
    readCfg.minConnections = 1;
    readCfg.maxConnections = 2;

    auto writePool = std::make_unique<ConnectionPool>(dbPath.string(), writeCfg);
    auto readPool = std::make_unique<ConnectionPool>(dbPath.string(), readCfg);

    REQUIRE((writePool->initialize().has_value()));
    REQUIRE((readPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *writePool, readPool.get(), MetadataRepository::SchemaBootstrapMode::AssumeReady);

    auto doc = makeDocumentWithPath("/tmp/dual-pool-session.txt", "dual-pool-session-hash");
    auto insertResult = repository->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    MetadataValue sessionValue;
    sessionValue.value = "sess-123";
    sessionValue.type = MetadataValueType::String;
    REQUIRE((repository->setMetadata(docId, "session_id", sessionValue).has_value()));

    auto countOk = repository->countDocumentsBySessionId("sess-123");
    REQUIRE((countOk.has_value()));
    REQUIRE((countOk.value() == 1));

    readPool->shutdown();

    auto countAfterShutdown = repository->countDocumentsBySessionId("sess-123");
    REQUIRE_FALSE(countAfterShutdown.has_value());

    MetadataValue writeValue;
    writeValue.value = "still-writeable-after-read-pool-down";
    writeValue.type = MetadataValueType::String;
    REQUIRE((repository->setMetadata(docId, "dual_pool_write_check", writeValue).has_value()));

    repository.reset();
    writePool->shutdown();
    writePool.reset();
    readPool.reset();

    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: snapshot metadata helpers round-trip",
          "[unit][metadata][repository][snapshot-metadata]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("repo/releases/a.txt", "snapshot-meta-a");
    auto docB = makeDocumentWithPath("repo/releases/b.txt", "snapshot-meta-b");
    auto docC = makeDocumentWithPath("repo/archive/c.txt", "snapshot-meta-c");

    auto aId = fix.repository_->insertDocument(docA);
    auto bId = fix.repository_->insertDocument(docB);
    auto cId = fix.repository_->insertDocument(docC);
    REQUIRE((aId.has_value()));
    REQUIRE((bId.has_value()));
    REQUIRE((cId.has_value()));

    auto setStringMeta = [&](int64_t docId, const std::string& key, const std::string& value) {
        MetadataValue mv;
        mv.type = MetadataValueType::String;
        mv.value = value;
        REQUIRE((fix.repository_->setMetadata(docId, key, mv).has_value()));
    };

    setStringMeta(aId.value(), "snapshot_id", "snap-1");
    setStringMeta(aId.value(), "snapshot_label", "release-a");
    setStringMeta(aId.value(), "git_commit", "abc123");
    setStringMeta(bId.value(), "snapshot_id", "snap-1");
    setStringMeta(bId.value(), "snapshot_label", "release-a");
    setStringMeta(bId.value(), "git_commit", "abc123");
    setStringMeta(cId.value(), "snapshot_id", "snap-2");
    setStringMeta(cId.value(), "snapshot_label", "release-b");
    setStringMeta(cId.value(), "git_commit", "def456");

    auto bySnapshot = fix.repository_->findDocumentsBySnapshot("snap-1");
    REQUIRE((bySnapshot.has_value()));
    REQUIRE((bySnapshot.value().size() == 2));
    std::vector<std::string> snapshotPaths;
    for (const auto& doc : bySnapshot.value()) {
        snapshotPaths.push_back(doc.filePath);
    }
    std::sort(snapshotPaths.begin(), snapshotPaths.end());
    auto expectedSnapshotPaths = std::vector<std::string>{docA.filePath, docB.filePath};
    std::sort(expectedSnapshotPaths.begin(), expectedSnapshotPaths.end());
    CHECK((snapshotPaths == expectedSnapshotPaths));

    auto byLabel = fix.repository_->findDocumentsBySnapshotLabel("release-a");
    REQUIRE((byLabel.has_value()));
    REQUIRE((byLabel.value().size() == 2));

    auto snapshots = fix.repository_->getSnapshots();
    REQUIRE((snapshots.has_value()));
    CHECK((snapshots.value() == std::vector<std::string>{"snap-1", "snap-2"}));
    auto snapshotsCached = fix.repository_->getSnapshots();
    REQUIRE((snapshotsCached.has_value()));
    CHECK((snapshotsCached.value() == snapshots.value()));

    auto labels = fix.repository_->getSnapshotLabels();
    REQUIRE((labels.has_value()));
    CHECK((labels.value() == std::vector<std::string>{"release-a", "release-b"}));
    auto labelsCached = fix.repository_->getSnapshotLabels();
    REQUIRE((labelsCached.has_value()));
    CHECK((labelsCached.value() == labels.value()));

    auto snap1Info = fix.repository_->getSnapshotInfo("snap-1");
    REQUIRE((snap1Info.has_value()));
    CHECK((snap1Info.value().fileCount == 2));
    CHECK((snap1Info.value().label == "release-a"));
    CHECK((snap1Info.value().gitCommit == "abc123"));
    CHECK((snap1Info.value().createdTime > 0));

    auto batchInfo = fix.repository_->batchGetSnapshotInfo({"snap-1", "snap-2"});
    REQUIRE((batchInfo.has_value()));
    REQUIRE((batchInfo.value().size() == 2));
    CHECK((batchInfo.value().at("snap-1").fileCount == 2));
    CHECK((batchInfo.value().at("snap-1").label == "release-a"));
    CHECK((batchInfo.value().at("snap-2").fileCount == 1));
    CHECK((batchInfo.value().at("snap-2").gitCommit == "def456"));
}

TEST_CASE("MetadataRepository: session and tag helpers round-trip",
          "[unit][metadata][repository][session-tags]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("repo/sessions/a.txt", "session-tag-a");
    auto docB = makeDocumentWithPath("repo/sessions/b.txt", "session-tag-b");
    auto docC = makeDocumentWithPath("repo/archive/c.txt", "session-tag-c");

    auto aId = fix.repository_->insertDocument(docA);
    auto bId = fix.repository_->insertDocument(docB);
    auto cId = fix.repository_->insertDocument(docC);
    REQUIRE((aId.has_value()));
    REQUIRE((bId.has_value()));
    REQUIRE((cId.has_value()));

    auto setStringMeta = [&](int64_t docId, const std::string& key, const std::string& value) {
        MetadataValue mv;
        mv.type = MetadataValueType::String;
        mv.value = value;
        REQUIRE((fix.repository_->setMetadata(docId, key, mv).has_value()));
    };

    setStringMeta(aId.value(), "session_id", "sess-a");
    setStringMeta(bId.value(), "session_id", "sess-a");
    setStringMeta(cId.value(), "session_id", "sess-b");

    auto sessionDocs = fix.repository_->findDocumentsBySessionId("sess-a");
    REQUIRE((sessionDocs.has_value()));
    REQUIRE((sessionDocs.value().size() == 2));
    std::vector<std::string> sessionPaths;
    for (const auto& doc : sessionDocs.value()) {
        sessionPaths.push_back(doc.filePath);
    }
    std::sort(sessionPaths.begin(), sessionPaths.end());
    auto expectedSessionPaths = std::vector<std::string>{docA.filePath, docB.filePath};
    std::sort(expectedSessionPaths.begin(), expectedSessionPaths.end());
    CHECK((sessionPaths == expectedSessionPaths));

    auto sessionCount = fix.repository_->countDocumentsBySessionId("sess-a");
    REQUIRE((sessionCount.has_value()));
    CHECK((sessionCount.value() == 2));

    REQUIRE((fix.repository_->removeSessionIdFromDocuments("sess-a").has_value()));
    auto sessionCountAfterRemove = fix.repository_->countDocumentsBySessionId("sess-a");
    REQUIRE((sessionCountAfterRemove.has_value()));
    CHECK((sessionCountAfterRemove.value() == 0));
    auto preservedDoc = fix.repository_->getDocument(aId.value());
    REQUIRE((preservedDoc.has_value()));
    REQUIRE((preservedDoc.value().has_value()));

    setStringMeta(aId.value(), "session_id", "sess-a");
    setStringMeta(bId.value(), "session_id", "sess-a");
    auto deletedCount = fix.repository_->deleteDocumentsBySessionId("sess-a");
    REQUIRE((deletedCount.has_value()));
    CHECK((deletedCount.value() == 2));
    auto deletedA = fix.repository_->getDocument(aId.value());
    auto deletedB = fix.repository_->getDocument(bId.value());
    auto survivingC = fix.repository_->getDocument(cId.value());
    REQUIRE((deletedA.has_value()));
    REQUIRE((deletedB.has_value()));
    REQUIRE((survivingC.has_value()));
    CHECK_FALSE(deletedA.value().has_value());
    CHECK_FALSE(deletedB.value().has_value());
    REQUIRE((survivingC.value().has_value()));

    setStringMeta(cId.value(), "tag:alpha", "alpha");
    setStringMeta(cId.value(), "tag:beta", "beta");

    auto docD = makeDocumentWithPath("repo/tags/d.txt", "session-tag-d");
    auto dId = fix.repository_->insertDocument(docD);
    REQUIRE((dId.has_value()));
    setStringMeta(dId.value(), "tag:alpha", "alpha");
    setStringMeta(dId.value(), "tag", "legacy");

    auto docCTags = fix.repository_->getDocumentTags(cId.value());
    REQUIRE((docCTags.has_value()));
    CHECK((docCTags.value() == std::vector<std::string>{"alpha", "beta"}));

    auto batchTags = fix.repository_->batchGetDocumentTags(
        std::vector<int64_t>{cId.value(), dId.value(), 999999});
    REQUIRE((batchTags.has_value()));
    REQUIRE((batchTags.value().contains(cId.value())));
    REQUIRE((batchTags.value().contains(dId.value())));
    CHECK((batchTags.value().at(cId.value()) == std::vector<std::string>{"alpha", "beta"}));
    CHECK((batchTags.value().at(dId.value()) == std::vector<std::string>{"alpha"}));
    CHECK_FALSE(batchTags.value().contains(999999));

    auto allTags = fix.repository_->getAllTags();
    REQUIRE((allTags.has_value()));
    CHECK((allTags.value() == std::vector<std::string>{"alpha", "beta"}));
    auto allTagsCached = fix.repository_->getAllTags();
    REQUIRE((allTagsCached.has_value()));
    CHECK((allTagsCached.value() == allTags.value()));

    setStringMeta(dId.value(), "tag:gamma", "gamma");
    auto allTagsAfterInsert = fix.repository_->getAllTags();
    REQUIRE((allTagsAfterInsert.has_value()));
    CHECK((allTagsAfterInsert.value() == std::vector<std::string>{"alpha", "beta", "gamma"}));

    auto anyAlpha = fix.repository_->findDocumentsByTags({"alpha"}, false);
    REQUIRE((anyAlpha.has_value()));
    REQUIRE((anyAlpha.value().size() == 2));

    auto matchAll = fix.repository_->findDocumentsByTags({"alpha", "beta"}, true);
    REQUIRE((matchAll.has_value()));
    REQUIRE((matchAll.value().size() == 1));
    CHECK((matchAll.value().front().id == cId.value()));

    auto legacyMatch = fix.repository_->findDocumentsByTags({"legacy"}, false);
    REQUIRE((legacyMatch.has_value()));
    REQUIRE((legacyMatch.value().size() == 1));
    CHECK((legacyMatch.value().front().id == dId.value()));

    auto emptyMatch = fix.repository_->findDocumentsByTags({}, true);
    REQUIRE((emptyMatch.has_value()));
    CHECK((emptyMatch.value().empty()));
}

TEST_CASE("MetadataRepository: dual-pool embedding-hash read route isolation",
          "[unit][metadata][repository][dual-pool]") {
    auto dbPath = tempDbPath("metadata_repo_dual_pool_embedding_hash_");

    ConnectionPoolConfig writeCfg;
    writeCfg.minConnections = 1;
    writeCfg.maxConnections = 2;

    ConnectionPoolConfig readCfg;
    readCfg.minConnections = 1;
    readCfg.maxConnections = 2;

    auto writePool = std::make_unique<ConnectionPool>(dbPath.string(), writeCfg);
    auto readPool = std::make_unique<ConnectionPool>(dbPath.string(), readCfg);

    REQUIRE((writePool->initialize().has_value()));
    REQUIRE((readPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *writePool, readPool.get(), MetadataRepository::SchemaBootstrapMode::AssumeReady);

    auto doc = makeDocumentWithPath("/tmp/dual-pool-embed-hash.txt", "dual-pool-embed-hash");
    auto insertResult = repository->insertDocument(doc);
    REQUIRE((insertResult.has_value()));

    auto readOk = repository->hasDocumentEmbeddingByHash("dual-pool-embed-hash");
    REQUIRE((readOk.has_value()));
    REQUIRE_FALSE(readOk.value());

    readPool->shutdown();

    auto readAfterShutdown = repository->hasDocumentEmbeddingByHash("dual-pool-embed-hash");
    REQUIRE_FALSE(readAfterShutdown.has_value());

    repository.reset();
    writePool->shutdown();
    writePool.reset();
    readPool.reset();

    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: reconcile embedding status from vector-backed hashes",
          "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/reconcile_a.txt", "reconcile-hash-a");
    auto docB = makeDocumentWithPath("/tmp/reconcile_b.txt", "reconcile-hash-b");

    auto docAId = fix.repository_->insertDocument(docA);
    auto docBId = fix.repository_->insertDocument(docB);
    REQUIRE((docAId.has_value()));
    REQUIRE((docBId.has_value()));

    REQUIRE(fix.repository_
                ->updateDocumentEmbeddingStatusByHash("reconcile-hash-a", true, "legacy-embed")
                .has_value());
    REQUIRE(fix.repository_
                ->updateDocumentEmbeddingStatusByHash("reconcile-hash-b", true, "legacy-embed")
                .has_value());

    auto beforeA = fix.repository_->hasDocumentEmbeddingByHash("reconcile-hash-a");
    auto beforeB = fix.repository_->hasDocumentEmbeddingByHash("reconcile-hash-b");
    REQUIRE((beforeA.has_value()));
    REQUIRE((beforeB.has_value()));
    CHECK((beforeA.value()));
    CHECK((beforeB.value()));

    REQUIRE(fix.repository_
                ->reconcileDocumentEmbeddingStatusByHashes({"reconcile-hash-a", "missing-hash"})
                .has_value());

    auto afterA = fix.repository_->hasDocumentEmbeddingByHash("reconcile-hash-a");
    auto afterB = fix.repository_->hasDocumentEmbeddingByHash("reconcile-hash-b");
    REQUIRE((afterA.has_value()));
    REQUIRE((afterB.has_value()));
    CHECK((afterA.value()));
    CHECK_FALSE(afterB.value());

    auto stats = fix.repository_->getCorpusStats();
    REQUIRE((stats.has_value()));
    CHECK((stats.value().embeddingCount == 1));
}

TEST_CASE(
    "MetadataRepository: batch embedding status update ignores missing hashes and duplicate inputs",
    "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/batch_embed.txt", "batch-embed-hash");
    REQUIRE((fix.repository_->insertDocument(doc).has_value()));

    REQUIRE(fix.repository_->batchUpdateDocumentEmbeddingStatusByHashes({}, true, "ignored-model")
                .has_value());

    auto warmStats = fix.repository_->getCorpusStats();
    REQUIRE((warmStats.has_value()));
    CHECK((warmStats.value().embeddingCount == 0));

    REQUIRE(
        fix.repository_
            ->batchUpdateDocumentEmbeddingStatusByHashes(
                {"missing-batch-hash", "batch-embed-hash", "batch-embed-hash"}, true, "batch-model")
            .has_value());

    auto hasEmbedding = fix.repository_->hasDocumentEmbeddingByHash("batch-embed-hash");
    REQUIRE((hasEmbedding.has_value()));
    CHECK((hasEmbedding.value()));

    auto missingEmbedding = fix.repository_->hasDocumentEmbeddingByHash("missing-batch-hash");
    REQUIRE((missingEmbedding.has_value()));
    CHECK_FALSE(missingEmbedding.value());

    auto storedStatus = getEmbeddingStatusRow(*fix.pool_, "batch-embed-hash");
    REQUIRE((storedStatus.has_value()));
    REQUIRE((storedStatus.value().has_value()));
    CHECK((storedStatus.value()->hasEmbedding));
    REQUIRE((storedStatus.value()->modelId.has_value()));
    CHECK((storedStatus.value()->modelId.value() == "batch-model"));

    auto afterEnableStats = fix.repository_->getCorpusStats();
    REQUIRE((afterEnableStats.has_value()));
    CHECK((afterEnableStats.value().embeddingCount == 1));

    REQUIRE(fix.repository_
                ->batchUpdateDocumentEmbeddingStatusByHashes(
                    {"batch-embed-hash", "missing-batch-hash", "batch-embed-hash"}, false, "")
                .has_value());

    auto disabledStatus = getEmbeddingStatusRow(*fix.pool_, "batch-embed-hash");
    REQUIRE((disabledStatus.has_value()));
    REQUIRE((disabledStatus.value().has_value()));
    CHECK_FALSE(disabledStatus.value()->hasEmbedding);
    CHECK_FALSE(disabledStatus.value()->modelId.has_value());

    auto afterDisableStats = fix.repository_->getCorpusStats();
    REQUIRE((afterDisableStats.has_value()));
    CHECK((afterDisableStats.value().embeddingCount == 0));
}

TEST_CASE("MetadataRepository: reconcile empty vector-backed hash set clears embedding ownership",
          "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/reconcile_clear_a.txt", "reconcile-clear-hash-a");
    auto docB = makeDocumentWithPath("/tmp/reconcile_clear_b.txt", "reconcile-clear-hash-b");
    REQUIRE((fix.repository_->insertDocument(docA).has_value()));
    REQUIRE((fix.repository_->insertDocument(docB).has_value()));

    REQUIRE(
        fix.repository_
            ->updateDocumentEmbeddingStatusByHash("reconcile-clear-hash-a", true, "legacy-model")
            .has_value());
    REQUIRE(
        fix.repository_
            ->updateDocumentEmbeddingStatusByHash("reconcile-clear-hash-b", true, "legacy-model")
            .has_value());
    REQUIRE((setEmbeddingChunkCount(*fix.pool_, "reconcile-clear-hash-a", 4).has_value()));
    REQUIRE((setEmbeddingChunkCount(*fix.pool_, "reconcile-clear-hash-b", 6).has_value()));

    auto beforeStats = fix.repository_->getCorpusStats();
    REQUIRE((beforeStats.has_value()));
    CHECK((beforeStats.value().embeddingCount == 2));

    REQUIRE((fix.repository_->reconcileDocumentEmbeddingStatusByHashes({}, "").has_value()));

    auto statusA = getEmbeddingStatusRow(*fix.pool_, "reconcile-clear-hash-a");
    auto statusB = getEmbeddingStatusRow(*fix.pool_, "reconcile-clear-hash-b");
    REQUIRE((statusA.has_value()));
    REQUIRE((statusB.has_value()));
    REQUIRE((statusA.value().has_value()));
    REQUIRE((statusB.value().has_value()));
    CHECK_FALSE(statusA.value()->hasEmbedding);
    CHECK_FALSE(statusB.value()->hasEmbedding);
    CHECK_FALSE(statusA.value()->modelId.has_value());
    CHECK_FALSE(statusB.value()->modelId.has_value());
    CHECK((statusA.value()->chunkCount == 0));
    CHECK((statusB.value()->chunkCount == 0));

    auto afterStats = fix.repository_->getCorpusStats();
    REQUIRE((afterStats.has_value()));
    CHECK((afterStats.value().embeddingCount == 0));
}

TEST_CASE("MetadataRepository: reconcile preserves chunk count and omitted model ids",
          "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/reconcile_preserve.txt", "reconcile-preserve-hash");
    REQUIRE((fix.repository_->insertDocument(doc).has_value()));
    REQUIRE(
        fix.repository_
            ->updateDocumentEmbeddingStatusByHash("reconcile-preserve-hash", true, "legacy-model")
            .has_value());
    REQUIRE((setEmbeddingChunkCount(*fix.pool_, "reconcile-preserve-hash", 7).has_value()));

    REQUIRE(
        fix.repository_->reconcileDocumentEmbeddingStatusByHashes({"reconcile-preserve-hash"}, "")
            .has_value());

    auto preserved = getEmbeddingStatusRow(*fix.pool_, "reconcile-preserve-hash");
    REQUIRE((preserved.has_value()));
    REQUIRE((preserved.value().has_value()));
    CHECK((preserved.value()->hasEmbedding));
    REQUIRE((preserved.value()->modelId.has_value()));
    CHECK((preserved.value()->modelId.value() == "legacy-model"));
    CHECK((preserved.value()->chunkCount == 7));

    REQUIRE(fix.repository_
                ->reconcileDocumentEmbeddingStatusByHashes(
                    {"reconcile-preserve-hash", "reconcile-preserve-hash"}, "fresh-model")
                .has_value());

    auto updated = getEmbeddingStatusRow(*fix.pool_, "reconcile-preserve-hash");
    REQUIRE((updated.has_value()));
    REQUIRE((updated.value().has_value()));
    CHECK((updated.value()->hasEmbedding));
    REQUIRE((updated.value()->modelId.has_value()));
    CHECK((updated.value()->modelId.value() == "fresh-model"));
    CHECK((updated.value()->chunkCount == 7));

    auto stats = fix.repository_->getCorpusStats();
    REQUIRE((stats.has_value()));
    CHECK((stats.value().embeddingCount == 1));
}

TEST_CASE("MetadataRepository: vector hash ownership ignores missing documents",
          "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/existing_vector_a.txt", "existing-vector-hash-a");
    auto docB = makeDocumentWithPath("/tmp/existing_vector_b.txt", "existing-vector-hash-b");

    REQUIRE((fix.repository_->insertDocument(docA).has_value()));
    REQUIRE((fix.repository_->insertDocument(docB).has_value()));

    auto existing = fix.repository_->getExistingDocumentHashes(
        {"existing-vector-hash-a", "orphan-vector-hash", "existing-vector-hash-b",
         "existing-vector-hash-a"});

    REQUIRE((existing.has_value()));
    CHECK((existing.value().size() == 2));
    CHECK((existing.value().contains("existing-vector-hash-a")));
    CHECK((existing.value().contains("existing-vector-hash-b")));
    CHECK_FALSE(existing.value().contains("orphan-vector-hash"));
}

TEST_CASE(
    "MetadataRepository: embedding status update by document id tracks counts and model changes",
    "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    auto doc = makeDocumentWithPath("/tmp/embed_by_id.txt", "embed-by-id-hash");
    auto insert = fix.repository_->insertDocument(doc);
    REQUIRE((insert.has_value()));
    const auto docId = insert.value();

    auto initialCount = fix.repository_->getEmbeddedDocumentCount();
    REQUIRE((initialCount.has_value()));
    CHECK((initialCount.value() == 0));

    REQUIRE((fix.repository_->updateDocumentEmbeddingStatus(docId, true, "model-a").has_value()));

    auto enabled = getEmbeddingStatusRow(*fix.pool_, "embed-by-id-hash");
    REQUIRE((enabled.has_value()));
    REQUIRE((enabled.value().has_value()));
    CHECK((enabled.value()->hasEmbedding));
    REQUIRE((enabled.value()->modelId.has_value()));
    CHECK((enabled.value()->modelId.value() == "model-a"));

    auto afterEnableCount = fix.repository_->getEmbeddedDocumentCount();
    REQUIRE((afterEnableCount.has_value()));
    CHECK((afterEnableCount.value() == 1));

    auto afterEnableStats = fix.repository_->getCorpusStats();
    REQUIRE((afterEnableStats.has_value()));
    CHECK((afterEnableStats.value().embeddingCount == 1));

    REQUIRE((fix.repository_->updateDocumentEmbeddingStatus(docId, true, "model-b").has_value()));

    auto updated = getEmbeddingStatusRow(*fix.pool_, "embed-by-id-hash");
    REQUIRE((updated.has_value()));
    REQUIRE((updated.value().has_value()));
    CHECK((updated.value()->hasEmbedding));
    REQUIRE((updated.value()->modelId.has_value()));
    CHECK((updated.value()->modelId.value() == "model-b"));

    auto afterRetagCount = fix.repository_->getEmbeddedDocumentCount();
    REQUIRE((afterRetagCount.has_value()));
    CHECK((afterRetagCount.value() == 1));

    REQUIRE((fix.repository_->updateDocumentEmbeddingStatus(docId, false, "").has_value()));

    auto disabled = getEmbeddingStatusRow(*fix.pool_, "embed-by-id-hash");
    REQUIRE((disabled.has_value()));
    REQUIRE((disabled.value().has_value()));
    CHECK_FALSE(disabled.value()->hasEmbedding);
    CHECK_FALSE(disabled.value()->modelId.has_value());

    auto afterDisableCount = fix.repository_->getEmbeddedDocumentCount();
    REQUIRE((afterDisableCount.has_value()));
    CHECK((afterDisableCount.value() == 0));

    auto afterDisableStats = fix.repository_->getCorpusStats();
    REQUIRE((afterDisableStats.has_value()));
    CHECK((afterDisableStats.value().embeddingCount == 0));

    REQUIRE((fix.repository_->updateDocumentEmbeddingStatus(docId, false, "").has_value()));
    auto afterNoopDisable = fix.repository_->getEmbeddedDocumentCount();
    REQUIRE((afterNoopDisable.has_value()));
    CHECK((afterNoopDisable.value() == 0));
}

TEST_CASE("MetadataRepository: embedding status update surfaces missing document identifiers",
          "[unit][metadata][repository][embeddings]") {
    MetadataRepositoryFixture fix;

    auto missingById = fix.repository_->updateDocumentEmbeddingStatus(999999, true, "model-x");
    REQUIRE_FALSE((missingById.has_value()));
    CHECK((missingById.error().message.find("Document not found") != std::string::npos));

    auto missingByHash =
        fix.repository_->updateDocumentEmbeddingStatusByHash("missing-embed-hash", true, "model-x");
    REQUIRE_FALSE((missingByHash.has_value()));
    CHECK(
        (missingByHash.error().message.find("Document with hash not found") != std::string::npos));

    auto hasMissing = fix.repository_->hasDocumentEmbeddingByHash("missing-embed-hash");
    REQUIRE((hasMissing.has_value()));
    CHECK_FALSE(hasMissing.value());

    auto embeddedCount = fix.repository_->getEmbeddedDocumentCount();
    REQUIRE((embeddedCount.has_value()));
    CHECK((embeddedCount.value() == 0));
}

TEST_CASE("MetadataRepository: repair status update increments attempts and persists state",
          "[unit][metadata][repository][repair]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/repair-single.txt", "repair-single-hash");
    auto insert = fix.repository_->insertDocument(doc);
    REQUIRE((insert.has_value()));
    const auto docId = insert.value();

    REQUIRE(
        (fix.repository_->updateDocumentRepairStatus("repair-single-hash", RepairStatus::Processing)
             .has_value()));

    auto firstRead = fix.repository_->getDocument(docId);
    REQUIRE((firstRead.has_value()));
    REQUIRE((firstRead.value().has_value()));
    CHECK((firstRead.value()->repairStatus == RepairStatus::Processing));
    CHECK((firstRead.value()->repairAttempts == 1));
    CHECK((firstRead.value()->repairAttemptedAt.time_since_epoch().count() > 0));

    const auto firstAttemptedAt = firstRead.value()->repairAttemptedAt.time_since_epoch().count();

    REQUIRE((fix.repository_->updateDocumentRepairStatus("repair-single-hash", RepairStatus::Failed)
                 .has_value()));

    auto secondRead = fix.repository_->getDocument(docId);
    REQUIRE((secondRead.has_value()));
    REQUIRE((secondRead.value().has_value()));
    CHECK((secondRead.value()->repairStatus == RepairStatus::Failed));
    CHECK((secondRead.value()->repairAttempts == 2));
    CHECK((secondRead.value()->repairAttemptedAt.time_since_epoch().count() >= firstAttemptedAt));
}

TEST_CASE("MetadataRepository: repair status update ignores missing hash",
          "[unit][metadata][repository][repair]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/repair-missing-control.txt", "repair-missing-control");
    auto insert = fix.repository_->insertDocument(doc);
    REQUIRE((insert.has_value()));

    REQUIRE(
        (fix.repository_->updateDocumentRepairStatus("repair-missing-hash", RepairStatus::Completed)
             .has_value()));

    auto control = fix.repository_->getDocument(insert.value());
    REQUIRE((control.has_value()));
    REQUIRE((control.value().has_value()));
    CHECK((control.value()->repairStatus == RepairStatus::Pending));
    CHECK((control.value()->repairAttempts == 0));
    CHECK((control.value()->repairAttemptedAt.time_since_epoch().count() == 0));
}

TEST_CASE("MetadataRepository: batch repair status update skips missing hashes and preserves no-op "
          "empty batch",
          "[unit][metadata][repository][repair]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/repair-batch-a.txt", "repair-batch-hash-a");
    auto docB = makeDocumentWithPath("/tmp/repair-batch-b.txt", "repair-batch-hash-b");
    auto insertA = fix.repository_->insertDocument(docA);
    auto insertB = fix.repository_->insertDocument(docB);
    REQUIRE((insertA.has_value()));
    REQUIRE((insertB.has_value()));

    REQUIRE((fix.repository_->batchUpdateDocumentRepairStatuses({}, RepairStatus::Processing)
                 .has_value()));

    auto beforeA = fix.repository_->getDocument(insertA.value());
    auto beforeB = fix.repository_->getDocument(insertB.value());
    REQUIRE((beforeA.has_value()));
    REQUIRE((beforeA.value().has_value()));
    REQUIRE((beforeB.has_value()));
    REQUIRE((beforeB.value().has_value()));
    CHECK((beforeA.value()->repairAttempts == 0));
    CHECK((beforeB.value()->repairAttempts == 0));

    REQUIRE((fix.repository_
                 ->batchUpdateDocumentRepairStatuses(
                     {"repair-batch-hash-a", "repair-missing-hash", "repair-batch-hash-b"},
                     RepairStatus::Processing)
                 .has_value()));

    auto afterA = fix.repository_->getDocument(insertA.value());
    auto afterB = fix.repository_->getDocument(insertB.value());
    REQUIRE((afterA.has_value()));
    REQUIRE((afterA.value().has_value()));
    REQUIRE((afterB.has_value()));
    REQUIRE((afterB.value().has_value()));
    CHECK((afterA.value()->repairStatus == RepairStatus::Processing));
    CHECK((afterB.value()->repairStatus == RepairStatus::Processing));
    CHECK((afterA.value()->repairAttempts == 1));
    CHECK((afterB.value()->repairAttempts == 1));
    CHECK((afterA.value()->repairAttemptedAt.time_since_epoch().count() > 0));
    CHECK((afterB.value()->repairAttemptedAt.time_since_epoch().count() > 0));
}

TEST_CASE("MetadataRepository: batch repair status update preserves duplicate hash attempt counts",
          "[unit][metadata][repository][repair]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/repair-duplicate.txt", "repair-duplicate-hash");
    auto insert = fix.repository_->insertDocument(doc);
    REQUIRE((insert.has_value()));

    REQUIRE((fix.repository_
                 ->batchUpdateDocumentRepairStatuses(
                     {"repair-duplicate-hash", "repair-duplicate-hash", "repair-duplicate-hash"},
                     RepairStatus::Processing)
                 .has_value()));

    auto after = fix.repository_->getDocument(insert.value());
    REQUIRE((after.has_value()));
    REQUIRE((after.value().has_value()));
    CHECK((after.value()->repairStatus == RepairStatus::Processing));
    CHECK((after.value()->repairAttempts == 3));
    CHECK((after.value()->repairAttemptedAt.time_since_epoch().count() > 0));
}

TEST_CASE("MetadataRepository: batch repair status update does not stack retry loops under lock",
          "[unit][metadata][repository][repair][contention]") {
    const auto dbPath = tempDbPath("metadata_repo_repair_lock_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    ConnectionPoolConfig lockerCfg;
    lockerCfg.minConnections = 1;
    lockerCfg.maxConnections = 1;
    lockerCfg.busyTimeout = std::chrono::milliseconds(10);
    auto lockerPool = std::make_unique<ConnectionPool>(dbPath.string(), lockerCfg);
    REQUIRE((lockerPool->initialize().has_value()));

    auto doc = makeDocumentWithPath("/tmp/repair-lock.txt", "repair-lock-hash");
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));

    auto heldConn = lockerPool->acquire();
    REQUIRE((heldConn.has_value()));
    REQUIRE(((*heldConn.value())->execute("BEGIN IMMEDIATE").has_value()));

    const auto start = std::chrono::steady_clock::now();
    auto result =
        repository->batchUpdateDocumentRepairStatuses({"repair-lock-hash"}, RepairStatus::Failed);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    REQUIRE_FALSE((result.has_value()));
    CHECK((result.error().message.find("locked") != std::string::npos));
    CHECK((elapsed < std::chrono::milliseconds(3000)));

    REQUIRE(((*heldConn.value())->execute("ROLLBACK").has_value()));

    lockerPool->shutdown();
    lockerPool.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: getMetadataValueCounts with filters",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto now = std::chrono::system_clock::now();
    auto nowSec = std::chrono::floor<std::chrono::seconds>(now);

    // doc1: old document (outside filter range)
    auto doc1 = makeDocumentWithPath("/tmp/a.txt", "hash-a");
    doc1.createdTime = nowSec - std::chrono::hours(48);
    doc1.modifiedTime = doc1.createdTime;
    doc1.indexedTime = doc1.createdTime;
    auto doc1Res = fix.repository_->insertDocument(doc1);
    REQUIRE((doc1Res.has_value()));

    // doc2: recent document (inside filter range)
    auto doc2 = makeDocumentWithPath("/tmp/b.txt", "hash-b");
    doc2.createdTime = nowSec;
    doc2.modifiedTime = nowSec;
    doc2.indexedTime = nowSec;
    auto doc2Res = fix.repository_->insertDocument(doc2);
    REQUIRE((doc2Res.has_value()));

    // doc3: another recent document (inside filter range)
    auto doc3 = makeDocumentWithPath("/tmp/c.txt", "hash-c");
    doc3.createdTime = nowSec;
    doc3.modifiedTime = nowSec;
    doc3.indexedTime = nowSec;
    auto doc3Res = fix.repository_->insertDocument(doc3);
    REQUIRE((doc3Res.has_value()));

    MetadataValue v1;
    v1.value = "PBI-070";
    v1.type = MetadataValueType::String;
    // doc1 gets PBI-070 but is outside time filter
    REQUIRE((fix.repository_->setMetadata(doc1Res.value(), "pbi", v1).has_value()));
    // doc2 gets PBI-070 (inside time filter)
    REQUIRE((fix.repository_->setMetadata(doc2Res.value(), "pbi", v1).has_value()));

    MetadataValue v2;
    v2.value = "PBI-071";
    v2.type = MetadataValueType::String;
    // doc3 gets PBI-071 (inside time filter)
    REQUIRE((fix.repository_->setMetadata(doc3Res.value(), "pbi", v2).has_value()));

    DocumentQueryOptions opts;
    opts.modifiedAfter = std::chrono::duration_cast<std::chrono::seconds>(
                             (nowSec - std::chrono::hours(24)).time_since_epoch())
                             .count();

    auto countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE((countsRes.has_value()));

    const auto& map = countsRes.value();
    auto it = map.find("pbi");
    REQUIRE((it != map.end()));

    std::unordered_map<std::string, int64_t> counts;
    for (const auto& row : it->second) {
        counts[row.value] = row.count;
    }

    // Only doc2 (PBI-070) and doc3 (PBI-071) are in the time range
    // doc1 (PBI-070) is filtered out because it's 48 hours old
    REQUIRE((counts.size() == 2));
    REQUIRE((counts["PBI-070"] == 1));
    REQUIRE((counts["PBI-071"] == 1));
}

TEST_CASE("MetadataRepository: metadata value counts materialized view stays consistent",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/materialized_a.txt", "materialized-a");
    auto docB = makeDocumentWithPath("/tmp/materialized_b.txt", "materialized-b");

    auto docARes = fix.repository_->insertDocument(docA);
    auto docBRes = fix.repository_->insertDocument(docB);
    REQUIRE((docARes.has_value()));
    REQUIRE((docBRes.has_value()));

    MetadataValue v1;
    v1.value = "PBI-500";
    v1.type = MetadataValueType::String;
    REQUIRE((fix.repository_->setMetadata(docARes.value(), "pbi", v1).has_value()));
    REQUIRE((fix.repository_->setMetadata(docBRes.value(), "pbi", v1).has_value()));

    DocumentQueryOptions opts;
    auto countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE((countsRes.has_value()));
    auto it = countsRes.value().find("pbi");
    REQUIRE((it != countsRes.value().end()));
    REQUIRE((it->second.size() == 1));
    CHECK((it->second.front().value == "PBI-500"));
    CHECK((it->second.front().count == 2));

    MetadataValue v2;
    v2.value = "PBI-600";
    v2.type = MetadataValueType::String;
    REQUIRE((fix.repository_->setMetadata(docBRes.value(), "pbi", v2).has_value()));

    countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE((countsRes.has_value()));
    it = countsRes.value().find("pbi");
    REQUIRE((it != countsRes.value().end()));
    std::unordered_map<std::string, int64_t> latest;
    for (const auto& row : it->second) {
        latest[row.value] = row.count;
    }
    REQUIRE((latest.size() == 2));
    CHECK((latest["PBI-500"] == 1));
    CHECK((latest["PBI-600"] == 1));

    auto deleteResult = fix.repository_->deleteDocument(docARes.value());
    REQUIRE((deleteResult.has_value()));

    countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE((countsRes.has_value()));
    it = countsRes.value().find("pbi");
    REQUIRE((it != countsRes.value().end()));
    REQUIRE((it->second.size() == 1));
    CHECK((it->second.front().value == "PBI-600"));
    CHECK((it->second.front().count == 1));
}

TEST_CASE("MetadataRepository: semantic duplicate groups round-trip",
          "[unit][metadata][repository][semantic-dedupe]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/semantic-a.txt", "semantic-a");
    auto docB = makeDocumentWithPath("/tmp/semantic-b.txt", "semantic-b");
    auto aId = fix.repository_->insertDocument(docA);
    auto bId = fix.repository_->insertDocument(docB);
    REQUIRE((aId.has_value()));
    REQUIRE((bId.has_value()));

    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    SemanticDuplicateGroup group;
    group.groupKey = "semantic:test-group";
    group.algorithmVersion = "semantic-dedupe-v1";
    group.status = "suggested";
    group.reviewState = "pending";
    group.canonicalDocumentId = aId.value();
    group.memberCount = 2;
    group.maxPairScore = 0.97;
    group.threshold = 0.92;
    group.evidenceJson = R"({"source":"test"})";
    group.createdAt = now;
    group.updatedAt = now;
    group.lastComputedAt = now;

    auto groupId = fix.repository_->upsertSemanticDuplicateGroup(group);
    REQUIRE((groupId.has_value()));

    SemanticDuplicateGroupMember canonical;
    canonical.documentId = aId.value();
    canonical.role = "canonical";
    canonical.decision = "keep";
    canonical.reason = "keep-newest";
    canonical.createdAt = now;
    canonical.updatedAt = now;

    SemanticDuplicateGroupMember duplicate;
    duplicate.documentId = bId.value();
    duplicate.role = "duplicate";
    duplicate.similarityToCanonical = 0.98;
    duplicate.titleOverlap = 0.75;
    duplicate.pathOverlap = 0.25;
    duplicate.pairScore = 0.93;
    duplicate.decision = "unknown";
    duplicate.createdAt = now;
    duplicate.updatedAt = now;

    REQUIRE(fix.repository_
                ->replaceSemanticDuplicateGroupMembers(groupId.value(), {canonical, duplicate})
                .has_value());

    auto fetched = fix.repository_->getSemanticDuplicateGroupByKey("semantic:test-group");
    REQUIRE((fetched.has_value()));
    REQUIRE((fetched.value().has_value()));
    CHECK((fetched.value()->canonicalDocumentId == aId.value()));
    CHECK((fetched.value()->memberCount == 2));

    auto listed = fix.repository_->listSemanticDuplicateGroups();
    REQUIRE((listed.has_value()));
    REQUIRE_FALSE(listed.value().empty());

    auto byDoc = fix.repository_->getSemanticDuplicateGroupsForDocuments(
        std::array<int64_t, 2>{aId.value(), bId.value()});
    REQUIRE((byDoc.has_value()));
    REQUIRE((byDoc.value().contains(aId.value())));
    REQUIRE((byDoc.value().contains(bId.value())));
    CHECK((byDoc.value().at(aId.value()).group.groupKey == "semantic:test-group"));
    CHECK((byDoc.value().at(aId.value()).members.size() == 2));

    REQUIRE(fix.repository_->updateSemanticDuplicateGroupStatus("semantic:test-group", "applied")
                .has_value());
    auto updated = fix.repository_->getSemanticDuplicateGroupByKey("semantic:test-group");
    REQUIRE((updated.has_value()));
    REQUIRE((updated.value().has_value()));
    CHECK((updated.value()->status == "applied"));
}

TEST_CASE("MetadataRepository: metadata value counts falls back when path FTS join fails",
          "[unit][metadata][repository][fts-fallback]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/notes/todo.md", "fts-fallback-a");
    auto docB = makeDocumentWithPath("/archive/todo-old.md", "fts-fallback-b");

    auto aId = fix.repository_->insertDocument(docA);
    auto bId = fix.repository_->insertDocument(docB);
    REQUIRE((aId.has_value()));
    REQUIRE((bId.has_value()));

    MetadataValue ownerA;
    ownerA.type = MetadataValueType::String;
    ownerA.value = "alice";
    REQUIRE((fix.repository_->setMetadata(aId.value(), "owner", ownerA).has_value()));

    MetadataValue ownerB;
    ownerB.type = MetadataValueType::String;
    ownerB.value = "bob";
    REQUIRE((fix.repository_->setMetadata(bId.value(), "owner", ownerB).has_value()));

    // Force the FTS-join query path to fail while pathFtsAvailable_ remains true in the repository.
    Database db;
    REQUIRE((db.open(fix.dbPath_.string()).has_value()));
    REQUIRE((db.execute("DROP TABLE IF EXISTS documents_path_fts").has_value()));
    db.close();

    DocumentQueryOptions fallbackOpts;
    fallbackOpts.containsFragment = "todo.md";
    fallbackOpts.containsUsesFts = true;

    auto fallbackRes = fix.repository_->getMetadataValueCounts({"owner"}, fallbackOpts);
    REQUIRE((fallbackRes.has_value()));

    DocumentQueryOptions baselineOpts = fallbackOpts;
    baselineOpts.containsUsesFts = false;
    auto baselineRes = fix.repository_->getMetadataValueCounts({"owner"}, baselineOpts);
    REQUIRE((baselineRes.has_value()));

    const auto& fallbackMap = fallbackRes.value();
    const auto& baselineMap = baselineRes.value();
    REQUIRE((fallbackMap.contains("owner")));
    REQUIRE((baselineMap.contains("owner")));
    REQUIRE((fallbackMap.at("owner").size() == baselineMap.at("owner").size()));

    std::unordered_map<std::string, int64_t> fallbackCounts;
    std::unordered_map<std::string, int64_t> baselineCounts;
    for (const auto& row : fallbackMap.at("owner")) {
        fallbackCounts[row.value] = row.count;
    }
    for (const auto& row : baselineMap.at("owner")) {
        baselineCounts[row.value] = row.count;
    }

    CHECK((fallbackCounts == baselineCounts));
    CHECK((fallbackCounts["alice"] == 1));
    CHECK_FALSE(fallbackCounts.contains("bob"));
}

TEST_CASE("MetadataRepository: get document not found", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto result = fix.repository_->getDocument(999999);
    REQUIRE((result.has_value()));
    CHECK_FALSE(result.value().has_value());
}

TEST_CASE("MetadataRepository: get document by hash", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash456";
    docInfo.fileName = "hash_test.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));

    auto findResult = fix.repository_->getDocumentByHash("hash456");
    REQUIRE((findResult.has_value()));
    REQUIRE((findResult.value().has_value()));

    auto foundDoc = findResult.value().value();
    CHECK((foundDoc.sha256Hash == "hash456"));
    CHECK((foundDoc.fileName == "hash_test.txt"));
}

TEST_CASE("MetadataRepository: update document", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash789";
    docInfo.fileName = "original.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    docInfo.id = docId;
    docInfo.fileName = "updated.txt";
    docInfo.fileSize = 4096;
    docInfo.modifiedTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    auto updateResult = fix.repository_->updateDocument(docInfo);
    REQUIRE((updateResult.has_value()));

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE((getResult.has_value()));
    REQUIRE((getResult.value().has_value()));

    auto updatedDoc = getResult.value().value();
    CHECK((updatedDoc.fileName == "updated.txt"));
    CHECK((updatedDoc.fileSize == 4096));
}

TEST_CASE("MetadataRepository: corpus stats path depth max shrinks after update",
          "[unit][metadata][repository][stats]") {
    MetadataRepositoryFixture fix;

    auto shallow = makeDocumentWithPath("/root.txt", "path-depth-update-shallow");
    auto deep = makeDocumentWithPath("/alpha/beta/gamma/deep.txt", "path-depth-update-deep");
    REQUIRE((deep.pathDepth > shallow.pathDepth));

    auto shallowInsert = fix.repository_->insertDocument(shallow);
    auto deepInsert = fix.repository_->insertDocument(deep);
    REQUIRE((shallowInsert.has_value()));
    REQUIRE((deepInsert.has_value()));

    auto warmStats = fix.repository_->getCorpusStats();
    REQUIRE((warmStats.has_value()));
    CHECK_THAT(warmStats.value().pathDepthMax,
               Catch::Matchers::WithinRel(static_cast<double>(deep.pathDepth), 0.01));

    deep.id = deepInsert.value();
    deep.filePath = "/flattened.txt";
    deep.fileName = "flattened.txt";
    deep.fileExtension = ".txt";
    populatePathDerivedFields(deep);
    deep.modifiedTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    REQUIRE((fix.repository_->updateDocument(deep).has_value()));

    auto afterStats = fix.repository_->getCorpusStats();
    REQUIRE((afterStats.has_value()));
    const auto expectedMaxDepth = static_cast<double>(std::max(shallow.pathDepth, deep.pathDepth));
    CHECK_THAT(afterStats.value().pathDepthMax, Catch::Matchers::WithinRel(expectedMaxDepth, 0.01));
}

TEST_CASE("MetadataRepository: query documents handles exact prefix and suffix",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath(
        "/notes/todo.md", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docB = makeDocumentWithPath(
        "/notes/log.txt", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto docC = makeDocumentWithPath(
        "/reports/summary.pdf", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        "application/pdf");

    REQUIRE((fix.repository_->insertDocument(docA).has_value()));
    REQUIRE((fix.repository_->insertDocument(docB).has_value()));
    REQUIRE((fix.repository_->insertDocument(docC).has_value()));

    SECTION("Exact path lookup") {
        DocumentQueryOptions exactOpts;
        exactOpts.exactPath = "/notes/todo.md";
        auto exactRes = fix.repository_->queryDocuments(exactOpts);
        REQUIRE((exactRes.has_value()));
        REQUIRE((exactRes.value().size() == 1));
        CHECK((exactRes.value().front().filePath == "/notes/todo.md"));
    }

    SECTION("Directory prefix with subdirectories") {
        DocumentQueryOptions prefixOpts;
        prefixOpts.pathPrefix = "/notes";
        prefixOpts.prefixIsDirectory = true;
        prefixOpts.includeSubdirectories = true;
        auto prefixRes = fix.repository_->queryDocuments(prefixOpts);
        REQUIRE((prefixRes.has_value()));
        std::vector<std::string> notePaths;
        for (const auto& d : prefixRes.value())
            notePaths.push_back(d.filePath);
        std::sort(notePaths.begin(), notePaths.end());
        REQUIRE((notePaths.size() == 2));
        CHECK((notePaths[0] == "/notes/log.txt"));
        CHECK((notePaths[1] == "/notes/todo.md"));
    }

    SECTION("Contains fragment via FTS") {
        DocumentQueryOptions containsOpts;
        containsOpts.containsFragment = "todo.md";
        containsOpts.containsUsesFts = true;
        auto containsRes = fix.repository_->queryDocuments(containsOpts);
        REQUIRE((containsRes.has_value()));
        REQUIRE((containsRes.value().size() == 1));
        CHECK((containsRes.value().front().filePath == "/notes/todo.md"));
    }

    SECTION("Contains fragment via FTS survives status updates and tracks path changes") {
        DocumentQueryOptions todoOpts;
        todoOpts.containsFragment = "todo.md";
        todoOpts.containsUsesFts = true;
        auto initialTodo = fix.repository_->queryDocuments(todoOpts);
        REQUIRE((initialTodo.has_value()));
        REQUIRE((initialTodo.value().size() == 1));
        CHECK((initialTodo.value().front().filePath == "/notes/todo.md"));

        auto docResult = fix.repository_->getDocumentByHash(docA.sha256Hash);
        REQUIRE((docResult.has_value()));
        REQUIRE((docResult.value().has_value()));
        auto updated = docResult.value().value();

        std::vector<ExtractionStatusUpdate> statusUpdates;
        statusUpdates.push_back(
            ExtractionStatusUpdate{updated.id, false, ExtractionStatus::Failed, "expected test"});
        REQUIRE(
            (fix.repository_->batchUpdateDocumentExtractionStatuses(statusUpdates).has_value()));

        auto afterStatusUpdate = fix.repository_->queryDocuments(todoOpts);
        REQUIRE((afterStatusUpdate.has_value()));
        REQUIRE((afterStatusUpdate.value().size() == 1));
        CHECK((afterStatusUpdate.value().front().filePath == "/notes/todo.md"));

        updated.filePath = "/archive/renamed.md";
        updated.fileName = "renamed.md";
        updated.fileExtension = ".md";
        updated.modifiedTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        populatePathDerivedFields(updated);
        REQUIRE((fix.repository_->updateDocument(updated).has_value()));

        auto afterRenameOld = fix.repository_->queryDocuments(todoOpts);
        REQUIRE((afterRenameOld.has_value()));
        CHECK((afterRenameOld.value().empty()));

        DocumentQueryOptions renamedOpts;
        renamedOpts.containsFragment = "renamed.md";
        renamedOpts.containsUsesFts = true;
        auto afterRenameNew = fix.repository_->queryDocuments(renamedOpts);
        REQUIRE((afterRenameNew.has_value()));
        REQUIRE((afterRenameNew.value().size() == 1));
        CHECK((afterRenameNew.value().front().filePath == "/archive/renamed.md"));
    }

    SECTION("Extension filter") {
        DocumentQueryOptions extOpts;
        extOpts.extension = ".txt";
        auto extRes = fix.repository_->queryDocuments(extOpts);
        REQUIRE((extRes.has_value()));
        REQUIRE((extRes.value().size() == 1));
        CHECK((extRes.value().front().filePath == "/notes/log.txt"));
    }
}

TEST_CASE("MetadataRepository: list projection matches queryDocuments for shared filters",
          "[unit][metadata][repository][list-projection][parity]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath(
        "/work/notes/todo.md", "1111111111111111111111111111111111111111111111111111111111111111");
    auto docB = makeDocumentWithPath(
        "/work/notes/log.txt", "2222222222222222222222222222222222222222222222222222222222222222");
    auto docC = makeDocumentWithPath(
        "/work/reports/summary.pdf",
        "3333333333333333333333333333333333333333333333333333333333333333", "application/pdf");
    auto docD =
        makeDocumentWithPath("/archive/notes/todo-old.md",
                             "4444444444444444444444444444444444444444444444444444444444444444");

    docA.fileSize = 10;
    docB.fileSize = 20;
    docC.fileSize = 30;
    docD.fileSize = 40;
    docB.extractionStatus = ExtractionStatus::Failed;
    docB.contentExtracted = false;

    auto aId = fix.repository_->insertDocument(docA);
    auto bId = fix.repository_->insertDocument(docB);
    auto cId = fix.repository_->insertDocument(docC);
    auto dId = fix.repository_->insertDocument(docD);
    REQUIRE((aId.has_value()));
    REQUIRE((bId.has_value()));
    REQUIRE((cId.has_value()));
    REQUIRE((dId.has_value()));

    auto setStringMeta = [&](int64_t docId, const std::string& key, const std::string& value) {
        MetadataValue mv;
        mv.type = MetadataValueType::String;
        mv.value = value;
        REQUIRE((fix.repository_->setMetadata(docId, key, mv).has_value()));
    };

    setStringMeta(aId.value(), "owner", "alice");
    setStringMeta(bId.value(), "owner", "alice");
    setStringMeta(cId.value(), "owner", "bob");
    setStringMeta(aId.value(), "tag:notes", "notes");
    setStringMeta(dId.value(), "tag:notes", "notes");

    auto assertParity = [&](const DocumentQueryOptions& opts, const std::string& label) {
        CAPTURE(label);
        auto docsRes = fix.repository_->queryDocuments(opts);
        auto projRes = fix.repository_->queryDocumentsForListProjection(opts);
        REQUIRE((docsRes.has_value()));
        REQUIRE((projRes.has_value()));

        const auto& docs = docsRes.value();
        const auto& projs = projRes.value();
        REQUIRE((docs.size() == projs.size()));

        for (size_t i = 0; i < docs.size(); ++i) {
            CAPTURE(i);
            CHECK((projs[i].id == docs[i].id));
            CHECK((projs[i].filePath == docs[i].filePath));
            CHECK((projs[i].fileName == docs[i].fileName));
            CHECK((projs[i].fileExtension == docs[i].fileExtension));
            CHECK((projs[i].fileSize == docs[i].fileSize));
            CHECK((projs[i].sha256Hash == docs[i].sha256Hash));
            CHECK((projs[i].mimeType == docs[i].mimeType));
            CHECK((projs[i].createdTime == docs[i].createdTime));
            CHECK((projs[i].modifiedTime == docs[i].modifiedTime));
            CHECK((projs[i].indexedTime == docs[i].indexedTime));
            CHECK((projs[i].extractionStatus == docs[i].extractionStatus));
        }
    };

    SECTION("exact path") {
        DocumentQueryOptions opts;
        opts.exactPath = "/work/notes/todo.md";
        assertParity(opts, "exactPath");
    }

    SECTION("directory prefix") {
        DocumentQueryOptions opts;
        opts.pathPrefix = "/work/notes";
        opts.prefixIsDirectory = true;
        opts.includeSubdirectories = true;
        opts.orderByNameAsc = true;
        assertParity(opts, "pathPrefix");
    }

    SECTION("contains fragment via FTS") {
        DocumentQueryOptions opts;
        opts.containsFragment = "todo";
        opts.containsUsesFts = true;
        opts.orderByNameAsc = true;
        assertParity(opts, "containsFragment");
    }

    SECTION("combined metadata and tag filter") {
        DocumentQueryOptions opts;
        opts.extension = ".md";
        opts.metadataFilters.push_back({"owner", "alice"});
        opts.tags.push_back("notes");
        opts.orderByNameAsc = true;
        assertParity(opts, "metadata+tag");
    }

    SECTION("extraction status plus pagination") {
        DocumentQueryOptions opts;
        opts.pathPrefix = "/work";
        opts.prefixIsDirectory = true;
        opts.orderByNameAsc = true;
        opts.extractionStatuses = {ExtractionStatus::Success, ExtractionStatus::Failed};
        opts.limit = 2;
        opts.offset = 1;
        assertParity(opts, "extractionStatuses+pagination");
    }
}

TEST_CASE("MetadataRepository: delete document", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash999";
    docInfo.fileName = "delete_me.txt";
    docInfo.fileSize = 512;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    auto deleteResult = fix.repository_->deleteDocument(docId);
    REQUIRE((deleteResult.has_value()));

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE((getResult.has_value()));
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE("MetadataRepository: corpus stats path depth max shrinks after delete",
          "[unit][metadata][repository][stats]") {
    MetadataRepositoryFixture fix;

    auto shallow = makeDocumentWithPath("/keep.txt", "path-depth-delete-shallow");
    auto deep = makeDocumentWithPath("/one/two/three/four/deep.txt", "path-depth-delete-deep");
    REQUIRE((deep.pathDepth > shallow.pathDepth));

    auto shallowInsert = fix.repository_->insertDocument(shallow);
    auto deepInsert = fix.repository_->insertDocument(deep);
    REQUIRE((shallowInsert.has_value()));
    REQUIRE((deepInsert.has_value()));

    auto warmStats = fix.repository_->getCorpusStats();
    REQUIRE((warmStats.has_value()));
    CHECK_THAT(warmStats.value().pathDepthMax,
               Catch::Matchers::WithinRel(static_cast<double>(deep.pathDepth), 0.01));

    REQUIRE((fix.repository_->deleteDocument(deepInsert.value()).has_value()));

    auto afterStats = fix.repository_->getCorpusStats();
    REQUIRE((afterStats.has_value()));
    CHECK_THAT(afterStats.value().pathDepthMax,
               Catch::Matchers::WithinRel(static_cast<double>(shallow.pathDepth), 0.01));
}

TEST_CASE("MetadataRepository: corpus stats path depth max shrinks after batch delete",
          "[unit][metadata][repository][stats]") {
    MetadataRepositoryFixture fix;

    auto shallow = makeDocumentWithPath("/batch/keep.txt", "path-depth-batch-shallow");
    auto medium = makeDocumentWithPath("/batch/one/two/keep.txt", "path-depth-batch-medium");
    auto deep = makeDocumentWithPath("/batch/one/two/three/four/deep.txt", "path-depth-batch-deep");
    REQUIRE((deep.pathDepth > medium.pathDepth));
    REQUIRE((medium.pathDepth > shallow.pathDepth));

    auto shallowInsert = fix.repository_->insertDocument(shallow);
    auto mediumInsert = fix.repository_->insertDocument(medium);
    auto deepInsert = fix.repository_->insertDocument(deep);
    REQUIRE((shallowInsert.has_value()));
    REQUIRE((mediumInsert.has_value()));
    REQUIRE((deepInsert.has_value()));

    auto warmStats = fix.repository_->getCorpusStats();
    REQUIRE((warmStats.has_value()));
    CHECK_THAT(warmStats.value().pathDepthMax,
               Catch::Matchers::WithinRel(static_cast<double>(deep.pathDepth), 0.01));

    auto deleted = fix.repository_->deleteDocumentsBatch({deepInsert.value(), 999999});
    REQUIRE((deleted.has_value()));
    CHECK((deleted.value() == 1));

    auto afterStats = fix.repository_->getCorpusStats();
    REQUIRE((afterStats.has_value()));
    CHECK_THAT(afterStats.value().pathDepthMax,
               Catch::Matchers::WithinRel(static_cast<double>(medium.pathDepth), 0.01));
}

TEST_CASE("MetadataRepository: set and get metadata", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "metadata_test";
    docInfo.fileName = "meta.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    MetadataValue value("Test Author");

    auto setResult = fix.repository_->setMetadata(docId, "author", value);
    REQUIRE((setResult.has_value()));

    auto getResult = fix.repository_->getMetadata(docId, "author");
    REQUIRE((getResult.has_value()));
    REQUIRE((getResult.value().has_value()));

    auto retrievedValue = getResult.value().value();
    CHECK((retrievedValue.asString() == "Test Author"));
}

TEST_CASE("MetadataRepository: get all metadata", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "all_metadata_test";
    docInfo.fileName = "meta_all.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    MetadataValue authorValue("John Doe");
    fix.repository_->setMetadata(docId, "author", authorValue);

    MetadataValue yearValue(int64_t(2024));
    fix.repository_->setMetadata(docId, "year", yearValue);

    MetadataValue ratingValue(4.5);
    fix.repository_->setMetadata(docId, "rating", ratingValue);

    auto getAllResult = fix.repository_->getAllMetadata(docId);
    REQUIRE((getAllResult.has_value()));

    auto metadata = getAllResult.value();
    CHECK((metadata.size() == 3));

    CHECK((metadata["author"].asString() == "John Doe"));
    CHECK((metadata["year"].asInteger() == 2024));
    CHECK_THAT(metadata["rating"].asReal(), Catch::Matchers::WithinRel(4.5, 0.0001));
}

TEST_CASE("MetadataRepository: remove metadata", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "remove_metadata_test";
    docInfo.fileName = "remove.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    MetadataValue value("Temporary");
    fix.repository_->setMetadata(docId, "temp", value);

    auto getResult = fix.repository_->getMetadata(docId, "temp");
    REQUIRE((getResult.has_value()));
    REQUIRE((getResult.value().has_value()));

    auto removeResult = fix.repository_->removeMetadata(docId, "temp");
    REQUIRE((removeResult.has_value()));

    getResult = fix.repository_->getMetadata(docId, "temp");
    REQUIRE((getResult.has_value()));
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE("MetadataRepository: hasFtsEntry reports FTS5 presence correctly",
          "[unit][metadata][repository][fts5]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "fts5_entry_check";
    docInfo.fileName = "fts5check.txt";
    docInfo.fileSize = 100;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    SECTION("returns false before indexing") {
        auto hasFts = fix.repository_->hasFtsEntry(docId);
        REQUIRE((hasFts.has_value()));
        CHECK_FALSE(hasFts.value());
    }

    SECTION("returns true after indexing") {
        auto indexResult = fix.repository_->indexDocumentContent(
            docId, "Test Document", "Searchable content here", "text/plain");
        REQUIRE((indexResult.has_value()));

        auto hasFts = fix.repository_->hasFtsEntry(docId);
        REQUIRE((hasFts.has_value()));
        CHECK((hasFts.value()));
    }

    SECTION("returns false for non-existent document ID") {
        auto hasFts = fix.repository_->hasFtsEntry(999999);
        REQUIRE((hasFts.has_value()));
        CHECK_FALSE(hasFts.value());
    }
}

TEST_CASE("MetadataRepository: indexed count reflects actual FTS rows, not extraction success",
          "[unit][metadata][repository][fts5][counts]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    DocumentInfo doc;
    doc.sha256Hash = "indexed_count_fts_only_hash";
    doc.fileName = "indexed_count_fts_only.txt";
    doc.fileSize = 128;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;

    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    const auto docId = insertResult.value();

    REQUIRE(fix.repository_->updateDocumentExtractionStatus(docId, true, ExtractionStatus::Success)
                .has_value());

    auto indexedWithoutFts = fix.repository_->getIndexedDocumentCount();
    REQUIRE((indexedWithoutFts.has_value()));
    CHECK((indexedWithoutFts.value() == 0));
    CHECK((fix.repository_->getCachedIndexedCount() == 0));
    CHECK((fix.repository_->getCachedExtractedCount() == 1));

    DocumentContent content;
    content.documentId = docId;
    content.contentText = "fts count probe";
    content.contentLength = static_cast<int64_t>(content.contentText.size());
    content.extractionMethod = "test";
    content.language = "en";
    REQUIRE((fix.repository_->insertContent(content).has_value()));
    REQUIRE(fix.repository_
                ->indexDocumentContent(docId, doc.fileName, content.contentText, "text/plain")
                .has_value());

    auto indexedWithFts = fix.repository_->getIndexedDocumentCount();
    REQUIRE((indexedWithFts.has_value()));
    CHECK((indexedWithFts.value() == 1));
    CHECK((fix.repository_->getCachedIndexedCount() == 1));
}

TEST_CASE("MetadataRepository: batch extraction status updates share one transaction",
          "[unit][metadata][repository][extraction]") {
    MetadataRepositoryFixture fix;

    auto pendingDoc = makeDocumentWithPath("/tmp/yams/batch-extraction-pending.txt",
                                           "batch-extraction-pending-hash");
    pendingDoc.contentExtracted = false;
    pendingDoc.extractionStatus = ExtractionStatus::Pending;
    auto pendingId = fix.repository_->insertDocument(pendingDoc);
    REQUIRE((pendingId.has_value()));

    auto extractedDoc = makeDocumentWithPath("/tmp/yams/batch-extraction-extracted.txt",
                                             "batch-extraction-extracted-hash");
    extractedDoc.contentExtracted = true;
    extractedDoc.extractionStatus = ExtractionStatus::Success;
    auto extractedId = fix.repository_->insertDocument(extractedDoc);
    REQUIRE((extractedId.has_value()));

    fix.repository_->initializeCounters();
    CHECK((fix.repository_->getCachedExtractedCount() == 1));

    std::vector<ExtractionStatusUpdate> updates;
    updates.push_back(
        ExtractionStatusUpdate{pendingId.value(), true, ExtractionStatus::Success, std::string{}});
    updates.push_back(ExtractionStatusUpdate{extractedId.value(), false, ExtractionStatus::Failed,
                                             "extraction failed"});
    updates.push_back(
        ExtractionStatusUpdate{999999, true, ExtractionStatus::Success, "missing ignored"});

    auto result = fix.repository_->batchUpdateDocumentExtractionStatuses(updates);
    REQUIRE((result.has_value()));

    auto pendingAfter = fix.repository_->getDocument(pendingId.value());
    REQUIRE((pendingAfter.has_value()));
    REQUIRE((pendingAfter.value().has_value()));
    CHECK((pendingAfter.value()->contentExtracted));
    CHECK((pendingAfter.value()->extractionStatus == ExtractionStatus::Success));
    CHECK((pendingAfter.value()->extractionError.empty()));

    auto extractedAfter = fix.repository_->getDocument(extractedId.value());
    REQUIRE((extractedAfter.has_value()));
    REQUIRE((extractedAfter.value().has_value()));
    CHECK_FALSE(extractedAfter.value()->contentExtracted);
    CHECK((extractedAfter.value()->extractionStatus == ExtractionStatus::Failed));
    CHECK((extractedAfter.value()->extractionError == "extraction failed"));
    CHECK((fix.repository_->getCachedExtractedCount() == 1));
}

TEST_CASE("MetadataRepository: batch extraction status updates use the last update per document",
          "[unit][metadata][repository][extraction]") {
    MetadataRepositoryFixture fix;

    auto pendingDoc =
        makeDocumentWithPath("/tmp/yams/extraction-duplicate-pending.txt", "extract-dup-pending");
    pendingDoc.contentExtracted = false;
    pendingDoc.extractionStatus = ExtractionStatus::Pending;

    auto successDoc =
        makeDocumentWithPath("/tmp/yams/extraction-duplicate-success.txt", "extract-dup-success");
    successDoc.contentExtracted = true;
    successDoc.extractionStatus = ExtractionStatus::Success;

    auto pendingId = fix.repository_->insertDocument(pendingDoc);
    auto successId = fix.repository_->insertDocument(successDoc);
    REQUIRE((pendingId.has_value()));
    REQUIRE((successId.has_value()));

    fix.repository_->initializeCounters();
    CHECK((fix.repository_->getCachedExtractedCount() == 1));

    std::vector<ExtractionStatusUpdate> updates;
    updates.push_back(
        ExtractionStatusUpdate{pendingId.value(), true, ExtractionStatus::Success, std::string{}});
    updates.push_back(ExtractionStatusUpdate{pendingId.value(), false, ExtractionStatus::Failed,
                                             "duplicate failure wins"});
    updates.push_back(
        ExtractionStatusUpdate{successId.value(), false, ExtractionStatus::Failed, "transient"});
    updates.push_back(
        ExtractionStatusUpdate{successId.value(), true, ExtractionStatus::Success, std::string{}});

    REQUIRE((fix.repository_->batchUpdateDocumentExtractionStatuses(updates).has_value()));

    auto pendingAfter = fix.repository_->getDocument(pendingId.value());
    auto successAfter = fix.repository_->getDocument(successId.value());
    REQUIRE((pendingAfter.has_value()));
    REQUIRE((successAfter.has_value()));
    REQUIRE((pendingAfter.value().has_value()));
    REQUIRE((successAfter.value().has_value()));

    CHECK_FALSE(pendingAfter.value()->contentExtracted);
    CHECK((pendingAfter.value()->extractionStatus == ExtractionStatus::Failed));
    CHECK((pendingAfter.value()->extractionError == "duplicate failure wins"));

    CHECK((successAfter.value()->contentExtracted));
    CHECK((successAfter.value()->extractionStatus == ExtractionStatus::Success));
    CHECK((successAfter.value()->extractionError.empty()));
    CHECK((fix.repository_->getCachedExtractedCount() == 1));
}

TEST_CASE(
    "MetadataRepository: extraction status counters handle empty, negative, and missing updates",
    "[unit][metadata][repository][extraction]") {
    MetadataRepositoryFixture fix;

    auto pendingDoc =
        makeDocumentWithPath("/tmp/yams/extraction-count-pending.txt", "extract-count-pending");
    pendingDoc.contentExtracted = false;
    pendingDoc.extractionStatus = ExtractionStatus::Pending;

    auto failedDoc =
        makeDocumentWithPath("/tmp/yams/extraction-count-failed.txt", "extract-count-failed");
    failedDoc.contentExtracted = false;
    failedDoc.extractionStatus = ExtractionStatus::Failed;

    auto successDoc =
        makeDocumentWithPath("/tmp/yams/extraction-count-success.txt", "extract-count-success");
    successDoc.contentExtracted = true;
    successDoc.extractionStatus = ExtractionStatus::Success;

    auto pendingId = fix.repository_->insertDocument(pendingDoc);
    auto failedId = fix.repository_->insertDocument(failedDoc);
    auto successId = fix.repository_->insertDocument(successDoc);
    REQUIRE((pendingId.has_value()));
    REQUIRE((failedId.has_value()));
    REQUIRE((successId.has_value()));

    auto pendingCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Pending);
    auto failedCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Failed);
    auto successCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Success);
    auto skippedCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Skipped);
    REQUIRE((pendingCount.has_value()));
    REQUIRE((failedCount.has_value()));
    REQUIRE((successCount.has_value()));
    REQUIRE((skippedCount.has_value()));
    CHECK((pendingCount.value() == 1));
    CHECK((failedCount.value() == 1));
    CHECK((successCount.value() == 1));
    CHECK((skippedCount.value() == 0));

    fix.repository_->initializeCounters();
    CHECK((fix.repository_->getCachedExtractedCount() == 1));

    REQUIRE((fix.repository_->batchUpdateDocumentExtractionStatuses({}).has_value()));
    CHECK((fix.repository_->getCachedExtractedCount() == 1));

    std::vector<ExtractionStatusUpdate> updates;
    updates.push_back(
        ExtractionStatusUpdate{-7, true, ExtractionStatus::Success, "ignored negative"});
    updates.push_back(
        ExtractionStatusUpdate{999999, true, ExtractionStatus::Success, "ignored missing"});
    updates.push_back(
        ExtractionStatusUpdate{pendingId.value(), true, ExtractionStatus::Success, std::string{}});
    updates.push_back(
        ExtractionStatusUpdate{failedId.value(), false, ExtractionStatus::Failed, "still failed"});

    REQUIRE((fix.repository_->batchUpdateDocumentExtractionStatuses(updates).has_value()));

    auto pendingAfter = fix.repository_->getDocument(pendingId.value());
    auto failedAfter = fix.repository_->getDocument(failedId.value());
    auto successAfter = fix.repository_->getDocument(successId.value());
    REQUIRE((pendingAfter.has_value()));
    REQUIRE((failedAfter.has_value()));
    REQUIRE((successAfter.has_value()));
    REQUIRE((pendingAfter.value().has_value()));
    REQUIRE((failedAfter.value().has_value()));
    REQUIRE((successAfter.value().has_value()));
    CHECK((pendingAfter.value()->contentExtracted));
    CHECK((pendingAfter.value()->extractionStatus == ExtractionStatus::Success));
    CHECK((pendingAfter.value()->extractionError.empty()));
    CHECK_FALSE(failedAfter.value()->contentExtracted);
    CHECK((failedAfter.value()->extractionStatus == ExtractionStatus::Failed));
    CHECK((failedAfter.value()->extractionError == "still failed"));
    CHECK((successAfter.value()->contentExtracted));
    CHECK((fix.repository_->getCachedExtractedCount() == 2));

    auto pendingAfterCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Pending);
    auto failedAfterCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Failed);
    auto successAfterCount =
        fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Success);
    REQUIRE((pendingAfterCount.has_value()));
    REQUIRE((failedAfterCount.has_value()));
    REQUIRE((successAfterCount.has_value()));
    CHECK((pendingAfterCount.value() == 0));
    CHECK((failedAfterCount.value() == 1));
    CHECK((successAfterCount.value() == 2));

    auto stats = fix.repository_->getCorpusStats();
    REQUIRE((stats.has_value()));
    CHECK((stats.value().contentExtractedCount == 2));
}

TEST_CASE("MetadataRepository: single extraction status update does not amplify lock retries",
          "[unit][metadata][repository][extraction][contention]") {
    const auto dbPath = tempDbPath("metadata_repo_single_extraction_lock_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    ConnectionPoolConfig lockerCfg;
    lockerCfg.minConnections = 1;
    lockerCfg.maxConnections = 1;
    lockerCfg.busyTimeout = std::chrono::milliseconds(10);
    auto lockerPool = std::make_unique<ConnectionPool>(dbPath.string(), lockerCfg);
    REQUIRE((lockerPool->initialize().has_value()));

    auto doc =
        makeDocumentWithPath("/tmp/single-extraction-lock.txt", "single-extraction-lock-hash");
    doc.contentExtracted = false;
    doc.extractionStatus = ExtractionStatus::Pending;
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));

    auto heldConn = lockerPool->acquire();
    REQUIRE((heldConn.has_value()));
    REQUIRE(((*heldConn.value())->execute("BEGIN IMMEDIATE").has_value()));

    const auto start = std::chrono::steady_clock::now();
    auto result = repository->updateDocumentExtractionStatus(
        insert.value(), true, ExtractionStatus::Success, std::string{});
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    REQUIRE_FALSE((result.has_value()));
    CHECK((result.error().message.find("locked") != std::string::npos));
    CHECK((elapsed < std::chrono::milliseconds(3000)));

    REQUIRE(((*heldConn.value())->execute("ROLLBACK").has_value()));

    lockerPool->shutdown();
    lockerPool.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: batch extraction status update does not amplify lock retries",
          "[unit][metadata][repository][extraction][contention]") {
    const auto dbPath = tempDbPath("metadata_repo_extraction_lock_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    ConnectionPoolConfig lockerCfg;
    lockerCfg.minConnections = 1;
    lockerCfg.maxConnections = 1;
    lockerCfg.busyTimeout = std::chrono::milliseconds(10);
    auto lockerPool = std::make_unique<ConnectionPool>(dbPath.string(), lockerCfg);
    REQUIRE((lockerPool->initialize().has_value()));

    auto doc = makeDocumentWithPath("/tmp/extraction-lock.txt", "extraction-lock-hash");
    doc.contentExtracted = false;
    doc.extractionStatus = ExtractionStatus::Pending;
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));

    auto heldConn = lockerPool->acquire();
    REQUIRE((heldConn.has_value()));
    REQUIRE(((*heldConn.value())->execute("BEGIN IMMEDIATE").has_value()));

    const auto start = std::chrono::steady_clock::now();
    auto result = repository->batchUpdateDocumentExtractionStatuses(
        {{insert.value(), true, ExtractionStatus::Success, std::string{}}});
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    REQUIRE_FALSE((result.has_value()));
    CHECK((result.error().message.find("locked") != std::string::npos));
    CHECK((elapsed < std::chrono::milliseconds(3000)));

    REQUIRE(((*heldConn.value())->execute("ROLLBACK").has_value()));

    lockerPool->shutdown();
    lockerPool.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: initializeCounters keeps extracted and indexed counts distinct",
          "[unit][metadata][repository][fts5][counters]") {
    MetadataRepositoryFixture fix;

    DocumentInfo doc;
    doc.sha256Hash = "counter_distinct_hash";
    doc.fileName = "counter_distinct.txt";
    doc.fileSize = 64;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;

    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    const auto docId = insertResult.value();

    REQUIRE(fix.repository_->updateDocumentExtractionStatus(docId, true, ExtractionStatus::Success)
                .has_value());

    fix.repository_->initializeCounters();
    CHECK((fix.repository_->getCachedExtractedCount() == 1));
    CHECK((fix.repository_->getCachedIndexedCount() == 0));

    DocumentContent content;
    content.documentId = docId;
    content.contentText = "counter refresh content";
    content.contentLength = static_cast<int64_t>(content.contentText.size());
    content.extractionMethod = "test";
    content.language = "en";
    REQUIRE((fix.repository_->insertContent(content).has_value()));
    REQUIRE(fix.repository_
                ->indexDocumentContent(docId, doc.fileName, content.contentText, "text/plain")
                .has_value());

    auto exactIndexed = fix.repository_->getIndexedDocumentCount();
    REQUIRE((exactIndexed.has_value()));
    CHECK((exactIndexed.value() == 1));
    CHECK((fix.repository_->getCachedExtractedCount() == 1));
}

TEST_CASE("MetadataRepository: getFts5IndexedRowIdSet returns indexed document IDs",
          "[unit][metadata][repository][fts5]") {
    MetadataRepositoryFixture fix;

    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    // Insert two documents
    DocumentInfo doc1;
    doc1.sha256Hash = "fts5_set_doc1";
    doc1.fileName = "doc1.txt";
    doc1.fileSize = 50;
    doc1.mimeType = "text/plain";
    doc1.createdTime = now;
    doc1.modifiedTime = now;
    auto id1Res = fix.repository_->insertDocument(doc1);
    REQUIRE((id1Res.has_value()));
    auto docId1 = id1Res.value();

    DocumentInfo doc2;
    doc2.sha256Hash = "fts5_set_doc2";
    doc2.fileName = "doc2.txt";
    doc2.fileSize = 60;
    doc2.mimeType = "text/plain";
    doc2.createdTime = now;
    doc2.modifiedTime = now;
    auto id2Res = fix.repository_->insertDocument(doc2);
    REQUIRE((id2Res.has_value()));
    auto docId2 = id2Res.value();

    // Before indexing: set should be empty
    auto setBefore = fix.repository_->getFts5IndexedRowIdSet();
    REQUIRE((setBefore.has_value()));
    CHECK((setBefore.value().count(docId1) == 0));
    CHECK((setBefore.value().count(docId2) == 0));

    // Index only doc1
    auto indexResult = fix.repository_->indexDocumentContent(
        docId1, "Doc One", "First document content", "text/plain");
    REQUIRE((indexResult.has_value()));

    // Set should contain only doc1
    auto setAfter = fix.repository_->getFts5IndexedRowIdSet();
    REQUIRE((setAfter.has_value()));
    CHECK((setAfter.value().count(docId1) == 1));
    CHECK((setAfter.value().count(docId2) == 0));

    // Index doc2
    auto indexResult2 = fix.repository_->indexDocumentContent(
        docId2, "Doc Two", "Second document content", "text/plain");
    REQUIRE((indexResult2.has_value()));

    // Set should contain both
    auto setFinal = fix.repository_->getFts5IndexedRowIdSet();
    REQUIRE((setFinal.has_value()));
    CHECK((setFinal.value().count(docId1) == 1));
    CHECK((setFinal.value().count(docId2) == 1));
}

TEST_CASE("MetadataRepository: search functionality", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_test";
    docInfo.fileName = "search.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    auto indexResult = fix.repository_->indexDocumentContent(
        docId, "Test Document", "This is a test document with searchable content", "text/plain");
    REQUIRE((indexResult.has_value()));

    auto searchResult = fix.repository_->search("test", 10, 0);
    REQUIRE((searchResult.has_value()));

    auto results = searchResult.value();
    CHECK((results.results.size() > 0));
}

TEST_CASE("MetadataRepository: search totalCount honors docIds filter",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc1 = makeDocumentWithPath("/tmp/doc_count_1.txt", "search_doc_count_1");
    auto doc2 = makeDocumentWithPath("/tmp/doc_count_2.txt", "search_doc_count_2");

    auto insert1 = fix.repository_->insertDocument(doc1);
    auto insert2 = fix.repository_->insertDocument(doc2);
    REQUIRE((insert1.has_value()));
    REQUIRE((insert2.has_value()));

    auto docId1 = insert1.value();
    auto docId2 = insert2.value();

    REQUIRE(fix.repository_
                ->indexDocumentContent(docId1, "Doc One", "needle token appears in doc one",
                                       "text/plain")
                .has_value());
    REQUIRE(fix.repository_
                ->indexDocumentContent(docId2, "Doc Two", "needle token appears in doc two",
                                       "text/plain")
                .has_value());

    const std::optional<std::vector<int64_t>> scopedDocIds = std::vector<int64_t>{docId1};
    auto searchResult = fix.repository_->search("needle", 1, 0, scopedDocIds);
    REQUIRE((searchResult.has_value()));

    const auto& results = searchResult.value();
    REQUIRE((results.results.size() == 1));
    CHECK((results.results.front().document.id == docId1));
    CHECK((results.totalCount == 1));
}

TEST_CASE("MetadataRepository: fuzzy search returns content matches",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath(
        "/notes/fuzzy_content.txt",
        "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");

    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    constexpr char kRareTerm[] = "blorptastic";
    std::string contentText = std::string("The ") + kRareTerm;

    DocumentContent content;
    content.documentId = docId;
    content.contentText = contentText;
    content.contentLength = static_cast<int64_t>(contentText.length());
    content.extractionMethod = "test";
    content.language = "en";
    auto contentInsertResult = fix.repository_->insertContent(content);
    REQUIRE((contentInsertResult.has_value()));

    auto indexResult =
        fix.repository_->indexDocumentContent(docId, doc.fileName, contentText, "text/plain");
    REQUIRE((indexResult.has_value()));

    auto fuzzyResult = fix.repository_->fuzzySearch(kRareTerm, 0.6f, 10);
    REQUIRE((fuzzyResult.has_value()));

    const auto& searchResults = fuzzyResult.value();
    REQUIRE((searchResults.results.size() == 1));
    CHECK((searchResults.results.front().document.id == docId));
}

TEST_CASE("MetadataRepository: keyword search does not implicitly run fuzzy fallback",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath(
        "/notes/keyword_no_fuzzy_fallback.txt",
        "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");

    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    constexpr char kIndexedTerm[] = "blorptastic";
    std::string contentText = std::string("The ") + kIndexedTerm;

    DocumentContent content;
    content.documentId = docId;
    content.contentText = contentText;
    content.contentLength = static_cast<int64_t>(contentText.length());
    content.extractionMethod = "test";
    content.language = "en";
    REQUIRE((fix.repository_->insertContent(content).has_value()));

    REQUIRE(fix.repository_->indexDocumentContent(docId, doc.fileName, contentText, "text/plain")
                .has_value());
    fix.repository_->addSymSpellTerm(kIndexedTerm, 1);

    auto fuzzyResult = fix.repository_->fuzzySearch("blorptastik", 0.6f, 10);
    REQUIRE((fuzzyResult.has_value()));
    REQUIRE((fuzzyResult.value().results.size() == 1));
    CHECK((fuzzyResult.value().results.front().document.id == docId));

    auto keywordResult = fix.repository_->search("blorptastik", 10);
    REQUIRE((keywordResult.has_value()));
    CHECK((keywordResult.value().results.empty()));
    CHECK((keywordResult.value().totalCount == 0));
}

TEST_CASE(
    "MetadataRepository: concurrent SymSpell writes use coordinator without blocking fuzzy reads",
    "[unit][metadata][repository][symspell]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath(
        "/notes/symspell_lock_storm.txt",
        "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEF");
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    const auto docId = insertResult.value();

    const std::string indexedTerm = "symstormtarget";
    const std::string contentText = std::string("The ") + indexedTerm;
    REQUIRE((fix.repository_->indexDocumentContent(docId, doc.fileName, contentText, "text/plain")
                 .has_value()));

    boost::asio::io_context io;
    yams::daemon::WriteCoordinator::Config config;
    config.maxBatchSize = 8;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 512;
    auto repoRef =
        std::shared_ptr<MetadataRepository>(fix.repository_.get(), [](MetadataRepository*) {});
    yams::daemon::WriteCoordinator coordinator(io, {}, repoRef, config);
    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&coordinator, worker, indexedTerm] {
            for (int i = 0; i < 50; ++i) {
                (void)worker;
                (void)i;
                coordinator.enqueue(makeSymSpellBatch("test/symspell_lock_storm", indexedTerm));
            }
        });
    }
    for (int worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&] {
            for (int i = 0; i < 50; ++i) {
                auto result = fix.repository_->fuzzySearch(indexedTerm, 0.6f, 5);
                if (!result || result.value().results.empty()) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }

    REQUIRE((flushResult.has_value()));
    CHECK_FALSE((failed.load(std::memory_order_relaxed)));
    CHECK((coordinator.getStats().symSpellTermsAdded >= 200));

    auto fuzzyResult = fix.repository_->fuzzySearch("symstormtargat", 0.6f, 10);
    REQUIRE((fuzzyResult.has_value()));
    REQUIRE((fuzzyResult.value().results.size() == 1));
    CHECK((fuzzyResult.value().results.front().document.id == docId));
}

TEST_CASE("MetadataRepository: coordinator coalesces duplicate SymSpell terms preserving "
          "frequency",
          "[unit][metadata][repository][symspell]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath(
        "/notes/symspell_coalesce.txt",
        "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE70");
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    const auto docId = insertResult.value();

    const std::string indexedTerm = "coalescestormtarget";
    const std::string contentText = std::string("The ") + indexedTerm;
    REQUIRE((fix.repository_->indexDocumentContent(docId, doc.fileName, contentText, "text/plain")
                 .has_value()));

    boost::asio::io_context io;
    yams::daemon::WriteCoordinator::Config config;
    config.maxBatchSize = 8;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 512;
    auto repoRef =
        std::shared_ptr<MetadataRepository>(fix.repository_.get(), [](MetadataRepository*) {});
    yams::daemon::WriteCoordinator coordinator(io, {}, repoRef, config);
    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    constexpr std::uint64_t kDuplicateBatches = 300;
    for (std::uint64_t i = 0; i < kDuplicateBatches; ++i) {
        coordinator.enqueue(makeSymSpellBatch("test/symspell_coalesce", indexedTerm));
    }

    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }

    REQUIRE((flushResult.has_value()));
    CHECK((coordinator.getStats().symSpellTermsAdded == kDuplicateBatches));

    auto fuzzyResult = fix.repository_->fuzzySearch("coalescestormtargat", 0.6f, 10);
    REQUIRE((fuzzyResult.has_value()));
    REQUIRE((fuzzyResult.value().results.size() == 1));
    CHECK((fuzzyResult.value().results.front().document.id == docId));
}

TEST_CASE("MetadataRepository: SymSpell terms spanning write chunk boundaries are all indexed",
          "[unit][metadata][repository][symspell]") {
    MetadataRepositoryFixture fix;

    struct Target {
        std::string term;
        std::string typo;
        std::string path;
        std::string hashSuffix;
        int64_t docId = 0;
    };
    std::vector<Target> targets = {
        {"alphachunkzero", "alphachunkzera", "/notes/chunk_alpha.txt", "71", 0},
        {"betachunkmiddle", "betachunkmiddel", "/notes/chunk_beta.txt", "72", 0},
        {"gammachunklast", "gammachunklasd", "/notes/chunk_gamma.txt", "73", 0},
    };

    for (auto& target : targets) {
        auto doc = makeDocumentWithPath(
            target.path,
            std::string("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE") +
                target.hashSuffix);
        auto insertResult = fix.repository_->insertDocument(doc);
        REQUIRE((insertResult.has_value()));
        target.docId = insertResult.value();
        const std::string contentText = std::string("The ") + target.term;
        REQUIRE((fix.repository_
                     ->indexDocumentContent(target.docId, doc.fileName, contentText, "text/plain")
                     .has_value()));
    }

    constexpr std::size_t kTotalTerms = 520;
    std::vector<std::pair<std::string, int64_t>> termPairs;
    termPairs.reserve(kTotalTerms);
    for (std::size_t i = 0; i < kTotalTerms; ++i) {
        termPairs.emplace_back("fillerchunkterm" + std::to_string(i), 1);
    }
    termPairs[0] = {targets[0].term, 1};
    termPairs[511] = {targets[1].term, 1};
    termPairs[519] = {targets[2].term, 1};

    auto addTermsResult = fix.repository_->tryAddSymSpellTerms(termPairs);
    REQUIRE((addTermsResult.has_value()));

    for (const auto& target : targets) {
        auto fuzzyResult = fix.repository_->fuzzySearch(target.typo, 0.6f, 10);
        REQUIRE((fuzzyResult.has_value()));
        REQUIRE((fuzzyResult.value().results.size() == 1));
        CHECK((fuzzyResult.value().results.front().document.id == target.docId));
    }
}

TEST_CASE("MetadataRepository: search sanitizes snippet UTF-8", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_bad_utf8";
    docInfo.fileName = "bad_utf8.txt";
    docInfo.fileSize = 32;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE((createResult.has_value()));
    auto docId = createResult.value();

    std::string badContent = "alpha";
    badContent.push_back(static_cast<char>(0xFF));
    badContent += "beta";

    auto indexResult =
        fix.repository_->indexDocumentContent(docId, "Bad UTF-8", badContent, "text/plain");
    REQUIRE((indexResult.has_value()));

    auto searchResult = fix.repository_->search("alpha", 10, 0);
    REQUIRE((searchResult.has_value()));
    const auto& results = searchResult.value();
    REQUIRE_FALSE(results.results.empty());

    bool matchedDocument = false;
    for (const auto& item : results.results) {
        if (item.document.sha256Hash == "search_bad_utf8") {
            matchedDocument = true;
            auto sanitized = common::sanitizeUtf8(item.snippet);
            CHECK((item.snippet == sanitized));
            CHECK((item.snippet.find(static_cast<char>(0xFF)) == std::string::npos));
        }
    }

    CHECK((matchedDocument));
}

TEST_CASE("MetadataRepository: counts and modified since", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    using clock = std::chrono::system_clock;
    auto now = std::chrono::floor<std::chrono::seconds>(clock::now());

    auto d1 = makeDocumentWithPath("/tmp/a.txt", "H-A");
    d1.extractionStatus = ExtractionStatus::Success;
    d1.contentExtracted = true;
    d1.modifiedTime = now - std::chrono::hours(2);

    auto d2 = makeDocumentWithPath("/tmp/b.txt", "H-B");
    d2.extractionStatus = ExtractionStatus::Pending;
    d2.contentExtracted = false;
    d2.modifiedTime = now - std::chrono::seconds(10);

    auto d3 = makeDocumentWithPath("/tmp/c.txt", "H-C");
    d3.extractionStatus = ExtractionStatus::Failed;
    d3.contentExtracted = true;
    d3.modifiedTime = now - std::chrono::hours(3);

    REQUIRE((fix.repository_->insertDocument(d1).has_value()));
    REQUIRE((fix.repository_->insertDocument(d2).has_value()));
    REQUIRE((fix.repository_->insertDocument(d3).has_value()));

    auto total = fix.repository_->getDocumentCount();
    REQUIRE((total.has_value()));
    CHECK((total.value() >= 3));

    auto extracted = fix.repository_->getContentExtractedDocumentCount();
    REQUIRE((extracted.has_value()));
    CHECK((extracted.value() >= 2));

    auto c_pending = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Pending);
    auto c_success = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Success);
    auto c_failed = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Failed);
    REQUIRE((c_pending.has_value()));
    REQUIRE((c_success.has_value()));
    REQUIRE((c_failed.has_value()));
    CHECK((c_pending.value() >= 1));
    CHECK((c_success.value() >= 1));
    CHECK((c_failed.value() >= 1));

    auto since = clock::now() - std::chrono::seconds(60);
    auto modifiedRes = fix.repository_->findDocumentsModifiedSince(since);
    REQUIRE((modifiedRes.has_value()));
    bool foundRecent = false;
    for (const auto& doc : modifiedRes.value()) {
        if (doc.sha256Hash == d2.sha256Hash) {
            foundRecent = true;
            break;
        }
    }
    CHECK((foundRecent));
}

TEST_CASE("MetadataRepository: path tree upsert creates nodes and counts",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/src/example.txt", "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE((docInsert.has_value()));
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto upsert = fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    REQUIRE((upsert.has_value()));

    auto lookup = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE((lookup.has_value()));
    REQUIRE((lookup.value().has_value()));
    auto node = lookup.value().value();
    CHECK((node.fullPath == "/src"));
    CHECK((node.docCount == 1));

    auto repeat = fix.repository_->upsertPathTreeForDocument(docInfo, docId, false, {});
    REQUIRE((repeat.has_value()));
    auto afterRepeat = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE((afterRepeat.has_value()));
    REQUIRE((afterRepeat.value().has_value()));
    CHECK((afterRepeat.value()->docCount == 1));

    auto fullLookup = fix.repository_->findPathTreeNodeByFullPath("/src/example.txt");
    REQUIRE((fullLookup.has_value()));
    REQUIRE((fullLookup.value().has_value()));
    CHECK((fullLookup.value()->fullPath == "/src/example.txt"));
}

TEST_CASE("MetadataRepository: path tree centroid accumulates embeddings",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/src/lib/foo.cpp", "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE((docInsert.has_value()));
    auto docId = docInsert.value();
    docInfo.id = docId;

    REQUIRE((fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {}).has_value()));

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    REQUIRE(
        fix.repository_
            ->upsertPathTreeForDocument(docInfo, docId, false,
                                        std::span<const float>(embedding.data(), embedding.size()))
            .has_value());

    auto node = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE((node.has_value()));
    REQUIRE((node.value().has_value()));
    CHECK((node.value()->centroidWeight == 1));
}

TEST_CASE("MetadataRepository: path tree upsert rolls back partial changes on failure",
          "[unit][metadata][repository][path-tree][atomicity]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/atomic/lib/file.txt", "ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE((docInsert.has_value()));
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto installTrigger = fix.pool_->withConnection([&](Database& db) -> Result<void> {
        return db.execute("CREATE TRIGGER abort_path_tree_upsert_leaf_centroid "
                          "BEFORE UPDATE OF centroid_weight ON path_tree_nodes "
                          "WHEN NEW.full_path = '/atomic/lib/file.txt' "
                          "AND NEW.centroid_weight = OLD.centroid_weight + 1 "
                          "BEGIN "
                          "SELECT RAISE(ABORT, 'injected path tree upsert failure'); "
                          "END");
    });
    REQUIRE((installTrigger.has_value()));

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    auto upsert = fix.repository_->upsertPathTreeForDocument(
        docInfo, docId, true, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE_FALSE((upsert.has_value()));

    auto rootNode = fix.repository_->findPathTreeNodeByFullPath("/atomic");
    REQUIRE((rootNode.has_value()));
    CHECK_FALSE((rootNode.value().has_value()));

    auto libNode = fix.repository_->findPathTreeNodeByFullPath("/atomic/lib");
    REQUIRE((libNode.has_value()));
    CHECK_FALSE((libNode.value().has_value()));

    auto leafNode = fix.repository_->findPathTreeNodeByFullPath("/atomic/lib/file.txt");
    REQUIRE((leafNode.has_value()));
    CHECK_FALSE((leafNode.value().has_value()));

    auto assocCount = fix.pool_->withConnection([&](Database& db) -> Result<int64_t> {
        auto stmtResult =
            db.prepare("SELECT COUNT(*) FROM path_tree_node_documents WHERE document_id = ?");
        if (!stmtResult) {
            return stmtResult.error();
        }

        auto stmt = std::move(stmtResult).value();
        if (auto bindDoc = stmt.bind(1, docId); !bindDoc) {
            return bindDoc.error();
        }

        auto stepResult = stmt.step();
        if (!stepResult) {
            return stepResult.error();
        }
        if (!stepResult.value()) {
            return int64_t{0};
        }
        return stmt.getInt64(0);
    });
    REQUIRE((assocCount.has_value()));
    CHECK((assocCount.value() == 0));
}

TEST_CASE("MetadataRepository: remove path tree decrements counts and deletes empty nodes",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo =
        makeDocumentWithPath("/project/src/lib/util.cpp",
                             "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE((docInsert.has_value()));
    auto docId = docInsert.value();
    docInfo.id = docId;

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    auto upsert = fix.repository_->upsertPathTreeForDocument(
        docInfo, docId, true, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE((upsert.has_value()));

    auto leafNode = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib/util.cpp");
    REQUIRE((leafNode.has_value()));
    REQUIRE((leafNode.value().has_value()));
    CHECK((leafNode.value()->docCount == 1));
    CHECK((leafNode.value()->centroidWeight == 1));

    auto libNode = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib");
    REQUIRE((libNode.has_value()));
    REQUIRE((libNode.value().has_value()));
    CHECK((libNode.value()->docCount == 1));

    auto remove = fix.repository_->removePathTreeForDocument(
        docInfo, docId, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE((remove.has_value()));

    auto afterRemoveLeaf = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib/util.cpp");
    REQUIRE((afterRemoveLeaf.has_value()));
    CHECK_FALSE(afterRemoveLeaf.value().has_value());

    auto afterRemoveLib = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib");
    REQUIRE((afterRemoveLib.has_value()));
    CHECK_FALSE(afterRemoveLib.value().has_value());
}

TEST_CASE("MetadataRepository: remove path tree recalculates centroid",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc1 = makeDocumentWithPath(
        "/shared/file1.txt", "1111111111111111111111111111111111111111111111111111111111111111");
    auto doc1Insert = fix.repository_->insertDocument(doc1);
    REQUIRE((doc1Insert.has_value()));
    doc1.id = doc1Insert.value();

    auto doc2 = makeDocumentWithPath(
        "/shared/file2.txt", "2222222222222222222222222222222222222222222222222222222222222222");
    auto doc2Insert = fix.repository_->insertDocument(doc2);
    REQUIRE((doc2Insert.has_value()));
    doc2.id = doc2Insert.value();

    std::vector<float> emb1{1.0F, 0.0F, 0.0F};
    std::vector<float> emb2{0.0F, 1.0F, 0.0F};

    auto upsert1 = fix.repository_->upsertPathTreeForDocument(
        doc1, doc1.id, true, std::span<const float>(emb1.data(), emb1.size()));
    REQUIRE((upsert1.has_value()));

    auto upsert2 = fix.repository_->upsertPathTreeForDocument(
        doc2, doc2.id, true, std::span<const float>(emb2.data(), emb2.size()));
    REQUIRE((upsert2.has_value()));

    auto sharedNode = fix.repository_->findPathTreeNodeByFullPath("/shared");
    REQUIRE((sharedNode.has_value()));
    REQUIRE((sharedNode.value().has_value()));
    CHECK((sharedNode.value()->docCount == 2));
    CHECK((sharedNode.value()->centroidWeight == 2));

    auto remove1 = fix.repository_->removePathTreeForDocument(
        doc1, doc1.id, std::span<const float>(emb1.data(), emb1.size()));
    REQUIRE((remove1.has_value()));

    auto afterRemove = fix.repository_->findPathTreeNodeByFullPath("/shared");
    REQUIRE((afterRemove.has_value()));
    REQUIRE((afterRemove.value().has_value()));
    CHECK((afterRemove.value()->docCount == 1));
    CHECK((afterRemove.value()->centroidWeight == 1));
}

TEST_CASE("MetadataRepository: path tree child listing and repair helpers",
          "[unit][metadata][repository][path-tree]") {
    MetadataRepositoryFixture fix;

    auto missingDoc = makeDocumentWithPath("/repair/missing.txt", "path-tree-missing-hash");
    auto treeDocA = makeDocumentWithPath("/repair/tree/a.txt", "path-tree-a-hash");
    auto treeDocB = makeDocumentWithPath("/repair/tree/b.txt", "path-tree-b-hash");

    auto missingId = fix.repository_->insertDocument(missingDoc);
    auto treeAId = fix.repository_->insertDocument(treeDocA);
    auto treeBId = fix.repository_->insertDocument(treeDocB);
    REQUIRE((missingId.has_value()));
    REQUIRE((treeAId.has_value()));
    REQUIRE((treeBId.has_value()));

    treeDocA.id = treeAId.value();
    treeDocB.id = treeBId.value();
    REQUIRE(
        fix.repository_->upsertPathTreeForDocument(treeDocA, treeDocA.id, true, {}).has_value());
    REQUIRE(
        fix.repository_->upsertPathTreeForDocument(treeDocB, treeDocB.id, true, {}).has_value());

    auto missingCount = fix.repository_->countDocsMissingPathTree();
    REQUIRE((missingCount.has_value()));
    CHECK((missingCount.value() == 1));

    auto missingDocs = fix.repository_->findDocsMissingPathTree(5);
    REQUIRE((missingDocs.has_value()));
    REQUIRE((missingDocs.value().size() == 1));
    CHECK((missingDocs.value().front().sha256Hash == missingDoc.sha256Hash));

    auto rootChildren = fix.repository_->listPathTreeChildren("/", 10);
    REQUIRE((rootChildren.has_value()));
    auto repairIt =
        std::find_if(rootChildren.value().begin(), rootChildren.value().end(),
                     [](const PathTreeNode& node) { return node.fullPath == "/repair"; });
    REQUIRE((repairIt != rootChildren.value().end()));
    CHECK((repairIt->docCount == 2));

    auto repairChildren = fix.repository_->listPathTreeChildren("/repair", 10);
    REQUIRE((repairChildren.has_value()));
    REQUIRE((repairChildren.value().size() == 1));
    CHECK((repairChildren.value().front().fullPath == "/repair/tree"));
    CHECK((repairChildren.value().front().docCount == 2));

    auto limitedChildren = fix.repository_->listPathTreeChildren("/repair/tree", 1);
    REQUIRE((limitedChildren.has_value()));
    REQUIRE((limitedChildren.value().size() == 1));
    CHECK((limitedChildren.value().front().fullPath == "/repair/tree/a.txt"));

    auto missingParentChildren = fix.repository_->listPathTreeChildren("/repair/missing.txt", 10);
    REQUIRE((missingParentChildren.has_value()));
    CHECK((missingParentChildren.value().empty()));

    auto singleConnDbPath = tempDbPath("metadata_repo_single_conn_path_tree_");
    ConnectionPoolConfig singleConnConfig;
    singleConnConfig.minConnections = 1;
    singleConnConfig.maxConnections = 1;
    auto singleConnPool =
        std::make_unique<ConnectionPool>(singleConnDbPath.string(), singleConnConfig);
    REQUIRE((singleConnPool->initialize().has_value()));
    auto singleConnRepo = std::make_unique<MetadataRepository>(
        *singleConnPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    auto singleConnDoc =
        makeDocumentWithPath("/single/connection/child.txt", "single-connection-path-tree-hash");
    auto singleConnInsert = singleConnRepo->insertDocument(singleConnDoc);
    REQUIRE((singleConnInsert.has_value()));
    singleConnDoc.id = singleConnInsert.value();
    REQUIRE((singleConnRepo->upsertPathTreeForDocument(singleConnDoc, singleConnDoc.id, true, {})
                 .has_value()));

    auto singleConnChildren = singleConnRepo->listPathTreeChildren("/single/connection", 10);
    REQUIRE((singleConnChildren.has_value()));
    REQUIRE((singleConnChildren.value().size() == 1));
    CHECK((singleConnChildren.value().front().fullPath == "/single/connection/child.txt"));

    singleConnRepo.reset();
    singleConnPool->shutdown();
    singleConnPool.reset();
    {
        std::error_code ec;
        std::filesystem::remove(singleConnDbPath, ec);
    }

    auto prefixDocs = fix.repository_->findDocumentsByPathTreePrefix("/repair/tree", true, 10);
    REQUIRE((prefixDocs.has_value()));
    std::vector<std::string> hashes;
    for (const auto& doc : prefixDocs.value()) {
        hashes.push_back(doc.sha256Hash);
    }
    CHECK((std::find(hashes.begin(), hashes.end(), treeDocA.sha256Hash) != hashes.end()));
    CHECK((std::find(hashes.begin(), hashes.end(), treeDocB.sha256Hash) != hashes.end()));
    CHECK((std::find(hashes.begin(), hashes.end(), missingDoc.sha256Hash) == hashes.end()));
}

TEST_CASE("MetadataRepository: direct path tree helpers round-trip",
          "[unit][metadata][repository][path-tree][helpers]") {
    MetadataRepositoryFixture fix;

    auto rootNode =
        fix.repository_->insertPathTreeNode(PathTreeNode::kNullParent, "manual", "/manual");
    REQUIRE((rootNode.has_value()));
    CHECK((rootNode.value().fullPath == "/manual"));
    CHECK((rootNode.value().parentId == PathTreeNode::kNullParent));

    auto childNode =
        fix.repository_->insertPathTreeNode(rootNode.value().id, "child", "/manual/child");
    REQUIRE((childNode.has_value()));
    CHECK((childNode.value().parentId == rootNode.value().id));
    CHECK((childNode.value().fullPath == "/manual/child"));

    auto rootLookup = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "manual");
    REQUIRE((rootLookup.has_value()));
    REQUIRE((rootLookup.value().has_value()));
    CHECK((rootLookup.value()->id == rootNode.value().id));

    auto childLookup = fix.repository_->findPathTreeNode(rootNode.value().id, "child");
    REQUIRE((childLookup.has_value()));
    REQUIRE((childLookup.value().has_value()));
    CHECK((childLookup.value()->id == childNode.value().id));

    auto missingLookup = fix.repository_->findPathTreeNode(rootNode.value().id, "missing");
    REQUIRE((missingLookup.has_value()));
    CHECK_FALSE((missingLookup.value().has_value()));

    auto emptyPathLookup = fix.repository_->findPathTreeNodeByFullPath("");
    REQUIRE((emptyPathLookup.has_value()));
    CHECK_FALSE((emptyPathLookup.value().has_value()));

    auto doc = makeDocumentWithPath("/manual/document.txt", "path-tree-helper-doc");
    auto docId = fix.repository_->insertDocument(doc);
    REQUIRE((docId.has_value()));

    REQUIRE((fix.repository_->incrementPathTreeDocCount(rootNode.value().id, docId.value())
                 .has_value()));
    REQUIRE((fix.repository_->incrementPathTreeDocCount(rootNode.value().id, docId.value())
                 .has_value()));

    auto rootAfterIncrement = fix.repository_->findPathTreeNodeByFullPath("/manual");
    REQUIRE((rootAfterIncrement.has_value()));
    REQUIRE((rootAfterIncrement.value().has_value()));
    CHECK((rootAfterIncrement.value()->docCount == 1));

    auto rootChildrenNoLimit = fix.repository_->listPathTreeChildren("", 0);
    REQUIRE((rootChildrenNoLimit.has_value()));
    auto manualIt =
        std::find_if(rootChildrenNoLimit.value().begin(), rootChildrenNoLimit.value().end(),
                     [](const PathTreeNode& node) { return node.fullPath == "/manual"; });
    REQUIRE((manualIt != rootChildrenNoLimit.value().end()));
    CHECK((manualIt->docCount == 1));
}

TEST_CASE("MetadataRepository: direct path tree centroid helper handles reset and missing node",
          "[unit][metadata][repository][path-tree][helpers]") {
    MetadataRepositoryFixture fix;

    auto node = fix.repository_->insertPathTreeNode(PathTreeNode::kNullParent, "embed", "/embed");
    REQUIRE((node.has_value()));

    REQUIRE((fix.repository_->accumulatePathTreeCentroid(node.value().id, std::span<const float>{})
                 .has_value()));

    auto afterEmpty = fix.repository_->findPathTreeNodeByFullPath("/embed");
    REQUIRE((afterEmpty.has_value()));
    REQUIRE((afterEmpty.value().has_value()));
    CHECK((afterEmpty.value()->centroidWeight == 0));
    CHECK((afterEmpty.value()->centroid.empty()));

    const std::vector<float> firstEmbedding{1.0F, 2.0F, 3.0F};
    REQUIRE((fix.repository_
                 ->accumulatePathTreeCentroid(
                     node.value().id,
                     std::span<const float>(firstEmbedding.data(), firstEmbedding.size()))
                 .has_value()));

    auto afterFirst = fix.repository_->findPathTreeNodeByFullPath("/embed");
    REQUIRE((afterFirst.has_value()));
    REQUIRE((afterFirst.value().has_value()));
    CHECK((afterFirst.value()->centroidWeight == 1));
    CHECK((afterFirst.value()->centroid == firstEmbedding));

    const std::vector<float> secondEmbedding{4.0F, 5.0F, 6.0F};
    REQUIRE((fix.repository_
                 ->accumulatePathTreeCentroid(
                     node.value().id,
                     std::span<const float>(secondEmbedding.data(), secondEmbedding.size()))
                 .has_value()));

    auto afterSecond = fix.repository_->findPathTreeNodeByFullPath("/embed");
    REQUIRE((afterSecond.has_value()));
    REQUIRE((afterSecond.value().has_value()));
    CHECK((afterSecond.value()->centroidWeight == 2));
    REQUIRE((afterSecond.value()->centroid.size() == 3));
    CHECK_THAT(afterSecond.value()->centroid[0], Catch::Matchers::WithinRel(2.5F, 0.01F));
    CHECK_THAT(afterSecond.value()->centroid[1], Catch::Matchers::WithinRel(3.5F, 0.01F));
    CHECK_THAT(afterSecond.value()->centroid[2], Catch::Matchers::WithinRel(4.5F, 0.01F));

    const std::vector<std::byte> corruptBlob{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
    REQUIRE((overwritePathTreeCentroid(*fix.pool_, "/embed", corruptBlob, 5).has_value()));

    const std::vector<float> resetEmbedding{9.0F, 10.0F, 11.0F};
    REQUIRE((fix.repository_
                 ->accumulatePathTreeCentroid(
                     node.value().id,
                     std::span<const float>(resetEmbedding.data(), resetEmbedding.size()))
                 .has_value()));

    auto afterReset = fix.repository_->findPathTreeNodeByFullPath("/embed");
    REQUIRE((afterReset.has_value()));
    REQUIRE((afterReset.value().has_value()));
    CHECK((afterReset.value()->centroidWeight == 1));
    CHECK((afterReset.value()->centroid == resetEmbedding));

    auto missingNodeResult = fix.repository_->accumulatePathTreeCentroid(
        999999, std::span<const float>(resetEmbedding.data(), resetEmbedding.size()));
    REQUIRE_FALSE((missingNodeResult.has_value()));
    CHECK((missingNodeResult.error().message.find("not found") != std::string::npos));
}

TEST_CASE("MetadataRepository: path tree supports relative document paths",
          "[unit][metadata][repository][path-tree][relative]") {
    MetadataRepositoryFixture fix;

    auto storedDoc = makeDocumentWithPath("/backing/relative-source.txt", "path-tree-relative-doc");
    auto docInsert = fix.repository_->insertDocument(storedDoc);
    REQUIRE((docInsert.has_value()));
    storedDoc.id = docInsert.value();

    auto relativePathInfo = storedDoc;
    relativePathInfo.filePath = "relative/tree/file.txt";

    REQUIRE((fix.repository_->upsertPathTreeForDocument(relativePathInfo, storedDoc.id, true, {})
                 .has_value()));

    auto relativeRoot = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "relative");
    REQUIRE((relativeRoot.has_value()));
    REQUIRE((relativeRoot.value().has_value()));
    CHECK((relativeRoot.value()->fullPath == "relative"));
    CHECK((relativeRoot.value()->docCount == 1));

    auto relativeChild = fix.repository_->findPathTreeNodeByFullPath("relative/tree");
    REQUIRE((relativeChild.has_value()));
    REQUIRE((relativeChild.value().has_value()));
    CHECK((relativeChild.value()->docCount == 1));

    auto relativeChildren = fix.repository_->listPathTreeChildren("relative", 0);
    REQUIRE((relativeChildren.has_value()));
    REQUIRE((relativeChildren.value().size() == 1));
    CHECK((relativeChildren.value().front().fullPath == "relative/tree"));

    REQUIRE((fix.repository_->removePathTreeForDocument(relativePathInfo, storedDoc.id, {})
                 .has_value()));

    auto removedLeaf = fix.repository_->findPathTreeNodeByFullPath("relative/tree/file.txt");
    REQUIRE((removedLeaf.has_value()));
    CHECK_FALSE((removedLeaf.value().has_value()));

    auto preservedRoot = fix.repository_->findPathTreeNodeByFullPath("relative");
    REQUIRE((preservedRoot.has_value()));
    REQUIRE((preservedRoot.value().has_value()));
    CHECK((preservedRoot.value()->docCount == 0));
}

TEST_CASE("MetadataRepository: tree snapshots and changes round-trip",
          "[unit][metadata][repository][tree-diff]") {
    MetadataRepositoryFixture fix;

    TreeSnapshotRecord base;
    base.snapshotId = "snapshot-base";
    base.rootTreeHash = "root-base";
    base.createdTime = 100;
    base.fileCount = 1;
    base.metadata["directory_path"] = "/repo";
    base.metadata["snapshot_label"] = "base";
    base.metadata["git_commit"] = "abc123";
    base.metadata["git_branch"] = "main";
    base.metadata["git_remote"] = "origin";

    TreeSnapshotRecord target = base;
    target.snapshotId = "snapshot-target";
    target.rootTreeHash = "root-target";
    target.createdTime = 200;
    target.fileCount = 2;
    target.metadata["snapshot_label"] = "target";
    target.metadata["git_commit"] = "def456";

    REQUIRE((fix.repository_->upsertTreeSnapshot(base).has_value()));
    REQUIRE((fix.repository_->upsertTreeSnapshot(target).has_value()));

    auto snapshots = fix.repository_->listTreeSnapshots(10);
    REQUIRE((snapshots.has_value()));
    REQUIRE((snapshots.value().size() >= 2));
    CHECK((snapshots.value().front().snapshotId == "snapshot-target"));
    CHECK((snapshots.value().front().metadata.at("snapshot_label") == "target"));
    CHECK((snapshots.value().front().metadata.at("git_commit") == "def456"));
    CHECK((snapshots.value().front().fileCount == 2));

    TreeDiffDescriptor descriptor;
    descriptor.baseSnapshotId = base.snapshotId;
    descriptor.targetSnapshotId = target.snapshotId;
    descriptor.computedAt = 300;
    descriptor.status = "pending";
    auto diffId = fix.repository_->beginTreeDiff(descriptor);
    REQUIRE((diffId.has_value()));
    REQUIRE((diffId.value() > 0));

    TreeChangeRecord added;
    added.type = TreeChangeType::Added;
    added.newPath = "/repo/src/new.cpp";
    added.newHash = "new-hash";
    added.mode = 0644;
    added.contentDeltaHash = "delta-hash";

    TreeChangeRecord moved;
    moved.type = TreeChangeType::Moved;
    moved.oldPath = "/repo/src/old.cpp";
    moved.newPath = "/repo/include/old.hpp";
    moved.oldHash = "old-hash";
    moved.newHash = "old-hash";

    TreeChangeRecord deletedDirectory;
    deletedDirectory.type = TreeChangeType::Deleted;
    deletedDirectory.oldPath = "/repo/tmp";
    deletedDirectory.isDirectory = true;

    REQUIRE(fix.repository_->appendTreeChanges(diffId.value(), {added, moved, deletedDirectory})
                .has_value());

    TreeDiffQuery allChanges;
    allChanges.baseSnapshotId = base.snapshotId;
    allChanges.targetSnapshotId = target.snapshotId;
    allChanges.limit = 10;
    auto changes = fix.repository_->listTreeChanges(allChanges);
    REQUIRE((changes.has_value()));
    REQUIRE((changes.value().size() == 3));
    CHECK((changes.value()[0].type == TreeChangeType::Added));
    CHECK((changes.value()[1].type == TreeChangeType::Moved));
    CHECK((changes.value()[2].type == TreeChangeType::Deleted));

    TreeDiffQuery addedUnderSrc;
    addedUnderSrc.baseSnapshotId = base.snapshotId;
    addedUnderSrc.targetSnapshotId = target.snapshotId;
    addedUnderSrc.pathPrefix = "/repo/src";
    addedUnderSrc.typeFilter = TreeChangeType::Added;
    auto filtered = fix.repository_->listTreeChanges(addedUnderSrc);
    REQUIRE((filtered.has_value()));
    REQUIRE((filtered.value().size() == 1));
    CHECK((filtered.value().front().newPath == added.newPath));
    REQUIRE((filtered.value().front().mode.has_value()));
    CHECK((filtered.value().front().mode.value() == 0644));
    REQUIRE((filtered.value().front().contentDeltaHash.has_value()));
    CHECK((filtered.value().front().contentDeltaHash.value() == "delta-hash"));

    REQUIRE(fix.repository_->finalizeTreeDiff(diffId.value(), changes.value().size(), "complete")
                .has_value());

    auto verifyStatus = fix.pool_->withConnection([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare(R"(
            SELECT files_added, files_deleted, files_modified, files_renamed, status
            FROM tree_diffs WHERE diff_id = ?
        )");
        REQUIRE((stmtResult.has_value()));
        auto& stmt = stmtResult.value();
        REQUIRE((stmt.bind(1, diffId.value()).has_value()));
        auto step = stmt.step();
        REQUIRE((step.has_value()));
        REQUIRE((step.value()));
        CHECK((stmt.getInt64(0) == 1));
        CHECK((stmt.getInt64(1) == 0));
        CHECK((stmt.getInt64(2) == 0));
        CHECK((stmt.getInt64(3) == 1));
        CHECK((stmt.getString(4) == "complete"));
        return Result<void>();
    });
    REQUIRE((verifyStatus.has_value()));
}

// --- batchInsertContentAndIndex counter accuracy tests ---

TEST_CASE("batchInsertContentAndIndex: fresh documents increment extracted and indexed counters",
          "[unit][metadata-repo][batch-content][counters]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    // Insert 3 fresh documents (not yet extracted/indexed)
    std::vector<int64_t> docIds;
    for (int i = 0; i < 3; ++i) {
        DocumentInfo doc;
        doc.sha256Hash = "counter_hash_fresh_" + std::to_string(i);
        doc.fileName = "counter_fresh_" + std::to_string(i) + ".txt";
        doc.fileSize = 100;
        doc.mimeType = "text/plain";
        doc.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.modifiedTime = doc.createdTime;
        // Leave contentExtracted=false, extractionStatus=Pending (defaults)
        doc.contentExtracted = false;
        doc.extractionStatus = ExtractionStatus::Pending;
        auto result = fix.repository_->insertDocument(doc);
        REQUIRE((result.has_value()));
        docIds.push_back(result.value());
    }

    auto extractedBefore = fix.repository_->getCachedExtractedCount();
    auto indexedBefore = fix.repository_->getCachedIndexedCount();

    // Batch insert content for all 3
    std::vector<BatchContentEntry> entries;
    for (int i = 0; i < 3; ++i) {
        BatchContentEntry entry;
        entry.documentId = docIds[static_cast<size_t>(i)];
        entry.title = "Title " + std::to_string(i);
        entry.contentText = "Content text " + std::to_string(i);
        entry.mimeType = "text/plain";
        entry.extractionMethod = "test";
        entry.language = "en";
        entry.priorStateKnown = true;
        entry.priorContentExtracted = false;
        entry.priorExtractionStatus = ExtractionStatus::Pending;
        entries.push_back(entry);
    }
    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    CHECK((fix.repository_->getCachedExtractedCount() == extractedBefore + 3));
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore + 3));

    for (auto docId : docIds) {
        auto docResult = fix.repository_->getDocument(docId);
        REQUIRE((docResult.has_value()));
        REQUIRE((docResult.value().has_value()));
        CHECK((docResult.value()->repairStatus == RepairStatus::Completed));
        CHECK((docResult.value()->repairAttempts == 1));
    }
}

TEST_CASE(
    "batchInsertContentAndIndex: duplicate document entries preserve per-entry repair attempts",
    "[unit][metadata-repo][batch-content][status-update][duplicates]") {
    MetadataRepositoryFixture fix;

    DocumentInfo doc;
    doc.sha256Hash = "counter_hash_duplicate_status";
    doc.fileName = "duplicate_status.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    const auto docId = insertResult.value();

    BatchContentEntry first = makeBatchContentEntry(docId, "First title", "First content");
    BatchContentEntry second = makeBatchContentEntry(docId, "Second title", "Second content");

    auto batchResult = fix.repository_->batchInsertContentAndIndex({first, second});
    REQUIRE((batchResult.has_value()));

    auto docResult = fix.repository_->getDocument(docId);
    REQUIRE((docResult.has_value()));
    REQUIRE((docResult.value().has_value()));
    CHECK((docResult.value()->repairStatus == RepairStatus::Completed));
    CHECK((docResult.value()->repairAttempts == 2));

    auto contentResult = fix.repository_->getContent(docId);
    REQUIRE((contentResult.has_value()));
    REQUIRE((contentResult.value().has_value()));
    CHECK((contentResult.value()->contentText == "Second content"));
}

TEST_CASE("batchInsertContentAndIndex: skips stale missing document ids",
          "[unit][metadata-repo][batch-content][counters][stale]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    const auto docsBefore = fix.repository_->getCachedDocumentCount();
    const auto extractedBefore = fix.repository_->getCachedExtractedCount();
    const auto indexedBefore = fix.repository_->getCachedIndexedCount();

    BatchContentEntry stale;
    stale.documentId = 999999;
    stale.title = "stale title";
    stale.contentText = "stale content";
    stale.mimeType = "text/plain";
    stale.extractionMethod = "test";
    stale.language = "en";

    auto batchResult = fix.repository_->batchInsertContentAndIndex({stale});
    REQUIRE((batchResult.has_value()));

    CHECK((fix.repository_->getCachedDocumentCount() == docsBefore));
    CHECK((fix.repository_->getCachedExtractedCount() == extractedBefore));
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore));

    auto exactIndexed = fix.repository_->getIndexedDocumentCount();
    REQUIRE((exactIndexed.has_value()));
    CHECK((static_cast<uint64_t>(exactIndexed.value()) == indexedBefore));
}

TEST_CASE("batchInsertContentAndIndex: updates warmed FTS indexed ID cache",
          "[unit][metadata-repo][batch-content][fts-cache]") {
    MetadataRepositoryFixture fix;

    std::vector<int64_t> docIds;
    for (int i = 0; i < 3; ++i) {
        DocumentInfo doc;
        doc.sha256Hash = "fts_cache_batch_hash_" + std::to_string(i);
        doc.fileName = "fts_cache_batch_" + std::to_string(i) + ".txt";
        doc.fileSize = 100;
        doc.mimeType = "text/plain";
        doc.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.modifiedTime = doc.createdTime;
        auto result = fix.repository_->insertDocument(doc);
        REQUIRE((result.has_value()));
        docIds.push_back(result.value());
    }

    auto initialIds = fix.repository_->getAllFts5IndexedDocumentIds();
    REQUIRE((initialIds.has_value()));
    REQUIRE((initialIds.value().empty()));

    std::vector<BatchContentEntry> entries;
    for (int i = 0; i < 3; ++i) {
        BatchContentEntry entry;
        entry.documentId = docIds[static_cast<size_t>(i)];
        entry.title = "FTS Cache Title " + std::to_string(i);
        entry.contentText = "FTS cache content " + std::to_string(i);
        entry.mimeType = "text/plain";
        entry.extractionMethod = "test";
        entry.language = "en";
        entries.push_back(entry);
    }

    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    auto indexedIds = fix.repository_->getAllFts5IndexedDocumentIds();
    REQUIRE((indexedIds.has_value()));

    auto actual = indexedIds.value();
    std::sort(actual.begin(), actual.end());
    std::sort(docIds.begin(), docIds.end());
    CHECK((actual == docIds));
}

TEST_CASE("batchInsertContentAndIndex: already-extracted documents don't double-count",
          "[unit][metadata-repo][batch-content][counters]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    DocumentInfo doc;
    doc.sha256Hash = "counter_hash_already";
    doc.fileName = "counter_already.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    // First batch insert — transitions to extracted+indexed
    std::vector<BatchContentEntry> entries = {makeBatchContentEntry(docId, "Title", "Content")};
    auto batch1 = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batch1.has_value()));

    auto extractedAfterFirst = fix.repository_->getCachedExtractedCount();
    auto indexedAfterFirst = fix.repository_->getCachedIndexedCount();

    // Re-insert same document — counters should not change
    auto batch2 = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batch2.has_value()));

    CHECK((fix.repository_->getCachedExtractedCount() == extractedAfterFirst));
    CHECK((fix.repository_->getCachedIndexedCount() == indexedAfterFirst));
}

TEST_CASE("batchInsertContentAndIndex: mixed fresh and already-extracted batch",
          "[unit][metadata-repo][batch-content][counters]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    // Insert 2 documents
    DocumentInfo doc1;
    doc1.sha256Hash = "counter_mixed_1";
    doc1.fileName = "mixed_1.txt";
    doc1.fileSize = 100;
    doc1.mimeType = "text/plain";
    doc1.createdTime = now;
    doc1.modifiedTime = now;
    auto id1 = fix.repository_->insertDocument(doc1);
    REQUIRE((id1.has_value()));

    DocumentInfo doc2;
    doc2.sha256Hash = "counter_mixed_2";
    doc2.fileName = "mixed_2.txt";
    doc2.fileSize = 200;
    doc2.mimeType = "text/plain";
    doc2.createdTime = now;
    doc2.modifiedTime = now;
    auto id2 = fix.repository_->insertDocument(doc2);
    REQUIRE((id2.has_value()));

    // Pre-extract doc1 via batchInsertContentAndIndex so it has extraction_status='Success'
    std::vector<BatchContentEntry> preEntries = {
        makeBatchContentEntry(id1.value(), "Title 1", "Content 1")};
    auto preResult = fix.repository_->batchInsertContentAndIndex(preEntries);
    REQUIRE((preResult.has_value()));

    auto extractedBefore = fix.repository_->getCachedExtractedCount();
    auto indexedBefore = fix.repository_->getCachedIndexedCount();

    // Now batch both: doc1 (already extracted) + doc2 (fresh)
    std::vector<BatchContentEntry> entries = {
        makeBatchContentEntry(id1.value(), "Title 1 updated", "Content 1 updated"),
        makeBatchContentEntry(id2.value(), "Title 2", "Content 2")};
    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    // Only doc2 (fresh) should increment counters
    CHECK((fix.repository_->getCachedExtractedCount() == extractedBefore + 1));
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore + 1));
}

TEST_CASE(
    "batchInsertContentAndIndex: priorStateKnown must not treat extraction success as FTS indexed",
    "[unit][metadata-repo][batch-content][counters][fts]") {
    MetadataRepositoryFixture fix;

    DocumentInfo doc;
    doc.sha256Hash = "counter_hash_missing_fts";
    doc.fileName = "missing_fts.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    BatchContentEntry initialEntry;
    initialEntry.documentId = docId;
    initialEntry.title = "Title";
    initialEntry.contentText = "Content";
    initialEntry.mimeType = "text/plain";
    initialEntry.extractionMethod = "test";
    initialEntry.language = "en";
    REQUIRE((fix.repository_->batchInsertContentAndIndex({initialEntry}).has_value()));

    REQUIRE(fix.pool_
                ->withConnection([&](Database& db) -> Result<void> {
                    auto stmtResult = db.prepare("DELETE FROM documents_fts WHERE rowid = ?");
                    REQUIRE((stmtResult.has_value()));
                    auto& stmt = stmtResult.value();
                    REQUIRE((stmt.bind(1, docId).has_value()));
                    REQUIRE((stmt.execute().has_value()));
                    return {};
                })
                .has_value());

    fix.repository_->initializeCounters();
    auto indexedBefore = fix.repository_->getCachedIndexedCount();
    REQUIRE((indexedBefore == 0));

    BatchContentEntry repairEntry;
    repairEntry.documentId = docId;
    repairEntry.title = "Title";
    repairEntry.contentText = "Content repaired";
    repairEntry.mimeType = "text/plain";
    repairEntry.extractionMethod = "test";
    repairEntry.language = "en";
    repairEntry.priorStateKnown = true;
    repairEntry.priorContentExtracted = true;
    repairEntry.priorExtractionStatus = ExtractionStatus::Success;

    auto batchResult = fix.repository_->batchInsertContentAndIndex({repairEntry});
    REQUIRE((batchResult.has_value()));

    auto indexedIds = fix.repository_->getAllFts5IndexedDocumentIds();
    REQUIRE((indexedIds.has_value()));
    REQUIRE(std::find(indexedIds.value().begin(), indexedIds.value().end(), docId) !=
            indexedIds.value().end());
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore + 1));
}

TEST_CASE("batchInsertContentAndIndex: mixed repaired and stale entries stay accurate",
          "[unit][metadata-repo][batch-content][counters][fts]") {
    MetadataRepositoryFixture fix;

    DocumentInfo doc;
    doc.sha256Hash = "counter_hash_mixed_repair_stale";
    doc.fileName = "mixed_repair_stale.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    BatchContentEntry initialEntry;
    initialEntry.documentId = docId;
    initialEntry.title = "Title";
    initialEntry.contentText = "Content";
    initialEntry.mimeType = "text/plain";
    initialEntry.extractionMethod = "test";
    initialEntry.language = "en";
    REQUIRE((fix.repository_->batchInsertContentAndIndex({initialEntry}).has_value()));

    REQUIRE(fix.pool_
                ->withConnection([&](Database& db) -> Result<void> {
                    auto stmtResult = db.prepare("DELETE FROM documents_fts WHERE rowid = ?");
                    REQUIRE((stmtResult.has_value()));
                    auto& stmt = stmtResult.value();
                    REQUIRE((stmt.bind(1, docId).has_value()));
                    REQUIRE((stmt.execute().has_value()));
                    return {};
                })
                .has_value());

    fix.repository_->initializeCounters();
    auto indexedBefore = fix.repository_->getCachedIndexedCount();
    REQUIRE((indexedBefore == 0));

    BatchContentEntry repairEntry;
    repairEntry.documentId = docId;
    repairEntry.title = "Title repaired";
    repairEntry.contentText = "Content repaired";
    repairEntry.mimeType = "text/plain";
    repairEntry.extractionMethod = "test";
    repairEntry.language = "en";
    repairEntry.priorStateKnown = true;
    repairEntry.priorContentExtracted = true;
    repairEntry.priorExtractionStatus = ExtractionStatus::Success;

    BatchContentEntry staleEntry = repairEntry;
    staleEntry.documentId = docId + 999999;
    staleEntry.title = "Stale";
    staleEntry.contentText = "Stale content";

    auto batchResult = fix.repository_->batchInsertContentAndIndex({repairEntry, staleEntry});
    REQUIRE((batchResult.has_value()));

    auto indexedIds = fix.repository_->getAllFts5IndexedDocumentIds();
    REQUIRE((indexedIds.has_value()));
    REQUIRE(std::find(indexedIds.value().begin(), indexedIds.value().end(), docId) !=
            indexedIds.value().end());
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore + 1));
}

TEST_CASE("batchInsertContentAndIndex: clears extraction_error",
          "[unit][metadata-repo][batch-content][counters]") {
    MetadataRepositoryFixture fix;
    fix.repository_->initializeCounters();

    // Insert a document with extraction_status=Success but an error set
    DocumentInfo doc;
    doc.sha256Hash = "counter_hash_error_clear";
    doc.fileName = "error_clear.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.modifiedTime = doc.createdTime;
    doc.contentExtracted = true;
    doc.extractionStatus = ExtractionStatus::Success;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));
    auto docId = insertResult.value();

    // Manually set an extraction error via raw SQL
    fix.pool_->withConnection([&](Database& db) -> Result<void> {
        auto stmtResult =
            db.prepare("UPDATE documents SET extraction_error = 'stale error' WHERE id = ?");
        REQUIRE((stmtResult.has_value()));
        auto& stmt = stmtResult.value();
        REQUIRE((stmt.bind(1, docId).has_value()));
        REQUIRE((stmt.execute().has_value()));
        return {};
    });

    // Batch insert should clear the error
    std::vector<BatchContentEntry> entries = {makeBatchContentEntry(docId, "Title", "Content")};
    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    // Verify error is cleared
    fix.pool_->withConnection([&](Database& db) -> Result<void> {
        auto stmtResult = db.prepare("SELECT extraction_error FROM documents WHERE id = ?");
        REQUIRE((stmtResult.has_value()));
        auto& stmt = stmtResult.value();
        REQUIRE((stmt.bind(1, docId).has_value()));
        auto hasRow = stmt.step();
        REQUIRE((hasRow.has_value()));
        REQUIRE((hasRow.value()));
        CHECK((stmt.isNull(0)));
        return {};
    });
}

TEST_CASE("batchInsertContentAndIndex: does not amplify lock retries",
          "[unit][metadata-repo][batch-content][contention]") {
    const auto dbPath = tempDbPath("metadata_repo_batch_content_lock_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    ConnectionPoolConfig lockerCfg;
    lockerCfg.minConnections = 1;
    lockerCfg.maxConnections = 1;
    lockerCfg.busyTimeout = std::chrono::milliseconds(10);
    auto lockerPool = std::make_unique<ConnectionPool>(dbPath.string(), lockerCfg);
    REQUIRE((lockerPool->initialize().has_value()));

    auto doc = makeDocumentWithPath("/tmp/batch-content-lock.txt", "batch-content-lock-hash");
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));

    auto heldConn = lockerPool->acquire();
    REQUIRE((heldConn.has_value()));
    REQUIRE(((*heldConn.value())->execute("BEGIN IMMEDIATE").has_value()));

    BatchContentEntry entry;
    entry.documentId = insert.value();
    entry.title = "Title";
    entry.contentText = "Locked content write";
    entry.mimeType = "text/plain";
    entry.extractionMethod = "test";
    entry.language = "en";
    entry.priorStateKnown = true;
    entry.priorContentExtracted = false;
    entry.priorExtractionStatus = ExtractionStatus::Pending;

    const auto start = std::chrono::steady_clock::now();
    auto result = repository->batchInsertContentAndIndex({entry});
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    REQUIRE_FALSE((result.has_value()));
    CHECK((result.error().message.find("locked") != std::string::npos));
    CHECK((elapsed < std::chrono::milliseconds(3000)));

    REQUIRE(((*heldConn.value())->execute("ROLLBACK").has_value()));

    lockerPool->shutdown();
    lockerPool.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

// --- batchGetDocumentsWithContentPreview tests ---

TEST_CASE("batchGetDocumentsWithContentPreview: documents with content",
          "[metadata-repo][batch-docs-preview]") {
    MetadataRepositoryFixture fix;

    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    // Insert 3 documents with content
    std::vector<int64_t> docIds;
    std::vector<std::string> hashes;
    for (int i = 0; i < 3; ++i) {
        DocumentInfo doc;
        doc.sha256Hash = "preview_hash_" + std::to_string(i);
        doc.fileName = "preview_doc_" + std::to_string(i) + ".txt";
        doc.filePath = "/test/preview_doc_" + std::to_string(i) + ".txt";
        doc.fileSize = 100;
        doc.mimeType = "text/plain";
        doc.createdTime = now;
        doc.modifiedTime = now;
        auto result = fix.repository_->insertDocument(doc);
        REQUIRE((result.has_value()));
        docIds.push_back(result.value());
        hashes.push_back(doc.sha256Hash);
    }

    // Insert content for each document
    std::vector<BatchContentEntry> entries;
    for (int i = 0; i < 3; ++i) {
        entries.push_back({docIds[static_cast<size_t>(i)], "Title " + std::to_string(i),
                           /* abstract */ "", "Content text for document " + std::to_string(i),
                           "text/plain", "test", "en"});
    }
    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    // Fetch with combined method
    auto result = fix.repository_->batchGetDocumentsWithContentPreview(hashes, 500);
    REQUIRE((result.has_value()));

    auto& map = result.value();
    CHECK((map.size() == 3));

    for (int i = 0; i < 3; ++i) {
        std::string hash = "preview_hash_" + std::to_string(i);
        REQUIRE((map.count(hash) == 1));
        auto& [info, preview] = map.at(hash);
        CHECK((info.id == docIds[static_cast<size_t>(i)]));
        CHECK((info.sha256Hash == hash));
        CHECK_FALSE(preview.empty());
        CHECK(
            (preview.find("Content text for document " + std::to_string(i)) != std::string::npos));
    }
}

TEST_CASE("batchGetDocumentsWithContentPreview: documents without content",
          "[metadata-repo][batch-docs-preview]") {
    MetadataRepositoryFixture fix;

    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    DocumentInfo doc;
    doc.sha256Hash = "no_content_hash";
    doc.fileName = "no_content_doc.txt";
    doc.filePath = "/test/no_content_doc.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = now;
    doc.modifiedTime = now;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));

    std::vector<std::string> hashes = {"no_content_hash"};
    auto result = fix.repository_->batchGetDocumentsWithContentPreview(hashes, 500);
    REQUIRE((result.has_value()));

    auto& map = result.value();
    REQUIRE((map.size() == 1));
    auto& [info, preview] = map.at("no_content_hash");
    CHECK((info.id == insertResult.value()));
    CHECK((preview.empty()));
}

TEST_CASE("batchGetDocumentsWithContentPreview: mixed with and without content",
          "[metadata-repo][batch-docs-preview]") {
    MetadataRepositoryFixture fix;

    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    // Doc with content
    DocumentInfo doc1;
    doc1.sha256Hash = "mixed_hash_with";
    doc1.fileName = "mixed_with.txt";
    doc1.filePath = "/test/mixed_with.txt";
    doc1.fileSize = 100;
    doc1.mimeType = "text/plain";
    doc1.createdTime = now;
    doc1.modifiedTime = now;
    auto id1 = fix.repository_->insertDocument(doc1);
    REQUIRE((id1.has_value()));

    std::vector<BatchContentEntry> entries = {
        makeBatchContentEntry(id1.value(), "Title", "Has content")};
    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    // Doc without content
    DocumentInfo doc2;
    doc2.sha256Hash = "mixed_hash_without";
    doc2.fileName = "mixed_without.txt";
    doc2.filePath = "/test/mixed_without.txt";
    doc2.fileSize = 100;
    doc2.mimeType = "text/plain";
    doc2.createdTime = now;
    doc2.modifiedTime = now;
    auto id2 = fix.repository_->insertDocument(doc2);
    REQUIRE((id2.has_value()));

    std::vector<std::string> hashes = {"mixed_hash_with", "mixed_hash_without"};
    auto result = fix.repository_->batchGetDocumentsWithContentPreview(hashes, 500);
    REQUIRE((result.has_value()));

    auto& map = result.value();
    CHECK((map.size() == 2));
    CHECK_FALSE(map.at("mixed_hash_with").second.empty());
    CHECK((map.at("mixed_hash_without").second.empty()));
}

TEST_CASE("batchGetDocumentsWithContentPreview: empty hash list",
          "[metadata-repo][batch-docs-preview]") {
    MetadataRepositoryFixture fix;

    std::vector<std::string> hashes;
    auto result = fix.repository_->batchGetDocumentsWithContentPreview(hashes, 500);
    REQUIRE((result.has_value()));
    CHECK((result.value().empty()));
}

TEST_CASE("batchGetDocumentsWithContentPreview: preview truncation",
          "[metadata-repo][batch-docs-preview]") {
    MetadataRepositoryFixture fix;

    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    DocumentInfo doc;
    doc.sha256Hash = "truncate_hash";
    doc.fileName = "truncate_doc.txt";
    doc.filePath = "/test/truncate_doc.txt";
    doc.fileSize = 100;
    doc.mimeType = "text/plain";
    doc.createdTime = now;
    doc.modifiedTime = now;
    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE((insertResult.has_value()));

    // Insert large content (10KB)
    std::string largeContent(10000, 'x');
    std::vector<BatchContentEntry> entries = {
        makeBatchContentEntry(insertResult.value(), "Title", largeContent)};
    auto batchResult = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE((batchResult.has_value()));

    std::vector<std::string> hashes = {"truncate_hash"};
    auto result = fix.repository_->batchGetDocumentsWithContentPreview(hashes, 200);
    REQUIRE((result.has_value()));

    auto& [info, preview] = result.value().at("truncate_hash");
    CHECK((preview.size() <= 200));
    CHECK_FALSE(preview.empty());
}

TEST_CASE("MetadataRepository: setMetadataBatch preserves typed values and latest write wins",
          "[unit][metadata][repository][batch][metadata-value]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/typed-metadata.txt", "typed-metadata-hash");
    auto insert = fix.repository_->insertDocument(doc);
    REQUIRE((insert.has_value()));
    const int64_t docId = insert.value();

    const std::vector<uint8_t> blob = {0x00, 0x01, 0x41, 0xFE, 0xFF};
    std::vector<std::tuple<int64_t, std::string, MetadataValue>> entries = {
        {docId, "author", MetadataValue("old")},
        {docId, "author", MetadataValue("new")},
        {docId, "revision", MetadataValue(int64_t{42})},
        {docId, "score", MetadataValue(0.875)},
        {docId, "published", MetadataValue(true)},
        {docId, "payload", MetadataValue::fromBlob(blob)},
    };

    auto setBatch = fix.repository_->setMetadataBatch(entries);
    REQUIRE((setBatch.has_value()));

    auto all = fix.repository_->getAllMetadata(docId);
    REQUIRE((all.has_value()));
    REQUIRE((all.value().size() == 5));

    CHECK((all.value().at("author").type == MetadataValueType::String));
    CHECK((all.value().at("author").asString() == "new"));
    CHECK((all.value().at("revision").type == MetadataValueType::Integer));
    CHECK((all.value().at("revision").asInteger() == 42));
    CHECK((all.value().at("score").type == MetadataValueType::Real));
    CHECK_THAT(all.value().at("score").asReal(), Catch::Matchers::WithinRel(0.875, 0.0001));
    CHECK((all.value().at("published").type == MetadataValueType::Boolean));
    CHECK((all.value().at("published").asBoolean()));
    CHECK((all.value().at("payload").type == MetadataValueType::Blob));

    auto payload = all.value().at("payload").asVariant();
    REQUIRE((std::holds_alternative<std::vector<uint8_t>>(payload)));
    CHECK((std::get<std::vector<uint8_t>>(payload) == blob));

    std::vector<int64_t> ids{docId, 999999};
    auto batch = fix.repository_->getMetadataForDocuments(ids);
    REQUIRE((batch.has_value()));
    REQUIRE((batch.value().contains(docId)));
    CHECK((batch.value().at(docId).at("author").asString() == "new"));
    CHECK_FALSE(batch.value().contains(999999));
}

TEST_CASE("MetadataRepository: setMetadataBatch does not amplify lock retries",
          "[unit][metadata][repository][batch][metadata][contention]") {
    const auto dbPath = tempDbPath("metadata_repo_set_metadata_batch_lock_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    ConnectionPoolConfig lockerCfg;
    lockerCfg.minConnections = 1;
    lockerCfg.maxConnections = 1;
    lockerCfg.busyTimeout = std::chrono::milliseconds(10);
    auto lockerPool = std::make_unique<ConnectionPool>(dbPath.string(), lockerCfg);
    REQUIRE((lockerPool->initialize().has_value()));

    auto doc = makeDocumentWithPath("/tmp/metadata-lock.txt", "metadata-lock-hash");
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));

    auto heldConn = lockerPool->acquire();
    REQUIRE((heldConn.has_value()));
    REQUIRE(((*heldConn.value())->execute("BEGIN IMMEDIATE").has_value()));

    const auto start = std::chrono::steady_clock::now();
    auto result = repository->setMetadataBatch(
        {{insert.value(), "title", MetadataValue("locked metadata write")}});
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    REQUIRE_FALSE((result.has_value()));
    CHECK((result.error().message.find("locked") != std::string::npos));
    CHECK((elapsed < std::chrono::milliseconds(3000)));

    REQUIRE(((*heldConn.value())->execute("ROLLBACK").has_value()));

    lockerPool->shutdown();
    lockerPool.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: feedback event write does not wait for a busy pool",
          "[unit][metadata][repository][feedback][contention]") {
    const auto dbPath = tempDbPath("metadata_repo_feedback_pool_busy_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    auto heldConnResult = repoPool->acquire();
    REQUIRE((heldConnResult.has_value()));
    auto heldConn = std::move(heldConnResult).value();

    FeedbackEvent event;
    event.eventId = "feedback-busy-pool-event";
    event.traceId = "feedback-busy-pool-trace";
    event.createdAt = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    event.source = "test";
    event.eventType = "retrieval_served";
    event.payloadJson = "{\"served\":1}";

    const auto start = std::chrono::steady_clock::now();
    auto result = repository->insertFeedbackEvent(event);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    REQUIRE_FALSE((result.has_value()));
    CHECK((elapsed < std::chrono::milliseconds(250)));
    CHECK((result.error().code == ErrorCode::Timeout ||
           result.error().code == ErrorCode::DatabaseError ||
           result.error().message.find("Timeout acquiring connection") != std::string::npos));

    heldConn.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataWriteFacade: fallback path batches writes until flush",
          "[unit][daemon][metadata-write-facade][fallback]") {
    const auto dbPath = tempDbPath("metadata_write_facade_fallback_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    auto doc = makeDocumentWithPath("/tmp/facade-fallback.txt", "facade-fallback-hash");
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));
    const auto docId = insert.value();
    REQUIRE((repository->updateDocumentExtractionStatus(docId, true, ExtractionStatus::Success)
                 .has_value()));
    REQUIRE(
        (repository->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, RepairStatus::Completed)
             .has_value()));

    yams::daemon::MetadataWriteFacade facade(nullptr, repository.get());
    facade.setMetadata(docId, "title", MetadataValue(std::string("title-1")));
    facade.setMetadata(docId, "title", MetadataValue(std::string("title-2")));
    facade.updateExtractionStatus(docId, false, ExtractionStatus::Pending,
                                  "reset by fallback facade");
    facade.updateRepairStatus({doc.sha256Hash}, RepairStatus::Processing);
    facade.updateRepairStatus({doc.sha256Hash}, RepairStatus::Pending);

    auto metadataBeforeFlush = repository->getMetadata(docId, "title");
    REQUIRE((metadataBeforeFlush.has_value()));
    CHECK_FALSE((metadataBeforeFlush.value().has_value()));

    auto docBeforeFlush = repository->getDocument(docId);
    REQUIRE((docBeforeFlush.has_value()));
    REQUIRE((docBeforeFlush.value().has_value()));
    CHECK((docBeforeFlush.value()->extractionStatus == ExtractionStatus::Success));
    CHECK((docBeforeFlush.value()->repairStatus == RepairStatus::Completed));

    facade.flush();

    auto metadataAfterFlush = repository->getMetadata(docId, "title");
    REQUIRE((metadataAfterFlush.has_value()));
    REQUIRE((metadataAfterFlush.value().has_value()));
    CHECK((metadataAfterFlush.value()->asString() == "title-2"));

    auto docAfterFlush = repository->getDocument(docId);
    REQUIRE((docAfterFlush.has_value()));
    REQUIRE((docAfterFlush.value().has_value()));
    CHECK((docAfterFlush.value()->extractionStatus == ExtractionStatus::Pending));
    CHECK((docAfterFlush.value()->repairStatus == RepairStatus::Pending));
    CHECK_FALSE((docAfterFlush.value()->contentExtracted));

    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataWriteFacade: fallback flush retains pending writes after a transient lock",
          "[unit][daemon][metadata-write-facade][fallback][contention]") {
    const auto dbPath = tempDbPath("metadata_write_facade_retry_");

    ConnectionPoolConfig repoCfg;
    repoCfg.minConnections = 1;
    repoCfg.maxConnections = 1;
    repoCfg.busyTimeout = std::chrono::milliseconds(10);
    auto repoPool = std::make_unique<ConnectionPool>(dbPath.string(), repoCfg);
    REQUIRE((repoPool->initialize().has_value()));

    auto repository = std::make_unique<MetadataRepository>(
        *repoPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    ConnectionPoolConfig lockerCfg;
    lockerCfg.minConnections = 1;
    lockerCfg.maxConnections = 1;
    lockerCfg.busyTimeout = std::chrono::milliseconds(10);
    auto lockerPool = std::make_unique<ConnectionPool>(dbPath.string(), lockerCfg);
    REQUIRE((lockerPool->initialize().has_value()));

    auto doc = makeDocumentWithPath("/tmp/facade-fallback-retry.txt", "facade-fallback-retry-hash");
    auto insert = repository->insertDocument(doc);
    REQUIRE((insert.has_value()));
    const auto docId = insert.value();
    REQUIRE((repository->updateDocumentExtractionStatus(docId, true, ExtractionStatus::Success)
                 .has_value()));
    REQUIRE(
        (repository->batchUpdateDocumentRepairStatuses({doc.sha256Hash}, RepairStatus::Completed)
             .has_value()));

    yams::daemon::MetadataWriteFacade facade(nullptr, repository.get());
    facade.setMetadata(docId, "title", MetadataValue(std::string("title-after-retry")));
    facade.updateExtractionStatus(docId, false, ExtractionStatus::Pending,
                                  "retry after lock clears");
    facade.updateRepairStatus({doc.sha256Hash}, RepairStatus::Pending);

    auto heldConn = lockerPool->acquire();
    REQUIRE((heldConn.has_value()));
    REQUIRE(((*heldConn.value())->execute("BEGIN IMMEDIATE").has_value()));

    facade.flush();

    auto metadataDuringLock = repository->getMetadata(docId, "title");
    REQUIRE((metadataDuringLock.has_value()));
    CHECK_FALSE((metadataDuringLock.value().has_value()));

    auto docDuringLock = repository->getDocument(docId);
    REQUIRE((docDuringLock.has_value()));
    REQUIRE((docDuringLock.value().has_value()));
    CHECK((docDuringLock.value()->extractionStatus == ExtractionStatus::Success));
    CHECK((docDuringLock.value()->repairStatus == RepairStatus::Completed));
    CHECK((docDuringLock.value()->contentExtracted));

    REQUIRE(((*heldConn.value())->execute("ROLLBACK").has_value()));

    facade.flush();

    auto metadataAfterRetry = repository->getMetadata(docId, "title");
    REQUIRE((metadataAfterRetry.has_value()));
    REQUIRE((metadataAfterRetry.value().has_value()));
    CHECK((metadataAfterRetry.value()->asString() == "title-after-retry"));

    auto docAfterRetry = repository->getDocument(docId);
    REQUIRE((docAfterRetry.has_value()));
    REQUIRE((docAfterRetry.value().has_value()));
    CHECK((docAfterRetry.value()->extractionStatus == ExtractionStatus::Pending));
    CHECK((docAfterRetry.value()->repairStatus == RepairStatus::Pending));
    CHECK_FALSE((docAfterRetry.value()->contentExtracted));

    lockerPool->shutdown();
    lockerPool.reset();
    repository.reset();
    repoPool->shutdown();
    repoPool.reset();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("MetadataRepository: getMetadataForDocuments chunks large batches",
          "[unit][metadata][repository][batch][metadata]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/chunk-a.txt", "chunk-metadata-hash-a");
    auto docB = makeDocumentWithPath("/tmp/chunk-b.txt", "chunk-metadata-hash-b");
    auto docAInsert = fix.repository_->insertDocument(docA);
    auto docBInsert = fix.repository_->insertDocument(docB);
    REQUIRE((docAInsert.has_value()));
    REQUIRE((docBInsert.has_value()));

    const int64_t docAId = docAInsert.value();
    const int64_t docBId = docBInsert.value();
    REQUIRE((fix.repository_->setMetadata(docAId, "author", MetadataValue("alpha")).has_value()));
    REQUIRE(
        (fix.repository_->setMetadata(docBId, "revision", MetadataValue(int64_t{7})).has_value()));

    constexpr std::size_t kLargeBatchSize = 33000;
    std::vector<int64_t> ids(kLargeBatchSize, 999999);
    ids.front() = docAId;
    ids[kLargeBatchSize / 2] = docBId;
    ids.back() = docAId;

    auto batch = fix.repository_->getMetadataForDocuments(ids);
    REQUIRE((batch.has_value()));
    REQUIRE((batch.value().size() == 2));
    REQUIRE((batch.value().contains(docAId)));
    REQUIRE((batch.value().contains(docBId)));
    CHECK((batch.value().at(docAId).at("author").asString() == "alpha"));
    CHECK((batch.value().at(docBId).at("revision").asInteger() == 7));
    CHECK_FALSE(batch.value().contains(999999));
}

TEST_CASE("MetadataRepository: batch document mutations preserve observable behavior",
          "[unit][metadata][repository][batch][document]") {
    MetadataRepositoryFixture fix;

    auto parent = makeDocumentWithPath("/tmp/batch-parent.txt", "batch-parent-hash");
    auto child = makeDocumentWithPath("/tmp/batch-child.txt", "batch-child-hash");
    auto survivor = makeDocumentWithPath("/tmp/batch-survivor.md", "batch-survivor-hash");

    auto parentInsert = fix.repository_->insertDocument(parent);
    auto childInsert = fix.repository_->insertDocument(child);
    auto survivorInsert = fix.repository_->insertDocument(survivor);
    REQUIRE((parentInsert.has_value()));
    REQUIRE((childInsert.has_value()));
    REQUIRE((survivorInsert.has_value()));

    const int64_t parentId = parentInsert.value();
    const int64_t childId = childInsert.value();
    const int64_t survivorId = survivorInsert.value();

    REQUIRE(fix.repository_
                ->insertContent(DocumentContent{parentId, "parent content", 14, "test", "en"})
                .has_value());
    REQUIRE(
        fix.repository_->setMetadata(parentId, "tag:batch", MetadataValue("delete")).has_value());
    REQUIRE(fix.repository_->indexDocumentContent(parentId, "parent", "parent content", "text")
                .has_value());

    DocumentRelationship rel;
    rel.parentId = parentId;
    rel.childId = childId;
    rel.relationshipType = RelationshipType::References;
    rel.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    auto relInsert = fix.repository_->insertRelationship(rel);
    REQUIRE((relInsert.has_value()));

    auto mimeUpdate = fix.repository_->updateDocumentsMimeBatch(
        {{childId, "text/markdown"}, {survivorId, "application/json"}, {999999, "ignored/type"}});
    REQUIRE((mimeUpdate.has_value()));
    CHECK((mimeUpdate.value() == 2));

    auto updatedChild = fix.repository_->getDocument(childId);
    REQUIRE((updatedChild.has_value()));
    REQUIRE((updatedChild.value().has_value()));
    CHECK((updatedChild.value()->mimeType == "text/markdown"));

    auto deleted = fix.repository_->deleteDocumentsBatch({parentId, 999999});
    REQUIRE((deleted.has_value()));
    CHECK((deleted.value() == 1));

    auto missingParent = fix.repository_->getDocument(parentId);
    REQUIRE((missingParent.has_value()));
    CHECK_FALSE(missingParent.value().has_value());

    auto removedContent = fix.repository_->getContent(parentId);
    REQUIRE((removedContent.has_value()));
    CHECK_FALSE(removedContent.value().has_value());

    auto removedMetadata = fix.repository_->getAllMetadata(parentId);
    REQUIRE((removedMetadata.has_value()));
    CHECK((removedMetadata.value().empty()));

    auto childRels = fix.repository_->getRelationships(childId);
    REQUIRE((childRels.has_value()));
    CHECK((childRels.value().empty()));

    auto survivorDoc = fix.repository_->getDocument(survivorId);
    REQUIRE((survivorDoc.has_value()));
    REQUIRE((survivorDoc.value().has_value()));
    CHECK((survivorDoc.value()->mimeType == "application/json"));
}

TEST_CASE("MetadataRepository: batch get APIs skip missing inputs and keep keyed results",
          "[unit][metadata][repository][batch][read]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/batch-a.txt", "batch-get-hash-a");
    auto docB = makeDocumentWithPath("/tmp/batch-b.txt", "batch-get-hash-b");
    auto docC = makeDocumentWithPath("/tmp/batch-c.txt", "batch-get-hash-c");
    auto insertA = fix.repository_->insertDocument(docA);
    auto insertB = fix.repository_->insertDocument(docB);
    auto insertC = fix.repository_->insertDocument(docC);
    REQUIRE((insertA.has_value()));
    REQUIRE((insertB.has_value()));
    REQUIRE((insertC.has_value()));

    REQUIRE(fix.repository_
                ->insertContent(DocumentContent{insertA.value(), "alpha body", 10, "test", "en"})
                .has_value());
    REQUIRE(fix.repository_
                ->insertContent(DocumentContent{insertC.value(), "gamma body", 10, "test", "en"})
                .has_value());

    auto docs = fix.repository_->batchGetDocumentsByHash(
        {"batch-get-hash-a", "missing-hash", "batch-get-hash-c", "batch-get-hash-a"});
    REQUIRE((docs.has_value()));
    CHECK((docs.value().size() == 2));
    CHECK((docs.value().at("batch-get-hash-a").id == insertA.value()));
    CHECK((docs.value().at("batch-get-hash-c").id == insertC.value()));
    CHECK_FALSE(docs.value().contains("missing-hash"));

    auto content = fix.repository_->batchGetContent(
        {insertA.value(), insertB.value(), insertC.value(), 999999, insertA.value()});
    REQUIRE((content.has_value()));
    CHECK((content.value().size() == 2));
    CHECK((content.value().at(insertA.value()).contentText == "alpha body"));
    CHECK((content.value().at(insertC.value()).contentText == "gamma body"));
    CHECK_FALSE(content.value().contains(insertB.value()));
    CHECK_FALSE(content.value().contains(999999));

    auto emptyDocs = fix.repository_->batchGetDocumentsByHash({});
    REQUIRE((emptyDocs.has_value()));
    CHECK((emptyDocs.value().empty()));

    auto emptyContent = fix.repository_->batchGetContent({});
    REQUIRE((emptyContent.has_value()));
    CHECK((emptyContent.value().empty()));
}

TEST_CASE("MetadataRepository: removeFromIndexByHashBatch reports actual FTS removals",
          "[unit][metadata][repository][batch][fts5]") {
    MetadataRepositoryFixture fix;

    auto indexed = makeDocumentWithPath("/tmp/indexed.txt", "batch-indexed-hash");
    auto notIndexed = makeDocumentWithPath("/tmp/not-indexed.txt", "batch-not-indexed-hash");
    auto indexedInsert = fix.repository_->insertDocument(indexed);
    auto notIndexedInsert = fix.repository_->insertDocument(notIndexed);
    REQUIRE((indexedInsert.has_value()));
    REQUIRE((notIndexedInsert.has_value()));

    REQUIRE(fix.repository_
                ->indexDocumentContent(indexedInsert.value(), "indexed", "needle", "text/plain")
                .has_value());

    auto beforeIndexed = fix.repository_->hasFtsEntry(indexedInsert.value());
    auto beforeNotIndexed = fix.repository_->hasFtsEntry(notIndexedInsert.value());
    REQUIRE((beforeIndexed.has_value()));
    REQUIRE((beforeNotIndexed.has_value()));
    REQUIRE((beforeIndexed.value()));
    REQUIRE_FALSE(beforeNotIndexed.value());

    auto removed = fix.repository_->removeFromIndexByHashBatch(
        {"batch-indexed-hash", "batch-not-indexed-hash", "missing-hash"});
    REQUIRE((removed.has_value()));
    CHECK((removed.value() == 1));

    auto afterIndexed = fix.repository_->hasFtsEntry(indexedInsert.value());
    auto afterNotIndexed = fix.repository_->hasFtsEntry(notIndexedInsert.value());
    REQUIRE((afterIndexed.has_value()));
    REQUIRE((afterNotIndexed.has_value()));
    CHECK_FALSE(afterIndexed.value());
    CHECK_FALSE(afterNotIndexed.value());
}

TEST_CASE("MetadataRepository: batchInsertDocumentsWithMetadata coalesces inserts",
          "[metadata][batch][insert]") {
    MetadataRepositoryFixture fix;

    auto makeItem = [](const std::string& path, const std::string& hash, const std::string& tagKey,
                       const std::string& tagVal, bool pathTree) {
        BatchDocumentInsert item;
        item.info = makeDocumentWithPath(path, hash);
        item.tags.emplace_back(tagKey, MetadataValue(tagVal));
        item.updatePathTreeInTransaction = pathTree;
        return item;
    };

    std::vector<BatchDocumentInsert> items;
    items.push_back(makeItem("repo/batch/a.txt", "batch-hash-a", "stage", "alpha", true));
    items.push_back(makeItem("repo/batch/b.txt", "batch-hash-b", "stage", "beta", true));
    items.push_back(makeItem("repo/batch/c.txt", "batch-hash-c", "stage", "gamma", true));

    // Attach a snapshot to the middle item.
    TreeSnapshotRecord snap;
    snap.snapshotId = "batch-snap-1";
    snap.rootTreeHash = "batch-root-hash";
    snap.createdTime = 1700000000;
    snap.metadata["directory_path"] = "repo/batch";
    items[1].snapshot = snap;

    auto batchResult = fix.repository_->batchInsertDocumentsWithMetadata(items);
    REQUIRE((batchResult.has_value()));
    auto ids = batchResult.value();
    REQUIRE((ids.size() == 3));

    SECTION("ids are valid, distinct, and resolve to the right documents") {
        std::vector<int64_t> sorted = ids;
        std::sort(sorted.begin(), sorted.end());
        CHECK((std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end()));
        for (auto id : ids) {
            CHECK((id > 0));
        }
        const std::vector<std::string> hashes = {"batch-hash-a", "batch-hash-b", "batch-hash-c"};
        for (std::size_t i = 0; i < hashes.size(); ++i) {
            auto doc = fix.repository_->getDocumentByHash(hashes[i]);
            REQUIRE((doc.has_value()));
            REQUIRE((doc.value().has_value()));
            CHECK((doc.value().value().id == ids[i]));
        }
    }

    SECTION("metadata tags are persisted for each document") {
        const std::vector<std::pair<int64_t, std::string>> expected = {
            {ids[0], "alpha"}, {ids[1], "beta"}, {ids[2], "gamma"}};
        for (const auto& [id, val] : expected) {
            auto mv = fix.repository_->getMetadata(id, "stage");
            REQUIRE((mv.has_value()));
            REQUIRE((mv.value().has_value()));
            CHECK((mv.value().value().value == val));
        }
    }

    SECTION("snapshot attached to one item is committed and bound to its document") {
        // All-or-nothing batch committed (batchResult ok), so the tree_snapshots row was written
        // without constraint failure. Verify the in-transaction wiring set ingestDocumentId.
        REQUIRE((items[1].snapshot.has_value()));
        REQUIRE((items[1].snapshot->ingestDocumentId.has_value()));
        CHECK((items[1].snapshot->ingestDocumentId.value() == ids[1]));
    }

    SECTION("existing-hash item in a later batch dedups to the same id") {
        std::vector<BatchDocumentInsert> dupItems;
        dupItems.push_back(makeItem("repo/batch/a.txt", "batch-hash-a", "stage", "alpha", true));
        auto dupResult = fix.repository_->batchInsertDocumentsWithMetadata(dupItems);
        REQUIRE((dupResult.has_value()));
        REQUIRE((dupResult.value().size() == 1));
        CHECK((dupResult.value()[0] == ids[0]));
    }

    SECTION("empty batch is a no-op success") {
        std::vector<BatchDocumentInsert> empty;
        auto emptyResult = fix.repository_->batchInsertDocumentsWithMetadata(empty);
        REQUIRE((emptyResult.has_value()));
        CHECK((emptyResult.value().empty()));
    }
}

TEST_CASE("MetadataInsertWriter coalesces concurrent submits into correct per-doc ids",
          "[metadata][batch][writer]") {
    auto dbPath = tempDbPath("metadata_insert_writer_test_");
    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 2;
    auto pool = std::make_shared<ConnectionPool>(dbPath.string(), config);
    REQUIRE((pool->initialize().has_value()));
    auto repo = std::make_shared<MetadataRepository>(
        *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    constexpr int kDocs = 64;
    {
        MetadataInsertWriter writer(repo);

        // Submit from several threads to exercise concurrent coalescing.
        std::atomic<int> next{0};
        std::mutex futMutex;
        std::vector<std::future<Result<int64_t>>> collected;
        collected.reserve(kDocs);
        constexpr int kThreads = 8;
        std::vector<std::thread> submitters;
        for (int t = 0; t < kThreads; ++t) {
            submitters.emplace_back([&]() {
                while (true) {
                    int i = next.fetch_add(1);
                    if (i >= kDocs)
                        break;
                    auto fut = writer.submit(makeIndexedBatchInsert(i));
                    std::lock_guard<std::mutex> lk(futMutex);
                    collected.push_back(std::move(fut));
                }
            });
        }
        for (auto& th : submitters)
            th.join();

        std::vector<int64_t> ids;
        ids.reserve(kDocs);
        for (auto& f : collected) {
            auto r = f.get();
            REQUIRE((r.has_value()));
            CHECK((r.value() > 0));
            ids.push_back(r.value());
        }
        REQUIRE((static_cast<int>(ids.size()) == kDocs));
        std::sort(ids.begin(), ids.end());
        CHECK((std::adjacent_find(ids.begin(), ids.end()) == ids.end()));
    }

    // All documents are durable after the writer drains.
    for (int i = 0; i < kDocs; ++i) {
        auto doc = repo->getDocumentByHash("writer-hash-" + std::to_string(i));
        REQUIRE((doc.has_value()));
        CHECK((doc.value().has_value()));
    }

    pool->shutdown();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("ContentIndexWriter commits content-index chunks from concurrent submits",
          "[metadata][batch][writer][content]") {
    auto dbPath = tempDbPath("content_index_writer_test_");
    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 2;
    auto pool = std::make_shared<ConnectionPool>(dbPath.string(), config);
    REQUIRE((pool->initialize().has_value()));
    auto repo = std::make_shared<MetadataRepository>(
        *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    repo->initializeCounters();

    constexpr int kDocs = 32;
    std::vector<int64_t> docIds;
    for (int i = 0; i < kDocs; ++i) {
        DocumentInfo doc;
        doc.sha256Hash = "ciw-hash-" + std::to_string(i);
        doc.fileName = "ciw-" + std::to_string(i) + ".txt";
        doc.fileSize = 100;
        doc.mimeType = "text/plain";
        doc.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.modifiedTime = doc.createdTime;
        doc.contentExtracted = false;
        doc.extractionStatus = ExtractionStatus::Pending;
        auto r = repo->insertDocument(doc);
        REQUIRE((r.has_value()));
        docIds.push_back(r.value());
    }

    {
        ContentIndexWriter writer(repo);
        std::atomic<int> next{0};
        std::mutex futMutex;
        std::vector<std::future<Result<void>>> collected;
        std::vector<std::thread> submitters;
        for (int t = 0; t < 6; ++t) {
            submitters.emplace_back([&]() {
                while (true) {
                    int i = next.fetch_add(1);
                    if (i >= kDocs)
                        break;
                    std::vector<BatchContentEntry> chunk{makeBatchContentEntry(
                        docIds[static_cast<size_t>(i)], "Title " + std::to_string(i),
                        "Content body " + std::to_string(i))};
                    auto fut = writer.submit(std::move(chunk));
                    std::lock_guard<std::mutex> lk(futMutex);
                    collected.push_back(std::move(fut));
                }
            });
        }
        for (auto& th : submitters)
            th.join();
        for (auto& f : collected) {
            auto r = f.get();
            CHECK((r.has_value()));
        }
    }

    // Every document is now FTS-indexed after the writer drains.
    for (int i = 0; i < kDocs; ++i) {
        auto fts = repo->hasFtsEntry(docIds[static_cast<size_t>(i)]);
        REQUIRE((fts.has_value()));
        CHECK((fts.value()));
    }

    pool->shutdown();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}
// NOLINTEND(bugprone-chained-comparison)
