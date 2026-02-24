// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <thread>
#include <vector>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/common/utf8_utils.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
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
    auto derived = computePathDerivedValues(path);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    return info;
}

struct MetadataRepositoryFixture {
    MetadataRepositoryFixture() {
        dbPath_ = tempDbPath("metadata_repo_catch2_test_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~MetadataRepositoryFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

} // namespace

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
    REQUIRE(result.has_value());

    auto docId = result.value();
    CHECK(docId > 0);

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto retrievedDoc = getResult.value().value();
    CHECK(retrievedDoc.id == docId);
    CHECK(retrievedDoc.sha256Hash == docInfo.sha256Hash);
    CHECK(retrievedDoc.fileName == docInfo.fileName);
    CHECK(retrievedDoc.fileSize == docInfo.fileSize);
    CHECK(retrievedDoc.mimeType == docInfo.mimeType);
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

    REQUIRE(writePool->initialize().has_value());
    REQUIRE(readPool->initialize().has_value());

    auto repository = std::make_unique<MetadataRepository>(*writePool, readPool.get());

    auto doc = makeDocumentWithPath("/tmp/dual-pool.txt", "dual-pool-hash");
    auto insertResult = repository->insertDocument(doc);
    REQUIRE(insertResult.has_value());
    auto docId = insertResult.value();

    auto readOk = repository->getDocumentByHash("dual-pool-hash");
    REQUIRE(readOk.has_value());
    REQUIRE(readOk.value().has_value());

    readPool->shutdown();

    auto readAfterShutdown = repository->getDocumentByHash("dual-pool-hash");
    REQUIRE_FALSE(readAfterShutdown.has_value());

    MetadataValue value;
    value.value = "still-writeable";
    value.type = MetadataValueType::String;
    auto writeAfterReadPoolDown = repository->setMetadata(docId, "dual_pool_test", value);
    REQUIRE(writeAfterReadPoolDown.has_value());

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

    REQUIRE(writePool->initialize().has_value());
    REQUIRE(readPool->initialize().has_value());

    auto repository = std::make_unique<MetadataRepository>(*writePool, readPool.get());

    auto doc = makeDocumentWithPath("/tmp/dual-pool-session.txt", "dual-pool-session-hash");
    auto insertResult = repository->insertDocument(doc);
    REQUIRE(insertResult.has_value());
    auto docId = insertResult.value();

    MetadataValue sessionValue;
    sessionValue.value = "sess-123";
    sessionValue.type = MetadataValueType::String;
    REQUIRE(repository->setMetadata(docId, "session_id", sessionValue).has_value());

    auto countOk = repository->countDocumentsBySessionId("sess-123");
    REQUIRE(countOk.has_value());
    REQUIRE(countOk.value() == 1);

    readPool->shutdown();

    auto countAfterShutdown = repository->countDocumentsBySessionId("sess-123");
    REQUIRE_FALSE(countAfterShutdown.has_value());

    MetadataValue writeValue;
    writeValue.value = "still-writeable-after-read-pool-down";
    writeValue.type = MetadataValueType::String;
    REQUIRE(repository->setMetadata(docId, "dual_pool_write_check", writeValue).has_value());

    repository.reset();
    writePool->shutdown();
    writePool.reset();
    readPool.reset();

    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
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

    REQUIRE(writePool->initialize().has_value());
    REQUIRE(readPool->initialize().has_value());

    auto repository = std::make_unique<MetadataRepository>(*writePool, readPool.get());

    auto doc = makeDocumentWithPath("/tmp/dual-pool-embed-hash.txt", "dual-pool-embed-hash");
    auto insertResult = repository->insertDocument(doc);
    REQUIRE(insertResult.has_value());

    auto readOk = repository->hasDocumentEmbeddingByHash("dual-pool-embed-hash");
    REQUIRE(readOk.has_value());
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
    REQUIRE(doc1Res.has_value());

    // doc2: recent document (inside filter range)
    auto doc2 = makeDocumentWithPath("/tmp/b.txt", "hash-b");
    doc2.createdTime = nowSec;
    doc2.modifiedTime = nowSec;
    doc2.indexedTime = nowSec;
    auto doc2Res = fix.repository_->insertDocument(doc2);
    REQUIRE(doc2Res.has_value());

    // doc3: another recent document (inside filter range)
    auto doc3 = makeDocumentWithPath("/tmp/c.txt", "hash-c");
    doc3.createdTime = nowSec;
    doc3.modifiedTime = nowSec;
    doc3.indexedTime = nowSec;
    auto doc3Res = fix.repository_->insertDocument(doc3);
    REQUIRE(doc3Res.has_value());

    MetadataValue v1;
    v1.value = "PBI-070";
    v1.type = MetadataValueType::String;
    // doc1 gets PBI-070 but is outside time filter
    REQUIRE(fix.repository_->setMetadata(doc1Res.value(), "pbi", v1).has_value());
    // doc2 gets PBI-070 (inside time filter)
    REQUIRE(fix.repository_->setMetadata(doc2Res.value(), "pbi", v1).has_value());

    MetadataValue v2;
    v2.value = "PBI-071";
    v2.type = MetadataValueType::String;
    // doc3 gets PBI-071 (inside time filter)
    REQUIRE(fix.repository_->setMetadata(doc3Res.value(), "pbi", v2).has_value());

    DocumentQueryOptions opts;
    opts.modifiedAfter = std::chrono::duration_cast<std::chrono::seconds>(
                             (nowSec - std::chrono::hours(24)).time_since_epoch())
                             .count();

    auto countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE(countsRes.has_value());

    const auto& map = countsRes.value();
    auto it = map.find("pbi");
    REQUIRE(it != map.end());

    std::unordered_map<std::string, int64_t> counts;
    for (const auto& row : it->second) {
        counts[row.value] = row.count;
    }

    // Only doc2 (PBI-070) and doc3 (PBI-071) are in the time range
    // doc1 (PBI-070) is filtered out because it's 48 hours old
    REQUIRE(counts.size() == 2);
    REQUIRE(counts["PBI-070"] == 1);
    REQUIRE(counts["PBI-071"] == 1);
}

TEST_CASE("MetadataRepository: metadata value counts materialized view stays consistent",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/tmp/materialized_a.txt", "materialized-a");
    auto docB = makeDocumentWithPath("/tmp/materialized_b.txt", "materialized-b");

    auto docARes = fix.repository_->insertDocument(docA);
    auto docBRes = fix.repository_->insertDocument(docB);
    REQUIRE(docARes.has_value());
    REQUIRE(docBRes.has_value());

    MetadataValue v1;
    v1.value = "PBI-500";
    v1.type = MetadataValueType::String;
    REQUIRE(fix.repository_->setMetadata(docARes.value(), "pbi", v1).has_value());
    REQUIRE(fix.repository_->setMetadata(docBRes.value(), "pbi", v1).has_value());

    DocumentQueryOptions opts;
    auto countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE(countsRes.has_value());
    auto it = countsRes.value().find("pbi");
    REQUIRE(it != countsRes.value().end());
    REQUIRE(it->second.size() == 1);
    CHECK(it->second.front().value == "PBI-500");
    CHECK(it->second.front().count == 2);

    MetadataValue v2;
    v2.value = "PBI-600";
    v2.type = MetadataValueType::String;
    REQUIRE(fix.repository_->setMetadata(docBRes.value(), "pbi", v2).has_value());

    countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE(countsRes.has_value());
    it = countsRes.value().find("pbi");
    REQUIRE(it != countsRes.value().end());
    std::unordered_map<std::string, int64_t> latest;
    for (const auto& row : it->second) {
        latest[row.value] = row.count;
    }
    REQUIRE(latest.size() == 2);
    CHECK(latest["PBI-500"] == 1);
    CHECK(latest["PBI-600"] == 1);

    auto deleteResult = fix.repository_->deleteDocument(docARes.value());
    REQUIRE(deleteResult.has_value());

    countsRes = fix.repository_->getMetadataValueCounts({"pbi"}, opts);
    REQUIRE(countsRes.has_value());
    it = countsRes.value().find("pbi");
    REQUIRE(it != countsRes.value().end());
    REQUIRE(it->second.size() == 1);
    CHECK(it->second.front().value == "PBI-600");
    CHECK(it->second.front().count == 1);
}

TEST_CASE("MetadataRepository: metadata value counts falls back when path FTS join fails",
          "[unit][metadata][repository][fts-fallback]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath("/notes/todo.md", "fts-fallback-a");
    auto docB = makeDocumentWithPath("/archive/todo-old.md", "fts-fallback-b");

    auto aId = fix.repository_->insertDocument(docA);
    auto bId = fix.repository_->insertDocument(docB);
    REQUIRE(aId.has_value());
    REQUIRE(bId.has_value());

    MetadataValue ownerA;
    ownerA.type = MetadataValueType::String;
    ownerA.value = "alice";
    REQUIRE(fix.repository_->setMetadata(aId.value(), "owner", ownerA).has_value());

    MetadataValue ownerB;
    ownerB.type = MetadataValueType::String;
    ownerB.value = "bob";
    REQUIRE(fix.repository_->setMetadata(bId.value(), "owner", ownerB).has_value());

    // Force the FTS-join query path to fail while pathFtsAvailable_ remains true in the repository.
    Database db;
    REQUIRE(db.open(fix.dbPath_.string()).has_value());
    REQUIRE(db.execute("DROP TABLE IF EXISTS documents_path_fts").has_value());
    db.close();

    DocumentQueryOptions fallbackOpts;
    fallbackOpts.containsFragment = "todo.md";
    fallbackOpts.containsUsesFts = true;

    auto fallbackRes = fix.repository_->getMetadataValueCounts({"owner"}, fallbackOpts);
    REQUIRE(fallbackRes.has_value());

    DocumentQueryOptions baselineOpts = fallbackOpts;
    baselineOpts.containsUsesFts = false;
    auto baselineRes = fix.repository_->getMetadataValueCounts({"owner"}, baselineOpts);
    REQUIRE(baselineRes.has_value());

    const auto& fallbackMap = fallbackRes.value();
    const auto& baselineMap = baselineRes.value();
    REQUIRE(fallbackMap.contains("owner"));
    REQUIRE(baselineMap.contains("owner"));
    REQUIRE(fallbackMap.at("owner").size() == baselineMap.at("owner").size());

    std::unordered_map<std::string, int64_t> fallbackCounts;
    std::unordered_map<std::string, int64_t> baselineCounts;
    for (const auto& row : fallbackMap.at("owner")) {
        fallbackCounts[row.value] = row.count;
    }
    for (const auto& row : baselineMap.at("owner")) {
        baselineCounts[row.value] = row.count;
    }

    CHECK(fallbackCounts == baselineCounts);
    CHECK(fallbackCounts["alice"] == 1);
    CHECK_FALSE(fallbackCounts.contains("bob"));
}

TEST_CASE("MetadataRepository: get document not found", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto result = fix.repository_->getDocument(999999);
    REQUIRE(result.has_value());
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
    REQUIRE(createResult.has_value());

    auto findResult = fix.repository_->getDocumentByHash("hash456");
    REQUIRE(findResult.has_value());
    REQUIRE(findResult.value().has_value());

    auto foundDoc = findResult.value().value();
    CHECK(foundDoc.sha256Hash == "hash456");
    CHECK(foundDoc.fileName == "hash_test.txt");
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    docInfo.id = docId;
    docInfo.fileName = "updated.txt";
    docInfo.fileSize = 4096;
    docInfo.modifiedTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    auto updateResult = fix.repository_->updateDocument(docInfo);
    REQUIRE(updateResult.has_value());

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto updatedDoc = getResult.value().value();
    CHECK(updatedDoc.fileName == "updated.txt");
    CHECK(updatedDoc.fileSize == 4096);
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

    REQUIRE(fix.repository_->insertDocument(docA).has_value());
    REQUIRE(fix.repository_->insertDocument(docB).has_value());
    REQUIRE(fix.repository_->insertDocument(docC).has_value());

    SECTION("Exact path lookup") {
        DocumentQueryOptions exactOpts;
        exactOpts.exactPath = "/notes/todo.md";
        auto exactRes = fix.repository_->queryDocuments(exactOpts);
        REQUIRE(exactRes.has_value());
        REQUIRE(exactRes.value().size() == 1);
        CHECK(exactRes.value().front().filePath == "/notes/todo.md");
    }

    SECTION("Directory prefix with subdirectories") {
        DocumentQueryOptions prefixOpts;
        prefixOpts.pathPrefix = "/notes";
        prefixOpts.prefixIsDirectory = true;
        prefixOpts.includeSubdirectories = true;
        auto prefixRes = fix.repository_->queryDocuments(prefixOpts);
        REQUIRE(prefixRes.has_value());
        std::vector<std::string> notePaths;
        for (const auto& d : prefixRes.value())
            notePaths.push_back(d.filePath);
        std::sort(notePaths.begin(), notePaths.end());
        REQUIRE(notePaths.size() == 2);
        CHECK(notePaths[0] == "/notes/log.txt");
        CHECK(notePaths[1] == "/notes/todo.md");
    }

    SECTION("Contains fragment via FTS") {
        DocumentQueryOptions containsOpts;
        containsOpts.containsFragment = "todo.md";
        containsOpts.containsUsesFts = true;
        auto containsRes = fix.repository_->queryDocuments(containsOpts);
        REQUIRE(containsRes.has_value());
        REQUIRE(containsRes.value().size() == 1);
        CHECK(containsRes.value().front().filePath == "/notes/todo.md");
    }

    SECTION("Extension filter") {
        DocumentQueryOptions extOpts;
        extOpts.extension = ".txt";
        auto extRes = fix.repository_->queryDocuments(extOpts);
        REQUIRE(extRes.has_value());
        REQUIRE(extRes.value().size() == 1);
        CHECK(extRes.value().front().filePath == "/notes/log.txt");
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
    REQUIRE(aId.has_value());
    REQUIRE(bId.has_value());
    REQUIRE(cId.has_value());
    REQUIRE(dId.has_value());

    auto setStringMeta = [&](int64_t docId, const std::string& key, const std::string& value) {
        MetadataValue mv;
        mv.type = MetadataValueType::String;
        mv.value = value;
        REQUIRE(fix.repository_->setMetadata(docId, key, mv).has_value());
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
        REQUIRE(docsRes.has_value());
        REQUIRE(projRes.has_value());

        const auto& docs = docsRes.value();
        const auto& projs = projRes.value();
        REQUIRE(docs.size() == projs.size());

        for (size_t i = 0; i < docs.size(); ++i) {
            CAPTURE(i);
            CHECK(projs[i].id == docs[i].id);
            CHECK(projs[i].filePath == docs[i].filePath);
            CHECK(projs[i].fileName == docs[i].fileName);
            CHECK(projs[i].fileExtension == docs[i].fileExtension);
            CHECK(projs[i].fileSize == docs[i].fileSize);
            CHECK(projs[i].sha256Hash == docs[i].sha256Hash);
            CHECK(projs[i].mimeType == docs[i].mimeType);
            CHECK(projs[i].createdTime == docs[i].createdTime);
            CHECK(projs[i].modifiedTime == docs[i].modifiedTime);
            CHECK(projs[i].indexedTime == docs[i].indexedTime);
            CHECK(projs[i].extractionStatus == docs[i].extractionStatus);
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    auto deleteResult = fix.repository_->deleteDocument(docId);
    REQUIRE(deleteResult.has_value());

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE(getResult.has_value());
    CHECK_FALSE(getResult.value().has_value());
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    MetadataValue value("Test Author");

    auto setResult = fix.repository_->setMetadata(docId, "author", value);
    REQUIRE(setResult.has_value());

    auto getResult = fix.repository_->getMetadata(docId, "author");
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto retrievedValue = getResult.value().value();
    CHECK(retrievedValue.asString() == "Test Author");
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    MetadataValue authorValue("John Doe");
    fix.repository_->setMetadata(docId, "author", authorValue);

    MetadataValue yearValue(int64_t(2024));
    fix.repository_->setMetadata(docId, "year", yearValue);

    MetadataValue ratingValue(4.5);
    fix.repository_->setMetadata(docId, "rating", ratingValue);

    auto getAllResult = fix.repository_->getAllMetadata(docId);
    REQUIRE(getAllResult.has_value());

    auto metadata = getAllResult.value();
    CHECK(metadata.size() == 3);

    CHECK(metadata["author"].asString() == "John Doe");
    CHECK(metadata["year"].asInteger() == 2024);
    CHECK(metadata["rating"].asReal() == Catch::Approx(4.5));
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    MetadataValue value("Temporary");
    fix.repository_->setMetadata(docId, "temp", value);

    auto getResult = fix.repository_->getMetadata(docId, "temp");
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto removeResult = fix.repository_->removeMetadata(docId, "temp");
    REQUIRE(removeResult.has_value());

    getResult = fix.repository_->getMetadata(docId, "temp");
    REQUIRE(getResult.has_value());
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    SECTION("returns false before indexing") {
        auto hasFts = fix.repository_->hasFtsEntry(docId);
        REQUIRE(hasFts.has_value());
        CHECK_FALSE(hasFts.value());
    }

    SECTION("returns true after indexing") {
        auto indexResult = fix.repository_->indexDocumentContent(
            docId, "Test Document", "Searchable content here", "text/plain");
        REQUIRE(indexResult.has_value());

        auto hasFts = fix.repository_->hasFtsEntry(docId);
        REQUIRE(hasFts.has_value());
        CHECK(hasFts.value());
    }

    SECTION("returns false for non-existent document ID") {
        auto hasFts = fix.repository_->hasFtsEntry(999999);
        REQUIRE(hasFts.has_value());
        CHECK_FALSE(hasFts.value());
    }
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
    REQUIRE(id1Res.has_value());
    auto docId1 = id1Res.value();

    DocumentInfo doc2;
    doc2.sha256Hash = "fts5_set_doc2";
    doc2.fileName = "doc2.txt";
    doc2.fileSize = 60;
    doc2.mimeType = "text/plain";
    doc2.createdTime = now;
    doc2.modifiedTime = now;
    auto id2Res = fix.repository_->insertDocument(doc2);
    REQUIRE(id2Res.has_value());
    auto docId2 = id2Res.value();

    // Before indexing: set should be empty
    auto setBefore = fix.repository_->getFts5IndexedRowIdSet();
    REQUIRE(setBefore.has_value());
    CHECK(setBefore.value().count(docId1) == 0);
    CHECK(setBefore.value().count(docId2) == 0);

    // Index only doc1
    auto indexResult = fix.repository_->indexDocumentContent(
        docId1, "Doc One", "First document content", "text/plain");
    REQUIRE(indexResult.has_value());

    // Set should contain only doc1
    auto setAfter = fix.repository_->getFts5IndexedRowIdSet();
    REQUIRE(setAfter.has_value());
    CHECK(setAfter.value().count(docId1) == 1);
    CHECK(setAfter.value().count(docId2) == 0);

    // Index doc2
    auto indexResult2 = fix.repository_->indexDocumentContent(
        docId2, "Doc Two", "Second document content", "text/plain");
    REQUIRE(indexResult2.has_value());

    // Set should contain both
    auto setFinal = fix.repository_->getFts5IndexedRowIdSet();
    REQUIRE(setFinal.has_value());
    CHECK(setFinal.value().count(docId1) == 1);
    CHECK(setFinal.value().count(docId2) == 1);
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    auto indexResult = fix.repository_->indexDocumentContent(
        docId, "Test Document", "This is a test document with searchable content", "text/plain");
    REQUIRE(indexResult.has_value());

    auto searchResult = fix.repository_->search("test", 10, 0);
    REQUIRE(searchResult.has_value());

    auto results = searchResult.value();
    CHECK(results.results.size() > 0);
}

TEST_CASE("MetadataRepository: search totalCount honors docIds filter",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc1 = makeDocumentWithPath("/tmp/doc_count_1.txt", "search_doc_count_1");
    auto doc2 = makeDocumentWithPath("/tmp/doc_count_2.txt", "search_doc_count_2");

    auto insert1 = fix.repository_->insertDocument(doc1);
    auto insert2 = fix.repository_->insertDocument(doc2);
    REQUIRE(insert1.has_value());
    REQUIRE(insert2.has_value());

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
    REQUIRE(searchResult.has_value());

    const auto& results = searchResult.value();
    REQUIRE(results.results.size() == 1);
    CHECK(results.results.front().document.id == docId1);
    CHECK(results.totalCount == 1);
}

TEST_CASE("MetadataRepository: fuzzy search returns content matches",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath(
        "/notes/fuzzy_content.txt",
        "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");

    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE(insertResult.has_value());
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
    REQUIRE(contentInsertResult.has_value());

    auto indexResult =
        fix.repository_->indexDocumentContent(docId, doc.fileName, contentText, "text/plain");
    REQUIRE(indexResult.has_value());

    auto fuzzyResult = fix.repository_->fuzzySearch(kRareTerm, 0.6f, 10);
    REQUIRE(fuzzyResult.has_value());

    const auto& searchResults = fuzzyResult.value();
    REQUIRE(searchResults.results.size() == 1);
    CHECK(searchResults.results.front().document.id == docId);
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
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    std::string badContent = "alpha";
    badContent.push_back(static_cast<char>(0xFF));
    badContent += "beta";

    auto indexResult =
        fix.repository_->indexDocumentContent(docId, "Bad UTF-8", badContent, "text/plain");
    REQUIRE(indexResult.has_value());

    auto searchResult = fix.repository_->search("alpha", 10, 0);
    REQUIRE(searchResult.has_value());
    const auto& results = searchResult.value();
    REQUIRE_FALSE(results.results.empty());

    bool matchedDocument = false;
    for (const auto& item : results.results) {
        if (item.document.sha256Hash == "search_bad_utf8") {
            matchedDocument = true;
            auto sanitized = common::sanitizeUtf8(item.snippet);
            CHECK(item.snippet == sanitized);
            CHECK(item.snippet.find(static_cast<char>(0xFF)) == std::string::npos);
        }
    }

    CHECK(matchedDocument);
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

    REQUIRE(fix.repository_->insertDocument(d1).has_value());
    REQUIRE(fix.repository_->insertDocument(d2).has_value());
    REQUIRE(fix.repository_->insertDocument(d3).has_value());

    auto total = fix.repository_->getDocumentCount();
    REQUIRE(total.has_value());
    CHECK(total.value() >= 3);

    auto extracted = fix.repository_->getContentExtractedDocumentCount();
    REQUIRE(extracted.has_value());
    CHECK(extracted.value() >= 2);

    auto c_pending = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Pending);
    auto c_success = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Success);
    auto c_failed = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Failed);
    REQUIRE(c_pending.has_value());
    REQUIRE(c_success.has_value());
    REQUIRE(c_failed.has_value());
    CHECK(c_pending.value() >= 1);
    CHECK(c_success.value() >= 1);
    CHECK(c_failed.value() >= 1);

    auto since = clock::now() - std::chrono::seconds(60);
    auto modifiedRes = fix.repository_->findDocumentsModifiedSince(since);
    REQUIRE(modifiedRes.has_value());
    bool foundRecent = false;
    for (const auto& doc : modifiedRes.value()) {
        if (doc.sha256Hash == d2.sha256Hash) {
            foundRecent = true;
            break;
        }
    }
    CHECK(foundRecent);
}

TEST_CASE("MetadataRepository: path tree upsert creates nodes and counts",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/src/example.txt", "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto upsert = fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    REQUIRE(upsert.has_value());

    auto lookup = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(lookup.has_value());
    REQUIRE(lookup.value().has_value());
    auto node = lookup.value().value();
    CHECK(node.fullPath == "/src");
    CHECK(node.docCount == 1);

    auto repeat = fix.repository_->upsertPathTreeForDocument(docInfo, docId, false, {});
    REQUIRE(repeat.has_value());
    auto afterRepeat = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(afterRepeat.has_value());
    REQUIRE(afterRepeat.value().has_value());
    CHECK(afterRepeat.value()->docCount == 1);

    auto fullLookup = fix.repository_->findPathTreeNodeByFullPath("/src/example.txt");
    REQUIRE(fullLookup.has_value());
    REQUIRE(fullLookup.value().has_value());
    CHECK(fullLookup.value()->fullPath == "/src/example.txt");
}

TEST_CASE("MetadataRepository: path tree centroid accumulates embeddings",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/src/lib/foo.cpp", "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    REQUIRE(fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {}).has_value());

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    REQUIRE(
        fix.repository_
            ->upsertPathTreeForDocument(docInfo, docId, false,
                                        std::span<const float>(embedding.data(), embedding.size()))
            .has_value());

    auto node = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    CHECK(node.value()->centroidWeight == 1);
}

TEST_CASE("MetadataRepository: remove path tree decrements counts and deletes empty nodes",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo =
        makeDocumentWithPath("/project/src/lib/util.cpp",
                             "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    auto upsert = fix.repository_->upsertPathTreeForDocument(
        docInfo, docId, true, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE(upsert.has_value());

    auto leafNode = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib/util.cpp");
    REQUIRE(leafNode.has_value());
    REQUIRE(leafNode.value().has_value());
    CHECK(leafNode.value()->docCount == 1);
    CHECK(leafNode.value()->centroidWeight == 1);

    auto libNode = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib");
    REQUIRE(libNode.has_value());
    REQUIRE(libNode.value().has_value());
    CHECK(libNode.value()->docCount == 1);

    auto remove = fix.repository_->removePathTreeForDocument(
        docInfo, docId, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE(remove.has_value());

    auto afterRemoveLeaf = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib/util.cpp");
    REQUIRE(afterRemoveLeaf.has_value());
    CHECK_FALSE(afterRemoveLeaf.value().has_value());

    auto afterRemoveLib = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib");
    REQUIRE(afterRemoveLib.has_value());
    CHECK_FALSE(afterRemoveLib.value().has_value());
}

TEST_CASE("MetadataRepository: remove path tree recalculates centroid",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc1 = makeDocumentWithPath(
        "/shared/file1.txt", "1111111111111111111111111111111111111111111111111111111111111111");
    auto doc1Insert = fix.repository_->insertDocument(doc1);
    REQUIRE(doc1Insert.has_value());
    doc1.id = doc1Insert.value();

    auto doc2 = makeDocumentWithPath(
        "/shared/file2.txt", "2222222222222222222222222222222222222222222222222222222222222222");
    auto doc2Insert = fix.repository_->insertDocument(doc2);
    REQUIRE(doc2Insert.has_value());
    doc2.id = doc2Insert.value();

    std::vector<float> emb1{1.0F, 0.0F, 0.0F};
    std::vector<float> emb2{0.0F, 1.0F, 0.0F};

    auto upsert1 = fix.repository_->upsertPathTreeForDocument(
        doc1, doc1.id, true, std::span<const float>(emb1.data(), emb1.size()));
    REQUIRE(upsert1.has_value());

    auto upsert2 = fix.repository_->upsertPathTreeForDocument(
        doc2, doc2.id, true, std::span<const float>(emb2.data(), emb2.size()));
    REQUIRE(upsert2.has_value());

    auto sharedNode = fix.repository_->findPathTreeNodeByFullPath("/shared");
    REQUIRE(sharedNode.has_value());
    REQUIRE(sharedNode.value().has_value());
    CHECK(sharedNode.value()->docCount == 2);
    CHECK(sharedNode.value()->centroidWeight == 2);

    auto remove1 = fix.repository_->removePathTreeForDocument(
        doc1, doc1.id, std::span<const float>(emb1.data(), emb1.size()));
    REQUIRE(remove1.has_value());

    auto afterRemove = fix.repository_->findPathTreeNodeByFullPath("/shared");
    REQUIRE(afterRemove.has_value());
    REQUIRE(afterRemove.value().has_value());
    CHECK(afterRemove.value()->docCount == 1);
    CHECK(afterRemove.value()->centroidWeight == 1);
}
