// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

// Unit tests for path_tree removePathTreeForDocument functionality

#include <chrono>
#include <filesystem>
#include <memory>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

using namespace yams::metadata;

namespace {

std::filesystem::path tempTestDir(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return base / (std::string(prefix) + std::to_string(ts));
}

struct PathTreeDeleteFixture {
    PathTreeDeleteFixture() {
        testDir_ = tempTestDir("yams_path_tree_delete_catch2_test_");
        std::filesystem::create_directories(testDir_);
        dbPath_ = testDir_ / "test.db";

        ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolCfg);
        auto initRes = pool_->initialize();
        REQUIRE(initRes.has_value());

        repository_ = std::make_shared<MetadataRepository>(*pool_);

        // Run migrations
        auto connRes = pool_->acquire();
        REQUIRE(connRes.has_value());
        auto& db = **connRes.value();
        yams::metadata::MigrationManager mgr(db);
        mgr.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
        auto initRes2 = mgr.initialize();
        REQUIRE(initRes2.has_value());
        auto migrate = mgr.migrate();
        REQUIRE(migrate.has_value());
    }

    ~PathTreeDeleteFixture() {
        repository_.reset();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir_, ec);
    }

    DocumentInfo makeDocumentWithPath(const std::string& path, const std::string& hash) {
        DocumentInfo doc;
        doc.sha256Hash = hash;
        doc.filePath = path;
        doc.mimeType = "text/plain";
        doc.fileExtension = ".txt";
        doc.fileSize = 100;
        auto now =
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.indexedTime = now;
        doc.modifiedTime = now;
        doc.createdTime = now;
        return doc;
    }

    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repository_;
};

} // namespace

TEST_CASE("PathTreeDelete: remove document decrements count", "[unit][metadata][path_tree]") {
    PathTreeDeleteFixture fix;

    auto docInfo = fix.makeDocumentWithPath(
        "/src/example.txt", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto upsert = fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    REQUIRE(upsert.has_value());

    auto lookup = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(lookup.has_value());
    REQUIRE(lookup.value().has_value());
    CHECK(lookup.value()->docCount == 1);

    auto remove = fix.repository_->removePathTreeForDocument(docInfo, docId, {});
    REQUIRE(remove.has_value());

    auto afterRemove = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(afterRemove.has_value());
    REQUIRE(afterRemove.value().has_value());
    CHECK(afterRemove.value()->docCount == 0);
}

TEST_CASE("PathTreeDelete: remove document deletes empty nodes", "[unit][metadata][path_tree]") {
    PathTreeDeleteFixture fix;

    auto docInfo = fix.makeDocumentWithPath(
        "/src/lib/example.txt", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto upsert = fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    REQUIRE(upsert.has_value());

    auto srcLookup = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(srcLookup.has_value());
    REQUIRE(srcLookup.value().has_value());

    auto libLookup = fix.repository_->findPathTreeNode(srcLookup.value()->id, "lib");
    REQUIRE(libLookup.has_value());
    REQUIRE(libLookup.value().has_value());

    auto remove = fix.repository_->removePathTreeForDocument(docInfo, docId, {});
    REQUIRE(remove.has_value());

    auto leafLookup = fix.repository_->findPathTreeNodeByFullPath("/src/lib/example.txt");
    REQUIRE(leafLookup.has_value());
    CHECK_FALSE(leafLookup.value().has_value());

    auto libAfter = fix.repository_->findPathTreeNode(srcLookup.value()->id, "lib");
    REQUIRE(libAfter.has_value());
    CHECK_FALSE(libAfter.value().has_value());
}

TEST_CASE("PathTreeDelete: remove document recalculates centroid", "[unit][metadata][path_tree]") {
    PathTreeDeleteFixture fix;

    auto doc1 = fix.makeDocumentWithPath(
        "/src/file1.txt", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
    auto doc1Insert = fix.repository_->insertDocument(doc1);
    REQUIRE(doc1Insert.has_value());
    doc1.id = doc1Insert.value();

    auto doc2 = fix.makeDocumentWithPath(
        "/src/file2.txt", "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    auto doc2Insert = fix.repository_->insertDocument(doc2);
    REQUIRE(doc2Insert.has_value());
    doc2.id = doc2Insert.value();

    std::vector<float> embedding1{1.0F, 2.0F, 3.0F};
    std::vector<float> embedding2{5.0F, 6.0F, 7.0F};

    auto upsert1 = fix.repository_->upsertPathTreeForDocument(
        doc1, doc1.id, true, std::span<const float>(embedding1.data(), embedding1.size()));
    REQUIRE(upsert1.has_value());

    auto upsert2 = fix.repository_->upsertPathTreeForDocument(
        doc2, doc2.id, true, std::span<const float>(embedding2.data(), embedding2.size()));
    REQUIRE(upsert2.has_value());

    auto srcNode = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(srcNode.has_value());
    REQUIRE(srcNode.value().has_value());
    CHECK(srcNode.value()->centroidWeight == 2);
    REQUIRE(srcNode.value()->centroid.size() == 3);

    // Expected centroid: [(1+5)/2, (2+6)/2, (3+7)/2] = [3.0, 4.0, 5.0]
    CHECK(srcNode.value()->centroid[0] == Catch::Approx(3.0F).epsilon(0.01));
    CHECK(srcNode.value()->centroid[1] == Catch::Approx(4.0F).epsilon(0.01));
    CHECK(srcNode.value()->centroid[2] == Catch::Approx(5.0F).epsilon(0.01));

    auto remove = fix.repository_->removePathTreeForDocument(
        doc1, doc1.id, std::span<const float>(embedding1.data(), embedding1.size()));
    REQUIRE(remove.has_value());

    auto srcAfter = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(srcAfter.has_value());
    REQUIRE(srcAfter.value().has_value());
    CHECK(srcAfter.value()->centroidWeight == 1);
    REQUIRE(srcAfter.value()->centroid.size() == 3);
    CHECK(srcAfter.value()->centroid[0] == Catch::Approx(5.0F).epsilon(0.01));
    CHECK(srcAfter.value()->centroid[1] == Catch::Approx(6.0F).epsilon(0.01));
    CHECK(srcAfter.value()->centroid[2] == Catch::Approx(7.0F).epsilon(0.01));
}

TEST_CASE("PathTreeDelete: remove nonexistent document succeeds", "[unit][metadata][path_tree]") {
    PathTreeDeleteFixture fix;

    auto docInfo = fix.makeDocumentWithPath(
        "/nonexistent/file.txt",
        "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    docInfo.id = docInsert.value();

    auto remove = fix.repository_->removePathTreeForDocument(docInfo, docInfo.id, {});
    CHECK(remove.has_value());
}

TEST_CASE("PathTreeDelete: remove document keeps shared ancestors", "[unit][metadata][path_tree]") {
    PathTreeDeleteFixture fix;

    auto doc1 = fix.makeDocumentWithPath(
        "/src/dir1/file1.txt", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
    auto doc1Insert = fix.repository_->insertDocument(doc1);
    REQUIRE(doc1Insert.has_value());
    doc1.id = doc1Insert.value();

    auto doc2 = fix.makeDocumentWithPath(
        "/src/dir2/file2.txt", "1111111111111111111111111111111111111111111111111111111111111111");
    auto doc2Insert = fix.repository_->insertDocument(doc2);
    REQUIRE(doc2Insert.has_value());
    doc2.id = doc2Insert.value();

    auto upsert1 = fix.repository_->upsertPathTreeForDocument(doc1, doc1.id, true, {});
    REQUIRE(upsert1.has_value());

    auto upsert2 = fix.repository_->upsertPathTreeForDocument(doc2, doc2.id, true, {});
    REQUIRE(upsert2.has_value());

    auto srcBefore = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(srcBefore.has_value());
    REQUIRE(srcBefore.value().has_value());
    CHECK(srcBefore.value()->docCount == 2);

    auto remove = fix.repository_->removePathTreeForDocument(doc1, doc1.id, {});
    REQUIRE(remove.has_value());

    auto srcAfter = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(srcAfter.has_value());
    REQUIRE(srcAfter.value().has_value());
    CHECK(srcAfter.value()->docCount == 1);

    auto dir1After = fix.repository_->findPathTreeNodeByFullPath("/src/dir1");
    REQUIRE(dir1After.has_value());
    CHECK_FALSE(dir1After.value().has_value());

    auto dir2After = fix.repository_->findPathTreeNodeByFullPath("/src/dir2");
    REQUIRE(dir2After.has_value());
    CHECK(dir2After.value().has_value());
}
