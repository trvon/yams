// Unit tests for path_tree removePathTreeForDocument functionality
// Phase 1, Step 6: Tests for delete operation

#include <filesystem>
#include <memory>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

using namespace yams::metadata;

class PathTreeDeleteTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = std::filesystem::temp_directory_path() / "yams_path_tree_delete_test";
        std::filesystem::create_directories(testDir_);
        dbPath_ = testDir_ / "test.db";

        ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolCfg);
        auto initRes = pool_->initialize();
        ASSERT_TRUE(initRes.has_value()) << "Pool init failed: " << initRes.error().message;

        repository_ = std::make_shared<MetadataRepository>(*pool_);

        // Run migrations
        auto connRes = pool_->acquire();
        ASSERT_TRUE(connRes.has_value());
        auto& db = **connRes.value();
        yams::metadata::MigrationManager mgr(db);
        mgr.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
        auto initRes2 = mgr.initialize();
        ASSERT_TRUE(initRes2.has_value());
        auto migrate = mgr.migrate();
        ASSERT_TRUE(migrate.has_value()) << "Migration failed: " << migrate.error().message;
    }

    void TearDown() override {
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

TEST_F(PathTreeDeleteTest, RemoveDocumentDecrementsCount) {
    // Setup: Insert a document and build path tree
    auto docInfo = makeDocumentWithPath(
        "/src/example.txt", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docInsert = repository_->insertDocument(docInfo);
    ASSERT_TRUE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    // Build path tree with doc count
    auto upsert = repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    ASSERT_TRUE(upsert.has_value());

    // Verify initial state
    auto lookup = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(lookup.has_value());
    ASSERT_TRUE(lookup.value().has_value());
    EXPECT_EQ(lookup.value()->docCount, 1);

    // Act: Remove the document
    auto remove = repository_->removePathTreeForDocument(docInfo, docId, {});
    ASSERT_TRUE(remove.has_value()) << "Remove failed: " << remove.error().message;

    // Assert: Doc count should be decremented
    auto afterRemove = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(afterRemove.has_value());
    ASSERT_TRUE(afterRemove.value().has_value());
    EXPECT_EQ(afterRemove.value()->docCount, 0);
}

TEST_F(PathTreeDeleteTest, RemoveDocumentDeletesEmptyNodes) {
    // Setup: Insert a document
    auto docInfo = makeDocumentWithPath(
        "/src/lib/example.txt", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto docInsert = repository_->insertDocument(docInfo);
    ASSERT_TRUE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto upsert = repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    ASSERT_TRUE(upsert.has_value());

    // Verify nodes exist
    auto srcLookup = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(srcLookup.has_value() && srcLookup.value().has_value());

    auto libLookup = repository_->findPathTreeNode(srcLookup.value()->id, "lib");
    ASSERT_TRUE(libLookup.has_value() && libLookup.value().has_value());

    // Act: Remove the document
    auto remove = repository_->removePathTreeForDocument(docInfo, docId, {});
    ASSERT_TRUE(remove.has_value());

    // Assert: Leaf nodes should be deleted (no children, no docs)
    auto leafLookup = repository_->findPathTreeNodeByFullPath("/src/lib/example.txt");
    ASSERT_TRUE(leafLookup.has_value());
    EXPECT_FALSE(leafLookup.value().has_value()) << "Leaf node should be deleted";

    // lib should also be deleted (no children, no docs)
    auto libAfter = repository_->findPathTreeNode(srcLookup.value()->id, "lib");
    ASSERT_TRUE(libAfter.has_value());
    EXPECT_FALSE(libAfter.value().has_value()) << "lib node should be deleted";
}

TEST_F(PathTreeDeleteTest, RemoveDocumentRecalculatesCentroid) {
    // Setup: Insert two documents with embeddings
    auto doc1 = makeDocumentWithPath(
        "/src/file1.txt", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
    auto doc1Insert = repository_->insertDocument(doc1);
    ASSERT_TRUE(doc1Insert.has_value());
    doc1.id = doc1Insert.value();

    auto doc2 = makeDocumentWithPath(
        "/src/file2.txt", "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    auto doc2Insert = repository_->insertDocument(doc2);
    ASSERT_TRUE(doc2Insert.has_value());
    doc2.id = doc2Insert.value();

    // Add with embeddings
    std::vector<float> embedding1{1.0F, 2.0F, 3.0F};
    std::vector<float> embedding2{5.0F, 6.0F, 7.0F};

    auto upsert1 = repository_->upsertPathTreeForDocument(
        doc1, doc1.id, true, std::span<const float>(embedding1.data(), embedding1.size()));
    ASSERT_TRUE(upsert1.has_value());

    auto upsert2 = repository_->upsertPathTreeForDocument(
        doc2, doc2.id, true, std::span<const float>(embedding2.data(), embedding2.size()));
    ASSERT_TRUE(upsert2.has_value());

    // Verify centroid was calculated (average of two embeddings)
    auto srcNode = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(srcNode.has_value() && srcNode.value().has_value());
    EXPECT_EQ(srcNode.value()->centroidWeight, 2);
    ASSERT_EQ(srcNode.value()->centroid.size(), 3u);

    // Expected centroid: [(1+5)/2, (2+6)/2, (3+7)/2] = [3.0, 4.0, 5.0]
    EXPECT_NEAR(srcNode.value()->centroid[0], 3.0F, 0.01F);
    EXPECT_NEAR(srcNode.value()->centroid[1], 4.0F, 0.01F);
    EXPECT_NEAR(srcNode.value()->centroid[2], 5.0F, 0.01F);

    // Act: Remove doc1
    auto remove = repository_->removePathTreeForDocument(
        doc1, doc1.id, std::span<const float>(embedding1.data(), embedding1.size()));
    ASSERT_TRUE(remove.has_value());

    // Assert: Centroid should now be embedding2
    auto srcAfter = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(srcAfter.has_value() && srcAfter.value().has_value());
    EXPECT_EQ(srcAfter.value()->centroidWeight, 1);
    ASSERT_EQ(srcAfter.value()->centroid.size(), 3u);
    EXPECT_NEAR(srcAfter.value()->centroid[0], 5.0F, 0.01F);
    EXPECT_NEAR(srcAfter.value()->centroid[1], 6.0F, 0.01F);
    EXPECT_NEAR(srcAfter.value()->centroid[2], 7.0F, 0.01F);
}

TEST_F(PathTreeDeleteTest, RemoveNonExistentDocumentSucceeds) {
    // Setup: Create a document but don't add to path tree
    auto docInfo =
        makeDocumentWithPath("/nonexistent/file.txt",
                             "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");
    auto docInsert = repository_->insertDocument(docInfo);
    ASSERT_TRUE(docInsert.has_value());
    docInfo.id = docInsert.value();

    // Act: Remove (should succeed silently)
    auto remove = repository_->removePathTreeForDocument(docInfo, docInfo.id, {});
    ASSERT_TRUE(remove.has_value()) << "Remove should succeed even if path doesn't exist";
}

TEST_F(PathTreeDeleteTest, RemoveDocumentKeepsSharedAncestors) {
    // Setup: Two documents sharing /src prefix
    auto doc1 = makeDocumentWithPath(
        "/src/dir1/file1.txt", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
    auto doc1Insert = repository_->insertDocument(doc1);
    ASSERT_TRUE(doc1Insert.has_value());
    doc1.id = doc1Insert.value();

    auto doc2 = makeDocumentWithPath(
        "/src/dir2/file2.txt", "1111111111111111111111111111111111111111111111111111111111111111");
    auto doc2Insert = repository_->insertDocument(doc2);
    ASSERT_TRUE(doc2Insert.has_value());
    doc2.id = doc2Insert.value();

    auto upsert1 = repository_->upsertPathTreeForDocument(doc1, doc1.id, true, {});
    ASSERT_TRUE(upsert1.has_value());

    auto upsert2 = repository_->upsertPathTreeForDocument(doc2, doc2.id, true, {});
    ASSERT_TRUE(upsert2.has_value());

    // Verify /src has doc_count = 2
    auto srcBefore = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(srcBefore.has_value() && srcBefore.value().has_value());
    EXPECT_EQ(srcBefore.value()->docCount, 2);

    // Act: Remove doc1
    auto remove = repository_->removePathTreeForDocument(doc1, doc1.id, {});
    ASSERT_TRUE(remove.has_value());

    // Assert: /src should still exist with doc_count = 1
    auto srcAfter = repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    ASSERT_TRUE(srcAfter.has_value() && srcAfter.value().has_value());
    EXPECT_EQ(srcAfter.value()->docCount, 1);

    // /src/dir1 should be deleted
    auto dir1After = repository_->findPathTreeNodeByFullPath("/src/dir1");
    ASSERT_TRUE(dir1After.has_value());
    EXPECT_FALSE(dir1After.value().has_value());

    // /src/dir2 should still exist
    auto dir2After = repository_->findPathTreeNodeByFullPath("/src/dir2");
    ASSERT_TRUE(dir2After.has_value());
    EXPECT_TRUE(dir2After.value().has_value());
}
