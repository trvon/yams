// Integration tests for grep --path-tree CLI flag
// Phase 3, Step 4: Tests for CLI integration

#include <gtest/gtest.h>
// removed: yams/app/cli.h (header no longer exists)
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

using namespace yams;
using namespace yams::metadata;

class GrepPathTreeIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = std::filesystem::temp_directory_path() / "yams_grep_path_tree_integration";
        std::filesystem::create_directories(testDir_);
        dbPath_ = testDir_ / "yams.db";

        // Initialize repository with test data
        ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolCfg);
        auto initRes = pool_->initialize();
        ASSERT_TRUE(initRes.has_value());

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
        ASSERT_TRUE(migrate.has_value());

        // Seed test data
        seedTestDocuments();
    }

    void TearDown() override {
        repository_.reset();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir_, ec);
    }

    void seedTestDocuments() {
        // Create documents with path tree structure
        std::vector<std::tuple<std::string, std::string, std::vector<float>>> docs = {
            {"/project/src/main.cpp", "HASH001", {1.0F, 2.0F, 3.0F}},
            {"/project/src/utils.cpp", "HASH002", {1.5F, 2.5F, 3.5F}},
            {"/project/tests/test_main.cpp", "HASH003", {5.0F, 6.0F, 7.0F}},
            {"/project/docs/README.md", "HASH004", {9.0F, 10.0F, 11.0F}},
        };

        for (const auto& [path, hash, embedding] : docs) {
            DocumentInfo doc;
            doc.sha256Hash = hash;
            doc.filePath = path;
            doc.mimeType = "text/plain";
            doc.fileSize = 100;
            auto now = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::now());
            doc.indexedTime = std::chrono::time_point_cast<std::chrono::seconds>(now);
            doc.modifiedTime = std::chrono::time_point_cast<std::chrono::seconds>(now);
            doc.createdTime = std::chrono::time_point_cast<std::chrono::seconds>(now);

            auto insertRes = repository_->insertDocument(doc);
            ASSERT_TRUE(insertRes.has_value());
            doc.id = insertRes.value();

            // Build path tree with embeddings
            auto upsertRes = repository_->upsertPathTreeForDocument(
                doc, doc.id, true, std::span<const float>(embedding.data(), embedding.size()));
            ASSERT_TRUE(upsertRes.has_value());
        }
    }

    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repository_;
};

// TODO: These tests are stubs for Phase 3, Step 4
// Once grep_command.cpp is wired to call RetrievalService with PathTreeOptions,
// these tests will exercise the full CLI → Service → Repository flow

TEST_F(GrepPathTreeIntegrationTest, GrepWithPathTreeFlagParsesCorrectly) {
    // This test verifies that the --path-tree flag is parsed without error
    // Expected behavior: CLI should accept the flag and pass it to the service layer

    // TODO: Once grep_command.cpp calls rsvc.grep(..., PathTreeOptions{...}),
    // this test should verify that PathTreeOptions.enabled = true when flag is set

    GTEST_SKIP() << "CLI wiring pending - Phase 3, Step 3";
}

TEST_F(GrepPathTreeIntegrationTest, GrepPathTreeReturnsChildNodes) {
    // Test: yams grep --path-tree --paths "/project/src" should return child nodes
    // Expected: main.cpp and utils.cpp (2 docs under /project/src)

    // TODO: Implement once service layer returns synthetic matches with matchType="path_tree"

    GTEST_SKIP() << "Service implementation pending";
}

TEST_F(GrepPathTreeIntegrationTest, GrepPathTreeShowsDocCounts) {
    // Test: Path-tree results should include doc counts in output
    // Expected: Each child node shows its doc_count

    GTEST_SKIP() << "Service implementation pending";
}

TEST_F(GrepPathTreeIntegrationTest, GrepPathTreeFallbackModeWorks) {
    // Test: --path-tree mode=fallback should try path-tree first, fallback to normal grep
    // Expected: If path-tree query returns empty, normal grep runs

    GTEST_SKIP() << "Service implementation pending";
}

TEST_F(GrepPathTreeIntegrationTest, GrepPathTreePreferredModeSkipsFallback) {
    // Test: --path-tree mode=preferred should only use path-tree, no fallback
    // Expected: If path-tree returns empty, command returns empty (no normal grep)

    GTEST_SKIP() << "Service implementation pending";
}

TEST_F(GrepPathTreeIntegrationTest, GrepPathTreeLimitsChildren) {
    // Test: childLimit parameter should cap number of children returned
    // Expected: Only top N children by doc_count

    GTEST_SKIP() << "Service implementation pending";
}

// NOTE: To run these tests when Phase 3 is complete:
// 1. Remove GTEST_SKIP() calls
// 2. Add actual CLI invocation or service calls
// 3. Verify output format matches expected synthetic GrepResponse structure
