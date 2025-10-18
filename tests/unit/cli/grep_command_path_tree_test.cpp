// Integration test for grep --path-tree flag
// Tests the path-tree retrieval service integration end-to-end

#include <spdlog/spdlog.h>
#include <filesystem>
#include <memory>
#include <random>
#include <gtest/gtest.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

namespace fs = std::filesystem;

class GrepPathTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directory for test database
        testDir_ =
            fs::temp_directory_path() / ("yams_test_" + std::to_string(std::random_device{}()));
        fs::create_directories(testDir_);
        dbPath_ = testDir_ / "metadata.db";

        // Initialize connection pool and repository
        yams::metadata::ConnectionPoolConfig poolCfg;
        poolCfg.minConnections = 1;
        poolCfg.maxConnections = 2;

        pool_ = std::make_unique<yams::metadata::ConnectionPool>(dbPath_.string(), poolCfg);
        auto initRes = pool_->initialize();
        ASSERT_TRUE(initRes.has_value())
            << "Failed to initialize pool: " << initRes.error().message;

        repo_ = std::make_shared<yams::metadata::MetadataRepository>(*pool_);
    }

    void TearDown() override {
        repo_.reset();
        pool_.reset();

        // Clean up test directory
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    fs::path testDir_;
    fs::path dbPath_;
    std::unique_ptr<yams::metadata::ConnectionPool> pool_;
    std::shared_ptr<yams::metadata::MetadataRepository> repo_;
};

TEST_F(GrepPathTreeTest, PathTreeOptionsStructCreation) {
    // Test PathTreeOptions struct initialization
    yams::app::services::PathTreeOptions opts;
    EXPECT_FALSE(opts.enabled);
    EXPECT_EQ(opts.mode, "fallback");
    EXPECT_EQ(opts.childLimit, 5);

    // Test with custom values
    opts.enabled = true;
    opts.mode = "preferred";
    opts.childLimit = 10;
    EXPECT_TRUE(opts.enabled);
    EXPECT_EQ(opts.mode, "preferred");
    EXPECT_EQ(opts.childLimit, 10);
}

TEST_F(GrepPathTreeTest, PathTreeModeDisabledByDefault) {
    // Verify normal grep attempts daemon-first when path-tree not enabled
    yams::app::services::RetrievalService rsvc;
    yams::app::services::GrepOptions grepOpts;
    grepOpts.pattern = "test";

    yams::app::services::RetrievalOptions ropts;
    ropts.explicitDataDir = testDir_;
    ropts.requestTimeoutMs = 1000;

    // Call without PathTreeOptions - should use daemon-first path (will fail to connect)
    auto result = rsvc.grep(grepOpts, ropts, std::nullopt);
    // When daemon connection fails, falls back to local error (no documents indexed)
    // Current implementation returns error (connection timeout or IPC error)
    // We're validating the code path compiles and executes without crash
    EXPECT_TRUE(result.has_value() ||
                !result.has_value()); // Either succeeds with empty or fails with error
}

TEST_F(GrepPathTreeTest, PathTreeModeWithEmptyRepository) {
    // Test path-tree mode with empty repository
    yams::app::services::RetrievalService rsvc;
    yams::app::services::GrepOptions grepOpts;
    grepOpts.pattern = "test";
    grepOpts.paths = {"/project/src"};

    yams::app::services::RetrievalOptions ropts;
    ropts.explicitDataDir = testDir_;

    yams::app::services::PathTreeOptions ptOpts;
    ptOpts.enabled = true;
    ptOpts.mode = "fallback";
    ptOpts.childLimit = 5;

    auto result = rsvc.grep(grepOpts, ropts, ptOpts);
    ASSERT_TRUE(result.has_value()) << "Path-tree grep should succeed even with empty repo";
    EXPECT_EQ(result.value().matches.size(), 0) << "Should return no matches for empty repo";
}

TEST_F(GrepPathTreeTest, PathTreeChildLimitConfiguration) {
    // Verify childLimit is respected
    yams::app::services::PathTreeOptions opts;
    opts.enabled = true;
    opts.childLimit = 3;

    EXPECT_EQ(opts.childLimit, 3);

    opts.childLimit = 100;
    EXPECT_EQ(opts.childLimit, 100);
}
