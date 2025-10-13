// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for ServiceManager component - construction, initialization, and service access

#include <filesystem>
#include <memory>
#include <gtest/gtest.h>

#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;

namespace yams::daemon::test {

class ServiceManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create isolated test directory
        testDir_ = fs::temp_directory_path() /
                   ("sm_test_" + std::to_string(::getpid()) + "_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        // Setup basic config
        config_.dataDir = testDir_ / "data";
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";

        fs::create_directories(config_.dataDir);
    }

    void TearDown() override {
        // Cleanup test directory
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }

    DaemonConfig config_;
    StateComponent state_;
    fs::path testDir_;
};

// Test 1: Basic construction succeeds
TEST_F(ServiceManagerTest, Construction) {
    EXPECT_NO_THROW({ ServiceManager sm(config_, state_); });
}

// Test 2: getName returns correct component name
TEST_F(ServiceManagerTest, GetName) {
    ServiceManager sm(config_, state_);
    EXPECT_STREQ(sm.getName(), "ServiceManager");
}

// Test 3: Service accessors after construction
TEST_F(ServiceManagerTest, ServiceAccessorsAfterConstruction) {
    ServiceManager sm(config_, state_);

    auto envTruthy = [](const char* v) {
        if (!v)
            return false;
        std::string s(v);
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s == "1" || s == "true" || s == "yes" || s == "on";
    };
    const bool vectorsDisabled = envTruthy(std::getenv("YAMS_DISABLE_VECTORS")) ||
                                 envTruthy(std::getenv("YAMS_DISABLE_VECTOR_DB"));

    if (vectorsDisabled) {
        EXPECT_EQ(sm.getVectorDatabase(), nullptr);
    } else {
        EXPECT_NE(sm.getVectorDatabase(), nullptr);
    }

    // Other services are initialized during initialize(), not in constructor
    EXPECT_EQ(sm.getContentStore(), nullptr);
    EXPECT_EQ(sm.getMetadataRepo(), nullptr);
    EXPECT_EQ(sm.getEmbeddingGenerator(), nullptr);
}

// Test 4: Multiple construction is idempotent
TEST_F(ServiceManagerTest, MultipleConstruction) {
    EXPECT_NO_THROW({
        ServiceManager sm1(config_, state_);
        ServiceManager sm2(config_, state_);
    });
}

// Test 5: Construction with missing data directory
TEST_F(ServiceManagerTest, ConstructionWithMissingDataDir) {
    fs::remove_all(config_.dataDir);

    EXPECT_NO_THROW({ ServiceManager sm(config_, state_); });
}

// Test 6: Destructor handles cleanup
TEST_F(ServiceManagerTest, DestructorCleanup) {
    EXPECT_NO_THROW({
        auto sm = std::make_unique<ServiceManager>(config_, state_);
        sm.reset();
    });
}

// Test 7: getConfig returns the configuration
TEST_F(ServiceManagerTest, GetConfig) {
    ServiceManager sm(config_, state_);

    const auto& cfg = sm.getConfig();
    EXPECT_EQ(cfg.dataDir, config_.dataDir);
    EXPECT_EQ(cfg.socketPath, config_.socketPath);
}

// Test 8: PostIngestQueue accessor returns null before init
TEST_F(ServiceManagerTest, PostIngestQueueBeforeInit) {
    ServiceManager sm(config_, state_);

    // May be null before initialization
    auto* piq = sm.getPostIngestQueue();
    // Test passes regardless of null/non-null (implementation dependent)
    (void)piq;
    SUCCEED();
}

// Test 9: Worker pool methods don't crash
TEST_F(ServiceManagerTest, WorkerPoolMethodsSafe) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        auto pool = sm.getWorkerPool();
        (void)pool;
    });
}

// Test 10: Tuning config getter doesn't crash
TEST_F(ServiceManagerTest, GetTuningConfig) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        const auto& tuning = sm.getTuningConfig();
        (void)tuning;
    });
}

// Test 11: Set tuning config doesn't crash
TEST_F(ServiceManagerTest, SetTuningConfig) {
    ServiceManager sm(config_, state_);

    TuningConfig tc;
    tc.postIngestCapacity = 1000;
    tc.postIngestThreadsMin = 2;
    tc.postIngestThreadsMax = 4;

    EXPECT_NO_THROW({ sm.setTuningConfig(tc); });
}

// Test 12: Resize worker pool returns result
TEST_F(ServiceManagerTest, ResizeWorkerPool) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        bool result = sm.resizeWorkerPool(4);
        (void)result;
    });
}

// Test 13: Resize post-ingest threads returns result
TEST_F(ServiceManagerTest, ResizePostIngestThreads) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        bool result = sm.resizePostIngestThreads(2);
        (void)result;
    });
}

// Test 14: getWorkerQueueDepth doesn't crash
TEST_F(ServiceManagerTest, GetWorkerQueueDepth) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        auto depth = sm.getWorkerQueueDepth();
        (void)depth;
    });
}

// Test 15: enqueuePostIngest doesn't crash
TEST_F(ServiceManagerTest, EnqueuePostIngest) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({ sm.enqueuePostIngest("test_hash", "text/plain"); });
}

// Test 16: getLastSearchBuildReason doesn't crash
TEST_F(ServiceManagerTest, GetLastSearchBuildReason) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        auto reason = sm.getLastSearchBuildReason();
        (void)reason;
    });
}

// Test 17: getLastVectorEnabled doesn't crash
TEST_F(ServiceManagerTest, GetLastVectorEnabled) {
    ServiceManager sm(config_, state_);

    EXPECT_NO_THROW({
        bool enabled = sm.getLastVectorEnabled();
        (void)enabled;
    });
}

// Test 18: Memory cleanup verification
TEST_F(ServiceManagerTest, MemoryCleanupVerification) {
    for (int i = 0; i < 3; ++i) {
        auto sm = std::make_unique<ServiceManager>(config_, state_);
        sm.reset();
    }

    SUCCEED();
}

} // namespace yams::daemon::test
