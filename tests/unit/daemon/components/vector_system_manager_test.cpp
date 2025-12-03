// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for VectorSystemManager component

#include <filesystem>
#include <gtest/gtest.h>

#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/daemon/daemon.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

namespace fs = std::filesystem;
using namespace yams::daemon;

namespace yams::daemon::test {

class VectorSystemManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("vsm_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);
        dataDir_ = testDir_ / "data";
        fs::create_directories(dataDir_);
    }

    void TearDown() override {
        manager_.reset();
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }

    VectorSystemManager::Dependencies createDeps() {
        VectorSystemManager::Dependencies deps;
        deps.state = &state_;
        deps.serviceFsm = nullptr;
        deps.resolvePreferredModel = []() { return std::string{"test-model"}; };
        deps.getEmbeddingDimension = []() { return size_t{384}; };
        return deps;
    }

    StateComponent state_;
    std::unique_ptr<VectorSystemManager> manager_;
    fs::path testDir_;
    fs::path dataDir_;
};

TEST_F(VectorSystemManagerTest, Construction_Succeeds) {
    auto deps = createDeps();
    EXPECT_NO_THROW(manager_ = std::make_unique<VectorSystemManager>(deps));
}

TEST_F(VectorSystemManagerTest, Initialize_Succeeds) {
    auto deps = createDeps();
    manager_ = std::make_unique<VectorSystemManager>(deps);

    auto result = manager_->initialize();
    EXPECT_TRUE(result.has_value());
}

TEST_F(VectorSystemManagerTest, Shutdown_AfterInitialize) {
    auto deps = createDeps();
    manager_ = std::make_unique<VectorSystemManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->shutdown());
}

TEST_F(VectorSystemManagerTest, GetVectorDatabase_NullBeforeInit) {
    auto deps = createDeps();
    manager_ = std::make_unique<VectorSystemManager>(deps);

    EXPECT_EQ(manager_->getVectorDatabase(), nullptr);
}

TEST_F(VectorSystemManagerTest, GetVectorIndexManager_NullBeforeInit) {
    auto deps = createDeps();
    manager_ = std::make_unique<VectorSystemManager>(deps);

    EXPECT_EQ(manager_->getVectorIndexManager(), nullptr);
}

TEST_F(VectorSystemManagerTest, ShutdownWithoutInitialize_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<VectorSystemManager>(deps);

    EXPECT_NO_THROW(manager_->shutdown());
}

TEST_F(VectorSystemManagerTest, MultipleShutdowns_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<VectorSystemManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->shutdown());
    EXPECT_NO_THROW(manager_->shutdown());
}

} // namespace yams::daemon::test
