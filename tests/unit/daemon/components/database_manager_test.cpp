// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for DatabaseManager component

#include <filesystem>
#include <gtest/gtest.h>

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

namespace fs = std::filesystem;
using namespace yams::daemon;

namespace yams::daemon::test {

class DatabaseManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("dbm_test_" + std::to_string(getpid()) + "_" +
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

    DatabaseManager::Dependencies createDeps() {
        DatabaseManager::Dependencies deps;
        deps.state = &state_;
        return deps;
    }

    StateComponent state_;
    std::unique_ptr<DatabaseManager> manager_;
    fs::path testDir_;
    fs::path dataDir_;
};

TEST_F(DatabaseManagerTest, Construction_Succeeds) {
    auto deps = createDeps();
    EXPECT_NO_THROW(manager_ = std::make_unique<DatabaseManager>(deps));
}

TEST_F(DatabaseManagerTest, Initialize_Succeeds) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);

    auto result = manager_->initialize();
    EXPECT_TRUE(result.has_value());
}

TEST_F(DatabaseManagerTest, Shutdown_AfterInitialize) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->shutdown());
}

TEST_F(DatabaseManagerTest, GetMetadataRepo_AccessorNoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->getMetadataRepo());
}

TEST_F(DatabaseManagerTest, GetKgStore_AccessorNoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->getKgStore());
}

TEST_F(DatabaseManagerTest, DoubleInitialize_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);

    auto result1 = manager_->initialize();
    auto result2 = manager_->initialize();

    EXPECT_TRUE(result1.has_value());
    EXPECT_TRUE(result2.has_value());
}

TEST_F(DatabaseManagerTest, ShutdownWithoutInitialize_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);

    EXPECT_NO_THROW(manager_->shutdown());
}

TEST_F(DatabaseManagerTest, MultipleShutdowns_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<DatabaseManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->shutdown());
    EXPECT_NO_THROW(manager_->shutdown());
}

} // namespace yams::daemon::test
