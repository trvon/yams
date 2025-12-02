// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for PluginManager component

#include <filesystem>
#include <gtest/gtest.h>

#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/daemon.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

namespace fs = std::filesystem;
using namespace yams::daemon;

namespace yams::daemon::test {

class PluginManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("pm_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);
        dataDir_ = testDir_ / "data";
        fs::create_directories(dataDir_);
        fs::create_directories(testDir_ / "plugins");
    }

    void TearDown() override {
        manager_.reset();
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }

    PluginManager::Dependencies createDeps() {
        PluginManager::Dependencies deps;
        deps.config = &config_;
        deps.state = &state_;
        deps.lifecycleFsm = &lifecycleFsm_;
        deps.dataDir = dataDir_;
        deps.resolvePreferredModel = []() { return std::string{"test-model"}; };
        return deps;
    }

    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    std::unique_ptr<PluginManager> manager_;
    fs::path testDir_;
    fs::path dataDir_;
};

TEST_F(PluginManagerTest, Construction_Succeeds) {
    auto deps = createDeps();
    EXPECT_NO_THROW(manager_ = std::make_unique<PluginManager>(deps));
}

TEST_F(PluginManagerTest, Initialize_Succeeds) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);

    auto result = manager_->initialize();
    EXPECT_TRUE(result.has_value());
}

TEST_F(PluginManagerTest, Shutdown_AfterInitialize) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->shutdown());
}

TEST_F(PluginManagerTest, GetModelProvider_NullBeforeAdoption) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    EXPECT_EQ(manager_->getModelProvider(), nullptr);
}

TEST_F(PluginManagerTest, AdoptModelProvider_NoPlugins) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    auto result = manager_->adoptModelProvider();
    EXPECT_TRUE(result.has_value());
    EXPECT_FALSE(result.value()); // No plugins to adopt
}

TEST_F(PluginManagerTest, AdoptContentExtractors_NoPlugins) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    auto result = manager_->adoptContentExtractors();
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 0u); // No extractors adopted
}

TEST_F(PluginManagerTest, AdoptSymbolExtractors_NoPlugins) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    auto result = manager_->adoptSymbolExtractors();
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 0u); // No extractors adopted
}

TEST_F(PluginManagerTest, IsModelProviderDegraded_FalseInitially) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    EXPECT_FALSE(manager_->isModelProviderDegraded());
}

TEST_F(PluginManagerTest, ClearModelProviderError_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->clearModelProviderError());
}

TEST_F(PluginManagerTest, RefreshStatusSnapshot_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->refreshStatusSnapshot());
}

TEST_F(PluginManagerTest, ShutdownWithoutInitialize_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);

    EXPECT_NO_THROW(manager_->shutdown());
}

TEST_F(PluginManagerTest, MultipleShutdowns_NoThrow) {
    auto deps = createDeps();
    manager_ = std::make_unique<PluginManager>(deps);
    manager_->initialize();

    EXPECT_NO_THROW(manager_->shutdown());
    EXPECT_NO_THROW(manager_->shutdown());
}

} // namespace yams::daemon::test
