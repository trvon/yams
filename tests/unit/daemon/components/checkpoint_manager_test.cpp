// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for CheckpointManager component (PBI-090)

#include <gtest/gtest.h>

#include <yams/daemon/components/CheckpointManager.h>

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace yams::daemon::test {

class CheckpointManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        io_ = std::make_shared<boost::asio::io_context>();
        work_guard_ = std::make_unique<
            boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            io_->get_executor());
        io_thread_ = std::thread([this]() { io_->run(); });

        stopRequested_ = std::make_shared<std::atomic<bool>>(false);

        temp_dir_ = std::filesystem::temp_directory_path() / "yams_checkpoint_test";
        std::filesystem::create_directories(temp_dir_);
    }

    void TearDown() override {
        stopRequested_->store(true);
        work_guard_.reset();
        io_->stop();
        if (io_thread_.joinable()) {
            io_thread_.join();
        }

        std::error_code ec;
        std::filesystem::remove_all(temp_dir_, ec);
    }

    CheckpointManager::Config makeConfig(std::chrono::seconds interval = 1s) {
        CheckpointManager::Config cfg;
        cfg.checkpoint_interval = interval;
        cfg.vector_index_insert_threshold = 100;
        cfg.enable_hotzone_persistence = false;
        cfg.data_dir = temp_dir_;
        return cfg;
    }

    CheckpointManager::Dependencies makeDeps() {
        CheckpointManager::Dependencies deps;
        deps.vectorSystemManager = nullptr;
        deps.hotzoneManager = nullptr;
        deps.executor = io_->get_executor();
        deps.stopRequested = stopRequested_;
        return deps;
    }

    std::shared_ptr<boost::asio::io_context> io_;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;
    std::thread io_thread_;
    std::shared_ptr<std::atomic<bool>> stopRequested_;
    std::filesystem::path temp_dir_;
};

TEST_F(CheckpointManagerTest, Construction) {
    auto cfg = makeConfig();
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    EXPECT_FALSE(mgr.isRunning());
    EXPECT_EQ(mgr.vectorCheckpointCount(), 0);
    EXPECT_EQ(mgr.hotzoneCheckpointCount(), 0);
    EXPECT_EQ(mgr.checkpointErrorCount(), 0);
}

TEST_F(CheckpointManagerTest, StartStop) {
    auto cfg = makeConfig(60s);
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    EXPECT_FALSE(mgr.isRunning());

    mgr.start();
    std::this_thread::sleep_for(50ms);
    EXPECT_TRUE(mgr.isRunning());

    mgr.stop();
    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(mgr.isRunning());
}

TEST_F(CheckpointManagerTest, StartIdempotent) {
    auto cfg = makeConfig(60s);
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    mgr.start();
    mgr.start();
    mgr.start();

    std::this_thread::sleep_for(50ms);
    EXPECT_TRUE(mgr.isRunning());

    mgr.stop();
}

TEST_F(CheckpointManagerTest, StopIdempotent) {
    auto cfg = makeConfig(60s);
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    mgr.start();
    std::this_thread::sleep_for(50ms);

    mgr.stop();
    mgr.stop();
    mgr.stop();

    EXPECT_FALSE(mgr.isRunning());
}

TEST_F(CheckpointManagerTest, CheckpointNow_NoDependencies) {
    auto cfg = makeConfig();
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    bool result = mgr.checkpointNow();
    EXPECT_TRUE(result);

    EXPECT_EQ(mgr.vectorCheckpointCount(), 0);
    EXPECT_EQ(mgr.hotzoneCheckpointCount(), 0);
    EXPECT_EQ(mgr.checkpointErrorCount(), 0);
}

TEST_F(CheckpointManagerTest, ConfigDefaults) {
    CheckpointManager::Config cfg;

    EXPECT_EQ(cfg.checkpoint_interval.count(), 300);
    EXPECT_EQ(cfg.vector_index_insert_threshold, 1000);
    EXPECT_FALSE(cfg.enable_hotzone_persistence);
}

TEST_F(CheckpointManagerTest, StatsInitiallyZero) {
    auto cfg = makeConfig();
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    EXPECT_EQ(mgr.vectorCheckpointCount(), 0);
    EXPECT_EQ(mgr.hotzoneCheckpointCount(), 0);
    EXPECT_EQ(mgr.checkpointErrorCount(), 0);
    EXPECT_EQ(mgr.lastVectorCheckpointEpoch(), 0);
    EXPECT_EQ(mgr.lastHotzoneCheckpointEpoch(), 0);
}

TEST_F(CheckpointManagerTest, DestructorStops) {
    auto cfg = makeConfig(60s);
    auto deps = makeDeps();

    {
        CheckpointManager mgr(cfg, deps);
        mgr.start();
        std::this_thread::sleep_for(50ms);
        EXPECT_TRUE(mgr.isRunning());
    }
}

TEST_F(CheckpointManagerTest, CheckpointNow_WithHotzoneEnabled) {
    auto cfg = makeConfig();
    cfg.enable_hotzone_persistence = true;
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    bool result = mgr.checkpointNow();
    EXPECT_TRUE(result);
}

TEST_F(CheckpointManagerTest, MultipleManualCheckpoints) {
    auto cfg = makeConfig();
    auto deps = makeDeps();

    CheckpointManager mgr(cfg, deps);

    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(mgr.checkpointNow());
    }

    EXPECT_EQ(mgr.checkpointErrorCount(), 0);
}

} // namespace yams::daemon::test
