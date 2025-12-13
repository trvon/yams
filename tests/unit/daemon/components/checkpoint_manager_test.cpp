// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for CheckpointManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/CheckpointManager.h>

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace {

struct CheckpointManagerFixture {
    std::shared_ptr<boost::asio::io_context> io;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        workGuard;
    std::thread ioThread;
    std::shared_ptr<std::atomic<bool>> stopRequested;
    std::filesystem::path tempDir;

    CheckpointManagerFixture() {
        io = std::make_shared<boost::asio::io_context>();
        workGuard = std::make_unique<
            boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            io->get_executor());
        ioThread = std::thread([this]() { io->run(); });

        stopRequested = std::make_shared<std::atomic<bool>>(false);

        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_checkpoint_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);
    }

    ~CheckpointManagerFixture() {
        stopRequested->store(true);
        workGuard.reset();
        io->stop();
        if (ioThread.joinable()) {
            ioThread.join();
        }

        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    CheckpointManager::Config makeConfig(std::chrono::seconds interval = 1s) {
        CheckpointManager::Config cfg;
        cfg.checkpoint_interval = interval;
        cfg.vector_index_insert_threshold = 100;
        cfg.enable_hotzone_persistence = false;
        cfg.data_dir = tempDir;
        return cfg;
    }

    CheckpointManager::Dependencies makeDeps() {
        CheckpointManager::Dependencies deps;
        deps.vectorSystemManager = nullptr;
        deps.hotzoneManager = nullptr;
        deps.executor = io->get_executor();
        deps.stopRequested = stopRequested;
        return deps;
    }
};

} // namespace

TEST_CASE_METHOD(CheckpointManagerFixture,
                 "CheckpointManager construction and state",
                 "[daemon][components][checkpoint][catch2]") {
    auto cfg = makeConfig();
    auto deps = makeDeps();
    CheckpointManager mgr(cfg, deps);

    SECTION("initial state is not running") {
        CHECK_FALSE(mgr.isRunning());
    }

    SECTION("initial counters are zero") {
        CHECK(mgr.vectorCheckpointCount() == 0);
        CHECK(mgr.hotzoneCheckpointCount() == 0);
        CHECK(mgr.checkpointErrorCount() == 0);
    }

    SECTION("stats are initially zero") {
        CHECK(mgr.lastVectorCheckpointEpoch() == 0);
        CHECK(mgr.lastHotzoneCheckpointEpoch() == 0);
    }
}

TEST_CASE_METHOD(CheckpointManagerFixture,
                 "CheckpointManager start/stop lifecycle",
                 "[daemon][components][checkpoint][catch2]") {
    auto cfg = makeConfig(60s);
    auto deps = makeDeps();
    CheckpointManager mgr(cfg, deps);

    SECTION("start and stop work correctly") {
        CHECK_FALSE(mgr.isRunning());

        mgr.start();
        std::this_thread::sleep_for(50ms);
        CHECK(mgr.isRunning());

        mgr.stop();
        std::this_thread::sleep_for(50ms);
        CHECK_FALSE(mgr.isRunning());
    }

    SECTION("start is idempotent") {
        mgr.start();
        mgr.start();
        mgr.start();

        std::this_thread::sleep_for(50ms);
        CHECK(mgr.isRunning());

        mgr.stop();
    }

    SECTION("stop is idempotent") {
        mgr.start();
        std::this_thread::sleep_for(50ms);

        mgr.stop();
        mgr.stop();
        mgr.stop();

        CHECK_FALSE(mgr.isRunning());
    }
}

TEST_CASE_METHOD(CheckpointManagerFixture,
                 "CheckpointManager manual checkpoint",
                 "[daemon][components][checkpoint][catch2]") {
    SECTION("checkpoint with no dependencies succeeds") {
        auto cfg = makeConfig();
        auto deps = makeDeps();
        CheckpointManager mgr(cfg, deps);

        bool result = mgr.checkpointNow();
        CHECK(result);
        CHECK(mgr.vectorCheckpointCount() == 0);
        CHECK(mgr.hotzoneCheckpointCount() == 0);
        CHECK(mgr.checkpointErrorCount() == 0);
    }

    SECTION("checkpoint with hotzone enabled succeeds") {
        auto cfg = makeConfig();
        cfg.enable_hotzone_persistence = true;
        auto deps = makeDeps();
        CheckpointManager mgr(cfg, deps);

        bool result = mgr.checkpointNow();
        CHECK(result);
    }

    SECTION("multiple manual checkpoints work") {
        auto cfg = makeConfig();
        auto deps = makeDeps();
        CheckpointManager mgr(cfg, deps);

        for (int i = 0; i < 5; ++i) {
            CHECK(mgr.checkpointNow());
        }
        CHECK(mgr.checkpointErrorCount() == 0);
    }
}

TEST_CASE("CheckpointManager config defaults", "[daemon][components][checkpoint][catch2]") {
    CheckpointManager::Config cfg;

    CHECK(cfg.checkpoint_interval.count() == 300);
    CHECK(cfg.vector_index_insert_threshold == 1000);
    CHECK_FALSE(cfg.enable_hotzone_persistence);
}
