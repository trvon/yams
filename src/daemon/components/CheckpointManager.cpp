// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/CheckpointManager.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/search/hotzone_manager.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

namespace yams::daemon {

CheckpointManager::CheckpointManager(Config config, Dependencies deps)
    : config_(std::move(config)), deps_(std::move(deps)) {}

CheckpointManager::~CheckpointManager() {
    if (running_.load(std::memory_order_acquire)) {
        stop();
    }
}

void CheckpointManager::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        spdlog::debug("[CheckpointManager] Already running, skipping start");
        return;
    }

    spdlog::info("[CheckpointManager] Starting checkpoint task (interval={}s, insert_threshold={})",
                 config_.checkpoint_interval.count(), config_.vector_index_insert_threshold);

    launchCheckpointLoop();
}

void CheckpointManager::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        spdlog::debug("[CheckpointManager] Not running, skipping stop");
        return;
    }

    spdlog::info("[CheckpointManager] Stopping checkpoint task");
}

bool CheckpointManager::checkpointNow() {
    bool success = true;

    if (!checkpointVectorIndex()) {
        success = false;
    }

    if (config_.enable_hotzone_persistence && !checkpointHotzone()) {
        success = false;
    }

    return success;
}

void CheckpointManager::launchCheckpointLoop() {
    auto exec = deps_.executor;
    auto stopFlag = deps_.stopRequested;
    auto* self = this;

    boost::asio::co_spawn(
        exec,
        [self, stopFlag]() -> boost::asio::awaitable<void> {
            using namespace std::chrono_literals;

            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer timer(executor);

            timer.expires_after(30s);
            try {
                co_await timer.async_wait(boost::asio::use_awaitable);
            } catch (const boost::system::system_error& e) {
                if (e.code() == boost::asio::error::operation_aborted) {
                    co_return;
                }
                throw;
            }

            while (!stopFlag->load(std::memory_order_acquire) &&
                   self->running_.load(std::memory_order_acquire)) {
                // VectorIndexManager removed - checkpoints are no longer needed for vector index
                // VectorDatabase uses SQLite which has its own transaction/WAL mechanism

                timer.expires_after(self->config_.checkpoint_interval);
                try {
                    co_await timer.async_wait(boost::asio::use_awaitable);
                } catch (const boost::system::system_error& e) {
                    if (e.code() == boost::asio::error::operation_aborted) {
                        break;
                    }
                    throw;
                }

                self->checkpointNow();
            }

            spdlog::debug("[CheckpointManager] Checkpoint loop stopped");
            co_return;
        },
        boost::asio::detached);
}

bool CheckpointManager::checkpointVectorIndex() {
    if (!deps_.vectorSystemManager) {
        return true;
    }

    auto vectorDb = deps_.vectorSystemManager->getVectorDatabase();
    if (!vectorDb || !vectorDb->isInitialized()) {
        return true;
    }

    try {
        if (vectorDb->optimizeIndex()) {
            auto now = std::chrono::system_clock::now();
            auto epoch =
                std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
            stats_.last_vector_checkpoint_epoch.store(static_cast<uint64_t>(epoch),
                                                      std::memory_order_relaxed);
            stats_.vector_checkpoints.fetch_add(1, std::memory_order_relaxed);
            spdlog::debug("[CheckpointManager] Vector index checkpoint completed");
            return true;
        }
    } catch (const std::exception& e) {
        spdlog::warn("[CheckpointManager] Vector index checkpoint failed: {}", e.what());
        stats_.checkpoint_errors.fetch_add(1, std::memory_order_relaxed);
    }

    return false;
}

bool CheckpointManager::checkpointHotzone() {
    if (!deps_.hotzoneManager) {
        return true;
    }

    auto hotzonePath = config_.data_dir / "hotzone.json";

    try {
        if (deps_.hotzoneManager->save(hotzonePath)) {
            auto now = std::chrono::system_clock::now();
            auto epoch =
                std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
            stats_.last_hotzone_checkpoint_epoch.store(static_cast<uint64_t>(epoch),
                                                       std::memory_order_relaxed);
            stats_.hotzone_checkpoints.fetch_add(1, std::memory_order_relaxed);

            spdlog::debug("[CheckpointManager] Hotzone checkpoint completed");
            return true;
        }
    } catch (const std::exception& e) {
        spdlog::warn("[CheckpointManager] Hotzone checkpoint failed: {}", e.what());
        stats_.checkpoint_errors.fetch_add(1, std::memory_order_relaxed);
    }

    return false;
}

} // namespace yams::daemon
