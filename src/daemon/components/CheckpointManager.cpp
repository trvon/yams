// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/CheckpointManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/components/VectorSystemManager.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/hotzone_manager.h>
#include <yams/vector/vector_database.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <filesystem>

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
    cv_.notify_all();
    if (checkpointThread_.joinable()) {
        checkpointThread_.join();
    }

    if (deps_.metadataRepository) {
        try {
            auto result = deps_.metadataRepository->checkpointWalTruncate();
            if (result) {
                spdlog::info("[CheckpointManager] Shutdown WAL checkpoint (TRUNCATE) completed");
            } else {
                spdlog::warn("[CheckpointManager] Shutdown WAL TRUNCATE failed: {}",
                             result.error().message);
            }
        } catch (const std::exception& e) {
            spdlog::warn("[CheckpointManager] Shutdown WAL TRUNCATE threw: {}", e.what());
        }
    }
}

bool CheckpointManager::checkpointNow() {
    bool success = true;

    if (!checkpointWal()) {
        success = false;
    }

    if (!checkpointVectorWal()) {
        success = false;
    }

    if (!checkpointVectorIndex()) {
        success = false;
    }

    if (config_.enable_hotzone_persistence && !checkpointHotzone()) {
        success = false;
    }

    return success;
}

void CheckpointManager::launchCheckpointLoop() {
    // Path 1b fix: previously the loop ran via boost::asio::co_spawn on
    // deps_.executor (the ingest WorkCoordinator). Under bulk ingest that
    // executor's worker threads stay saturated with embedding + topology
    // work, so the timer completion for the checkpoint coroutine never gets
    // picked up. Bench logs at 65k-doc ingest showed exactly one
    // "Starting checkpoint task" line followed by zero actual checkpoints
    // over 60+ minutes — the WAL grew to 40+ GB unimpeded.
    //
    // Move the loop onto a dedicated std::thread with condition_variable-
    // based wait so shutdown still interrupts promptly (no 5-min sleeps
    // blocking stop()), but the loop itself is immune to executor pressure.
    auto stopFlag = deps_.stopRequested;
    checkpointThread_ = std::thread([this, stopFlag]() {
        using namespace std::chrono_literals;
        std::unique_lock lock(cvMutex_);
        // Initial 30s warmup (matches the prior asio-based timing).
        cv_.wait_for(lock, 30s, [this, &stopFlag]() {
            return (stopFlag && stopFlag->load(std::memory_order_acquire)) ||
                   !running_.load(std::memory_order_acquire);
        });

        while (running_.load(std::memory_order_acquire) &&
               (!stopFlag || !stopFlag->load(std::memory_order_acquire))) {
            cv_.wait_for(lock, config_.checkpoint_interval, [this, &stopFlag]() {
                return (stopFlag && stopFlag->load(std::memory_order_acquire)) ||
                       !running_.load(std::memory_order_acquire);
            });
            if (!running_.load(std::memory_order_acquire) ||
                (stopFlag && stopFlag->load(std::memory_order_acquire))) {
                break;
            }
            // Release the lock while performing the (potentially long-running)
            // checkpoint so stop() can wake us via notify without contending.
            lock.unlock();
            try {
                checkpointNow();
            } catch (const std::exception& e) {
                spdlog::warn("[CheckpointManager] checkpoint thread exception: {}", e.what());
                stats_.checkpoint_errors.fetch_add(1, std::memory_order_relaxed);
            }
            lock.lock();
        }

        spdlog::debug("[CheckpointManager] Checkpoint loop stopped");
    });
}

bool CheckpointManager::checkpointVectorIndex() {
    if (!deps_.vectorSystemManager) {
        return true;
    }

    if (deps_.state) {
        const bool repairInProgress =
            deps_.state->stats.repairInProgress.load(std::memory_order_relaxed);
        const auto repairQueueDepth =
            deps_.state->stats.repairQueueDepth.load(std::memory_order_relaxed);
        if (repairInProgress || repairQueueDepth > 0) {
            spdlog::debug("[CheckpointManager] Skipping vector index persistence during repair "
                          "(in_progress={} queue_depth={})",
                          repairInProgress ? 1 : 0, repairQueueDepth);
            return true;
        }
    }

    auto vectorDb = deps_.vectorSystemManager->getVectorDatabase();
    if (!vectorDb || !vectorDb->isInitialized()) {
        return true;
    }

    // Route through the coordinator when available so the persist runs serialised
    // on the same strand that owns rebuild/finalize. Falling back to direct
    // persistIndex() is only safe when the coordinator isn't wired yet (boot).
    try {
        bool ok = false;
        if (deps_.vectorIndexCoordinator) {
            ok = deps_.vectorIndexCoordinator->requestCheckpoint();
        } else {
            ok = vectorDb->persistIndex();
        }
        if (ok) {
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

bool CheckpointManager::checkpointWal() {
    if (!deps_.metadataRepository) {
        return true;
    }

    // When the WAL grows beyond the watermark, skip PASSIVE entirely and go
    // straight to TRUNCATE with retries. PASSIVE cannot shrink the WAL when
    // readers hold shared locks — it will silently checkpoint zero pages.
    constexpr std::uint64_t kTruncateWatermarkBytes = 256ULL * 1024ULL * 1024ULL; // 256 MiB

    auto walSizeBytes = [&]() -> std::optional<uint64_t> {
        if (config_.data_dir.empty())
            return std::nullopt;
        std::error_code ec;
        auto sz = std::filesystem::file_size(config_.data_dir / "yams.db-wal", ec);
        return ec ? std::nullopt : std::optional<uint64_t>(sz);
    };

    bool wantsTruncate = false;
    auto preSize = walSizeBytes();
    if (preSize && *preSize >= kTruncateWatermarkBytes) {
        wantsTruncate = true;
        spdlog::info("[CheckpointManager] yams.db-wal is {} MB (>= {} MiB watermark), "
                     "escalating to TRUNCATE checkpoint",
                     *preSize / (1024ULL * 1024ULL), kTruncateWatermarkBytes / (1024ULL * 1024ULL));
    }

    if (wantsTruncate) {
        // ── TRUNCATE with retry (exclusive-lock contention is common under load) ──
        constexpr int kMaxRetries = 3;
        for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
            if (attempt > 0) {
                const int delayMs = (attempt == 1 ? 100 : (attempt == 2 ? 500 : 2000));
                spdlog::info("[CheckpointManager] TRUNCATE retry {}/{} after {}ms", attempt + 1,
                             kMaxRetries, delayMs);
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            }
            try {
                auto result = deps_.metadataRepository->checkpointWalTruncate();
                if (result) {
                    stats_.wal_checkpoints.fetch_add(1, std::memory_order_relaxed);
                    auto postSize = walSizeBytes();
                    spdlog::info("[CheckpointManager] WAL checkpoint (TRUNCATE) completed  "
                                 "{} MB -> {} MB",
                                 preSize ? std::to_string(*preSize / (1024 * 1024)) : "?",
                                 postSize ? std::to_string(*postSize / (1024 * 1024)) : "?");
                    return true;
                }
                spdlog::warn("[CheckpointManager] TRUNCATE attempt {} failed: {}", attempt + 1,
                             result.error().message);
            } catch (const std::exception& e) {
                spdlog::warn("[CheckpointManager] TRUNCATE attempt {} exception: {}", attempt + 1,
                             e.what());
            }
        }
        stats_.checkpoint_errors.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("[CheckpointManager] TRUNCATE failed after {} retries — WAL at {} MB",
                     kMaxRetries, preSize ? std::to_string(*preSize / (1024 * 1024)) : "?");
        return false;
    }

    // ── Routine PASSIVE checkpoint ──
    try {
        auto result = deps_.metadataRepository->checkpointWal();
        if (result) {
            stats_.wal_checkpoints.fetch_add(1, std::memory_order_relaxed);
            spdlog::debug("[CheckpointManager] WAL checkpoint (PASSIVE) completed");
            // Post-PASSIVE re-check: if WAL grew past the watermark between
            // the pre-check and now, schedule TRUNCATE for the next cycle by
            // leaving it alone (it'll be caught above on next interval).
            auto postSize = walSizeBytes();
            if (postSize && *postSize >= kTruncateWatermarkBytes) {
                spdlog::info("[CheckpointManager] WAL at {} MB after PASSIVE — "
                             "will TRUNCATE on next cycle",
                             *postSize / (1024ULL * 1024ULL));
            }
            return true;
        }
        spdlog::warn("[CheckpointManager] WAL checkpoint (PASSIVE) failed: {}",
                     result.error().message);
    } catch (const std::exception& e) {
        spdlog::warn("[CheckpointManager] WAL checkpoint exception: {}", e.what());
        stats_.checkpoint_errors.fetch_add(1, std::memory_order_relaxed);
    }

    return false;
}

bool CheckpointManager::checkpointVectorWal() {
    if (!deps_.vectorSystemManager) {
        return true;
    }

    auto vectorDb = deps_.vectorSystemManager->getVectorDatabase();
    if (!vectorDb || !vectorDb->isInitialized()) {
        return true;
    }

    try {
        auto result = vectorDb->checkpointWal();
        if (result) {
            spdlog::debug("[CheckpointManager] vectors.db WAL checkpoint completed");
            return true;
        }
        spdlog::warn("[CheckpointManager] vectors.db WAL checkpoint failed: {}",
                     result.error().message);
    } catch (const std::exception& e) {
        spdlog::warn("[CheckpointManager] vectors.db WAL checkpoint exception: {}", e.what());
        stats_.checkpoint_errors.fetch_add(1, std::memory_order_relaxed);
    }

    return false;
}

} // namespace yams::daemon
