// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync.h>
#include <yams/storage/storage_backend.h>

namespace yams::memory_sync {

/// Typed configuration for the memory-sync service. The backend itself is injected
/// (so filesystem and S3/R2 resolution stay with the caller, e.g.
/// StorageBackendFactory + resolveStorageBootstrapDecision). No YAMS_* env knobs.
struct MemorySyncConfig {
    std::string nodeId;                 /// this daemon's writer identity
    std::uint32_t syncIntervalMs{5000}; /// periodic reconcile cadence
};

/// Daemon-facing service that runs the memory-sync loop periodically and exposes
/// publish/read/syncOnce. Owns the backend; start/stop are idempotent and stop is
/// noexcept-safe for the shutdown path.
class MemorySyncService {
public:
    MemorySyncService(std::unique_ptr<storage::IStorageBackend> backend, MemorySyncConfig config)
        : backend_(std::move(backend)), config_(std::move(config)),
          loop_(*backend_, config_.nodeId) {}

    ~MemorySyncService() { stop(); }

    MemorySyncService(const MemorySyncService&) = delete;
    MemorySyncService& operator=(const MemorySyncService&) = delete;

    /// Start the periodic sync worker (idempotent).
    Result<void> start() {
        if (worker_.joinable()) {
            return {};
        }
        if (config_.syncIntervalMs == 0) {
            return Error{ErrorCode::InvalidArgument,
                         "memory_sync.syncIntervalMs must be greater than zero"};
        }
        stop_.store(false, std::memory_order_release);
        try {
            worker_ = std::thread([this] { workerLoop(); });
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, e.what()};
        }
        return {};
    }

    /// Stop the periodic worker and join (idempotent, noexcept).
    void stop() noexcept {
        stop_.store(true, std::memory_order_release);
        if (worker_.joinable()) {
            try {
                worker_.join();
            } catch (...) {
            }
        }
    }

    bool started() const noexcept { return worker_.joinable(); }

    Result<void> publish(std::string_view key, std::span<const std::byte> content) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.publish(key, content);
    }

    Result<std::vector<std::byte>> read(std::string_view key) {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.read(key);
    }

    Result<std::map<std::string, MemoryIndexRecord>> syncOnce() {
        std::lock_guard<std::mutex> lock(loopMutex_);
        return loop_.sync();
    }

private:
    void workerLoop() {
        while (!stop_.load(std::memory_order_acquire)) {
            {
                std::lock_guard<std::mutex> lock(loopMutex_);
                (void)loop_.sync();
            }
            for (std::uint32_t waited = 0;
                 waited < config_.syncIntervalMs && !stop_.load(std::memory_order_acquire);
                 waited += 50) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }

    std::unique_ptr<storage::IStorageBackend> backend_;
    MemorySyncConfig config_;
    MemorySyncLoop loop_;
    std::atomic<bool> stop_{false};
    std::thread worker_;
    // Serializes all MemorySyncLoop access (worker sync vs publish/read/syncOnce);
    // the loop keeps mutable version-vector/logical-clock/merged state.
    std::mutex loopMutex_;
};

} // namespace yams::memory_sync
