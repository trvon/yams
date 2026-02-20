// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <boost/asio/any_io_executor.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::daemon {

class VectorSystemManager;

}

namespace yams::search {

class HotzoneManager;

}

namespace yams::daemon {

class CheckpointManager {
public:
    struct Config {
        std::chrono::seconds checkpoint_interval{300};
        size_t vector_index_insert_threshold{1000};
        bool enable_hotzone_persistence{false};
        std::filesystem::path data_dir;
    };

    struct Dependencies {
        VectorSystemManager* vectorSystemManager{nullptr};
        search::HotzoneManager* hotzoneManager{nullptr};
        metadata::MetadataRepository* metadataRepository{nullptr};
        boost::asio::any_io_executor executor;
        std::shared_ptr<std::atomic<bool>> stopRequested;
    };

    struct Stats {
        std::atomic<uint64_t> vector_checkpoints{0};
        std::atomic<uint64_t> hotzone_checkpoints{0};
        std::atomic<uint64_t> wal_checkpoints{0};
        std::atomic<uint64_t> checkpoint_errors{0};
        std::atomic<uint64_t> last_vector_checkpoint_epoch{0};
        std::atomic<uint64_t> last_hotzone_checkpoint_epoch{0};
        std::atomic<size_t> last_vector_insert_count{0};
    };

    explicit CheckpointManager(Config config, Dependencies deps);
    ~CheckpointManager();

    CheckpointManager(const CheckpointManager&) = delete;
    CheckpointManager& operator=(const CheckpointManager&) = delete;
    CheckpointManager(CheckpointManager&&) = delete;
    CheckpointManager& operator=(CheckpointManager&&) = delete;

    void start();
    void stop();

    bool isRunning() const noexcept { return running_.load(std::memory_order_acquire); }

    bool checkpointNow();

    uint64_t vectorCheckpointCount() const {
        return stats_.vector_checkpoints.load(std::memory_order_relaxed);
    }
    uint64_t hotzoneCheckpointCount() const {
        return stats_.hotzone_checkpoints.load(std::memory_order_relaxed);
    }
    uint64_t checkpointErrorCount() const {
        return stats_.checkpoint_errors.load(std::memory_order_relaxed);
    }
    uint64_t lastVectorCheckpointEpoch() const {
        return stats_.last_vector_checkpoint_epoch.load(std::memory_order_relaxed);
    }
    uint64_t lastHotzoneCheckpointEpoch() const {
        return stats_.last_hotzone_checkpoint_epoch.load(std::memory_order_relaxed);
    }

private:
    void launchCheckpointLoop();

    bool checkpointVectorIndex();
    bool checkpointHotzone();
    bool checkpointWal();

    Config config_;
    Dependencies deps_;
    Stats stats_;
    std::atomic<bool> running_{false};
};

} // namespace yams::daemon
