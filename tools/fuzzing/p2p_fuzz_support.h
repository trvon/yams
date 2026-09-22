// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// Shared fixtures for the direct-P2P fuzz harnesses: an in-memory storage backend, a memory-sync
// service over it, a handshake config for that service, and a trap-on-violation oracle helper.

#include "../../src/daemon/p2p/p2p_fuzz.h"

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_protocol.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_backend.h>

#include <chrono>
#include <cstddef>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace yams::fuzzing {

/// Abort the run when a harness oracle is violated, so libFuzzer and AFL++ record a crash.
inline void fuzzRequire(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

inline std::span<const std::byte> asBytes(const std::uint8_t* data, std::size_t size) {
    return {reinterpret_cast<const std::byte*>(data), size};
}

class InMemoryBackend final : public storage::IStorageBackend {
public:
    Result<void> initialize(const storage::BackendConfig&) override { return {}; }

    Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        std::lock_guard lock(mutex_);
        objects_[std::string(key)] = std::vector<std::byte>(data.begin(), data.end());
        return {};
    }

    Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        std::lock_guard lock(mutex_);
        const auto found = objects_.find(std::string(key));
        if (found == objects_.end()) {
            return Error{ErrorCode::NotFound, "fuzz object is absent"};
        }
        return found->second;
    }

    Result<bool> exists(std::string_view key) const override {
        std::lock_guard lock(mutex_);
        return objects_.contains(std::string(key));
    }

    Result<void> remove(std::string_view key) override {
        std::lock_guard lock(mutex_);
        objects_.erase(std::string(key));
        return {};
    }

    Result<std::vector<std::string>> list(std::string_view prefix = "") const override {
        std::lock_guard lock(mutex_);
        std::vector<std::string> keys;
        for (const auto& [key, _] : objects_) {
            if (key.starts_with(prefix)) {
                keys.push_back(key);
            }
        }
        return keys;
    }

    Result<StorageStats> getStats() const override { return StorageStats{}; }

    std::future<Result<void>> storeAsync(std::string_view key,
                                         std::span<const std::byte> data) override {
        std::promise<Result<void>> done;
        done.set_value(store(key, data));
        return done.get_future();
    }

    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view key) const override {
        std::promise<Result<std::vector<std::byte>>> done;
        done.set_value(retrieve(key));
        return done.get_future();
    }

    std::string getType() const override { return "fuzz-memory"; }
    bool isRemote() const override { return false; }
    Result<void> flush() override { return {}; }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::vector<std::byte>> objects_;
};

inline constexpr std::string_view kFuzzCorpusId = "fuzz-corpus";
inline constexpr std::uint64_t kFuzzCorpusEpoch = 1;
inline constexpr std::string_view kFuzzLocalNodeId = "fuzz-local";
inline constexpr std::string_view kFuzzPeerNodeId = "fuzz-peer";
/// 64 lowercase hex characters: the shape of a normalized SHA-256 SPKI pin.
inline constexpr std::string_view kFuzzPeerPin =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

inline std::unique_ptr<memory_sync::MemorySyncService> makeMemorySyncService() {
    return std::make_unique<memory_sync::MemorySyncService>(
        std::make_unique<InMemoryBackend>(),
        memory_sync::MemorySyncConfig{std::string(kFuzzLocalNodeId), 60'000,
                                      std::string(kFuzzCorpusId), kFuzzCorpusEpoch});
}

inline daemon::p2p::PeerHandshakeConfig makeHandshakeConfig(memory_sync::MemorySyncService& service,
                                                            bool allowFirstContact) {
    const auto state = service.replicationState();
    return daemon::p2p::PeerHandshakeConfig{
        .nodeId = std::string(kFuzzLocalNodeId),
        .corpusId = std::string(kFuzzCorpusId),
        .corpusEpoch = kFuzzCorpusEpoch,
        .localVersion = state.version,
        .localCommitments = state.commitments,
        .localQuarantinedWriters = state.quarantinedWriters,
        .resolveLocalCommitment =
            [&service](std::uint64_t counter) { return service.localHistoryCommitmentAt(counter); },
        .resolveLocalWindow =
            [&service](std::uint64_t peerCounter, std::size_t maxRecords,
                       std::size_t maxWireBytes) {
                return service.localHistoryWindowAfter(peerCounter, maxRecords, maxWireBytes);
            },
        .allowFirstContact = allowFirstContact,
        .timeout = std::chrono::milliseconds(50)};
}

inline daemon::p2p::detail::ChannelIdentity peerChannelIdentity() {
    return daemon::p2p::detail::ChannelIdentity{.localNodeId = std::string(kFuzzLocalNodeId),
                                                .peerCertCn = std::string(kFuzzPeerNodeId),
                                                .peerSpkiPin = std::string(kFuzzPeerPin)};
}

} // namespace yams::fuzzing
