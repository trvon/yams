// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_protocol.h>
#include <yams/metadata/database.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon::p2p {

struct PeerRegistryRecord {
    std::string nodeId;
    std::string spkiPin;
    std::string corpusId;
    std::uint64_t corpusEpoch{0};
    memory_sync::VersionVector lastSeenVersion;
    std::int64_t lastConnectedMs{0};
    bool pinnedByOperator{false};
};

/// SQLite-backed peer trust + resume registry stored in the daemon metadata DB.
/// One dedicated connection is serialized by mutex; statements are per-call RAII
/// objects, so bind/step/reset state is never shared across threads.
class PeerRegistry final : public IPeerTrustStore {
public:
    static Result<std::unique_ptr<PeerRegistry>> open(const std::filesystem::path& databasePath,
                                                      std::size_t maxPeers = 1024);
    ~PeerRegistry() override;

    PeerRegistry(const PeerRegistry&) = delete;
    PeerRegistry& operator=(const PeerRegistry&) = delete;

    Result<PeerTrustDecision> verifyOrPin(std::string_view nodeId, std::string_view spkiPin,
                                          bool allowFirstContact) override;
    [[nodiscard]] std::optional<std::string> pinnedPin(std::string_view nodeId) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto pin = findPinLocked(nodeId);
        return pin ? pin.value() : std::optional<std::string>{};
    }

    Result<void> updatePeerState(std::string_view nodeId, std::string_view corpusId,
                                 std::uint64_t corpusEpoch,
                                 const memory_sync::VersionVector& lastSeenVersion,
                                 std::int64_t lastConnectedMs, bool pinnedByOperator = false);
    Result<std::vector<PeerRegistryRecord>> listPeers() const;
    Result<void> removePeer(std::string_view nodeId);

private:
    PeerRegistry(std::unique_ptr<metadata::Database> database, std::size_t maxPeers);
    Result<void> initializeSchema();
    Result<std::optional<std::string>> findPinLocked(std::string_view nodeId) const;
    Result<std::size_t> peerCountLocked() const;

    mutable std::mutex mutex_;
    std::unique_ptr<metadata::Database> database_;
    std::size_t maxPeers_{1024};
};

} // namespace yams::daemon::p2p
