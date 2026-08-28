// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// pi-lens-ignore: fatal error
#include <yams/core/types.h>
#include <yams/daemon/p2p/p2p_transport.h>
#include <yams/memory_sync/version_vector.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace yams::daemon::p2p {

inline constexpr std::uint32_t kP2pProtocolVersion = 1;
inline constexpr std::uint32_t kP2pEnvelopeSchemaVersion =
    memory_sync::kAuthenticatedMemoryIndexSchemaVersion;

struct PeerTrustDecision {
    bool firstContactPinned{false};
};

/// Trust-store seam. #43 uses InMemoryPeerTrustStore; #45 supplies persistent
/// daemon metadata storage without changing handshake semantics.
class IPeerTrustStore {
public:
    virtual ~IPeerTrustStore() = default;

    /// Verify `nodeId -> spkiPin`; pin unknown peers only when `allowFirstContact`
    /// is explicit. Existing-node pin mismatch always fails closed.
    virtual Result<PeerTrustDecision> verifyOrPin(std::string_view nodeId, std::string_view spkiPin,
                                                  bool allowFirstContact) = 0;
    [[nodiscard]] virtual std::optional<std::string> pinnedPin(std::string_view nodeId) const = 0;
};

class InMemoryPeerTrustStore final : public IPeerTrustStore {
public:
    Result<PeerTrustDecision> verifyOrPin(std::string_view nodeId, std::string_view spkiPin,
                                          bool allowFirstContact) override;
    [[nodiscard]] std::optional<std::string> pinnedPin(std::string_view nodeId) const override;

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::string> pins_;
};

struct PeerHandshakeConfig {
    std::string nodeId;
    std::string corpusId;
    std::uint64_t corpusEpoch{0};
    memory_sync::VersionVector localVersion;
    bool allowFirstContact{false};
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};

struct PeerHandshakeResult {
    std::string peerNodeId;
    std::string peerSpkiPin;
    memory_sync::VersionVector peerVersion;
    // pi-lens-ignore: no-bit-fields
    std::map<memory_sync::NodeId, std::uint64_t> peerSeen; // NOLINT(no-bit-fields)
    bool firstContactPinned{false};

    [[nodiscard]] std::uint64_t peerWatermark(std::string_view writerId) const;
};

/// Client role: hello -> hello_ack -> state -> peer state.
Result<PeerHandshakeResult> initiatePeerHandshake(P2pConnection& connection,
                                                  const PeerHandshakeConfig& config,
                                                  IPeerTrustStore& trustStore);

/// Server role: receive hello -> hello_ack -> receive state -> state.
Result<PeerHandshakeResult> acceptPeerHandshake(P2pConnection& connection,
                                                const PeerHandshakeConfig& config,
                                                IPeerTrustStore& trustStore);

} // namespace yams::daemon::p2p
