// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// pi-lens-ignore: fatal error
#include <yams/core/types.h>
#include <yams/daemon/p2p/p2p_transport.h>
#include <yams/memory_sync/memory_sync.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>

namespace yams::daemon::p2p {

inline constexpr std::uint32_t kP2pProtocolVersion = 3;
inline constexpr std::uint32_t kP2pEnvelopeSchemaVersion =
    memory_sync::kAuthenticatedMemoryIndexSchemaVersion;
inline constexpr std::size_t kMaxP2pReplicationWriters = 256;
inline constexpr std::size_t kMaxP2pIdentityBytes = 256;

struct PeerTrustDecision {
    bool firstContactPinned{false};
};

/// Validate and lowercase a SHA-256 SPKI pin for stable storage/comparison.
Result<std::string> normalizePeerSpkiPin(std::string_view spkiPin);
/// Shared trust decision for in-memory and persistent stores.
Result<PeerTrustDecision> evaluatePeerPin(const std::optional<std::string>& existingPin,
                                          std::string_view normalizedPin, bool allowFirstContact);

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
    // pi-lens-ignore: no-bit-fields
    std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment> localCommitments; // NOLINT
    std::set<memory_sync::NodeId> localQuarantinedWriters;
    std::function<Result<memory_sync::WriterHistoryCommitment>(std::uint64_t)>
        resolveLocalCommitment;
    bool allowFirstContact{false};
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};

struct PeerHandshakeResult {
    std::string peerNodeId;
    std::string peerSpkiPin;
    memory_sync::VersionVector localVersion;
    memory_sync::VersionVector peerVersion;
    // pi-lens-ignore: no-bit-fields
    std::map<memory_sync::NodeId, std::uint64_t> peerSeen; // NOLINT(no-bit-fields)
    // pi-lens-ignore: no-bit-fields
    std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment> peerCommitments; // NOLINT
    std::set<memory_sync::NodeId> peerQuarantinedWriters;
    std::uint64_t peerPrefixCounter{0};
    bool peerPrefixVerified{false};
    bool peerPrefixMatches{false};
    bool firstContactPinned{false};

    [[nodiscard]] std::uint64_t peerWatermark(std::string_view writerId) const;
};

/// Compare only the TLS-authenticated peer writer when both sides claim the same
/// counter. Missing or malformed equal-counter commitments fail closed.
Result<bool> requiresPeerWriterQuarantine(const memory_sync::ReplicationState& local,
                                          const PeerHandshakeResult& peer);

/// Client role: hello -> hello_ack -> state -> peer state.
Result<PeerHandshakeResult> initiatePeerHandshake(P2pConnection& connection,
                                                  const PeerHandshakeConfig& config,
                                                  IPeerTrustStore& trustStore);

/// Server role: receive hello -> hello_ack -> receive state -> state.
Result<PeerHandshakeResult> acceptPeerHandshake(P2pConnection& connection,
                                                const PeerHandshakeConfig& config,
                                                IPeerTrustStore& trustStore);

} // namespace yams::daemon::p2p
