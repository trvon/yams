// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// Internal parser and state-machine seams shared by the direct-P2P fuzz harnesses and focused
// tests. Every entry point here runs the production code path; the only substitution is the
// FrameSource (a replayed byte stream instead of a TLS socket) and the TLS-derived identity values
// that production code reads from P2pConnection.

// pi-lens-ignore: fatal error
#include <yams/core/types.h>
#include <yams/daemon/p2p/p2p_delta.h>
#include <yams/daemon/p2p/p2p_protocol.h>

#include "p2p_frame_source.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <span>
#include <string>
#include <string_view>

namespace yams::daemon::p2p {

class PeerRegistry;

namespace detail {

Result<void> validateHandshakeControlFrame(std::span<const std::byte> frame);
Result<void> validateDeltaControlFrame(std::span<const std::byte> frame,
                                       const DeltaExchangeOptions& options);

// --- Handshake -------------------------------------------------------------------------------

/// Parse a `hello`/`hello_ack` frame and apply the acceptor-side identity/bounds checks against
/// the TLS certificate CN the channel authenticated.
Result<void> validatePeerHelloFrame(std::span<const std::byte> frame, std::string_view expectedType,
                                    const PeerHandshakeConfig& local, std::string_view peerCertCn);

/// Peer replication state after replacing the TLS peer writer's frontier with its bounded
/// handshake window.
struct BoundedPeerWindow {
    memory_sync::VersionVector version;
    std::map<memory_sync::NodeId, memory_sync::WriterHistoryCommitment> commitments;
};

/// Parse hello + state + writer_window frames and validate the peer window against the
/// negotiated bound (the same checks the handshake applies before any delta traffic).
Result<BoundedPeerWindow> validatePeerWindowFrames(std::span<const std::byte> helloFrame,
                                                   std::span<const std::byte> stateFrame,
                                                   std::span<const std::byte> windowFrame,
                                                   const PeerHandshakeConfig& local);

/// Parse a `history_proof` frame and verify it against the local commitment for `peerNodeId`.
/// Returns whether the proof matches the local prefix.
Result<bool> validatePeerHistoryProofFrame(std::span<const std::byte> frame,
                                           const PeerHandshakeConfig& local,
                                           std::string_view peerNodeId);

Result<PeerHandshakeResult> initiatePeerHandshake(FrameSource& frames,
                                                  const ChannelIdentity& identity,
                                                  const PeerHandshakeConfig& config,
                                                  IPeerTrustStore& trustStore);
Result<PeerHandshakeResult> acceptPeerHandshake(FrameSource& frames,
                                                const ChannelIdentity& identity,
                                                const PeerHandshakeConfig& config,
                                                IPeerTrustStore& trustStore);

// --- Delta exchange --------------------------------------------------------------------------

struct ReceivedDeltaBatch {
    memory_sync::MemoryDeltaBatch batch;
    std::size_t wireBytes{0};
};

struct BootstrapPhaseResult {
    DeltaExchangeStats stats;
    memory_sync::VersionVector peerVersion;
};

/// One `delta_batch` header plus its records, bounded by the remaining session budgets.
Result<ReceivedDeltaBatch> receiveDeltaBatch(FrameSource& frames,
                                             const DeltaExchangeOptions& options,
                                             std::size_t remainingDeltas,
                                             std::size_t remainingBytes);
/// Receiver side of the `replication_mode` / cold-bootstrap phase.
Result<BootstrapPhaseResult> receiveColdBootstrapPhase(FrameSource& frames,
                                                       memory_sync::MemorySyncService& service,
                                                       const PeerHandshakeResult& handshake,
                                                       const DeltaExchangeOptions& options);
/// Receiver side of the bounded delta window (all batches until `has_more=false`).
Result<DeltaExchangeStats> receiveAllDeltas(FrameSource& frames, const std::string& localNodeId,
                                            memory_sync::MemorySyncService& service,
                                            const PeerHandshakeResult& handshake,
                                            const DeltaExchangeOptions& options);
Result<DeltaExchangeStats> initiateDeltaExchange(FrameSource& frames,
                                                 const std::string& localNodeId,
                                                 memory_sync::MemorySyncService& service,
                                                 const PeerHandshakeResult& handshake,
                                                 const DeltaExchangeOptions& options);
Result<DeltaExchangeStats> acceptDeltaExchange(FrameSource& frames, const std::string& localNodeId,
                                               memory_sync::MemorySyncService& service,
                                               const PeerHandshakeResult& handshake,
                                               const DeltaExchangeOptions& options);

// --- Manager inbound session -----------------------------------------------------------------

/// Daemon identity and bounds the manager applies to every inbound session.
struct InboundSessionOptions {
    std::string nodeId;
    std::string corpusId;
    std::uint64_t corpusEpoch{0};
    bool allowFirstContact{false};
    std::chrono::milliseconds timeout{std::chrono::seconds(10)};
};

/// Stage an inbound session reached before returning.
enum class InboundSessionStage : std::uint8_t {
    Handshake,
    PeerHistory,
    DeltaExchange,
    PostExchangeHistory,
    RegistryUpdate,
};

struct InboundSessionOutcome {
    InboundSessionStage stage{InboundSessionStage::Handshake};
    Result<void> result;
};

/// The P2pManager inbound sequence: accept handshake -> enforce peer history -> accept delta
/// exchange -> enforce peer history -> persist the registry row.
InboundSessionOutcome runInboundSession(FrameSource& frames, const ChannelIdentity& identity,
                                        memory_sync::MemorySyncService& service,
                                        PeerRegistry& registry,
                                        const InboundSessionOptions& options);

} // namespace detail
} // namespace yams::daemon::p2p
