// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_protocol.h>
#include <yams/memory_sync/memory_sync_service.h>

#include <chrono>
#include <cstddef>

namespace yams::daemon::p2p {

struct DeltaExchangeOptions {
    std::size_t maxDeltasPerBatch{128};
    std::size_t maxBatches{4096};
    std::size_t maxDeltasPerSession{1024};
    std::size_t maxWireBytesPerSession{std::size_t{64} * 1024 * 1024};
    std::chrono::milliseconds timeout{std::chrono::seconds(10)};
};

struct DeltaExchangeStats {
    std::size_t batchesSent{0};
    std::size_t batchesReceived{0};
    std::size_t deltasSent{0};
    std::size_t deltasReceived{0};
    std::size_t merged{0};
    std::size_t replayed{0};
    std::size_t quarantined{0};
    std::size_t peerQuarantined{0};
};

/// Client role: send all local deltas the peer lacks, then receive peer deltas.
Result<DeltaExchangeStats> initiateDeltaExchange(P2pConnection& connection,
                                                 memory_sync::MemorySyncService& service,
                                                 const PeerHandshakeResult& handshake,
                                                 const DeltaExchangeOptions& options = {});

/// Server role: receive client deltas, then send all local deltas the peer lacks.
Result<DeltaExchangeStats> acceptDeltaExchange(P2pConnection& connection,
                                               memory_sync::MemorySyncService& service,
                                               const PeerHandshakeResult& handshake,
                                               const DeltaExchangeOptions& options = {});

} // namespace yams::daemon::p2p
