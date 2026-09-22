// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Direct-P2P delta receive paths replayed from a length-prefixed frame stream, with the session
// bounds drawn from the input. Oracles: accepted batches and sessions stay inside every count
// and byte budget and never account for more wire bytes than were actually read.

#include "p2p_fuzz_support.h"

#include <fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace yams::daemon::p2p;
    using yams::fuzzing::fuzzRequire;
    if (data == nullptr || size > 4 * kP2pMaxControlFrameBytes) {
        return 0;
    }
    FuzzedDataProvider input(data, size);
    DeltaExchangeOptions options;
    options.maxDeltasPerBatch = input.ConsumeIntegralInRange<std::size_t>(1, 16);
    options.maxBatches = input.ConsumeIntegralInRange<std::size_t>(1, 8);
    options.maxDeltasPerSession = input.ConsumeIntegralInRange<std::size_t>(1, 64);
    options.maxWireBytesPerSession = input.ConsumeIntegralInRange<std::size_t>(1, 256 * 1024);
    options.maxSnapshotRecords = input.ConsumeIntegralInRange<std::size_t>(1, 64);
    options.maxSnapshotWireBytes = input.ConsumeIntegralInRange<std::size_t>(1, 256 * 1024);
    options.timeout = std::chrono::milliseconds(50);
    const int mode = input.ConsumeIntegralInRange<int>(0, 2);
    const auto remainingDeltas = input.ConsumeIntegralInRange<std::size_t>(0, 64);
    const auto remainingBytes = input.ConsumeIntegralInRange<std::size_t>(0, 256 * 1024);

    const auto stream = input.ConsumeRemainingBytes<std::uint8_t>();
    detail::BufferFrameSource frames(yams::fuzzing::asBytes(stream.data(), stream.size()));

    switch (mode) {
        case 0: {
            auto batch =
                detail::receiveDeltaBatch(frames, options, remainingDeltas, remainingBytes);
            if (batch) {
                fuzzRequire(batch.value().batch.deltas.size() <=
                            std::min(remainingDeltas, options.maxDeltasPerBatch));
                fuzzRequire(batch.value().wireBytes <= remainingBytes);
                fuzzRequire(batch.value().wireBytes <= frames.consumedBytes());
            }
            break;
        }
        case 1: {
            auto service = yams::fuzzing::makeMemorySyncService();
            PeerHandshakeResult handshake;
            handshake.peerNodeId = std::string(yams::fuzzing::kFuzzPeerNodeId);
            handshake.peerSpkiPin = std::string(yams::fuzzing::kFuzzPeerPin);
            auto stats = detail::receiveAllDeltas(
                frames, std::string(yams::fuzzing::kFuzzLocalNodeId), *service, handshake, options);
            if (stats) {
                fuzzRequire(stats.value().deltasReceived <= options.maxDeltasPerSession);
                fuzzRequire(stats.value().batchesReceived <= options.maxBatches);
            }
            break;
        }
        default: {
            auto service = yams::fuzzing::makeMemorySyncService();
            PeerHandshakeResult handshake;
            handshake.peerNodeId = std::string(yams::fuzzing::kFuzzPeerNodeId);
            handshake.peerSpkiPin = std::string(yams::fuzzing::kFuzzPeerPin);
            auto phase = detail::receiveColdBootstrapPhase(frames, *service, handshake, options);
            if (phase) {
                fuzzRequire(phase.value().stats.snapshotsReceived <= 1);
            }
            break;
        }
    }
    fuzzRequire(frames.consumedBytes() <= stream.size());
    return 0;
}
