// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// The P2pManager inbound session (handshake -> history -> delta exchange -> history -> registry)
// replayed from a length-prefixed frame stream against a fresh registry and memory-sync service.
// Oracles: a session that completes has pinned the TLS peer, and the stream is never over-read.

#include "p2p_fuzz_support.h"

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/peer_registry.h>

#include <fuzzer/FuzzedDataProvider.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unistd.h>

namespace {

std::filesystem::path registryPath() {
    // One registry file per fuzzing process; it is recreated for every input.
    static const auto path = std::filesystem::temp_directory_path() /
                             ("yams-fuzz-p2p-inbound-" + std::to_string(::getpid()) + ".db");
    return path;
}

void removeRegistry() {
    std::error_code ec;
    for (const auto* suffix : {"", "-wal", "-shm", "-journal"}) {
        std::filesystem::remove(registryPath().string() + suffix, ec);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace yams::daemon::p2p;
    using yams::fuzzing::fuzzRequire;
    if (data == nullptr || size > 4 * kP2pMaxControlFrameBytes) {
        return 0;
    }
    FuzzedDataProvider input(data, size);
    detail::InboundSessionOptions options;
    options.nodeId = std::string(yams::fuzzing::kFuzzLocalNodeId);
    options.corpusId = std::string(yams::fuzzing::kFuzzCorpusId);
    options.corpusEpoch = yams::fuzzing::kFuzzCorpusEpoch;
    options.allowFirstContact = input.ConsumeBool();
    options.timeout = std::chrono::milliseconds(50);

    removeRegistry();
    auto registry = PeerRegistry::open(registryPath(), 4);
    if (!registry) {
        return 0;
    }
    auto service = yams::fuzzing::makeMemorySyncService();
    const auto identity = yams::fuzzing::peerChannelIdentity();
    const auto stream = input.ConsumeRemainingBytes<std::uint8_t>();
    detail::BufferFrameSource frames(yams::fuzzing::asBytes(stream.data(), stream.size()));

    const auto outcome =
        detail::runInboundSession(frames, identity, *service, *registry.value(), options);
    fuzzRequire(frames.consumedBytes() <= stream.size());
    if (outcome.result && outcome.stage == detail::InboundSessionStage::RegistryUpdate) {
        // Completing the session means the unknown peer was pinned under first-contact trust.
        fuzzRequire(options.allowFirstContact);
        auto pinned =
            registry.value()->verifyOrPin(identity.peerCertCn, identity.peerSpkiPin, false);
        fuzzRequire(pinned.has_value());
    }
    registry.value().reset();
    removeRegistry();
    return 0;
}
