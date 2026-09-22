// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Direct-P2P handshake: the individual peer-frame validators, and the full acceptor state
// machine replayed from a length-prefixed frame stream. Oracles: an accepted handshake binds the
// TLS-authenticated peer identity and never reads past the supplied stream.

#include "p2p_fuzz_support.h"

#include <fuzzer/FuzzedDataProvider.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

std::vector<std::byte> takeFrame(FuzzedDataProvider& input) {
    const auto size = input.ConsumeIntegralInRange<std::size_t>(0, 64 * 1024);
    auto bytes = input.ConsumeBytes<std::uint8_t>(size);
    std::vector<std::byte> frame(bytes.size());
    std::memcpy(frame.data(), bytes.data(), bytes.size());
    return frame;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace yams::daemon::p2p;
    using yams::fuzzing::fuzzRequire;
    if (data == nullptr || size > 2 * kP2pMaxControlFrameBytes) {
        return 0;
    }
    FuzzedDataProvider input(data, size);
    auto service = yams::fuzzing::makeMemorySyncService();
    const bool allowFirstContact = input.ConsumeBool();
    const auto config = yams::fuzzing::makeHandshakeConfig(*service, allowFirstContact);
    const auto identity = yams::fuzzing::peerChannelIdentity();

    switch (input.ConsumeIntegralInRange<int>(0, 3)) {
        case 0: {
            const auto expectedType = input.ConsumeBool() ? "hello" : "hello_ack";
            const auto frame = takeFrame(input);
            (void)detail::validatePeerHelloFrame(frame, expectedType, config, identity.peerCertCn);
            break;
        }
        case 1: {
            const auto hello = takeFrame(input);
            const auto state = takeFrame(input);
            const auto window = takeFrame(input);
            (void)detail::validatePeerWindowFrames(hello, state, window, config);
            break;
        }
        case 2: {
            const auto frame = takeFrame(input);
            (void)detail::validatePeerHistoryProofFrame(frame, config, identity.peerCertCn);
            break;
        }
        default: {
            const auto stream = input.ConsumeRemainingBytes<std::uint8_t>();
            detail::BufferFrameSource frames(yams::fuzzing::asBytes(stream.data(), stream.size()));
            InMemoryPeerTrustStore trust;
            auto accepted = detail::acceptPeerHandshake(frames, identity, config, trust);
            fuzzRequire(frames.consumedBytes() <= stream.size());
            if (accepted) {
                fuzzRequire(accepted.value().peerNodeId == identity.peerCertCn);
                fuzzRequire(accepted.value().peerSpkiPin == identity.peerSpkiPin);
                // Acceptance requires an explicit first-contact grant for an unknown peer.
                fuzzRequire(allowFirstContact);
            }
            break;
        }
    }
    return 0;
}
