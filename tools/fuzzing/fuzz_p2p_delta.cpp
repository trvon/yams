// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Direct-P2P delta/bootstrap control frames. The first bytes choose the exchange limits so the
// validators run against the same range of bounds a peer can negotiate, not only the defaults.

#include "../../src/daemon/p2p/p2p_fuzz.h"

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_delta.h>
#include <yams/daemon/p2p/p2p_transport.h>

#include <fuzzer/FuzzedDataProvider.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace yams::daemon::p2p;
    if (data == nullptr || size > kP2pMaxControlFrameBytes + 64) {
        return 0;
    }
    FuzzedDataProvider input(data, size);
    DeltaExchangeOptions options;
    options.maxDeltasPerBatch = input.ConsumeIntegralInRange<std::size_t>(0, 256);
    options.maxBatches = input.ConsumeIntegralInRange<std::size_t>(0, 8192);
    options.maxDeltasPerSession = input.ConsumeIntegralInRange<std::size_t>(0, 4096);
    options.maxWireBytesPerSession = input.ConsumeIntegral<std::uint32_t>();
    options.maxSnapshotRecords = input.ConsumeIntegralInRange<std::size_t>(0, 4096);
    options.maxSnapshotWireBytes = input.ConsumeIntegral<std::uint32_t>();
    const auto frame = input.ConsumeRemainingBytes<std::uint8_t>();
    (void)detail::validateDeltaControlFrame(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(frame.data()), frame.size()),
        options);
    return 0;
}
