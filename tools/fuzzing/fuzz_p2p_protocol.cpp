// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include "../../src/daemon/p2p/p2p_fuzz.h"

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_transport.h>

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr || size > yams::daemon::p2p::kP2pMaxControlFrameBytes) {
        return 0;
    }
    const auto frame = std::span<const std::byte>(reinterpret_cast<const std::byte*>(data), size);
    (void)yams::daemon::p2p::detail::validateHandshakeControlFrame(frame);
    return 0;
}
