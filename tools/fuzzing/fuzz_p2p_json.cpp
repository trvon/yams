// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Direct-P2P control-frame JSON parsing. Oracles: an accepted frame never nests deeper than the
// pre-authentication limit, and an accepted document survives a dump -> parse round trip.

#include "p2p_fuzz_support.h"

#include "../../src/daemon/p2p/p2p_json.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace {

std::size_t nestingDepth(const nlohmann::json& value) {
    if (!value.is_structured()) {
        return 0;
    }
    std::size_t deepest = 0;
    for (const auto& child : value) {
        deepest = std::max(deepest, nestingDepth(child));
    }
    return deepest + 1;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace yams::daemon::p2p;
    if (data == nullptr || size > kP2pMaxControlFrameBytes) {
        return 0;
    }
    const auto frame = yams::fuzzing::asBytes(data, size);
    auto parsed = detail::parseJsonFrame(frame);
    if (!parsed) {
        return 0;
    }
    yams::fuzzing::fuzzRequire(nestingDepth(parsed.value()) <= detail::kMaxP2pJsonNestingDepth);

    std::string dumped;
    try {
        dumped = parsed.value().dump();
    } catch (const nlohmann::json::exception&) {
        // Invalid UTF-8 inside strings is accepted by the parser but refused by dump(); the
        // frame is still bounded, so this is not an oracle violation.
        return 0;
    }
    auto reparsed = detail::parseJsonFrame(std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(dumped.data()), dumped.size()));
    yams::fuzzing::fuzzRequire(reparsed.has_value() && reparsed.value() == parsed.value());
    return 0;
}
