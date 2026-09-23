// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Peer connection strings and SPKI pins. Oracles: a parsed endpoint re-parses to the same host and
// port, pin normalization is idempotent, and the pin trust decision never pins a mismatched key.

#include "p2p_fuzz_support.h"

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_manager.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace yams::daemon::p2p;
    using yams::fuzzing::fuzzRequire;
    if (data == nullptr || size > 4096) {
        return 0;
    }
    const std::string_view text(reinterpret_cast<const char*>(data), size);

    if (auto spec = parseP2pConnectionString(text); spec) {
        fuzzRequire(!spec.value().host.empty());
        auto again = parseP2pConnectionString(spec.value().endpoint());
        fuzzRequire(again.has_value());
        fuzzRequire(again.value().host == spec.value().host);
        fuzzRequire(again.value().port == spec.value().port);
    }

    auto pin = normalizePeerSpkiPin(text);
    if (pin) {
        auto twice = normalizePeerSpkiPin(pin.value());
        fuzzRequire(twice.has_value() && twice.value() == pin.value());

        // The same key is always accepted; a different stored key is never silently re-pinned.
        const std::optional<std::string> same = pin.value();
        auto sameDecision = evaluatePeerPin(same, pin.value(), false);
        fuzzRequire(sameDecision.has_value());
        std::string other = pin.value();
        other.back() = other.back() == '0' ? '1' : '0';
        auto mismatch = evaluatePeerPin(std::optional<std::string>{other}, pin.value(), true);
        fuzzRequire(!mismatch.has_value());
        auto firstContact = evaluatePeerPin(std::nullopt, pin.value(), true);
        fuzzRequire(firstContact.has_value() && firstContact.value().firstContactPinned);
        auto refused = evaluatePeerPin(std::nullopt, pin.value(), false);
        fuzzRequire(!refused.has_value());
    }
    return 0;
}
