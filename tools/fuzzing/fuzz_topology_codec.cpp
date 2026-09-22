// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Topology snapshot decoding from untrusted stored payloads (binary codec and the JSON/zstd
// envelope). Oracle: a binary batch that decodes re-encodes and decodes to the same shape.

#include <yams/topology/topology_codec.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace {

void fuzzRequire(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr || size < 1 || size > 1024 * 1024) {
        return 0;
    }
    const bool binary = (data[0] & 1U) != 0;
    const auto payload =
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(data + 1), size - 1);
    if (!binary) {
        const std::string_view text(reinterpret_cast<const char*>(payload.data()), payload.size());
        (void)yams::topology::deserializeTopologyBatchCompressed(text);
        return 0;
    }
    auto decoded = yams::topology::deserializeTopologyBatchBinary(payload);
    if (!decoded) {
        return 0;
    }
    auto encoded = yams::topology::serializeTopologyBatchBinary(decoded.value());
    fuzzRequire(encoded.has_value());
    auto again = yams::topology::deserializeTopologyBatchBinary(encoded.value());
    fuzzRequire(again.has_value());
    fuzzRequire(again.value().snapshotId == decoded.value().snapshotId);
    fuzzRequire(again.value().clusters.size() == decoded.value().clusters.size());
    fuzzRequire(again.value().memberships.size() == decoded.value().memberships.size());
    fuzzRequire(again.value().topologyEpoch == decoded.value().topologyEpoch);
    return 0;
}
