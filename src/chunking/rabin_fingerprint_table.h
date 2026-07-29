#pragma once

#include <yams/core/assert.hpp>

#include <array>
#include <cstddef>
#include <cstdint>

namespace yams::chunking::detail {

inline constexpr std::uint64_t kDefaultRabinPolynomial = 0x3DA3358B4DC173ULL;

struct RabinFingerprintTable {
    std::array<std::uint64_t, 256> outTable{};

    explicit RabinFingerprintTable(std::uint64_t polynomial) {
        YAMS_PRECONDITION(polynomial != 0, "Rabin fingerprint polynomial must be non-zero");
        for (int byte = 0; byte < 256; ++byte) {
            std::uint64_t hash = 0;
            for (int bit = 0; bit < 8; ++bit) {
                if ((byte & (1 << bit)) != 0) {
                    hash ^= polynomial << bit;
                }
            }
            outTable[static_cast<std::size_t>(byte)] = hash;
        }
    }
};

} // namespace yams::chunking::detail
