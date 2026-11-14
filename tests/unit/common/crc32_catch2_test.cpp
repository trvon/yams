// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/common/crc32.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <vector>

namespace {

// Reference bit-by-bit CRC32 (the previous production implementation). Kept
// here as an oracle so the fast slice-by-8 path is proven byte-identical for
// every input length, including the tail handling.
uint32_t referenceBitByBit(const std::byte* data, std::size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (std::size_t i = 0; i < length; ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
        }
    }
    return ~crc;
}

std::vector<std::byte> asBytes(const std::string& s) {
    std::vector<std::byte> out(s.size());
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

TEST_CASE("crc32 matches known-good reference values", "[common][crc32]") {
    CHECK(yams::common::crc32(std::span<const std::byte>{}) == 0x00000000U);
    CHECK(yams::common::crc32(asBytes("hello")) == 0x3610A686U);
    CHECK(yams::common::crc32(asBytes("The quick brown fox jumps over the lazy dog")) ==
          0x414FA339U);
    CHECK(yams::common::crc32(asBytes("yams storage integrity check 1234567890")) == 0x81E9A155U);
}

TEST_CASE("crc32Update continues a running computation across chunks", "[common][crc32]") {
    const auto part1 = asBytes("hello ");
    const auto part2 = asBytes("world");
    // Whole-data checksum must equal chunked checksum via crc32Update.
    std::vector<std::byte> whole = part1;
    whole.insert(whole.end(), part2.begin(), part2.end());
    const auto wholeCrc = yams::common::crc32(std::span<const std::byte>(whole));
    const auto chunked =
        yams::common::crc32Update(yams::common::crc32Update(0xFFFFFFFFU, part1), part2) ^
        0xFFFFFFFFU;
    CHECK(chunked == wholeCrc);
}

TEST_CASE("crc32 slice-by-8 matches bit-by-bit reference for all tail lengths", "[common][crc32]") {
    // Deterministic pseudo-random data covering every remainder modulo 8.
    std::vector<std::byte> data;
    data.reserve(512);
    std::uint32_t seed = 0x12345678;
    for (int i = 0; i < 512; ++i) {
        seed = seed * 1664525U + 1013904223U;
        data.push_back(static_cast<std::byte>(seed >> 24));
    }

    for (std::size_t len = 0; len <= data.size(); ++len) {
        const auto got = yams::common::crc32(std::span<const std::byte>(data.data(), len));
        const auto want = referenceBitByBit(data.data(), len);
        if (got != want) {
            FAIL("mismatch at length " << len << ": got 0x" << std::hex << got << " want 0x"
                                       << want);
        }
    }
}

TEST_CASE("crc32Update agrees with the portable tables at every length and alignment",
          "[common][crc32]") {
    // Long enough to cross the four-lane loop, the single-lane loop and the scalar tail of an
    // accelerated kernel several times.
    std::vector<std::byte> data;
    data.reserve(1300);
    std::uint32_t seed = 0x9E3779B9U;
    for (int i = 0; i < 1300; ++i) {
        seed = seed * 1664525U + 1013904223U;
        data.push_back(static_cast<std::byte>(seed >> 24));
    }

    for (const std::uint32_t running : {0xFFFFFFFFU, 0x00000000U, 0x12345678U}) {
        for (std::size_t offset = 0; offset < 17; ++offset) {
            for (std::size_t len = 0; offset + len <= 1280; ++len) {
                const std::span<const std::byte> input(data.data() + offset, len);
                const auto got = yams::common::crc32Update(running, input);
                const auto want = yams::common::crc32UpdatePortable(running, input);
                if (got != want) {
                    FAIL("mismatch at offset " << offset << " length " << len << " seed 0x"
                                               << std::hex << running);
                }
            }
        }
    }
}

TEST_CASE("crc32 of a chunk-sized buffer matches the bit-by-bit reference", "[common][crc32]") {
    std::vector<std::byte> data(256 * 1024 + 13);
    std::uint32_t seed = 0xC0FFEE11U;
    for (auto& b : data) {
        seed = seed * 1664525U + 1013904223U;
        b = static_cast<std::byte>(seed >> 24);
    }
    CHECK(yams::common::crc32(std::span<const std::byte>(data)) ==
          referenceBitByBit(data.data(), data.size()));

    // Splitting the input anywhere must not change the result, including splits that leave
    // one side too short for an accelerated kernel.
    for (const std::size_t split : {std::size_t{1}, std::size_t{63}, std::size_t{64},
                                    std::size_t{65}, std::size_t{4096}, data.size() - 7}) {
        const std::span<const std::byte> all(data);
        const auto chunked =
            yams::common::crc32Update(yams::common::crc32Update(0xFFFFFFFFU, all.first(split)),
                                      all.subspan(split)) ^
            0xFFFFFFFFU;
        CHECK(chunked == yams::common::crc32(all));
    }
}

} // namespace
