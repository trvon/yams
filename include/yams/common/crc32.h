// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

// Carry-less-multiply folding needs GCC/Clang per-function target attributes, so the rest of
// the translation unit keeps the baseline ISA and the kernel only runs after a CPUID check.
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#define YAMS_CRC32_X86_CLMUL 1
#include <immintrin.h>
#else
#define YAMS_CRC32_X86_CLMUL 0
#endif

// The ARMv8 CRC32 instructions implement this exact polynomial. Use them only where the
// compiler already targets them (Apple Silicon, or -march=...+crc), so no runtime probe is
// needed.
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#define YAMS_CRC32_ARM 1
#include <arm_acle.h>
#else
#define YAMS_CRC32_ARM 0
#endif

namespace yams::common {

namespace detail {

// CRC-32/ISO-HDLC (polynomial 0xEDB88320, reflected), slice-by-8 tables.
inline constexpr std::array<std::array<std::uint32_t, 256>, 8> kCrc32SliceTables = [] {
    std::array<std::array<std::uint32_t, 256>, 8> tables{};
    for (std::size_t i = 0; i < 256; ++i) {
        std::uint32_t c = static_cast<std::uint32_t>(i);
        for (int k = 0; k < 8; ++k) {
            c = (c & 1U) ? (0xEDB88320U ^ (c >> 1U)) : (c >> 1U);
        }
        tables[0][i] = c;
    }
    for (std::size_t i = 0; i < 256; ++i) {
        std::uint32_t c = tables[0][i];
        for (std::size_t k = 1; k < 8; ++k) {
            c = tables[0][c & 0xFFU] ^ (c >> 8U);
            tables[k][i] = c;
        }
    }
    return tables;
}();

} // namespace detail

/**
 * @brief Table-driven CRC-32 step. Portable on every target; the accelerated paths below
 * fall back to it for short inputs and tails, and tests use it as their oracle.
 *
 * @p running is the pre-final-xor state (0xFFFFFFFF for a fresh computation).
 */
[[nodiscard]] inline std::uint32_t crc32UpdatePortable(std::uint32_t running,
                                                       std::span<const std::byte> data) {
    const auto* bytes = reinterpret_cast<const std::uint8_t*>(data.data());
    std::size_t len = data.size();
    std::uint32_t crc = running;
    const auto& t = detail::kCrc32SliceTables;

    while (len >= 8) {
        std::uint32_t lo;
        std::uint32_t hi;
        std::memcpy(&lo, bytes, 4);
        std::memcpy(&hi, bytes + 4, 4);
        crc ^= lo;
        crc = t[7][crc & 0xFFU] ^ t[6][(crc >> 8U) & 0xFFU] ^ t[5][(crc >> 16U) & 0xFFU] ^
              t[4][crc >> 24U] ^ t[3][hi & 0xFFU] ^ t[2][(hi >> 8U) & 0xFFU] ^
              t[1][(hi >> 16U) & 0xFFU] ^ t[0][hi >> 24U];
        bytes += 8;
        len -= 8;
    }
    while (len-- > 0) {
        crc = (crc >> 8U) ^ t[0][(crc ^ *bytes++) & 0xFFU];
    }
    return crc;
}

namespace detail {

#if YAMS_CRC32_X86_CLMUL

/// Smallest input the folding kernel accepts: four 128-bit lanes.
inline constexpr std::size_t kCrc32ClmulMinBytes = 64;

// Lambdas do not inherit a target attribute, so the folding step is a function of its own.
#define YAMS_CRC32_CLMUL_TARGET __attribute__((target("pclmul,sse4.1")))

[[nodiscard]] inline __m128i crc32Load128(const std::uint8_t* p) {
    return _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
}

/// One folding step: carry the 128-bit remainder @p acc forward and absorb @p next.
[[nodiscard]] YAMS_CRC32_CLMUL_TARGET inline __m128i crc32Fold128(__m128i acc, __m128i k,
                                                                  __m128i next) {
    const __m128i lo = _mm_clmulepi64_si128(acc, k, 0x00);
    const __m128i hi = _mm_clmulepi64_si128(acc, k, 0x11);
    return _mm_xor_si128(_mm_xor_si128(lo, hi), next);
}

/**
 * Fold @p len bytes into @p crc with PCLMULQDQ. @p len must be at least
 * kCrc32ClmulMinBytes and a multiple of 16.
 *
 * The folding and reduction constants for the reflected polynomial 0x1DB710641 come from
 * Gopal et al., "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ Instruction"
 * (Intel, 2009). The unit test checks this kernel against the table implementation at every
 * length and alignment.
 */
[[nodiscard]] YAMS_CRC32_CLMUL_TARGET inline std::uint32_t
crc32FoldClmul(std::uint32_t crc, const std::uint8_t* buf, std::size_t len) {
    const __m128i foldBy4 = _mm_set_epi64x(0x01c6e41596, 0x0154442bd4); // across four lanes
    const __m128i foldBy1 = _mm_set_epi64x(0x00ccaa009e, 0x01751997d0); // across one lane
    const __m128i foldTo64 = _mm_set_epi64x(0, 0x0163cd6124);
    const __m128i barrett = _mm_set_epi64x(0x01f7011641, 0x01db710641); // mu, polynomial
    const __m128i low32 = _mm_setr_epi32(~0, 0, ~0, 0);

    __m128i x1 = _mm_xor_si128(crc32Load128(buf), _mm_cvtsi32_si128(static_cast<int>(crc)));
    __m128i x2 = crc32Load128(buf + 16);
    __m128i x3 = crc32Load128(buf + 32);
    __m128i x4 = crc32Load128(buf + 48);
    buf += 64;
    len -= 64;

    while (len >= 64) {
        x1 = crc32Fold128(x1, foldBy4, crc32Load128(buf));
        x2 = crc32Fold128(x2, foldBy4, crc32Load128(buf + 16));
        x3 = crc32Fold128(x3, foldBy4, crc32Load128(buf + 32));
        x4 = crc32Fold128(x4, foldBy4, crc32Load128(buf + 48));
        buf += 64;
        len -= 64;
    }

    x1 = crc32Fold128(x1, foldBy1, x2);
    x1 = crc32Fold128(x1, foldBy1, x3);
    x1 = crc32Fold128(x1, foldBy1, x4);
    while (len >= 16) {
        x1 = crc32Fold128(x1, foldBy1, crc32Load128(buf));
        buf += 16;
        len -= 16;
    }

    // 128 -> 64 bits.
    __m128i t = _mm_clmulepi64_si128(x1, foldBy1, 0x10);
    x1 = _mm_xor_si128(_mm_srli_si128(x1, 8), t);
    t = _mm_srli_si128(x1, 4);
    x1 = _mm_xor_si128(_mm_clmulepi64_si128(_mm_and_si128(x1, low32), foldTo64, 0x00), t);

    // Barrett reduction, 64 -> 32 bits.
    t = _mm_clmulepi64_si128(_mm_and_si128(x1, low32), barrett, 0x10);
    t = _mm_clmulepi64_si128(_mm_and_si128(t, low32), barrett, 0x00);
    x1 = _mm_xor_si128(x1, t);
    return static_cast<std::uint32_t>(_mm_extract_epi32(x1, 1));
}

/// CPUID result, read once per process.
[[nodiscard]] inline bool crc32HasClmul() noexcept {
    static const bool supported = [] {
        __builtin_cpu_init();
        return __builtin_cpu_supports("pclmul") && __builtin_cpu_supports("sse4.1");
    }();
    return supported;
}

#undef YAMS_CRC32_CLMUL_TARGET

#endif // YAMS_CRC32_X86_CLMUL

#if YAMS_CRC32_ARM

[[nodiscard]] inline std::uint32_t crc32UpdateArm(std::uint32_t crc, const std::uint8_t* buf,
                                                  std::size_t len) {
    while (len >= 8) {
        std::uint64_t word;
        std::memcpy(&word, buf, 8); // AArch64 targets YAMS supports are little-endian.
        crc = __crc32d(crc, word);
        buf += 8;
        len -= 8;
    }
    while (len-- > 0) {
        crc = __crc32b(crc, *buf++);
    }
    return crc;
}

#endif // YAMS_CRC32_ARM

} // namespace detail

/// True when crc32Update uses a hardware kernel on this CPU.
[[nodiscard]] inline bool crc32IsAccelerated() noexcept {
#if YAMS_CRC32_X86_CLMUL
    return detail::crc32HasClmul();
#elif YAMS_CRC32_ARM
    return true;
#else
    return false;
#endif
}

/**
 * @brief Continue a CRC-32 computation over @p data, seeded with @p running
 * (the pre-final-xor state from a previous crc32Update call).
 *
 * Every path computes CRC-32/ISO-HDLC, so stored checksums stay valid whichever one runs.
 */
[[nodiscard]] inline std::uint32_t crc32Update(std::uint32_t running,
                                               std::span<const std::byte> data) {
#if YAMS_CRC32_X86_CLMUL
    if (data.size() >= detail::kCrc32ClmulMinBytes && detail::crc32HasClmul()) {
        const std::size_t folded = data.size() & ~std::size_t{15};
        running = detail::crc32FoldClmul(
            running, reinterpret_cast<const std::uint8_t*>(data.data()), folded);
        data = data.subspan(folded);
    }
    return crc32UpdatePortable(running, data);
#elif YAMS_CRC32_ARM
    return detail::crc32UpdateArm(running, reinterpret_cast<const std::uint8_t*>(data.data()),
                                  data.size());
#else
    return crc32UpdatePortable(running, data);
#endif
}

/// CRC-32/ISO-HDLC checksum of @p data (the value zlib's crc32 returns).
[[nodiscard]] inline std::uint32_t crc32(std::span<const std::byte> data) {
    return crc32Update(0xFFFFFFFFU, data) ^ 0xFFFFFFFFU;
}

} // namespace yams::common
