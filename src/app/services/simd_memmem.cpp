// SIMD-accelerated substring search (memmem) implementation.
//
// Uses the "generic SIMD" two-byte technique (Daniel Lemire / ripgrep memmem):
//   1. Pick two discriminating bytes from needle (first + last).
//   2. Broadcast both to SIMD registers.
//   3. Scan haystack in register-width chunks: cmpeq both bytes at offset 0
//      and offset (needleLen-1), AND the bitmasks, tzcnt each hit, verify
//      with memcmp.
//   4. Scalar tail for remaining bytes < register width.
//   5. For needles of 1 byte, delegate to memchr.
//
// Runtime CPUID dispatch (x86): a static function-pointer is initialised once
// at first call via pickImpl().  ARM NEON is selected unconditionally at
// compile time.

#include <algorithm>
#include <cctype>
#include <cstring>
#ifdef _MSC_VER
#include <intrin.h>
#endif

#include <yams/app/services/simd_memmem.hpp>

namespace yams {
namespace app {
namespace services {

// ---------------------------------------------------------------------------
// Helper: ASCII tolower (branchless)
// ---------------------------------------------------------------------------
static inline unsigned char asciiLower(unsigned char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<unsigned char>(c + 32) : c;
}

// ---------------------------------------------------------------------------
// Scalar implementation — memchr + memcmp loop (same as old containsFast)
// ---------------------------------------------------------------------------
static size_t scalarMemmem(const char* haystack, size_t haystackLen, const char* needle,
                           size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;

    if (needleLen == 1) {
        const void* p = std::memchr(haystack, static_cast<unsigned char>(needle[0]), haystackLen);
        return p ? static_cast<size_t>(static_cast<const char*>(p) - haystack) : kMemmemNpos;
    }

    const char first = needle[0];
    const char* scan = haystack;
    size_t remaining = haystackLen;

    while (remaining >= needleLen) {
        const void* found =
            std::memchr(scan, static_cast<unsigned char>(first), remaining - needleLen + 1);
        if (!found)
            return kMemmemNpos;

        const char* candidate = static_cast<const char*>(found);
        if (std::memcmp(candidate, needle, needleLen) == 0) {
            return static_cast<size_t>(candidate - haystack);
        }
        const size_t advance = static_cast<size_t>(candidate - scan) + 1;
        scan = candidate + 1;
        remaining -= advance;
    }
    return kMemmemNpos;
}

// Scalar case-insensitive — per-char tolower comparison
static size_t scalarMemmemCI(const char* haystack, size_t haystackLen, const char* needle,
                             size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;

    // Needle is expected to already be lowercase.
    for (size_t i = 0; i <= haystackLen - needleLen; ++i) {
        bool match = true;
        for (size_t j = 0; j < needleLen; ++j) {
            if (asciiLower(static_cast<unsigned char>(haystack[i + j])) !=
                static_cast<unsigned char>(needle[j])) {
                match = false;
                break;
            }
        }
        if (match)
            return i;
    }
    return kMemmemNpos;
}

// ============================================================================
// AVX2 Implementation (256-bit, 32 bytes per loop)
// ============================================================================
#if defined(YAMS_SIMD_AVX2)

static size_t avx2Memmem(const char* haystack, size_t haystackLen, const char* needle,
                         size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;
    if (needleLen == 1) {
        const void* p = std::memchr(haystack, static_cast<unsigned char>(needle[0]), haystackLen);
        return p ? static_cast<size_t>(static_cast<const char*>(p) - haystack) : kMemmemNpos;
    }

    const auto firstByte = _mm256_set1_epi8(static_cast<char>(needle[0]));
    const auto lastByte = _mm256_set1_epi8(static_cast<char>(needle[needleLen - 1]));

    // We scan positions [0 .. haystackLen - needleLen].
    // At each SIMD step we load 32 bytes starting at `pos` (first byte) and
    // 32 bytes starting at `pos + needleLen - 1` (last byte).
    const size_t scanEnd = haystackLen - needleLen;

    size_t pos = 0;
    while (pos + 32 <= scanEnd + 1) {
        const __m256i blockFirst =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(haystack + pos));
        const __m256i blockLast =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(haystack + pos + needleLen - 1));

        const __m256i eqFirst = _mm256_cmpeq_epi8(blockFirst, firstByte);
        const __m256i eqLast = _mm256_cmpeq_epi8(blockLast, lastByte);

        uint32_t mask =
            static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_and_si256(eqFirst, eqLast)));

        while (mask != 0) {
#ifdef _MSC_VER
            unsigned long bit;
            _BitScanForward(&bit, mask);
#else
            int bit = __builtin_ctz(mask);
#endif
            const size_t candidate = pos + static_cast<size_t>(bit);
            if (candidate + needleLen <= haystackLen &&
                std::memcmp(haystack + candidate + 1, needle + 1, needleLen - 2) == 0) {
                return candidate;
            }
            mask &= mask - 1; // clear lowest set bit
        }

        pos += 32;
    }

    // Scalar tail for remaining positions
    if (pos <= scanEnd) {
        size_t tail = scalarMemmem(haystack + pos, haystackLen - pos, needle, needleLen);
        if (tail != kMemmemNpos)
            return pos + tail;
    }
    return kMemmemNpos;
}

static size_t avx2MemmemCI(const char* haystack, size_t haystackLen, const char* needle,
                           size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;

    // Needle must already be lowercase.
    // For CI matching with SIMD we need to compare tolower(haystack byte) with
    // needle byte.  We use a trick: broadcast both cases of the first and last
    // needle byte and OR the comparison masks so that we catch either case in
    // the haystack.  Full verification is done with per-byte tolower memcmp.

    const char firstLow = needle[0];
    const char firstUp =
        (firstLow >= 'a' && firstLow <= 'z') ? static_cast<char>(firstLow - 32) : firstLow;
    const char lastLow = needle[needleLen - 1];
    const char lastUp =
        (lastLow >= 'a' && lastLow <= 'z') ? static_cast<char>(lastLow - 32) : lastLow;

    const __m256i firstLowV = _mm256_set1_epi8(firstLow);
    const __m256i firstUpV = _mm256_set1_epi8(firstUp);
    const __m256i lastLowV = _mm256_set1_epi8(lastLow);
    const __m256i lastUpV = _mm256_set1_epi8(lastUp);

    const size_t scanEnd = haystackLen - needleLen;
    size_t pos = 0;

    while (pos + 32 <= scanEnd + 1) {
        const __m256i blockFirst =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(haystack + pos));
        const __m256i blockLast =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(haystack + pos + needleLen - 1));

        // Match either case of first byte
        const __m256i eqFirstLow = _mm256_cmpeq_epi8(blockFirst, firstLowV);
        const __m256i eqFirstUp = _mm256_cmpeq_epi8(blockFirst, firstUpV);
        const __m256i eqFirst = _mm256_or_si256(eqFirstLow, eqFirstUp);

        // Match either case of last byte
        const __m256i eqLastLow = _mm256_cmpeq_epi8(blockLast, lastLowV);
        const __m256i eqLastUp = _mm256_cmpeq_epi8(blockLast, lastUpV);
        const __m256i eqLast = _mm256_or_si256(eqLastLow, eqLastUp);

        uint32_t mask =
            static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_and_si256(eqFirst, eqLast)));

        while (mask != 0) {
#ifdef _MSC_VER
            unsigned long bit;
            _BitScanForward(&bit, mask);
#else
            int bit = __builtin_ctz(mask);
#endif
            const size_t candidate = pos + static_cast<size_t>(bit);
            if (candidate + needleLen <= haystackLen) {
                // Verify full match with tolower
                bool ok = true;
                for (size_t k = 1; k < needleLen - 1; ++k) {
                    if (asciiLower(static_cast<unsigned char>(haystack[candidate + k])) !=
                        static_cast<unsigned char>(needle[k])) {
                        ok = false;
                        break;
                    }
                }
                if (ok)
                    return candidate;
            }
            mask &= mask - 1;
        }

        pos += 32;
    }

    // Scalar tail
    if (pos <= scanEnd) {
        size_t tail = scalarMemmemCI(haystack + pos, haystackLen - pos, needle, needleLen);
        if (tail != kMemmemNpos)
            return pos + tail;
    }
    return kMemmemNpos;
}

#endif // YAMS_SIMD_AVX2

// ============================================================================
// SSE2 Implementation (128-bit, 16 bytes per loop)
// ============================================================================
#if defined(YAMS_SIMD_SSE2)

static size_t sse2Memmem(const char* haystack, size_t haystackLen, const char* needle,
                         size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;
    if (needleLen == 1) {
        const void* p = std::memchr(haystack, static_cast<unsigned char>(needle[0]), haystackLen);
        return p ? static_cast<size_t>(static_cast<const char*>(p) - haystack) : kMemmemNpos;
    }

    const __m128i firstByte = _mm_set1_epi8(static_cast<char>(needle[0]));
    const __m128i lastByte = _mm_set1_epi8(static_cast<char>(needle[needleLen - 1]));

    const size_t scanEnd = haystackLen - needleLen;
    size_t pos = 0;

    while (pos + 16 <= scanEnd + 1) {
        const __m128i blockFirst =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack + pos));
        const __m128i blockLast =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack + pos + needleLen - 1));

        const __m128i eqFirst = _mm_cmpeq_epi8(blockFirst, firstByte);
        const __m128i eqLast = _mm_cmpeq_epi8(blockLast, lastByte);

        int mask = _mm_movemask_epi8(_mm_and_si128(eqFirst, eqLast));

        while (mask != 0) {
#ifdef _MSC_VER
            unsigned long bit;
            _BitScanForward(&bit, mask);
#else
            int bit = __builtin_ctz(mask);
#endif
            const size_t candidate = pos + static_cast<size_t>(bit);
            if (candidate + needleLen <= haystackLen &&
                std::memcmp(haystack + candidate + 1, needle + 1, needleLen - 2) == 0) {
                return candidate;
            }
            mask &= mask - 1;
        }

        pos += 16;
    }

    if (pos <= scanEnd) {
        size_t tail = scalarMemmem(haystack + pos, haystackLen - pos, needle, needleLen);
        if (tail != kMemmemNpos)
            return pos + tail;
    }
    return kMemmemNpos;
}

static size_t sse2MemmemCI(const char* haystack, size_t haystackLen, const char* needle,
                           size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;

    const char firstLow = needle[0];
    const char firstUp =
        (firstLow >= 'a' && firstLow <= 'z') ? static_cast<char>(firstLow - 32) : firstLow;
    const char lastLow = needle[needleLen - 1];
    const char lastUp =
        (lastLow >= 'a' && lastLow <= 'z') ? static_cast<char>(lastLow - 32) : lastLow;

    const __m128i firstLowV = _mm_set1_epi8(firstLow);
    const __m128i firstUpV = _mm_set1_epi8(firstUp);
    const __m128i lastLowV = _mm_set1_epi8(lastLow);
    const __m128i lastUpV = _mm_set1_epi8(lastUp);

    const size_t scanEnd = haystackLen - needleLen;
    size_t pos = 0;

    while (pos + 16 <= scanEnd + 1) {
        const __m128i blockFirst =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack + pos));
        const __m128i blockLast =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack + pos + needleLen - 1));

        const __m128i eqFirstLow = _mm_cmpeq_epi8(blockFirst, firstLowV);
        const __m128i eqFirstUp = _mm_cmpeq_epi8(blockFirst, firstUpV);
        const __m128i eqFirst = _mm_or_si128(eqFirstLow, eqFirstUp);

        const __m128i eqLastLow = _mm_cmpeq_epi8(blockLast, lastLowV);
        const __m128i eqLastUp = _mm_cmpeq_epi8(blockLast, lastUpV);
        const __m128i eqLast = _mm_or_si128(eqLastLow, eqLastUp);

        int mask = _mm_movemask_epi8(_mm_and_si128(eqFirst, eqLast));

        while (mask != 0) {
#ifdef _MSC_VER
            unsigned long bit;
            _BitScanForward(&bit, mask);
#else
            int bit = __builtin_ctz(mask);
#endif
            const size_t candidate = pos + static_cast<size_t>(bit);
            if (candidate + needleLen <= haystackLen) {
                bool ok = true;
                for (size_t k = 1; k < needleLen - 1; ++k) {
                    if (asciiLower(static_cast<unsigned char>(haystack[candidate + k])) !=
                        static_cast<unsigned char>(needle[k])) {
                        ok = false;
                        break;
                    }
                }
                if (ok)
                    return candidate;
            }
            mask &= mask - 1;
        }

        pos += 16;
    }

    if (pos <= scanEnd) {
        size_t tail = scalarMemmemCI(haystack + pos, haystackLen - pos, needle, needleLen);
        if (tail != kMemmemNpos)
            return pos + tail;
    }
    return kMemmemNpos;
}

#endif // YAMS_SIMD_SSE2

// ============================================================================
// NEON Implementation (128-bit, 16 bytes per loop — ARM / aarch64)
// ============================================================================
#if defined(YAMS_SIMD_NEON)

// Helper: create a 16-bit bitmask from a NEON uint8x16 comparison result.
// Each byte in `v` is either 0x00 or 0xFF; we extract one bit per byte.
static inline uint16_t neonMovemask(uint8x16_t v) {
    // Shift each byte so that the MSB of byte i ends up in bit position i.
    static const int8_t kShift[16] = {-7, -6, -5, -4, -3, -2, -1, 0, -7, -6, -5, -4, -3, -2, -1, 0};
    const int8x16_t shift = vld1q_s8(kShift);
    uint8x16_t shifted = vreinterpretq_u8_s8(vshlq_s8(vreinterpretq_s8_u8(v), shift));
    // Pairwise-add across bytes to accumulate bits.
    uint8x8_t lo = vget_low_u8(shifted);
    uint8x8_t hi = vget_high_u8(shifted);
    uint8x8_t sumLo = vpadd_u8(vpadd_u8(vpadd_u8(lo, lo), vpadd_u8(lo, lo)),
                               vpadd_u8(vpadd_u8(lo, lo), vpadd_u8(lo, lo)));
    uint8x8_t sumHi = vpadd_u8(vpadd_u8(vpadd_u8(hi, hi), vpadd_u8(hi, hi)),
                               vpadd_u8(vpadd_u8(hi, hi), vpadd_u8(hi, hi)));
    return static_cast<uint16_t>(vget_lane_u8(sumLo, 0)) |
           (static_cast<uint16_t>(vget_lane_u8(sumHi, 0)) << 8);
}

// Count trailing zeros for a 16-bit mask.
static inline int ctz16(uint16_t mask) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctz(static_cast<unsigned>(mask));
#else
    int count = 0;
    while ((mask & 1u) == 0) {
        ++count;
        mask >>= 1;
    }
    return count;
#endif
}

static size_t neonMemmem(const char* haystack, size_t haystackLen, const char* needle,
                         size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;
    if (needleLen == 1) {
        const void* p = std::memchr(haystack, static_cast<unsigned char>(needle[0]), haystackLen);
        return p ? static_cast<size_t>(static_cast<const char*>(p) - haystack) : kMemmemNpos;
    }

    const uint8x16_t firstByte = vdupq_n_u8(static_cast<uint8_t>(needle[0]));
    const uint8x16_t lastByte = vdupq_n_u8(static_cast<uint8_t>(needle[needleLen - 1]));

    const size_t scanEnd = haystackLen - needleLen;
    size_t pos = 0;

    while (pos + 16 <= scanEnd + 1) {
        const uint8x16_t blockFirst = vld1q_u8(reinterpret_cast<const uint8_t*>(haystack + pos));
        const uint8x16_t blockLast =
            vld1q_u8(reinterpret_cast<const uint8_t*>(haystack + pos + needleLen - 1));

        const uint8x16_t eqFirst = vceqq_u8(blockFirst, firstByte);
        const uint8x16_t eqLast = vceqq_u8(blockLast, lastByte);
        const uint8x16_t combined = vandq_u8(eqFirst, eqLast);

        uint16_t mask = neonMovemask(combined);
        while (mask != 0) {
            int bit = ctz16(mask);
            const size_t candidate = pos + static_cast<size_t>(bit);
            if (candidate + needleLen <= haystackLen &&
                std::memcmp(haystack + candidate + 1, needle + 1, needleLen - 2) == 0) {
                return candidate;
            }
            mask &= static_cast<uint16_t>(mask - 1);
        }

        pos += 16;
    }

    if (pos <= scanEnd) {
        size_t tail = scalarMemmem(haystack + pos, haystackLen - pos, needle, needleLen);
        if (tail != kMemmemNpos)
            return pos + tail;
    }
    return kMemmemNpos;
}

static size_t neonMemmemCI(const char* haystack, size_t haystackLen, const char* needle,
                           size_t needleLen) {
    if (needleLen == 0)
        return 0;
    if (needleLen > haystackLen)
        return kMemmemNpos;

    const char firstLow = needle[0];
    const char firstUp =
        (firstLow >= 'a' && firstLow <= 'z') ? static_cast<char>(firstLow - 32) : firstLow;
    const char lastLow = needle[needleLen - 1];
    const char lastUp =
        (lastLow >= 'a' && lastLow <= 'z') ? static_cast<char>(lastLow - 32) : lastLow;

    const uint8x16_t firstLowV = vdupq_n_u8(static_cast<uint8_t>(firstLow));
    const uint8x16_t firstUpV = vdupq_n_u8(static_cast<uint8_t>(firstUp));
    const uint8x16_t lastLowV = vdupq_n_u8(static_cast<uint8_t>(lastLow));
    const uint8x16_t lastUpV = vdupq_n_u8(static_cast<uint8_t>(lastUp));

    const size_t scanEnd = haystackLen - needleLen;
    size_t pos = 0;

    while (pos + 16 <= scanEnd + 1) {
        const uint8x16_t blockFirst = vld1q_u8(reinterpret_cast<const uint8_t*>(haystack + pos));
        const uint8x16_t blockLast =
            vld1q_u8(reinterpret_cast<const uint8_t*>(haystack + pos + needleLen - 1));

        const uint8x16_t eqFirstLow = vceqq_u8(blockFirst, firstLowV);
        const uint8x16_t eqFirstUp = vceqq_u8(blockFirst, firstUpV);
        const uint8x16_t eqFirst = vorrq_u8(eqFirstLow, eqFirstUp);

        const uint8x16_t eqLastLow = vceqq_u8(blockLast, lastLowV);
        const uint8x16_t eqLastUp = vceqq_u8(blockLast, lastUpV);
        const uint8x16_t eqLast = vorrq_u8(eqLastLow, eqLastUp);

        const uint8x16_t combined = vandq_u8(eqFirst, eqLast);
        uint16_t mask = neonMovemask(combined);

        while (mask != 0) {
            int bit = ctz16(mask);
            const size_t candidate = pos + static_cast<size_t>(bit);
            if (candidate + needleLen <= haystackLen) {
                bool ok = true;
                for (size_t k = 1; k < needleLen - 1; ++k) {
                    if (asciiLower(static_cast<unsigned char>(haystack[candidate + k])) !=
                        static_cast<unsigned char>(needle[k])) {
                        ok = false;
                        break;
                    }
                }
                if (ok)
                    return candidate;
            }
            mask &= static_cast<uint16_t>(mask - 1);
        }

        pos += 16;
    }

    if (pos <= scanEnd) {
        size_t tail = scalarMemmemCI(haystack + pos, haystackLen - pos, needle, needleLen);
        if (tail != kMemmemNpos)
            return pos + tail;
    }
    return kMemmemNpos;
}

#endif // YAMS_SIMD_NEON

// ============================================================================
// Runtime dispatch via static function pointer (initialised once)
// ============================================================================

using MemmemFn = size_t (*)(const char*, size_t, const char*, size_t);

static MemmemFn pickMemmemImpl() {
#if defined(YAMS_SIMD_AVX2)
    // Compiled with -mavx2: AVX2 is always available at runtime.
    return avx2Memmem;
#elif defined(YAMS_SIMD_X86)
    // Compiled without -mavx2 but on x86 — only SSE2 is guaranteed at
    // compile-time.  Note: we cannot emit AVX2 code without the compiler
    // flag, so the best we can offer here is SSE2.
#if defined(YAMS_SIMD_SSE2)
    return sse2Memmem;
#else
    return scalarMemmem;
#endif
#elif defined(YAMS_SIMD_NEON)
    return neonMemmem;
#else
    return scalarMemmem;
#endif
}

static MemmemFn pickMemmemCIImpl() {
#if defined(YAMS_SIMD_AVX2)
    return avx2MemmemCI;
#elif defined(YAMS_SIMD_X86)
#if defined(YAMS_SIMD_SSE2)
    return sse2MemmemCI;
#else
    return scalarMemmemCI;
#endif
#elif defined(YAMS_SIMD_NEON)
    return neonMemmemCI;
#else
    return scalarMemmemCI;
#endif
}

// Static initialisation — the function pointer is resolved exactly once.
static const MemmemFn gMemmemImpl = pickMemmemImpl();
static const MemmemFn gMemmemCIImpl = pickMemmemCIImpl();

// ============================================================================
// Public API
// ============================================================================

size_t simdMemmem(const char* haystack, size_t haystackLen, const char* needle, size_t needleLen) {
    return gMemmemImpl(haystack, haystackLen, needle, needleLen);
}

size_t simdMemmemCI(const char* haystack, size_t haystackLen, const char* needle,
                    size_t needleLen) {
    return gMemmemCIImpl(haystack, haystackLen, needle, needleLen);
}

} // namespace services
} // namespace app
} // namespace yams
