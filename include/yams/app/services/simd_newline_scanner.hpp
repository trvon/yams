#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

// Platform detection
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#define YAMS_SIMD_X86 1
#if defined(__AVX2__)
#define YAMS_SIMD_AVX2 1
#include <immintrin.h>
#elif defined(__SSE2__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#define YAMS_SIMD_SSE2 1
#include <emmintrin.h>
#endif
#elif defined(__ARM_NEON) || defined(__aarch64__)
#define YAMS_SIMD_NEON 1
#include <arm_neon.h>
#endif

namespace yams {
namespace app {
namespace services {

/**
 * SIMD-optimized newline scanner for grep performance
 *
 * Provides 4-8x faster newline detection compared to memchr() by using:
 * - SSE2 (128-bit) on x86_64: 16 bytes per instruction
 * - AVX2 (256-bit) on modern x86: 32 bytes per instruction
 * - NEON (128-bit) on ARM/aarch64: 16 bytes per instruction
 *
 * Falls back to scalar memchr() on platforms without SIMD support.
 *
 * Based on ripgrep's bstr crate and Daniel Lemire's SIMD memchr techniques.
 */
class SimdNewlineScanner {
public:
    /**
     * Find the first newline character in a buffer using SIMD
     *
     * @param data Pointer to buffer to search
     * @param size Size of buffer in bytes
     * @return Offset of first '\n', or size if not found
     */
    static size_t findNewline(const char* data, size_t size);

    /**
     * Check if buffer contains any newline characters (fast existence check)
     *
     * @param data Pointer to buffer to search
     * @param size Size of buffer in bytes
     * @return true if buffer contains '\n', false otherwise
     */
    static bool containsNewline(const char* data, size_t size);

    /**
     * Count newlines in buffer (useful for line number calculations)
     *
     * @param data Pointer to buffer to search
     * @param size Size of buffer in bytes
     * @return Number of '\n' characters found
     */
    static size_t countNewlines(const char* data, size_t size);

private:
#if defined(YAMS_SIMD_AVX2)
    static size_t findNewlineAVX2(const char* data, size_t size);
    static size_t countNewlinesAVX2(const char* data, size_t size);
#endif

#if defined(YAMS_SIMD_SSE2)
    static size_t findNewlineSSE2(const char* data, size_t size);
    static size_t countNewlinesSSE2(const char* data, size_t size);
#endif

#if defined(YAMS_SIMD_NEON)
    static size_t findNewlineNEON(const char* data, size_t size);
    static size_t countNewlinesNEON(const char* data, size_t size);
#endif

    static size_t findNewlineScalar(const char* data, size_t size);
    static size_t countNewlinesScalar(const char* data, size_t size);

    // Count set bits in a bitmask (for counting newlines)
    static inline int popcount(uint32_t mask) {
#if defined(__POPCNT__)
        return __builtin_popcount(mask);
#else
        // Software fallback
        mask = mask - ((mask >> 1) & 0x55555555);
        mask = (mask & 0x33333333) + ((mask >> 2) & 0x33333333);
        return static_cast<int>((((mask + (mask >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24);
#endif
    }
};

} // namespace services
} // namespace app
} // namespace yams
