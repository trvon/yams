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
 * Newline scanner for grep performance
 *
 * Uses memchr() / scalar scanning: benchmarks show memchr is faster than
 * custom SIMD implementations for single-byte search on modern CPUs.
 */
class SimdNewlineScanner {
public:
    /**
     * Find the first newline character in a buffer using SIMD
     *
     * @param data Pointer to buffer to search
     * @param size Size of buffer in bytes
     * @return Offset of first '\n', or size if not found
     * Null data with nonzero size is treated as no match.
     */
    static inline size_t findNewline(const char* data, size_t size) {
        if (size == 0 || data == nullptr) {
            return size;
        }
        const void* result = std::memchr(data, '\n', size);
        if (result == nullptr) {
            return size;
        }
        return static_cast<size_t>(static_cast<const char*>(result) - data);
    }

    /**
     * Check if buffer contains any newline characters (fast existence check)
     *
     * @param data Pointer to buffer to search
     * @param size Size of buffer in bytes
     * @return true if buffer contains '\n', false otherwise
     * Null data with nonzero size is treated as no match.
     */
    static inline bool containsNewline(const char* data, size_t size) {
        return findNewline(data, size) < size;
    }

    /**
     * Count newlines in buffer (useful for line number calculations)
     *
     * @param data Pointer to buffer to search
     * @param size Size of buffer in bytes
     * @return Number of '\n' characters found
     * Null data with nonzero size returns 0.
     */
    static size_t countNewlines(const char* data, size_t size);

private:
    static size_t findNewlineScalar(const char* data, size_t size);
    static size_t countNewlinesScalar(const char* data, size_t size);
};

} // namespace services
} // namespace app
} // namespace yams
