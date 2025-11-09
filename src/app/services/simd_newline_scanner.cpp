#include <yams/app/services/simd_newline_scanner.hpp>
#include <cstring>

namespace yams {
namespace app {
namespace services {

// ============================================================================
// AVX2 Implementation (256-bit, 32 bytes per loop)
// ============================================================================

#if defined(YAMS_SIMD_AVX2)

size_t SimdNewlineScanner::findNewlineAVX2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    // Load newline pattern into all 32 bytes of YMM register
    const __m256i newline = _mm256_set1_epi8('\n');
    
    // Process 32 bytes at a time
    while (ptr + 32 <= end) {
        // Load 32 bytes
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        
        // Compare all 32 bytes with '\n' in parallel
        __m256i cmp = _mm256_cmpeq_epi8(chunk, newline);
        
        // Convert comparison result to bitmask
        int mask = _mm256_movemask_epi8(cmp);
        
        if (mask != 0) {
            // Found at least one newline, find first one
            int offset = __builtin_ctz(mask);
            return static_cast<size_t>(ptr - data) + static_cast<size_t>(offset);
        }
        
        ptr += 32;
    }
    
    // Handle remaining bytes with scalar fallback
    return findNewlineScalar(ptr, static_cast<size_t>(end - ptr)) + 
           static_cast<size_t>(ptr - data);
}

size_t SimdNewlineScanner::countNewlinesAVX2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;
    
    const __m256i newline = _mm256_set1_epi8('\n');
    
    while (ptr + 32 <= end) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, newline);
        int mask = _mm256_movemask_epi8(cmp);
        count += static_cast<size_t>(popcount(static_cast<uint32_t>(mask)));
        ptr += 32;
    }
    
    // Handle remaining bytes
    count += countNewlinesScalar(ptr, static_cast<size_t>(end - ptr));
    return count;
}

#endif // YAMS_SIMD_AVX2

// ============================================================================
// SSE2 Implementation (128-bit, 16 bytes per loop)
// ============================================================================

#if defined(YAMS_SIMD_SSE2)

size_t SimdNewlineScanner::findNewlineSSE2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    // Load newline pattern into all 16 bytes of XMM register
    const __m128i newline = _mm_set1_epi8('\n');
    
    // Process 16 bytes at a time
    while (ptr + 16 <= end) {
        // Load 16 bytes (unaligned)
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        
        // Compare all 16 bytes with '\n' in parallel
        __m128i cmp = _mm_cmpeq_epi8(chunk, newline);
        
        // Convert comparison result to bitmask (1 bit per byte)
        int mask = _mm_movemask_epi8(cmp);
        
        if (mask != 0) {
            // Found at least one newline, find the first one
            int offset = __builtin_ctz(mask);
            return static_cast<size_t>(ptr - data) + static_cast<size_t>(offset);
        }
        
        ptr += 16;
    }
    
    // Handle remaining bytes (< 16) with scalar fallback
    return findNewlineScalar(ptr, static_cast<size_t>(end - ptr)) + 
           static_cast<size_t>(ptr - data);
}

size_t SimdNewlineScanner::countNewlinesSSE2(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;
    
    const __m128i newline = _mm_set1_epi8('\n');
    
    while (ptr + 16 <= end) {
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        __m128i cmp = _mm_cmpeq_epi8(chunk, newline);
        int mask = _mm_movemask_epi8(cmp);
        count += static_cast<size_t>(popcount(static_cast<uint32_t>(mask)));
        ptr += 16;
    }
    
    count += countNewlinesScalar(ptr, static_cast<size_t>(end - ptr));
    return count;
}

#endif // YAMS_SIMD_SSE2

// ============================================================================
// NEON Implementation (128-bit, 16 bytes per loop - ARM/aarch64)
// ============================================================================

#if defined(YAMS_SIMD_NEON)

size_t SimdNewlineScanner::findNewlineNEON(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    
    // Load newline pattern into all 16 bytes
    const uint8x16_t newline = vdupq_n_u8('\n');
    
    while (ptr + 16 <= end) {
        // Load 16 bytes
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
        
        // Compare all 16 bytes with '\n'
        uint8x16_t cmp = vceqq_u8(chunk, newline);
        
        // Check if any byte matched
        uint64x2_t cmp64 = vreinterpretq_u64_u8(cmp);
        uint64_t low = vgetq_lane_u64(cmp64, 0);
        uint64_t high = vgetq_lane_u64(cmp64, 1);
        
        if (low != 0 || high != 0) {
            // Found newline, find position by scanning bytes
            for (int i = 0; i < 16; ++i) {
                if (ptr[i] == '\n') {
                    return static_cast<size_t>(ptr - data) + static_cast<size_t>(i);
                }
            }
        }
        
        ptr += 16;
    }
    
    return findNewlineScalar(ptr, static_cast<size_t>(end - ptr)) + 
           static_cast<size_t>(ptr - data);
}

size_t SimdNewlineScanner::countNewlinesNEON(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;
    
    const uint8x16_t newline = vdupq_n_u8('\n');
    
    while (ptr + 16 <= end) {
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
        uint8x16_t cmp = vceqq_u8(chunk, newline);
        
        // Count matches: each match is 0xFF, convert to count
        // Sum all bytes (0xFF becomes 1, 0x00 becomes 0)
        uint8x16_t ones = vandq_u8(cmp, vdupq_n_u8(1));
        uint64x2_t sum64 = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(ones)));
        count += vgetq_lane_u64(sum64, 0) + vgetq_lane_u64(sum64, 1);
        
        ptr += 16;
    }
    
    count += countNewlinesScalar(ptr, static_cast<size_t>(end - ptr));
    return count;
}

#endif // YAMS_SIMD_NEON

// ============================================================================
// Scalar Fallback (standard memchr)
// ============================================================================

size_t SimdNewlineScanner::findNewlineScalar(const char* data, size_t size) {
    const void* result = std::memchr(data, '\n', size);
    if (result == nullptr) {
        return size;
    }
    return static_cast<size_t>(static_cast<const char*>(result) - data);
}

size_t SimdNewlineScanner::countNewlinesScalar(const char* data, size_t size) {
    size_t count = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == '\n') {
            ++count;
        }
    }
    return count;
}

// ============================================================================
// Public API (dispatches to best available implementation)
// ============================================================================

size_t SimdNewlineScanner::findNewline(const char* data, size_t size) {
    if (size == 0) {
        return 0;
    }

#if defined(YAMS_SIMD_AVX2)
    return findNewlineAVX2(data, size);
#elif defined(YAMS_SIMD_SSE2)
    return findNewlineSSE2(data, size);
#elif defined(YAMS_SIMD_NEON)
    return findNewlineNEON(data, size);
#else
    return findNewlineScalar(data, size);
#endif
}

bool SimdNewlineScanner::containsNewline(const char* data, size_t size) {
    return findNewline(data, size) < size;
}

size_t SimdNewlineScanner::countNewlines(const char* data, size_t size) {
    if (size == 0) {
        return 0;
    }

#if defined(YAMS_SIMD_AVX2)
    return countNewlinesAVX2(data, size);
#elif defined(YAMS_SIMD_SSE2)
    return countNewlinesSSE2(data, size);
#elif defined(YAMS_SIMD_NEON)
    return countNewlinesNEON(data, size);
#else
    return countNewlinesScalar(data, size);
#endif
}

} // namespace services
} // namespace app
} // namespace yams
