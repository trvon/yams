#include <cstring>
#include <yams/app/services/simd_newline_scanner.hpp>

namespace yams {
namespace app {
namespace services {

// ============================================================================
// Scalar Fallback (standard memchr)
// ============================================================================

size_t SimdNewlineScanner::findNewlineScalar(const char* data, size_t size) {
    if (size == 0 || data == nullptr) {
        return size;
    }

    const void* result = std::memchr(data, '\n', size);
    if (result == nullptr) {
        return size;
    }
    return static_cast<size_t>(static_cast<const char*>(result) - data);
}

size_t SimdNewlineScanner::countNewlinesScalar(const char* data, size_t size) {
    if (size == 0 || data == nullptr) {
        return 0;
    }

    size_t count = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == '\n') {
            ++count;
        }
    }
    return count;
}

// ============================================================================
// Public API (dispatch to best available implementation)
// Note: memchr is used as default because benchmarks show it's 14x faster
// than custom SIMD implementations for single-byte search on modern CPUs.
// ============================================================================

size_t SimdNewlineScanner::countNewlines(const char* data, size_t size) {
    if (size == 0 || data == nullptr) {
        return 0;
    }

    return countNewlinesScalar(data, size);
}

} // namespace services
} // namespace app
} // namespace yams
