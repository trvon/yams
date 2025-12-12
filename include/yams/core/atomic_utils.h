#pragma once

/**
 * @file atomic_utils.h
 * @brief Cross-platform atomic counter utilities with underflow protection.
 *
 * Provides saturating arithmetic operations for atomic counters to prevent
 * underflow when tracking metrics like byte counts, document counts, etc.
 * These utilities are platform-agnostic and work on Windows, Linux, and macOS.
 */

#include <atomic>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace yams::core {

/**
 * @brief Performs a saturating subtraction on an atomic counter (minimum 0).
 *
 * Uses a lock-free compare-and-swap loop to atomically subtract `delta` from
 * the counter, clamping the result to zero if the counter would underflow.
 * This prevents the counter from wrapping to UINT64_MAX on underflow.
 *
 * @tparam T Integral type (must be unsigned)
 * @param counter The atomic counter to modify
 * @param delta The value to subtract
 * @return The value before subtraction
 *
 * Thread-safe: Yes (lock-free CAS loop)
 * Progress: Wait-free for typical workloads (bounded retries in practice)
 *
 * @example
 *   std::atomic<uint64_t> byteCount{100};
 *   saturating_sub(byteCount, 150);  // byteCount is now 0, not UINT64_MAX-50
 */
template <typename T>
    requires std::is_unsigned_v<T>
T saturating_sub(std::atomic<T>& counter, T delta) noexcept {
    T current = counter.load(std::memory_order_relaxed);
    while (true) {
        const T next = (current <= delta) ? T{0} : (current - delta);
        if (counter.compare_exchange_weak(current, next, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
            return current;
        }
        // current is updated by compare_exchange_weak on failure
    }
}

/**
 * @brief Performs a saturating addition on an atomic counter (maximum T_MAX).
 *
 * Uses a lock-free compare-and-swap loop to atomically add `delta` to
 * the counter, clamping the result to the maximum value of T if it would overflow.
 *
 * @tparam T Integral type (must be unsigned)
 * @param counter The atomic counter to modify
 * @param delta The value to add
 * @return The value before addition
 *
 * Thread-safe: Yes (lock-free CAS loop)
 */
template <typename T>
    requires std::is_unsigned_v<T>
T saturating_add(std::atomic<T>& counter, T delta) noexcept {
    constexpr T max_val = std::numeric_limits<T>::max();
    T current = counter.load(std::memory_order_relaxed);
    while (true) {
        const T headroom = max_val - current;
        const T next = (delta > headroom) ? max_val : (current + delta);
        if (counter.compare_exchange_weak(current, next, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
            return current;
        }
    }
}

/**
 * @brief Conditionally decrements an atomic counter only if it's greater than zero.
 *
 * This is useful for counters where a decrement should be a no-op if already at zero,
 * avoiding the need for external synchronization to check before decrement.
 *
 * @tparam T Integral type (must be unsigned)
 * @param counter The atomic counter to modify
 * @return true if the counter was decremented, false if it was already zero
 */
template <typename T>
    requires std::is_unsigned_v<T>
bool decrement_if_positive(std::atomic<T>& counter) noexcept {
    T current = counter.load(std::memory_order_relaxed);
    while (current > T{0}) {
        if (counter.compare_exchange_weak(current, current - T{1}, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
            return true;
        }
        // current is updated by compare_exchange_weak on failure
    }
    return false;
}

/**
 * @brief Atomically sets counter to max(counter, value).
 *
 * Useful for tracking high-water marks without external locking.
 *
 * @tparam T Integral type
 * @param counter The atomic counter to modify
 * @param value The value to compare against
 * @return The previous value of the counter
 */
template <typename T>
    requires std::is_arithmetic_v<T>
T atomic_max(std::atomic<T>& counter, T value) noexcept {
    T current = counter.load(std::memory_order_relaxed);
    while (current < value) {
        if (counter.compare_exchange_weak(current, value, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
            return current;
        }
    }
    return current;
}

/**
 * @brief Atomically sets counter to min(counter, value).
 *
 * Useful for tracking low-water marks without external locking.
 *
 * @tparam T Integral type
 * @param counter The atomic counter to modify
 * @param value The value to compare against
 * @return The previous value of the counter
 */
template <typename T>
    requires std::is_arithmetic_v<T>
T atomic_min(std::atomic<T>& counter, T value) noexcept {
    T current = counter.load(std::memory_order_relaxed);
    while (current > value) {
        if (counter.compare_exchange_weak(current, value, std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
            return current;
        }
    }
    return current;
}

} // namespace yams::core
