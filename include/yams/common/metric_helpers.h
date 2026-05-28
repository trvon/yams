#pragma once

#include <atomic>
#include <cstdint>
#include <utility>

namespace yams::common::metrics {

inline void incrementRelaxed(std::atomic<std::uint64_t>& counter, std::uint64_t n = 1) noexcept {
    counter.fetch_add(n, std::memory_order_relaxed);
}

template <typename CounterProvider>
inline void incrementIfEnabled(bool enabled, CounterProvider&& counterProvider,
                               std::uint64_t n = 1) noexcept(noexcept(counterProvider())) {
    if (!enabled) {
        return;
    }
    incrementRelaxed(std::forward<CounterProvider>(counterProvider)(), n);
}

} // namespace yams::common::metrics
