#pragma once

#include <atomic>
#include <cstdint>

namespace yams::vector::batchmetrics {

struct Metrics {
    std::atomic<std::uint64_t> effectiveTokens{0};
    std::atomic<std::uint64_t> recentAvgDocs{0};
    std::atomic<std::uint64_t> successes{0};
    std::atomic<std::uint64_t> failures{0};
    std::atomic<std::uint64_t> backoffs{0};
};

// Get mutable global metrics singleton
Metrics& get();

// Convenience helpers
inline void set_effective_tokens(std::uint64_t t) {
    get().effectiveTokens.store(t);
}
inline void record_success(std::uint64_t docs_in_batch) {
    auto& m = get();
    m.successes.fetch_add(1);
    // Exponential moving average with alpha=0.2
    auto prev = m.recentAvgDocs.load();
    std::uint64_t next = static_cast<std::uint64_t>(prev * 0.8 + docs_in_batch * 0.2);
    if (next == 0)
        next = docs_in_batch;
    m.recentAvgDocs.store(next);
}
inline void record_failure() {
    get().failures.fetch_add(1);
}
inline void record_backoff() {
    get().backoffs.fetch_add(1);
}

} // namespace yams::vector::batchmetrics
