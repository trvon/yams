#if __has_include(<benchmark/benchmark.h>)
#include <benchmark/benchmark.h>
#include <algorithm>
#include <random>
#include <vector>
#include <string>

// Optional includes (not strictly required by this file, but present to align with module context)
// If the headers are available, the build system will find them; otherwise, these aren't used directly.
#if __has_include(<yams/search/result_ranker.h>)
#include <yams/search/result_ranker.h>
#endif
#if __has_include(<yams/search/search_results.h>)
#include <yams/search/search_results.h>
#endif

// Deterministic RNG for reproducibility across runs
static std::mt19937& bench_rng() {
    static std::mt19937 gen(1337u);
    return gen;
}

// Generate a stable vector of scores in [0, 1)
static std::vector<double> makeScores(std::size_t n) {
    std::vector<double> scores;
    scores.reserve(n);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (std::size_t i = 0; i < n; ++i) {
        scores.push_back(dist(bench_rng()));
    }
    return scores;
}

// Utility: label string "N=<n>,K=<k>"
static std::string labelNK(std::size_t n, std::size_t k) {
    return "N=" + std::to_string(n) + ",K=" + std::to_string(k);
}

// Benchmark 1: Full sort by score (descending) to pick top-K
// Args: {N, K}
static void BM_ResultRanker_FullSort(benchmark::State& state) {
    const std::size_t N = static_cast<std::size_t>(state.range(0));
    const std::size_t K = static_cast<std::size_t>(state.range(1));
    state.SetLabel(labelNK(N, K));

    // Pre-generate base scores once (outside timing)
    const auto base = makeScores(N);

    std::size_t itemsProcessed = 0;
    for (auto _ : state) {
        auto scores = base;  // copy to avoid mutating base
        // Sort descending
        std::sort(scores.begin(), scores.end(), std::greater<double>());

        // Take top-K view (do not copy out to minimize overhead beyond sorting)
        const std::size_t take = std::min(K, scores.size());
        benchmark::DoNotOptimize(scores.data());
        benchmark::DoNotOptimize(take);
        itemsProcessed += N;
    }

    state.SetItemsProcessed(itemsProcessed);
    // Items/sec reported as a rate
    state.counters["items_per_sec"] = benchmark::Counter(
        static_cast<double>(itemsProcessed), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_ResultRanker_FullSort)
    ->Args({100, 10})
    ->Args({1000, 10})
    ->Args({1000, 100})
    ->Args({10000, 100})
    ->Args({100000, 100})
    ->UseRealTime();

// Benchmark 2: nth_element + sort top-K (typical top-K selection pattern)
// Args: {N, K}
static void BM_ResultRanker_NthElementThenSortTopK(benchmark::State& state) {
    const std::size_t N = static_cast<std::size_t>(state.range(0));
    const std::size_t K = static_cast<std::size_t>(state.range(1));
    state.SetLabel(labelNK(N, K));

    const auto base = makeScores(N);

    std::size_t itemsProcessed = 0;
    for (auto _ : state) {
        auto scores = base;

        const std::size_t take = std::min(K, scores.size());
        // Partition so that the first 'take' elements are the top elements (in any order)
        auto nthIt = scores.begin() + static_cast<std::ptrdiff_t>(take);
        std::nth_element(scores.begin(), nthIt, scores.end(), std::greater<double>());

        // Sort only top-K range to get strict ordering
        std::sort(scores.begin(), nthIt, std::greater<double>());

        benchmark::DoNotOptimize(scores.data());
        benchmark::DoNotOptimize(take);
        itemsProcessed += N;
    }

    state.SetItemsProcessed(itemsProcessed);
    state.counters["items_per_sec"] = benchmark::Counter(
        static_cast<double>(itemsProcessed), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_ResultRanker_NthElementThenSortTopK)
    ->Args({100, 10})
    ->Args({1000, 10})
    ->Args({1000, 100})
    ->Args({10000, 100})
    ->Args({100000, 100})
    ->UseRealTime();

// Benchmark 3: partial_sort for top-K
// Args: {N, K}
static void BM_ResultRanker_PartialSort(benchmark::State& state) {
    const std::size_t N = static_cast<std::size_t>(state.range(0));
    const std::size_t K = static_cast<std::size_t>(state.range(1));
    state.SetLabel(labelNK(N, K));

    const auto base = makeScores(N);

    std::size_t itemsProcessed = 0;
    for (auto _ : state) {
        auto scores = base;

        const std::size_t take = std::min(K, scores.size());
        // partial_sort ensures the first 'take' elements are sorted and are the top ones
        std::partial_sort(scores.begin(),
                          scores.begin() + static_cast<std::ptrdiff_t>(take),
                          scores.end(),
                          std::greater<double>());

        benchmark::DoNotOptimize(scores.data());
        benchmark::DoNotOptimize(take);
        itemsProcessed += N;
    }

    state.SetItemsProcessed(itemsProcessed);
    state.counters["items_per_sec"] = benchmark::Counter(
        static_cast<double>(itemsProcessed), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_ResultRanker_PartialSort)
    ->Args({100, 10})
    ->Args({1000, 10})
    ->Args({1000, 100})
    ->Args({10000, 100})
    ->Args({100000, 100})
    ->UseRealTime();

BENCHMARK_MAIN();
#else
#include <iostream>
int main(int, char**) {
    std::cerr << "Benchmarks are disabled: Google Benchmark not available.\n";
    return 0;
}
#endif