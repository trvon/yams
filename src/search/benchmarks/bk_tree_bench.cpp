#include <string>
#include <vector>
#include <benchmark/benchmark.h>
#include <yams/search/bk_tree.h>

using namespace yams::search;

// Sample words for testing
static const std::vector<std::string> sampleWords = {
    "hello",       "world",   "search", "engine",   "fuzzy", "matching",  "algorithm",  "benchmark",
    "performance", "testing", "query",  "database", "index", "retrieval", "similarity", "distance"};

// BK-tree construction benchmark
static void BM_BKTree_Construction(benchmark::State& state) {
    const int vocab = static_cast<int>(state.range(0));
    for (auto _ : state) {
        BKTree tree;
        for (int i = 0; i < vocab; ++i) {
            tree.add("w" + std::to_string(i));
        }
        benchmark::DoNotOptimize(tree);
    }

    // Items processed = total inserts
    state.SetItemsProcessed(state.iterations() * static_cast<uint64_t>(vocab));
    // Insertion throughput (items/sec)
    state.counters["insert_qps"] =
        benchmark::Counter(static_cast<double>(vocab) * static_cast<double>(state.iterations()),
                           benchmark::Counter::kIsRate);
}
BENCHMARK(BM_BKTree_Construction)->Arg(16)->Arg(256)->Arg(4096)->Arg(65536)->UseRealTime();

// BK-tree search benchmark
static void BM_BKTree_Search(benchmark::State& state) {
    const int vocab = static_cast<int>(state.range(0));
    const int maxDistance = static_cast<int>(state.range(1));

    // Pre-build tree
    BKTree tree;
    for (int i = 0; i < vocab; ++i) {
        tree.add("w" + std::to_string(i));
    }

    const std::string query = "w0"; // deterministic query
    size_t total_matches = 0;
    size_t total_query_bytes = 0;

    state.SetLabel("vocab=" + std::to_string(vocab) + ",dist=" + std::to_string(maxDistance));

    for (auto _ : state) {
        auto results = tree.search(query, maxDistance);
        total_matches += results.size();
        total_query_bytes += query.size();
        benchmark::DoNotOptimize(results);
    }

    state.SetItemsProcessed(state.iterations()); // queries executed
    state.SetBytesProcessed(total_query_bytes);  // total query bytes
    if (state.iterations() > 0) {
        state.counters["avg_matches"] =
            static_cast<double>(total_matches) / static_cast<double>(state.iterations());
        state.counters["throughput_Bps"] =
            benchmark::Counter(static_cast<double>(total_query_bytes), benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_BKTree_Search)
    ->Args({100, 1})
    ->Args({100, 2})
    ->Args({1000, 2})
    ->Args({10000, 2})
    ->Args({10000, 3})
    ->UseRealTime();

// Various distance searches
static void BM_BKTree_SearchVarious(benchmark::State& state) {
    BKTree tree;
    for (const auto& word : sampleWords) {
        tree.add(word);
    }

    size_t queryIndex = 0;

    for (auto _ : state) {
        const auto& query = sampleWords[queryIndex % sampleWords.size()];
        auto results = tree.search(query, 1 + (queryIndex % 3)); // distances 1-3
        benchmark::DoNotOptimize(results);
        queryIndex++;
    }
}
BENCHMARK(BM_BKTree_SearchVarious);

BENCHMARK_MAIN();