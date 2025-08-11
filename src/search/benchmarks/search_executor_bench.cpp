#include <benchmark/benchmark.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/connection_pool.h>
#include <string>
#include <memory>

using namespace yams::search;
using namespace yams::metadata;

// Helper to generate a query string of approximately target_len characters
static std::string MakeQuery(size_t target_len) {
    static const char* terms[] = {
        "database", "search", "engine", "vector", "semantic", "precision",
        "recall", "index", "hybrid", "ranking", "token", "parser", "query"
    };
    constexpr size_t terms_count = sizeof(terms) / sizeof(terms[0]);
    std::string q;
    q.reserve(target_len + 16);
    size_t i = 0;
    while (q.size() < target_len) {
        if (!q.empty()) q.push_back(' ');
        q.append(terms[i % terms_count]);
        ++i;
    }
    return q;
}

// Parameterized search benchmark: query length (chars) and limit (results)
static void BM_SearchExecutor_QueryLen_Limit(benchmark::State& state) {
    const size_t query_len = static_cast<size_t>(state.range(0));
    const size_t limit = static_cast<size_t>(state.range(1));

    // Prepare repository and executor (outside timing)
    auto pool = std::make_shared<ConnectionPool>(":memory:");
    auto initResult = pool->initialize();
    if (!initResult) {
        state.SkipWithError("Failed to initialize metadata pool");
        return;
    }
    auto metadataRepo = std::make_shared<MetadataRepository>(*pool);
    SearchExecutor executor(metadataRepo);

    // NOTE: Repository is currently unseeded (no public seeding API used here).
    // This still exercises parsing/planning and integration layers.
    // Label the run for clarity in reports.
    state.SetLabel("len=" + std::to_string(query_len) + ",limit=" + std::to_string(limit));

    size_t total_hits = 0;
    size_t total_query_bytes = 0;

    for (auto _ : state) {
        std::string query = MakeQuery(query_len);
        total_query_bytes += query.size();

        auto results = executor.search(query, limit);
        total_hits += results.size();
        benchmark::DoNotOptimize(results);
    }

    // Items == queries executed
    state.SetItemsProcessed(state.iterations());
    // Bytes processed == total query bytes
    state.SetBytesProcessed(total_query_bytes);

    // Counters
    if (state.iterations() > 0) {
        state.counters["avg_hits_per_query"] = benchmark::Counter(
            static_cast<double>(total_hits) / static_cast<double>(state.iterations()));
        // Report throughput as a rate (bytes per second)
        state.counters["throughput_Bps"] = benchmark::Counter(
            static_cast<double>(total_query_bytes), benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_SearchExecutor_QueryLen_Limit)
    ->Args({16, 10})
    ->Args({64, 10})
    ->Args({128, 10})
    ->Args({16, 20})
    ->Args({64, 20})
    ->Args({128, 20})
    ->UseRealTime();

BENCHMARK_MAIN();