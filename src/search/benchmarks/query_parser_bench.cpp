#if __has_include(<benchmark/benchmark.h>)
#include <benchmark/benchmark.h>
#include <yams/search/query_parser.h>
#include <yams/search/query_ast.h>
#include <string>
#include <vector>

using namespace yams::search;

// Helpers to generate queries of a target length and optional nesting
static std::string MakeFlatQuery(size_t target_len) {
    static const char* terms[] = {
        "database","search","engine","vector","semantic","precision",
        "recall","index","hybrid","ranking","token","parser","query"
    };
    constexpr size_t N = sizeof(terms) / sizeof(terms[0]);
    std::string q;
    q.reserve(target_len + 16);
    size_t i = 0;
    while (q.size() < target_len) {
        if (!q.empty()) q.push_back(' ');
        q += terms[i % N];
        ++i;
    }
    return q;
}

static std::string MakeNestedQuery(size_t target_len, int nesting) {
    std::string expr = "t";
    for (int i = 0; i < nesting; ++i) {
        expr = "(" + expr + " AND t" + std::to_string(i) + ")";
    }
    std::string q;
    q.reserve(target_len + expr.size());
    while (q.size() < target_len) {
        if (!q.empty()) q.push_back(' ');
        q += expr;
    }
    return q;
}

// Simple query parsing benchmark
static void BM_QueryParser_Simple(benchmark::State& state) {
    QueryParser parser;
    const std::string query = "simple query";
    size_t total_bytes = 0;
    size_t total_ast_chars = 0;
    
    for (auto _ : state) {
        auto result = parser.parse(query);
        total_bytes += query.size();
        if (result.has_value()) {
            auto ast = std::move(result).value();
            // Simple traversal to ensure work is done
            std::string astString = ast->toString();
            total_ast_chars += astString.size();
            benchmark::DoNotOptimize(astString);
        }
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(total_bytes);
    if (state.iterations() > 0) {
        state.counters["avg_ast_chars"] = static_cast<double>(total_ast_chars) / static_cast<double>(state.iterations());
        state.counters["throughput_Bps"] = benchmark::Counter(static_cast<double>(total_bytes), benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_QueryParser_Simple)->UseRealTime();

// Complex query parsing benchmark
static void BM_QueryParser_Complex(benchmark::State& state) {
    QueryParser parser;
    const std::string query = "very complex (query OR search) AND (terms OR words) OR (expressions AND logic)";
    size_t total_bytes = 0;
    size_t total_ast_chars = 0;
    
    for (auto _ : state) {
        auto result = parser.parse(query);
        total_bytes += query.size();
        if (result.has_value()) {
            auto ast = std::move(result).value();
            // Simple traversal to ensure work is done
            std::string astString = ast->toString();
            total_ast_chars += astString.size();
            benchmark::DoNotOptimize(astString);
        }
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(total_bytes);
    if (state.iterations() > 0) {
        state.counters["avg_ast_chars"] = static_cast<double>(total_ast_chars) / static_cast<double>(state.iterations());
        state.counters["throughput_Bps"] = benchmark::Counter(static_cast<double>(total_bytes), benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_QueryParser_Complex)->UseRealTime();

// Parameterized: query length and nesting depth
static void BM_QueryParser_LenNesting(benchmark::State& state) {
    QueryParser parser;
    const size_t query_len = static_cast<size_t>(state.range(0));
    const int nesting = static_cast<int>(state.range(1));
    state.SetLabel("len=" + std::to_string(query_len) + ",nest=" + std::to_string(nesting));
    
    size_t total_bytes = 0;
    size_t total_ast_chars = 0;
    
    for (auto _ : state) {
        std::string query = MakeNestedQuery(query_len, nesting);
        total_bytes += query.size();
        auto result = parser.parse(query);
        if (result.has_value()) {
            auto ast = std::move(result).value();
            std::string astString = ast->toString();
            total_ast_chars += astString.size();
            benchmark::DoNotOptimize(astString);
        }
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(total_bytes);
    if (state.iterations() > 0) {
        state.counters["avg_ast_chars"] = static_cast<double>(total_ast_chars) / static_cast<double>(state.iterations());
        state.counters["throughput_Bps"] = benchmark::Counter(static_cast<double>(total_bytes), benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_QueryParser_LenNesting)
    ->Args({16, 0})
    ->Args({64, 0})
    ->Args({128, 0})
    ->Args({64, 1})
    ->Args({128, 1})
    ->Args({256, 2})
    ->UseRealTime();

BENCHMARK_MAIN();
#else
#include <iostream>
int main(int, char**) {
    std::cerr << "Benchmarks are disabled: Google Benchmark not available.\n";
    return 0;
}
#endif