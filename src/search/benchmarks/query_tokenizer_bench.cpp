#if __has_include(<benchmark/benchmark.h>)
#include <benchmark/benchmark.h>
#include <yams/search/query_tokenizer.h>
#include <string>
#include <vector>
#include <random>

// NOLINTBEGIN(readability-identifier-naming)

using namespace yams::search;

// Deterministic RNG for reproducibility
static std::mt19937& rng() {
    static std::mt19937 gen(123456789u);
    return gen;
}

enum class AlphabetKind : int {
    AsciiWords = 0,     // simple lower-case words
    AlphaNum,           // alphanumeric terms
    Operators,          // AND/OR/NOT with parentheses and quotes
    Mixed               // mixture of words, operators, wildcards, phrases
};

static std::string makeAsciiWordsQuery(size_t targetLen) {
    static const char* words[] = {
        "search", "engine", "database", "vector", "semantic", "precision",
        "recall", "index", "hybrid", "ranking", "token", "parser", "query"
    };
    constexpr size_t N = sizeof(words) / sizeof(words[0]);

    std::string query;
    query.reserve(targetLen + 16);

    size_t i = 0;
    while (query.size() < targetLen) {
        if (!query.empty()) query.push_back(' ');
        query += words[i % N];
        ++i;
    }
    return query;
}

static std::string makeAlphaNumQuery(size_t targetLen) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";

    std::uniform_int_distribution<size_t> pick(0, sizeof(charset) - 2);

    std::string query;
    query.reserve(targetLen + 16);

    while (query.size() < targetLen) {
        // produce term of 3-10 chars
        std::uniform_int_distribution<int> lenDist(3, 10);
        int termLen = lenDist(rng());

        if (!query.empty()) query.push_back(' ');
        for (int i = 0; i < termLen; ++i) {
            query.push_back(charset[pick(rng())]);
        }
    }
    return query;
}

static std::string makeOperatorsQuery(size_t targetLen) {
    // Construct with logical operators and parentheses/quotes
    static const char* patterns[] = {
        R"(title:"exact phrase" AND content:vector)",
        R"((search OR query) AND (engine OR index))",
        R"(NOT archived AND (status:active OR status:recent))",
        R"((alpha AND beta) OR (gamma AND NOT delta))",
    };
    constexpr size_t N = sizeof(patterns) / sizeof(patterns[0]);

    std::string query;
    query.reserve(targetLen + 32);
    size_t i = 0;

    while (query.size() < targetLen) {
        if (!query.empty()) query.push_back(' ');
        query += patterns[i % N];
        ++i;
    }
    return query;
}

static std::string makeMixedQuery(size_t targetLen) {
    static const char* words[] = {
        "search", "engine", "database", "vector", "semantic", "precision",
        "recall", "index", "hybrid", "ranking", "token", "parser", "query"
    };
    constexpr size_t N = sizeof(words) / sizeof(words[0]);

    std::uniform_int_distribution<int> pick(0, 3);
    std::uniform_int_distribution<int> wordPick(0, static_cast<int>(N - 1));
    std::uniform_int_distribution<int> opPick(0, 2); // AND/OR/NOT

    std::string query;
    query.reserve(targetLen + 64);

    auto appendWord = [&](bool quoted) {
        if (quoted) query.push_back('"');
        query += words[static_cast<size_t>(wordPick(rng()))];
        if (quoted) query.push_back('"');
    };

    while (query.size() < targetLen) {
        if (!query.empty()) query.push_back(' ');

        int choice = pick(rng());
        switch (choice) {
            case 0: { // word
                appendWord(false);
                break;
            }
            case 1: { // phrase
                appendWord(true);
                break;
            }
            case 2: { // wildcard
                query += words[static_cast<size_t>(wordPick(rng()))];
                query.push_back('*');
                break;
            }
            case 3: { // operator + grouping
                query.push_back('(');
                appendWord(false);
                query.push_back(' ');
                const char* op = (opPick(rng()) == 0 ? "AND" : (opPick(rng()) == 1 ? "OR" : "NOT"));
                query += op;
                query.push_back(' ');
                appendWord(false);
                query.push_back(')');
                break;
            }
            default:
                appendWord(false);
                break;
        }
    }

    return query;
}

static std::string makeQuery(size_t targetLen, AlphabetKind kind) {
    switch (kind) {
        case AlphabetKind::AsciiWords: return makeAsciiWordsQuery(targetLen);
        case AlphabetKind::AlphaNum:   return makeAlphaNumQuery(targetLen);
        case AlphabetKind::Operators:  return makeOperatorsQuery(targetLen);
        case AlphabetKind::Mixed:      return makeMixedQuery(targetLen);
        default:                       return makeAsciiWordsQuery(targetLen);
    }
}

static const char* kindToString(AlphabetKind kind) {
    switch (kind) {
        case AlphabetKind::AsciiWords: return "ascii";
        case AlphabetKind::AlphaNum:   return "alnum";
        case AlphabetKind::Operators:  return "ops";
        case AlphabetKind::Mixed:      return "mixed";
        default:                       return "ascii";
    }
}

// Args: { query_length, alphabet_kind }
static void BM_QueryTokenizer_LenKind(benchmark::State& state) {
    const size_t queryLen = static_cast<size_t>(state.range(0));
    const auto kind = static_cast<AlphabetKind>(state.range(1));

    QueryTokenizer tokenizer;
    size_t totalBytes = 0;
    size_t totalTokens = 0;

    state.SetLabel(std::string("len=") + std::to_string(queryLen) +
                   ",kind=" + kindToString(kind));

    for (auto _ : state) {
        std::string query = makeQuery(queryLen, kind);
        totalBytes += query.size();

        // Tokenize; do not assume exact return type (vector, optional, Result, etc.).
        // We only prevent optimization on the returned value.
        auto tokens = tokenizer.tokenize(query);
        benchmark::DoNotOptimize(tokens);

        // Best-effort token count (works when tokens is a container). If not, ignored by optimizer.
        // We avoid touching elements to keep the benchmark focused on tokenize().
        // NOLINTNEXTLINE(bugprone-sizeof-expression)
        if constexpr (requires { tokens.size(); }) {
            totalTokens += tokens.size();
        }
    }

    state.SetItemsProcessed(state.iterations());            // number of queries
    state.SetBytesProcessed(totalBytes);                    // total query bytes processed
    if (state.iterations() > 0) {
        state.counters["avg_tokens_per_query"] =
            benchmark::Counter(static_cast<double>(totalTokens) /
                               static_cast<double>(state.iterations()));
        state.counters["throughput_Bps"] =
            benchmark::Counter(static_cast<double>(totalBytes),
                               benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_QueryTokenizer_LenKind)
    ->Args({16,  static_cast<int>(AlphabetKind::AsciiWords)})
    ->Args({64,  static_cast<int>(AlphabetKind::AsciiWords)})
    ->Args({128, static_cast<int>(AlphabetKind::AsciiWords)})
    ->Args({64,  static_cast<int>(AlphabetKind::AlphaNum)})
    ->Args({128, static_cast<int>(AlphabetKind::AlphaNum)})
    ->Args({64,  static_cast<int>(AlphabetKind::Operators)})
    ->Args({128, static_cast<int>(AlphabetKind::Operators)})
    ->Args({64,  static_cast<int>(AlphabetKind::Mixed)})
    ->Args({256, static_cast<int>(AlphabetKind::Mixed)})
    ->UseRealTime();

// Pathological operator-heavy case to stress parentheses and boolean logic
static void BM_QueryTokenizer_PathologicalOps(benchmark::State& state) {
    QueryTokenizer tokenizer;

    // Build a deeply nested expression of size ~queryLen
    const size_t queryLen = static_cast<size_t>(state.range(0));
    std::string query;
    query.reserve(queryLen + 64);

    // Generate like (((t AND t1) OR (t2 AND NOT t3)) AND ... )
    int depth = 0;
    while (query.size() + 32 < queryLen) {
        query += "((t AND t1) OR (t2 AND NOT t3)) ";
        depth++;
    }

    size_t totalBytes = 0;
    size_t totalTokens = 0;

    for (auto _ : state) {
        totalBytes += query.size();
        auto tokens = tokenizer.tokenize(query);
        benchmark::DoNotOptimize(tokens);

        if constexpr (requires { tokens.size(); }) {
            totalTokens += tokens.size();
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(totalBytes);
    state.counters["depth_hint"] = static_cast<double>(depth);
    if (state.iterations() > 0) {
        state.counters["avg_tokens_per_query"] =
            benchmark::Counter(static_cast<double>(totalTokens) /
                               static_cast<double>(state.iterations()));
        state.counters["throughput_Bps"] =
            benchmark::Counter(static_cast<double>(totalBytes),
                               benchmark::Counter::kIsRate);
    }
}
BENCHMARK(BM_QueryTokenizer_PathologicalOps)
    ->Arg(256)
    ->Arg(1024)
    ->UseRealTime();

BENCHMARK_MAIN();
#else
#include <iostream>
int main(int, char**) {
    std::cerr << "Benchmarks are disabled: Google Benchmark not available.\n";
    return 0;
}
#endif

// NOLINTEND(readability-identifier-naming)