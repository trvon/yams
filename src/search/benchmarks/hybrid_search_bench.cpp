/*
  hybrid_search_bench.cpp

  Google Benchmark to measure end-to-end query latency for YAMS hybrid search via the CLI.
  It also estimates stage breakdowns using ablation runs (keyword-only, vector-only, KG-only)
  by toggling environment variables before invoking the CLI.

  Design choices:
  - We invoke the installed/built CLI (yams) instead of linking directly to internal search APIs.
    This keeps the benchmark stable against internal API changes and mirrors real user behavior.
  - Ablations are controlled via environment variables that the yams process can read. If the
    running version does not support these variables, the benchmark still executes and reports
    observed latencies (the ablations will effectively be the same as hybrid).
  - Stage breakdown is estimated by timing the ablations individually.
  - Output counters expose averaged milliseconds across iterations for hybrid and ablated modes.

  Configuration (environment variables):
  - YAMS_CLI_PATH        : Path to the yams CLI binary. Default: "yams"
  - YAMS_BENCH_QUERY     : Query string to run. Default: "PBI"
  - YAMS_BENCH_TOPK      : Limit for results. Default: "10"
  - YAMS_BENCH_ARGS      : Extra args for search subcommand (e.g., "--storage /path")
  - YAMS_BENCH_VERBOSE   : "1" to pass --verbose (and --json) to CLI. Default: "1"
  - YAMS_BENCH_WARMUP    : Number of warmup runs per case. Default: "1"

  Ablation env toggles (set for the spawned yams process only):
  - YAMS_DISABLE_KEYWORD : "1" disables keyword scoring
  - YAMS_DISABLE_VECTOR  : "1" disables vector scoring
  - YAMS_DISABLE_KG      : "1" disables KG scoring

  Note:
  - This benchmark assumes the yams CLI supports "--json" and the "search" subcommand.
  - If the CLI path is invalid or invocation fails, the benchmark will still run and report
    the overhead measured (likely near-zero with fast failures).
*/

#include <benchmark/benchmark.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(_WIN32)
  #include <windows.h>
#else
  #include <unistd.h>
#endif

namespace {

// --------- Utility helpers ---------

std::string getEnvOr(const char* key, const std::string& fallback) {
    if (const char* v = std::getenv(key)) {
        return std::string(v);
    }
    return fallback;
}

int toIntOr(const std::string& s, int fallback) {
    try {
        return std::stoi(s);
    } catch (...) {
        return fallback;
    }
}

// Basic shell escaping for single-quoted strings.
// Replaces ' with '\'' (close quote, escaped single quote, reopen).
std::string shellEscapeSingleQuoted(const std::string& in) {
    std::string out;
    out.reserve(in.size() + 16);
    out.push_back('\'');
    for (char c : in) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('\'');
    return out;
}

// Temporarily set a group of environment variables; restores them when destroyed.
class ScopedEnv {
public:
    explicit ScopedEnv(std::unordered_map<std::string, std::optional<std::string>> vars) {
        // Capture previous values and set new ones
        for (auto& [k, v] : vars) {
            const char* prev = std::getenv(k.c_str());
            if (prev) {
                previous_[k] = std::string(prev);
            } else {
                previous_[k] = std::nullopt;
            }
            if (v.has_value()) {
                setEnv(k.c_str(), v->c_str());
            } else {
                unsetEnv(k.c_str());
            }
        }
        keys_.reserve(vars.size());
        for (auto& [k, _] : vars) keys_.push_back(k);
    }

    ~ScopedEnv() {
        // Restore previous values
        for (const auto& k : keys_) {
            auto it = previous_.find(k);
            if (it == previous_.end()) continue;
            if (it->second.has_value()) {
                setEnv(k.c_str(), it->second->c_str());
            } else {
                unsetEnv(k.c_str());
            }
        }
    }

    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

private:
    static void setEnv(const char* key, const char* value) {
    #if defined(_WIN32)
        _putenv_s(key, value);
    #else
        ::setenv(key, value, 1);
    #endif
    }

    static void unsetEnv(const char* key) {
    #if defined(_WIN32)
        _putenv_s(key, "");
    #else
        ::unsetenv(key);
    #endif
    }

    std::vector<std::string> keys_;
    std::unordered_map<std::string, std::optional<std::string>> previous_;
};

// Execute a command and optionally consume output (discarded here).
// Returns true if the command executed and exited with status 0.
bool execCommand(const std::string& cmd) {
#if defined(_WIN32)
    // On Windows, use _popen/_pclose
    FILE* pipe = _popen(cmd.c_str(), "rt");
    if (!pipe) return false;
    char buffer[256];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        // Discard or capture output if needed
    }
    int rc = _pclose(pipe);
    return rc == 0;
#else
    // On POSIX, use popen/pclose
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) return false;
    char buffer[256];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        // Discard or capture output if needed
    }
    int status = pclose(pipe);
    // If pclose returns -1, we treat as failure
    if (status == -1) return false;
    // WEXITSTATUS only meaningful if WIFEXITED(status); be tolerant
    return status == 0;
#endif
}

// Time a single CLI search invocation with specific ablation toggles.
// Returns elapsed milliseconds.
double timeSearchCLI(const std::string& cliPath,
                     const std::string& query,
                     int topK,
                     bool verbose,
                     const std::string& extraArgs,
                     bool disableKeyword,
                     bool disableVector,
                     bool disableKG) {
    // Prepare environment toggles for the child process
    ScopedEnv env({
        {"YAMS_DISABLE_KEYWORD", disableKeyword ? std::optional<std::string>("1") : std::nullopt},
        {"YAMS_DISABLE_VECTOR",  disableVector  ? std::optional<std::string>("1") : std::nullopt},
        {"YAMS_DISABLE_KG",      disableKG      ? std::optional<std::string>("1") : std::nullopt},
    });

    // Build command
    std::ostringstream cmd;
#if defined(_WIN32)
    // Basic Windows quoting (best effort)
    cmd << "\"" << cliPath << "\" ";
    if (verbose) cmd << "--json --verbose ";
    cmd << "search ";
    if (!extraArgs.empty()) cmd << extraArgs << " ";
    cmd << "\"" << query << "\" ";
    cmd << "--limit " << topK;
#else
    const std::string q = shellEscapeSingleQuoted(query);
    // Prefer absolute/relative path as-is; rely on PATH otherwise
    cmd << cliPath << " ";
    if (verbose) cmd << "--json --verbose ";
    cmd << "search ";
    if (!extraArgs.empty()) cmd << extraArgs << " ";
    cmd << q << " ";
    cmd << "--limit " << topK;
#endif

    const auto start = std::chrono::steady_clock::now();
    (void)execCommand(cmd.str()); // We don't fail the benchmark if the command fails
    const auto end   = std::chrono::steady_clock::now();

    const auto ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
    return ms;
}

struct BenchConfig {
    std::string cliPath = getEnvOr("YAMS_CLI_PATH", "yams");
    std::string query   = getEnvOr("YAMS_BENCH_QUERY", "PBI");
    int topK            = toIntOr(getEnvOr("YAMS_BENCH_TOPK", "10"), 10);
    std::string extraArgs = getEnvOr("YAMS_BENCH_ARGS", "");
    bool verbose        = (getEnvOr("YAMS_BENCH_VERBOSE", "1") != "0");
    int warmup          = toIntOr(getEnvOr("YAMS_BENCH_WARMUP", "1"), 1);
};

BenchConfig getBenchConfig() {
    static BenchConfig cfg{};
    return cfg;
}

} // namespace

// --------- Benchmarks ---------

// End-to-end hybrid latency
static void BM_Query_Hybrid(benchmark::State& state) {
    const auto cfg = getBenchConfig();

    // Warmup
    for (int i = 0; i < cfg.warmup; ++i) {
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/false, /*disableVector=*/false, /*disableKG=*/false);
    }

    for (auto _ : state) {
        const double ms = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                        /*disableKeyword=*/false, /*disableVector=*/false, /*disableKG=*/false);
        benchmark::DoNotOptimize(ms);
        state.SetIterationTime(ms / 1000.0); // seconds for ManualTime? (not using ManualTime; counter will show avg)
    }

    // Report average ms per iteration as a counter
    // (Google Benchmark reports wall time already; we include a counter for convenient export)
    state.counters["hybrid_ms"] = benchmark::Counter( state.iterations(), benchmark::Counter::kIsRate) * 0.0; // placeholder
}
BENCHMARK(BM_Query_Hybrid)->Unit(benchmark::kMillisecond);

// Keyword-only latency
static void BM_Query_KeywordOnly(benchmark::State& state) {
    const auto cfg = getBenchConfig();

    for (int i = 0; i < cfg.warmup; ++i) {
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/false, /*disableVector=*/true, /*disableKG=*/true);
    }

    for (auto _ : state) {
        const double ms = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                        /*disableKeyword=*/false, /*disableVector=*/true, /*disableKG=*/true);
        benchmark::DoNotOptimize(ms);
        state.SetIterationTime(ms / 1000.0);
    }
}
BENCHMARK(BM_Query_KeywordOnly)->Unit(benchmark::kMillisecond);

// Vector-only latency
static void BM_Query_VectorOnly(benchmark::State& state) {
    const auto cfg = getBenchConfig();

    for (int i = 0; i < cfg.warmup; ++i) {
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/true, /*disableVector=*/false, /*disableKG=*/true);
    }

    for (auto _ : state) {
        const double ms = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                        /*disableKeyword=*/true, /*disableVector=*/false, /*disableKG=*/true);
        benchmark::DoNotOptimize(ms);
        state.SetIterationTime(ms / 1000.0);
    }
}
BENCHMARK(BM_Query_VectorOnly)->Unit(benchmark::kMillisecond);

// KG-only latency
static void BM_Query_KGOnly(benchmark::State& state) {
    const auto cfg = getBenchConfig();

    for (int i = 0; i < cfg.warmup; ++i) {
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/true, /*disableVector=*/true, /*disableKG=*/false);
    }

    for (auto _ : state) {
        const double ms = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                        /*disableKeyword=*/true, /*disableVector=*/true, /*disableKG=*/false);
        benchmark::DoNotOptimize(ms);
        state.SetIterationTime(ms / 1000.0);
    }
}
BENCHMARK(BM_Query_KGOnly)->Unit(benchmark::kMillisecond);

// Combined: run ablations in-sequence per iteration and report breakdown counters.
static void BM_Query_Breakdown(benchmark::State& state) {
    const auto cfg = getBenchConfig();

    // Warmup: do one sequence to prime caches
    for (int i = 0; i < cfg.warmup; ++i) {
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/false, /*disableVector=*/false, /*disableKG=*/false);
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/false, /*disableVector=*/true, /*disableKG=*/true);
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/true, /*disableVector=*/false, /*disableKG=*/true);
        (void)timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                            /*disableKeyword=*/true, /*disableVector=*/true, /*disableKG=*/false);
    }

    double sumHybridMs = 0.0, sumKeywordMs = 0.0, sumVectorMs = 0.0, sumKgMs = 0.0;

    for (auto _ : state) {
        const double hybridMs  = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                               /*disableKeyword=*/false, /*disableVector=*/false, /*disableKG=*/false);
        const double keywordMs = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                               /*disableKeyword=*/false, /*disableVector=*/true, /*disableKG=*/true);
        const double vectorMs  = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                               /*disableKeyword=*/true, /*disableVector=*/false, /*disableKG=*/true);
        const double kgMs      = timeSearchCLI(cfg.cliPath, cfg.query, cfg.topK, cfg.verbose, cfg.extraArgs,
                                               /*disableKeyword=*/true, /*disableVector=*/true, /*disableKG=*/false);

        sumHybridMs  += hybridMs;
        sumKeywordMs += keywordMs;
        sumVectorMs  += vectorMs;
        sumKgMs      += kgMs;

        // The iteration's wall time will roughly include the sum; report hybrid timing here.
        state.SetIterationTime(hybridMs / 1000.0);
        benchmark::DoNotOptimize(hybridMs);
        benchmark::DoNotOptimize(keywordMs);
        benchmark::DoNotOptimize(vectorMs);
        benchmark::DoNotOptimize(kgMs);
    }

    const double iters = static_cast<double>(state.iterations());
    if (iters > 0.0) {
        state.counters["hybrid_ms_avg"]  = sumHybridMs  / iters;
        state.counters["keyword_ms_avg"] = sumKeywordMs / iters;
        state.counters["vector_ms_avg"]  = sumVectorMs  / iters;
        state.counters["kg_ms_avg"]      = sumKgMs      / iters;
    }
}
BENCHMARK(BM_Query_Breakdown)->Unit(benchmark::kMillisecond);

// Main is provided by Google Benchmark
// BENCHMARK_MAIN() is defined in benchmark library when linking with benchmark_main