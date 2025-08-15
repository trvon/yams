#include <spdlog/spdlog.h>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <numeric>
#include <random>
#include <yams/benchmarks/vector_benchmark.h>

#if __has_include(<format>)
#include <format>
#define YAMS_HAVE_STD_FORMAT 1
#else
#define YAMS_HAVE_STD_FORMAT 0
#endif

namespace yams::benchmarks {

// VectorBenchmark implementation
VectorBenchmark::VectorBenchmark(const std::string& name, const BenchmarkConfig& config)
    : name_(name), config_(config) {}

VectorBenchmark::~VectorBenchmark() = default;

BenchmarkResult VectorBenchmark::run() {
    spdlog::info("Running benchmark: {}", name_);

    setUp();
    auto result = runInternal();
    tearDown();

    return result;
}

BenchmarkResult VectorBenchmark::runInternal() {
    BenchmarkResult result;
    result.name = name_;
    result.timestamp = std::chrono::system_clock::now();

    // Run warmup
    if (config_.warmup_iterations > 0) {
        runWarmup();
    }

    // Determine number of iterations
    size_t iterations = config_.min_iterations;

    // Collect timings
    auto timings = collectTimings(iterations);

    // Calculate statistics
    calculateStatistics(result, timings);

    // Collect custom metrics
    result.custom_metrics = collectMetrics();

    // Calculate throughput
    if (result.mean_time_us > 0) {
        result.ops_per_second = 1000000.0 / result.mean_time_us;
    }

    result.iterations = iterations;
    result.memory_used_bytes = getCurrentMemoryUsage();

    return result;
}

void VectorBenchmark::runWarmup() {
    for (size_t i = 0; i < config_.warmup_iterations; ++i) {
        runIteration();
    }
}

std::vector<double> VectorBenchmark::collectTimings(size_t iterations) {
    std::vector<double> timings;
    timings.reserve(iterations);

    for (size_t i = 0; i < iterations; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        runIteration();
        auto end = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        timings.push_back(static_cast<double>(duration.count()));
    }

    return timings;
}

void VectorBenchmark::calculateStatistics(BenchmarkResult& result,
                                          const std::vector<double>& timings) {
    if (timings.empty())
        return;

    // Sort timings for percentile calculations
    std::vector<double> sorted_timings = timings;
    std::sort(sorted_timings.begin(), sorted_timings.end());

    // Min and max
    result.min_time_us = sorted_timings.front();
    result.max_time_us = sorted_timings.back();

    // Mean
    double sum = std::accumulate(sorted_timings.begin(), sorted_timings.end(), 0.0);
    result.mean_time_us = sum / sorted_timings.size();

    // Median
    size_t mid = sorted_timings.size() / 2;
    if (sorted_timings.size() % 2 == 0) {
        result.median_time_us = (sorted_timings[mid - 1] + sorted_timings[mid]) / 2.0;
    } else {
        result.median_time_us = sorted_timings[mid];
    }

    // Standard deviation
    double sq_sum = 0.0;
    for (double t : sorted_timings) {
        sq_sum += (t - result.mean_time_us) * (t - result.mean_time_us);
    }
    result.stddev_time_us = std::sqrt(sq_sum / sorted_timings.size());

    // Percentiles
    size_t p95_idx = static_cast<size_t>(sorted_timings.size() * 0.95);
    size_t p99_idx = static_cast<size_t>(sorted_timings.size() * 0.99);

    result.p95_time_us = sorted_timings[std::min(p95_idx, sorted_timings.size() - 1)];
    result.p99_time_us = sorted_timings[std::min(p99_idx, sorted_timings.size() - 1)];
}

size_t VectorBenchmark::getCurrentMemoryUsage() const {
    // Simplified - would use platform-specific APIs in real implementation
    return 0;
}

// BenchmarkSuite implementation
BenchmarkSuite::BenchmarkSuite(const std::string& name) : name_(name) {}

void BenchmarkSuite::addBenchmark(std::shared_ptr<VectorBenchmark> benchmark) {
    benchmarks_.push_back(benchmark);
}

std::vector<BenchmarkResult> BenchmarkSuite::runAll() {
    std::vector<BenchmarkResult> results;

    spdlog::info("Running benchmark suite: {}", name_);

    for (auto& benchmark : benchmarks_) {
        auto result = benchmark->run();
        results.push_back(result);
    }

    return results;
}

BenchmarkResult BenchmarkSuite::run(const std::string& benchmark_name) {
    for (auto& benchmark : benchmarks_) {
        if (benchmark->getName() == benchmark_name) {
            return benchmark->run();
        }
    }

    throw std::runtime_error("Benchmark not found: " + benchmark_name);
}

void BenchmarkSuite::generateReport(const std::vector<BenchmarkResult>& results,
                                    const std::string& output_path) {
    std::ofstream file(output_path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open output file: " + output_path);
    }

    file << "Benchmark Report: " << name_ << "\n";
    file << "=====================================\n\n";

    for (const auto& result : results) {
        file << "Benchmark: " << result.name << "\n";
        file << "  Iterations: " << result.iterations << "\n";
        file << "  Mean Time: " << result.mean_time_us << " μs\n";
        file << "  Median Time: " << result.median_time_us << " μs\n";
        file << "  Min Time: " << result.min_time_us << " μs\n";
        file << "  Max Time: " << result.max_time_us << " μs\n";
        file << "  Std Dev: " << result.stddev_time_us << " μs\n";
        file << "  P95: " << result.p95_time_us << " μs\n";
        file << "  P99: " << result.p99_time_us << " μs\n";
        file << "  Throughput: " << result.ops_per_second << " ops/s\n";
        file << "  Memory Used: " << result.memory_used_bytes << " bytes\n";
        file << "\n";
    }
}

void BenchmarkSuite::compareResults(const std::vector<BenchmarkResult>& baseline,
                                    const std::vector<BenchmarkResult>& current) {
    spdlog::info("Comparing benchmark results:");

    for (size_t i = 0; i < std::min(baseline.size(), current.size()); ++i) {
        const auto& base = baseline[i];
        const auto& curr = current[i];

        if (base.name != curr.name)
            continue;

        double speedup = base.mean_time_us / curr.mean_time_us;
        double memory_change = static_cast<double>(curr.memory_used_bytes) / base.memory_used_bytes;

        spdlog::info("  {}: Speedup: {:.2f}x, Memory: {:.2f}x", base.name, speedup, memory_change);
    }
}

// AccuracyBenchmark implementation
AccuracyBenchmark::AccuracyBenchmark(const std::string& name) : name_(name) {}
AccuracyBenchmark::~AccuracyBenchmark() = default;

double AccuracyBenchmark::calculateRecallAtK(const std::vector<std::vector<std::string>>& retrieved,
                                             const std::vector<std::vector<std::string>>& relevant,
                                             size_t k) {
    if (retrieved.size() != relevant.size()) {
        throw std::invalid_argument("Mismatched sizes for retrieved and relevant");
    }

    double total_recall = 0.0;

    for (size_t i = 0; i < retrieved.size(); ++i) {
        size_t retrieved_k = std::min(k, retrieved[i].size());
        size_t relevant_found = 0;

        for (size_t j = 0; j < retrieved_k; ++j) {
            if (std::find(relevant[i].begin(), relevant[i].end(), retrieved[i][j]) !=
                relevant[i].end()) {
                relevant_found++;
            }
        }

        if (!relevant[i].empty()) {
            total_recall += static_cast<double>(relevant_found) / relevant[i].size();
        }
    }

    return total_recall / retrieved.size();
}

double
AccuracyBenchmark::calculatePrecisionAtK(const std::vector<std::vector<std::string>>& retrieved,
                                         const std::vector<std::vector<std::string>>& relevant,
                                         size_t k) {
    if (retrieved.size() != relevant.size()) {
        throw std::invalid_argument("Mismatched sizes for retrieved and relevant");
    }

    double total_precision = 0.0;

    for (size_t i = 0; i < retrieved.size(); ++i) {
        size_t retrieved_k = std::min(k, retrieved[i].size());
        size_t relevant_found = 0;

        for (size_t j = 0; j < retrieved_k; ++j) {
            if (std::find(relevant[i].begin(), relevant[i].end(), retrieved[i][j]) !=
                relevant[i].end()) {
                relevant_found++;
            }
        }

        if (retrieved_k > 0) {
            total_precision += static_cast<double>(relevant_found) / retrieved_k;
        }
    }

    return total_precision / retrieved.size();
}

double AccuracyBenchmark::calculateNDCG(
    const std::vector<std::vector<std::pair<std::string, double>>>& retrieved,
    const std::vector<std::vector<std::pair<std::string, double>>>& relevant, size_t k) {
    // Simplified NDCG calculation
    double total_ndcg = 0.0;

    for (size_t i = 0; i < retrieved.size(); ++i) {
        double dcg = 0.0;
        double idcg = 0.0;

        // Calculate DCG
        size_t retrieved_k = std::min(k, retrieved[i].size());
        for (size_t j = 0; j < retrieved_k; ++j) {
            double relevance = 0.0;
            for (const auto& [id, score] : relevant[i]) {
                if (id == retrieved[i][j].first) {
                    relevance = score;
                    break;
                }
            }
            dcg += relevance / std::log2(j + 2);
        }

        // Calculate IDCG
        std::vector<double> ideal_scores;
        for (const auto& [id, score] : relevant[i]) {
            ideal_scores.push_back(score);
        }
        std::sort(ideal_scores.rbegin(), ideal_scores.rend());

        size_t ideal_k = std::min(k, ideal_scores.size());
        for (size_t j = 0; j < ideal_k; ++j) {
            idcg += ideal_scores[j] / std::log2(j + 2);
        }

        if (idcg > 0) {
            total_ndcg += dcg / idcg;
        }
    }

    return total_ndcg / retrieved.size();
}

// Utility functions
namespace utils {

std::vector<std::vector<float>> generateRandomVectors(size_t count, size_t dimension,
                                                      bool normalize) {
    std::vector<std::vector<float>> vectors;
    vectors.reserve(count);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

    for (size_t i = 0; i < count; ++i) {
        std::vector<float> vec(dimension);

        for (size_t j = 0; j < dimension; ++j) {
            vec[j] = dist(gen);
        }

        if (normalize) {
            float norm = 0.0f;
            for (float val : vec) {
                norm += val * val;
            }
            norm = std::sqrt(norm);

            if (norm > 0) {
                for (float& val : vec) {
                    val /= norm;
                }
            }
        }

        vectors.push_back(vec);
    }

    return vectors;
}

std::vector<std::string> generateSyntheticTexts(size_t count, size_t avg_length) {
    std::vector<std::string> texts;
    texts.reserve(count);

    const std::vector<std::string> words = {
        "the",         "quick",     "brown",    "fox",      "jumps",     "over",   "lazy",
        "dog",         "machine",   "learning", "vector",   "embedding", "search", "database",
        "performance", "benchmark", "test",     "accuracy", "precision", "recall"};

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> word_dist(0, words.size() - 1);
    std::uniform_int_distribution<size_t> length_dist(avg_length / 2, avg_length * 3 / 2);

    for (size_t i = 0; i < count; ++i) {
        std::string text;
        size_t target_length = length_dist(gen);

        while (text.length() < target_length) {
            if (!text.empty()) {
                text += " ";
            }
            text += words[word_dist(gen)];
        }

        texts.push_back(text);
    }

    return texts;
}

void saveResults(const std::vector<BenchmarkResult>& results, const std::string& path) {
    std::ofstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }

    // Simple CSV format
    file << "Name,Iterations,Mean(us),Median(us),Min(us),Max(us),StdDev(us),"
         << "P95(us),P99(us),Throughput(ops/s),Memory(bytes)\n";

    for (const auto& result : results) {
        file << result.name << "," << result.iterations << "," << result.mean_time_us << ","
             << result.median_time_us << "," << result.min_time_us << "," << result.max_time_us
             << "," << result.stddev_time_us << "," << result.p95_time_us << ","
             << result.p99_time_us << "," << result.ops_per_second << ","
             << result.memory_used_bytes << "\n";
    }
}

std::string formatTime(double microseconds) {
#if YAMS_HAVE_STD_FORMAT
    if (microseconds < 1000) {
        return std::format("{:.2f} μs", microseconds);
    } else if (microseconds < 1000000) {
        return std::format("{:.2f} ms", microseconds / 1000);
    } else {
        return std::format("{:.2f} s", microseconds / 1000000);
    }
#else
    auto to_fixed = [](double v, const char* unit) {
        std::string s = std::to_string(v);
        auto dot = s.find('.');
        if (dot != std::string::npos && dot + 3 < s.size()) {
            s = s.substr(0, dot + 3);
        }
        return s + " " + unit;
    };
    if (microseconds < 1000) {
        return to_fixed(microseconds, "μs");
    } else if (microseconds < 1000000) {
        return to_fixed(microseconds / 1000.0, "ms");
    } else {
        return to_fixed(microseconds / 1000000.0, "s");
    }
#endif
}

std::string formatMemory(size_t bytes) {
#if YAMS_HAVE_STD_FORMAT
    if (bytes < 1024) {
        return std::format("{} B", bytes);
    } else if (bytes < 1024 * 1024) {
        return std::format("{:.2f} KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
        return std::format("{:.2f} MB", bytes / (1024.0 * 1024));
    } else {
        return std::format("{:.2f} GB", bytes / (1024.0 * 1024 * 1024));
    }
#else
    auto to_fixed = [](double v, const char* unit) {
        std::string s = std::to_string(v);
        auto dot = s.find('.');
        if (dot != std::string::npos && dot + 3 < s.size()) {
            s = s.substr(0, dot + 3);
        }
        return s + " " + unit;
    };
    if (bytes < 1024) {
        return std::to_string(bytes) + " B";
    } else if (bytes < 1024 * 1024) {
        return to_fixed(static_cast<double>(bytes) / 1024.0, " KB");
    } else if (bytes < 1024 * 1024 * 1024) {
        return to_fixed(static_cast<double>(bytes) / (1024.0 * 1024), " MB");
    } else {
        return to_fixed(static_cast<double>(bytes) / (1024.0 * 1024 * 1024), " GB");
    }
#endif
}

void printResultsTable(const std::vector<BenchmarkResult>& results) {
    // Print header
    spdlog::info("{:<30} {:>12} {:>12} {:>12} {:>12} {:>15}", "Benchmark", "Mean", "Median", "P95",
                 "P99", "Throughput");
    spdlog::info("{:-<95}", "");

    // Print results
    for (const auto& result : results) {
        spdlog::info("{:<30} {:>12} {:>12} {:>12} {:>12} {:>15.2f} ops/s", result.name,
                     formatTime(result.mean_time_us), formatTime(result.median_time_us),
                     formatTime(result.p95_time_us), formatTime(result.p99_time_us),
                     result.ops_per_second);
    }
}

} // namespace utils

// MemoryProfiler implementation
class MemoryProfiler::Impl {
public:
    void start() {
        profiling_ = true;
        start_memory_ = getCurrentMemoryUsage();
        peak_memory_ = start_memory_;
    }

    void stop() { profiling_ = false; }

    size_t getCurrentUsage() const { return getCurrentMemoryUsage(); }

    size_t getPeakUsage() const { return peak_memory_; }

private:
    size_t getCurrentMemoryUsage() const {
        // Platform-specific implementation would go here
        // For now, return a mock value
        return 100 * 1024 * 1024; // 100MB
    }

    std::atomic<bool> profiling_{false};
    size_t start_memory_ = 0;
    size_t peak_memory_ = 0;
};

MemoryProfiler::MemoryProfiler() : pImpl(std::make_unique<Impl>()) {}
MemoryProfiler::~MemoryProfiler() = default;

void MemoryProfiler::start() {
    pImpl->start();
}
void MemoryProfiler::stop() {
    pImpl->stop();
}
size_t MemoryProfiler::getCurrentUsage() const {
    return pImpl->getCurrentUsage();
}
size_t MemoryProfiler::getPeakUsage() const {
    return pImpl->getPeakUsage();
}

} // namespace yams::benchmarks