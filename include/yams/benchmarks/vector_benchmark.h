#pragma once

#include <yams/core/types.h>
#include <string>
#include <vector>
#include <chrono>
#include <map>
#include <functional>
#include <memory>
#include <atomic>

namespace yams::benchmarks {

/**
 * Benchmark result for a single operation
 */
struct BenchmarkResult {
    std::string name;
    size_t iterations = 0;
    size_t operations_per_iteration = 1;
    
    // Timing statistics (in microseconds)
    double min_time_us = 0.0;
    double max_time_us = 0.0;
    double mean_time_us = 0.0;
    double median_time_us = 0.0;
    double stddev_time_us = 0.0;
    double p95_time_us = 0.0;
    double p99_time_us = 0.0;
    
    // Throughput metrics
    double ops_per_second = 0.0;
    double mb_per_second = 0.0;
    
    // Resource usage
    size_t memory_used_bytes = 0;
    size_t peak_memory_bytes = 0;
    double cpu_usage_percent = 0.0;
    
    // Additional metrics
    std::map<std::string, double> custom_metrics;
    
    // Metadata
    std::string description;
    std::chrono::system_clock::time_point timestamp;
    std::map<std::string, std::string> tags;
    
    // Comparison
    bool operator<(const BenchmarkResult& other) const {
        return mean_time_us < other.mean_time_us;
    }
};

/**
 * Configuration for benchmarks
 */
struct BenchmarkConfig {
    size_t warmup_iterations = 10;
    size_t min_iterations = 100;
    size_t max_iterations = 10000;
    std::chrono::seconds max_duration{30};
    std::chrono::milliseconds target_time_per_iteration{100};
    bool enable_profiling = false;
    bool verbose = false;
    size_t num_threads = 1;
};

/**
 * Base class for vector benchmarks
 */
class VectorBenchmark {
public:
    explicit VectorBenchmark(const std::string& name, const BenchmarkConfig& config = {});
    virtual ~VectorBenchmark();
    
    // Run the benchmark
    virtual BenchmarkResult run();
    
    // Get benchmark name
    const std::string& getName() const { return name_; }
    
    // Set configuration
    void setConfig(const BenchmarkConfig& config) { config_ = config; }
    
protected:
    // Setup before benchmark (not timed)
    virtual void setUp() {}
    
    // Teardown after benchmark (not timed)
    virtual void tearDown() {}
    
    // The actual operation to benchmark
    virtual void runIteration() = 0;
    
    // Optional: custom metric collection
    virtual std::map<std::string, double> collectMetrics() { return {}; }
    
    // Helper to measure memory usage
    size_t getCurrentMemoryUsage() const;
    
protected:
    std::string name_;
    BenchmarkConfig config_;
    
private:
    BenchmarkResult runInternal();
    void runWarmup();
    std::vector<double> collectTimings(size_t iterations);
    void calculateStatistics(BenchmarkResult& result, const std::vector<double>& timings);
};

/**
 * Benchmark suite for running multiple benchmarks
 */
class BenchmarkSuite {
public:
    explicit BenchmarkSuite(const std::string& name);
    
    // Add benchmark to suite
    void addBenchmark(std::shared_ptr<VectorBenchmark> benchmark);
    
    // Run all benchmarks
    std::vector<BenchmarkResult> runAll();
    
    // Run specific benchmark
    BenchmarkResult run(const std::string& benchmark_name);
    
    // Generate report
    void generateReport(const std::vector<BenchmarkResult>& results, 
                       const std::string& output_path);
    
    // Compare results
    void compareResults(const std::vector<BenchmarkResult>& baseline,
                       const std::vector<BenchmarkResult>& current);
    
private:
    std::string name_;
    std::vector<std::shared_ptr<VectorBenchmark>> benchmarks_;
};

/**
 * Accuracy metrics for vector search
 */
struct AccuracyMetrics {
    double recall_at_1 = 0.0;
    double recall_at_5 = 0.0;
    double recall_at_10 = 0.0;
    double recall_at_100 = 0.0;
    
    double precision_at_1 = 0.0;
    double precision_at_5 = 0.0;
    double precision_at_10 = 0.0;
    
    double mean_average_precision = 0.0;
    double ndcg = 0.0;  // Normalized Discounted Cumulative Gain
    
    double f1_score = 0.0;
    double mean_reciprocal_rank = 0.0;
};

/**
 * Base class for accuracy benchmarks
 */
class AccuracyBenchmark {
public:
    explicit AccuracyBenchmark(const std::string& name);
    virtual ~AccuracyBenchmark();
    
    // Run accuracy evaluation
    virtual AccuracyMetrics evaluate() = 0;
    
    // Load ground truth data
    virtual void loadGroundTruth(const std::string& path) = 0;
    
protected:
    // Calculate recall@k
    double calculateRecallAtK(
        const std::vector<std::vector<std::string>>& retrieved,
        const std::vector<std::vector<std::string>>& relevant,
        size_t k
    );
    
    // Calculate precision@k
    double calculatePrecisionAtK(
        const std::vector<std::vector<std::string>>& retrieved,
        const std::vector<std::vector<std::string>>& relevant,
        size_t k
    );
    
    // Calculate NDCG
    double calculateNDCG(
        const std::vector<std::vector<std::pair<std::string, double>>>& retrieved,
        const std::vector<std::vector<std::pair<std::string, double>>>& relevant,
        size_t k
    );
    
protected:
    std::string name_;
};

/**
 * Scalability test configuration
 */
struct ScalabilityConfig {
    std::vector<size_t> dataset_sizes = {1000, 10000, 100000, 1000000};
    std::vector<size_t> query_batch_sizes = {1, 10, 100, 1000};
    std::vector<size_t> concurrent_users = {1, 5, 10, 20, 50};
    bool measure_memory_scaling = true;
    bool measure_index_size = true;
};

/**
 * Scalability benchmark results
 */
struct ScalabilityResult {
    size_t dataset_size = 0;
    size_t query_batch_size = 0;
    size_t concurrent_users = 0;
    
    double indexing_time_ms = 0.0;
    double query_time_ms = 0.0;
    double throughput_qps = 0.0;  // Queries per second
    
    size_t index_size_bytes = 0;
    size_t memory_usage_bytes = 0;
    
    double latency_p50_ms = 0.0;
    double latency_p95_ms = 0.0;
    double latency_p99_ms = 0.0;
};

/**
 * Stress test configuration
 */
struct StressTestConfig {
    size_t max_concurrent_operations = 100;
    std::chrono::seconds test_duration{60};
    size_t target_qps = 1000;  // Target queries per second
    bool enable_memory_pressure = false;
    size_t memory_limit_bytes = 0;
    bool enable_chaos = false;  // Random failures
    double failure_rate = 0.01;  // 1% failure rate
};

/**
 * Stress test results
 */
struct StressTestResult {
    size_t total_operations = 0;
    size_t successful_operations = 0;
    size_t failed_operations = 0;
    
    double achieved_qps = 0.0;
    double success_rate = 0.0;
    
    double min_latency_ms = 0.0;
    double max_latency_ms = 0.0;
    double mean_latency_ms = 0.0;
    double p99_latency_ms = 0.0;
    
    size_t peak_memory_bytes = 0;
    size_t errors_recovered = 0;
    
    std::map<std::string, size_t> error_counts;
    std::vector<std::pair<std::chrono::system_clock::time_point, double>> qps_timeline;
};

/**
 * Benchmark utilities
 */
namespace utils {
    // Generate random vectors for testing
    std::vector<std::vector<float>> generateRandomVectors(
        size_t count, 
        size_t dimension,
        bool normalize = true
    );
    
    // Generate synthetic text data
    std::vector<std::string> generateSyntheticTexts(
        size_t count,
        size_t avg_length = 100
    );
    
    // Load vectors from file
    std::vector<std::vector<float>> loadVectors(const std::string& path);
    
    // Save benchmark results to JSON
    void saveResults(const std::vector<BenchmarkResult>& results, 
                    const std::string& path);
    
    // Load benchmark results from JSON
    std::vector<BenchmarkResult> loadResults(const std::string& path);
    
    // Calculate speedup between results
    double calculateSpeedup(const BenchmarkResult& baseline, 
                           const BenchmarkResult& current);
    
    // Format time for display
    std::string formatTime(double microseconds);
    
    // Format memory for display
    std::string formatMemory(size_t bytes);
    
    // Print results table
    void printResultsTable(const std::vector<BenchmarkResult>& results);
    
    // Generate performance report
    void generatePerformanceReport(
        const std::vector<BenchmarkResult>& results,
        const std::string& output_path
    );
}

/**
 * Memory profiler for benchmarks
 */
class MemoryProfiler {
public:
    MemoryProfiler();
    ~MemoryProfiler();
    
    void start();
    void stop();
    
    size_t getCurrentUsage() const;
    size_t getPeakUsage() const;
    
    struct AllocationInfo {
        size_t size;
        std::string location;
        std::chrono::system_clock::time_point timestamp;
    };
    
    std::vector<AllocationInfo> getAllocations() const;
    
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::benchmarks