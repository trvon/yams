#pragma once

#include <nlohmann/json.hpp>
#include <chrono>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "../common/benchmark_tracker.h"

namespace yams::benchmark {

class BenchmarkBase {
public:
    struct Result {
        std::string name;
        double duration_ms;
        size_t operations;
        double ops_per_sec;
        size_t memory_used_bytes;
        std::map<std::string, double> custom_metrics;

        nlohmann::json toJSON() const {
            nlohmann::json j;
            j["name"] = name;
            j["duration_ms"] = duration_ms;
            j["operations"] = operations;
            j["ops_per_sec"] = ops_per_sec;
            j["memory_used_bytes"] = memory_used_bytes;
            j["metrics"] = custom_metrics;
            return j;
        }
    };

    struct Config {
        size_t warmup_iterations;
        size_t benchmark_iterations;
        bool verbose;
        bool track_memory;
        std::string output_file;

        Config()
            : warmup_iterations(3), benchmark_iterations(10), verbose(false), track_memory(true) {}
    };

    BenchmarkBase(const std::string& name, const Config& config = Config())
        : name_(name), config_(config) {}

    virtual ~BenchmarkBase() = default;

    // Run the benchmark
    Result run() {
        if (config_.verbose) {
            std::cout << "Running benchmark: " << name_ << std::endl;
        }

        // Warmup
        if (config_.warmup_iterations > 0) {
            if (config_.verbose) {
                std::cout << "  Warming up (" << config_.warmup_iterations << " iterations)..."
                          << std::endl;
            }
            for (size_t i = 0; i < config_.warmup_iterations; ++i) {
                runIteration();
            }
        }

        // Actual benchmark
        if (config_.verbose) {
            std::cout << "  Benchmarking (" << config_.benchmark_iterations << " iterations)..."
                      << std::endl;
        }

        std::vector<double> durations;
        size_t totalOperations = 0;
        size_t memoryBefore = 0;
        size_t memoryAfter = 0;

        if (config_.track_memory) {
            memoryBefore = getCurrentMemoryUsage();
        }

        for (size_t i = 0; i < config_.benchmark_iterations; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            size_t ops = runIteration();
            auto end = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            durations.push_back(duration.count() / 1000.0); // Convert to ms
            totalOperations += ops;
        }

        if (config_.track_memory) {
            memoryAfter = getCurrentMemoryUsage();
        }

        // Calculate statistics
        double totalDuration = 0;
        for (double d : durations) {
            totalDuration += d;
        }
        double avgDuration = totalDuration / durations.size();

        Result result;
        result.name = name_;
        result.duration_ms = avgDuration;
        result.operations = totalOperations / config_.benchmark_iterations;
        result.ops_per_sec = (result.operations / avgDuration) * 1000.0;
        result.memory_used_bytes = memoryAfter - memoryBefore;

        // Add custom metrics
        collectCustomMetrics(result.custom_metrics);

        // Report results
        reportResult(result);

        // Save to file if configured
        if (!config_.output_file.empty()) {
            saveResult(result);
        }

        return result;
    }

    // Compare with baseline
    bool checkRegression(const Result& current, const Result& baseline, double threshold = 0.1) {
        double change = (current.duration_ms - baseline.duration_ms) / baseline.duration_ms;
        return change > threshold;
    }

protected:
    // Override this to implement the actual benchmark
    virtual size_t runIteration() = 0;

    // Override to collect custom metrics
    virtual void collectCustomMetrics([[maybe_unused]] std::map<std::string, double>& metrics) {
        // Default: no custom metrics
    }

    // Utility functions for derived classes
    template <typename Func> double timeOperation(Func&& func) {
        auto start = std::chrono::high_resolution_clock::now();
        func();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        return duration.count() / 1000.0; // Return in ms
    }

    size_t getCurrentMemoryUsage() {
        // Platform-specific memory measurement
        // This is a simplified version - real implementation would use platform APIs
        std::ifstream status("/proc/self/status");
        std::string line;
        while (std::getline(status, line)) {
            if (line.substr(0, 6) == "VmRSS:") {
                size_t rss;
                std::sscanf(line.c_str(), "VmRSS: %zu kB", &rss);
                return rss * 1024; // Convert to bytes
            }
        }
        return 0;
    }

private:
    std::string name_;
    Config config_;

    void reportResult(const Result& result) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Benchmark: " << result.name << std::endl;
        std::cout << "  Duration: " << result.duration_ms << " ms" << std::endl;
        std::cout << "  Operations: " << result.operations << std::endl;
        std::cout << "  Throughput: " << result.ops_per_sec << " ops/sec" << std::endl;

        if (config_.track_memory && result.memory_used_bytes > 0) {
            std::cout << "  Memory: " << (result.memory_used_bytes / 1024.0 / 1024.0) << " MB"
                      << std::endl;
        }

        if (!result.custom_metrics.empty()) {
            std::cout << "  Custom Metrics:" << std::endl;
            for (const auto& [key, value] : result.custom_metrics) {
                std::cout << "    " << key << ": " << value << std::endl;
            }
        }
    }

    void saveResult(const Result& result) {
        std::ofstream file(config_.output_file, std::ios::app);
        file << result.toJSON().dump() << std::endl;
    }
};

// Macro for easy benchmark definition
#define BENCHMARK_F(ClassName, BenchName)                                                          \
    class BenchName##Benchmark : public ClassName {                                                \
    public:                                                                                        \
        BenchName##Benchmark() : ClassName(#BenchName) {}                                          \
        explicit BenchName##Benchmark(const ::yams::benchmark::BenchmarkBase::Config& cfg)         \
            : ClassName(#BenchName, cfg) {}                                                        \
                                                                                                   \
    protected:                                                                                     \
        size_t runIteration() override;                                                            \
    };                                                                                             \
    size_t BenchName##Benchmark::runIteration()

} // namespace yams::benchmark
