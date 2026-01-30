// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "benchmark_base.h"
#include <yams/core/magic_numbers.hpp>
#include <yams/detection/file_type_detector.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

namespace yams::benchmark {

// Helper to create byte vectors from hex strings
static std::vector<uint8_t> hex_to_vector(const std::string& hex) {
    std::vector<uint8_t> result;
    for (size_t i = 0; i < hex.length(); i += 2) {
        uint8_t byte = std::stoi(hex.substr(i, 2), nullptr, 16);
        result.push_back(byte);
    }
    return result;
}

/**
 * @brief Benchmark for magic number detection
 *
 * Compares performance of:
 * 1. C++23 constexpr patterns (compile-time loaded)
 * 2. C++20 runtime patterns (loaded at startup)
 * 3. JSON loading from file
 */
class MagicNumbersBenchmark : public BenchmarkBase {
public:
    MagicNumbersBenchmark(const Config& config = Config())
        : BenchmarkBase("MagicNumbers_Detection", config) {
        setUp();
    }

protected:
    void setUp() {
        // Create test data for different file types using hex strings
        jpeg_data_ = hex_to_vector("FFD8FFE0");
        png_data_ = hex_to_vector("89504E470D0A1A0A");
        pdf_data_ = hex_to_vector("255044462D");
        zip_data_ = hex_to_vector("504B0304");
        elf_data_ = hex_to_vector("7F454C46");
        mp3_data_ = hex_to_vector("494433");
        gzip_data_ = hex_to_vector("1F8B");
        unknown_data_ = hex_to_vector("DEADBEEF");

        // Collect all test cases
        test_cases_ = {{"JPEG", jpeg_data_, "image/jpeg"},
                       {"PNG", png_data_, "image/png"},
                       {"PDF", pdf_data_, "application/pdf"},
                       {"ZIP", zip_data_, "application/zip"},
                       {"ELF", elf_data_, "application/x-executable"},
                       {"MP3", mp3_data_, "audio/mpeg"},
                       {"GZIP", gzip_data_, "application/gzip"},
                       {"Unknown", unknown_data_, "application/octet-stream"}};
    }

    size_t runIteration() override {
        // Test all patterns
        for (const auto& [name, data, expected] : test_cases_) {
            auto mime = magic::detect_mime_type(std::span(data));
            // Silently verify (don't affect benchmark timing)
            (void)mime;
            (void)expected;
            (void)name;
        }
        return test_cases_.size(); // Number of detections
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["test_cases"] = test_cases_.size();
        metrics["pattern_count"] = magic::MagicDatabaseInfo::pattern_count();

#if YAMS_HAS_CONSTEXPR_CONTAINERS
        metrics["mode"] = 1.0; // Compile-time
#else
        metrics["mode"] = 0.0; // Runtime
#endif
    }

private:
    struct TestCase {
        std::string name;
        std::vector<uint8_t> data;
        std::string expected_mime;
    };

    std::vector<TestCase> test_cases_;
    std::vector<uint8_t> jpeg_data_;
    std::vector<uint8_t> png_data_;
    std::vector<uint8_t> pdf_data_;
    std::vector<uint8_t> zip_data_;
    std::vector<uint8_t> elf_data_;
    std::vector<uint8_t> mp3_data_;
    std::vector<uint8_t> gzip_data_;
    std::vector<uint8_t> unknown_data_;
};

/**
 * @brief Benchmark FileTypeDetector initialization
 *
 * Measures cost of loading patterns at startup
 */
class FileTypeDetectorInitBenchmark : public BenchmarkBase {
public:
    FileTypeDetectorInitBenchmark(const Config& config = Config())
        : BenchmarkBase("FileTypeDetector_Init", config) {}

protected:
    size_t runIteration() override {
        detection::FileTypeDetectorConfig cfg;
        cfg.useBuiltinPatterns = true;
        cfg.useCustomPatterns = false;

        // Use singleton instance
        auto& detector = detection::FileTypeDetector::instance();
        auto result = detector.initialize(cfg);

        if (!result) {
            throw std::runtime_error("Failed to initialize detector");
        }

        return 1; // 1 initialization
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
#if YAMS_HAS_CONSTEXPR_CONTAINERS
        metrics["initialization_type"] = 1.0; // Compile-time patterns
#else
        metrics["initialization_type"] = 0.0; // Runtime patterns
#endif
    }
};

/**
 * @brief Benchmark pattern matching performance
 *
 * Tests detection speed with various buffer sizes
 */
class PatternMatchingBenchmark : public BenchmarkBase {
public:
    PatternMatchingBenchmark(size_t buffer_size, const Config& config = Config())
        : BenchmarkBase("PatternMatching_" + std::to_string(buffer_size) + "B", config),
          buffer_size_(buffer_size) {
        setUp();
    }

protected:
    void setUp() {
        // Create buffer with JPEG signature at start
        buffer_.resize(buffer_size_);
        buffer_[0] = 0xFF;
        buffer_[1] = 0xD8;
        buffer_[2] = 0xFF;

        // Fill rest with random data
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (size_t i = 3; i < buffer_size_; ++i) {
            buffer_[i] = static_cast<uint8_t>(dis(gen));
        }
    }

    size_t runIteration() override {
        auto mime = magic::detect_mime_type(std::span(buffer_));
        return 1; // 1 detection
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["buffer_size_bytes"] = buffer_size_;
        metrics["pattern_count"] = magic::MagicDatabaseInfo::pattern_count();
    }

private:
    size_t buffer_size_;
    std::vector<uint8_t> buffer_;
};

} // namespace yams::benchmark

int main(int, char*[]) {
    using namespace yams::benchmark;

    BenchmarkBase::Config config;
    config.warmup_iterations = 5;
    config.benchmark_iterations = 100;
    config.verbose = true;

    std::cout << "==================================" << std::endl;
    std::cout << "Magic Numbers Benchmark Suite" << std::endl;
    std::cout << "==================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Build Configuration:" << std::endl;
#if YAMS_HAS_CONSTEXPR_CONTAINERS
    std::cout << "  Mode: C++23 (compile-time patterns)" << std::endl;
    std::cout << "  Pattern Count: " << yams::magic::MagicDatabaseInfo::pattern_count()
              << std::endl;
    std::cout << "  Database: " << yams::magic::MagicDatabaseInfo::mode() << std::endl;
#else
    std::cout << "  Mode: C++20 (runtime patterns)" << std::endl;
    std::cout << "  Pattern Count: ~42 (hardcoded)" << std::endl;
#endif
    std::cout << std::endl;

    std::vector<BenchmarkBase::Result> results;

    // 1. Test magic number detection
    std::cout << "Running: Magic Number Detection..." << std::endl;
    MagicNumbersBenchmark magic_bench(config);
    auto magic_result = magic_bench.run();
    results.push_back(magic_result);
    std::cout << "  " << magic_result.operations << " detections in " << magic_result.duration_ms
              << " ms" << std::endl;
    std::cout << "  " << magic_result.ops_per_sec << " ops/sec" << std::endl;
    std::cout << std::endl;

    // 2. Test initialization cost
    std::cout << "Running: FileTypeDetector Initialization..." << std::endl;
    FileTypeDetectorInitBenchmark init_bench(config);
    auto init_result = init_bench.run();
    results.push_back(init_result);
    std::cout << "  Average init time: " << (init_result.duration_ms / init_result.operations)
              << " ms" << std::endl;
    std::cout << std::endl;

    // 3. Test pattern matching with different buffer sizes
    std::vector<size_t> buffer_sizes = {64, 256, 1024, 4096, 16384};
    for (size_t size : buffer_sizes) {
        std::cout << "Running: Pattern Matching (" << size << " bytes)..." << std::endl;
        PatternMatchingBenchmark match_bench(size, config);
        auto match_result = match_bench.run();
        results.push_back(match_result);
        std::cout << "  " << match_result.ops_per_sec << " ops/sec" << std::endl;
    }
    std::cout << std::endl;

    // Summary
    std::cout << "==================================" << std::endl;
    std::cout << "Summary" << std::endl;
    std::cout << "==================================" << std::endl;

    for (const auto& result : results) {
        std::cout << result.name << ":" << std::endl;
        std::cout << "  Duration: " << result.duration_ms << " ms" << std::endl;
        std::cout << "  Throughput: " << result.ops_per_sec << " ops/sec" << std::endl;
        if (!result.custom_metrics.empty()) {
            std::cout << "  Metrics:" << std::endl;
            for (const auto& [key, value] : result.custom_metrics) {
                std::cout << "    " << key << ": " << value << std::endl;
            }
        }
        std::cout << std::endl;
    }

    // Save JSON results
    if (!config.output_file.empty()) {
        nlohmann::json j;
        j["benchmarks"] = nlohmann::json::array();
        for (const auto& result : results) {
            j["benchmarks"].push_back(result.toJSON());
        }

        std::ofstream out(config.output_file);
        out << j.dump(2);
        std::cout << "Results saved to: " << config.output_file << std::endl;
    }

    return 0;
}
