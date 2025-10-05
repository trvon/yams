// Copyright 2025 YAMS Project
// SPDX-License-Identifier: Apache-2.0

/**
 * @file tree_diff_benchmarks.cpp
 * @brief Performance benchmarks for PBI-043 Tree-Diff implementation
 *
 * Validates acceptance criteria:
 * - AC #2: Diff latency < 750ms p95 for 10k-entry snapshots
 * - AC #7: Rename detection accuracy ≥ 99%
 * - Storage overhead ≤ 15%
 *
 * Metrics tracked:
 * - Diff computation latency (with/without rename detection)
 * - Rename detection accuracy (true positive rate)
 * - Storage overhead (tree objects vs flat snapshot)
 * - Subtree hash optimization effectiveness
 * - Large snapshot performance (10k, 50k, 100k entries)
 */

#include <filesystem>
#include <fstream>
#include <random>
#include "../benchmarks/benchmark_base.h"
#include <yams/metadata/tree_builder.h>
#include <yams/metadata/tree_differ.h>
#include <yams/storage/cas_storage_engine.h>

namespace fs = std::filesystem;
using namespace yams::metadata;
using namespace yams::benchmark;

namespace {

// Test data generator
class TestDataGenerator {
public:
    struct FileInfo {
        std::string path;
        std::string hash;
        uint64_t size;
        bool isDirectory;
    };

    static std::vector<FileInfo> generateFiles(size_t count, size_t avgDirDepth = 3) {
        std::vector<FileInfo> files;
        std::mt19937 rng(42); // Fixed seed for reproducibility
        std::uniform_int_distribution<size_t> depthDist(1, avgDirDepth);
        std::uniform_int_distribution<uint64_t> sizeDist(100, 1000000);

        for (size_t i = 0; i < count; ++i) {
            FileInfo info;

            // Generate path
            size_t depth = depthDist(rng);
            std::string path;
            for (size_t d = 0; d < depth; ++d) {
                if (d > 0)
                    path += "/";
                path += "dir" + std::to_string(d);
            }
            path += "/file" + std::to_string(i) + ".txt";

            info.path = path;
            info.hash = generateHash(i);
            info.size = sizeDist(rng);
            info.isDirectory = false;

            files.push_back(info);
        }

        return files;
    }

    static std::string generateHash(size_t seed) {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (int i = 0; i < 8; ++i) {
            ss << std::setw(8) << (seed + i);
        }
        return ss.str();
    }

    // Apply mutations to simulate changes
    static std::vector<FileInfo> applyMutations(const std::vector<FileInfo>& base, double addRate,
                                                double deleteRate, double modifyRate,
                                                double renameRate) {
        std::vector<FileInfo> result;
        std::mt19937 rng(123);
        std::uniform_real_distribution<double> dist(0.0, 1.0);

        // Process existing files
        for (const auto& file : base) {
            double roll = dist(rng);

            if (roll < deleteRate) {
                // Delete (skip)
                continue;
            } else if (roll < deleteRate + modifyRate) {
                // Modify (change hash)
                FileInfo modified = file;
                modified.hash = generateHash(std::hash<std::string>{}(file.hash) + 1);
                result.push_back(modified);
            } else if (roll < deleteRate + modifyRate + renameRate) {
                // Rename (same hash, different path)
                FileInfo renamed = file;
                renamed.path = file.path + ".renamed";
                result.push_back(renamed);
            } else {
                // Unchanged
                result.push_back(file);
            }
        }

        // Add new files
        size_t newFiles = static_cast<size_t>(base.size() * addRate);
        for (size_t i = 0; i < newFiles; ++i) {
            FileInfo newFile;
            newFile.path = "newfiles/file" + std::to_string(i) + ".txt";
            newFile.hash = generateHash(base.size() + i + 1000);
            newFile.size = 1000;
            newFile.isDirectory = false;
            result.push_back(newFile);
        }

        return result;
    }
};

// Build tree from file list
TreeNode buildTreeFromFiles(const std::vector<TestDataGenerator::FileInfo>& files) {
    TreeNode root;

    for (const auto& file : files) {
        TreeEntry entry;
        entry.name = fs::path(file.path).filename().string();
        entry.hash = file.hash;
        entry.mode = 0100644;
        entry.size = file.size;
        entry.isDirectory = file.isDirectory;

        root.addEntry(entry);
    }

    return root;
}

} // anonymous namespace

// Benchmark: Baseline diff performance
class BaselineDiffBenchmark : public BenchmarkBase {
public:
    BaselineDiffBenchmark(size_t fileCount, const Config& config)
        : BenchmarkBase("BaselineDiff_" + std::to_string(fileCount), config),
          fileCount_(fileCount) {
        // Generate test data once
        baseFiles_ = TestDataGenerator::generateFiles(fileCount_);
        targetFiles_ = TestDataGenerator::applyMutations(baseFiles_, 0.01, 0.01, 0.05,
                                                         0.0); // 1% add, 1% del, 5% mod

        baseTree_ = buildTreeFromFiles(baseFiles_);
        targetTree_ = buildTreeFromFiles(targetFiles_);
    }

protected:
    size_t runIteration() override {
        TreeDiffer differ;
        DiffOptions options;
        options.detectRenames = false; // Baseline without renames

        auto result = differ.computeDiff(baseTree_, targetTree_, options);
        if (!result) {
            return 0;
        }

        return result.value().changes.size();
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["file_count"] = fileCount_;
        metrics["change_rate"] = 0.07; // 7% change rate
    }

private:
    size_t fileCount_;
    std::vector<TestDataGenerator::FileInfo> baseFiles_;
    std::vector<TestDataGenerator::FileInfo> targetFiles_;
    TreeNode baseTree_;
    TreeNode targetTree_;
};

// Benchmark: Rename detection performance
class RenameDetectionBenchmark : public BenchmarkBase {
public:
    RenameDetectionBenchmark(size_t fileCount, double renameRate, const Config& config)
        : BenchmarkBase("RenameDetection_" + std::to_string(fileCount) + "_rate" +
                            std::to_string(int(renameRate * 100)),
                        config),
          fileCount_(fileCount), renameRate_(renameRate) {
        // Generate test data with renames
        baseFiles_ = TestDataGenerator::generateFiles(fileCount_);
        targetFiles_ = TestDataGenerator::applyMutations(baseFiles_, 0.0, 0.0, 0.0, renameRate_);

        baseTree_ = buildTreeFromFiles(baseFiles_);
        targetTree_ = buildTreeFromFiles(targetFiles_);
    }

protected:
    size_t runIteration() override {
        TreeDiffer differ;
        DiffOptions options;
        options.detectRenames = true;

        auto result = differ.computeDiff(baseTree_, targetTree_, options);
        if (!result) {
            return 0;
        }

        return result.value().changes.size();
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        // Measure rename detection accuracy
        TreeDiffer differ;
        DiffOptions options;
        options.detectRenames = true;

        auto result = differ.computeDiff(baseTree_, targetTree_, options);
        if (result) {
            size_t expectedRenames = static_cast<size_t>(fileCount_ * renameRate_);
            size_t detectedRenames = result.value().filesRenamed;

            double accuracy =
                expectedRenames > 0 ? static_cast<double>(detectedRenames) / expectedRenames : 1.0;

            metrics["rename_accuracy"] = accuracy * 100.0; // Percentage
            metrics["expected_renames"] = expectedRenames;
            metrics["detected_renames"] = detectedRenames;
            metrics["rename_rate"] = renameRate_ * 100.0;
        }
    }

private:
    size_t fileCount_;
    double renameRate_;
    std::vector<TestDataGenerator::FileInfo> baseFiles_;
    std::vector<TestDataGenerator::FileInfo> targetFiles_;
    TreeNode baseTree_;
    TreeNode targetTree_;
};

// Benchmark: Latency acceptance criteria (AC #2)
class LatencyAcceptanceBenchmark : public BenchmarkBase {
public:
    LatencyAcceptanceBenchmark(const Config& config)
        : BenchmarkBase("LatencyAcceptance_10k", config) {
        // Generate 10k-entry snapshot (AC #2 requirement)
        baseFiles_ = TestDataGenerator::generateFiles(10000);
        targetFiles_ =
            TestDataGenerator::applyMutations(baseFiles_, 0.02, 0.02, 0.05, 0.05); // Mixed workload

        baseTree_ = buildTreeFromFiles(baseFiles_);
        targetTree_ = buildTreeFromFiles(targetFiles_);
    }

protected:
    size_t runIteration() override {
        TreeDiffer differ;
        DiffOptions options;
        options.detectRenames = true;

        auto result = differ.computeDiff(baseTree_, targetTree_, options);
        if (!result) {
            return 0;
        }

        return result.value().changes.size();
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        // Run multiple iterations to compute p95 latency
        std::vector<double> latencies;
        TreeDiffer differ;
        DiffOptions options;
        options.detectRenames = true;

        for (int i = 0; i < 100; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            auto result = differ.computeDiff(baseTree_, targetTree_, options);
            auto end = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            latencies.push_back(duration.count());
        }

        // Compute p95
        std::sort(latencies.begin(), latencies.end());
        size_t p95_idx = static_cast<size_t>(latencies.size() * 0.95);
        double p95_latency = latencies[p95_idx];

        metrics["p95_latency_ms"] = p95_latency;
        metrics["p50_latency_ms"] = latencies[latencies.size() / 2];
        metrics["max_latency_ms"] = latencies.back();
        metrics["ac2_threshold_ms"] = 750.0;
        metrics["ac2_passed"] = (p95_latency < 750.0) ? 1.0 : 0.0;
    }

private:
    std::vector<TestDataGenerator::FileInfo> baseFiles_;
    std::vector<TestDataGenerator::FileInfo> targetFiles_;
    TreeNode baseTree_;
    TreeNode targetTree_;
};

// Benchmark: Storage overhead
class StorageOverheadBenchmark : public BenchmarkBase {
public:
    StorageOverheadBenchmark(size_t fileCount, const Config& config)
        : BenchmarkBase("StorageOverhead_" + std::to_string(fileCount), config),
          fileCount_(fileCount) {
        files_ = TestDataGenerator::generateFiles(fileCount_);
        tree_ = buildTreeFromFiles(files_);
    }

protected:
    size_t runIteration() override {
        // Serialize tree and measure size
        auto serialized = tree_.serialize();
        treeSize_ = serialized.size();

        // Estimate flat snapshot size
        // Flat snapshot: each file requires path + hash + metadata
        flatSize_ = 0;
        for (const auto& file : files_) {
            flatSize_ += file.path.size(); // Path
            flatSize_ += 64;               // SHA-256 hash (hex)
            flatSize_ += 16;               // Metadata (size, mode, etc.)
        }

        return files_.size();
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        double overhead =
            flatSize_ > 0 ? (static_cast<double>(treeSize_) / flatSize_ - 1.0) * 100.0 : 0.0;

        metrics["tree_size_bytes"] = treeSize_;
        metrics["flat_size_bytes"] = flatSize_;
        metrics["storage_overhead_pct"] = overhead;
        metrics["ac_threshold_pct"] = 15.0;
        metrics["ac_passed"] = (overhead <= 15.0) ? 1.0 : 0.0;
    }

private:
    size_t fileCount_;
    std::vector<TestDataGenerator::FileInfo> files_;
    TreeNode tree_;
    size_t treeSize_ = 0;
    size_t flatSize_ = 0;
};

// Benchmark: Subtree hash optimization
class SubtreeHashBenchmark : public BenchmarkBase {
public:
    SubtreeHashBenchmark(const Config& config) : BenchmarkBase("SubtreeHashOptimization", config) {
        // Generate large tree with deep hierarchy
        files_ = TestDataGenerator::generateFiles(5000, 10);
        baseTree_ = buildTreeFromFiles(files_);

        // Target: change only 1% of files
        targetFiles_ = TestDataGenerator::applyMutations(files_, 0.0, 0.0, 0.01, 0.0);
        targetTree_ = buildTreeFromFiles(targetFiles_);
    }

protected:
    size_t runIteration() override {
        TreeDiffer differ;
        DiffOptions options;
        options.compareSubtrees = true; // Enable optimization

        auto result = differ.computeDiff(baseTree_, targetTree_, options);
        if (!result) {
            return 0;
        }

        return result.value().changes.size();
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        // Compare with/without optimization
        TreeDiffer differ;

        // With optimization
        DiffOptions optOn;
        optOn.compareSubtrees = true;
        auto start = std::chrono::high_resolution_clock::now();
        auto resultOn = differ.computeDiff(baseTree_, targetTree_, optOn);
        auto end = std::chrono::high_resolution_clock::now();
        auto durationOn = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // Without optimization
        DiffOptions optOff;
        optOff.compareSubtrees = false;
        start = std::chrono::high_resolution_clock::now();
        auto resultOff = differ.computeDiff(baseTree_, targetTree_, optOff);
        end = std::chrono::high_resolution_clock::now();
        auto durationOff = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        double speedup = durationOff.count() > 0
                             ? static_cast<double>(durationOff.count()) / durationOn.count()
                             : 1.0;

        metrics["with_opt_ms"] = durationOn.count();
        metrics["without_opt_ms"] = durationOff.count();
        metrics["speedup_factor"] = speedup;
    }

private:
    std::vector<TestDataGenerator::FileInfo> files_;
    std::vector<TestDataGenerator::FileInfo> targetFiles_;
    TreeNode baseTree_;
    TreeNode targetTree_;
};

} // anonymous namespace

// Main benchmark runner
int main(int argc, char** argv) {
    using namespace yams::benchmark;

    std::cout << "=== PBI-043 Tree-Diff Benchmarks ===" << std::endl;
    std::cout << "Acceptance Criteria Validation:" << std::endl;
    std::cout << "  AC #2: Diff latency < 750ms p95 (10k entries)" << std::endl;
    std::cout << "  AC #7: Rename accuracy ≥ 99%" << std::endl;
    std::cout << "  Storage overhead ≤ 15%" << std::endl;
    std::cout << std::endl;

    BenchmarkBase::Config config;
    config.warmup_iterations = 5;
    config.benchmark_iterations = 20;
    config.verbose = true;
    config.track_memory = true;
    config.output_file = "tree_diff_benchmark_results.jsonl";

    bool allPassed = true;

    // 1. Latency acceptance test (AC #2)
    std::cout << "\n[1/6] Latency Acceptance (AC #2)" << std::endl;
    std::cout << "-----------------------------------" << std::endl;
    LatencyAcceptanceBenchmark latencyBench(config);
    auto latencyResult = latencyBench.run();
    double p95 = latencyResult.custom_metrics["p95_latency_ms"];
    bool ac2Passed = (p95 < 750.0);
    allPassed &= ac2Passed;
    std::cout << (ac2Passed ? "✓ PASSED" : "✗ FAILED") << ": p95=" << p95 << "ms (threshold: 750ms)"
              << std::endl;

    // 2. Rename detection accuracy (AC #7)
    std::cout << "\n[2/6] Rename Detection Accuracy (AC #7)" << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    RenameDetectionBenchmark renameBench(1000, 0.20, config); // 20% renames
    auto renameResult = renameBench.run();
    double accuracy = renameResult.custom_metrics["rename_accuracy"];
    bool ac7Passed = (accuracy >= 99.0);
    allPassed &= ac7Passed;
    std::cout << (ac7Passed ? "✓ PASSED" : "✗ FAILED") << ": accuracy=" << accuracy
              << "% (threshold: 99%)" << std::endl;

    // 3. Storage overhead
    std::cout << "\n[3/6] Storage Overhead" << std::endl;
    std::cout << "----------------------" << std::endl;
    StorageOverheadBenchmark storageBench(10000, config);
    auto storageResult = storageBench.run();
    double overhead = storageResult.custom_metrics["storage_overhead_pct"];
    bool storagePassed = (overhead <= 15.0);
    allPassed &= storagePassed;
    std::cout << (storagePassed ? "✓ PASSED" : "✗ FAILED") << ": overhead=" << overhead
              << "% (threshold: 15%)" << std::endl;

    // 4. Baseline diff performance (1k files)
    std::cout << "\n[4/6] Baseline Diff (1k files)" << std::endl;
    std::cout << "-------------------------------" << std::endl;
    BaselineDiffBenchmark baseline1k(1000, config);
    baseline1k.run();

    // 5. Baseline diff performance (10k files)
    std::cout << "\n[5/6] Baseline Diff (10k files)" << std::endl;
    std::cout << "--------------------------------" << std::endl;
    BaselineDiffBenchmark baseline10k(10000, config);
    baseline10k.run();

    // 6. Subtree hash optimization
    std::cout << "\n[6/6] Subtree Hash Optimization" << std::endl;
    std::cout << "--------------------------------" << std::endl;
    SubtreeHashBenchmark subtreeBench(config);
    auto subtreeResult = subtreeBench.run();
    double speedup = subtreeResult.custom_metrics["speedup_factor"];
    std::cout << "Speedup: " << speedup << "x" << std::endl;

    // Summary
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << (allPassed ? "✓ ALL ACCEPTANCE CRITERIA PASSED"
                            : "✗ SOME ACCEPTANCE CRITERIA FAILED")
              << std::endl;
    std::cout << "Results saved to: " << config.output_file << std::endl;

    return allPassed ? 0 : 1;
}
