// Copyright 2025 YAMS Project
// SPDX-License-Identifier: GPL-3.0-or-later

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

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <unordered_map>
#include "../benchmarks/benchmark_base.h"
#include "../common/benchmark_tracker.h"
#include <yams/metadata/tree_builder.h>
#include <yams/metadata/tree_differ.h>
#include <yams/storage/storage_engine.h>

namespace fs = std::filesystem;
using namespace yams::metadata;
using namespace yams::benchmark;
using yams::test::BenchmarkTracker;

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
            // Force exactly 8 hex chars per chunk so we always emit 64 hex chars total.
            // size_t is commonly 64-bit; without masking, values can exceed 8 hex chars.
            const std::uint32_t word =
                static_cast<std::uint32_t>((seed + static_cast<size_t>(i)) & 0xffffffffu);
            ss << std::setw(8) << word;
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

struct SyntheticDirNode {
    std::map<std::string, SyntheticDirNode> children;
    std::vector<TreeEntry> files;
};

struct SnapshotStorageSummary {
    size_t blockOnlySize = 0;
    std::vector<std::pair<std::string, uint64_t>> blocks;
    std::vector<std::pair<std::string, size_t>> treeNodes;
};

SyntheticDirNode
buildSyntheticDirectoryTree(const std::vector<TestDataGenerator::FileInfo>& files) {
    SyntheticDirNode root;

    for (const auto& file : files) {
        fs::path path(file.path);
        SyntheticDirNode* current = &root;

        for (auto it = path.begin(); it != path.end(); ++it) {
            const bool isLeaf = std::next(it) == path.end();
            const std::string component = it->string();
            if (component.empty()) {
                continue;
            }

            if (isLeaf) {
                TreeEntry entry;
                entry.name = component;
                entry.hash = file.hash;
                entry.mode = 0100644;
                entry.size = file.size;
                entry.isDirectory = false;
                current->files.push_back(std::move(entry));
            } else {
                current = &current->children[component];
            }
        }
    }

    return root;
}

std::string
collectSyntheticDirectorySummary(const SyntheticDirNode& node,
                                 std::vector<std::pair<std::string, size_t>>& treeNodes) {
    std::vector<TreeEntry> entries;
    entries.reserve(node.children.size() + node.files.size());

    for (const auto& [name, child] : node.children) {
        TreeEntry entry;
        entry.name = name;
        entry.hash = collectSyntheticDirectorySummary(child, treeNodes);
        entry.mode = 040000;
        entry.size = 0;
        entry.isDirectory = true;
        entries.push_back(std::move(entry));
    }

    entries.insert(entries.end(), node.files.begin(), node.files.end());

    TreeNode tree(std::move(entries));
    auto serialized = tree.serialize();
    auto hash = tree.computeHash();
    treeNodes.emplace_back(hash, serialized.size());
    return hash;
}

SnapshotStorageSummary
buildSnapshotStorageSummary(const std::vector<TestDataGenerator::FileInfo>& files) {
    SnapshotStorageSummary summary;
    summary.blocks.reserve(files.size());

    for (const auto& file : files) {
        summary.blockOnlySize += file.size;
        summary.blocks.emplace_back(file.hash, file.size);
    }

    auto root = buildSyntheticDirectoryTree(files);
    collectSyntheticDirectorySummary(root, summary.treeNodes);
    return summary;
}

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
        metrics["file_count"] = static_cast<double>(fileCount_);
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
        for (const auto& file : targetFiles_) {
            if (file.path.ends_with(".renamed")) {
                ++actualRenames_;
            }
        }

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
            const size_t detectedRenames = result.value().filesRenamed;

            double accuracy =
                actualRenames_ > 0 ? static_cast<double>(detectedRenames) / actualRenames_ : 1.0;

            metrics["rename_accuracy"] = accuracy * 100.0; // Percentage
            metrics["expected_renames"] = static_cast<double>(actualRenames_);
            metrics["detected_renames"] = static_cast<double>(detectedRenames);
            metrics["rename_rate"] = renameRate_ * 100.0;
        }
    }

private:
    size_t fileCount_;
    double renameRate_;
    size_t actualRenames_ = 0;
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
            latencies.push_back(static_cast<double>(duration.count()));
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

        metrics["tree_size_bytes"] = static_cast<double>(treeSize_);
        metrics["flat_size_bytes"] = static_cast<double>(flatSize_);
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

// Benchmark: Multi-snapshot storage deduplication (Task 043-07)
class MultiSnapshotStorageBenchmark : public BenchmarkBase {
public:
    MultiSnapshotStorageBenchmark(size_t snapshotCount, const Config& config)
        : BenchmarkBase("MultiSnapshotStorage_" + std::to_string(snapshotCount), config),
          snapshotCount_(snapshotCount) {
        // Generate initial file set (1000 files as realistic repository size)
        baseFiles_ = TestDataGenerator::generateFiles(1000, 5);
    }

protected:
    size_t runIteration() override {
        ensurePrepared();
        calculateStorageSizes();

        return snapshotCount_;
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        // Calculate deduplication benefit
        double blockOnlySize = blockOnlyTotalSize_;
        double treeWithBlockSize = treeStorageSize_ + blockDeduplicatedSize_;

        double savings = 0.0;
        if (blockOnlySize > 0) {
            savings = ((blockOnlySize - treeWithBlockSize) / blockOnlySize) * 100.0;
        }

        // Storage overhead of tree metadata relative to deduplicated blocks
        double treeOverhead = 0.0;
        if (blockDeduplicatedSize_ > 0) {
            treeOverhead = (static_cast<double>(treeStorageSize_) / blockDeduplicatedSize_) * 100.0;
        }

        metrics["snapshot_count"] = static_cast<double>(snapshotCount_);
        metrics["block_only_total_mb"] =
            static_cast<double>(blockOnlyTotalSize_) / (1024.0 * 1024.0);
        metrics["block_deduplicated_mb"] =
            static_cast<double>(blockDeduplicatedSize_) / (1024.0 * 1024.0);
        metrics["tree_metadata_mb"] = static_cast<double>(treeStorageSize_) / (1024.0 * 1024.0);
        metrics["tree_plus_blocks_mb"] =
            static_cast<double>(treeStorageSize_ + blockDeduplicatedSize_) / (1024.0 * 1024.0);
        metrics["dedup_savings_pct"] = savings;
        metrics["tree_overhead_pct"] = treeOverhead;
        metrics["unique_tree_nodes"] = static_cast<double>(uniqueTreeNodes_);
        metrics["unique_blocks"] = static_cast<double>(uniqueBlocks_);
        metrics["ac7_threshold_pct"] = 15.0;
        metrics["ac7_passed"] = (treeOverhead <= 15.0) ? 1.0 : 0.0;

        // PRD claims 10-20% additional savings from tree-level dedup
        bool expectedSavings = (savings >= 10.0 && savings <= 20.0);
        metrics["prd_claim_validated"] = expectedSavings ? 1.0 : 0.0;
    }

private:
    size_t snapshotCount_;
    std::vector<TestDataGenerator::FileInfo> baseFiles_;
    std::vector<std::vector<TestDataGenerator::FileInfo>> fileStates_;
    std::vector<SnapshotStorageSummary> snapshotSummaries_;
    bool prepared_ = false;

    // Storage metrics
    size_t blockOnlyTotalSize_ = 0;
    size_t blockDeduplicatedSize_ = 0;
    size_t treeStorageSize_ = 0;
    size_t uniqueTreeNodes_ = 0;
    size_t uniqueBlocks_ = 0;

    void ensurePrepared() {
        if (prepared_) {
            return;
        }

        fileStates_.clear();
        snapshotSummaries_.clear();
        fileStates_.reserve(snapshotCount_);
        snapshotSummaries_.reserve(snapshotCount_);
        fileStates_.push_back(baseFiles_);

        for (size_t i = 1; i < snapshotCount_; ++i) {
            // Realistic commit: 2% add, 1% delete, 3% modify, 0.5% rename.
            fileStates_.push_back(
                TestDataGenerator::applyMutations(fileStates_.back(), 0.02, 0.01, 0.03, 0.005));
        }

        for (const auto& state : fileStates_) {
            snapshotSummaries_.push_back(buildSnapshotStorageSummary(state));
        }

        prepared_ = true;
    }

    void calculateStorageSizes() {
        // Track unique blocks (content-addressed by hash)
        std::unordered_map<std::string, uint64_t> uniqueBlockSizes;

        // Calculate block-only storage (no tree metadata)
        blockOnlyTotalSize_ = 0;
        for (const auto& snapshot : snapshotSummaries_) {
            blockOnlyTotalSize_ += snapshot.blockOnlySize;
            for (const auto& [hash, size] : snapshot.blocks) {
                uniqueBlockSizes.emplace(hash, size);
            }
        }

        // Calculate deduplicated block storage
        blockDeduplicatedSize_ = 0;
        for (const auto& [hash, size] : uniqueBlockSizes) {
            static_cast<void>(hash);
            blockDeduplicatedSize_ += size;
        }
        uniqueBlocks_ = uniqueBlockSizes.size();

        // Calculate tree metadata storage across all unique directory subtrees.
        std::unordered_map<std::string, size_t> uniqueTreeSizes;
        treeStorageSize_ = 0;
        uniqueTreeNodes_ = 0;

        for (const auto& snapshot : snapshotSummaries_) {
            for (const auto& [hash, size] : snapshot.treeNodes) {
                if (uniqueTreeSizes.emplace(hash, size).second) {
                    ++uniqueTreeNodes_;
                }
            }
        }

        for (const auto& [hash, size] : uniqueTreeSizes) {
            static_cast<void>(hash);
            treeStorageSize_ += size;
        }
    }
};

// Benchmark: Subtree hash optimization
class SubtreeHashBenchmark : public BenchmarkBase {
public:
    SubtreeHashBenchmark(const Config& config) : BenchmarkBase("SubtreeHashOptimization", config) {
        // Generate large tree with deep hierarchy
        files_ = TestDataGenerator::generateFiles(5000, 10);
        baseTree_ = buildTreeFromFiles(files_);
        unchangedTree_ = baseTree_;

        // Target: change only 1% of files
        targetFiles_ = TestDataGenerator::applyMutations(files_, 0.0, 0.0, 0.01, 0.0);
        targetTree_ = buildTreeFromFiles(targetFiles_);
    }

protected:
    size_t runIteration() override {
        constexpr size_t kBatchIterations = 250;
        TreeDiffer differ;
        DiffOptions options;
        options.compareSubtrees = true;

        // The current implementation short-circuits identical trees via root-hash comparison.
        for (size_t i = 0; i < kBatchIterations; ++i) {
            auto result = differ.computeDiff(baseTree_, unchangedTree_, options);
            if (!result) {
                return 0;
            }
        }

        return kBatchIterations;
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        constexpr size_t kTrials = 25;
        constexpr size_t kBatchIterations = 250;
        TreeDiffer differ;

        auto measureAverageMs = [&](const TreeNode& lhs, const TreeNode& rhs,
                                    const DiffOptions& options) -> double {
            double totalMs = 0.0;
            for (size_t trial = 0; trial < kTrials; ++trial) {
                auto start = std::chrono::high_resolution_clock::now();
                for (size_t iter = 0; iter < kBatchIterations; ++iter) {
                    auto result = differ.computeDiff(lhs, rhs, options);
                    if (!result) {
                        return 0.0;
                    }
                }
                auto end = std::chrono::high_resolution_clock::now();
                totalMs +=
                    std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() /
                    1000.0;
            }
            return totalMs / static_cast<double>(kTrials * kBatchIterations);
        };

        // Measure the optimization on identical trees, which is the only current fast path.
        DiffOptions optOn;
        optOn.compareSubtrees = true;
        const double durationOnMs = measureAverageMs(baseTree_, unchangedTree_, optOn);
        if (durationOnMs <= 0.0) {
            return;
        }

        DiffOptions optOff;
        optOff.compareSubtrees = false;
        const double durationOffMs = measureAverageMs(baseTree_, unchangedTree_, optOff);
        if (durationOffMs <= 0.0) {
            return;
        }

        // Keep a changed-tree measurement for context so this benchmark still surfaces real diff
        // cost.
        DiffOptions changedTreeOpts;
        changedTreeOpts.compareSubtrees = true;
        auto changedStart = std::chrono::high_resolution_clock::now();
        auto changedResult = differ.computeDiff(baseTree_, targetTree_, changedTreeOpts);
        auto changedEnd = std::chrono::high_resolution_clock::now();
        if (!changedResult) {
            return;
        }
        const double changedTreeMs =
            std::chrono::duration_cast<std::chrono::microseconds>(changedEnd - changedStart)
                .count() /
            1000.0;

        double speedup = durationOnMs > 0.0 ? durationOffMs / durationOnMs : 1.0;

        metrics["batch_iterations"] = static_cast<double>(kBatchIterations);
        metrics["with_opt_ms"] = durationOnMs;
        metrics["without_opt_ms"] = durationOffMs;
        metrics["changed_tree_ms"] = changedTreeMs;
        metrics["speedup_factor"] = speedup;
        metrics["with_opt_ops_per_sec"] = 1000.0 / durationOnMs;
        metrics["without_opt_ops_per_sec"] = 1000.0 / durationOffMs;
    }

private:
    std::vector<TestDataGenerator::FileInfo> files_;
    std::vector<TestDataGenerator::FileInfo> targetFiles_;
    TreeNode baseTree_;
    TreeNode unchangedTree_;
    TreeNode targetTree_;
};

} // anonymous namespace

// Main benchmark runner
int main(int argc, char** argv) {
    const auto cli = parseBenchmarkArgs(argc, argv);
    auto hasFlag = [&](std::string_view flag) {
        for (int i = 1; i < argc; ++i) {
            if (argv[i] && flag == argv[i]) {
                return true;
            }
        }
        return false;
    };

    std::cout << "=== PBI-043 Tree-Diff Benchmarks ===" << std::endl;
    std::cout << "Acceptance Criteria Validation:" << std::endl;
    std::cout << "  AC #2: Diff latency < 750ms p95 (10k entries)" << std::endl;
    std::cout << "  AC #7: Rename accuracy ≥ 99%" << std::endl;
    std::cout << "  Storage overhead ≤ 15%" << std::endl;
    std::cout << "  Task 043-07: Multi-snapshot storage deduplication" << std::endl;
    std::cout << std::endl;

    BenchmarkBase::Config config;
    config.warmup_iterations = hasFlag("--warmup") ? cli.warmupIterations : 5;
    config.benchmark_iterations = hasFlag("--iterations") ? cli.iterations : 20;
    config.verbose = cli.verbose;
    config.track_memory = cli.trackMemory;

    std::filesystem::path outDir = cli.outDir;
    std::error_code ec_mkdir;
    std::filesystem::create_directories(outDir, ec_mkdir);
    if (ec_mkdir) {
        std::cerr << "WARNING: unable to create bench_results directory: " << ec_mkdir.message()
                  << std::endl;
    }

    const std::filesystem::path suiteHistoryJson = outDir / "tree_diff_benchmark_results.json";
    const std::filesystem::path suiteResultsJsonl = outDir / "tree_diff_benchmark_results.jsonl";
    if (cli.outputFile) {
        config.output_file = cli.outputFile->string();
    } else {
        config.output_file = suiteResultsJsonl.string();
    }

    BenchmarkTracker tracker(suiteHistoryJson);

    auto recordResult = [&](const BenchmarkBase::Result& result) {
        BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        tracker.recordResult(trackerResult);
    };

    auto runSelected = [&](BenchmarkBase& benchmark) -> std::optional<BenchmarkBase::Result> {
        if (!matchesAnyFilter(benchmark.name(), cli.filters, cli.exactFilters)) {
            return std::nullopt;
        }
        auto result = benchmark.run();
        recordResult(result);
        return result;
    };

    bool allPassed = true;

    // 1. Latency acceptance test (AC #2)
    std::cout << "\n[1/9] Latency Acceptance (AC #2)" << std::endl;
    std::cout << "-----------------------------------" << std::endl;
    LatencyAcceptanceBenchmark latencyBench(config);
    if (auto latencyResult = runSelected(latencyBench)) {
        double p95 = latencyResult->custom_metrics["p95_latency_ms"];
        bool ac2Passed = (p95 < 750.0);
        allPassed &= ac2Passed;
        std::cout << (ac2Passed ? "✓ PASSED" : "✗ FAILED") << ": p95=" << p95
                  << "ms (threshold: 750ms)" << std::endl;
    }

    // 2. Rename detection accuracy (AC #7)
    std::cout << "\n[2/9] Rename Detection Accuracy (AC #7)" << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    RenameDetectionBenchmark renameBench(1000, 0.20, config); // 20% renames
    if (auto renameResult = runSelected(renameBench)) {
        double accuracy = renameResult->custom_metrics["rename_accuracy"];
        bool ac7Passed = (accuracy >= 99.0);
        allPassed &= ac7Passed;
        std::cout << (ac7Passed ? "✓ PASSED" : "✗ FAILED") << ": accuracy=" << accuracy
                  << "% (threshold: 99%)" << std::endl;
    }

    // 3. Storage overhead (single snapshot)
    std::cout << "\n[3/9] Storage Overhead (Single Snapshot)" << std::endl;
    std::cout << "-----------------------------------------" << std::endl;
    StorageOverheadBenchmark storageBench(10000, config);
    if (auto storageResult = runSelected(storageBench)) {
        double overhead = storageResult->custom_metrics["storage_overhead_pct"];
        bool storagePassed = (overhead <= 15.0);
        allPassed &= storagePassed;
        std::cout << (storagePassed ? "✓ PASSED" : "✗ FAILED") << ": overhead=" << overhead
                  << "% (threshold: 15%)" << std::endl;
    }

    // 4. Multi-snapshot storage (10 commits) - Task 043-07
    std::cout << "\n[4/9] Multi-Snapshot Storage (10 commits)" << std::endl;
    std::cout << "-------------------------------------------" << std::endl;
    MultiSnapshotStorageBenchmark multiStorage10(10, config);
    if (auto multiResult10 = runSelected(multiStorage10)) {
        double savings10 = multiResult10->custom_metrics["dedup_savings_pct"];
        double treeOverhead10 = multiResult10->custom_metrics["tree_overhead_pct"];
        bool multi10Passed = (treeOverhead10 <= 15.0);
        allPassed &= multi10Passed;
        std::cout << "  Dedup savings: " << savings10 << "%" << std::endl;
        std::cout << "  Tree overhead: " << treeOverhead10 << "%" << std::endl;
        std::cout << (multi10Passed ? "✓ PASSED" : "✗ FAILED") << ": overhead=" << treeOverhead10
                  << "% (threshold: 15%)" << std::endl;
    }

    // 5. Multi-snapshot storage (100 commits) - Task 043-07
    std::cout << "\n[5/9] Multi-Snapshot Storage (100 commits)" << std::endl;
    std::cout << "--------------------------------------------" << std::endl;
    MultiSnapshotStorageBenchmark multiStorage100(100, config);
    if (auto multiResult100 = runSelected(multiStorage100)) {
        double savings100 = multiResult100->custom_metrics["dedup_savings_pct"];
        double treeOverhead100 = multiResult100->custom_metrics["tree_overhead_pct"];
        bool multi100Passed = (treeOverhead100 <= 15.0);
        allPassed &= multi100Passed;
        std::cout << "  Dedup savings: " << savings100 << "%" << std::endl;
        std::cout << "  Tree overhead: " << treeOverhead100 << "%" << std::endl;
        std::cout << (multi100Passed ? "✓ PASSED" : "✗ FAILED") << ": overhead=" << treeOverhead100
                  << "% (threshold: 15%)" << std::endl;
    }

    // 6. Multi-snapshot storage (1000 commits) - Task 043-07
    std::cout << "\n[6/9] Multi-Snapshot Storage (1000 commits)" << std::endl;
    std::cout << "---------------------------------------------" << std::endl;
    MultiSnapshotStorageBenchmark multiStorage1000(1000, config);
    if (auto multiResult1000 = runSelected(multiStorage1000)) {
        double savings1000 = multiResult1000->custom_metrics["dedup_savings_pct"];
        double treeOverhead1000 = multiResult1000->custom_metrics["tree_overhead_pct"];
        bool multi1000Passed = (treeOverhead1000 <= 15.0);
        allPassed &= multi1000Passed;
        std::cout << "  Dedup savings: " << savings1000 << "%" << std::endl;
        std::cout << "  Tree overhead: " << treeOverhead1000 << "%" << std::endl;
        std::cout << "  PRD claim (10-20% savings): "
                  << (multiResult1000->custom_metrics["prd_claim_validated"] > 0.5 ? "✓ VALIDATED"
                                                                                   : "✗ NOT MET")
                  << std::endl;
        std::cout << (multi1000Passed ? "✓ PASSED" : "✗ FAILED")
                  << ": overhead=" << treeOverhead1000 << "% (threshold: 15%)" << std::endl;
    }

    // 7. Baseline diff performance (1k files)
    std::cout << "\n[7/9] Baseline Diff (1k files)" << std::endl;
    std::cout << "-------------------------------" << std::endl;
    BaselineDiffBenchmark baseline1k(1000, config);
    runSelected(baseline1k);

    // 8. Baseline diff performance (10k files)
    std::cout << "\n[8/9] Baseline Diff (10k files)" << std::endl;
    std::cout << "--------------------------------" << std::endl;
    BaselineDiffBenchmark baseline10k(10000, config);
    runSelected(baseline10k);

    // 9. Subtree hash optimization
    std::cout << "\n[9/9] Subtree Hash Optimization" << std::endl;
    std::cout << "--------------------------------" << std::endl;
    SubtreeHashBenchmark subtreeBench(config);
    if (auto subtreeResult = runSelected(subtreeBench)) {
        double speedup = subtreeResult->custom_metrics["speedup_factor"];
        std::cout << "Speedup: " << speedup << "x" << std::endl;
    }

    tracker.generateReport(suiteHistoryJson);
    tracker.generateMarkdownReport(outDir / "tree_diff_benchmark_report.md");

    if (cli.archive) {
        tracker.flushHistory();
        if (auto dir = archiveJsonFileBestEffort(suiteHistoryJson, cli.archiveDir, "tree-diff")) {
            std::error_code ec;
            std::filesystem::copy_file(suiteResultsJsonl, *dir / suiteResultsJsonl.filename(),
                                       std::filesystem::copy_options::overwrite_existing, ec);
            tracker.snapshotTo(*dir / "snapshot.json");
        }
    }

    // Summary
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << (allPassed ? "✓ ALL ACCEPTANCE CRITERIA PASSED"
                            : "✗ SOME ACCEPTANCE CRITERIA FAILED")
              << std::endl;
    std::cout << "Results saved to: " << suiteHistoryJson << std::endl;
    std::cout << "Raw results saved to: " << config.output_file << std::endl;

    return allPassed ? 0 : 1;
}
