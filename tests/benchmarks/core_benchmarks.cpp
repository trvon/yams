#include <cstring>
#include <filesystem>
#include <memory>
#include <random>
#include <vector>

#include "../common/test_data_generator.h"
#include "benchmark_base.h"

#include <yams/chunking/chunker.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

namespace yams::benchmark {

// --- Hashing Benchmarks ---

class HashingBenchmark : public BenchmarkBase {
public:
    HashingBenchmark(const std::string& name, size_t dataSize, const Config& config = Config())
        : BenchmarkBase("Hashing_" + name, config), dataSize_(dataSize) {
        setUp();
    }

protected:
    void setUp() {
        hasher_ = crypto::createSHA256Hasher();
        data_ = test::TestDataGenerator().generateRandomBytes(dataSize_);
    }

    size_t runIteration() override {
        auto hash = hasher_->hash(data_);
        return 1; // 1 hash operation
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["bytes_processed"] = static_cast<double>(dataSize_);
    }

    std::unique_ptr<crypto::IContentHasher> hasher_;
    std::vector<std::byte> data_;
    size_t dataSize_;
};

// --- Chunking Benchmarks ---

class ChunkingBenchmark : public BenchmarkBase {
public:
    ChunkingBenchmark(const std::string& name, size_t dataSize,
                      const chunking::ChunkingConfig& chunkConfig, const Config& config = Config())
        : BenchmarkBase("Chunking_" + name, config), dataSize_(dataSize),
          chunkConfig_(chunkConfig) {
        setUp();
    }

protected:
    void setUp() {
        chunker_ = std::make_unique<chunking::RabinChunker>(chunkConfig_);
        data_ = test::TestDataGenerator().generateRandomBytes(dataSize_);
    }

    size_t runIteration() override {
        auto chunks = chunker_->chunkData(data_);
        return chunks.size();
    }

    size_t dataSize_;
    chunking::ChunkingConfig chunkConfig_;
    std::unique_ptr<chunking::RabinChunker> chunker_;
    std::vector<std::byte> data_;
    size_t dataSize_;
};

// --- Compression Benchmarks ---

class CompressionBenchmark : public BenchmarkBase {
public:
    CompressionBenchmark(const std::string& name, size_t dataSize, const std::string& pattern,
                         uint8_t level, const Config& config = Config())
        : BenchmarkBase("Compression_" + name, config), dataSize_(dataSize), pattern_(pattern),
          level_(level) {
        setUp();
    }

protected:
    void setUp() {
        auto& registry = compression::CompressionRegistry::instance();
        compressor_ = registry.createCompressor(compression::CompressionAlgorithm::Zstandard);
        if (!compressor_) {
            throw std::runtime_error("Zstandard compressor not available");
        }
        data_ =
            test::TestDataGenerator().generateRandomBytes(dataSize_); // Simplified data generation
    }

    size_t runIteration() override {
        auto result = compressor_->compress(data_, level_);
        if (result.has_value()) {
            return 1;
        }
        return 0;
    }

    std::unique_ptr<compression::ICompressor> compressor_;
    std::vector<std::byte> data_;
    size_t dataSize_;
    std::string pattern_;
    uint8_t level_;
};

} // namespace yams::benchmark

// --- Main Runner ---

using yams::benchmark::archiveJsonFileBestEffort;
using yams::benchmark::BenchmarkBase;
using yams::benchmark::ChunkingBenchmark;
using yams::benchmark::CompressionBenchmark;
using yams::benchmark::HashingBenchmark;
using yams::benchmark::matchesAnyFilter;
using yams::benchmark::parseBenchmarkArgs;
using yams::test::BenchmarkTracker;

int main(int argc, char** argv) {
    const auto cli = parseBenchmarkArgs(argc, argv);

    BenchmarkBase::Config config;
    config.verbose = cli.verbose;
    config.warmup_iterations = cli.warmupIterations;
    config.benchmark_iterations = cli.iterations;
    config.track_memory = cli.trackMemory;

    std::cout << "YAMS Core Performance Benchmarks\n";
    std::cout << "====================================\n\n";

    std::filesystem::path outDir = cli.outDir;
    std::error_code ec_mkdir;
    std::filesystem::create_directories(outDir, ec_mkdir);
    if (ec_mkdir) {
        std::cerr << "WARNING: unable to create bench_results directory: " << ec_mkdir.message()
                  << std::endl;
    }
    const std::filesystem::path suiteHistoryJson = outDir / "core_benchmarks.json";
    const std::filesystem::path suiteResultsJsonl = outDir / "core_benchmarks.jsonl";
    if (cli.outputFile) {
        config.output_file = cli.outputFile->string();
    } else {
        config.output_file = suiteResultsJsonl.string();
    }

    BenchmarkTracker tracker(suiteHistoryJson);
    std::vector<std::unique_ptr<BenchmarkBase>> benchmarks;

    // Hashing benchmarks
    benchmarks.push_back(std::make_unique<HashingBenchmark>("SHA256_1KB", 1024, config));
    benchmarks.push_back(std::make_unique<HashingBenchmark>("SHA256_1MB", 1024 * 1024, config));

    // Chunking benchmarks
    yams::chunking::ChunkingConfig chunkConfig1 = {
        .minChunkSize = 4096, .targetChunkSize = 16384, .maxChunkSize = 65536};
    benchmarks.push_back(
        std::make_unique<ChunkingBenchmark>("Rabin_1MB", 1024 * 1024, chunkConfig1, config));

    // Compression benchmarks
    benchmarks.push_back(
        std::make_unique<CompressionBenchmark>("Zstd_10KB_Text_L3", 10 * 1024, "text", 3, config));
    benchmarks.push_back(
        std::make_unique<CompressionBenchmark>("Zstd_1MB_Text_L9", 1024 * 1024, "text", 9, config));

    for (auto& benchmark : benchmarks) {
        if (!matchesAnyFilter(benchmark->name(), cli.filters)) {
            continue;
        }
        auto result = benchmark->run();
        BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        tracker.recordResult(trackerResult);
    }

    tracker.generateReport(outDir / "core_benchmark_report.json");
    tracker.generateMarkdownReport(outDir / "core_benchmark_report.md");

    if (cli.archive) {
        tracker.flushHistory();
        if (auto dir = archiveJsonFileBestEffort(suiteHistoryJson, cli.archiveDir, "core")) {
            std::error_code ec;
            std::filesystem::copy_file(suiteResultsJsonl, *dir / suiteResultsJsonl.filename(),
                                       std::filesystem::copy_options::overwrite_existing, ec);
            tracker.snapshotTo(*dir / "snapshot.json");
        }
    }

    std::cout << "\n====================================\n";
    std::cout << "Benchmark complete. Reports generated.\n";

    return 0;
}
