#include <algorithm>
#include <filesystem>
#include <iterator>
#include <random>
#include <thread>
#include <vector>

#include "../common/fixture_manager.h"
#include "../common/test_data_generator.h"
#include "benchmark_base.h"

#include <yams/api/content_metadata.h>
#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/storage_engine.h>

using namespace yams;
using namespace yams::benchmark;

// --- Ingestion Benchmarks ---

class IngestionBenchmark : public BenchmarkBase {
public:
    IngestionBenchmark(const std::string& name, const Config& config = Config())
        : BenchmarkBase("Ingestion_" + name, config) {
        setUp();
    }
    ~IngestionBenchmark() { tearDown(); }

protected:
    void setUp() {
        tempDir_ = std::filesystem::temp_directory_path() / "yams_bench_ingestion";
        std::filesystem::create_directories(tempDir_);
        auto result = api::createContentStore(tempDir_ / "storage");
        if (!result) {
            throw std::runtime_error("Failed to create content store: " + result.error().message);
        }
        contentStore_ = std::move(result).value();
        generator_ = std::make_unique<test::TestDataGenerator>();
    }

    void tearDown() { std::filesystem::remove_all(tempDir_); }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["dedup_ratio"] = lastDedupRatio_;
        metrics["avg_chunk_size"] = avgChunkSize_;
        metrics["total_bytes"] = totalBytesProcessed_;
    }

    std::filesystem::path tempDir_;
    std::unique_ptr<api::IContentStore> contentStore_;
    std::unique_ptr<test::TestDataGenerator> generator_;

    double lastDedupRatio_ = 0.0;
    double avgChunkSize_ = 0.0;
    size_t totalBytesProcessed_ = 0;
};

BENCHMARK_F(IngestionBenchmark, SmallDocument) {
    auto content = generator_->generateTextDocument(1024);
    std::vector<std::byte> contentBytes;
    contentBytes.reserve(content.size());
    std::transform(content.begin(), content.end(), std::back_inserter(contentBytes),
                   [](char c) { return std::byte(c); });
    api::ContentMetadata metadata;
    metadata.name = "small_doc.txt";
    auto result = contentStore_->storeBytes(contentBytes, metadata);
    if (result.has_value()) {
        lastDedupRatio_ = result.value().dedupRatio();
        totalBytesProcessed_ = 1024;
        return 1;
    }
    return 0;
}

BENCHMARK_F(IngestionBenchmark, MediumDocument) {
    auto content = generator_->generateTextDocument(100 * 1024);
    std::vector<std::byte> contentBytes;
    contentBytes.reserve(content.size());
    std::transform(content.begin(), content.end(), std::back_inserter(contentBytes),
                   [](char c) { return std::byte(c); });
    api::ContentMetadata metadata;
    metadata.name = "medium_doc.txt";
    auto result = contentStore_->storeBytes(contentBytes, metadata);
    if (result.has_value()) {
        lastDedupRatio_ = result.value().dedupRatio();
        totalBytesProcessed_ = 100 * 1024;
        return 1;
    }
    return 0;
}

// --- Metadata Benchmarks ---

class MetadataBenchmark : public BenchmarkBase {
public:
    MetadataBenchmark(const std::string& name, const Config& config = Config())
        : BenchmarkBase("Metadata_" + name, config) {
        setUp();
    }
    ~MetadataBenchmark() { tearDown(); }

protected:
    void setUp() {
        tempDir_ = std::filesystem::temp_directory_path() / "yams_bench_metadata";
        std::filesystem::create_directories(tempDir_);
        auto dbPath = tempDir_ / "metadata.db";
        connectionPool_ = std::make_unique<metadata::ConnectionPool>(dbPath.string());
        metadataRepo_ = std::make_unique<metadata::MetadataRepository>(*connectionPool_);
        createTestDocuments();
        generator_ = std::make_unique<test::TestDataGenerator>();
    }

    void tearDown() {
        metadataRepo_.reset();
        connectionPool_.reset();
        std::filesystem::remove_all(tempDir_);
    }

    void createTestDocuments() {
        for (size_t i = 0; i < 1000; ++i) {
            metadata::DocumentInfo doc;
            doc.fileName = "file_" + std::to_string(i) + ".txt";
            doc.filePath = (tempDir_ / doc.fileName).string();
            doc.sha256Hash = "hash_" + std::to_string(i);
            doc.fileSize = 1024 * (i % 100 + 1);
            doc.createdTime = std::chrono::system_clock::now();
            auto result = metadataRepo_->insertDocument(doc);
            if (result) {
                documentIds_.push_back(result.value());
            }
        }
    }

    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["num_documents"] = documentIds_.size();
        metrics["avg_metadata_size"] = avgMetadataSize_;
        metrics["operations_failed"] = failedOperations_;
    }

    std::filesystem::path tempDir_;
    std::unique_ptr<metadata::ConnectionPool> connectionPool_;
    std::unique_ptr<metadata::MetadataRepository> metadataRepo_;
    std::unique_ptr<test::TestDataGenerator> generator_;
    std::vector<int64_t> documentIds_;
    double avgMetadataSize_ = 0;
    size_t failedOperations_ = 0;
};

BENCHMARK_F(MetadataBenchmark, SingleUpdate) {
    static size_t docIndex = 0;
    const auto& docId = documentIds_[docIndex % documentIds_.size()];
    docIndex++;
    auto result = metadataRepo_->setMetadata(
        docId, "status", metadata::MetadataValue("updated_" + std::to_string(docIndex)));
    if (!result) {
        failedOperations_++;
        return 0;
    }
    return 1;
}

BENCHMARK_F(MetadataBenchmark, BulkUpdate) {
    const size_t batchSize = 100;
    size_t successCount = 0;
    auto metadata = generator_->generateMetadata(5);
    for (size_t i = 0; i < batchSize && i < documentIds_.size(); ++i) {
        for (const auto& [key, value] : metadata) {
            auto result = metadataRepo_->setMetadata(documentIds_[i], key, value);
            if (result) {
                successCount++;
            } else {
                failedOperations_++;
            }
        }
    }
    avgMetadataSize_ = metadata.size();
    return successCount;
}

// --- Main Runner ---

int main(int argc, char** argv) {
    BenchmarkBase::Config config;
    config.verbose = true;
    config.benchmark_iterations = 5;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--quiet") {
            config.verbose = false;
        } else if (arg == "--iterations" && i + 1 < argc) {
            config.benchmark_iterations = std::stoi(argv[++i]);
        } else if (arg == "--output" && i + 1 < argc) {
            config.output_file = argv[++i];
        }
    }

    std::cout << "YAMS API Performance Benchmarks\n";
    std::cout << "====================================\n\n";

    std::filesystem::path outDir = "bench_results";
    std::error_code ec_mkdir;
    std::filesystem::create_directories(outDir, ec_mkdir);
    if (ec_mkdir) {
        std::cerr << "WARNING: unable to create bench_results directory: " << ec_mkdir.message()
                  << std::endl;
    }
    if (config.output_file.empty()) {
        config.output_file = (outDir / "api_benchmarks.json").string();
    }
    test::BenchmarkTracker tracker(outDir / "api_benchmarks.json");

    std::vector<std::unique_ptr<BenchmarkBase>> benchmarks;
    benchmarks.push_back(std::make_unique<SmallDocumentBenchmark>());
    benchmarks.push_back(std::make_unique<MediumDocumentBenchmark>());
    benchmarks.push_back(std::make_unique<SingleUpdateBenchmark>(config));
    benchmarks.push_back(std::make_unique<BulkUpdateBenchmark>(config));

    for (auto& benchmark : benchmarks) {
        auto result = benchmark->run();
        test::BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        tracker.recordResult(trackerResult);
    }

    tracker.generateReport(outDir / "api_benchmark_report.json");
    tracker.generateMarkdownReport(outDir / "api_benchmark_report.md");

    std::cout << "\n====================================\n";
    std::cout << "Benchmark complete. Reports generated.\n";

    return 0;
}
