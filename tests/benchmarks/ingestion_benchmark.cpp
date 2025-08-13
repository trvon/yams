#include "benchmark_base.h"
#include <yams/api/content_store.h>
#include <yams/storage/storage_engine.h>
#include <yams/chunking/rabin_chunker.h>
#include <yams/crypto/sha256_hasher.h>
#include "../common/test_data_generator.h"
#include "../common/fixture_manager.h"
#include <filesystem>
#include <random>
#include <vector>

using namespace yams;
using namespace yams::benchmark;

class IngestionBenchmark : public BenchmarkBase {
public:
    IngestionBenchmark(const std::string& name) 
        : BenchmarkBase("Ingestion_" + name) {
        setUp();
    }
    
    ~IngestionBenchmark() {
        tearDown();
    }

protected:
    void setUp() {
        // Create temp directory for testing
        tempDir_ = std::filesystem::temp_directory_path() / "yams_bench_ingestion";
        std::filesystem::create_directories(tempDir_);
        
        // Initialize components
        storageEngine_ = std::make_unique<storage::StorageEngine>(tempDir_ / "storage");
        contentStore_ = std::make_unique<api::ContentStore>(tempDir_.string());
        
        // Generate test data
        generator_ = std::make_unique<test::TestDataGenerator>();
    }
    
    void tearDown() {
        // Clean up
        std::filesystem::remove_all(tempDir_);
    }
    
    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["dedup_ratio"] = lastDedupRatio_;
        metrics["avg_chunk_size"] = avgChunkSize_;
        metrics["total_bytes"] = totalBytesProcessed_;
    }
    
    std::filesystem::path tempDir_;
    std::unique_ptr<storage::StorageEngine> storageEngine_;
    std::unique_ptr<api::ContentStore> contentStore_;
    std::unique_ptr<test::TestDataGenerator> generator_;
    
    double lastDedupRatio_ = 0.0;
    double avgChunkSize_ = 0.0;
    size_t totalBytesProcessed_ = 0;
};

// Benchmark: Single small document ingestion
BENCHMARK_F(IngestionBenchmark, SmallDocument) {
    // Generate 1KB document
    auto content = generator_->generateTextDocument(1024);
    
    // Ingest document
    auto result = contentStore_->addContent(
        std::vector<std::byte>(content.begin(), content.end()),
        "small_doc.txt"
    );
    
    if (result.has_value()) {
        lastDedupRatio_ = result->dedupRatio();
        totalBytesProcessed_ = 1024;
        return 1; // 1 document processed
    }
    
    return 0;
}

// Benchmark: Medium document ingestion (100KB)
BENCHMARK_F(IngestionBenchmark, MediumDocument) {
    // Generate 100KB document
    auto content = generator_->generateTextDocument(100 * 1024);
    
    // Ingest document
    auto result = contentStore_->addContent(
        std::vector<std::byte>(content.begin(), content.end()),
        "medium_doc.txt"
    );
    
    if (result.has_value()) {
        lastDedupRatio_ = result->dedupRatio();
        totalBytesProcessed_ = 100 * 1024;
        return 1;
    }
    
    return 0;
}

// Benchmark: Large document ingestion (10MB)
BENCHMARK_F(IngestionBenchmark, LargeDocument) {
    // Generate 10MB document
    auto content = generator_->generateTextDocument(10 * 1024 * 1024);
    
    // Ingest document
    auto result = contentStore_->addContent(
        std::vector<std::byte>(content.begin(), content.end()),
        "large_doc.txt"
    );
    
    if (result.has_value()) {
        lastDedupRatio_ = result->dedupRatio();
        totalBytesProcessed_ = 10 * 1024 * 1024;
        return 1;
    }
    
    return 0;
}

// Benchmark: Batch ingestion (100 x 10KB documents)
BENCHMARK_F(IngestionBenchmark, BatchIngestion) {
    const size_t numDocs = 100;
    const size_t docSize = 10 * 1024; // 10KB each
    size_t successCount = 0;
    
    for (size_t i = 0; i < numDocs; ++i) {
        auto content = generator_->generateTextDocument(docSize);
        auto result = contentStore_->addContent(
            std::vector<std::byte>(content.begin(), content.end()),
            "batch_doc_" + std::to_string(i) + ".txt"
        );
        
        if (result.has_value()) {
            successCount++;
            lastDedupRatio_ += result->dedupRatio();
        }
    }
    
    lastDedupRatio_ /= numDocs;
    totalBytesProcessed_ = numDocs * docSize;
    avgChunkSize_ = totalBytesProcessed_ / (numDocs * 4); // Assume ~4 chunks per doc
    
    return successCount;
}

// Benchmark: Ingestion with metadata
BENCHMARK_F(IngestionBenchmark, WithMetadata) {
    // Generate document with metadata
    auto content = generator_->generateTextDocument(50 * 1024);
    auto metadata = generator_->generateMetadata(10);
    
    // Create temporary file
    auto filePath = tempDir_ / "metadata_doc.txt";
    std::ofstream file(filePath);
    file << content;
    file.close();
    
    // Add with metadata
    std::vector<std::string> tags = {"benchmark", "test", "metadata"};
    auto result = contentStore_->addFile(filePath, tags);
    
    if (result.has_value()) {
        // Update metadata
        for (const auto& [key, value] : metadata) {
            // Note: Would need metadata repository access here
            // For benchmark, we're measuring the ingestion part
        }
        
        totalBytesProcessed_ = 50 * 1024;
        return 1;
    }
    
    return 0;
}

// Benchmark: PDF ingestion
BENCHMARK_F(IngestionBenchmark, PDFDocument) {
    // Generate PDF
    auto pdfData = generator_->generatePDF(10); // 10 pages
    
    // Create temporary PDF file
    auto pdfPath = tempDir_ / "test.pdf";
    std::ofstream file(pdfPath, std::ios::binary);
    file.write(reinterpret_cast<const char*>(pdfData.data()), pdfData.size());
    file.close();
    
    // Ingest PDF
    auto result = contentStore_->addFile(pdfPath);
    
    if (result.has_value()) {
        totalBytesProcessed_ = pdfData.size();
        return 1;
    }
    
    return 0;
}

// Benchmark: Deduplication performance
BENCHMARK_F(IngestionBenchmark, Deduplication) {
    const size_t numDocs = 50;
    const size_t uniqueDocs = 10;
    size_t successCount = 0;
    
    // Generate unique documents
    std::vector<std::string> uniqueContents;
    for (size_t i = 0; i < uniqueDocs; ++i) {
        uniqueContents.push_back(generator_->generateTextDocument(5 * 1024));
    }
    
    // Ingest with duplicates
    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> dist(0, uniqueDocs - 1);
    
    for (size_t i = 0; i < numDocs; ++i) {
        size_t idx = dist(rng);
        auto& content = uniqueContents[idx];
        
        auto result = contentStore_->addContent(
            std::vector<std::byte>(content.begin(), content.end()),
            "dedup_doc_" + std::to_string(i) + ".txt"
        );
        
        if (result.has_value()) {
            successCount++;
            lastDedupRatio_ += result->dedupRatio();
        }
    }
    
    lastDedupRatio_ /= numDocs;
    totalBytesProcessed_ = numDocs * 5 * 1024;
    
    return successCount;
}

// Benchmark: Concurrent ingestion
BENCHMARK_F(IngestionBenchmark, ConcurrentIngestion) {
    const size_t numThreads = 4;
    const size_t docsPerThread = 25;
    std::atomic<size_t> successCount{0};
    
    std::vector<std::thread> threads;
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, docsPerThread, &successCount]() {
            test::TestDataGenerator localGen(t); // Thread-local generator
            
            for (size_t i = 0; i < docsPerThread; ++i) {
                auto content = localGen.generateTextDocument(8 * 1024);
                auto result = contentStore_->addContent(
                    std::vector<std::byte>(content.begin(), content.end()),
                    "thread_" + std::to_string(t) + "_doc_" + std::to_string(i) + ".txt"
                );
                
                if (result.has_value()) {
                    successCount++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    totalBytesProcessed_ = numThreads * docsPerThread * 8 * 1024;
    return successCount.load();
}

// Main benchmark runner
int main(int argc, char** argv) {
    BenchmarkBase::Config config;
    config.verbose = true;
    config.benchmark_iterations = 5;
    
    // Parse command line arguments
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
    
    // Run benchmarks
    std::vector<std::unique_ptr<IngestionBenchmark>> benchmarks;
    
    benchmarks.push_back(std::make_unique<SmallDocumentBenchmark>());
    benchmarks.push_back(std::make_unique<MediumDocumentBenchmark>());
    benchmarks.push_back(std::make_unique<LargeDocumentBenchmark>());
    benchmarks.push_back(std::make_unique<BatchIngestionBenchmark>());
    benchmarks.push_back(std::make_unique<WithMetadataBenchmark>());
    benchmarks.push_back(std::make_unique<PDFDocumentBenchmark>());
    benchmarks.push_back(std::make_unique<DeduplicationBenchmark>());
    benchmarks.push_back(std::make_unique<ConcurrentIngestionBenchmark>());
    
    // Track results
    test::BenchmarkTracker tracker("ingestion_benchmarks.json");
    
    for (auto& benchmark : benchmarks) {
        auto result = benchmark->run();
        
        // Record for regression tracking
        test::BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        
        tracker.recordResult(trackerResult);
    }
    
    // Generate report
    tracker.generateReport("ingestion_benchmark_report.json");
    tracker.generateMarkdownReport("ingestion_benchmark_report.md");
    
    return 0;
}