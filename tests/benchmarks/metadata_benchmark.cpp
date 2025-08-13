#include "benchmark_base.h"
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
#include <yams/api/content_store.h>
#include "../common/test_data_generator.h"
#include <filesystem>
#include <random>

using namespace yams;
using namespace yams::benchmark;
using namespace yams::metadata;

class MetadataBenchmark : public BenchmarkBase {
public:
    MetadataBenchmark(const std::string& name) 
        : BenchmarkBase("Metadata_" + name) {
        setUp();
    }
    
    ~MetadataBenchmark() {
        tearDown();
    }

protected:
    void setUp() {
        // Create temp directory
        tempDir_ = std::filesystem::temp_directory_path() / "yams_bench_metadata";
        std::filesystem::create_directories(tempDir_);
        
        // Initialize database and repository
        auto dbPath = tempDir_ / "metadata.db";
        database_ = std::make_shared<Database>(dbPath.string());
        database_->initialize();
        
        metadataRepo_ = std::make_unique<MetadataRepository>(database_);
        
        // Create test documents
        createTestDocuments();
        
        // Generate test data
        generator_ = std::make_unique<test::TestDataGenerator>();
    }
    
    void tearDown() {
        metadataRepo_.reset();
        database_.reset();
        std::filesystem::remove_all(tempDir_);
    }
    
    void createTestDocuments() {
        // Add sample documents to work with
        for (size_t i = 0; i < 1000; ++i) {
            Document doc;
            doc.id = "doc_" + std::to_string(i);
            doc.hash = "hash_" + std::to_string(i);
            doc.name = "file_" + std::to_string(i) + ".txt";
            doc.path = tempDir_ / doc.name;
            doc.size = 1024 * (i % 100 + 1);
            doc.addedTime = std::chrono::system_clock::now();
            
            metadataRepo_->addDocument(doc);
            documentIds_.push_back(doc.id);
        }
    }
    
    void collectCustomMetrics(std::map<std::string, double>& metrics) override {
        metrics["num_documents"] = documentIds_.size();
        metrics["avg_metadata_size"] = avgMetadataSize_;
        metrics["operations_failed"] = failedOperations_;
    }
    
    std::filesystem::path tempDir_;
    std::shared_ptr<Database> database_;
    std::unique_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<test::TestDataGenerator> generator_;
    std::vector<std::string> documentIds_;
    
    double avgMetadataSize_ = 0;
    size_t failedOperations_ = 0;
};

// Single metadata update
BENCHMARK_F(MetadataBenchmark, SingleUpdate) {
    MetadataBenchmark("SingleUpdate") {}
    
    size_t runIteration() override {
        static size_t docIndex = 0;
        
        const auto& docId = documentIds_[docIndex % documentIds_.size()];
        docIndex++;
        
        // Update a single metadata field
        auto result = metadataRepo_->setMetadata(
            docId, 
            "status", 
            MetadataValue("updated_" + std::to_string(docIndex))
        );
        
        if (!result.isOk()) {
            failedOperations_++;
            return 0;
        }
        
        return 1;
    }
};

// Bulk metadata update (100 documents)
BENCHMARK_F(MetadataBenchmark, BulkUpdate) {
    MetadataBenchmark("BulkUpdate") {}
    
    size_t runIteration() override {
        const size_t batchSize = 100;
        size_t successCount = 0;
        
        auto metadata = generator_->generateMetadata(5);
        
        for (size_t i = 0; i < batchSize && i < documentIds_.size(); ++i) {
            for (const auto& [key, value] : metadata) {
                auto result = metadataRepo_->setMetadata(
                    documentIds_[i],
                    key,
                    value
                );
                
                if (result.isOk()) {
                    successCount++;
                } else {
                    failedOperations_++;
                }
            }
        }
        
        avgMetadataSize_ = metadata.size();
        return successCount;
    }
};

// Metadata query by single field
BENCHMARK_F(MetadataBenchmark, QuerySingleField) {
    MetadataBenchmark("QuerySingleField") {}
    
    void setUp() {
        MetadataBenchmark::setUp();
        
        // Add searchable metadata
        for (size_t i = 0; i < documentIds_.size(); ++i) {
            metadataRepo_->setMetadata(
                documentIds_[i],
                "category",
                MetadataValue("cat_" + std::to_string(i % 10))
            );
        }
    }
    
    size_t runIteration() override {
        size_t totalResults = 0;
        
        // Query different categories
        for (int cat = 0; cat < 10; ++cat) {
            SearchOptions options;
            options.metadataFilter = "category:cat_" + std::to_string(cat);
            
            auto results = metadataRepo_->searchDocuments("", options);
            if (results.has_value()) {
                totalResults += results->size();
            }
        }
        
        return totalResults;
    }
};

// Complex metadata query (multiple fields)
BENCHMARK_F(MetadataBenchmark, ComplexQuery) {
    MetadataBenchmark("ComplexQuery") {}
    
    void setUp() {
        MetadataBenchmark::setUp();
        
        // Add multiple metadata fields
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> catDist(0, 9);
        std::uniform_int_distribution<int> statusDist(0, 2);
        std::uniform_int_distribution<int> priorityDist(1, 5);
        
        std::vector<std::string> statuses = {"pending", "active", "completed"};
        
        for (const auto& docId : documentIds_) {
            metadataRepo_->setMetadata(docId, "category", 
                MetadataValue("cat_" + std::to_string(catDist(rng))));
            metadataRepo_->setMetadata(docId, "status", 
                MetadataValue(statuses[statusDist(rng)]));
            metadataRepo_->setMetadata(docId, "priority", 
                MetadataValue(static_cast<double>(priorityDist(rng))));
        }
    }
    
    size_t runIteration() override {
        size_t totalResults = 0;
        
        // Complex queries with multiple conditions
        std::vector<std::string> queries = {
            "category:cat_1 AND status:active",
            "priority:>3 AND status:pending",
            "category:cat_5 OR category:cat_6",
            "status:completed AND priority:<3",
            "(category:cat_2 OR category:cat_3) AND status:active"
        };
        
        for (const auto& query : queries) {
            SearchOptions options;
            options.metadataFilter = query;
            
            auto results = metadataRepo_->searchDocuments("", options);
            if (results.has_value()) {
                totalResults += results->size();
            }
        }
        
        return queries.size();
    }
};

// Get all metadata for a document
BENCHMARK_F(MetadataBenchmark, GetAllMetadata) {
    MetadataBenchmark("GetAllMetadata") {}
    
    void setUp() {
        MetadataBenchmark::setUp();
        
        // Add 20 metadata fields per document
        for (size_t i = 0; i < 100; ++i) {
            auto metadata = generator_->generateMetadata(20);
            for (const auto& [key, value] : metadata) {
                metadataRepo_->setMetadata(documentIds_[i], key, value);
            }
        }
    }
    
    size_t runIteration() override {
        size_t totalFields = 0;
        
        // Get all metadata for first 100 documents
        for (size_t i = 0; i < 100; ++i) {
            auto result = metadataRepo_->getAllMetadata(documentIds_[i]);
            if (result.has_value()) {
                totalFields += result->size();
            }
        }
        
        avgMetadataSize_ = totalFields / 100.0;
        return 100; // Number of documents queried
    }
};

// Remove metadata
BENCHMARK_F(MetadataBenchmark, RemoveMetadata) {
    MetadataBenchmark("RemoveMetadata") {}
    
    void setUp() {
        MetadataBenchmark::setUp();
        
        // Add metadata to remove
        for (const auto& docId : documentIds_) {
            metadataRepo_->setMetadata(docId, "temp_field", MetadataValue("temp_value"));
        }
    }
    
    size_t runIteration() override {
        static size_t removeIndex = 0;
        
        if (removeIndex >= documentIds_.size()) {
            removeIndex = 0;
            // Re-add for next iteration
            for (const auto& docId : documentIds_) {
                metadataRepo_->setMetadata(docId, "temp_field", MetadataValue("temp_value"));
            }
        }
        
        auto result = metadataRepo_->removeMetadata(
            documentIds_[removeIndex], 
            "temp_field"
        );
        
        removeIndex++;
        
        if (!result.isOk()) {
            failedOperations_++;
            return 0;
        }
        
        return 1;
    }
};

// Concurrent metadata operations
BENCHMARK_F(MetadataBenchmark, ConcurrentOperations) {
    MetadataBenchmark("ConcurrentOperations") {}
    
    size_t runIteration() override {
        const size_t numThreads = 4;
        std::atomic<size_t> successCount{0};
        std::atomic<size_t> failCount{0};
        
        std::vector<std::thread> threads;
        
        for (size_t t = 0; t < numThreads; ++t) {
            threads.emplace_back([this, t, &successCount, &failCount]() {
                std::mt19937 rng(t);
                std::uniform_int_distribution<size_t> docDist(0, documentIds_.size() - 1);
                std::uniform_int_distribution<int> opDist(0, 2);
                
                for (size_t i = 0; i < 25; ++i) {
                    size_t docIdx = docDist(rng);
                    int op = opDist(rng);
                    
                    Result<void> result;
                    
                    switch (op) {
                        case 0: // Set
                            result = metadataRepo_->setMetadata(
                                documentIds_[docIdx],
                                "thread_" + std::to_string(t),
                                MetadataValue("value_" + std::to_string(i))
                            );
                            break;
                            
                        case 1: // Get
                            {
                                auto getResult = metadataRepo_->getMetadata(
                                    documentIds_[docIdx],
                                    "thread_" + std::to_string(t)
                                );
                                result = getResult.has_value() ? 
                                    Result<void>::ok() : 
                                    Result<void>::error(getResult.error());
                            }
                            break;
                            
                        case 2: // Remove
                            result = metadataRepo_->removeMetadata(
                                documentIds_[docIdx],
                                "thread_" + std::to_string(t)
                            );
                            break;
                    }
                    
                    if (result.isOk()) {
                        successCount++;
                    } else {
                        failCount++;
                    }
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        failedOperations_ = failCount.load();
        return successCount.load();
    }
};

// Main benchmark runner
int main(int argc, char** argv) {
    BenchmarkBase::Config config;
    config.verbose = true;
    config.benchmark_iterations = 10;
    config.warmup_iterations = 2;
    
    // Parse arguments
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
    
    std::cout << "YAMS Metadata Performance Benchmarks\n";
    std::cout << "====================================\n\n";
    
    // Run benchmarks
    std::vector<std::unique_ptr<MetadataBenchmark>> benchmarks;
    
    benchmarks.push_back(std::make_unique<SingleUpdateBenchmark>());
    benchmarks.push_back(std::make_unique<BulkUpdateBenchmark>());
    benchmarks.push_back(std::make_unique<QuerySingleFieldBenchmark>());
    benchmarks.push_back(std::make_unique<ComplexQueryBenchmark>());
    benchmarks.push_back(std::make_unique<GetAllMetadataBenchmark>());
    benchmarks.push_back(std::make_unique<RemoveMetadataBenchmark>());
    benchmarks.push_back(std::make_unique<ConcurrentOperationsBenchmark>());
    
    // Track results
    test::BenchmarkTracker tracker("metadata_benchmarks.json");
    
    for (auto& benchmark : benchmarks) {
        benchmark->config = config;
        std::cout << "\n";
        auto result = benchmark->run();
        
        // Record for tracking
        test::BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        
        tracker.recordResult(trackerResult);
    }
    
    // Generate reports
    tracker.generateReport("metadata_benchmark_report.json");
    tracker.generateMarkdownReport("metadata_benchmark_report.md");
    
    std::cout << "\n====================================\n";
    std::cout << "Benchmark complete. Reports generated.\n";
    
    return 0;
}