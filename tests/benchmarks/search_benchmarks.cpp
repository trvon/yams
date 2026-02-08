#include <algorithm>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "../common/test_data_generator.h"
#include "../common/test_helpers_catch2.h"
#include "benchmark_base.h"

#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/parallel_post_processor.hpp>
#include <yams/search/query_parser.h>
#include <yams/search/query_tokenizer.h>

namespace yams::benchmark {

// --- Search Benchmarks from search_benchmark.cpp ---

class SearchBenchmark : public BenchmarkBase {
public:
    SearchBenchmark(const std::string& name, size_t corpusSize, const Config& config = Config())
        : BenchmarkBase("Search_" + name, config), corpusSize_(corpusSize) {
        setUp();
    }
    ~SearchBenchmark() { tearDown(); }

protected:
    void setUp() {
        tempDir_ = test::make_temp_dir("yams_bench_search_");
        auto result = api::createContentStore(tempDir_ / "storage");
        if (!result) {
            throw std::runtime_error("Failed to create content store: " + result.error().message);
        }
        contentStore_ = std::move(result).value();
        test::TestDataGenerator generator;
        for (size_t i = 0; i < corpusSize_; ++i) {
            auto content = generator.generateTextDocument(1024 + (i % 10) * 1024);
            api::ContentMetadata metadata;
            metadata.name = "doc_" + std::to_string(i) + ".txt";
            contentStore_->storeBytes(std::as_bytes(std::span(content)), metadata);
        }
        queries_ = {"benchmark", "performance", "search"};
    }

    void tearDown() {
        std::error_code ec;
        std::filesystem::remove_all(tempDir_, ec);
    }

    size_t runIteration() override {
        // This is a mock implementation as IContentStore has no search method.
        return queries_.size();
    }

    std::filesystem::path tempDir_;
    std::unique_ptr<api::IContentStore> contentStore_;
    size_t corpusSize_;
    std::vector<std::string> queries_;
};

// --- QueryParser Benchmarks from query_parser_bench.cpp ---

class QueryParserBenchmark : public BenchmarkBase {
public:
    QueryParserBenchmark(const std::string& name, std::string query,
                         const Config& config = Config())
        : BenchmarkBase(name, config), query_(std::move(query)) {}

protected:
    size_t runIteration() override {
        search::QueryParser parser;
        auto result = parser.parse(query_);
        return result.has_value() ? 1 : 0;
    }
    std::string query_;
};

// --- Parallel Post-Processor Benchmarks (PBI-001 Phase 3) ---

class ParallelPostProcessorBenchmark : public BenchmarkBase {
public:
    ParallelPostProcessorBenchmark(const std::string& name, int resultCount,
                                   const Config& config = Config())
        : BenchmarkBase(name, config), resultCount_(resultCount) {
        // Create test results
        for (int i = 0; i < resultCount_; ++i) {
            search::SearchResultItem item;
            item.documentId = i;
            item.title = "Document " + std::to_string(i);
            item.contentPreview = "This is the content preview for document " + std::to_string(i) +
                                  " with some searchable text.";
            item.contentType = (i % 3 == 0)   ? "text/plain"
                               : (i % 3 == 1) ? "text/markdown"
                                              : "text/html";
            item.detectedLanguage = (i % 2 == 0) ? "en" : "es";
            results_.push_back(item);
        }
        facetFields_ = {"contentType", "language"};
    }

protected:
    size_t runIteration() override {
        // Copy results for each iteration
        auto resultsCopy = results_;

        auto result =
            search::ParallelPostProcessor::process(std::move(resultsCopy),
                                                   nullptr, // No filters
                                                   facetFields_,
                                                   nullptr, // No query AST (no highlights)
                                                   100,     // Snippet length
                                                   3        // Max highlights
            );

        return result.filteredResults.size();
    }

    int resultCount_;
    std::vector<search::SearchResultItem> results_;
    std::vector<std::string> facetFields_;
};

} // namespace yams::benchmark

// --- Main Runner ---

using yams::benchmark::BenchmarkBase;
using yams::benchmark::QueryParserBenchmark;
using yams::benchmark::SearchBenchmark;
using yams::test::BenchmarkTracker;

int main(int argc, char** argv) {
    BenchmarkBase::Config config;
    config.verbose = true;
    config.benchmark_iterations = 10;

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

    std::cout << "YAMS Search Performance Benchmarks\n";
    std::cout << "====================================\n\n";

    std::filesystem::path outDir = "bench_results";
    std::error_code ec_mkdir;
    std::filesystem::create_directories(outDir, ec_mkdir);
    if (ec_mkdir) {
        std::cerr << "WARNING: unable to create bench_results directory: " << ec_mkdir.message()
                  << std::endl;
    }
    if (config.output_file.empty()) {
        config.output_file = (outDir / "search_benchmarks.json").string();
    }

    BenchmarkTracker tracker(outDir / "search_benchmarks.json");
    std::vector<std::unique_ptr<BenchmarkBase>> benchmarks;

    // Search benchmarks
    benchmarks.push_back(std::make_unique<SearchBenchmark>("ExactMatch_1K", 1000, config));

    // QueryParser benchmarks
    benchmarks.push_back(
        std::make_unique<QueryParserBenchmark>("QueryParser_Simple", "simple query", config));
    benchmarks.push_back(std::make_unique<QueryParserBenchmark>(
        "QueryParser_Complex", "(query OR search) AND (terms OR words)", config));

    // Parallel Post-Processor benchmarks (PBI-001 Phase 3)
    benchmarks.push_back(std::make_unique<yams::benchmark::ParallelPostProcessorBenchmark>(
        "ParallelPostProcessor_100", 100, config));
    benchmarks.push_back(std::make_unique<yams::benchmark::ParallelPostProcessorBenchmark>(
        "ParallelPostProcessor_500", 500, config));
    benchmarks.push_back(std::make_unique<yams::benchmark::ParallelPostProcessorBenchmark>(
        "ParallelPostProcessor_1000", 1000, config));

    for (auto& benchmark : benchmarks) {
        auto result = benchmark->run();
        BenchmarkTracker::BenchmarkResult trackerResult;
        trackerResult.name = result.name;
        trackerResult.value = result.duration_ms;
        trackerResult.unit = "ms";
        trackerResult.timestamp = std::chrono::system_clock::now();
        trackerResult.metrics = result.custom_metrics;
        tracker.recordResult(trackerResult);
    }

    tracker.generateReport(outDir / "search_benchmark_report.json");
    tracker.generateMarkdownReport(outDir / "search_benchmark_report.md");

    std::cout << "\n====================================\n";
    std::cout << "Benchmark complete. Reports generated.\n";

    return 0;
}
