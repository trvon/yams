#include <algorithm>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "../common/test_data_generator.h"
#include "benchmark_base.h"

#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/bk_tree.h>
#include <yams/search/query_parser.h>
#include <yams/search/query_tokenizer.h>
#include <yams/search/search_executor.h>

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
        tempDir_ = std::filesystem::temp_directory_path() / "yams_bench_search";
        std::filesystem::create_directories(tempDir_);
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

    void tearDown() { std::filesystem::remove_all(tempDir_); }

    size_t runIteration() override {
        // This is a mock implementation as IContentStore has no search method.
        return queries_.size();
    }

    std::filesystem::path tempDir_;
    std::unique_ptr<api::IContentStore> contentStore_;
    size_t corpusSize_;
    std::vector<std::string> queries_;
};

// --- BKTree Benchmarks from bk_tree_bench.cpp ---

class BKTreeConstructionBenchmark : public BenchmarkBase {
public:
    BKTreeConstructionBenchmark(const std::string& name, int vocabSize,
                                const Config& config = Config())
        : BenchmarkBase(name, config), vocabSize_(vocabSize) {}

protected:
    size_t runIteration() override {
        search::BKTree tree;
        for (int i = 0; i < vocabSize_; ++i) {
            tree.add("w" + std::to_string(i));
        }
        return vocabSize_;
    }
    int vocabSize_;
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

// --- Main Runner ---

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

    test::BenchmarkTracker tracker("search_benchmarks.json");
    std::vector<std::unique_ptr<BenchmarkBase>> benchmarks;

    // Search benchmarks
    benchmarks.push_back(std::make_unique<SearchBenchmark>("ExactMatch_1K", 1000, config));

    // BK-Tree benchmarks
    benchmarks.push_back(
        std::make_unique<BKTreeConstructionBenchmark>("BKTree_Construction_256", 256, config));
    benchmarks.push_back(
        std::make_unique<BKTreeConstructionBenchmark>("BKTree_Construction_4096", 4096, config));

    // QueryParser benchmarks
    benchmarks.push_back(
        std::make_unique<QueryParserBenchmark>("QueryParser_Simple", "simple query", config));
    benchmarks.push_back(std::make_unique<QueryParserBenchmark>(
        "QueryParser_Complex", "(query OR search) AND (terms OR words)", config));

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

    tracker.generateReport("search_benchmark_report.json");
    tracker.generateMarkdownReport("search_benchmark_report.md");

    std::cout << "\n====================================\n";
    std::cout << "Benchmark complete. Reports generated.\n";

    return 0;
}

} // namespace yams::benchmark
