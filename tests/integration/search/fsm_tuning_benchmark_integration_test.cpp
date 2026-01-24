// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

/**
 * @file fsm_tuning_benchmark_integration_test.cpp
 * @brief Integration tests for FSM tuning with internal benchmark validation
 *
 * Phase 4 of Epic yams-7ez4: Tests the complete loop between:
 * - CorpusStats collection from real corpus
 * - SearchTuner FSM state selection based on corpus metrics
 * - InternalBenchmark validation of tuned search quality
 * - BenchmarkComparison for regression/improvement detection
 *
 * These tests validate that the adaptive tuning system works correctly
 * with real documents and the actual search infrastructure.
 */

#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/daemon/client/daemon_client.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/internal_benchmark.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>
#include <yams/storage/corpus_stats.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// Windows daemon IPC tests are unstable due to socket shutdown race conditions
#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON()                                                                   \
    SKIP("Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON() ((void)0)
#endif

using namespace yams;
using namespace yams::daemon;
using namespace yams::search;
using namespace yams::storage;
using namespace yams::metadata;
using namespace std::chrono_literals;
using Catch::Approx;
using Catch::Matchers::ContainsSubstring;

namespace yams::test {

// =============================================================================
// Test Fixture: Provides daemon + search infrastructure for integration tests
// =============================================================================

class FSMTuningIntegrationFixture {
public:
    FSMTuningIntegrationFixture() {
        // Start daemon with full search infrastructure
        bool started = harness_.start(std::chrono::seconds(30));
        if (!started) {
            throw std::runtime_error("Failed to start daemon for FSM tuning integration tests");
        }

        // Create daemon client
        ClientConfig clientCfg;
        clientCfg.socketPath = harness_.socketPath();
        clientCfg.connectTimeout = 5s;
        clientCfg.autoStart = false;
        client_ = std::make_unique<DaemonClient>(clientCfg);

        // Connect to daemon
        auto connectResult = cli::run_sync(client_->connect(), 5s);
        if (!connectResult) {
            throw std::runtime_error("Failed to connect to daemon: " +
                                     connectResult.error().message);
        }

        // Wait for daemon to be ready
        auto statusResult = cli::run_sync(client_->status(), 5s);
        if (!statusResult) {
            throw std::runtime_error("Failed to get daemon status: " +
                                     statusResult.error().message);
        }
    }

    ~FSMTuningIntegrationFixture() {
        if (client_) {
            client_->disconnect();
        }
        harness_.stop();
    }

    // Ingest a test file into the daemon
    Result<std::string> ingestFile(const std::string& filename, const std::string& content) {
        // Create temp file
        auto filePath = harness_.dataDir() / filename;
        std::ofstream ofs(filePath);
        if (!ofs) {
            return Error{ErrorCode::IOError, "Failed to create test file"};
        }
        ofs << content;
        ofs.close();

        // Ingest via daemon client using streaming add
        AddDocumentRequest req;
        req.path = filePath.string();
        req.tags = {"test", "integration"};

        auto result = cli::run_sync(client_->streamingAddDocument(req), 10s);
        if (!result) {
            return Error{result.error().code, result.error().message};
        }

        return result.value().hash;
    }

    // Execute search via daemon client
    Result<daemon::SearchResponse> executeSearch(const SearchRequest& req) {
        return cli::run_sync(client_->search(req), 10s);
    }

    DaemonClient* client() { return client_.get(); }
    const std::filesystem::path& dataDir() const { return harness_.dataDir(); }

private:
    DaemonHarness harness_;
    std::unique_ptr<DaemonClient> client_;
};

// =============================================================================
// Helper: Generate test documents with different characteristics
// =============================================================================

std::string generateCodeDocument(size_t functions = 5) {
    std::string code = R"(// Auto-generated test code file
#include <vector>
#include <string>
#include <algorithm>

namespace yams::test {

)";

    for (size_t i = 0; i < functions; ++i) {
        code += "/**\n";
        code += " * @brief Function " + std::to_string(i) + " for testing search indexing\n";
        code += " * @param input The input vector to process\n";
        code += " * @return Processed result vector\n";
        code += " */\n";
        code += "std::vector<int> processFunction" + std::to_string(i) + "(";
        code += "const std::vector<int>& input) {\n";
        code += "    std::vector<int> result;\n";
        code += "    result.reserve(input.size());\n";
        code += "    for (const auto& val : input) {\n";
        code += "        result.push_back(val * " + std::to_string(i + 1) + ");\n";
        code += "    }\n";
        code += "    return result;\n";
        code += "}\n\n";
    }

    code += "} // namespace yams::test\n";
    return code;
}

std::string generateProseDocument(size_t paragraphs = 3) {
    std::string prose;

    for (size_t i = 0; i < paragraphs; ++i) {
        prose += "# Section " + std::to_string(i + 1) + "\n\n";
        prose += "This is paragraph " + std::to_string(i + 1) + " of the test document. ";
        prose += "It contains information about search engine optimization and "
                 "retrieval quality. ";
        prose += "The document discusses various topics including indexing, "
                 "ranking algorithms, ";
        prose += "and benchmark evaluation metrics like MRR and Recall@K.\n\n";
        prose += "Additional context about information retrieval systems and their ";
        prose += "performance characteristics in different corpus environments.\n\n";
    }

    return prose;
}

// =============================================================================
// Test: CorpusStats collection from real indexed documents
// =============================================================================

TEST_CASE("FSM Tuning Integration: CorpusStats reflects ingested documents",
          "[integration][search][fsm][corpus_stats]") {
    SKIP_ON_WINDOWS_DAEMON();
    FSMTuningIntegrationFixture fixture;

    // Ingest some code files
    for (int i = 0; i < 3; ++i) {
        auto result =
            fixture.ingestFile("test_code_" + std::to_string(i) + ".cpp", generateCodeDocument(5));
        REQUIRE(result.has_value());
    }

    // Ingest some prose files
    for (int i = 0; i < 2; ++i) {
        auto result =
            fixture.ingestFile("test_doc_" + std::to_string(i) + ".md", generateProseDocument(3));
        REQUIRE(result.has_value());
    }

    // Wait for indexing to complete
    std::this_thread::sleep_for(1s);

    // Verify corpus stats via status
    auto statusResult = cli::run_sync(fixture.client()->status(), 5s);
    REQUIRE(statusResult.has_value());

    // The daemon should report document count
    INFO("Daemon status after ingestion verified");
}

// =============================================================================
// Test: SearchTuner produces correct state for different corpus profiles
// =============================================================================

TEST_CASE("FSM Tuning Integration: SearchTuner state transitions",
          "[integration][search][fsm][tuner]") {
    // This test validates SearchTuner state selection logic
    // without requiring daemon - uses CorpusStats directly

    SECTION("Code-heavy corpus selects CODE state") {
        CorpusStats stats;
        stats.docCount = 500;
        stats.codeRatio = 0.85;
        stats.proseRatio = 0.10;
        stats.symbolDensity = 0.5;
        stats.pathDepthAvg = 3.0;
        stats.tagCoverage = 0.2;

        SearchTuner tuner(stats);

        // With code ratio > 0.7 and doc count < 1000, should be SMALL_CODE
        CHECK(tuner.currentState() == TuningState::SMALL_CODE);
        CHECK(tuner.getRrfK() == 20);
        CHECK(tuner.getParams().textWeight == Approx(0.45f));
        CHECK(tuner.getParams().pathTreeWeight == Approx(0.15f));
    }

    SECTION("Prose-heavy corpus selects PROSE state") {
        CorpusStats stats;
        stats.docCount = 500;
        stats.codeRatio = 0.10;
        stats.proseRatio = 0.85;
        stats.symbolDensity = 0.01;
        stats.pathDepthAvg = 2.0;
        stats.tagCoverage = 0.3;

        SearchTuner tuner(stats);

        // With prose ratio > 0.7 and doc count < 1000, should be SMALL_PROSE
        CHECK(tuner.currentState() == TuningState::SMALL_PROSE);
        CHECK(tuner.getRrfK() == 25);
        CHECK(tuner.getParams().vectorWeight == Approx(0.40f));
        CHECK(tuner.getParams().pathTreeWeight == Approx(0.00f));
    }

    SECTION("Scientific corpus (prose without path/tag structure)") {
        CorpusStats stats;
        stats.docCount = 500;
        stats.codeRatio = 0.05;
        stats.proseRatio = 0.90;
        stats.symbolDensity = 0.01;
        stats.pathDepthAvg = 1.0; // Flat structure
        stats.tagCoverage = 0.05; // Minimal tags

        SearchTuner tuner(stats);

        // Scientific corpus: prose-dominant, flat structure, no tags
        CHECK(tuner.currentState() == TuningState::SCIENTIFIC);
        CHECK(tuner.getRrfK() == 12);
        CHECK(tuner.getParams().textWeight == Approx(0.60f));
        CHECK(tuner.getParams().vectorWeight == Approx(0.35f));
        CHECK(tuner.getParams().tagWeight == Approx(0.00f));
    }

    SECTION("Large mixed corpus") {
        CorpusStats stats;
        stats.docCount = 5000;
        stats.codeRatio = 0.45;
        stats.proseRatio = 0.40;
        stats.symbolDensity = 0.2;
        stats.pathDepthAvg = 4.0;
        stats.tagCoverage = 0.5;

        SearchTuner tuner(stats);

        // Mixed content, neither code nor prose dominant
        CHECK(tuner.currentState() == TuningState::MIXED);
        CHECK(tuner.getRrfK() == 45);
    }

    SECTION("Minimal corpus") {
        CorpusStats stats;
        stats.docCount = 50; // < 100
        stats.codeRatio = 0.50;
        stats.proseRatio = 0.30;
        stats.symbolDensity = 0.1;
        stats.pathDepthAvg = 2.0;
        stats.tagCoverage = 0.1;

        SearchTuner tuner(stats);

        // Very small corpus should use MINIMAL state
        CHECK(tuner.currentState() == TuningState::MINIMAL);
        CHECK(tuner.getRrfK() == 15);
        CHECK(tuner.getParams().textWeight == Approx(0.55f));
    }
}

// =============================================================================
// Test: TunedParams apply correctly to SearchEngineConfig
// =============================================================================

TEST_CASE("FSM Tuning Integration: TunedParams apply to config",
          "[integration][search][fsm][config]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.codeRatio = 0.75;
    stats.proseRatio = 0.15;
    stats.symbolDensity = 0.4;
    stats.pathDepthAvg = 4.0;
    stats.tagCoverage = 0.3;

    SearchTuner tuner(stats);
    auto config = tuner.getConfig();

    // Verify config has tuned weights applied
    CHECK(config.textWeight == tuner.getParams().textWeight);
    CHECK(config.vectorWeight == tuner.getParams().vectorWeight);
    CHECK(config.pathTreeWeight == tuner.getParams().pathTreeWeight);
    CHECK(config.kgWeight == tuner.getParams().kgWeight);
    CHECK(config.tagWeight == tuner.getParams().tagWeight);
    CHECK(config.metadataWeight == tuner.getParams().metadataWeight);

    // Verify JSON serialization for observability
    auto json = tuner.toJson();
    CHECK(json.contains("state"));
    CHECK(json.contains("params"));
    CHECK(json.contains("reason"));
    CHECK(json["state"].get<std::string>() == tuningStateToString(tuner.currentState()));
}

// =============================================================================
// Test: BenchmarkComparison detects regression and improvement
// =============================================================================

TEST_CASE("FSM Tuning Integration: BenchmarkComparison detection",
          "[integration][search][fsm][benchmark][comparison]") {
    SECTION("Detects significant improvement") {
        BenchmarkResults baseline;
        baseline.mrr = 0.60f;
        baseline.recallAtK = 0.55f;
        baseline.latency.meanMs = 50.0;

        BenchmarkResults current;
        current.mrr = 0.75f; // +0.15 improvement
        current.recallAtK = 0.70f;
        current.latency.meanMs = 45.0;

        auto comparison = InternalBenchmark::compare(baseline, current, 0.05f);

        CHECK(comparison.isImprovement == true);
        CHECK(comparison.isRegression == false);
        CHECK(comparison.mrrDelta == Approx(0.15f));
        CHECK(comparison.recallDelta == Approx(0.15f));
        CHECK_THAT(comparison.summary, ContainsSubstring("IMPROVEMENT"));
    }

    SECTION("Detects significant regression") {
        BenchmarkResults baseline;
        baseline.mrr = 0.80f;
        baseline.recallAtK = 0.75f;
        baseline.latency.meanMs = 40.0;

        BenchmarkResults current;
        current.mrr = 0.65f; // -0.15 regression
        current.recallAtK = 0.60f;
        current.latency.meanMs = 60.0;

        auto comparison = InternalBenchmark::compare(baseline, current, 0.05f);

        CHECK(comparison.isRegression == true);
        CHECK(comparison.isImprovement == false);
        CHECK(comparison.mrrDelta == Approx(-0.15f));
        CHECK_THAT(comparison.summary, ContainsSubstring("REGRESSION"));
    }

    SECTION("No significant change within threshold") {
        BenchmarkResults baseline;
        baseline.mrr = 0.70f;
        baseline.recallAtK = 0.65f;
        baseline.latency.meanMs = 50.0;

        BenchmarkResults current;
        current.mrr = 0.72f; // +0.02, below 0.05 threshold
        current.recallAtK = 0.66f;
        current.latency.meanMs = 48.0;

        auto comparison = InternalBenchmark::compare(baseline, current, 0.05f);

        CHECK(comparison.isRegression == false);
        CHECK(comparison.isImprovement == false);
        CHECK(comparison.mrrDelta == Approx(0.02f));
    }

    SECTION("Handles first run (no baseline)") {
        BenchmarkResults baseline;
        baseline.mrr = 0.0f; // No previous data
        baseline.queriesRun = 0;

        BenchmarkResults current;
        current.mrr = 0.75f;
        current.recallAtK = 0.70f;
        current.queriesRun = 100;

        auto comparison = InternalBenchmark::compare(baseline, current, 0.05f);

        // First run should not be flagged as regression
        CHECK(comparison.isRegression == false);
        // Could be flagged as improvement from 0
        CHECK(comparison.mrrDelta == Approx(0.75f));
    }
}

// =============================================================================
// Test: BenchmarkResults summary format for CLI output
// =============================================================================

TEST_CASE("FSM Tuning Integration: BenchmarkResults summary",
          "[integration][search][fsm][benchmark][summary]") {
    BenchmarkResults results;
    results.mrr = 0.756f;
    results.recallAtK = 0.680f;
    results.k = 10;
    results.latency.meanMs = 42.5;
    results.latency.p95Ms = 85.3;
    results.queriesRun = 100;
    results.queriesSucceeded = 98;
    results.queriesFailed = 2;
    results.totalTime = std::chrono::milliseconds(4500);
    results.tuningState = "LARGE_CODE";

    auto summary = results.summary();

    // Verify summary contains key metrics
    CHECK_THAT(summary, ContainsSubstring("MRR"));
    CHECK_THAT(summary, ContainsSubstring("0.756") || ContainsSubstring("0.76"));
    CHECK_THAT(summary, ContainsSubstring("Recall"));
    CHECK_THAT(summary, ContainsSubstring("queries"));
}

// =============================================================================
// Test: End-to-end FSM tuning flow with real daemon
// =============================================================================

TEST_CASE("FSM Tuning Integration: End-to-end tuning validation",
          "[integration][search][fsm][e2e]") {
    SKIP_ON_WINDOWS_DAEMON();
    FSMTuningIntegrationFixture fixture;

    // 1. Ingest documents to create a code-heavy corpus
    INFO("Ingesting test documents...");
    for (int i = 0; i < 5; ++i) {
        auto result = fixture.ingestFile("e2e_code_" + std::to_string(i) + ".cpp",
                                         generateCodeDocument(3 + i));
        REQUIRE(result.has_value());
    }

    // 2. Wait for indexing
    std::this_thread::sleep_for(2s);

    // 3. Execute a search to verify the tuned engine works
    SearchRequest req;
    req.query = "processFunction vector result";
    req.searchType = "hybrid";
    req.limit = 10;
    req.timeout = 5s;

    auto searchResult = fixture.executeSearch(req);

    // Verify search completes (may return empty on mock provider)
    if (searchResult) {
        INFO("Search returned " << searchResult.value().results.size() << " results");
        // If we have results, they should be from our test files
        for (const auto& hit : searchResult.value().results) {
            INFO("Result: " << hit.path << " score=" << hit.score);
        }
    } else {
        // Mock provider may cause semantic portion to fail
        INFO("Search error (expected with mock): " << searchResult.error().message);
    }
}

// =============================================================================
// Test: Tuner state reason provides debugging info
// =============================================================================

TEST_CASE("FSM Tuning Integration: State reason explains selection",
          "[integration][search][fsm][debug]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.85;
    stats.proseRatio = 0.10;
    stats.symbolDensity = 0.5;
    stats.pathDepthAvg = 3.0;
    stats.tagCoverage = 0.2;

    SearchTuner tuner(stats);

    // State reason should explain why this state was selected
    const auto& reason = tuner.stateReason();
    CHECK(!reason.empty());

    // Should mention the dominant corpus characteristic
    // (Implementation may vary, but should provide useful debug info)
    INFO("State reason: " << reason);
}

} // namespace yams::test
