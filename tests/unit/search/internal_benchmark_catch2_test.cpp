// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

/**
 * @file internal_benchmark_catch2_test.cpp
 * @brief Unit tests for InternalBenchmark (Phase 3 of Epic yams-7ez4)
 *
 * Tests cover:
 * - LatencyStats computation (percentiles, mean, median, stddev)
 * - QueryType enum and string conversion
 * - SyntheticQuery JSON serialization
 * - BenchmarkResults summary() output format
 * - BenchmarkResults JSON serialization
 * - BenchmarkComparison regression/improvement detection
 * - QueryGeneratorConfig default values
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/search/internal_benchmark.h>

#include <cmath>
#include <vector>

using namespace yams::search;
using Catch::Approx;
using Catch::Matchers::ContainsSubstring;

// =============================================================================
// QueryType enum and string conversion tests
// =============================================================================

TEST_CASE("QueryType: string conversion", "[unit][internal_benchmark][enum]") {
    CHECK(std::string(queryTypeToString(QueryType::KNOWN_ITEM)) == "KNOWN_ITEM");
    CHECK(std::string(queryTypeToString(QueryType::TFIDF_TERMS)) == "TFIDF_TERMS");
    CHECK(std::string(queryTypeToString(QueryType::SEMANTIC)) == "SEMANTIC");
}

// =============================================================================
// SyntheticQuery tests
// =============================================================================

TEST_CASE("SyntheticQuery: JSON serialization", "[unit][internal_benchmark][query]") {
    SyntheticQuery query;
    query.text = "test query";
    query.expectedDocHash = "abc123hash";
    query.expectedFilePath = "/path/to/file.txt";
    query.type = QueryType::KNOWN_ITEM;
    query.sourcePhrase = "original phrase";

    auto json = query.toJson();

    CHECK(json["text"] == "test query");
    CHECK(json["expected_doc_hash"] == "abc123hash");
    CHECK(json["expected_file_path"] == "/path/to/file.txt");
    CHECK(json["type"] == "KNOWN_ITEM");
    CHECK(json["source_phrase"] == "original phrase");
}

TEST_CASE("SyntheticQuery: all query types serialize correctly",
          "[unit][internal_benchmark][query]") {
    std::vector<QueryType> types = {QueryType::KNOWN_ITEM, QueryType::TFIDF_TERMS,
                                    QueryType::SEMANTIC};

    for (auto type : types) {
        SyntheticQuery query;
        query.type = type;
        auto json = query.toJson();

        CHECK(json["type"] == queryTypeToString(type));
    }
}

// =============================================================================
// LatencyStats computation tests
// =============================================================================

TEST_CASE("LatencyStats: empty samples returns zeroed stats",
          "[unit][internal_benchmark][latency]") {
    std::vector<double> samples;
    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 0);
    CHECK(stats.minMs == 0.0);
    CHECK(stats.maxMs == 0.0);
    CHECK(stats.meanMs == 0.0);
    CHECK(stats.medianMs == 0.0);
    CHECK(stats.p95Ms == 0.0);
    CHECK(stats.p99Ms == 0.0);
    CHECK(stats.stddevMs == 0.0);
}

TEST_CASE("LatencyStats: single sample", "[unit][internal_benchmark][latency]") {
    std::vector<double> samples = {42.0};
    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 1);
    CHECK(stats.minMs == 42.0);
    CHECK(stats.maxMs == 42.0);
    CHECK(stats.meanMs == 42.0);
    CHECK(stats.medianMs == 42.0);
    CHECK(stats.p95Ms == 42.0);
    CHECK(stats.p99Ms == 42.0);
    CHECK(stats.stddevMs == 0.0); // No variance with single sample
}

TEST_CASE("LatencyStats: two samples", "[unit][internal_benchmark][latency]") {
    std::vector<double> samples = {10.0, 20.0};
    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 2);
    CHECK(stats.minMs == 10.0);
    CHECK(stats.maxMs == 20.0);
    CHECK(stats.meanMs == 15.0);
    CHECK(stats.medianMs == 15.0); // Average of two values
}

TEST_CASE("LatencyStats: odd number of samples (median is middle value)",
          "[unit][internal_benchmark][latency]") {
    std::vector<double> samples = {5.0, 10.0, 15.0, 20.0, 25.0};
    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 5);
    CHECK(stats.minMs == 5.0);
    CHECK(stats.maxMs == 25.0);
    CHECK(stats.meanMs == 15.0);
    CHECK(stats.medianMs == 15.0); // Middle value
}

TEST_CASE("LatencyStats: even number of samples (median is average of two middle)",
          "[unit][internal_benchmark][latency]") {
    std::vector<double> samples = {10.0, 20.0, 30.0, 40.0};
    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 4);
    CHECK(stats.minMs == 10.0);
    CHECK(stats.maxMs == 40.0);
    CHECK(stats.meanMs == 25.0);
    CHECK(stats.medianMs == 25.0); // Average of 20 and 30
}

TEST_CASE("LatencyStats: percentile calculations with 100 samples",
          "[unit][internal_benchmark][latency]") {
    // Create 100 samples: 1.0, 2.0, 3.0, ..., 100.0
    std::vector<double> samples;
    samples.reserve(100);
    for (int i = 1; i <= 100; ++i) {
        samples.push_back(static_cast<double>(i));
    }

    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 100);
    CHECK(stats.minMs == 1.0);
    CHECK(stats.maxMs == 100.0);
    CHECK(stats.meanMs == Approx(50.5));   // Sum 1-100 = 5050, avg = 50.5
    CHECK(stats.medianMs == Approx(50.5)); // Average of 50 and 51
    CHECK(stats.p95Ms == Approx(95.0).margin(1.0));
    CHECK(stats.p99Ms == Approx(99.0).margin(1.0));
}

TEST_CASE("LatencyStats: standard deviation calculation", "[unit][internal_benchmark][latency]") {
    // Simple case: samples with known stddev
    std::vector<double> samples = {2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0};
    // Mean = 40/8 = 5
    // Variance = sum((x-5)^2)/(n-1) = (9+1+1+1+0+0+4+16)/7 = 32/7 ≈ 4.571
    // Stddev = sqrt(4.571) ≈ 2.138

    auto stats = LatencyStats::compute(samples);

    CHECK(stats.meanMs == 5.0);
    CHECK(stats.stddevMs == Approx(2.138).margin(0.01));
}

TEST_CASE("LatencyStats: unsorted input is handled correctly",
          "[unit][internal_benchmark][latency]") {
    // Samples in random order
    std::vector<double> samples = {50.0, 10.0, 90.0, 30.0, 70.0};
    auto stats = LatencyStats::compute(samples);

    // After sorting: 10, 30, 50, 70, 90
    CHECK(stats.minMs == 10.0);
    CHECK(stats.maxMs == 90.0);
    CHECK(stats.medianMs == 50.0);
    CHECK(stats.meanMs == 50.0);
}

TEST_CASE("LatencyStats: JSON serialization", "[unit][internal_benchmark][latency]") {
    std::vector<double> samples = {10.0, 20.0, 30.0};
    auto stats = LatencyStats::compute(samples);
    auto json = stats.toJson();

    CHECK(json["min_ms"].get<double>() == 10.0);
    CHECK(json["max_ms"].get<double>() == 30.0);
    CHECK(json["mean_ms"].get<double>() == 20.0);
    CHECK(json["median_ms"].get<double>() == 20.0);
    CHECK(json["sample_count"] == 3);
}

// =============================================================================
// BenchmarkResults tests
// =============================================================================

TEST_CASE("BenchmarkResults: summary output format", "[unit][internal_benchmark][results]") {
    BenchmarkResults results;
    results.mrr = 0.85f;
    results.recallAtK = 0.7f;
    results.k = 10;
    results.queriesRun = 100;
    results.queriesSucceeded = 95;
    results.queriesFailed = 5;
    results.totalTime = std::chrono::milliseconds(5000);
    results.latency.meanMs = 45.5;
    results.latency.medianMs = 42.0;
    results.latency.p95Ms = 85.0;
    results.latency.p99Ms = 120.0;

    auto summary = results.summary();

    // Check key elements are present
    CHECK_THAT(summary, ContainsSubstring("100 queries"));
    CHECK_THAT(summary, ContainsSubstring("MRR"));
    CHECK_THAT(summary, ContainsSubstring("0.85"));
    CHECK_THAT(summary, ContainsSubstring("Recall@10"));
    CHECK_THAT(summary, ContainsSubstring("70%"));
    CHECK_THAT(summary, ContainsSubstring("Mean"));
    CHECK_THAT(summary, ContainsSubstring("Median"));
    CHECK_THAT(summary, ContainsSubstring("P95"));
    CHECK_THAT(summary, ContainsSubstring("P99"));
    CHECK_THAT(summary, ContainsSubstring("Succeeded"));
    CHECK_THAT(summary, ContainsSubstring("95"));
    CHECK_THAT(summary, ContainsSubstring("Failed"));
    CHECK_THAT(summary, ContainsSubstring("5"));
}

TEST_CASE("BenchmarkResults: summary includes tuning state when present",
          "[unit][internal_benchmark][results]") {
    BenchmarkResults results;
    results.queriesRun = 10;
    results.tuningState = "SMALL_CODE";

    auto summary = results.summary();

    CHECK_THAT(summary, ContainsSubstring("Tuning"));
    CHECK_THAT(summary, ContainsSubstring("SMALL_CODE"));
}

TEST_CASE("BenchmarkResults: JSON serialization", "[unit][internal_benchmark][results]") {
    BenchmarkResults results;
    results.mrr = 0.75f;
    results.recallAtK = 0.65f;
    results.precisionAtK = 0.1f;
    results.k = 10;
    results.queriesRun = 50;
    results.queriesSucceeded = 48;
    results.queriesFailed = 2;
    results.totalTime = std::chrono::milliseconds(3500);
    results.timestamp = "2025-01-06T12:00:00Z";

    auto json = results.toJson();

    CHECK(json["mrr"].get<float>() == Approx(0.75f));
    CHECK(json["recall_at_k"].get<float>() == Approx(0.65f));
    CHECK(json["precision_at_k"].get<float>() == Approx(0.1f));
    CHECK(json["k"] == 10);
    CHECK(json["queries_run"] == 50);
    CHECK(json["queries_succeeded"] == 48);
    CHECK(json["queries_failed"] == 2);
    CHECK(json["total_time_ms"] == 3500);
    CHECK(json["timestamp"] == "2025-01-06T12:00:00Z");
}

TEST_CASE("BenchmarkResults: JSON includes optional tuning fields",
          "[unit][internal_benchmark][results]") {
    BenchmarkResults results;
    results.tuningState = "LARGE_PROSE";
    results.tunedParams = nlohmann::json{{"rrf_k", 60}, {"text_weight", 0.4f}};

    auto json = results.toJson();

    CHECK(json["tuning_state"] == "LARGE_PROSE");
    CHECK(json["tuned_params"]["rrf_k"] == 60);
    CHECK(json["tuned_params"]["text_weight"].get<float>() == Approx(0.4f));
}

TEST_CASE("BenchmarkResults: JSON omits optional fields when not set",
          "[unit][internal_benchmark][results]") {
    BenchmarkResults results;

    auto json = results.toJson();

    CHECK_FALSE(json.contains("tuning_state"));
    CHECK_FALSE(json.contains("tuned_params"));
}

// =============================================================================
// BenchmarkComparison tests
// =============================================================================

TEST_CASE("BenchmarkComparison: no change (delta within threshold)",
          "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.80f;
    baseline.recallAtK = 0.70f;
    baseline.latency.meanMs = 50.0;

    BenchmarkResults current;
    current.mrr = 0.81f; // +0.01, within default threshold of 0.05
    current.recallAtK = 0.71f;
    current.latency.meanMs = 48.0;

    auto cmp = InternalBenchmark::compare(baseline, current);

    CHECK(cmp.mrrDelta == Approx(0.01f));
    CHECK(cmp.recallDelta == Approx(0.01f));
    CHECK(cmp.latencyDelta == Approx(-2.0f));
    CHECK_FALSE(cmp.isRegression);
    CHECK_FALSE(cmp.isImprovement);
    CHECK_THAT(cmp.summary, ContainsSubstring("No significant change"));
}

TEST_CASE("BenchmarkComparison: regression detected", "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.80f;
    baseline.recallAtK = 0.70f;
    baseline.latency.meanMs = 50.0;

    BenchmarkResults current;
    current.mrr = 0.70f; // -0.10, exceeds default threshold of 0.05
    current.recallAtK = 0.60f;
    current.latency.meanMs = 60.0;

    auto cmp = InternalBenchmark::compare(baseline, current);

    CHECK(cmp.mrrDelta == Approx(-0.10f));
    CHECK(cmp.isRegression);
    CHECK_FALSE(cmp.isImprovement);
    CHECK_THAT(cmp.summary, ContainsSubstring("REGRESSION"));
}

TEST_CASE("BenchmarkComparison: improvement detected", "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.70f;
    baseline.recallAtK = 0.60f;
    baseline.latency.meanMs = 80.0;

    BenchmarkResults current;
    current.mrr = 0.85f; // +0.15, exceeds default threshold of 0.05
    current.recallAtK = 0.75f;
    current.latency.meanMs = 45.0;

    auto cmp = InternalBenchmark::compare(baseline, current);

    CHECK(cmp.mrrDelta == Approx(0.15f));
    CHECK_FALSE(cmp.isRegression);
    CHECK(cmp.isImprovement);
    CHECK_THAT(cmp.summary, ContainsSubstring("IMPROVEMENT"));
}

TEST_CASE("BenchmarkComparison: custom threshold", "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.80f;

    BenchmarkResults current;
    current.mrr = 0.82f; // +0.02

    // With default threshold (0.05), this is not an improvement
    auto cmp1 = InternalBenchmark::compare(baseline, current);
    CHECK_FALSE(cmp1.isImprovement);

    // With lower threshold (0.01), this IS an improvement
    auto cmp2 = InternalBenchmark::compare(baseline, current, 0.01f);
    CHECK(cmp2.isImprovement);
}

TEST_CASE("BenchmarkComparison: boundary just below threshold",
          "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.80f;

    BenchmarkResults current;
    current.mrr = 0.849f; // Just under +0.05

    auto cmp = InternalBenchmark::compare(baseline, current, 0.05f);

    // Delta < threshold, so no improvement
    CHECK(cmp.mrrDelta == Approx(0.049f).margin(0.001f));
    CHECK_FALSE(cmp.isImprovement);
}

TEST_CASE("BenchmarkComparison: JSON serialization", "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.80f;
    baseline.recallAtK = 0.70f;
    baseline.latency.meanMs = 50.0;

    BenchmarkResults current;
    current.mrr = 0.65f;
    current.recallAtK = 0.55f;
    current.latency.meanMs = 75.0;

    auto cmp = InternalBenchmark::compare(baseline, current);
    auto json = cmp.toJson();

    CHECK(json["mrr_delta"].get<float>() == Approx(-0.15f));
    CHECK(json["recall_delta"].get<float>() == Approx(-0.15f));
    CHECK(json["latency_delta_ms"].get<float>() == Approx(25.0f));
    CHECK(json["is_regression"] == true);
    CHECK(json["is_improvement"] == false);
    CHECK_FALSE(json["summary"].get<std::string>().empty());
}

// =============================================================================
// QueryGeneratorConfig tests
// =============================================================================

TEST_CASE("QueryGeneratorConfig: default values", "[unit][internal_benchmark][config]") {
    QueryGeneratorConfig config;

    CHECK(config.queryCount == 100);
    CHECK(config.minPhraseLength == 3);
    CHECK(config.maxPhraseLength == 8);
    CHECK(config.minDocumentLength == 100);
    CHECK(config.knownItemRatio == Approx(0.5f));
    CHECK(config.tfidfRatio == Approx(0.3f));
    CHECK(config.semanticRatio == Approx(0.2f));
    CHECK(config.seed == 42);
    CHECK(config.excludeBinaryFiles == true);
    CHECK(config.preferCodeFiles == false);
}

TEST_CASE("QueryGeneratorConfig: ratios sum to 1.0", "[unit][internal_benchmark][config]") {
    QueryGeneratorConfig config;
    float sum = config.knownItemRatio + config.tfidfRatio + config.semanticRatio;

    CHECK(sum == Approx(1.0f));
}

// =============================================================================
// BenchmarkConfig tests
// =============================================================================

TEST_CASE("BenchmarkConfig: default values", "[unit][internal_benchmark][config]") {
    BenchmarkConfig config;

    CHECK(config.queryCount == 100);
    CHECK(config.k == 10);
    CHECK(config.warmupQueries == 5);
    CHECK(config.includeExecutions == false);
    CHECK(config.verbose == false);
    CHECK(config.regressionThreshold == Approx(0.05f));
    CHECK(config.improvementThreshold == Approx(0.05f));
}

// =============================================================================
// QueryExecution tests
// =============================================================================

TEST_CASE("QueryExecution: default state", "[unit][internal_benchmark][execution]") {
    QueryExecution exec;

    CHECK(exec.reciprocalRank == 0);
    CHECK_FALSE(exec.foundInTopK);
    CHECK(exec.latencyMs == 0.0);
    CHECK(exec.error.empty());
    CHECK(exec.retrievedHashes.empty());
}

// =============================================================================
// Edge cases
// =============================================================================

TEST_CASE("LatencyStats: large sample set (1000 samples)", "[unit][internal_benchmark][latency]") {
    std::vector<double> samples;
    samples.reserve(1000);

    // Create samples with known distribution
    for (int i = 0; i < 1000; ++i) {
        samples.push_back(static_cast<double>(i + 1));
    }

    auto stats = LatencyStats::compute(samples);

    CHECK(stats.sampleCount == 1000);
    CHECK(stats.minMs == 1.0);
    CHECK(stats.maxMs == 1000.0);
    CHECK(stats.meanMs == Approx(500.5));
}

TEST_CASE("LatencyStats: all identical values", "[unit][internal_benchmark][latency]") {
    std::vector<double> samples(100, 42.0);
    auto stats = LatencyStats::compute(samples);

    CHECK(stats.minMs == 42.0);
    CHECK(stats.maxMs == 42.0);
    CHECK(stats.meanMs == 42.0);
    CHECK(stats.medianMs == 42.0);
    CHECK(stats.p95Ms == 42.0);
    CHECK(stats.p99Ms == 42.0);
    CHECK(stats.stddevMs == 0.0);
}

TEST_CASE("BenchmarkComparison: large improvements are still improvements",
          "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.1f;

    BenchmarkResults current;
    current.mrr = 0.9f; // +0.8, huge improvement

    auto cmp = InternalBenchmark::compare(baseline, current);

    CHECK(cmp.mrrDelta == Approx(0.8f));
    CHECK(cmp.isImprovement);
    CHECK_FALSE(cmp.isRegression);
}

TEST_CASE("BenchmarkComparison: zero baseline MRR", "[unit][internal_benchmark][compare]") {
    BenchmarkResults baseline;
    baseline.mrr = 0.0f;

    BenchmarkResults current;
    current.mrr = 0.5f;

    auto cmp = InternalBenchmark::compare(baseline, current);

    CHECK(cmp.mrrDelta == Approx(0.5f));
    CHECK(cmp.isImprovement);
}
