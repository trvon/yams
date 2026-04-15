#include <yams/search/benchmark_history_store.h>

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>

using yams::search::BenchmarkHistoryStore;
using yams::search::BenchmarkResults;

namespace {

std::filesystem::path uniqueTempFile(const char* suffix) {
    auto name = std::string("yams_bench_hist_test_") +
                std::to_string(static_cast<long long>(
                    std::chrono::steady_clock::now().time_since_epoch().count())) +
                "_" + std::to_string(static_cast<long long>(::getpid())) + suffix;
    return std::filesystem::temp_directory_path() / name;
}

BenchmarkResults makeResults(float mrr, std::size_t queries) {
    BenchmarkResults r;
    r.mrr = mrr;
    r.recallAtK = 0.75f;
    r.precisionAtK = 0.5f;
    r.k = 10;
    r.queriesRun = queries;
    r.queriesSucceeded = queries;
    r.queriesFailed = 0;
    r.latency.meanMs = 12.5;
    r.latency.medianMs = 10.0;
    r.latency.p95Ms = 25.0;
    r.latency.p99Ms = 30.0;
    r.latency.maxMs = 31.0;
    r.latency.minMs = 3.0;
    r.totalTime = std::chrono::milliseconds{1234};
    r.timestamp = "2026-04-15T12:00:00Z";
    r.tuningState = "EFFICIENT";
    r.tunedParams = nlohmann::json{{"kg_weight", 0.8}};
    return r;
}

} // namespace

TEST_CASE("BenchmarkHistoryStore: append and read round-trip", "[search][benchmark][history]") {
    auto path = uniqueTempFile(".json");
    std::filesystem::remove(path);

    BenchmarkHistoryStore store(path);

    BenchmarkHistoryStore::Row row1;
    row1.results = makeResults(0.42f, 10);
    row1.configHash = "10-10-100";
    row1.lexicalEpoch = 7;
    row1.topologyEpoch = 3;

    BenchmarkHistoryStore::Row row2;
    row2.results = makeResults(0.55f, 20);
    row2.configHash = "20-10-100";

    REQUIRE(store.append(row1).has_value());
    REQUIRE(store.append(row2).has_value());

    auto readback = store.read(10);
    REQUIRE(readback.has_value());
    REQUIRE(readback.value().size() == 2);

    const auto& first = readback.value()[0];
    CHECK(first.results.mrr == row1.results.mrr);
    CHECK(first.results.queriesRun == row1.results.queriesRun);
    CHECK(first.configHash == row1.configHash);
    REQUIRE(first.lexicalEpoch.has_value());
    CHECK(*first.lexicalEpoch == 7);
    REQUIRE(first.topologyEpoch.has_value());
    CHECK(*first.topologyEpoch == 3);
    REQUIRE(first.results.tuningState.has_value());
    CHECK(*first.results.tuningState == "EFFICIENT");
    REQUIRE(first.results.tunedParams.has_value());
    CHECK((*first.results.tunedParams).at("kg_weight").get<double>() == 0.8);

    const auto& second = readback.value()[1];
    CHECK(second.results.mrr == row2.results.mrr);
    CHECK(second.configHash == row2.configHash);
    CHECK_FALSE(second.lexicalEpoch.has_value());

    std::filesystem::remove(path);
}

TEST_CASE("BenchmarkHistoryStore: read honors limit", "[search][benchmark][history]") {
    auto path = uniqueTempFile(".json");
    std::filesystem::remove(path);
    BenchmarkHistoryStore store(path);

    for (int i = 0; i < 5; ++i) {
        BenchmarkHistoryStore::Row row;
        row.results = makeResults(static_cast<float>(i) / 10.0f, static_cast<std::size_t>(i + 1));
        row.configHash = std::to_string(i);
        REQUIRE(store.append(row).has_value());
    }

    auto last2 = store.read(2);
    REQUIRE(last2.has_value());
    REQUIRE(last2.value().size() == 2);
    CHECK(last2.value()[0].configHash == "3");
    CHECK(last2.value()[1].configHash == "4");

    std::filesystem::remove(path);
}

TEST_CASE("BenchmarkHistoryStore: read of missing file returns empty",
          "[search][benchmark][history]") {
    auto path = uniqueTempFile(".json");
    std::filesystem::remove(path);

    BenchmarkHistoryStore store(path);
    auto rows = store.read(10);
    REQUIRE(rows.has_value());
    CHECK(rows.value().empty());
}

TEST_CASE("BenchmarkHistoryStore: corrupt file is treated as empty, not fatal",
          "[search][benchmark][history]") {
    auto path = uniqueTempFile(".json");
    {
        std::ofstream out(path);
        out << "this is not json {{{";
    }

    BenchmarkHistoryStore store(path);
    auto rows = store.read(10);
    REQUIRE(rows.has_value());
    CHECK(rows.value().empty());

    BenchmarkHistoryStore::Row row;
    row.results = makeResults(0.1f, 1);
    row.configHash = "recovered";
    REQUIRE(store.append(row).has_value());

    auto after = store.read(10);
    REQUIRE(after.has_value());
    REQUIRE(after.value().size() == 1);
    CHECK(after.value()[0].configHash == "recovered");

    std::filesystem::remove(path);
}

TEST_CASE("BenchmarkHistoryStore: clear removes file", "[search][benchmark][history]") {
    auto path = uniqueTempFile(".json");
    BenchmarkHistoryStore store(path);
    BenchmarkHistoryStore::Row row;
    row.results = makeResults(0.3f, 1);
    row.configHash = "x";
    REQUIRE(store.append(row).has_value());
    REQUIRE(std::filesystem::exists(path));

    REQUIRE(store.clear().has_value());
    CHECK_FALSE(std::filesystem::exists(path));
}
