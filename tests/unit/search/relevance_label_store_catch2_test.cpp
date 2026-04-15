#include <yams/search/relevance_label_store.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#ifndef _WIN32
#include <unistd.h>
#endif

using yams::search::LabeledQuery;
using yams::search::RelevanceLabel;
using yams::search::RelevanceLabelStore;
using yams::search::RelevanceSession;

namespace {

std::filesystem::path uniqueTempFile(const char* suffix) {
    auto name = std::string("yams_relevance_labels_test_") +
                std::to_string(static_cast<long long>(
                    std::chrono::steady_clock::now().time_since_epoch().count())) +
                "_" + std::to_string(static_cast<long long>(::getpid())) + suffix;
    return std::filesystem::temp_directory_path() / name;
}

RelevanceSession makeSession(const std::string& hash, double reward) {
    RelevanceSession s;
    s.timestamp = "2026-04-15T12:00:00Z";
    s.configHash = hash;
    s.source = "interactive";
    s.k = 5;
    s.corpusEpoch = 42;
    s.topologyEpoch = 7;
    LabeledQuery q;
    q.queryText = "alpha beta";
    q.rankedDocHashes = {"h1", "h2", "h3"};
    q.labels = {RelevanceLabel::Relevant, RelevanceLabel::NotRelevant, RelevanceLabel::Unknown};
    q.reward = reward;
    s.queries.push_back(std::move(q));
    return s;
}

} // namespace

TEST_CASE("RelevanceLabelStore: append and readRecent round-trip", "[search][relevance][store]") {
    auto path = uniqueTempFile(".jsonl");
    std::filesystem::remove(path);

    RelevanceLabelStore store(path);
    REQUIRE(store.append(makeSession("h-a", 0.25)).has_value());
    REQUIRE(store.append(makeSession("h-b", 0.75)).has_value());

    auto readback = store.readRecent(10);
    REQUIRE(readback.has_value());
    REQUIRE(readback.value().size() == 2);

    const auto& first = readback.value()[0];
    CHECK(first.configHash == "h-a");
    CHECK(first.source == "interactive");
    CHECK(first.k == 5);
    REQUIRE(first.corpusEpoch.has_value());
    CHECK(*first.corpusEpoch == 42);
    REQUIRE(first.topologyEpoch.has_value());
    CHECK(*first.topologyEpoch == 7);
    REQUIRE(first.queries.size() == 1);
    const auto& q = first.queries[0];
    CHECK(q.queryText == "alpha beta");
    REQUIRE(q.rankedDocHashes.size() == 3);
    CHECK(q.rankedDocHashes[0] == "h1");
    REQUIRE(q.labels.size() == 3);
    CHECK(q.labels[0] == RelevanceLabel::Relevant);
    CHECK(q.labels[1] == RelevanceLabel::NotRelevant);
    CHECK(q.labels[2] == RelevanceLabel::Unknown);
    CHECK(q.reward == 0.25);

    std::filesystem::remove(path);
}

TEST_CASE("RelevanceLabelStore: readRecent honors limit and newest-last order",
          "[search][relevance][store]") {
    auto path = uniqueTempFile(".jsonl");
    std::filesystem::remove(path);
    RelevanceLabelStore store(path);

    for (int i = 0; i < 5; ++i) {
        REQUIRE(store.append(makeSession(std::to_string(i), static_cast<double>(i) / 10.0))
                    .has_value());
    }

    auto last2 = store.readRecent(2);
    REQUIRE(last2.has_value());
    REQUIRE(last2.value().size() == 2);
    CHECK(last2.value()[0].configHash == "3");
    CHECK(last2.value()[1].configHash == "4");

    std::filesystem::remove(path);
}

TEST_CASE("RelevanceLabelStore: readByConfigHash filters", "[search][relevance][store]") {
    auto path = uniqueTempFile(".jsonl");
    std::filesystem::remove(path);
    RelevanceLabelStore store(path);

    REQUIRE(store.append(makeSession("cfg-A", 0.1)).has_value());
    REQUIRE(store.append(makeSession("cfg-B", 0.9)).has_value());
    REQUIRE(store.append(makeSession("cfg-A", 0.5)).has_value());

    auto hits = store.readByConfigHash("cfg-A", 10);
    REQUIRE(hits.has_value());
    REQUIRE(hits.value().size() == 2);
    CHECK(hits.value()[0].configHash == "cfg-A");
    CHECK(hits.value()[1].configHash == "cfg-A");

    std::filesystem::remove(path);
}

TEST_CASE("RelevanceLabelStore: missing file returns empty", "[search][relevance][store]") {
    auto path = uniqueTempFile(".jsonl");
    std::filesystem::remove(path);

    RelevanceLabelStore store(path);
    auto rows = store.readRecent(10);
    REQUIRE(rows.has_value());
    CHECK(rows.value().empty());
}

TEST_CASE("RelevanceLabelStore: malformed lines are skipped, not fatal",
          "[search][relevance][store]") {
    auto path = uniqueTempFile(".jsonl");
    {
        std::ofstream out(path);
        out << "this is not json {{{\n";
        out << R"({"timestamp":"2026-04-15T00:00:00Z","config_hash":"valid","source":"interactive","k":5,"queries":[]})"
            << "\n";
        out << "garbage again\n";
    }

    RelevanceLabelStore store(path);
    auto rows = store.readRecent(10);
    REQUIRE(rows.has_value());
    REQUIRE(rows.value().size() == 1);
    CHECK(rows.value()[0].configHash == "valid");

    REQUIRE(store.append(makeSession("recovered", 0.3)).has_value());
    auto after = store.readRecent(10);
    REQUIRE(after.has_value());
    REQUIRE(after.value().size() == 2);
    CHECK(after.value()[1].configHash == "recovered");

    std::filesystem::remove(path);
}

TEST_CASE("RelevanceLabelStore: clear removes file", "[search][relevance][store]") {
    auto path = uniqueTempFile(".jsonl");
    RelevanceLabelStore store(path);
    REQUIRE(store.append(makeSession("x", 0.0)).has_value());
    REQUIRE(std::filesystem::exists(path));

    REQUIRE(store.clear().has_value());
    CHECK_FALSE(std::filesystem::exists(path));
}

TEST_CASE("RelevanceSession::meanReward averages queries", "[search][relevance][store]") {
    RelevanceSession s;
    CHECK(s.meanReward() == 0.0);

    LabeledQuery a;
    a.reward = 0.2;
    LabeledQuery b;
    b.reward = 0.8;
    s.queries.push_back(a);
    s.queries.push_back(b);
    CHECK(s.meanReward() == 0.5);
}
