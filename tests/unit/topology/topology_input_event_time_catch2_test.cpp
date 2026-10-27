// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Typed event time and session on topology inputs. The extractor reads the documented
// `event_time` metadata key (ISO-8601 date / datetime or integer epoch seconds) ahead of the
// document's createdTime and `session_id` for session grouping. Nothing downstream consumes
// the fields yet: artifacts, codecs and fingerprints must not change.

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_input_extractor.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::metadata;
using namespace yams::topology;

namespace {

constexpr std::int64_t kCreated = 1600000000;        // 2020-09-13T12:26:40Z
constexpr std::int64_t kMay20 = 1684540800;          // 2023-05-20T00:00:00Z
constexpr std::int64_t kMay20At0221 = kMay20 + 8460; // 2023-05-20T02:21:00Z

std::filesystem::path tempDbPath(const char* prefix) {
    const auto configured = yams::config::getenv_optional("YAMS_TEST_TMPDIR");
    auto base = configured && !configured->empty() ? std::filesystem::path(*configured)
                                                   : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

struct ExtractorFixture {
    ExtractorFixture() {
        dbPath = tempDbPath("topology_event_time_");
        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repository = std::make_shared<MetadataRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());
    }

    ~ExtractorFixture() {
        kgStore.reset();
        repository.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    /// Insert a document with the given createdTime (0 = unset) and metadata rows.
    void addDocument(const std::string& hash, std::int64_t createdUnix,
                     const std::map<std::string, std::string>& metadata) {
        DocumentInfo info;
        const std::string path = "/repo/" + hash + ".md";
        info.fileName = hash + ".md";
        info.fileExtension = ".md";
        info.fileSize = 42;
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        if (createdUnix != 0) {
            info.setCreatedTime(createdUnix);
            info.setModifiedTime(createdUnix);
            info.setIndexedTime(createdUnix);
        }
        auto derived = computePathDerivedValues(path);
        info.filePath = derived.normalizedPath;
        info.pathPrefix = derived.pathPrefix;
        info.reversePath = derived.reversePath;
        info.pathHash = derived.pathHash;
        info.parentHash = derived.parentHash;
        info.pathDepth = derived.pathDepth;
        auto id = repository->insertDocument(info);
        REQUIRE(id.has_value());
        for (const auto& [key, value] : metadata) {
            REQUIRE(repository->setMetadata(id.value(), key, MetadataValue(value)).has_value());
        }
        KGNode node;
        node.nodeKey = "doc:" + hash;
        node.label = hash;
        node.type = std::string{"document"};
        REQUIRE(kgStore->upsertNode(node).has_value());
    }

    std::map<std::string, TopologyDocumentInput> extract(bool includeMetadata,
                                                         TopologyExtractionStats* stats = nullptr) {
        TopologyInputExtractor extractor(repository, kgStore, nullptr);
        TopologyExtractionConfig config;
        config.limit = 32;
        config.includeEmbeddings = false;
        config.includeMetadata = includeMetadata;
        auto extracted = extractor.extract(config, stats);
        REQUIRE(extracted.has_value());
        std::map<std::string, TopologyDocumentInput> byHash;
        for (auto& doc : extracted.value()) {
            byHash[doc.documentHash] = std::move(doc);
        }
        return byHash;
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repository;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

std::vector<TopologyDocumentInput> twoClusterInputs() {
    // Two reciprocal pairs on orthogonal axes plus a weak cross link so roads exist.
    return {
        TopologyDocumentInput{
            .documentHash = "a1",
            .filePath = "/repo/a1.md",
            .embedding = {1.0F, 0.0F},
            .neighbors = {{.documentHash = "a2", .score = 0.9F, .reciprocal = true},
                          {.documentHash = "b1", .score = 0.2F}}},
        TopologyDocumentInput{
            .documentHash = "a2",
            .filePath = "/repo/a2.md",
            .embedding = {0.9F, 0.1F},
            .neighbors = {{.documentHash = "a1", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "b1",
            .filePath = "/repo/b1.md",
            .embedding = {0.0F, 1.0F},
            .neighbors = {{.documentHash = "b2", .score = 0.8F, .reciprocal = true},
                          {.documentHash = "a1", .score = 0.2F}}},
        TopologyDocumentInput{
            .documentHash = "b2",
            .filePath = "/repo/b2.md",
            .embedding = {0.1F, 0.9F},
            .neighbors = {{.documentHash = "b1", .score = 0.8F, .reciprocal = true}}},
    };
}

} // namespace

TEST_CASE("event_time parser accepts epoch seconds and ISO-8601 dates and datetimes",
          "[unit][topology][extractor][event-time]") {
    CHECK(parseTopologyEventTime("1684549260") == std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("  1684549260\n") == std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("0") == std::optional<std::int64_t>{0});
    CHECK(parseTopologyEventTime("-86400") == std::optional<std::int64_t>{-86400});

    CHECK(parseTopologyEventTime("2023-05-20") == std::optional<std::int64_t>{kMay20});
    CHECK(parseTopologyEventTime("2023-05-20T02:21") == std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-20T02:21:00") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-20 02:21:00") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-20T02:21:00Z") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-20T02:21:00.250Z") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-20T04:21:00+02:00") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-19T21:21:00-05:00") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("2023-05-20T04:21:00+0200") ==
          std::optional<std::int64_t>{kMay20At0221});
    CHECK(parseTopologyEventTime("1970-01-01") == std::optional<std::int64_t>{0});
    CHECK(parseTopologyEventTime("2024-02-29").has_value());
}

TEST_CASE("event_time parser rejects malformed values", "[unit][topology][extractor][event-time]") {
    for (const char* bad :
         {"", "   ", "yesterday", "12abc", "2023-13-01", "2023-00-10", "2023-02-30", "2023-02-29",
          "2023-05-20X", "2023-05-20T25:00", "2023-05-20T02:61", "2023-05-20T02:21:00+25:00",
          "2023-05-20T02:21:00Q", "2023/05/20", "1e9", "99999999999999999999"}) {
        INFO("value='" << bad << "'");
        CHECK_FALSE(parseTopologyEventTime(bad).has_value());
    }
}

TEST_CASE("Topology extractor derives event time and session from documented metadata keys",
          "[unit][topology][extractor][event-time]") {
    ExtractorFixture fix;
    fix.addDocument("iso", kCreated,
                    {{"event_time", "2023-05-20T02:21:00Z"}, {"session_id", "sess-1"}});
    fix.addDocument("epoch", kCreated, {{"event_time", "1684549260"}, {"session_id", "sess-1"}});
    fix.addDocument("date", kCreated, {{"event_time", "2023-05-20"}});
    fix.addDocument("created", kCreated, {{"other", "x"}});
    fix.addDocument("bad", kCreated, {{"event_time", "not a date"}, {"session_id", ""}});
    fix.addDocument("nocreated", 0, {});

    TopologyExtractionStats stats;
    const auto docs = fix.extract(/*includeMetadata=*/true, &stats);
    REQUIRE(docs.size() == 6U);

    SECTION("metadata event_time wins over createdTime, in either spelling") {
        CHECK(docs.at("iso").eventTime == std::optional<std::int64_t>{kMay20At0221});
        CHECK(docs.at("epoch").eventTime == std::optional<std::int64_t>{kMay20At0221});
        CHECK(docs.at("date").eventTime == std::optional<std::int64_t>{kMay20});
    }
    SECTION("createdTime is the fallback; a zero createdTime leaves it unset") {
        CHECK(docs.at("created").eventTime == std::optional<std::int64_t>{kCreated});
        CHECK_FALSE(docs.at("nocreated").eventTime.has_value());
    }
    SECTION("an unparsable event_time is ignored, not an error") {
        CHECK(docs.at("bad").eventTime == std::optional<std::int64_t>{kCreated});
        CHECK(stats.eventTimesInvalid == 1U);
    }
    SECTION("session_id is copied verbatim and empty when absent") {
        CHECK(docs.at("iso").sessionId == "sess-1");
        CHECK(docs.at("epoch").sessionId == "sess-1");
        CHECK(docs.at("created").sessionId.empty());
        CHECK(docs.at("bad").sessionId.empty());
    }
    SECTION("the raw metadata map still carries the keys") {
        CHECK(docs.at("iso").metadata.at("event_time") == "2023-05-20T02:21:00Z");
        CHECK(docs.at("iso").metadata.at("session_id") == "sess-1");
    }
    SECTION("stats count the sources") {
        CHECK(stats.eventTimesFromMetadata == 3U);
        CHECK(stats.eventTimesFromCreatedTime == 2U);
        CHECK(stats.sessionIdsLoaded == 2U);
    }
}

TEST_CASE("Topology extractor falls back to createdTime when metadata is not loaded",
          "[unit][topology][extractor][event-time]") {
    ExtractorFixture fix;
    fix.addDocument("iso", kCreated,
                    {{"event_time", "2023-05-20T02:21:00Z"}, {"session_id", "sess-1"}});

    TopologyExtractionStats stats;
    const auto docs = fix.extract(/*includeMetadata=*/false, &stats);
    REQUIRE(docs.contains("iso"));
    CHECK(docs.at("iso").eventTime == std::optional<std::int64_t>{kCreated});
    CHECK(docs.at("iso").sessionId.empty());
    CHECK(docs.at("iso").metadata.empty());
    CHECK(stats.eventTimesFromMetadata == 0U);
    CHECK(stats.eventTimesFromCreatedTime == 1U);
}

TEST_CASE("Event time and session do not reach artifacts, codecs or fingerprints",
          "[unit][topology][extractor][event-time][codec][fingerprint]") {
    auto plain = twoClusterInputs();
    auto typed = twoClusterInputs();
    std::int64_t t = kMay20;
    for (auto& doc : typed) {
        doc.eventTime = t;
        t += 3600;
        doc.sessionId = doc.documentHash.front() == 'a' ? "sess-a" : "sess-b";
    }

    TopologyBuildConfig config;
    config.mode = TopologyBuildMode::Exact;
    config.roadMaxPerCluster = 4;
    config.embeddingSpaceIdentity = "test-space";

    ConnectedComponentTopologyEngine engine;
    auto plainBatch = engine.buildArtifacts(plain, config);
    auto typedBatch = engine.buildArtifacts(typed, config);
    REQUIRE(plainBatch.has_value());
    REQUIRE(typedBatch.has_value());
    REQUIRE(plainBatch.value().clusters.size() == 2U);
    REQUIRE_FALSE(plainBatch.value().roads.empty());

    // The two builds differ only by wall-clock stamps; align them before comparing bytes.
    typedBatch.value().snapshotId = plainBatch.value().snapshotId;
    typedBatch.value().generatedAtUnixSeconds = plainBatch.value().generatedAtUnixSeconds;
    typedBatch.value().topologyEpoch = plainBatch.value().topologyEpoch;

    auto plainBinary = serializeTopologyBatchBinary(plainBatch.value());
    auto typedBinary = serializeTopologyBatchBinary(typedBatch.value());
    REQUIRE(plainBinary.has_value());
    REQUIRE(typedBinary.has_value());
    CHECK(plainBinary.value() == typedBinary.value());

    CHECK(yams::search::topologyRoutingConstructionFingerprint(plainBatch.value()) ==
          yams::search::topologyRoutingConstructionFingerprint(typedBatch.value()));
    CHECK(yams::search::topologyRoadFingerprint(plainBatch.value()) ==
          yams::search::topologyRoadFingerprint(typedBatch.value()));
    CHECK(plainBatch.value().roads == typedBatch.value().roads);
}
