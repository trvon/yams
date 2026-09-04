#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include "src/search/search_vector_pipeline_internal.h"
#include "tests/common/test_helpers_catch2.h"
#include <yams/vector/vector_database.h>

using yams::search::SearchEngineConfig;

TEST_CASE("Vector pipeline over-fetches chunks for aggregating unique docs",
          "[search][vector][catch2]") {
    SearchEngineConfig cfg;
    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::WEIGHTED_TOP_K_AVG;
    cfg.chunkAggregationTopK = 3;

    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, false) == 30U);
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, true) == 30U);

    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, false) == 10U);
}

TEST_CASE("Vector pipeline propagates exact routed work diagnostics",
          "[search][vector][topology][catch2]") {
    yams::test::ScopedEnvVar enableVectors("YAMS_DISABLE_VECTORS", std::string("0"));
    yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT", std::string("0"));

    yams::vector::VectorDatabaseConfig dbConfig;
    dbConfig.database_path = ":memory:";
    dbConfig.embedding_dim = 2;
    dbConfig.use_in_memory = true;
    auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(dbConfig);
    REQUIRE(vectorDb->initialize());

    for (const auto& [hash, embedding] : std::vector<std::pair<std::string, std::vector<float>>>{
             {"a", {1.0F, 0.0F}}, {"b", {0.8F, 0.2F}}, {"c", {0.0F, 1.0F}}}) {
        yams::vector::VectorRecord record;
        record.chunk_id = "chunk-" + hash;
        record.document_hash = hash;
        record.embedding = embedding;
        record.level = yams::vector::EmbeddingLevel::DOCUMENT;
        REQUIRE(vectorDb->insertVector(record));
    }

    SearchEngineConfig cfg;
    cfg.similarityThreshold = -1.0F;
    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
    yams::vector::VectorSearchDiagnostics diagnostics;
    const std::unordered_set<std::string> routed{"a", "b"};

    const auto result = yams::search::detail::queryVectorIndexPipeline(
        nullptr, vectorDb, {1.0F, 0.0F}, cfg, 2, routed, yams::vector::CandidateFilterMode::Exact,
        &diagnostics);

    REQUIRE(result.has_value());
    CHECK(result.value().size() == 2U);
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.rowsVisited == 2U);
    CHECK(diagnostics.exactDistanceEvaluations == 2U);
}

TEST_CASE("Vector pipeline scores every admitted document before exact-control truncation",
          "[search][vector][topology][candidate-rescue][catch2]") {
    yams::test::ScopedEnvVar enableVectors("YAMS_DISABLE_VECTORS", std::string("0"));
    yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT", std::string("0"));

    yams::vector::VectorDatabaseConfig dbConfig;
    dbConfig.database_path = ":memory:";
    dbConfig.embedding_dim = 2;
    dbConfig.use_in_memory = true;
    auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(dbConfig);
    REQUIRE(vectorDb->initialize());

    for (const auto& [chunkId, hash, embedding] :
         std::vector<std::tuple<std::string, std::string, std::vector<float>>>{
             {"crowding-1", "crowding", {1.0F, 0.0F}},
             {"crowding-2", "crowding", {0.9F, 0.1F}},
             {"rescued-1", "rescued", {0.6F, 0.8F}}}) {
        yams::vector::VectorRecord record;
        record.chunk_id = chunkId;
        record.document_hash = hash;
        record.embedding = embedding;
        record.level = yams::vector::EmbeddingLevel::CHUNK;
        REQUIRE(vectorDb->insertVector(record));
    }

    SearchEngineConfig cfg;
    cfg.similarityThreshold = 0.7F;
    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
    const std::unordered_set<std::string> routed{"crowding", "rescued"};

    const auto bounded = yams::search::detail::queryVectorIndexPipeline(
        nullptr, vectorDb, {1.0F, 0.0F}, cfg, 2, routed, yams::vector::CandidateFilterMode::Exact);
    REQUIRE(bounded.has_value());
    REQUIRE(bounded.value().size() == 1U);
    CHECK(bounded.value()[0].documentHash == "crowding");

    yams::vector::VectorSearchDiagnostics diagnostics;
    const auto documentComplete = yams::search::detail::queryVectorIndexPipeline(
        nullptr, vectorDb, {1.0F, 0.0F}, cfg, 2, routed,
        yams::vector::CandidateFilterMode::ExactDocumentComplete, &diagnostics);

    REQUIRE(documentComplete.has_value());
    REQUIRE(documentComplete.value().size() == 2U);
    CHECK(documentComplete.value()[0].documentHash == "crowding");
    CHECK(documentComplete.value()[1].documentHash == "rescued");
    CHECK(documentComplete.value()[1].score == Catch::Approx(0.6F));
    CHECK(diagnostics.rowsVisited == 3U);
    CHECK(diagnostics.exactDistanceEvaluations == 3U);
    CHECK(diagnostics.returnedRows == 3U);
}

TEST_CASE("Vector pipeline filters global hits by routed membership with zero-overlap fallback",
          "[search][vector][topology][narrowing][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> global{{.documentHash = "distractor", .score = 0.95F, .rank = 0},
                                        {.documentHash = "routed-a", .score = 0.90F, .rank = 1},
                                        {.documentHash = "routed-b", .score = 0.80F, .rank = 2}};

    auto filtered = yams::search::detail::filterVectorResultsByAllowedDocuments(
        global, std::unordered_set<std::string>{"routed-a", "routed-b"});

    REQUIRE(filtered.applied);
    CHECK_FALSE(filtered.fellBackToGlobal);
    CHECK(filtered.matched == 2U);
    CHECK(filtered.removed == 1U);
    REQUIRE(filtered.results.size() == 2U);
    CHECK(filtered.results[0].documentHash == "routed-a");
    CHECK(filtered.results[0].rank == 0U);
    CHECK(filtered.results[0].debugInfo.at("topology_route_filter") == "1");
    CHECK(filtered.results[1].documentHash == "routed-b");
    CHECK(filtered.results[1].rank == 1U);

    auto fallback = yams::search::detail::filterVectorResultsByAllowedDocuments(
        std::move(global), std::unordered_set<std::string>{"route-miss"});
    CHECK_FALSE(fallback.applied);
    REQUIRE(fallback.fellBackToGlobal);
    CHECK(fallback.matched == 0U);
    CHECK(fallback.removed == 0U);
    REQUIRE(fallback.results.size() == 3U);
    CHECK(fallback.results[0].documentHash == "distractor");
}

TEST_CASE("Vector pipeline candidate rescue preserves baseline and appends novel routed hits",
          "[search][vector][candidate-rescue][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> baseline{
        {.documentHash = "baseline-a", .score = 0.90F, .rank = 0},
        {.documentHash = "shared", .score = 0.70F, .rank = 1},
    };
    std::vector<ComponentResult> expansion{
        {.documentHash = "rescued-hash",
         .filePath = "/corpus/rescued-doc.txt",
         .score = 0.95F,
         .rank = 0},
        {.documentHash = "shared", .score = 0.70F, .rank = 1},
    };

    auto merged = yams::search::detail::mergeVectorCandidateRescues(std::move(baseline),
                                                                    std::move(expansion));

    CHECK(merged.added == 1U);
    CHECK(merged.novelDocuments == 1U);
    CHECK(merged.evidenceRescues == 0U);
    CHECK(merged.duplicates == 1U);
    CHECK(merged.novelDocumentHashes == std::vector<std::string>{"rescued-hash"});
    CHECK(merged.novelDocumentIds == std::vector<std::string>{"rescued-doc"});
    CHECK(merged.evidenceRescueDocumentHashes.empty());
    CHECK(merged.evidenceRescueDocumentIds.empty());
    REQUIRE(merged.addedDocumentHashes.size() == 1U);
    CHECK(merged.addedDocumentHashes[0] == "rescued-hash");
    REQUIRE(merged.addedDocumentIds.size() == 1U);
    CHECK(merged.addedDocumentIds[0] == "rescued-doc");
    REQUIRE(merged.results.size() == 3U);
    CHECK(merged.results[0].documentHash == "rescued-hash");
    CHECK(merged.results[0].rank == 0U);
    CHECK(merged.results[0].debugInfo.at("candidate_rescue") == "1");
    CHECK(merged.results[1].documentHash == "baseline-a");
    CHECK(merged.results[1].rank == 1U);
    CHECK(merged.results[2].documentHash == "shared");
    CHECK(merged.results[2].rank == 2U);
}

TEST_CASE("Vector pipeline candidate rescue enriches an existing lexical candidate",
          "[search][vector][candidate-rescue][evidence][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> baseline{
        {.documentHash = "global-vector", .score = 0.90F, .rank = 0},
    };
    std::vector<ComponentResult> expansion{
        {.documentHash = "buried-lexical",
         .filePath = "/corpus/buried-lexical.txt",
         .score = 0.80F,
         .rank = 0},
    };

    auto merged = yams::search::detail::mergeVectorCandidateRescues(
        std::move(baseline), std::move(expansion),
        std::unordered_set<std::string>{"buried-lexical"});

    CHECK(merged.added == 1U);
    CHECK(merged.novelDocuments == 0U);
    CHECK(merged.evidenceRescues == 1U);
    CHECK(merged.duplicates == 0U);
    CHECK(merged.novelDocumentHashes.empty());
    CHECK(merged.novelDocumentIds.empty());
    CHECK(merged.evidenceRescueDocumentHashes == std::vector<std::string>{"buried-lexical"});
    CHECK(merged.evidenceRescueDocumentIds == std::vector<std::string>{"buried-lexical"});
    REQUIRE(merged.results.size() == 2U);
    CHECK(merged.results[1].documentHash == "buried-lexical");
    CHECK(merged.results[1].debugInfo.at("candidate_rescue") == "1");
    CHECK(merged.results[1].debugInfo.at("candidate_rescue_kind") == "evidence");
}

TEST_CASE("Vector pipeline merges auxiliary candidates with stable policy counters",
          "[search][vector][auxiliary][catch2]") {
    using yams::search::ComponentResult;
    using yams::search::detail::AuxiliaryVectorCandidateBatch;

    SearchEngineConfig config;
    config.vectorMaxResults = 3;
    config.multiVectorScoreDecay = 0.5F;
    config.graphExpansionVectorPenalty = 0.5F;
    config.graphVectorRequireCorroboration = true;
    config.graphVectorRequireTextAnchoring = true;
    config.graphVectorRequireBaselineTextAnchoring = true;

    std::vector<ComponentResult> components{
        {.documentHash = "anchor", .score = 0.7F, .source = ComponentResult::Source::Text},
        {.documentHash = "base-b", .score = 0.6F, .source = ComponentResult::Source::Vector},
        {.documentHash = "base-a", .score = 0.6F, .source = ComponentResult::Source::Vector},
        {.documentHash = "base-a", .score = 0.8F, .source = ComponentResult::Source::EntityVector},
    };
    std::vector<AuxiliaryVectorCandidateBatch> phrases{
        {.query = "first",
         .candidates = {{.documentHash = "phrase-new", .score = 1.2F},
                        {.documentHash = "base-b", .score = 1.4F}}},
        {.query = "second",
         .candidates = {{.documentHash = "base-b", .score = 1.6F},
                        {.documentHash = "tie-z", .score = 1.0F},
                        {.documentHash = "tie-a", .score = 1.0F}}},
    };
    std::vector<AuxiliaryVectorCandidateBatch> graphTerms{
        {.query = "blocked",
         .weight = 1.0F,
         .candidates = {{.documentHash = "uncorroborated", .score = 1.0F}}},
        {.query = "allowed",
         .weight = 0.8F,
         .candidates = {{.documentHash = "anchor", .score = 1.0F},
                        {.documentHash = "anchor", .score = 1.2F}}},
    };

    auto merged = yams::search::detail::mergeAuxiliaryVectorCandidates(
        std::move(components), std::move(phrases), std::move(graphTerms), config);

    CHECK(merged.stats.baseVectorCount == 3U);
    CHECK(merged.stats.multiVectorRawHitCount == 5U);
    CHECK(merged.stats.multiVectorAddedNewCount == 3U);
    CHECK(merged.stats.multiVectorReplacedBaseCount == 2U);
    CHECK(merged.stats.multiVectorMergedCount == 5U);
    CHECK(merged.stats.graphVectorRawHitCount == 3U);
    CHECK(merged.stats.graphVectorAddedNewCount == 1U);
    CHECK(merged.stats.graphVectorReplacedBaseCount == 1U);
    CHECK(merged.stats.graphVectorBlockedUncorroboratedCount == 1U);
    CHECK(merged.stats.graphVectorBlockedMissingTextAnchorCount == 0U);
    CHECK(merged.stats.graphVectorBlockedMissingBaselineTextAnchorCount == 0U);

    REQUIRE(merged.components.size() == 5U);
    CHECK(merged.components[0].source == ComponentResult::Source::Text);
    const auto vectorsBegin = merged.components.begin() + 1;
    REQUIRE(std::distance(vectorsBegin, merged.components.end()) == 4);
    CHECK(((vectorsBegin[0].documentHash == "base-a" && vectorsBegin[1].documentHash == "base-b") ||
           (vectorsBegin[0].documentHash == "base-b" && vectorsBegin[1].documentHash == "base-a")));
    const auto& baseA =
        vectorsBegin[vectorsBegin[0].documentHash == "base-a" ? std::size_t{0} : std::size_t{1}];
    const auto& baseB =
        vectorsBegin[vectorsBegin[0].documentHash == "base-b" ? std::size_t{0} : std::size_t{1}];
    CHECK(baseA.score == Catch::Approx(0.8F));
    CHECK(baseB.score == Catch::Approx(0.8F));
    CHECK(baseB.debugInfo.at("multi_vector_phrase") == "second");
    CHECK(vectorsBegin[2].documentHash == "phrase-new");
    CHECK(vectorsBegin[2].score == Catch::Approx(0.6F));
    CHECK(vectorsBegin[2].rank == 2U);
    CHECK(vectorsBegin[3].documentHash == "anchor");
    CHECK(vectorsBegin[3].source == ComponentResult::Source::GraphVector);
    CHECK(vectorsBegin[3].score == Catch::Approx(0.48F));
    CHECK(vectorsBegin[3].rank == 0U);
    CHECK(vectorsBegin[3].debugInfo.at("graph_vector_term") == "allowed");
}

TEST_CASE("Vector pipeline applies graph gates independently without defining equal-score order",
          "[search][vector][auxiliary][graph-gates][catch2]") {
    using yams::search::ComponentResult;
    using yams::search::detail::AuxiliaryVectorCandidateBatch;

    SearchEngineConfig config;
    config.vectorMaxResults = 4;
    config.graphExpansionVectorPenalty = 1.0F;
    config.graphVectorRequireCorroboration = false;
    config.graphVectorRequireTextAnchoring = true;
    config.graphVectorRequireBaselineTextAnchoring = true;

    std::vector<ComponentResult> components{
        {.documentHash = "graph-only", .score = 0.9F, .source = ComponentResult::Source::GraphText},
        {.documentHash = "baseline", .score = 0.8F, .source = ComponentResult::Source::Text},
        {.documentHash = "z", .score = 0.4F, .source = ComponentResult::Source::Vector},
        {.documentHash = "a", .score = 0.4F, .source = ComponentResult::Source::Vector},
    };
    std::vector<AuxiliaryVectorCandidateBatch> graphTerms{
        {.query = "term",
         .weight = 1.0F,
         .candidates = {{.documentHash = "missing-text", .score = 0.8F},
                        {.documentHash = "graph-only", .score = 0.8F},
                        {.documentHash = "baseline", .score = 0.8F}}},
    };

    auto merged = yams::search::detail::mergeAuxiliaryVectorCandidates(
        std::move(components), {}, std::move(graphTerms), config);

    CHECK(merged.stats.graphVectorBlockedMissingTextAnchorCount == 1U);
    CHECK(merged.stats.graphVectorBlockedMissingBaselineTextAnchorCount == 1U);
    CHECK(merged.stats.graphVectorAddedNewCount == 1U);
    REQUIRE(merged.components.size() == 5U);
    CHECK(((merged.components[2].documentHash == "a" && merged.components[3].documentHash == "z") ||
           (merged.components[2].documentHash == "z" && merged.components[3].documentHash == "a")));
    CHECK(merged.components[4].documentHash == "baseline");
}
