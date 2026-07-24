#include <catch2/catch_test_macros.hpp>

#include "tests/benchmarks/search/retrieval_benchmark_support.h"
#include "tests/common/test_helpers_catch2.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>

namespace fs = std::filesystem;

using yams::bench::BenchmarkTopologyConstructionCertificate;
using yams::bench::buildGraphTopologyNeighbors;
using yams::bench::buildKeyphraseTopologyNeighbors;
using yams::bench::buildLexicalTopologyNeighbors;
using yams::bench::buildSegmentKeyphraseTopologyNeighbors;
using yams::bench::CorpusGenerator;
using yams::bench::GraphTopologyDocument;
using yams::bench::KeyphraseTopologyDocument;
using yams::bench::LexicalTopologyDocument;
using yams::bench::loadBenchmarkManifestDataset;
using yams::bench::SyntheticCorpusMode;
using yams::bench::topologyConstructionCertificateToJson;
using yams::bench::validateTopologyConstructionCertificate;

TEST_CASE("Benchmark index readiness trusts backend-validated Simeon persistence",
          "[bench][retrieval][vector]") {
    using yams::vector::VectorSearchEngine;

    CHECK_FALSE(yams::bench::benchmarkSearchIndexReusable(VectorSearchEngine::SimeonPqAdc, false));
    CHECK(yams::bench::benchmarkSearchIndexReusable(VectorSearchEngine::SimeonPqAdc, true));
    CHECK_FALSE(yams::bench::benchmarkSearchIndexReusable(VectorSearchEngine::Vec0L2, false));
    CHECK_FALSE(yams::bench::benchmarkSearchIndexReusable(VectorSearchEngine::ExactScan, true));
}

TEST_CASE("Benchmark support observes process-local peak RSS", "[bench][retrieval][resources]") {
    const auto peakRssMb = yams::bench::processPeakRssMb();
#if defined(__APPLE__) || defined(__GLIBC__)
    REQUIRE(peakRssMb.has_value());
    CHECK((*peakRssMb > 0.0));
#else
    CHECK_FALSE(peakRssMb.has_value());
#endif
}

TEST_CASE("Benchmark query readiness accepts a prepared Simeon exact fallback",
          "[bench][retrieval][vector]") {
    using yams::vector::VectorSearchEngine;

    CHECK(
        yams::bench::benchmarkSearchIndexQueryReady(VectorSearchEngine::SimeonPqAdc, false, true));
    CHECK_FALSE(
        yams::bench::benchmarkSearchIndexQueryReady(VectorSearchEngine::SimeonPqAdc, false, false));
    CHECK(yams::bench::benchmarkSearchIndexQueryReady(VectorSearchEngine::ExactScan, false, false));
}

TEST_CASE("Topology-disabled benchmark config suppresses semantic graph ingestion",
          "[bench][retrieval][config][topology]") {
    std::ostringstream disabled;
    yams::bench::writeTopologyDisabledEmbeddingSelection(disabled, true);
    CHECK((disabled.str() == "\n[embeddings.selection]\n"
                             "update_semantic_graph_during_ingest = false\n\n"));

    std::ostringstream enabled;
    yams::bench::writeTopologyDisabledEmbeddingSelection(enabled, false);
    CHECK((enabled.str().empty()));
}

TEST_CASE("Lexical topology neighbor builder links reciprocal topical peers",
          "[bench][retrieval][topology]") {
    const std::vector<LexicalTopologyDocument> docs = {
        {.documentHash = "auth_a",
         .text = "password reset token email recovery token confirmation"},
        {.documentHash = "auth_b", .text = "email password recovery reset token validation"},
        {.documentHash = "cache_a", .text = "cache invalidation stale reads eviction refresh"},
    };

    const auto neighbors = buildLexicalTopologyNeighbors(docs, 1, 0.05F, true);

    REQUIRE((neighbors.size() == 2));
    CHECK((neighbors[0].sourceHash == "auth_a"));
    CHECK((neighbors[0].targetHash == "auth_b"));
    CHECK((neighbors[0].score > 0.05F));
    CHECK((neighbors[1].sourceHash == "auth_b"));
    CHECK((neighbors[1].targetHash == "auth_a"));
    CHECK((neighbors[1].score == neighbors[0].score));
}

TEST_CASE("Lexical topology builder emits certificate witnesses", "[bench][retrieval][topology]") {
    const std::vector<LexicalTopologyDocument> docs = {
        {.documentHash = "auth_a",
         .text = "password reset token email recovery token confirmation"},
        {.documentHash = "auth_b", .text = "email password recovery reset token validation"},
        {.documentHash = "cache_a", .text = "cache invalidation stale reads eviction refresh"},
    };

    BenchmarkTopologyConstructionCertificate cert;
    const auto neighbors = buildLexicalTopologyNeighbors(docs, 1, 0.05F, true, &cert);
    const auto validation = validateTopologyConstructionCertificate(cert);

    REQUIRE((neighbors.size() == 2));
    REQUIRE(validation.ok);
    CHECK((cert.sourceName == "lexical"));
    CHECK((cert.observations.size() >= neighbors.size()));
    CHECK((cert.edgeWitnesses.size() == neighbors.size()));
    CHECK((cert.components.size() == 1));
    CHECK((cert.components.front().memberHashes.size() == 2));
    CHECK((cert.edgeWitnesses.front().signalGatePassed));
    CHECK((cert.edgeWitnesses.front().reciprocal));
    CHECK_FALSE(cert.edgeWitnesses.front().featureId.empty());

    const auto json = topologyConstructionCertificateToJson(cert, validation);
    CHECK((json["validation"]["ok"].get<bool>() == true));
    CHECK((json["summary"]["edge_witnesses"].get<std::size_t>() == neighbors.size()));
}

TEST_CASE("Graph topology neighbor builder links shared KG neighborhoods",
          "[bench][retrieval][topology]") {
    const std::vector<GraphTopologyDocument> docs = {
        {.documentHash = "doc_a", .relatedNodes = {{101, 0.9F}, {102, 0.8F}}},
        {.documentHash = "doc_b", .relatedNodes = {{101, 0.7F}, {102, 0.8F}}},
        {.documentHash = "doc_c", .relatedNodes = {{201, 1.0F}}},
    };

    const auto neighbors = buildGraphTopologyNeighbors(docs, 1, 0.50F, true);

    REQUIRE((neighbors.size() == 2));
    CHECK((neighbors[0].sourceHash == "doc_a"));
    CHECK((neighbors[0].targetHash == "doc_b"));
    CHECK((neighbors[0].score > 0.50F));
    CHECK((neighbors[1].sourceHash == "doc_b"));
    CHECK((neighbors[1].targetHash == "doc_a"));
    CHECK((neighbors[1].score == neighbors[0].score));
}

TEST_CASE("Graph topology builder emits entity certificate witnesses",
          "[bench][retrieval][topology]") {
    const std::vector<GraphTopologyDocument> docs = {
        {.documentHash = "doc_a", .relatedNodes = {{101, 0.9F}, {102, 0.8F}}},
        {.documentHash = "doc_b", .relatedNodes = {{101, 0.7F}, {102, 0.8F}}},
        {.documentHash = "doc_c", .relatedNodes = {{201, 1.0F}}},
    };

    BenchmarkTopologyConstructionCertificate cert;
    const auto neighbors = buildGraphTopologyNeighbors(docs, 1, 0.50F, true, &cert);
    const auto validation = validateTopologyConstructionCertificate(cert);

    REQUIRE((neighbors.size() == 2));
    REQUIRE(validation.ok);
    CHECK((cert.sourceName == "graph"));
    CHECK((cert.edgeWitnesses.size() == neighbors.size()));
    CHECK((cert.edgeWitnesses.front().featureId.rfind("kg:", 0) == 0));
    CHECK((cert.components.size() == 1));
}

TEST_CASE("Keyphrase topology builder emits sparse certificate witnesses",
          "[bench][retrieval][topology]") {
    const std::vector<KeyphraseTopologyDocument> docs = {
        {.documentHash = "auth_a",
         .text = "password reset token email recovery password reset token confirmation"},
        {.documentHash = "auth_b",
         .text = "password reset token validation email recovery account confirmation"},
        {.documentHash = "cache_a",
         .text = "cache invalidation stale reads eviction refresh cache invalidation"},
    };

    BenchmarkTopologyConstructionCertificate cert;
    const auto neighbors = buildKeyphraseTopologyNeighbors(docs, 1, 0.20F, 8, 2, true, &cert);
    const auto validation = validateTopologyConstructionCertificate(cert);

    REQUIRE((neighbors.size() == 2));
    REQUIRE(validation.ok);
    CHECK((cert.sourceName == "keyphrase"));
    CHECK((cert.edgeWitnesses.size() == neighbors.size()));
    CHECK((cert.edgeWitnesses.front().signalGatePassed));
    CHECK((cert.edgeWitnesses.front().reciprocal));
    CHECK((cert.edgeWitnesses.front().documentFrequency == 2));
    CHECK_FALSE(cert.edgeWitnesses.front().featureId.empty());
}

TEST_CASE("Segment keyphrase topology emits segment witnesses", "[bench][retrieval][topology]") {
    const std::vector<KeyphraseTopologyDocument> docs = {
        {.documentHash = "auth_a",
         .text = "password reset token email recovery password reset token confirmation"},
        {.documentHash = "auth_b",
         .text = "password reset token validation email recovery account confirmation"},
        {.documentHash = "cache_a",
         .text = "cache invalidation stale reads eviction refresh cache invalidation"},
    };

    BenchmarkTopologyConstructionCertificate cert;
    const auto neighbors =
        buildSegmentKeyphraseTopologyNeighbors(docs, 1, 0.20F, 6, 0, 8, 2, 4, 8, true, &cert);
    const auto validation = validateTopologyConstructionCertificate(cert);

    REQUIRE((neighbors.size() == 2));
    REQUIRE(validation.ok);
    CHECK((cert.sourceName == "segment_keyphrase"));
    REQUIRE_FALSE(cert.edgeWitnesses.empty());
    CHECK((cert.edgeWitnesses.front().signalGatePassed));
    CHECK((cert.edgeWitnesses.front().reciprocal));
    CHECK_FALSE(cert.edgeWitnesses.front().sourceSegmentId.empty());
    CHECK_FALSE(cert.edgeWitnesses.front().targetSegmentId.empty());

    const auto json = topologyConstructionCertificateToJson(cert, validation);
    REQUIRE_FALSE(json["edge_witnesses"].empty());
    CHECK(json["edge_witnesses"].front().contains("source_segment"));
    CHECK(json["edge_witnesses"].front().contains("target_segment"));
}

TEST_CASE("Segment keyphrase topology caps components before publishing edges",
          "[bench][retrieval][topology]") {
    const std::vector<KeyphraseTopologyDocument> docs = {
        {.documentHash = "doc_a", .text = "shared alpha beta gamma unique aaaa"},
        {.documentHash = "doc_b", .text = "shared alpha beta gamma unique bbbb"},
        {.documentHash = "doc_c", .text = "shared alpha beta gamma unique cccc"},
    };

    BenchmarkTopologyConstructionCertificate cert;
    const auto neighbors =
        buildSegmentKeyphraseTopologyNeighbors(docs, 2, 0.10F, 8, 0, 8, 3, 2, 2, true, &cert);
    const auto validation = validateTopologyConstructionCertificate(cert);

    REQUIRE(validation.ok);
    CHECK_FALSE(neighbors.empty());
    for (const auto& component : cert.components) {
        CHECK((component.memberHashes.size() <= 2));
    }
    CHECK((cert.edgeWitnesses.size() <= 2));
}

TEST_CASE("Topology certificate validation rejects oversized components",
          "[bench][retrieval][topology]") {
    BenchmarkTopologyConstructionCertificate cert;
    cert.gates.maxComponentDocs = 1;
    cert.components.push_back({.rootHash = "a", .memberHashes = {"a", "b"}});

    const auto validation = validateTopologyConstructionCertificate(cert);

    REQUIRE_FALSE(validation.ok);
    REQUIRE_FALSE(validation.errors.empty());
}

TEST_CASE("Benchmark manifest loader accepts inline and file-backed documents",
          "[bench][retrieval]") {
    yams::test::TempDirGuard tempDir{"retrieval_manifest_"};
    const fs::path root = tempDir.path();
    yams::test::write_file(root / "docs" / "storage_note.txt",
                           "Storage note\n\nretry budget backoff timeout circuit breaker\n");

    yams::test::write_file(root / "benchmark_manifest.json",
                           R"JSON({
  "name": "repo-mini",
  "documents": [
    {"id": "grep_service", "title": "Grep service", "text": "CAS first indexed content scan"},
    {"id": "storage_note", "path": "docs/storage_note.txt"}
  ],
  "queries": [
    {"id": "q1", "text": "indexed content scan"},
    {"id": "q2", "text": "retry budget backoff timeout circuit breaker"}
  ],
  "qrels": [
    {"query_id": "q1", "doc_id": "grep_service", "score": 3},
    {"query_id": "q2", "doc_id": "storage_note", "score": 2}
  ]
})JSON");

    auto datasetResult = loadBenchmarkManifestDataset("local-manifest", root);
    REQUIRE(datasetResult);

    const auto& dataset = datasetResult.value();
    REQUIRE((dataset.name == "repo-mini"));
    REQUIRE((dataset.documents.size() == 2));
    REQUIRE((dataset.queries.size() == 2));
    REQUIRE((dataset.qrels.size() == 2));
    REQUIRE((dataset.documents.at("grep_service").text == "CAS first indexed content scan"));
    REQUIRE((dataset.documents.at("storage_note").text.find("retry budget backoff") !=
             std::string::npos));
}

TEST_CASE("Benchmark manifest loader preserves KG entity annotations", "[bench][retrieval]") {
    yams::test::TempDirGuard tempDir{"retrieval_manifest_kg_"};
    const fs::path root = tempDir.path();

    yams::test::write_file(root / "benchmark_manifest.json",
                           R"JSON({
  "name": "repo-kg-mini",
  "documents": [
    {"id": "alpha", "title": "Alpha", "text": "shared graph concept one"},
    {"id": "beta", "title": "Beta", "text": "shared graph concept two"}
  ],
  "queries": [{"id": "q1", "text": "graph concept"}],
  "qrels": [{"query_id": "q1", "doc_id": "alpha", "score": 1}],
  "kg_entities": [
    {"doc_id": "alpha", "entity_key": "concept:shared", "label": "Shared Concept", "extractor": "gliner_title_nl", "weight": 0.9},
    {"doc_id": "beta", "entity_key": "concept:shared", "label": "Shared Concept", "weight": 0.8}
  ]
})JSON");

    auto datasetResult = loadBenchmarkManifestDataset("local-manifest", root);
    REQUIRE(datasetResult);

    const auto& dataset = datasetResult.value();
    REQUIRE((dataset.kgEntities.size() == 2));
    CHECK((dataset.kgEntities[0].docId == "alpha"));
    CHECK((dataset.kgEntities[0].entityKey == "concept:shared"));
    CHECK((dataset.kgEntities[0].label == "Shared Concept"));
    CHECK((dataset.kgEntities[0].relation == "mentioned_in"));
    CHECK((dataset.kgEntities[0].extractor == "gliner_title_nl"));
    CHECK((dataset.kgEntities[0].weight == 0.9F));
}

TEST_CASE("Hard lexical synthetic corpus produces graded retrieval queries", "[bench][retrieval]") {
    yams::test::TempDirGuard tempDir{"retrieval_hard_synth_"};
    CorpusGenerator corpus(tempDir.path(), SyntheticCorpusMode::HardLexical);

    corpus.generateDocuments(12);
    auto queries = corpus.generateQueries(8);

    REQUIRE((corpus.createdFiles.size() == 12));
    REQUIRE((queries.size() == 4));

    const auto hasFile = [&](const std::string& name) {
        return std::find(corpus.createdFiles.begin(), corpus.createdFiles.end(), name) !=
               corpus.createdFiles.end();
    };

    REQUIRE(hasFile("auth_password_reset_primary.txt"));
    REQUIRE(hasFile("database_schema_change_primary.txt"));

    const auto& first = queries.front();
    REQUIRE((first.query == "password reset token email recovery"));
    REQUIRE(first.relevantFiles.contains("auth_password_reset_primary.txt"));
    REQUIRE(first.relevantFiles.contains("auth_password_reset_secondary.txt"));
    REQUIRE((first.relevanceGrades.at("auth_password_reset_primary.txt") == 3));
    REQUIRE((first.relevanceGrades.at("auth_password_reset_secondary.txt") == 2));
    REQUIRE((first.relevanceGrades.at("auth_session_revocation_distractor.txt") == 1));
}
