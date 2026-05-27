#include <catch2/catch_test_macros.hpp>

#include "src/search/benchmarks/retrieval_benchmark_support.h"
#include "tests/common/test_helpers_catch2.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <set>

namespace fs = std::filesystem;

using yams::bench::CorpusGenerator;
using yams::bench::loadBenchmarkManifestDataset;
using yams::bench::SyntheticCorpusMode;

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
    REQUIRE(dataset.name == "repo-mini");
    REQUIRE(dataset.documents.size() == 2);
    REQUIRE(dataset.queries.size() == 2);
    REQUIRE(dataset.qrels.size() == 2);
    REQUIRE(dataset.documents.at("grep_service").text == "CAS first indexed content scan");
    REQUIRE(dataset.documents.at("storage_note").text.find("retry budget backoff") !=
            std::string::npos);
}

TEST_CASE("Hard lexical synthetic corpus produces graded retrieval queries", "[bench][retrieval]") {
    yams::test::TempDirGuard tempDir{"retrieval_hard_synth_"};
    CorpusGenerator corpus(tempDir.path(), SyntheticCorpusMode::HardLexical);

    corpus.generateDocuments(12);
    auto queries = corpus.generateQueries(8);

    REQUIRE(corpus.createdFiles.size() == 12);
    REQUIRE(queries.size() == 4);

    const auto hasFile = [&](const std::string& name) {
        return std::find(corpus.createdFiles.begin(), corpus.createdFiles.end(), name) !=
               corpus.createdFiles.end();
    };

    REQUIRE(hasFile("auth_password_reset_primary.txt"));
    REQUIRE(hasFile("database_schema_change_primary.txt"));

    const auto& first = queries.front();
    REQUIRE(first.query == "password reset token email recovery");
    REQUIRE(first.relevantFiles.contains("auth_password_reset_primary.txt"));
    REQUIRE(first.relevantFiles.contains("auth_password_reset_secondary.txt"));
    REQUIRE(first.relevanceGrades.at("auth_password_reset_primary.txt") == 3);
    REQUIRE(first.relevanceGrades.at("auth_password_reset_secondary.txt") == 2);
    REQUIRE(first.relevanceGrades.at("auth_session_revocation_distractor.txt") == 1);
}
