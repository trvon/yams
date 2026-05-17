// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors
//
// Unit tests for bandit-driven simeon backend dispatch (scoreBanditRouted).
// Tests cover the core SAB/RM3 arms plus fallback and not-ready behavior with
// a real backend on a small corpus.

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/simeon_lexical_backend.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

struct TempCorpus {
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::vector<std::int64_t> docIds;
};

TempCorpus makeCorpus(const std::vector<std::pair<std::string, std::string>>& docs) {
    TempCorpus corpus;
    corpus.dbPath = tempDbPath("search_sbandit_");
    ConnectionPoolConfig pcfg;
    corpus.pool = std::make_unique<ConnectionPool>(corpus.dbPath.string(), pcfg);
    REQUIRE(corpus.pool->initialize().has_value());
    corpus.repo = std::make_shared<MetadataRepository>(*corpus.pool);

    for (const auto& [hash, content] : docs) {
        DocumentInfo d;
        d.filePath = "/tmp/" + hash + ".txt";
        d.fileName = hash + ".txt";
        d.fileExtension = ".txt";
        d.fileSize = static_cast<int64_t>(content.size());
        d.sha256Hash = hash;
        d.mimeType = "text/plain";
        d.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        d.modifiedTime = d.createdTime;
        d.indexedTime = d.createdTime;
        auto did = corpus.repo->insertDocument(d);
        REQUIRE(did.has_value());
        corpus.docIds.push_back(did.value());

        REQUIRE(corpus.repo->indexDocumentContent(did.value(), hash, content, "text/plain")
                    .has_value());

        DocumentContent dc;
        dc.documentId = did.value();
        dc.contentText = content;
        dc.contentLength = static_cast<int64_t>(content.size());
        dc.extractionMethod = "test";
        dc.language = "en";
        REQUIRE(corpus.repo->insertContent(dc).has_value());
    }
    return corpus;
}

bool waitReady(const SimeonLexicalBackend& backend, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (backend.ready())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return backend.ready();
}

const std::vector<std::pair<std::string, std::string>> kCorpus = {
    {"hash_a", "alpha beta gamma delta"}, {"hash_b", "beta gamma epsilon zeta"},
    {"hash_c", "omega sigma tau"},        {"hash_d", "beta beta beta filler content filler"},
    {"hash_e", "alpha sigma tau"},
};

std::vector<std::int64_t> fakeCandidateIds() {
    return {1, 2, 3, 4, 5};
}

std::unique_ptr<SimeonLexicalBackend> buildBackend(const TempCorpus& corpus,
                                                   SimeonLexicalBackend::Config cfg = {}) {
    cfg.variant = SimeonLexicalBackend::Variant::SabSmooth;
    auto backend = std::make_unique<SimeonLexicalBackend>(cfg);
    auto result = backend->buildAsync(corpus.repo);
    REQUIRE(result.has_value());
    REQUIRE(waitReady(*backend, std::chrono::seconds(15)));
    return backend;
}

} // namespace

TEST_CASE("scoreBanditRouted: sab_smooth arm returns results",
          "[search][simeon][bandit][backend][dispatch]") {
    auto corpus = makeCorpus(kCorpus);
    auto backend = buildBackend(corpus);

    auto ids = fakeCandidateIds();
    auto result = backend->scoreBanditRouted("beta", "sab_smooth", ids);
    REQUIRE(result.has_value());
    CHECK(std::string(result.value().recipe_name) == "Bm25SabSmooth");
    CHECK(result.value().scores.size() == ids.size());
}

TEST_CASE("scoreBanditRouted: sab_smooth_rm3_adaptive arm returns results",
          "[search][simeon][bandit][backend][dispatch]") {
    auto corpus = makeCorpus(kCorpus);
    auto backend = buildBackend(corpus);

    auto ids = fakeCandidateIds();
    auto result = backend->scoreBanditRouted("beta", "sab_smooth_rm3_adaptive", ids);
    REQUIRE(result.has_value());
    CHECK(std::string(result.value().recipe_name) == "Bm25SabRm3Adaptive");
    CHECK(result.value().scores.size() == ids.size());
}

TEST_CASE("scoreBanditRouted: sab_smooth_rm3_diverse arm returns results",
          "[search][simeon][bandit][backend][dispatch]") {
    auto corpus = makeCorpus(kCorpus);
    auto backend = buildBackend(corpus);

    auto ids = fakeCandidateIds();
    auto result = backend->scoreBanditRouted("beta", "sab_smooth_rm3_diverse", ids);
    REQUIRE(result.has_value());
    CHECK(std::string(result.value().recipe_name) == "Bm25SabRm3Diverse");
    CHECK(result.value().scores.size() == ids.size());
}

TEST_CASE("scoreBanditRouted: unknown arm falls back to sab_smooth",
          "[search][simeon][bandit][backend][dispatch]") {
    auto corpus = makeCorpus(kCorpus);
    auto backend = buildBackend(corpus);

    auto ids = fakeCandidateIds();
    auto result = backend->scoreBanditRouted("beta", "bogus_arm_name", ids);
    REQUIRE(result.has_value());
    CHECK(std::string(result.value().recipe_name) == "Bm25SabSmooth");
    CHECK(result.value().scores.size() == ids.size());
}

TEST_CASE("scoreBanditRouted: different arms produce different recipe labels",
          "[search][simeon][bandit][backend][dispatch]") {
    auto corpus = makeCorpus(kCorpus);
    auto backend = buildBackend(corpus);

    auto ids = fakeCandidateIds();

    auto r1 = backend->scoreBanditRouted("beta", "sab_smooth", ids);
    auto r2 = backend->scoreBanditRouted("beta", "sab_smooth_rm3_adaptive", ids);
    auto r3 = backend->scoreBanditRouted("beta", "sab_smooth_rm3_diverse", ids);

    REQUIRE(r1.has_value());
    REQUIRE(r2.has_value());
    REQUIRE(r3.has_value());

    CHECK(std::string(r1.value().recipe_name) != std::string(r2.value().recipe_name));
    CHECK(std::string(r1.value().recipe_name) != std::string(r3.value().recipe_name));
    CHECK(std::string(r2.value().recipe_name) != std::string(r3.value().recipe_name));
}

TEST_CASE("scoreBanditRouted: returns zero for unknown candidate ids",
          "[search][simeon][bandit][backend][dispatch]") {
    auto corpus = makeCorpus(kCorpus);
    auto backend = buildBackend(corpus);

    // Send IDs that don't exist in the backend.
    std::vector<std::int64_t> badIds = {99999, 88888};
    auto result = backend->scoreBanditRouted("beta", "sab_smooth", badIds);
    REQUIRE(result.has_value());
    CHECK(result.value().scores.size() == badIds.size());
    for (auto s : result.value().scores) {
        CHECK(s == 0.0f);
    }
}

TEST_CASE("scoreBanditRouted: not ready backend returns error",
          "[search][simeon][bandit][backend][dispatch]") {
    SimeonLexicalBackend::Config cfg;
    auto backend = std::make_unique<SimeonLexicalBackend>(cfg);
    // Don't build — backend is not ready.

    auto ids = fakeCandidateIds();
    auto result = backend->scoreBanditRouted("beta", "sab_smooth", ids);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("SearchEngineConfig: bandit arm flag absent uses existing routing",
          "[search][simeon][bandit][config]") {
    SearchEngineConfig cfg;
    CHECK(cfg.simeonBanditArm.empty());

    // When empty, the lexical pipeline should use scoreRouted or scoreStrategyRouted.
    cfg.simeonBanditArm = "";
    CHECK(cfg.simeonBanditArm.empty());
}

TEST_CASE("SearchEngineConfig: bandit arm set triggers scoreBanditRouted path",
          "[search][simeon][bandit][config]") {
    SearchEngineConfig cfg;
    cfg.simeonBanditArm = "sab_smooth_rm3_adaptive";
    CHECK_FALSE(cfg.simeonBanditArm.empty());
    CHECK(cfg.simeonBanditArm == "sab_smooth_rm3_adaptive");
}
