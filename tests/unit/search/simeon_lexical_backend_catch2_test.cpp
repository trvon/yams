#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <yams/metadata/metadata_repository.h>
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
    corpus.dbPath = tempDbPath("simeon_lex_");
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
        if (backend.ready()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return backend.ready();
}

} // namespace

TEST_CASE("SimeonLexicalBackend default config uses SabSmooth", "[search][simeon][catch2]") {
    SimeonLexicalBackend::Config cfg;
    CHECK(cfg.variant == SimeonLexicalBackend::Variant::SabSmooth);
    CHECK(cfg.subword_gamma == 5.0f);
    CHECK(cfg.max_corpus_docs == 200'000u);
    CHECK(cfg.fragment_geometry_enabled);
    CHECK(cfg.fragment_geometry_min_corpus_docs == 1000u);
    CHECK(cfg.fragment_geometry_config.use_phss);
    CHECK(cfg.fragment_geometry_config.phss_config.criterion ==
          simeon::PhssConfig::Criterion::LargestGap);
}

TEST_CASE("SimeonLexicalBackend buildAsync flips ready on small corpus",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
        {"hash_c", "omega sigma tau"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    CHECK_FALSE(backend.ready());

    auto build = backend.buildAsync(corpus.repo);
    REQUIRE(build.has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));
    CHECK(backend.doc_count() == 3u);
}

TEST_CASE("SimeonLexicalBackend score returns one float per candidate",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
        {"hash_c", "omega sigma tau"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto scores = backend.score("beta", corpus.docIds);
    REQUIRE(scores.has_value());
    CHECK(scores.value().size() == corpus.docIds.size());
}

TEST_CASE("SimeonLexicalBackend skips fragment geometry below the default corpus threshold",
          "[search][simeon][catch2]") {
    std::vector<std::pair<std::string, std::string>> docs;
    docs.reserve(80);
    for (int i = 0; i < 80; ++i) {
        std::string content;
        for (int j = 0; j < 20; ++j) {
            if (j > 0) {
                content.push_back(' ');
            }
            content += "token" + std::to_string((i + j) % 80);
        }
        docs.emplace_back("hash_" + std::to_string(i), std::move(content));
    }
    auto corpus = makeCorpus(docs);

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(10)));
    CHECK_FALSE(backend.fragmentGeometryReady());
}

TEST_CASE("SimeonLexicalBackend score returns 0 for unknown doc_ids", "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma"},
        {"hash_b", "beta gamma epsilon"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    const std::int64_t missingId = 999'999;
    std::vector<std::int64_t> ids = {corpus.docIds.front(), missingId};
    auto scores = backend.score("beta", ids);
    REQUIRE(scores.has_value());
    REQUIRE(scores.value().size() == ids.size());
    CHECK(scores.value()[1] == 0.0f);
}

TEST_CASE("SimeonLexicalBackend score before ready returns NotInitialized",
          "[search][simeon][catch2]") {
    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE_FALSE(backend.ready());
    std::vector<std::int64_t> ids = {1};
    auto scores = backend.score("anything", ids);
    REQUIRE_FALSE(scores.has_value());
    CHECK(scores.error().code == ErrorCode::NotInitialized);
}

TEST_CASE("SimeonLexicalBackend Atire variant also builds", "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma"},
        {"hash_b", "beta gamma epsilon"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.variant = SimeonLexicalBackend::Variant::Atire;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto scores = backend.score("beta", corpus.docIds);
    REQUIRE(scores.has_value());
    CHECK(scores.value().size() == corpus.docIds.size());
}

TEST_CASE("SimeonLexicalBackend scoreRouted passthrough when router disabled",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
        {"hash_c", "omega sigma tau"},
    });

    SimeonLexicalBackend::Config cfg; // router_enabled=false by default
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto decision = backend.scoreRouted("beta", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(decision.value().scores.size() == corpus.docIds.size());
    // router=off → recipe label falls back to the configured variant.
    CHECK(std::string(decision.value().recipe_name) == "SabSmooth");
}

TEST_CASE("SimeonLexicalBackend scoreRouted picks a recipe with router on",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon zeta"},
        {"hash_c", "omega sigma tau upsilon"},
        {"hash_d", "beta gamma theta iota"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.router_enabled = true; // passE preset defaults
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto decision = backend.scoreRouted("beta gamma", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(decision.value().scores.size() == corpus.docIds.size());
    const std::string name = decision.value().recipe_name;
    CHECK((name == "Bm25Atire" || name == "Bm25SabSmooth" || name == "CascadeLinearAlpha"));
}

TEST_CASE("SimeonLexicalBackend scoreRouted routes OOV query to SabSmooth",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon zeta"},
        {"hash_c", "omega sigma tau upsilon"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.router_enabled = true; // passE: oov_threshold=0 → any OOV → SAB
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    // "xyzneverpresent" does not appear in any document → oov_rate = 1.0 > 0.
    auto decision = backend.scoreRouted("xyzneverpresent", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(std::string(decision.value().recipe_name) == "Bm25SabSmooth");
}
