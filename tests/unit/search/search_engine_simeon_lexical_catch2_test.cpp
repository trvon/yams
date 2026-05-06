#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
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
    corpus.dbPath = tempDbPath("search_simeon_lex_");
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

SearchEngineConfig makeLexicalOnlyConfig() {
    SearchEngineConfig cfg;
    cfg.textWeight = 1.0f;
    cfg.graphTextWeight = 0.0f;
    cfg.pathTreeWeight = 0.0f;
    cfg.kgWeight = 0.0f;
    cfg.vectorWeight = 0.0f;
    cfg.graphVectorWeight = 0.0f;
    cfg.entityVectorWeight = 0.0f;
    cfg.tagWeight = 0.0f;
    cfg.metadataWeight = 0.0f;
    cfg.enableParallelExecution = false;
    cfg.enableTieredExecution = false;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.maxResults = 20;
    return cfg;
}

std::unique_ptr<SearchEngine> makeEngine(const TempCorpus& corpus, const SearchEngineConfig& cfg) {
    return std::make_unique<SearchEngine>(corpus.repo, nullptr, nullptr, nullptr, cfg);
}

std::unordered_set<std::string> hashSet(const std::vector<SearchResult>& rs) {
    std::unordered_set<std::string> out;
    out.reserve(rs.size());
    for (const auto& r : rs) {
        out.insert(r.document.sha256Hash);
    }
    return out;
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

const std::vector<std::pair<std::string, std::string>> kCorpus = {
    {"hash_a", "alpha beta gamma delta"},
    {"hash_b", "beta gamma epsilon"},
    {"hash_c", "omega sigma tau"},
    {"hash_d", "beta beta beta filler content"},
};

} // namespace

TEST_CASE("SearchEngine returns lexical results without Simeon backend injected",
          "[search][simeon][integration][catch2]") {
    auto corpus = makeCorpus(kCorpus);
    auto engine = makeEngine(corpus, makeLexicalOnlyConfig());

    SearchParams params;
    params.limit = 10;
    auto result = engine->search("beta", params);
    REQUIRE(result.has_value());
    const auto& hits = result.value();
    CHECK_FALSE(hits.empty());

    const auto ids = hashSet(hits);
    CHECK(ids.count("hash_a") == 1u);
    CHECK(ids.count("hash_b") == 1u);
    CHECK(ids.count("hash_d") == 1u);
    CHECK(ids.count("hash_c") == 0u);

    for (const auto& r : hits) {
        CHECK(r.score >= 0.0);
    }
}

TEST_CASE("SearchEngine injected with Simeon backend returns results while still building",
          "[search][simeon][integration][catch2]") {
    auto corpus = makeCorpus(kCorpus);
    auto engine = makeEngine(corpus, makeLexicalOnlyConfig());

    auto backend = std::make_unique<SimeonLexicalBackend>(SimeonLexicalBackend::Config{});
    engine->setSimeonLexicalBackend(std::move(backend));

    SearchParams params;
    params.limit = 10;
    auto result = engine->search("beta", params);
    REQUIRE(result.has_value());
    CHECK_FALSE(result.value().empty());

    const auto ids = hashSet(result.value());
    CHECK(ids.count("hash_c") == 0u);
    for (const auto& r : result.value()) {
        CHECK(r.score >= 0.0);
    }
}

TEST_CASE("SearchEngine with ready Simeon backend rescores the FTS5 candidate pool",
          "[search][simeon][integration][catch2]") {
    auto corpus = makeCorpus(kCorpus);

    auto baselineEngine = makeEngine(corpus, makeLexicalOnlyConfig());
    SearchParams params;
    params.limit = 10;
    auto baselineResult = baselineEngine->search("beta", params);
    REQUIRE(baselineResult.has_value());
    const auto baselineIds = hashSet(baselineResult.value());

    auto simeonEngine = makeEngine(corpus, makeLexicalOnlyConfig());
    auto backend = std::make_unique<SimeonLexicalBackend>(SimeonLexicalBackend::Config{});
    auto* backendRaw = backend.get();
    simeonEngine->setSimeonLexicalBackend(std::move(backend));
    REQUIRE(waitReady(*backendRaw, std::chrono::seconds(5)));

    auto simeonResult = simeonEngine->search("beta", params);
    REQUIRE(simeonResult.has_value());
    const auto simeonIds = hashSet(simeonResult.value());

    CHECK(simeonIds == baselineIds);
    for (const auto& r : simeonResult.value()) {
        CHECK(r.score >= 0.0);
    }
}
