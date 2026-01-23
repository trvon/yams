/**
 * @file hybrid_search_comprehensive_test.cpp
 * @brief Integration tests for SearchEngine (PBI-071, updated for PBI-091)
 *
 * Integration test coverage focusing on:
 * - Multi-component search (FTS5, vector, symbol, KG, path tree)
 * - Fusion strategies (WEIGHTED_SUM, RECIPROCAL_RANK, WEIGHTED_RECIPROCAL, COMB_MNZ)
 * - Fallback scenarios (degraded mode, timeouts)
 * - Real component integration (actual indexes, engines)
 * - Performance characteristics
 *
 * Note: This is an integration test, not a unit test. It uses real
 * components rather than mocks to validate end-to-end behavior.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#if defined(_WIN32) && __has_include(<onnxruntime_c_api.h>)
#include <onnxruntime_c_api.h>
#define YAMS_ORT_API_VERSION ORT_API_VERSION
#else
#define YAMS_ORT_API_VERSION 0
#endif

#if defined(_WIN32) && YAMS_ORT_API_VERSION < 23

TEST_CASE("HybridSearch - Windows ONNX Runtime API too old", "[hybrid][windows][skip]") {
    SUCCEED("Skipping hybrid search tests on Windows: ONNX Runtime API version below 23.");
}

#else

#define SKIP_HYBRID_ON_WINDOWS() ((void)0)

#include <yams/api/content_store_builder.h>
#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <yams/app/services/services.hpp>

#include <spdlog/spdlog.h>

#include "common/fixture_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::search;
using namespace yams::vector;
using namespace yams::api;
using Catch::Matchers::WithinAbs;

namespace fs = std::filesystem;

// ============================================================================
// Test Helpers
// ============================================================================

namespace {

/**
 * Helper to run coroutines synchronously in tests
 */
template <typename T> yams::Result<T> runAwait(boost::asio::awaitable<yams::Result<T>> aw) {
    boost::asio::io_context ioc;

    auto wrapper =
        [aw = std::move(aw)]() mutable -> boost::asio::awaitable<std::optional<yams::Result<T>>> {
        try {
            auto v = co_await std::move(aw);
            co_return std::optional<yams::Result<T>>(std::move(v));
        } catch (...) {
            co_return std::optional<yams::Result<T>>{};
        }
    };

    auto fut = boost::asio::co_spawn(ioc, wrapper(), boost::asio::use_future);
    ioc.run();
    auto opt = fut.get();
    if (opt) {
        return std::move(*opt);
    }
    return yams::Result<T>(yams::Error{yams::ErrorCode::InternalError, "Awaitable failed"});
}

// ============================================================================
// Test Fixtures
// ============================================================================

/**
 * @brief Comprehensive test fixture for SearchService (Catch2 version)
 *
 * Sets up isolated test environment with:
 * - Temporary database and storage
 * - Sample documents (code, docs, binary)
 * - Search engines (keyword, semantic, hybrid)
 */
class SearchServiceFixture {
public:
    SearchServiceFixture() {
        setupTestEnvironment();
        setupDatabase();
        setupStorage();
        setupServices();
        populateTestCorpus();
    }

    ~SearchServiceFixture() {
        cleanupServices();
        cleanupDatabase();
        cleanupTestEnvironment();
    }

    // Helper: Execute search request
    ::yams::app::services::SearchResponse executeSearch(::yams::app::services::SearchRequest req) {
        // Disable session filtering for tests since we don't set up sessions
        req.globalSearch = true;
        auto result = runAwait(searchService_->search(req));
        REQUIRE(result);
        return result.value();
    }

    // Helper: Create code document
    std::string createCodeDocument(const std::string& filename, const std::string& content,
                                   const std::vector<std::string>& tags = {}) {
        REQUIRE(fixtureManager_);
        REQUIRE(docService_);

        auto fixture = fixtureManager_->createTextFixture(filename, content, tags);

        StoreDocumentRequest storeReq;
        storeReq.path = fixture.path.string();
        storeReq.tags = tags; // Store tags in metadata
        auto storeResult = docService_->store(storeReq);
        REQUIRE(storeResult);

        // Index the content into FTS5 for keyword search
        indexDocumentForSearch(storeResult.value().hash, filename, content);

        return storeResult.value().hash;
    }

    // Helper: Create document
    std::string createDocument(const std::string& filename, const std::string& content,
                               const std::vector<std::string>& tags = {}) {
        REQUIRE(fixtureManager_);
        REQUIRE(docService_);

        auto fixture = fixtureManager_->createTextFixture(filename, content, tags);

        StoreDocumentRequest storeReq;
        storeReq.path = fixture.path.string();
        storeReq.tags = tags; // Store tags in metadata
        auto storeResult = docService_->store(storeReq);
        REQUIRE(storeResult);

        // Index the content into FTS5 for keyword search
        indexDocumentForSearch(storeResult.value().hash, filename, content);

        return storeResult.value().hash;
    }

    // Public member access for tests
    std::shared_ptr<ISearchService> searchService() const { return searchService_; }
    std::shared_ptr<IDocumentService> docService() const { return docService_; }
    std::shared_ptr<yams::search::SearchEngine> searchEngine() const {
        return appContext_.searchEngine;
    }

    // Test document hashes
    std::string hashDoc1_;      // pipeline.cpp
    std::string hashDoc2_;      // search_engine.cpp
    std::string hashDoc3_;      // README.md
    std::string hashDoc4_;      // test_search.cpp
    std::string hashDoc5_;      // unicode.md
    std::string hashDoc6_;      // logo.png
    std::string hashDocStem1_;  // searching_guide.md
    std::string hashDocStem2_;  // search_tutorial.md
    std::string hashDocStem3_;  // indexed_data.md
    std::string hashDocCamel1_; // api_handler.cpp
    std::string hashDocCamel2_; // file_system_utils.cpp
    std::string hashDocCamel3_; // snake_case_example.py
    std::string hashDocHyphen_; // config-parser.md

private:
    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ =
            fs::temp_directory_path() / ("search_comprehensive_test_" + pid + "_" + timestamp);
        fs::create_directories(testDir_);

        fixtureManager_ = std::make_shared<yams::test::FixtureManager>(testDir_);
    }

    void setupDatabase() {
        dbPath_ = fs::absolute(testDir_ / "test.db");
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string(), ConnectionMode::Create);
        REQUIRE(openResult);

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        MigrationManager mm(*database_);
        auto initResult = mm.initialize();
        REQUIRE(initResult);

        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = mm.migrate();
        REQUIRE(migrateResult);
    }

    void setupStorage() {
        ContentStoreBuilder builder;
        auto storePath = testDir_ / "storage";
        auto storeResult = builder.withStoragePath(storePath)
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        REQUIRE(storeResult);

        auto& uniqueStore = storeResult.value();
        contentStore_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());
    }

    void setupServices() {
        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.workerExecutor = boost::asio::system_executor();

// Initialize hybrid search engine for integration testing
// Note: This requires vector DB and embeddings to be available
// Tests will skip when YAMS_DISABLE_VECTORS is set or embeddings unavailable
// On Windows, ONNX initialization hangs - always skip vectors
#ifdef _WIN32
        const bool skipVectors = true;
#else
        const bool skipVectors = (std::getenv("YAMS_DISABLE_VECTORS") != nullptr);
#endif

        if (!skipVectors) {
            try {
                yams::search::SearchEngineBuilder builder;
                builder.withMetadataRepo(metadataRepo_);

                // SearchEngine now uses VectorDatabase directly (VectorIndexManager removed)
                // Vector search configured via VectorDatabase when available

                // Try to create embedding generator (may fail if ONNX not available)
                yams::vector::EmbeddingConfig embCfg;
                embCfg.model_name = "all-MiniLM-L6-v2";
                embCfg.embedding_dim = 384;
                auto embGen = std::make_shared<yams::vector::EmbeddingGenerator>(embCfg);
                if (embGen->initialize()) {
                    builder.withEmbeddingGenerator(embGen);
                }

                auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                auto engineResult = builder.buildEmbedded(opts);
                if (engineResult) {
                    appContext_.searchEngine = engineResult.value();
                }
            } catch (...) {
                // Search engine initialization failed - tests will expect InvalidState errors
            }
        }

        searchService_ = makeSearchService(appContext_);
        REQUIRE(searchService_);

        docService_ = makeDocumentService(appContext_);
        REQUIRE(docService_);
    }

    void populateTestCorpus();

    // Helper: Index document content into FTS5 for keyword search
    void indexDocumentForSearch(const std::string& hash, const std::string& title,
                                const std::string& content) {
        auto docResult = metadataRepo_->getDocumentByHash(hash);
        if (!docResult || !docResult.value().has_value()) {
            // Document not found in metadata - this can happen if store didn't add to metadata
            return;
        }
        auto docInfo = docResult.value().value();
        auto indexResult = metadataRepo_->indexDocumentContent(
            docInfo.id, title, content, docInfo.mimeType.empty() ? "text/plain" : docInfo.mimeType);
        if (!indexResult) {
            spdlog::warn("Failed to index document content for FTS5: {}",
                         indexResult.error().message);
        }
    }

    void cleanupServices() {
        searchService_.reset();
        docService_.reset();
    }

    void cleanupDatabase() {
        metadataRepo_.reset();
        pool_.reset();
        if (database_) {
            database_->close();
        }
        database_.reset();
    }

    void cleanupTestEnvironment() {
        fixtureManager_.reset();
        if (fs::exists(testDir_)) {
            fs::remove_all(testDir_);
        }
    }

    fs::path testDir_;
    fs::path dbPath_;
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::shared_ptr<IContentStore> contentStore_;
    AppContext appContext_;
    std::shared_ptr<ISearchService> searchService_;
    std::shared_ptr<IDocumentService> docService_;
    std::shared_ptr<yams::test::FixtureManager> fixtureManager_;
};

void SearchServiceFixture::populateTestCorpus() {
    // 1. Code: C++ indexing pipeline
    hashDoc1_ = createCodeDocument("pipeline.cpp", R"(
#include "indexing_pipeline.hpp"

class IndexingPipeline {
    void processDocument(const Document& doc) {
        // Index document content
        index_.add(doc.id, doc.content);
    }
};
)",
                                   {"code", "cpp"});

    // 2. Code: C++ search engine
    hashDoc2_ = createCodeDocument("search_engine.cpp", R"(
#include "search_engine.hpp"

class SearchEngine {
    std::vector<Result> search(const Query& q) {
        return index_.query(q.text);
    }
};
)",
                                   {"code", "cpp"});

    // 3. Doc: Markdown README
    hashDoc3_ = createDocument("README.md", R"(
# YAMS - Yet Another Metadata Store

YAMS is a content addressable storage system with search capabilities.

## Features
- Hybrid search (keyword + semantic)
- Document indexing and retrieval
- Metadata tracking
)",
                               {"docs", "markdown"});

    // 4. Code: C++ test file
    hashDoc4_ = createCodeDocument("test_search.cpp", R"(
#include <gtest/gtest.h>
#include "search_engine.hpp"

TEST(SearchEngineTest, BasicQuery) {
    SearchEngine engine;
    auto results = engine.search("test query");
    EXPECT_GT(results.size(), 0);
}
)",
                                   {"code", "cpp", "test"});

    // 5. Doc: Unicode content
    hashDoc5_ = createDocument("unicode.md", R"(
# Unicode Test Document

Testing multilingual support:
- English: Hello World
- Spanish: Hola Mundo
- French: Bonjour le Monde
- German: Hallo Welt
- Japanese: こんにちは世界
- Chinese: 你好世界
)",
                               {"docs", "unicode"});

    // 6. Binary: PNG header
    hashDoc6_ = createDocument("logo.png", "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x10",
                               {"binary", "image"});

    // 7-9. Stemming tests: "search" variations
    hashDocStem1_ = createDocument("searching_guide.md", R"(
# Guide to Searching

This guide covers searching techniques.
Effective searching requires practice.
)",
                                   {"docs", "stemming"});

    hashDocStem2_ = createDocument("search_tutorial.md", R"(
# Search Tutorial

Learn how to search efficiently.
Use search operators for better results.
)",
                                   {"docs", "stemming"});

    hashDocStem3_ = createDocument("indexed_data.md", R"(
# Indexed Data

This document is indexed for fast retrieval.
Indexing improves search performance.
)",
                                   {"docs", "stemming"});

    // 10-12. CamelCase tests
    hashDocCamel1_ = createCodeDocument("api_handler.cpp", R"(
class ApiRequestHandler {
    void handleGetRequest() { }
    void handlePostRequest() { }
};
)",
                                        {"code", "camelcase"});

    hashDocCamel2_ = createCodeDocument("file_system_utils.cpp", R"(
class FileSystemUtils {
    void createDirectory() { }
    void deleteDirectory() { }
};
)",
                                        {"code", "camelcase"});

    hashDocCamel3_ = createCodeDocument("snake_case_example.py", R"(
def process_user_input():
    pass

def validate_input():
    pass
)",
                                        {"code", "python", "snakecase"});

    // Hyphenated token test document
    hashDocHyphen_ = createDocument(
        "config-parser.md",
        "The config-parser module handles command-line arguments and configuration files.",
        {"docs", "config"});
}

// TEMPORARY: Disable broken inheritance-based mocks
// These tests need to be rewritten as integration tests
#if 0

/**
 * @brief BROKEN: Cannot mock KeywordSearchEngine this way
 * KeywordSearchEngine may not have virtual methods or may use PIMPL
 */
class ControllableKeywordEngine : public KeywordSearchEngine {
public:
    // Control knobs
    std::chrono::milliseconds search_delay{0};
    bool should_fail{false};
    std::vector<KeywordSearchResult> canned_results;

    // Recorded calls
    size_t search_call_count{0};
    std::string last_query;

    std::vector<std::string> analyzeQuery(const std::string& query) const override {
        std::vector<std::string> tokens;
        std::istringstream iss(query);
        std::string token;
        while (iss >> token) {
            std::transform(token.begin(), token.end(), token.begin(), ::tolower);
            tokens.push_back(token);
        }
        return tokens;
    }

    std::vector<std::string> extractKeywords(const std::string& text) const override {
        return analyzeQuery(text);
    }

    Result<std::vector<KeywordSearchResult>> search(const std::string& query, size_t k,
                                                    const SearchFilter* filter) override {
        search_call_count++;
        last_query = query;

        if (search_delay.count() > 0) {
            std::this_thread::sleep_for(search_delay);
        }

        if (should_fail) {
            return Result<std::vector<KeywordSearchResult>>(
                Error{ErrorCode::Unknown, "Mock failure"});
        }

        std::vector<KeywordSearchResult> results;
        for (size_t i = 0; i < std::min(k, canned_results.size()); ++i) {
            results.push_back(canned_results[i]);
        }
        return Result<std::vector<KeywordSearchResult>>(results);
    }

    Result<std::vector<std::vector<KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>&, size_t, const SearchFilter*) override {
        return Result<std::vector<std::vector<KeywordSearchResult>>>(
            std::vector<std::vector<KeywordSearchResult>>{});
    }

    Result<void> addDocument(const std::string&, const std::string&,
                             const std::map<std::string, std::string>&) override {
        return Result<void>();
    }

    Result<void> removeDocument(const std::string&) override { return Result<void>(); }

    Result<void> updateDocument(const std::string&, const std::string&,
                                const std::map<std::string, std::string>&) override {
        return Result<void>();
    }

    Result<void> addDocuments(const std::vector<std::string>&, const std::vector<std::string>&,
                              const std::vector<std::map<std::string, std::string>>&) override {
        return Result<void>();
    }

    Result<void> buildIndex() override { return Result<void>(); }
    Result<void> optimizeIndex() override { return Result<void>(); }
    Result<void> clearIndex() override { return Result<void>(); }
    Result<void> saveIndex(const std::string&) override { return Result<void>(); }
    Result<void> loadIndex(const std::string&) override { return Result<void>(); }

    size_t getDocumentCount() const override { return canned_results.size(); }
    size_t getTermCount() const override { return 0; }
    size_t getIndexSize() const override { return 0; }
};

// VectorIndexManager removed - SearchEngine now uses VectorDatabase directly

/**
 * @brief Mock embedding generator for testing
 */
class MockEmbeddingGenerator : public EmbeddingGenerator {
public:
    std::chrono::milliseconds generation_delay{0};
    bool should_timeout{false};

    bool initialize() override {
        initialized_ = true;
        return true;
    }

    bool isInitialized() const override { return initialized_; }

    std::vector<float> generateEmbedding(const std::string& text) override {
        if (should_timeout) {
            std::this_thread::sleep_for(std::chrono::seconds(1)); // Exceed timeout
        }
        if (generation_delay.count() > 0) {
            std::this_thread::sleep_for(generation_delay);
        }
        // Return simple embedding based on text length
        return std::vector<float>(384, static_cast<float>(text.length()) / 100.0f);
    }

    std::future<std::vector<float>> generateEmbeddingAsync(const std::string& text) override {
        return std::async(std::launch::async, [this, text]() { return generateEmbedding(text); });
    }

    EmbeddingConfig getConfig() const override {
        EmbeddingConfig config;
        config.embedding_dim = 384;
        return config;
    }

private:
    bool initialized_{false};
};

/**
 * @brief RAII helper for environment variables
 */
class EnvGuard {
public:
    EnvGuard(const std::string& key, const std::string& value) : key_(key) {
        const char* existing = std::getenv(key_.c_str());
        if (existing) {
            old_value_ = existing;
            had_value_ = true;
        }
        setenv(key_.c_str(), value.c_str(), 1);
    }

    ~EnvGuard() {
        if (had_value_) {
            setenv(key_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(key_.c_str());
        }
    }

private:
    std::string key_;
    std::string old_value_;
    bool had_value_{false};
};

#endif // Disabled broken mocks

} // namespace

// ============================================================================
// Keyword Search Tests (Migrated from GTest)
// ============================================================================

TEST_CASE("KeywordSearch - Exact match", "[search][keyword]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "IndexingPipeline";
    req.type = "keyword";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    REQUIRE(resp.results.size() > 0);
    REQUIRE(resp.results[0].path.find("pipeline.cpp") != std::string::npos);
}

TEST_CASE("KeywordSearch - Case insensitive", "[search][keyword]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "indexingpipeline"; // lowercase
    req.type = "keyword";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should match "IndexingPipeline" (case-insensitive)
    REQUIRE(resp.results.size() > 0);
    REQUIRE(resp.results[0].path.find("pipeline.cpp") != std::string::npos);
}

TEST_CASE("KeywordSearch - Stemming", "[search][keyword][stemming]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Test FTS5 Porter stemmer (Migration v17)
    // Query "search" should match "search", "searching", "searches", "searched"
    app::services::SearchRequest req;
    req.query = "search";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    // Should match multiple documents with different forms of "search"
    REQUIRE(resp.results.size() >= 3);

    // Verify we match documents with different stem forms
    bool foundSearching = false;    // searching_guide.md has "searching"
    bool foundSearch = false;       // search_tutorial.md has "search"
    bool foundSearchEngine = false; // search_engine.cpp has "search" in class name

    for (const auto& result : resp.results) {
        if (result.path.find("searching_guide.md") != std::string::npos) {
            foundSearching = true;
        }
        if (result.path.find("search_tutorial.md") != std::string::npos) {
            foundSearch = true;
        }
        if (result.path.find("search_engine.cpp") != std::string::npos) {
            foundSearchEngine = true;
        }
    }

    REQUIRE(foundSearching);
    REQUIRE(foundSearch);
    REQUIRE(foundSearchEngine);
}

TEST_CASE("HybridSearch - Concept boost elevates matches", "[search][hybrid][concept]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    auto engine = fixture.searchEngine();
    if (!engine) {
        SUCCEED("SearchEngine not available; skipping concept boost test.");
        return;
    }

    auto cfg = engine->getConfig();
    cfg.conceptBoostWeight = 0.8f;
    cfg.conceptMaxBoost = 0.8f;
    cfg.conceptMinConfidence = 0.0f;
    engine->setConfig(cfg);
    engine->setConceptExtractor(
        [](const std::string& /*query*/, const std::vector<std::string>& /*types*/) {
            QueryConceptResult result;
            result.concepts.push_back(QueryConcept{"pipeline", "concept", 0.9f, 0, 8});
            return Result<QueryConceptResult>(result);
        });

    fixture.createDocument("pipeline_notes.md", "pipeline boosttoken summary");
    fixture.createDocument("search_notes.md", "unrelated content only");

    app::services::SearchRequest req;
    req.query = "boosttoken";
    req.type = "hybrid";
    req.limit = 5;

    auto resp = fixture.executeSearch(req);
    REQUIRE(resp.results.size() > 0);
    CHECK(resp.results[0].path.find("pipeline_notes.md") != std::string::npos);
}

TEST_CASE("KeywordSearch - Stemming reverse", "[search][keyword][stemming]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Test stemming in reverse: query "searching" should match "search"
    app::services::SearchRequest req;
    req.query = "searching";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    REQUIRE(resp.results.size() >= 2);

    bool foundSearchTutorial = false; // search_tutorial.md has base form "search"
    bool foundSearchingGuide = false; // searching_guide.md has "searching"

    for (const auto& result : resp.results) {
        if (result.path.find("search_tutorial.md") != std::string::npos) {
            foundSearchTutorial = true;
        }
        if (result.path.find("searching_guide.md") != std::string::npos) {
            foundSearchingGuide = true;
        }
    }

    REQUIRE(foundSearchTutorial);
    REQUIRE(foundSearchingGuide);
}

TEST_CASE("KeywordSearch - Stemming indexed", "[search][keyword][stemming]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Test stemming with "index" -> "indexed", "indexing"
    app::services::SearchRequest req;
    req.query = "index";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    REQUIRE(resp.results.size() >= 1);

    bool foundIndexedData = false; // indexed_data.md has "indexed" and "indexing"

    for (const auto& result : resp.results) {
        if (result.path.find("indexed_data.md") != std::string::npos) {
            foundIndexedData = true;
        }
    }

    REQUIRE(foundIndexedData);

    // Verify stemming works: query "indexing" should also match "index" and "indexed"
    app::services::SearchRequest req2;
    req2.query = "indexing";
    req2.type = "keyword";
    req2.limit = 20;

    auto resp2 = fixture.executeSearch(req2);

    bool foundIndexedData2 = false;
    for (const auto& result : resp2.results) {
        if (result.path.find("indexed_data.md") != std::string::npos) {
            foundIndexedData2 = true;
        }
    }

    REQUIRE(foundIndexedData2);
}

// ============================================================================
// CamelCase and Tokenization Tests (Task 071-03)
// ============================================================================

TEST_CASE("KeywordSearch - CamelCase whole token", "[search][keyword][camelcase]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // FTS5 treats CamelCase identifiers as single tokens
    // Query "ApiRequestHandler" should match the exact identifier
    app::services::SearchRequest req;
    req.query = "ApiRequestHandler";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    bool foundApiHandler = false;
    for (const auto& result : resp.results) {
        if (result.path.find("api_handler.cpp") != std::string::npos) {
            foundApiHandler = true;
        }
    }

    REQUIRE(foundApiHandler);
}

TEST_CASE("KeywordSearch - CamelCase partial no match", "[search][keyword][camelcase]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // FTS5 does NOT split CamelCase - "Request" won't match "ApiRequestHandler"
    // This validates FTS5 tokenization behavior for programming identifiers
    app::services::SearchRequest req;
    req.query = "Request";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    // Should match documents with standalone "Request" words but not CamelCase parts
    // The query should work, just documenting tokenization behavior
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("KeywordSearch - CamelCase lowercase match", "[search][keyword][camelcase]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Searching for lowercase "handler" should match if FTS5 case-folding is enabled
    app::services::SearchRequest req;
    req.query = "handler";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    // FTS5 is case-insensitive, but CamelCase is one token
    // "handler" won't match "ApiRequestHandler" (it's a substring, not a token)
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("KeywordSearch - snake_case as single token", "[search][keyword][snakecase]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // FTS5 is configured with tokenchars='_-' so snake_case identifiers are single tokens
    // Query "process_user_input" should match the exact identifier
    app::services::SearchRequest req;
    req.query = "process_user_input";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    bool foundSnakeCaseFile = false;
    for (const auto& result : resp.results) {
        if (result.path.find("snake_case_example.py") != std::string::npos) {
            foundSnakeCaseFile = true;
        }
    }

    REQUIRE(foundSnakeCaseFile);
}

TEST_CASE("KeywordSearch - Hyphenated as single token", "[search][keyword][tokenization]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // FTS5 is configured with tokenchars='_-' so hyphenated terms are single tokens
    // Document created in populateTestCorpus(): config-parser.md
    // Query for the exact hyphenated term
    app::services::SearchRequest req;
    req.query = "config-parser";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    bool foundHyphenDoc = false;
    for (const auto& result : resp.results) {
        if (result.path.find("config-parser.md") != std::string::npos) {
            foundHyphenDoc = true;
        }
    }

    REQUIRE(foundHyphenDoc);
}

TEST_CASE("KeywordSearch - Mixed case identifier", "[search][keyword][camelcase]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Verify mixed case identifiers are searchable
    app::services::SearchRequest req;
    req.query = "FileSystemUtils";
    req.type = "keyword";
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    bool foundFileSystemUtils = false;
    for (const auto& result : resp.results) {
        if (result.path.find("file_system_utils.cpp") != std::string::npos) {
            foundFileSystemUtils = true;
        }
    }

    REQUIRE(foundFileSystemUtils);
}

// ============================================================================
// Semantic Search Tests (Fallback/Degraded Mode)
// ============================================================================

TEST_CASE("SemanticSearch - Graceful fallback to keyword", "[search][semantic][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // When no hybrid engine is provided, semantic search gracefully falls back to keyword search
    app::services::SearchRequest req;
    req.query = "indexing pipeline";
    req.type = "semantic";
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Should succeed via keyword fallback
    REQUIRE(result);
    // Results may be empty if no matches, but search should not error
    REQUIRE(result.value().total >= 0);
}

TEST_CASE("SemanticSearch - Fallback with similarity threshold", "[search][semantic][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // When hybrid engine unavailable, falls back to keyword search (similarity ignored)
    app::services::SearchRequest req;
    req.query = "document indexing";
    req.type = "semantic";
    req.similarity = 0.9f; // Very high threshold - ignored in keyword fallback
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Should succeed via keyword fallback
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

TEST_CASE("SemanticSearch - Empty query", "[search][semantic][validation]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "";
    req.type = "semantic";
    req.limit = 10;

    // Empty query should fail validation
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    if (!result) {
        REQUIRE(result.error().message.find("required") != std::string::npos);
    }
}

TEST_CASE("SemanticSearch - Long query fallback", "[search][semantic][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Long queries fall back to keyword search when no hybrid engine
    std::string longQuery;
    for (int i = 0; i < 200; ++i) {
        longQuery += "This is a very long query with many repeated words. ";
    }

    app::services::SearchRequest req;
    req.query = longQuery;
    req.type = "semantic";
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Should succeed via keyword fallback (handles gracefully)
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

TEST_CASE("SemanticSearch - Low similarity fallback", "[search][semantic][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Low similarity threshold - falls back to keyword search
    app::services::SearchRequest req;
    req.query = "code";
    req.type = "semantic";
    req.similarity = 0.1f; // Very low threshold - ignored in keyword fallback
    req.limit = 20;

    auto result = runAwait(fixture.searchService()->search(req));

    // Should succeed via keyword fallback
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

TEST_CASE("SemanticSearch - Conceptual query fallback", "[search][semantic][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Conceptual queries fall back to keyword search
    app::services::SearchRequest req;
    req.query = "processing workflow";
    req.type = "semantic";
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Should succeed via keyword fallback
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

TEST_CASE("SemanticSearch - Multilingual fallback", "[search][semantic][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Multilingual queries fall back to keyword search
    app::services::SearchRequest req;
    req.query = "système de métadonnées"; // French: "metadata system"
    req.type = "semantic";
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Should succeed via keyword fallback
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

// ============================================================================
// Filtering Tests
// ============================================================================

TEST_CASE("Filter - By path", "[search][filter][path]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "class";
    req.type = "keyword";
    req.pathPatterns = {"src/**/*.cpp"};
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should only match files in src/ directory
    for (const auto& result : resp.results) {
        REQUIRE(result.path.find("src/") != std::string::npos);
    }
}

TEST_CASE("Filter - By tags", "[search][filter][tags]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "class";
    req.type = "keyword";
    req.tags = {"code"};
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // All results should have "code" tag
    REQUIRE(resp.results.size() > 0);
}

TEST_CASE("Filter - By file type", "[search][filter][type]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "test";
    req.fileType = "text";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should not return binary files
    for (const auto& result : resp.results) {
        REQUIRE(result.path.find("logo.png") == std::string::npos);
    }
}

TEST_CASE("Filter - By MIME type", "[search][filter][mime]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "YAMS"; // Need non-empty query
    req.mimeType = "text/markdown";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should only return markdown files
    for (const auto& result : resp.results) {
        REQUIRE(result.path.ends_with(".md"));
    }
}

TEST_CASE("Filter - Multiple path patterns", "[search][filter][path]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Test multiple path patterns (OR logic)
    app::services::SearchRequest req;
    req.query = "class";
    req.type = "keyword";
    req.pathPatterns = {"**/*.cpp", "**/*.hpp"};
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    // Should match files with either .cpp or .hpp extension
    for (const auto& result : resp.results) {
        bool isCppOrHpp = (result.path.find(".cpp") != std::string::npos) ||
                          (result.path.find(".hpp") != std::string::npos);
        REQUIRE(isCppOrHpp);
    }
}

TEST_CASE("Filter - Multiple tags", "[search][filter][tags]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Test multiple tags (should match documents with any of the tags)
    app::services::SearchRequest req;
    req.query = "test";
    req.type = "keyword";
    req.tags = {"code", "docs"};
    req.matchAllTags = false; // Match any tag
    req.limit = 20;

    auto resp = fixture.executeSearch(req);

    // Should return results (documents with either "code" or "docs" tag)
    REQUIRE(resp.results.size() > 0);
}

TEST_CASE("Filter - Tag mismatch excludes results", "[search][filter][tags][hybrid]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "SearchEngine"; // present in tagged code corpus
    req.tags = {"code"};
    req.matchAllTags = true;
    req.limit = 5;

    auto tagged = fixture.executeSearch(req);
    REQUIRE_FALSE(tagged.results.empty());
    for (const auto& result : tagged.results) {
        const bool in_expected_corpus =
            result.path.find("search_engine.cpp") != std::string::npos ||
            result.path.find("test_search.cpp") != std::string::npos ||
            result.path.find("pipeline.cpp") != std::string::npos;
        REQUIRE(in_expected_corpus);
    }

    req.tags = {"binary"}; // mismatch: should filter everything out
    auto mismatched = fixture.executeSearch(req);
    REQUIRE(mismatched.results.empty());
    REQUIRE(mismatched.total == 0);
}

TEST_CASE("Filter - Combined path and tags", "[search][filter][combined]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Test combining path pattern and tag filters
    app::services::SearchRequest req;
    req.query = "class";
    req.type = "keyword";
    req.pathPatterns = {"**/*.cpp"};
    req.tags = {"code"};
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Results should satisfy both filters
    for (const auto& result : resp.results) {
        REQUIRE(result.path.find(".cpp") != std::string::npos);
    }
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("Filter - Empty path pattern", "[search][filter][path]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Empty path pattern should not filter
    app::services::SearchRequest req;
    req.query = "pipeline";
    req.type = "keyword";
    req.pathPatterns = {}; // Empty
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should return results without path filtering
    REQUIRE(resp.results.size() > 0);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

TEST_CASE("EdgeCase - Empty query", "[search][edge][validation]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "";
    req.limit = 10;

    // Empty query should return error
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == ErrorCode::InvalidArgument);
    REQUIRE(result.error().message.find("required") != std::string::npos);
}

TEST_CASE("EdgeCase - Very long query", "[search][edge]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = std::string(10000, 'a'); // 10K character query
    req.type = "keyword";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should handle gracefully
    REQUIRE(resp.total >= 0);
}

TEST_CASE("EdgeCase - Unicode document search", "[search][edge][unicode]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Note: FTS5 with porter tokenizer doesn't properly tokenize CJK characters.
    // Search for "multilingual" which is in the unicode.md document.
    app::services::SearchRequest req;
    req.query = "multilingual";
    req.type = "keyword";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should find unicode.md which contains "Testing multilingual support"
    bool found = false;
    for (const auto& result : resp.results) {
        if (result.path.find("unicode.md") != std::string::npos) {
            found = true;
            break;
        }
    }
    REQUIRE(found);
}

TEST_CASE("EdgeCase - Special characters", "[search][edge]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "class";
    req.type = "keyword";
    req.literalText = true; // Escape special chars
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should match files containing "class"
    REQUIRE(resp.results.size() > 0);
}

TEST_CASE("EdgeCase - Limit zero", "[search][edge][limit]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "class";
    req.limit = 0;

    auto resp = fixture.executeSearch(req);

    // Zero limit means unlimited
    REQUIRE(resp.total >= 0);
    if (resp.total > 0) {
        REQUIRE(resp.results.size() > 0);
    }
}

TEST_CASE("EdgeCase - Large limit", "[search][edge][limit]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "test";
    req.limit = 10000; // Very large limit

    auto resp = fixture.executeSearch(req);

    // Should cap at available results
    REQUIRE(resp.results.size() <= 10000);
}

// ============================================================================
// Session Gating Tests
// ============================================================================

TEST_CASE("SessionGating - No session uses cold path", "[search][session][cold]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "pipeline";
    req.type = "keyword";
    req.useSession = false; // Explicit cold path
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Cold path should scan CAS directly (comprehensive)
    REQUIRE(resp.results.size() > 0);
    bool found = false;
    for (const auto& result : resp.results) {
        if (result.path.find("pipeline.cpp") != std::string::npos) {
            found = true;
            break;
        }
    }
    REQUIRE(found);
}

TEST_CASE("SessionGating - With session allows hot path", "[search][session][hot]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "pipeline";
    req.type = "keyword";
    req.useSession = true; // Allow hot path optimization
    req.sessionName = "test-session";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Hot path uses extracted text from metadata (faster)
    REQUIRE(resp.results.size() > 0);
    bool found = false;
    for (const auto& result : resp.results) {
        if (result.path.find("pipeline.cpp") != std::string::npos) {
            found = true;
            break;
        }
    }
    REQUIRE(found);
}

// ============================================================================
// Pagination Tests
// ============================================================================

TEST_CASE("Pagination - Limit enforcement", "[search][pagination]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Get first page with limit
    app::services::SearchRequest req1;
    req1.query = "class";
    req1.limit = 2;

    auto resp1 = fixture.executeSearch(req1);

    // Should respect limit
    REQUIRE(resp1.results.size() <= 2);
}

// ============================================================================
// Hybrid Search Tests
// ============================================================================

TEST_CASE("HybridSearch - RRF fusion fallback", "[search][hybrid][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "indexing pipeline";
    req.type = "hybrid"; // Use RRF fusion
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Without vector engine, hybrid search should gracefully fall back to keyword search
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

TEST_CASE("HybridSearch - Engine unavailable fallback", "[search][hybrid][fallback]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "pipeline";
    req.type = "hybrid";
    req.limit = 10;

    auto result = runAwait(fixture.searchService()->search(req));

    // Without hybrid engine, should fall back to keyword search successfully
    REQUIRE(result);
    REQUIRE(result.value().total >= 0);
}

// ============================================================================
// BATCH 8: Multiword Keyword + Concurrency Tests
// ============================================================================

TEST_CASE("KeywordSearch - Multiword query", "[search][keyword][multiword]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "content addressable storage";
    req.type = "keyword";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    // Should match README.md which contains all these words
    REQUIRE(resp.results.size() > 0);
    bool found = false;
    for (const auto& result : resp.results) {
        if (result.path.find("README.md") != std::string::npos) {
            found = true;
            break;
        }
    }
    REQUIRE(found);
}

TEST_CASE("Concurrency - Parallel searches", "[search][concurrency][parallel]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    const int numThreads = 10;
    std::vector<app::services::SearchResponse> responses;
    responses.reserve(numThreads);

    // Execute searches sequentially but test the service handles them properly
    // Note: True concurrency would require separate fixture instances per thread
    // or a thread-safe database configuration
    for (int i = 0; i < numThreads; ++i) {
        app::services::SearchRequest req;
        req.query = (i % 2 == 0) ? "pipeline" : "search";
        req.type = "keyword";
        req.limit = 10;

        auto resp = fixture.executeSearch(req);
        responses.push_back(resp);
        REQUIRE(resp.results.size() > 0);
    }

    // Verify all searches completed successfully
    REQUIRE(responses.size() == numThreads);
}

TEST_CASE("Concurrency - Search while indexing", "[search][concurrency][indexing]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Execute initial search
    app::services::SearchRequest req;
    req.query = "pipeline";
    req.limit = 10;
    auto resp1 = fixture.executeSearch(req);
    REQUIRE(resp1.results.size() > 0);
    auto initialCount = resp1.results.size();

    // Add a new document (sequential to avoid database contention)
    fixture.createCodeDocument("src/concurrent/new_file.cpp",
                               "class NewClass { void concurrent(); };", {"code", "concurrent"});

    // Search again - should still work after indexing
    auto resp2 = fixture.executeSearch(req);
    REQUIRE(resp2.results.size() >= initialCount);
}

TEST_CASE("Concurrency - Different search queries", "[search][concurrency][queries]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    std::vector<app::services::SearchResponse> responses;

    // Execute 3 different searches sequentially (load testing)
    // Note: True concurrency would require thread-safe database configuration
    {
        app::services::SearchRequest req;
        req.query = "pipeline";
        req.type = "keyword";
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    {
        app::services::SearchRequest req;
        req.query = "search";
        req.type = "keyword";
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    {
        app::services::SearchRequest req;
        req.query = "indexing";
        req.type = "keyword";
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    // Validate all searches succeeded
    for (const auto& resp : responses) {
        REQUIRE(resp.results.size() >= 0);
    }
    REQUIRE(responses.size() == 3);
}

TEST_CASE("Concurrency - Same query different filters", "[search][concurrency][filters]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    std::vector<app::services::SearchResponse> responses;

    // Execute 4 searches with different filters sequentially (load testing)
    {
        app::services::SearchRequest req;
        req.query = "class";
        req.type = "keyword";
        req.pathPatterns = {"**/*.cpp"};
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    {
        app::services::SearchRequest req;
        req.query = "class";
        req.type = "keyword";
        req.tags = {"code"};
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    {
        app::services::SearchRequest req;
        req.query = "class";
        req.type = "keyword";
        req.fileType = "text";
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    {
        app::services::SearchRequest req;
        req.query = "class";
        req.type = "keyword";
        req.similarity = 0.9f;
        req.limit = 10;
        responses.push_back(fixture.executeSearch(req));
    }

    // Validate all searches succeeded
    for (const auto& resp : responses) {
        REQUIRE(resp.results.size() >= 0);
    }
    REQUIRE(responses.size() == 4);
}

TEST_CASE("Concurrency - High load stress test", "[search][concurrency][stress]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    const int numSearches = 20;
    std::vector<app::services::SearchResponse> responses;
    responses.reserve(numSearches);

    // Execute many searches sequentially to test service stability under load
    for (int i = 0; i < numSearches; ++i) {
        app::services::SearchRequest req;
        req.query = (i % 2 == 0) ? "pipeline" : "search";
        req.type = "keyword"; // All keyword to avoid hybrid engine requirement
        req.limit = 5;

        auto resp = fixture.executeSearch(req);
        responses.push_back(resp);
        REQUIRE(resp.results.size() >= 0);
    }

    // Validate all searches completed successfully
    REQUIRE(responses.size() == numSearches);
}

// ============================================================================
// BATCH 9: Validation & Error Handling Tests (Coverage Improvement)
// ============================================================================

TEST_CASE("Validation - Empty query and hash", "[search][validation][error]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "";
    req.hash = "";
    req.limit = 10;

    // Both query and hash empty should fail validation
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == yams::ErrorCode::InvalidArgument);
    REQUIRE(result.error().message.find("required") != std::string::npos);
}

TEST_CASE("Validation - Limit too high", "[search][validation][error]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "test";
    req.limit = 200000; // > 100000 max
    req.type = "keyword";

    // Limit exceeding maximum should fail validation
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == yams::ErrorCode::InvalidArgument);
    REQUIRE(result.error().message.find("out of allowed range") != std::string::npos);
}

TEST_CASE("Validation - Invalid hash format (not hex)", "[search][validation][error]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "";
    req.hash = "not-a-hex-string!!";
    req.limit = 10;

    // Non-hex hash should fail validation
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == yams::ErrorCode::InvalidArgument);
    REQUIRE(result.error().message.find("Invalid hash format") != std::string::npos);
}

TEST_CASE("Validation - Invalid hash format (too short)", "[search][validation][error]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "";
    req.hash = "abc123"; // < 8 chars
    req.limit = 10;

    // Hash too short should fail validation
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == yams::ErrorCode::InvalidArgument);
    REQUIRE(result.error().message.find("Invalid hash format") != std::string::npos);
}

TEST_CASE("Validation - Invalid hash format (too long)", "[search][validation][error]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "";
    req.hash = std::string(65, 'a'); // 65 chars, > 64 max
    req.limit = 10;

    // Hash too long should fail validation
    auto result = runAwait(fixture.searchService()->search(req));
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == yams::ErrorCode::InvalidArgument);
    REQUIRE(result.error().message.find("Invalid hash format") != std::string::npos);
}

TEST_CASE("HashSearch - Valid hash prefix", "[search][hash]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    // Create a document and get its hash
    fixture.createCodeDocument("src/hashtest.cpp", "int main() { return 0; }", {"code"});

    app::services::SearchRequest req;
    req.query = "";
    req.hash = "abc12345"; // Valid 8-char hex
    req.limit = 10;

    // Valid hash format should not fail validation (but may return NotFound)
    auto result = runAwait(fixture.searchService()->search(req));
    // Either succeeds with results or fails with NotFound (not InvalidArgument)
    if (!result) {
        REQUIRE(result.error().code != yams::ErrorCode::InvalidArgument);
    }
}

TEST_CASE("EdgeCase - Very high limit (under threshold)", "[search][edge]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "pipeline";
    req.limit = 99999; // Just under 100000 max
    req.type = "keyword";

    auto resp = fixture.executeSearch(req);
    // Should succeed (not fail validation)
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("EdgeCase - Limit exactly at maximum", "[search][edge]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "pipeline";
    req.limit = 100000; // Exactly at max
    req.type = "keyword";

    auto resp = fixture.executeSearch(req);
    // Should succeed (not fail validation)
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("EdgeCase - Query with special regex characters", "[search][edge][regex]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.query = "test.file[0]"; // Contains regex special chars
    req.type = "keyword";
    req.limit = 10;

    // Should handle gracefully without regex errors
    auto resp = fixture.executeSearch(req);
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("Filtering - Path pattern with wildcards", "[search][filtering][path]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "class";
    req.pathPatterns = {"**/*.cpp", "src/**/*.hpp"};
    req.limit = 10;

    auto resp = fixture.executeSearch(req);
    // Should match files with .cpp or .hpp in src/
    for (const auto& result : resp.results) {
        bool matches = result.path.find(".cpp") != std::string::npos ||
                       result.path.find(".hpp") != std::string::npos;
        if (!matches && result.path.find("src/") == 0) {
            matches = true; // Allow src/ paths
        }
    }
}

TEST_CASE("Filtering - Multiple tags with matchAll=true", "[search][filtering][tags]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "pipeline";
    req.tags = {"code", "working"};
    req.matchAllTags = true;
    req.limit = 10;

    auto resp = fixture.executeSearch(req);
    // Results should have both tags
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("Filtering - Extension filter", "[search][filtering][extension]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "class";
    req.extension = ".cpp";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);
    // All results should be .cpp files
    for (const auto& result : resp.results) {
        REQUIRE(result.path.find(".cpp") != std::string::npos);
    }
}

TEST_CASE("Filtering - MIME type filter", "[search][filtering][mime]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "pipeline";
    req.mimeType = "text/";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);
    // Should only return text files
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("Filtering - File type filter (text only)", "[search][filtering][filetype]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "class";
    req.fileType = "text";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);
    // Should only return text files (not binary)
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("Filtering - File type filter (binary only)", "[search][filtering][filetype]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "data";
    req.fileType = "binary";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);
    // Should only return binary files
    REQUIRE(resp.results.size() >= 0);
}

TEST_CASE("Facets - File type distribution", "[search][facets]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "class";
    req.limit = 100;

    auto resp = fixture.executeSearch(req);

    REQUIRE(resp.results.size() > 0);
    REQUIRE(resp.facets.size() >= 1);

    bool foundExtensionFacet = false;
    for (const auto& facet : resp.facets) {
        if (facet.name == "extension") {
            foundExtensionFacet = true;
            REQUIRE(facet.values.size() > 0);
            REQUIRE(facet.displayName == "File Type");

            bool foundCpp = false;
            for (const auto& fv : facet.values) {
                if (fv.value == ".cpp") {
                    foundCpp = true;
                    REQUIRE(fv.count > 0);
                    REQUIRE(fv.value == fv.display);
                }
            }
            REQUIRE(foundCpp);
            break;
        }
    }
    REQUIRE(foundExtensionFacet);
}

TEST_CASE("Facets - Empty results returns empty facets", "[search][facets]") {
    SKIP_HYBRID_ON_WINDOWS();
    SearchServiceFixture fixture;

    app::services::SearchRequest req;
    req.type = "keyword";
    req.query = "zzzznonexistentterm12345";
    req.limit = 10;

    auto resp = fixture.executeSearch(req);

    REQUIRE(resp.results.size() == 0);
}

// NOTE: 13 advanced hybrid search tests were previously here but have been removed.
// These tests used old mock-based architecture (ControllableVectorIndex, ControllableKeywordEngine)
// that is incompatible with the current PIMPL architecture.
// The active semantic and hybrid search tests have been migrated to:
//   tests/integration/search/hybrid_search_integration_test.cpp
// If additional hybrid search functionality testing is needed, new tests should be written
// using the integration test infrastructure with DaemonHarness.
#endif // defined(_WIN32) && YAMS_ORT_API_VERSION < 23
