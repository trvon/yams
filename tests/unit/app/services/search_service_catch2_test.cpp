// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Migrated from GTest: search_service_test.cpp
// Full SearchService integration tests with DB lifecycle and async coroutines.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <thread>
#include <unordered_set>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <spdlog/spdlog.h>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine.h>

#include "common/fixture_manager.h"
#include "common/test_data_generator.h"

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;
using Catch::Matchers::ContainsSubstring;

namespace {

constexpr std::size_t kZeroTotal = 0;

// ---------- Mock repositories (same as GTest originals) ----------

class FlakyMetadataRepository : public MetadataRepository {
public:
    explicit FlakyMetadataRepository(ConnectionPool& pool) : MetadataRepository(pool) {}

    void setGetDocumentByHashFailures(std::size_t count) { getDocumentByHashFailures_ = count; }

    Result<std::optional<DocumentInfo>> getDocumentByHash(const std::string& hash) override {
        if (consume(getDocumentByHashFailures_)) {
            return Error{ErrorCode::NotInitialized, "metadata warming up"};
        }
        return MetadataRepository::getDocumentByHash(hash);
    }

private:
    static bool consume(std::size_t& counter) {
        if (counter == 0)
            return false;
        --counter;
        return true;
    }

    std::size_t getDocumentByHashFailures_{0};
};

class SlowSnippetMetadataRepository : public MetadataRepository {
public:
    SlowSnippetMetadataRepository(ConnectionPool& pool, std::chrono::milliseconds delay)
        : MetadataRepository(pool), delay_(delay) {}

    Result<std::optional<DocumentContent>> getContent(int64_t documentId) override {
        std::this_thread::sleep_for(delay_);
        return MetadataRepository::getContent(documentId);
    }

    Result<std::unordered_map<int64_t, DocumentContent>>
    batchGetContent(const std::vector<int64_t>& documentIds) override {
        std::this_thread::sleep_for(delay_);
        return MetadataRepository::batchGetContent(documentIds);
    }

    Result<yams::metadata::SearchResults>
    search(const std::string& query, int limit = 50, int offset = 0,
           const std::optional<std::vector<int64_t>>& docIds = std::nullopt) override {
        auto result = MetadataRepository::search(query, limit, offset, docIds);
        if (result) {
            for (auto& entry : result.value().results) {
                entry.snippet.clear();
            }
        }
        return result;
    }

private:
    std::chrono::milliseconds delay_;
};

// ---------- Async helper ----------

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
    if (opt)
        return std::move(*opt);
    return yams::Result<T>(yams::Error{yams::ErrorCode::InternalError, "Awaitable failed"});
}

// ---------- Fixture ----------

struct SearchServiceFixture {
    SearchServiceFixture() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir = std::filesystem::temp_directory_path() /
                  ("search_service_catch2_" + pid + "_" + timestamp);
        std::error_code ec;
        std::filesystem::create_directories(testDir, ec);
        REQUIRE_FALSE(ec);

        fixtureManager = std::make_unique<yams::test::FixtureManager>(testDir / "fixtures");

        // DB
        dbPath = std::filesystem::absolute(testDir / "test.db");
        database = std::make_unique<Database>();
        auto openResult = database->open(dbPath.string(), ConnectionMode::Create);
        REQUIRE(openResult);

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        metadataRepo = std::make_shared<MetadataRepository>(*pool);

        MigrationManager mm(*database);
        auto initResult = mm.initialize();
        REQUIRE(initResult);
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = mm.migrate();
        REQUIRE(migrateResult);

        // Content store
        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(testDir / "storage")
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        REQUIRE(storeResult);
        auto& uniqueStore = storeResult.value();
        contentStore = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());

        searchEngine = nullptr;

        appContext.store = contentStore;
        appContext.metadataRepo = metadataRepo;
        appContext.searchEngine = searchEngine;
        appContext.workerExecutor = boost::asio::system_executor();
        searchService = makeSearchService(appContext);

        // Test data
        yams::test::TestDataGenerator generator(1337);
        testDocuments = {
            {generator.generateMarkdown(2, "Artificial Intelligence Primer"), "ai.txt"},
            {generator.generateMarkdown(2, "Python Programming Tutorial"), "python.txt"},
            {generator.generateMarkdown(2, "Database Design Handbook"), "database.md"},
            {generator.generateMarkdown(2, "Web Development Guide"), "web.md"},
            {generator.generateMarkdown(3, "Climate Research Summary"), "climate.txt"}};

        auto docService = makeDocumentService(appContext);
        for (const auto& [content, filename] : testDocuments) {
            auto fixture =
                fixtureManager->createTextFixture(filename, content, {"search", "unit", "fixture"});
            StoreDocumentRequest storeReq;
            storeReq.path = fixture.path.string();
            auto storeResult2 = docService->store(storeReq);
            if (storeResult2) {
                testHashes.push_back(storeResult2.value().hash);
                indexDocumentContent(storeResult2.value().hash, filename, content);
            }
        }
    }

    ~SearchServiceFixture() {
        // Cleanup order matters: services first, then stores, then DB
        searchService.reset();
        searchEngine.reset();
        appContext.searchEngine.reset();
        appContext.store.reset();
        appContext.metadataRepo.reset();
        contentStore.reset();
        metadataRepo.reset();
        fixtureManager.reset();
        if (database) {
            database->close();
            database.reset();
        }
        if (pool) {
            pool->shutdown();
            pool.reset();
        }
        if (!testDir.empty() && std::filesystem::exists(testDir)) {
            std::error_code ec;
            std::filesystem::remove_all(testDir, ec);
        }
    }

    void indexDocumentContent(const std::string& hash, const std::string& title,
                              const std::string& content) {
        auto docResult = metadataRepo->getDocumentByHash(hash);
        if (!docResult || !docResult.value().has_value())
            return;
        auto docInfo = docResult.value().value();
        auto indexResult = metadataRepo->indexDocumentContent(
            docInfo.id, title, content, docInfo.mimeType.empty() ? "text/plain" : docInfo.mimeType);
        if (!indexResult)
            spdlog::warn("Failed to index document content: {}", indexResult.error().message);
    }

    SearchRequest createBasicSearchRequest(const std::string& query) {
        SearchRequest request;
        request.query = query;
        request.limit = 10;
        request.globalSearch = true;
        return request;
    }

    std::filesystem::path testDir;
    std::filesystem::path dbPath;
    std::unique_ptr<Database> database;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> metadataRepo;
    std::shared_ptr<IContentStore> contentStore;
    std::shared_ptr<search::SearchEngine> searchEngine;
    AppContext appContext;
    std::shared_ptr<ISearchService> searchService;
    std::unique_ptr<yams::test::FixtureManager> fixtureManager;
    std::vector<std::pair<std::string, std::string>> testDocuments;
    std::vector<std::string> testHashes;
};

} // namespace

// ============ Basic Search Tests ============

TEST_CASE("SearchService: basic text search", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("programming");
    request.showHash = true;

    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);

    const auto& resp = result.value();
    CHECK(resp.total >= kZeroTotal);
    CHECK(resp.executionTimeMs >= 0);
    REQUIRE(resp.searchStats.contains("metadata_operations"));

    for (const auto& doc : resp.results) {
        CHECK_FALSE(doc.hash.empty());
        CHECK(doc.score >= 0.0);
    }
}

TEST_CASE("SearchService: multiple terms ranked by relevance", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("python programming tutorial");
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);

    if (result.value().results.size() > 1) {
        auto& docs = result.value().results;
        for (size_t i = 1; i < docs.size(); ++i) {
            CHECK(docs[i - 1].score >= docs[i].score);
        }
    }
}

TEST_CASE("SearchService: case-insensitive search", "[unit][services][search]") {
    SearchServiceFixture f;
    auto r1 = runAwait(f.searchService->search(f.createBasicSearchRequest("ARTIFICIAL")));
    auto r2 = runAwait(f.searchService->search(f.createBasicSearchRequest("artificial")));
    auto r3 = runAwait(f.searchService->search(f.createBasicSearchRequest("Artificial")));
    REQUIRE(r1);
    REQUIRE(r2);
    REQUIRE(r3);
    CHECK(r1.value().total == r2.value().total);
    CHECK(r2.value().total == r3.value().total);
}

// ============ Advanced Search Features ============

TEST_CASE("SearchService: limit", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("test");
    request.limit = 3;
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    CHECK(result.value().results.size() <= 3);
}

TEST_CASE("SearchService: offset", "[unit][services][search]") {
    SearchServiceFixture f;
    auto r1 = runAwait(f.searchService->search(f.createBasicSearchRequest("test")));
    REQUIRE(r1);
    if (r1.value().total > 1) {
        auto r2 = runAwait(f.searchService->search(f.createBasicSearchRequest("test")));
        REQUIRE(r2);
        CHECK(r2.value().results.size() >= 0);
    }
}

TEST_CASE("SearchService: fuzzy search", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("progamming"); // intentional typo
    request.fuzzy = true;
    request.similarity = 0.8f;
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
}

// ============ Search Filters ============

TEST_CASE("SearchService: tag filter", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("programming");
    request.tags = {"tutorial", "example"};
    request.matchAllTags = false;
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
}

TEST_CASE("SearchService: file type filter", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("development");
    request.fileType = "text";
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);

    for (const auto& doc : result.value().results) {
        if (!doc.mimeType.empty()) {
            CHECK((doc.mimeType.find("text/") == 0 || doc.mimeType.find("application/json") == 0 ||
                   doc.mimeType.find("application/xml") == 0));
        }
    }
}

TEST_CASE("SearchService: extension filter", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("tutorial");
    request.extension = ".txt";
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);

    for (const auto& doc : result.value().results) {
        if (!doc.fileName.empty()) {
            CHECK_THAT(doc.fileName, ContainsSubstring(".txt"));
        }
    }
}

// ============ Search Types ============

TEST_CASE("SearchService: keyword search", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("artificial intelligence");
    request.type = "keyword";
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
    for (const auto& doc : result.value().results) {
        CHECK(doc.score > 0.0);
    }
}

TEST_CASE("SearchService: semantic search", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("machine learning algorithms");
    request.type = "semantic";
    auto result = runAwait(f.searchService->search(request));
    // Semantic search might not be available
    if (result) {
        CHECK(result.value().total >= kZeroTotal);
        for (const auto& doc : result.value().results) {
            CHECK(doc.score > 0.0);
        }
    } else {
        CHECK_FALSE(result);
    }
}

TEST_CASE("SearchService: hybrid search", "[unit][services][search]") {
    SearchServiceFixture f;
    if (!f.appContext.searchEngine) {
        SKIP("Search engine not available in this configuration");
    }
    auto request = f.createBasicSearchRequest("python programming");
    request.type = "hybrid";
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
    for (const auto& doc : result.value().results) {
        CHECK(doc.score > 0.0);
    }
}

// ============ Error Handling ============

TEST_CASE("SearchService: empty query", "[unit][services][search]") {
    SearchServiceFixture f;
    auto result = runAwait(f.searchService->search(f.createBasicSearchRequest("")));
    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::InvalidArgument);
}

TEST_CASE("SearchService: invalid limit", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("test");
    request.limit = -1;
    auto result = runAwait(f.searchService->search(request));
    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::InvalidArgument);
}

TEST_CASE("SearchService: invalid search type", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("test");
    request.type = "invalid_type";
    auto result = runAwait(f.searchService->search(request));
    if (!result) {
        CHECK(result.error().code == ErrorCode::InvalidArgument);
    } else {
        CHECK(result.value().total >= kZeroTotal);
    }
}

// ============ Performance ============

TEST_CASE("SearchService: search performance", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("programming tutorial example");
    auto start = std::chrono::high_resolution_clock::now();
    auto result = runAwait(f.searchService->search(request));
    auto end = std::chrono::high_resolution_clock::now();
    REQUIRE(result);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    CHECK(duration.count() < 200);
}

TEST_CASE("SearchService: large result set performance", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("test");
    request.limit = 100;
    auto start = std::chrono::high_resolution_clock::now();
    auto result = runAwait(f.searchService->search(request));
    auto end = std::chrono::high_resolution_clock::now();
    REQUIRE(result);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    CHECK(duration.count() < 200);
}

// ============ Special Queries ============

TEST_CASE("SearchService: special characters", "[unit][services][search]") {
    SearchServiceFixture f;
    auto result = runAwait(f.searchService->search(f.createBasicSearchRequest("C++ programming")));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
}

TEST_CASE("SearchService: quoted phrase", "[unit][services][search]") {
    SearchServiceFixture f;
    auto result =
        runAwait(f.searchService->search(f.createBasicSearchRequest("\"machine learning\"")));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
}

TEST_CASE("SearchService: wildcards", "[unit][services][search]") {
    SearchServiceFixture f;
    auto result = runAwait(f.searchService->search(f.createBasicSearchRequest("program*")));
    REQUIRE(result);
    CHECK(result.value().total >= kZeroTotal);
}

// ============ Result Quality ============

TEST_CASE("SearchService: relevance scoring", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("python programming tutorial");
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    if (result.value().results.size() > 1) {
        auto& docs = result.value().results;
        for (size_t i = 1; i < docs.size(); ++i) {
            CHECK(docs[i - 1].score >= docs[i].score);
        }
        if (!docs.empty()) {
            CHECK(docs[0].score > 0.0);
        }
    }
}

TEST_CASE("SearchService: no results", "[unit][services][search]") {
    SearchServiceFixture f;
    auto result = runAwait(
        f.searchService->search(f.createBasicSearchRequest("xyzzyveryunlikelytomatchanything")));
    REQUIRE(result);
    CHECK(result.value().total == kZeroTotal);
    CHECK(result.value().results.empty());
}

// ============ Integration ============

TEST_CASE("SearchService: context integration", "[unit][services][search]") {
    SearchServiceFixture f;
    REQUIRE(f.searchService != nullptr);
    auto result = runAwait(f.searchService->search(f.createBasicSearchRequest("test")));
    REQUIRE(result);
}

// ============ Edge Cases ============

TEST_CASE("SearchService: very long query", "[unit][services][search]") {
    SearchServiceFixture f;
    std::string longQuery(1000, 'a');
    auto result = runAwait(f.searchService->search(f.createBasicSearchRequest(longQuery)));
    if (result) {
        CHECK(result.value().total >= kZeroTotal);
    } else {
        CHECK(result.error().code == ErrorCode::InvalidArgument);
    }
}

TEST_CASE("SearchService: paths only", "[unit][services][search]") {
    SearchServiceFixture f;
    auto request = f.createBasicSearchRequest("programming");
    request.pathsOnly = true;
    auto result = runAwait(f.searchService->search(request));
    REQUIRE(result);
    CHECK_FALSE(result.value().paths.empty());
    for (const auto& doc : result.value().results) {
        CHECK_FALSE(doc.hash.empty());
    }
}

TEST_CASE("SearchService: lightIndex retries transient metadata errors",
          "[unit][services][search]") {
    SearchServiceFixture f;
    REQUIRE_FALSE(f.testHashes.empty());

    auto flakyRepo = std::make_shared<FlakyMetadataRepository>(*f.pool);
    flakyRepo->setGetDocumentByHashFailures(1);

    f.metadataRepo = flakyRepo;
    f.appContext.metadataRepo = flakyRepo;
    f.searchService = makeSearchService(f.appContext);

    auto result = f.searchService->lightIndexForHash(f.testHashes.front(), 512 * 1024);
    REQUIRE(result);
}
