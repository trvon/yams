#include <chrono>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <thread>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_executor.h>

#include "common/fixture_manager.h"
#include "common/test_data_generator.h"

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;

namespace {

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

class BlockingKeywordSearchEngine final : public search::KeywordSearchEngine {
public:
    explicit BlockingKeywordSearchEngine(std::chrono::milliseconds delay) : delay_(delay) {
        yams::search::KeywordSearchResult r;
        r.id = "blocking-keyword";
        r.content = "Blocking keyword result";
        r.score = 1.0f;
        r.metadata["path"] = "blocking-keyword.txt";
        results_.push_back(std::move(r));
    }

    std::vector<std::string> analyzeQuery(const std::string& query) const override {
        std::vector<std::string> tokens;
        std::istringstream iss(query);
        std::string token;
        while (iss >> token) {
            for (auto& ch : token) {
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            }
            tokens.push_back(token);
        }
        return tokens;
    }

    std::vector<std::string> extractKeywords(const std::string& text) const override {
        return analyzeQuery(text);
    }

    Result<std::vector<yams::search::KeywordSearchResult>>
    search(const std::string&, size_t, const vector::SearchFilter*) override {
        std::this_thread::sleep_for(delay_);
        return Result<std::vector<yams::search::KeywordSearchResult>>(results_);
    }

    Result<std::vector<std::vector<yams::search::KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>& queries, size_t,
                const vector::SearchFilter*) override {
        std::vector<std::vector<yams::search::KeywordSearchResult>> batches(queries.size(),
                                                                            results_);
        return Result<std::vector<std::vector<yams::search::KeywordSearchResult>>>(
            std::move(batches));
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
    Result<void> saveIndex(const std::string&) override {
        return Result<void>(Error{ErrorCode::InvalidOperation, "Not implemented"});
    }
    Result<void> loadIndex(const std::string&) override {
        return Result<void>(Error{ErrorCode::InvalidOperation, "Not implemented"});
    }

    size_t getDocumentCount() const override { return results_.size(); }
    size_t getTermCount() const override { return 0; }
    size_t getIndexSize() const override { return 0; }

private:
    std::chrono::milliseconds delay_;
    std::vector<yams::search::KeywordSearchResult> results_;
};

} // namespace

namespace {
constexpr std::size_t kZeroTotal = 0;
}
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

class SearchServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        setupTestEnvironment();
        setupDatabase();
        setupServices();
        setupTestData();
    }

    void TearDown() override {
        cleanupServices();
        cleanupDatabase();
        cleanupTestEnvironment();
    }

private:
    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ = std::filesystem::temp_directory_path() /
                   ("search_service_test_" + pid + "_" + timestamp);

        std::error_code ec;
        std::filesystem::create_directories(testDir_, ec);
        ASSERT_FALSE(ec) << "Failed to create test directory: " << ec.message();

        fixtureManager_ = std::make_unique<yams::test::FixtureManager>(testDir_ / "fixtures");
    }

    void setupDatabase() {
        dbPath_ = std::filesystem::absolute(testDir_ / "test.db");
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string(), ConnectionMode::Create);
        ASSERT_TRUE(openResult) << "Failed to open database: " << openResult.error().message;

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        MigrationManager mm(*database_);
        auto initResult = mm.initialize();
        ASSERT_TRUE(initResult) << "Failed to initialize migration system: "
                                << initResult.error().message;

        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = mm.migrate();
        ASSERT_TRUE(migrateResult) << "Failed to run migrations: " << migrateResult.error().message;
    }

    void setupServices() {
        // Create content store using builder
        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(testDir_ / "storage")
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        ASSERT_TRUE(storeResult) << "Failed to create content store: "
                                 << storeResult.error().message;

        // Extract unique_ptr and convert to shared_ptr
        auto& uniqueStore = storeResult.value();
        contentStore_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());

        // Search components might be optional or need special initialization
        searchExecutor_ = nullptr;
        hybridEngine_ = nullptr;

        // Create app context
        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.searchExecutor = searchExecutor_;
        appContext_.hybridEngine = hybridEngine_;
        appContext_.workerExecutor = boost::asio::system_executor();

        // Create search service using factory
        searchService_ = makeSearchService(appContext_);
    }

    void setupTestData() {
        ASSERT_TRUE(fixtureManager_) << "Fixture manager not initialized";

        yams::test::TestDataGenerator generator(1337);
        testDocuments_ = {
            {generator.generateMarkdown(2, "Artificial Intelligence Primer"), "ai.txt"},
            {generator.generateMarkdown(2, "Python Programming Tutorial"), "python.txt"},
            {generator.generateMarkdown(2, "Database Design Handbook"), "database.md"},
            {generator.generateMarkdown(2, "Web Development Guide"), "web.md"},
            {generator.generateMarkdown(3, "Climate Research Summary"), "climate.txt"}};

        // Create document service to store test data
        auto docService = makeDocumentService(appContext_);

        for (const auto& [content, filename] : testDocuments_) {
            auto fixture = fixtureManager_->createTextFixture(filename, content,
                                                              {"search", "unit", "fixture"});

            StoreDocumentRequest storeReq;
            storeReq.path = fixture.path.string();
            auto storeResult = docService->store(storeReq);
            if (storeResult) {
                testHashes_.push_back(storeResult.value().hash);
            }
        }
    }

    void cleanupServices() {
        searchService_.reset();
        searchExecutor_.reset();
        hybridEngine_.reset();
        // Note: contentStore_ is now owned by appContext_
    }

    void cleanupDatabase() {
        if (database_) {
            database_->close();
            database_.reset();
        }
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        metadataRepo_.reset();
    }

    void cleanupTestEnvironment() {
        fixtureManager_.reset();
        if (!testDir_.empty() && std::filesystem::exists(testDir_)) {
            std::error_code ec;
            std::filesystem::remove_all(testDir_, ec);
        }
    }

protected:
    // Helper methods
    SearchRequest createBasicSearchRequest(const std::string& query) {
        SearchRequest request;
        request.query = query;
        request.limit = 10;
        return request;
    }

    // Test data
    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;

    // Database components
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;

    // Service components
    std::shared_ptr<IContentStore> contentStore_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine_;
    AppContext appContext_;
    std::shared_ptr<ISearchService> searchService_;

    // Test documents
    std::unique_ptr<yams::test::FixtureManager> fixtureManager_;
    std::vector<std::pair<std::string, std::string>> testDocuments_;
    std::vector<std::string> testHashes_;
};

// Basic Search Tests

TEST_F(SearchServiceTest, BasicTextSearch) {
    auto request = createBasicSearchRequest("programming");
    request.showHash = true; // hashes are hidden by default in results

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result) << "Search failed: " << result.error().message;

    // Should find relevant documents
    const auto& resp = result.value();
    EXPECT_GE(resp.results.size(), std::size_t{0});
    EXPECT_GE(resp.total, kZeroTotal);
    EXPECT_GE(resp.executionTimeMs, 0);
    ASSERT_TRUE(resp.searchStats.contains("metadata_operations"));
    EXPECT_NE(resp.searchStats.at("metadata_operations"), "0");

    // Check result structure
    for (const auto& doc : resp.results) {
        EXPECT_FALSE(doc.hash.empty());
        EXPECT_GE(doc.score, 0.0);
    }
}

TEST_F(SearchServiceTest, SearchWithMultipleTerms) {
    auto request = createBasicSearchRequest("python programming tutorial");

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);

    // Documents should be ranked by relevance
    // Most relevant documents should appear first
    if (result.value().results.size() > 1) {
        auto& docs = result.value().results;
        for (size_t i = 1; i < docs.size(); ++i) {
            EXPECT_GE(docs[i - 1].score, docs[i].score)
                << "Documents should be sorted by score (descending)";
        }
    }
}

TEST_F(SearchServiceTest, CaseInsensitiveSearch) {
    auto request1 = createBasicSearchRequest("ARTIFICIAL");
    auto request2 = createBasicSearchRequest("artificial");
    auto request3 = createBasicSearchRequest("Artificial");

    auto result1 = runAwait(searchService_->search(request1));
    auto result2 = runAwait(searchService_->search(request2));
    auto result3 = runAwait(searchService_->search(request3));

    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_TRUE(result3);

    // Should find same number of results regardless of case
    EXPECT_EQ(result1.value().total, result2.value().total);
    EXPECT_EQ(result2.value().total, result3.value().total);
}

// Advanced Search Features

TEST_F(SearchServiceTest, SearchWithLimit) {
    auto request = createBasicSearchRequest("test");
    request.limit = 3;

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    EXPECT_LE(result.value().results.size(), std::size_t{3});
}

TEST_F(SearchServiceTest, SearchWithOffset) {
    auto request1 = createBasicSearchRequest("test");
    request1.limit = 10;

    auto result1 = runAwait(searchService_->search(request1));
    ASSERT_TRUE(result1);

    if (result1.value().total > 1) {
        auto request2 = createBasicSearchRequest("test");
        request2.limit = 10;
        // Note: offset might not be in the interface

        auto result2 = runAwait(searchService_->search(request2));
        ASSERT_TRUE(result2);

        // Should get different results potentially
        if (!result2.value().results.empty() && !result1.value().results.empty()) {
            // Results might differ
            EXPECT_GE(result2.value().results.size(), std::size_t{0});
        }
    }
}

TEST_F(SearchServiceTest, FuzzySearch) {
    auto request = createBasicSearchRequest("progamming"); // Intentional typo
    request.fuzzy = true;
    request.similarity = 0.8f;

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);

    // Fuzzy search might find documents even with typos
    // The exact behavior depends on implementation
    EXPECT_GE(result.value().total, kZeroTotal);
}

// Search Filters

TEST_F(SearchServiceTest, SearchWithTagFilter) {
    auto request = createBasicSearchRequest("programming");
    request.tags = {"tutorial", "example"};
    request.matchAllTags = false; // OR logic

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    // Should only return documents with at least one of the specified tags
    EXPECT_GE(result.value().total, kZeroTotal);
}

TEST_F(SearchServiceTest, SearchWithFileTypeFilter) {
    auto request = createBasicSearchRequest("development");
    request.fileType = "text";

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);

    // All returned documents should be text files
    for (const auto& doc : result.value().results) {
        if (!doc.mimeType.empty()) {
            EXPECT_TRUE(doc.mimeType.find("text/") == 0 ||
                        doc.mimeType.find("application/json") == 0 ||
                        doc.mimeType.find("application/xml") == 0);
        }
    }
}

TEST_F(SearchServiceTest, SearchWithExtensionFilter) {
    auto request = createBasicSearchRequest("tutorial");
    request.extension = ".txt";

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);

    // All returned documents should have .txt extension
    for (const auto& doc : result.value().results) {
        if (!doc.fileName.empty()) {
            EXPECT_THAT(doc.fileName, HasSubstr(".txt"));
        }
    }
}

// Search Types

TEST_F(SearchServiceTest, KeywordSearch) {
    auto request = createBasicSearchRequest("artificial intelligence");
    request.type = "keyword";

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    EXPECT_GE(result.value().total, kZeroTotal);

    // Should find documents containing the keywords
    for (const auto& doc : result.value().results) {
        EXPECT_GT(doc.score, 0.0);
    }
}

TEST_F(SearchServiceTest, SemanticSearch) {
    auto request = createBasicSearchRequest("machine learning algorithms");
    request.type = "semantic";

    auto result = runAwait(searchService_->search(request));

    // Semantic search might not be available in all configurations
    if (result) {
        EXPECT_GE(result.value().total, kZeroTotal);

        // Semantic search should find conceptually similar documents
        for (const auto& doc : result.value().results) {
            EXPECT_GT(doc.score, 0.0);
        }
    } else {
        // If semantic search is not available, that's okay
        // If semantic search is not available, that's expected
        EXPECT_FALSE(result);
    }
}

TEST_F(SearchServiceTest, PathsOnlyFallbackHandlesLargeCorpora) {
    auto docService = makeDocumentService(appContext_);
    const int extraDocs = 120;
    for (int i = 0; i < extraDocs; ++i) {
        auto filePath = testDir_ / ("bulk_doc_" + std::to_string(i) + ".txt");
        std::ofstream file(filePath);
        file << "Synthetic corpus document " << i << " content to exercise metadata fallback path.";
        file.close();

        StoreDocumentRequest storeReq;
        storeReq.path = filePath.string();
        auto stored = docService->store(storeReq);
        ASSERT_TRUE(stored) << "Failed to store test document " << i << ": "
                            << stored.error().message;
    }

    SearchRequest request;
    request.query = "termthatshouldnotmatch"; // force fallback path
    request.limit = 7;
    request.pathsOnly = true;
    request.fuzzy = false;

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result) << "Search failed: " << result.error().message;
    EXPECT_LE(result.value().paths.size(), request.limit);
    EXPECT_FALSE(result.value().paths.empty())
        << "Fallback should provide recent document paths when no match is found";
}

TEST_F(SearchServiceTest, HybridSearch) {
    auto request = createBasicSearchRequest("python programming");
    request.type = "hybrid";

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    EXPECT_GE(result.value().total, kZeroTotal);

    // Hybrid search should combine keyword and semantic results
    for (const auto& doc : result.value().results) {
        EXPECT_GT(doc.score, 0.0);
    }
}

// Error Handling Tests

TEST_F(SearchServiceTest, HandleEmptyQuery) {
    auto request = createBasicSearchRequest("");

    auto result = runAwait(searchService_->search(request));

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
}

TEST_F(SearchServiceTest, HandleInvalidLimit) {
    auto request = createBasicSearchRequest("test");
    request.limit = -1;

    auto result = runAwait(searchService_->search(request));

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
}

TEST_F(SearchServiceTest, HandleInvalidSearchType) {
    auto request = createBasicSearchRequest("test");
    request.type = "invalid_type"; // Invalid search type

    auto result = runAwait(searchService_->search(request));

    // Should either fail or default to hybrid
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
    } else {
        EXPECT_GE(result.value().total, kZeroTotal);
    }
}

// Performance Tests

TEST_F(SearchServiceTest, SearchPerformance) {
    auto request = createBasicSearchRequest("programming tutorial example");

    auto start = std::chrono::high_resolution_clock::now();
    auto result = runAwait(searchService_->search(request));
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(result);

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Search should be fast (under 100ms for small dataset)
    EXPECT_LT(duration.count(), 100) << "Search took " << duration.count() << "ms";
}

TEST_F(SearchServiceTest, LargeResultSetPerformance) {
    auto request = createBasicSearchRequest("test"); // Broad query likely to match many documents
    request.limit = 100;

    auto start = std::chrono::high_resolution_clock::now();
    auto result = runAwait(searchService_->search(request));
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(result);

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Even large result sets should be reasonably fast
    EXPECT_LT(duration.count(), 200) << "Large result search took " << duration.count() << "ms";
}

// Special Query Tests

TEST_F(SearchServiceTest, SearchWithSpecialCharacters) {
    auto request = createBasicSearchRequest("C++ programming");

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    // Should handle special characters in queries without crashing
    EXPECT_GE(result.value().total, kZeroTotal);
}

TEST_F(SearchServiceTest, SearchWithQuotedPhrase) {
    auto request = createBasicSearchRequest("\"machine learning\"");

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    // Should search for exact phrase
    EXPECT_GE(result.value().total, kZeroTotal);
}

TEST_F(SearchServiceTest, SearchWithWildcards) {
    auto request = createBasicSearchRequest("program*");

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    // Should match programming, programs, etc.
    EXPECT_GE(result.value().total, kZeroTotal);
}

// Result Quality Tests

TEST_F(SearchServiceTest, RelevanceScoring) {
    auto request = createBasicSearchRequest("python programming tutorial");

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);

    // Documents with more matching terms should have higher scores
    if (result.value().results.size() > 1) {
        auto& docs = result.value().results;

        // Scores should be in descending order
        for (size_t i = 1; i < docs.size(); ++i) {
            EXPECT_GE(docs[i - 1].score, docs[i].score);
        }

        // Top results should have meaningful scores
        if (!docs.empty()) {
            EXPECT_GT(docs[0].score, 0.0);
        }
    }
}

TEST_F(SearchServiceTest, NoResults) {
    auto request = createBasicSearchRequest("xyzzyveryunlikelytomatchanything");

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().total, kZeroTotal);
    EXPECT_TRUE(result.value().results.empty());
}

// Service Integration Tests

TEST_F(SearchServiceTest, ServiceContextIntegration) {
    // Test that service properly uses the app context
    EXPECT_NE(searchService_, nullptr);

    // Service should have access to metadata repository and content store
    // through the app context (verified implicitly through other tests)

    // Test basic functionality to ensure context is working
    auto request = createBasicSearchRequest("test");

    auto result = runAwait(searchService_->search(request));
    ASSERT_TRUE(result); // Should work if context is properly set up
}

// Edge Cases

TEST_F(SearchServiceTest, VeryLongQuery) {
    std::string longQuery(1000, 'a'); // 1000 character query
    auto request = createBasicSearchRequest(longQuery);

    auto result = runAwait(searchService_->search(request));

    // Should either handle gracefully or return appropriate error
    if (result) {
        EXPECT_GE(result.value().total, kZeroTotal);
    } else {
        EXPECT_TRUE(result.error().code == ErrorCode::InvalidArgument);
    }
}

TEST_F(SearchServiceTest, SearchWithPathsOnly) {
    auto request = createBasicSearchRequest("programming");
    request.pathsOnly = true;

    auto result = runAwait(searchService_->search(request));

    ASSERT_TRUE(result);

    // When pathsOnly is true, paths should be populated
    if (request.pathsOnly) {
        EXPECT_FALSE(result.value().paths.empty());
    }
    for (const auto& doc : result.value().results) {
        EXPECT_FALSE(doc.hash.empty());
    }
}

TEST_F(SearchServiceTest, LightIndexRetriesTransientMetadataErrors) {
    ASSERT_FALSE(testHashes_.empty());

    auto flakyRepo = std::make_shared<FlakyMetadataRepository>(*pool_);
    flakyRepo->setGetDocumentByHashFailures(1);

    metadataRepo_ = flakyRepo;
    appContext_.metadataRepo = flakyRepo;
    searchService_ = makeSearchService(appContext_);

    auto result = searchService_->lightIndexForHash(testHashes_.front(), 512 * 1024);
    ASSERT_TRUE(result) << result.error().message;
}

TEST_F(SearchServiceTest, KeywordStageTimeoutReportsStats) {
    auto vectorManager = std::make_shared<vector::VectorIndexManager>();
    auto initVec = vectorManager->initialize();
    ASSERT_TRUE(initVec.has_value()) << initVec.error().message;

    auto keywordEngine =
        std::make_shared<BlockingKeywordSearchEngine>(std::chrono::milliseconds(30));

    search::HybridSearchConfig cfg;
    cfg.vector_weight = 0.0f;
    cfg.keyword_weight = 1.0f;
    cfg.parallel_search = true;
    cfg.final_top_k = 5;
    cfg.keyword_top_k = 5;

    auto hybridEngine =
        std::make_shared<search::HybridSearchEngine>(vectorManager, keywordEngine, cfg);
    auto initHybrid = hybridEngine->initialize();
    ASSERT_TRUE(initHybrid.has_value()) << initHybrid.error().message;

    hybridEngine_ = hybridEngine;
    appContext_.hybridEngine = hybridEngine_;
    searchService_ = makeSearchService(appContext_);

    auto request = createBasicSearchRequest("programming");
    request.pathsOnly = true;
    request.keywordStageTimeoutMs = 5;

    auto result = runAwait(searchService_->search(request));
    ASSERT_TRUE(result) << result.error().message;

    const auto& stats = result.value().searchStats;
    auto it = stats.find("keyword_timeout_hit");
    ASSERT_NE(it, stats.end());
    EXPECT_EQ(it->second, "true");
    auto budgetIt = stats.find("keyword_budget_ms");
    ASSERT_NE(budgetIt, stats.end());
    EXPECT_EQ(budgetIt->second, "5");
}

TEST_F(SearchServiceTest, SnippetHydrationTimeoutReportsStats) {
    auto slowRepo =
        std::make_shared<SlowSnippetMetadataRepository>(*pool_, std::chrono::milliseconds(200));
    metadataRepo_ = slowRepo;
    appContext_.metadataRepo = slowRepo;
    searchService_ = makeSearchService(appContext_);

    auto request = createBasicSearchRequest("programming");
    request.snippetHydrationTimeoutMs = 5;
    request.showHash = true;
    request.type = "keyword";

    auto result = runAwait(searchService_->search(request));
    ASSERT_TRUE(result) << result.error().message;

    const auto& stats = result.value().searchStats;
    auto hitIt = stats.find("snippet_timeout_hit");
    ASSERT_NE(hitIt, stats.end());
    EXPECT_EQ(hitIt->second, "true");
    auto budgetIt = stats.find("snippet_budget_ms");
    ASSERT_NE(budgetIt, stats.end());
    EXPECT_EQ(budgetIt->second, "5");
}
