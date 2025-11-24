#include <chrono>
#include <filesystem>
#include <memory>
#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif
#include <gtest/gtest.h>
#include <yams/metadata/database.h>
#include <yams/search/parallel_post_processor.hpp>
#include <yams/search/search_executor.h>

using namespace yams::search;
using namespace yams::metadata;

class SearchExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create file-backed test database
        auto temp_dir = std::filesystem::temp_directory_path();
        db_path_ =
            (temp_dir / ("yams_search_exec_test_" + std::to_string(::getpid()) + ".db")).string();
        std::error_code removeEc;
        std::filesystem::remove(db_path_, removeEc);

        // Create a temporary database to verify FTS5 support
        auto ftsProbe = std::make_unique<Database>();
        ASSERT_TRUE(ftsProbe->open(":memory:", ConnectionMode::Create).has_value());
        auto fts5Support = ftsProbe->hasFTS5();
        if (!fts5Support.has_value()) {
            GTEST_SKIP() << "FTS5 detection failed: " << fts5Support.error().message;
        }
        if (!fts5Support.value()) {
            GTEST_SKIP() << "SQLite build lacks FTS5 support; skipping SearchExecutor tests.";
        }
        ftsProbe.reset();

        // Open database connection directly for tests
        database_ = std::make_shared<Database>();
        ASSERT_TRUE(database_->open(db_path_, ConnectionMode::Create).has_value());

        // Create search executor using direct database handle (metadata repo optional)
        SearchConfig config;
        config.enableQueryCache = false; // Disable cache for testing
        config.maxResults = 100;

        executor_ = std::make_unique<SearchExecutor>(database_, nullptr, config);

        // Set up test data
        setupTestData();
    }

    void setupTestData() {
        // Create schema for tests
        ASSERT_TRUE(database_
                        ->execute("CREATE TABLE IF NOT EXISTS documents (\n"
                                  "  id INTEGER PRIMARY KEY,\n"
                                  "  file_path TEXT NOT NULL,\n"
                                  "  file_name TEXT NOT NULL,\n"
                                  "  file_extension TEXT,\n"
                                  "  file_size INTEGER NOT NULL,\n"
                                  "  sha256_hash TEXT UNIQUE NOT NULL,\n"
                                  "  mime_type TEXT,\n"
                                  "  created_time INTEGER,\n"
                                  "  modified_time INTEGER,\n"
                                  "  indexed_time INTEGER,\n"
                                  "  content_extracted INTEGER DEFAULT 0,\n"
                                  "  extraction_status TEXT DEFAULT 'pending',\n"
                                  "  extraction_error TEXT\n"
                                  ")")
                        .has_value());

        ASSERT_TRUE(database_
                        ->execute("CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(\n"
                                  "  title, content\n"
                                  ")")
                        .has_value());

        ASSERT_TRUE(database_
                        ->execute("CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts_vocab USING "
                                  "fts5vocab(documents_fts, 'row')")
                        .has_value());

        // Insert test documents
        auto insertDoc = R"(
            INSERT INTO documents_fts (rowid, title, content)
            VALUES (?, ?, ?)
        )";

        auto stmtRes = database_->prepare(insertDoc);
        if (!stmtRes.has_value()) {
            FAIL() << "prepare(insertDoc) failed: " << stmtRes.error().message;
        }
        auto stmt = std::move(stmtRes.value());

        // Document 1
        ASSERT_TRUE(stmt.bind(1, 1).has_value());
        ASSERT_TRUE(stmt.bind(2, "Machine Learning Basics").has_value());
        ASSERT_TRUE(
            stmt.bind(3, "Introduction to machine learning algorithms and concepts. This document "
                         "covers "
                         "supervised learning, unsupervised learning, and reinforcement learning.")
                .has_value());
        ASSERT_TRUE(stmt.step().has_value());
        ASSERT_TRUE(stmt.reset().has_value());

        // Document 2
        ASSERT_TRUE(stmt.bind(1, 2).has_value());
        ASSERT_TRUE(stmt.bind(2, "Deep Learning with Neural Networks").has_value());
        ASSERT_TRUE(
            stmt.bind(3, "Neural networks are the foundation of deep learning. This guide explains "
                         "backpropagation, gradient descent, and common architectures.")
                .has_value());
        ASSERT_TRUE(stmt.step().has_value());
        ASSERT_TRUE(stmt.reset().has_value());

        // Document 3
        ASSERT_TRUE(stmt.bind(1, 3).has_value());
        ASSERT_TRUE(stmt.bind(2, "Data Structures and Algorithms").has_value());
        ASSERT_TRUE(
            stmt.bind(3,
                      "Comprehensive guide to data structures including arrays, lists, trees, and "
                      "graphs. Also covers sorting and searching algorithms.")
                .has_value());
        ASSERT_TRUE(stmt.step().has_value());
        ASSERT_TRUE(stmt.reset().has_value());

        // Add corresponding entries to documents table
        auto nowSeconds =
            static_cast<int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());

        std::string insertDocMeta = R"(
            INSERT INTO documents (
                id, file_path, file_name, file_extension, file_size, sha256_hash,
                mime_type, created_time, modified_time, indexed_time,
                content_extracted, extraction_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )";

        auto metaStmtRes = database_->prepare(insertDocMeta);
        if (!metaStmtRes.has_value()) {
            FAIL() << "prepare(insertDocMeta) failed: " << metaStmtRes.error().message;
        }
        auto metaStmt = std::move(metaStmtRes.value());

        ASSERT_TRUE(metaStmt.bind(1, 1).has_value());
        ASSERT_TRUE(metaStmt.bind(2, "/docs/ml-basics.txt").has_value());
        ASSERT_TRUE(metaStmt.bind(3, "Machine Learning Basics").has_value());
        ASSERT_TRUE(metaStmt.bind(4, "txt").has_value());
        ASSERT_TRUE(metaStmt.bind(5, 1024).has_value());
        ASSERT_TRUE(metaStmt.bind(6, "hash1").has_value());
        ASSERT_TRUE(metaStmt.bind(7, "text/plain").has_value());
        ASSERT_TRUE(metaStmt.bind(8, nowSeconds).has_value());
        ASSERT_TRUE(metaStmt.bind(9, nowSeconds - 3600).has_value());
        ASSERT_TRUE(metaStmt.bind(10, nowSeconds - 1800).has_value());
        ASSERT_TRUE(metaStmt.bind(11, 1).has_value());
        ASSERT_TRUE(metaStmt.bind(12, "indexed").has_value());
        ASSERT_TRUE(metaStmt.execute().has_value());
        ASSERT_TRUE(metaStmt.reset().has_value());
        ASSERT_TRUE(metaStmt.reset().has_value());

        ASSERT_TRUE(metaStmt.bind(1, 2).has_value());
        ASSERT_TRUE(metaStmt.bind(2, "/docs/deep-learning.txt").has_value());
        ASSERT_TRUE(metaStmt.bind(3, "Deep Learning with Neural Networks").has_value());
        ASSERT_TRUE(metaStmt.bind(4, "txt").has_value());
        ASSERT_TRUE(metaStmt.bind(5, 2048).has_value());
        ASSERT_TRUE(metaStmt.bind(6, "hash2").has_value());
        ASSERT_TRUE(metaStmt.bind(7, "text/plain").has_value());
        ASSERT_TRUE(metaStmt.bind(8, nowSeconds).has_value());
        ASSERT_TRUE(metaStmt.bind(9, nowSeconds - 7200).has_value());
        ASSERT_TRUE(metaStmt.bind(10, nowSeconds - 3600).has_value());
        ASSERT_TRUE(metaStmt.bind(11, 1).has_value());
        ASSERT_TRUE(metaStmt.bind(12, "indexed").has_value());
        ASSERT_TRUE(metaStmt.execute().has_value());
        ASSERT_TRUE(metaStmt.reset().has_value());

        ASSERT_TRUE(metaStmt.bind(1, 3).has_value());
        ASSERT_TRUE(metaStmt.bind(2, "/docs/data-structures.pdf").has_value());
        ASSERT_TRUE(metaStmt.bind(3, "Data Structures and Algorithms").has_value());
        ASSERT_TRUE(metaStmt.bind(4, "pdf").has_value());
        ASSERT_TRUE(metaStmt.bind(5, 4096).has_value());
        ASSERT_TRUE(metaStmt.bind(6, "hash3").has_value());
        ASSERT_TRUE(metaStmt.bind(7, "application/pdf").has_value());
        ASSERT_TRUE(metaStmt.bind(8, nowSeconds).has_value());
        ASSERT_TRUE(metaStmt.bind(9, nowSeconds - 10800).has_value());
        ASSERT_TRUE(metaStmt.bind(10, nowSeconds - 5400).has_value());
        ASSERT_TRUE(metaStmt.bind(11, 0).has_value());
        ASSERT_TRUE(metaStmt.bind(12, "pending").has_value());
        ASSERT_TRUE(metaStmt.execute().has_value());
    }

    void TearDown() override {
        executor_.reset();
        database_.reset();
        std::filesystem::remove(db_path_);
    }

    std::string db_path_;
    std::shared_ptr<Database> database_;
    std::unique_ptr<SearchExecutor> executor_;
};

TEST_F(SearchExecutorTest, BasicSearch) {
    auto result = executor_->search("machine learning");

    ASSERT_TRUE(result.has_value());

    const auto& results = result.value();
    EXPECT_GT(results.size(), 0u);
    // Query normalization details are internal; ensure we have results
}

TEST_F(SearchExecutorTest, SearchWithResults) {
    auto result = executor_->search("learning");

    ASSERT_TRUE(result.has_value());

    const auto& results = result.value();
    const auto& items = results.getItems();

    EXPECT_GT(items.size(), 0u);

    // Check that results contain expected fields
    for (const auto& item : items) {
        EXPECT_FALSE(item.title.empty());
        EXPECT_FALSE(item.path.empty());
        EXPECT_FALSE(item.contentType.empty());
        EXPECT_GE(item.relevanceScore, 0.0f);
        EXPECT_LE(item.relevanceScore, 1.0f);
    }
}

TEST_F(SearchExecutorTest, SearchWithPagination) {
    // Search with pagination
    auto result = executor_->search("learning", 0, 1); // Only 1 result

    ASSERT_TRUE(result.has_value());

    const auto& results = result.value();
    EXPECT_LE(results.size(), 1u);
}

TEST_F(SearchExecutorTest, SearchWithFilters) {
    SearchRequest request;
    request.query = "learning";

    // Add content type filter
    ContentTypeFilter typeFilter;
    typeFilter.allowedTypes.insert("text/plain");
    request.filters.addContentTypeFilter(typeFilter);

    auto result = executor_->search(request);

    ASSERT_TRUE(result.has_value());

    const auto& items = result.value().getItems();

    // All results should be text/plain
    for (const auto& item : items) {
        EXPECT_EQ(item.contentType, "text/plain");
    }
}

TEST_F(SearchExecutorTest, SearchWithSorting) {
    SearchRequest request;
    request.query = "learning";
    request.sortOrder = SearchConfig::SortOrder::TitleAsc;

    auto result = executor_->search(request);

    ASSERT_TRUE(result.has_value());

    const auto& items = result.value().getItems();

    if (items.size() > 1) {
        // Check that results are sorted by title
        for (size_t i = 1; i < items.size(); ++i) {
            EXPECT_LE(items[i - 1].title, items[i].title);
        }
    }
}

TEST_F(SearchExecutorTest, EmptyQuery) {
    auto result = executor_->search("");

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, yams::ErrorCode::InvalidArgument);
}

TEST_F(SearchExecutorTest, InvalidQuery) {
    auto result = executor_->search("(unclosed parenthesis");

    // Should handle gracefully
    if (!result.has_value()) {
        EXPECT_NE(result.error().code, yams::ErrorCode::InternalError);
    }
}

TEST_F(SearchExecutorTest, SearchSuggestions) {
    auto result = executor_->getSuggestions("mach", 5);

    ASSERT_TRUE(result.has_value());

    const auto& suggestions = result.value();
    EXPECT_LE(suggestions.size(), 5u);

    // Suggestions should start with the partial query
    for (const auto& suggestion : suggestions) {
        EXPECT_FALSE(suggestion.empty());
        EXPECT_NE(suggestion.find("mach"), std::string::npos);
    }
}

TEST_F(SearchExecutorTest, SearchFacets) {
    std::vector<std::string> facetFields = {"contentType"};
    auto result = executor_->getFacets("learning", facetFields);

    ASSERT_TRUE(result.has_value());

    const auto& facets = result.value();

    // Should have contentType facet
    bool foundContentTypeFacet = false;
    for (const auto& facet : facets) {
        if (facet.name == "contentType") {
            foundContentTypeFacet = true;
            EXPECT_GT(facet.values.size(), 0u);

            // Check facet values
            for (const auto& value : facet.values) {
                EXPECT_FALSE(value.value.empty());
                EXPECT_GT(value.count, 0u);
            }
        }
    }

    EXPECT_TRUE(foundContentTypeFacet);
}

TEST_F(SearchExecutorTest, SearchWithHighlights) {
    SearchRequest request;
    request.query = "machine learning";
    request.includeHighlights = true;

    auto result = executor_->search(request);

    ASSERT_TRUE(result.has_value());

    const auto& items = result.value().getItems();
    // Highlights may not always be present in this synthetic setup; if present, validate structure
    for (const auto& item : items) {
        for (const auto& highlight : item.highlights) {
            EXPECT_FALSE(highlight.field.empty());
            EXPECT_FALSE(highlight.snippet.empty());
            EXPECT_LE(highlight.startOffset, highlight.endOffset);
        }
    }
}

TEST_F(SearchExecutorTest, SearchStatistics) {
    auto result = executor_->search("learning");

    ASSERT_TRUE(result.has_value());

    const auto& results = result.value();
    const auto& stats = results.getStatistics();

    EXPECT_GE(stats.totalTime.count(), 0);
    EXPECT_GE(stats.searchTime.count(), 0);
    EXPECT_GE(stats.queryTime.count(), 0);
    EXPECT_FALSE(stats.originalQuery.empty());
    EXPECT_FALSE(stats.executedQuery.empty());
}

TEST_F(SearchExecutorTest, ExecutorStatistics) {
    // Perform several searches
    executor_->search("learning");
    executor_->search("neural networks");
    executor_->search("algorithms");

    auto stats = executor_->getStatistics();

    EXPECT_GE(stats.totalSearches, 3u);
    EXPECT_GE(stats.avgSearchTime.count(), 0);
    EXPECT_GE(stats.maxSearchTime.count(), 0);
}

TEST_F(SearchExecutorTest, ClearCache) {
    // Enable cache for this test
    SearchConfig config;
    config.enableQueryCache = true;
    config.cacheSize = 10;

    auto cachedExecutor = std::make_unique<SearchExecutor>(database_, nullptr, config);

    // Perform search to populate cache
    cachedExecutor->search("learning");

    // Clear cache
    cachedExecutor->clearCache();

    // Cache should be empty (no direct way to test this, but it should not crash)
    auto stats = cachedExecutor->getStatistics();
    EXPECT_GE(stats.totalSearches, 1u);
}

// Test search filters in more detail
class SearchFiltersTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create sample search results for filter testing
        SearchResultItem item1;
        item1.documentId = 1;
        item1.title = "Document 1";
        item1.contentType = "text/plain";
        item1.fileSize = 1000;
        item1.relevanceScore = 0.8f;
        item1.lastModified = std::chrono::system_clock::now() - std::chrono::hours(24);

        SearchResultItem item2;
        item2.documentId = 2;
        item2.title = "Document 2";
        item2.contentType = "application/pdf";
        item2.fileSize = 5000;
        item2.relevanceScore = 0.6f;
        item2.lastModified = std::chrono::system_clock::now() - std::chrono::hours(48);

        SearchResultItem item3;
        item3.documentId = 3;
        item3.title = "Document 3";
        item3.contentType = "text/plain";
        item3.fileSize = 2000;
        item3.relevanceScore = 0.4f;
        item3.lastModified = std::chrono::system_clock::now() - std::chrono::hours(12);

        testItems_ = {item1, item2, item3};
    }

    std::vector<SearchResultItem> testItems_;
};

TEST_F(SearchFiltersTest, ContentTypeFilter) {
    SearchFilters filters;
    ContentTypeFilter typeFilter;
    typeFilter.allowedTypes.insert("text/plain");
    filters.addContentTypeFilter(typeFilter);

    auto filtered = filters.apply(testItems_);

    EXPECT_EQ(filtered.size(), 2u); // Only text/plain documents
    for (const auto& item : filtered) {
        EXPECT_EQ(item.contentType, "text/plain");
    }
}

TEST_F(SearchFiltersTest, SizeRangeFilter) {
    SearchFilters filters;
    SizeRangeFilter sizeFilter;
    sizeFilter.minSize = 1500;
    sizeFilter.maxSize = 3000;
    filters.addSizeRangeFilter(sizeFilter);

    auto filtered = filters.apply(testItems_);

    EXPECT_EQ(filtered.size(), 1u); // Only Document 3 (2000 bytes)
    EXPECT_EQ(filtered[0].fileSize, 2000u);
}

TEST_F(SearchFiltersTest, RelevanceFilter) {
    SearchFilters filters;
    RelevanceFilter relFilter;
    relFilter.minScore = 0.5f;
    filters.addRelevanceFilter(relFilter);

    auto filtered = filters.apply(testItems_);

    EXPECT_EQ(filtered.size(), 2u); // Documents with score >= 0.5
    for (const auto& item : filtered) {
        EXPECT_GE(item.relevanceScore, 0.5f);
    }
}

TEST_F(SearchFiltersTest, MultipleFilters) {
    SearchFilters filters;

    ContentTypeFilter typeFilter;
    typeFilter.allowedTypes.insert("text/plain");
    filters.addContentTypeFilter(typeFilter);

    RelevanceFilter relFilter;
    relFilter.minScore = 0.5f;
    filters.addRelevanceFilter(relFilter);

    auto filtered = filters.apply(testItems_);

    EXPECT_EQ(filtered.size(), 1u); // Only Document 1 matches both filters
    EXPECT_EQ(filtered[0].documentId, 1);
}

TEST_F(SearchFiltersTest, NoFilters) {
    SearchFilters filters;

    auto filtered = filters.apply(testItems_);

    EXPECT_EQ(filtered.size(), testItems_.size()); // No filtering
}

// Test result ranker
class ResultRankerTest : public ::testing::Test {
protected:
    void SetUp() override {
        ranker_ = std::make_unique<ResultRanker>();

        // Create test query
        queryParser_ = std::make_unique<QueryParser>();
        auto parseResult = queryParser_->parse("machine learning");
        ASSERT_TRUE(parseResult.has_value());
        testQuery_ = std::move(parseResult.value());

        CorpusStatistics corpusStats;
        corpusStats.totalDocuments = 100;
        corpusStats.termDocumentFrequency["machine"] = 5;
        corpusStats.termDocumentFrequency["learning"] = 5;
        ranker_->setCorpusStatistics(corpusStats);
    }

    std::unique_ptr<ResultRanker> ranker_;
    std::unique_ptr<QueryParser> queryParser_;
    std::unique_ptr<QueryNode> testQuery_;
};

TEST_F(ResultRankerTest, BasicScoring) {
    SearchResultItem item;
    item.title = "Machine Learning Introduction";
    item.termFrequency = 2.0f;
    item.fileSize = 10000;
    item.lastModified = std::chrono::system_clock::now();
    item.languageConfidence = 0.9f;

    float score = ranker_->calculateScore(item, testQuery_.get());

    EXPECT_GE(score, 0.0f);
    EXPECT_LE(score, 1.0f);
}

TEST_F(ResultRankerTest, RankingResults) {
    std::vector<SearchResultItem> results;

    SearchResultItem item1;
    item1.documentId = 1;
    item1.title = "Machine Learning Basics";
    item1.termFrequency = 3.0f;

    SearchResultItem item2;
    item2.documentId = 2;
    item2.title = "Introduction to AI";
    item2.termFrequency = 1.0f;

    SearchResultItem item3;
    item3.documentId = 3;
    item3.title = "Advanced Machine Learning";
    item3.termFrequency = 4.0f;

    results = {item1, item2, item3};

    ranker_->rankResults(results, testQuery_.get());

    // Results should be sorted by relevance score
    for (size_t i = 1; i < results.size(); ++i) {
        EXPECT_GE(results[i - 1].relevanceScore, results[i].relevanceScore);
    }

    // Item with higher term frequency should generally score higher
    // (though other factors may affect this)
    EXPECT_GT(results[0].relevanceScore, 0.0f);
}

// --- Parallel Post-Processor Tests (PBI-001 Phase 3) ---

TEST(ParallelPostProcessorTest, BelowThreshold_UsesSequentialPath) {
    // Create test data below threshold (< 100 results)
    std::vector<SearchResultItem> results;
    for (int i = 0; i < 50; ++i) {
        SearchResultItem item;
        item.documentId = i;
        item.title = "Document " + std::to_string(i);
        item.contentPreview = "Content for document " + std::to_string(i);
        item.contentType = (i % 2 == 0) ? "text/plain" : "text/markdown";
        results.push_back(item);
    }

    // Process without filters or facets (just highlights and snippets)
    auto result = ParallelPostProcessor::process(std::move(results),
                                                 nullptr, // No filters
                                                 {},      // No facet fields
                                                 nullptr, // No query AST (no highlights)
                                                 100,     // Snippet length
                                                 3        // Max highlights
    );

    // Verify results were processed
    EXPECT_EQ(result.filteredResults.size(), 50);
    EXPECT_TRUE(result.snippetsGenerated);
    EXPECT_FALSE(result.highlightsGenerated); // No query AST provided
    EXPECT_TRUE(result.facets.empty());
}

TEST(ParallelPostProcessorTest, AboveThreshold_UsesParallelPath) {
    // Create test data above threshold (>= 100 results)
    std::vector<SearchResultItem> results;
    for (int i = 0; i < 150; ++i) {
        SearchResultItem item;
        item.documentId = i;
        item.title = "Document " + std::to_string(i);
        item.contentPreview = "Content for document " + std::to_string(i);
        item.contentType = (i % 3 == 0)   ? "text/plain"
                           : (i % 3 == 1) ? "text/markdown"
                                          : "text/html";
        item.detectedLanguage = (i % 2 == 0) ? "en" : "es";
        results.push_back(item);
    }

    // Process with facets
    std::vector<std::string> facetFields = {"contentType", "language"};
    auto result =
        ParallelPostProcessor::process(std::move(results), nullptr, facetFields, nullptr, 100, 3);

    // Verify parallel processing occurred
    EXPECT_EQ(result.filteredResults.size(), 150);
    EXPECT_EQ(result.facets.size(), 2); // contentType and language

    // Verify facet generation worked
    bool foundContentTypeFacet = false;
    bool foundLanguageFacet = false;
    for (const auto& facet : result.facets) {
        if (facet.name == "contentType") {
            foundContentTypeFacet = true;
            EXPECT_GT(facet.values.size(), 0);
            // Should have 3 types: text/plain, text/markdown, text/html
            EXPECT_LE(facet.values.size(), 3);
        }
        if (facet.name == "language") {
            foundLanguageFacet = true;
            EXPECT_EQ(facet.values.size(), 2); // en and es
        }
    }
    EXPECT_TRUE(foundContentTypeFacet);
    EXPECT_TRUE(foundLanguageFacet);
}

TEST(ParallelPostProcessorTest, SnippetGeneration) {
    std::vector<SearchResultItem> results;
    SearchResultItem item;
    item.documentId = 1;
    item.title = "Test Document";
    item.contentPreview = "This is a very long content preview that should be truncated to the "
                          "specified snippet length for display purposes.";
    results.push_back(item);

    auto result = ParallelPostProcessor::process(std::move(results), nullptr, {}, nullptr,
                                                 50, // Truncate to 50 chars
                                                 3);

    ASSERT_EQ(result.filteredResults.size(), 1);
    EXPECT_LE(result.filteredResults[0].contentPreview.length(), 53); // 50 + "..."
    EXPECT_TRUE(result.snippetsGenerated);
}

TEST(ParallelPostProcessorTest, FacetValueCounts) {
    std::vector<SearchResultItem> results;
    // Create results with specific distribution
    for (int i = 0; i < 120; ++i) {
        SearchResultItem item;
        item.documentId = i;
        item.title = "Document " + std::to_string(i);
        // 60 text/plain, 40 text/markdown, 20 text/html
        if (i < 60) {
            item.contentType = "text/plain";
        } else if (i < 100) {
            item.contentType = "text/markdown";
        } else {
            item.contentType = "text/html";
        }
        results.push_back(item);
    }

    std::vector<std::string> facetFields = {"contentType"};
    auto result = ParallelPostProcessor::process(std::move(results), nullptr, facetFields, nullptr,
                                                 0, // No snippets
                                                 0);

    ASSERT_EQ(result.facets.size(), 1);
    const auto& facet = result.facets[0];
    EXPECT_EQ(facet.name, "contentType");
    EXPECT_EQ(facet.values.size(), 3);

    // Verify facet values are sorted by count (descending)
    for (size_t i = 1; i < facet.values.size(); ++i) {
        EXPECT_GE(facet.values[i - 1].count, facet.values[i].count);
    }

    // Verify counts
    EXPECT_EQ(facet.values[0].count, 60); // text/plain (most common)
    EXPECT_EQ(facet.values[1].count, 40); // text/markdown
    EXPECT_EQ(facet.values[2].count, 20); // text/html (least common)
}

TEST(ParallelPostProcessorTest, EmptyResults) {
    std::vector<SearchResultItem> results; // Empty

    auto result = ParallelPostProcessor::process(std::move(results), nullptr, {"contentType"},
                                                 nullptr, 100, 3);

    EXPECT_TRUE(result.filteredResults.empty());
    EXPECT_TRUE(result.facets.empty());
}
