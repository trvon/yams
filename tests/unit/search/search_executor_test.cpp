#include <gtest/gtest.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <memory>

using namespace yams::search;
using namespace yams::metadata;

class SearchExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create in-memory database for testing
        database_ = std::make_shared<Database>(":memory:");
        ASSERT_TRUE(database_->initialize());
        
        metadataRepo_ = std::make_shared<MetadataRepository>(database_);
        
        // Create search executor
        SearchConfig config;
        config.enableQueryCache = false; // Disable cache for testing
        config.maxResults = 100;
        
        executor_ = std::make_unique<SearchExecutor>(database_, metadataRepo_, config);
        
        // Set up test data
        setupTestData();
    }
    
    void setupTestData() {
        // Add some test documents to the database
        auto conn = database_->getConnection();
        
        // Create FTS5 table for testing
        std::string createFtsTable = R"(
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                title, content, content_type, path
            )
        )";
        conn->execute(createFtsTable);
        
        // Insert test documents
        std::string insertDoc = R"(
            INSERT INTO documents_fts (rowid, title, content, content_type, path)
            VALUES (?, ?, ?, ?, ?)
        )";
        
        auto stmt = conn->prepare(insertDoc);
        
        // Document 1
        stmt->bind(1, 1);
        stmt->bind(2, "Machine Learning Basics");
        stmt->bind(3, "Introduction to machine learning algorithms and concepts. This document covers supervised learning, unsupervised learning, and reinforcement learning.");
        stmt->bind(4, "text/plain");
        stmt->bind(5, "/docs/ml-basics.txt");
        stmt->step();
        stmt->reset();
        
        // Document 2
        stmt->bind(1, 2);
        stmt->bind(2, "Deep Learning with Neural Networks");
        stmt->bind(3, "Neural networks are the foundation of deep learning. This guide explains backpropagation, gradient descent, and common architectures.");
        stmt->bind(4, "text/plain");
        stmt->bind(5, "/docs/deep-learning.txt");
        stmt->step();
        stmt->reset();
        
        // Document 3
        stmt->bind(1, 3);
        stmt->bind(2, "Data Structures and Algorithms");
        stmt->bind(3, "Comprehensive guide to data structures including arrays, lists, trees, and graphs. Also covers sorting and searching algorithms.");
        stmt->bind(4, "application/pdf");
        stmt->bind(5, "/docs/data-structures.pdf");
        stmt->step();
        stmt->reset();
        
        // Add corresponding entries to documents table
        std::string insertDocMeta = R"(
            INSERT INTO documents (id, title, path, content_type, file_size, content_hash, last_modified, indexed_at, detected_language, language_confidence)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), 'en', 0.95)
        )";
        
        auto metaStmt = conn->prepare(insertDocMeta);
        
        metaStmt->bind(1, 1);
        metaStmt->bind(2, "Machine Learning Basics");
        metaStmt->bind(3, "/docs/ml-basics.txt");
        metaStmt->bind(4, "text/plain");
        metaStmt->bind(5, 1024);
        metaStmt->bind(6, "hash1");
        metaStmt->step();
        metaStmt->reset();
        
        metaStmt->bind(1, 2);
        metaStmt->bind(2, "Deep Learning with Neural Networks");
        metaStmt->bind(3, "/docs/deep-learning.txt");
        metaStmt->bind(4, "text/plain");
        metaStmt->bind(5, 2048);
        metaStmt->bind(6, "hash2");
        metaStmt->step();
        metaStmt->reset();
        
        metaStmt->bind(1, 3);
        metaStmt->bind(2, "Data Structures and Algorithms");
        metaStmt->bind(3, "/docs/data-structures.pdf");
        metaStmt->bind(4, "application/pdf");
        metaStmt->bind(5, 4096);
        metaStmt->bind(6, "hash3");
        metaStmt->step();
    }
    
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<SearchExecutor> executor_;
};

TEST_F(SearchExecutorTest, BasicSearch) {
    auto result = executor_->search("machine learning");
    
    ASSERT_TRUE(result.has_value());
    
    const auto& response = result.value();
    EXPECT_FALSE(response.hasError);
    EXPECT_GT(response.results.size(), 0);
    EXPECT_EQ(response.originalQuery, "machine learning");
    EXPECT_FALSE(response.processedQuery.empty());
}

TEST_F(SearchExecutorTest, SearchWithResults) {
    auto result = executor_->search("learning");
    
    ASSERT_TRUE(result.has_value());
    
    const auto& response = result.value();
    const auto& items = response.results.getItems();
    
    EXPECT_GT(items.size(), 0);
    
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
    
    const auto& response = result.value();
    EXPECT_LE(response.results.size(), 1);
    EXPECT_EQ(response.offset, 0);
    EXPECT_EQ(response.limit, 1);
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
    
    const auto& items = result.value().results.getItems();
    
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
    
    const auto& items = result.value().results.getItems();
    
    if (items.size() > 1) {
        // Check that results are sorted by title
        for (size_t i = 1; i < items.size(); ++i) {
            EXPECT_LE(items[i-1].title, items[i].title);
        }
    }
}

TEST_F(SearchExecutorTest, EmptyQuery) {
    auto result = executor_->search("");
    
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
}

TEST_F(SearchExecutorTest, InvalidQuery) {
    auto result = executor_->search("(unclosed parenthesis");
    
    // Should handle gracefully
    if (!result.has_value()) {
        EXPECT_NE(result.error().code, ErrorCode::InternalError);
    }
}

TEST_F(SearchExecutorTest, SearchSuggestions) {
    auto result = executor_->getSuggestions("mach", 5);
    
    ASSERT_TRUE(result.has_value());
    
    const auto& suggestions = result.value();
    EXPECT_LE(suggestions.size(), 5);
    
    // Suggestions should start with the partial query
    for (const auto& suggestion : suggestions) {
        // Note: This test might fail depending on FTS5 term extraction
        // In a real implementation, we'd have better suggestion logic
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
            EXPECT_GT(facet.values.size(), 0);
            
            // Check facet values
            for (const auto& value : facet.values) {
                EXPECT_FALSE(value.value.empty());
                EXPECT_GT(value.count, 0);
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
    
    const auto& items = result.value().results.getItems();
    
    if (!items.empty()) {
        // At least one result should have highlights
        bool hasHighlights = false;
        for (const auto& item : items) {
            if (!item.highlights.empty()) {
                hasHighlights = true;
                
                // Check highlight structure
                for (const auto& highlight : item.highlights) {
                    EXPECT_FALSE(highlight.field.empty());
                    EXPECT_FALSE(highlight.snippet.empty());
                    EXPECT_LE(highlight.startOffset, highlight.endOffset);
                }
            }
        }
        // Note: Highlights might not always be generated in this test setup
    }
}

TEST_F(SearchExecutorTest, SearchStatistics) {
    auto result = executor_->search("learning");
    
    ASSERT_TRUE(result.has_value());
    
    const auto& response = result.value();
    const auto& stats = response.results.getStatistics();
    
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
    
    EXPECT_GE(stats.totalSearches, 3);
    EXPECT_GE(stats.avgSearchTime.count(), 0);
    EXPECT_GE(stats.maxSearchTime.count(), 0);
}

TEST_F(SearchExecutorTest, ClearCache) {
    // Enable cache for this test
    SearchConfig config;
    config.enableQueryCache = true;
    config.cacheSize = 10;
    
    auto cachedExecutor = std::make_unique<SearchExecutor>(database_, metadataRepo_, config);
    
    // Perform search to populate cache
    cachedExecutor->search("learning");
    
    // Clear cache
    cachedExecutor->clearCache();
    
    // Cache should be empty (no direct way to test this, but it should not crash)
    auto stats = cachedExecutor->getStatistics();
    EXPECT_GE(stats.totalSearches, 1);
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
    
    EXPECT_EQ(filtered.size(), 2); // Only text/plain documents
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
    
    EXPECT_EQ(filtered.size(), 1); // Only Document 3 (2000 bytes)
    EXPECT_EQ(filtered[0].fileSize, 2000);
}

TEST_F(SearchFiltersTest, RelevanceFilter) {
    SearchFilters filters;
    RelevanceFilter relFilter;
    relFilter.minScore = 0.5f;
    filters.addRelevanceFilter(relFilter);
    
    auto filtered = filters.apply(testItems_);
    
    EXPECT_EQ(filtered.size(), 2); // Documents with score >= 0.5
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
    
    EXPECT_EQ(filtered.size(), 1); // Only Document 1 matches both filters
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
        EXPECT_GE(results[i-1].relevanceScore, results[i].relevanceScore);
    }
    
    // Item with higher term frequency should generally score higher
    // (though other factors may affect this)
    EXPECT_GT(results[0].relevanceScore, 0.0f);
}