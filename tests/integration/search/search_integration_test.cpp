#include <gtest/gtest.h>
#include <yams/search/search_executor.h>
#include <yams/search/search_cache.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/indexing/document_indexer.h>
#include <yams/extraction/text_extractor.h>
#include <filesystem>
#include <fstream>
#include <chrono>

using namespace yams;
using namespace yams::search;
using namespace yams::metadata;
using namespace yams::indexing;
using namespace yams::extraction;
namespace fs = std::filesystem;

class SearchIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        testDir_ = fs::temp_directory_path() / "kronos_search_test";
        fs::create_directories(testDir_);
        
        // Create database
        dbPath_ = testDir_ / "test.db";
        database_ = std::make_shared<Database>(dbPath_.string());
        database_->initialize();
        
        // Initialize components
        repository_ = std::make_shared<MetadataRepository>(database_);
        repository_->initialize();
        
        indexer_ = std::make_shared<DocumentIndexer>(database_);
        parser_ = std::make_shared<QueryParser>();
        
        executor_ = std::make_unique<SearchExecutor>(database_, repository_, 
                                                     indexer_, parser_);
        
        // Set up cache
        SearchCacheConfig cacheConfig;
        cacheConfig.maxEntries = 100;
        cacheConfig.enableStatistics = true;
        cache_ = std::make_unique<SearchCache>(cacheConfig);
        
        // Create test documents
        createTestDocuments();
    }
    
    void TearDown() override {
        executor_.reset();
        cache_.reset();
        indexer_.reset();
        repository_.reset();
        database_.reset();
        
        // Clean up test directory
        fs::remove_all(testDir_);
    }
    
    void createTestDocuments() {
        // Document 1: Technical documentation
        createDocument("technical_guide.txt",
            "Kronos Technical Guide\n\n"
            "This document describes the architecture of the Kronos storage system. "
            "Kronos uses content-addressed storage with advanced compression algorithms "
            "to provide efficient data deduplication. The system supports multiple "
            "compression formats including Zstandard and LZMA.\n\n"
            "Key features include:\n"
            "- High-performance search with SQLite FTS5\n"
            "- Metadata management and indexing\n"
            "- Transparent compression layer\n"
            "- Cache-aware result optimization\n");
        
        // Document 2: User manual
        createDocument("user_manual.md",
            "# Kronos User Manual\n\n"
            "## Getting Started\n"
            "Welcome to Kronos! This manual will help you get started with the system.\n\n"
            "### Installation\n"
            "Follow these steps to install Kronos:\n"
            "1. Download the latest release\n"
            "2. Extract the archive\n"
            "3. Run the installer\n\n"
            "### Basic Usage\n"
            "To search for documents, use the search command with your query.\n"
            "Kronos supports advanced query syntax including boolean operators.\n");
        
        // Document 3: API Reference
        createDocument("api_reference.json",
            "{\n"
            "  \"title\": \"Kronos API Reference\",\n"
            "  \"version\": \"1.0.0\",\n"
            "  \"description\": \"Complete API documentation for Kronos\",\n"
            "  \"endpoints\": [\n"
            "    {\n"
            "      \"path\": \"/search\",\n"
            "      \"method\": \"POST\",\n"
            "      \"description\": \"Execute a search query\"\n"
            "    },\n"
            "    {\n"
            "      \"path\": \"/metadata\",\n"
            "      \"method\": \"GET\",\n"
            "      \"description\": \"Retrieve document metadata\"\n"
            "    }\n"
            "  ]\n"
            "}\n");
        
        // Document 4: Performance report
        createDocument("performance_report.txt",
            "Kronos Performance Analysis Report\n\n"
            "Date: 2024-01-06\n\n"
            "Executive Summary:\n"
            "The latest performance tests show significant improvements in search latency. "
            "Average query response time has been reduced by 40% through the implementation "
            "of result caching and query optimization.\n\n"
            "Key Metrics:\n"
            "- Search throughput: 10,000 queries/second\n"
            "- Average latency: 5ms\n"
            "- Cache hit rate: 85%\n"
            "- Memory usage: 512MB\n\n"
            "Recommendations:\n"
            "Further optimization can be achieved through index partitioning.\n");
        
        // Document 5: Release notes
        createDocument("release_notes.txt",
            "Kronos Release Notes - Version 3.0\n\n"
            "New Features:\n"
            "- Advanced search with query parser\n"
            "- Result caching for improved performance\n"
            "- Metadata API for document management\n\n"
            "Bug Fixes:\n"
            "- Fixed memory leak in cache eviction\n"
            "- Resolved race condition in concurrent searches\n\n"
            "Known Issues:\n"
            "- Large result sets may cause high memory usage\n"
            "- Wildcard queries have limited support\n");
    }
    
    void createDocument(const std::string& filename, const std::string& content) {
        fs::path filepath = testDir_ / filename;
        std::ofstream file(filepath);
        file << content;
        file.close();
        
        // Extract metadata
        DocumentMetadata meta;
        meta.title = filename;
        meta.path = filepath.string();
        meta.fileSize = content.size();
        meta.contentHash = std::to_string(std::hash<std::string>{}(content));
        meta.created = fs::last_write_time(filepath);
        meta.lastModified = meta.created;
        
        // Determine content type
        if (filename.ends_with(".json")) {
            meta.contentType = "application/json";
        } else if (filename.ends_with(".md")) {
            meta.contentType = "text/markdown";
        } else {
            meta.contentType = "text/plain";
        }
        
        // Insert metadata
        repository_->insertDocument(meta);
        
        // Index document
        IndexingConfig config;
        indexer_->indexDocument(filepath.string(), config);
    }
    
    fs::path testDir_;
    fs::path dbPath_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> repository_;
    std::shared_ptr<DocumentIndexer> indexer_;
    std::shared_ptr<QueryParser> parser_;
    std::unique_ptr<SearchExecutor> executor_;
    std::unique_ptr<SearchCache> cache_;
};

// ===== Basic Search Tests =====

TEST_F(SearchIntegrationTest, SimpleKeywordSearch) {
    SearchRequest request;
    request.query = "Kronos";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Should find all 5 documents mentioning "Kronos"
    EXPECT_EQ(response.totalResults, 5);
    EXPECT_EQ(response.results.size(), 5);
}

TEST_F(SearchIntegrationTest, PhraseSearch) {
    SearchRequest request;
    request.query = "\"search query\"";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Should find documents with exact phrase
    EXPECT_GT(response.totalResults, 0);
    
    // Verify phrase appears in results
    for (const auto& item : response.results) {
        EXPECT_TRUE(item.snippet.find("search query") != std::string::npos ||
                   item.title.find("search query") != std::string::npos);
    }
}

TEST_F(SearchIntegrationTest, BooleanSearch) {
    SearchRequest request;
    request.query = "performance AND (cache OR optimization)";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Should find performance report and technical guide
    EXPECT_GE(response.totalResults, 2);
}

TEST_F(SearchIntegrationTest, NegationSearch) {
    SearchRequest request;
    request.query = "Kronos NOT manual";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Should exclude user manual
    for (const auto& item : response.results) {
        EXPECT_FALSE(item.title.find("manual") != std::string::npos);
    }
}

// ===== Filtered Search Tests =====

TEST_F(SearchIntegrationTest, ContentTypeFilter) {
    SearchRequest request;
    request.query = "*"; // Match all
    request.maxResults = 10;
    
    request.filters = std::make_unique<SearchFilters>();
    request.filters->contentTypeFilter = ContentTypeFilter();
    request.filters->contentTypeFilter->types = {"application/json"};
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Should only find JSON documents
    EXPECT_EQ(response.totalResults, 1);
    EXPECT_TRUE(response.results[0].title.find(".json") != std::string::npos);
}

TEST_F(SearchIntegrationTest, SizeFilter) {
    SearchRequest request;
    request.query = "*";
    request.maxResults = 10;
    
    request.filters = std::make_unique<SearchFilters>();
    request.filters->sizeFilter = SizeFilter();
    request.filters->sizeFilter->minSize = 500;
    request.filters->sizeFilter->maxSize = 1000;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Verify all results are within size range
    for (const auto& item : response.results) {
        auto meta = repository_->getDocument(item.documentId);
        ASSERT_TRUE(meta.has_value());
        EXPECT_GE(meta->fileSize, 500);
        EXPECT_LE(meta->fileSize, 1000);
    }
}

// ===== Cache Integration Tests =====

TEST_F(SearchIntegrationTest, CacheHitPerformance) {
    SearchRequest request;
    request.query = "performance optimization";
    request.maxResults = 10;
    
    auto cacheKey = CacheKey::fromQuery(request.query, nullptr, 0, 10);
    
    // First search - cache miss
    auto start1 = std::chrono::high_resolution_clock::now();
    auto result1 = executor_->search(request);
    auto end1 = std::chrono::high_resolution_clock::now();
    auto duration1 = std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1);
    
    ASSERT_TRUE(result1.isSuccess());
    
    // Cache the result
    cache_->put(cacheKey, result1.value());
    
    // Second search - cache hit
    auto start2 = std::chrono::high_resolution_clock::now();
    auto cached = cache_->get(cacheKey);
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2);
    
    ASSERT_TRUE(cached.has_value());
    
    // Cache hit should be significantly faster
    std::cout << "Search time: " << duration1.count() << " us\n";
    std::cout << "Cache hit time: " << duration2.count() << " us\n";
    
    EXPECT_LT(duration2.count(), duration1.count() / 2);
}

TEST_F(SearchIntegrationTest, CacheInvalidation) {
    SearchRequest request;
    request.query = "cache";
    request.maxResults = 10;
    
    auto cacheKey = CacheKey::fromQuery(request.query, nullptr, 0, 10);
    
    // Execute search and cache result
    auto result = executor_->search(request);
    ASSERT_TRUE(result.isSuccess());
    cache_->put(cacheKey, result.value());
    
    // Verify cached
    EXPECT_TRUE(cache_->contains(cacheKey));
    
    // Simulate document update - invalidate cache
    cache_->invalidate(cacheKey);
    
    // Verify invalidated
    EXPECT_FALSE(cache_->contains(cacheKey));
}

// ===== Ranking and Relevance Tests =====

TEST_F(SearchIntegrationTest, RelevanceRanking) {
    SearchRequest request;
    request.query = "search performance cache";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Results should be ranked by relevance
    EXPECT_GT(response.results.size(), 0);
    
    // Verify scores are in descending order
    for (size_t i = 1; i < response.results.size(); ++i) {
        EXPECT_LE(response.results[i].score, response.results[i-1].score);
    }
}

TEST_F(SearchIntegrationTest, Highlighting) {
    SearchRequest request;
    request.query = "Kronos storage";
    request.maxResults = 10;
    request.includeHighlights = true;
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Verify highlights are present
    bool hasHighlights = false;
    for (const auto& item : response.results) {
        if (!item.highlights.empty()) {
            hasHighlights = true;
            
            // Verify highlight matches search terms
            for (const auto& highlight : item.highlights) {
                EXPECT_TRUE(highlight.text == "Kronos" || 
                           highlight.text == "storage");
            }
        }
    }
    
    EXPECT_TRUE(hasHighlights);
}

// ===== Pagination Tests =====

TEST_F(SearchIntegrationTest, Pagination) {
    SearchRequest request;
    request.query = "Kronos";
    request.maxResults = 2;
    request.offset = 0;
    
    // First page
    auto result1 = executor_->search(request);
    ASSERT_TRUE(result1.isSuccess());
    auto page1 = result1.value();
    
    EXPECT_EQ(page1.results.size(), 2);
    EXPECT_EQ(page1.totalResults, 5);
    EXPECT_TRUE(page1.hasMore);
    
    // Second page
    request.offset = 2;
    auto result2 = executor_->search(request);
    ASSERT_TRUE(result2.isSuccess());
    auto page2 = result2.value();
    
    EXPECT_EQ(page2.results.size(), 2);
    
    // Verify no overlap between pages
    for (const auto& item1 : page1.results) {
        for (const auto& item2 : page2.results) {
            EXPECT_NE(item1.documentId, item2.documentId);
        }
    }
}

// ===== Faceted Search Tests =====

TEST_F(SearchIntegrationTest, FacetedSearch) {
    SearchRequest request;
    request.query = "*";
    request.maxResults = 10;
    request.includeFacets = true;
    request.facetFields = {"contentType"};
    
    auto result = executor_->search(request);
    
    ASSERT_TRUE(result.isSuccess());
    auto response = result.value();
    
    // Should have facets for content types
    EXPECT_FALSE(response.facets.empty());
    
    for (const auto& facet : response.facets) {
        if (facet.field == "contentType") {
            // Should have multiple content types
            EXPECT_GT(facet.values.size(), 1);
            
            // Verify counts
            size_t totalCount = 0;
            for (const auto& value : facet.values) {
                totalCount += value.count;
            }
            EXPECT_EQ(totalCount, response.totalResults);
        }
    }
}

// ===== Error Handling Tests =====

TEST_F(SearchIntegrationTest, InvalidQueryHandling) {
    SearchRequest request;
    request.query = "((unbalanced";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    // Should handle gracefully
    if (!result.isSuccess()) {
        EXPECT_FALSE(result.error().message.empty());
    }
}

TEST_F(SearchIntegrationTest, EmptyQueryHandling) {
    SearchRequest request;
    request.query = "";
    request.maxResults = 10;
    
    auto result = executor_->search(request);
    
    // Should either return all results or handle gracefully
    EXPECT_NO_THROW({
        if (result.isSuccess()) {
            auto response = result.value();
            EXPECT_GE(response.totalResults, 0);
        }
    });
}