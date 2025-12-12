// Catch2 tests for Search Executor
// Migrated from GTest: search_executor_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif
#include <yams/metadata/database.h>
#include <yams/search/parallel_post_processor.hpp>
#include <yams/search/search_executor.h>

using namespace yams::search;
using namespace yams::metadata;

namespace {
struct SearchExecutorFixture {
    SearchExecutorFixture() {
        // Create file-backed test database
        auto temp_dir = std::filesystem::temp_directory_path();
        db_path_ =
            (temp_dir / ("yams_search_exec_test_" + std::to_string(::getpid()) + ".db")).string();
        std::error_code removeEc;
        std::filesystem::remove(db_path_, removeEc);

        // Create a temporary database to verify FTS5 support
        auto ftsProbe = std::make_unique<Database>();
        auto openRes = ftsProbe->open(":memory:", ConnectionMode::Create);
        if (!openRes.has_value()) {
            skip_ = true;
            skipReason_ = "Could not open probe database";
            return;
        }
        auto fts5Support = ftsProbe->hasFTS5();
        if (!fts5Support.has_value()) {
            skip_ = true;
            skipReason_ = "FTS5 detection failed: " + fts5Support.error().message;
            return;
        }
        if (!fts5Support.value()) {
            skip_ = true;
            skipReason_ = "SQLite build lacks FTS5 support; skipping SearchExecutor tests.";
            return;
        }
        ftsProbe.reset();

        // Open database connection directly for tests
        database_ = std::make_shared<Database>();
        auto dbOpenRes = database_->open(db_path_, ConnectionMode::Create);
        if (!dbOpenRes.has_value()) {
            skip_ = true;
            skipReason_ = "Could not open test database";
            return;
        }

        // Create search executor using direct database handle (metadata repo optional)
        SearchConfig config;
        config.maxResults = 100;

        executor_ = std::make_unique<SearchExecutor>(database_, nullptr, config);

        // Set up test data
        if (!setupTestData()) {
            skip_ = true;
            skipReason_ = "Failed to set up test data";
        }
    }

    ~SearchExecutorFixture() {
        executor_.reset();
        database_.reset();
        std::filesystem::remove(db_path_);
    }

    bool setupTestData() {
        // Create schema for tests
        auto createDocumentsRes = database_->execute(
            "CREATE TABLE IF NOT EXISTS documents (\n"
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
            ")");
        if (!createDocumentsRes.has_value()) return false;

        auto createFtsRes = database_->execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(\n"
            "  title, content\n"
            ")");
        if (!createFtsRes.has_value()) return false;

        auto createVocabRes = database_->execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts_vocab USING "
            "fts5vocab(documents_fts, 'row')");
        if (!createVocabRes.has_value()) return false;

        // Insert test documents
        auto insertDoc = R"(
            INSERT INTO documents_fts (rowid, title, content)
            VALUES (?, ?, ?)
        )";

        auto stmtRes = database_->prepare(insertDoc);
        if (!stmtRes.has_value()) return false;
        auto stmt = std::move(stmtRes.value());

        // Document 1
        if (!stmt.bind(1, 1).has_value()) return false;
        if (!stmt.bind(2, "Machine Learning Basics").has_value()) return false;
        if (!stmt.bind(3, "Introduction to machine learning algorithms and concepts. This document "
                          "covers supervised learning, unsupervised learning, and reinforcement learning.")
                 .has_value()) return false;
        if (!stmt.step().has_value()) return false;
        if (!stmt.reset().has_value()) return false;

        // Document 2
        if (!stmt.bind(1, 2).has_value()) return false;
        if (!stmt.bind(2, "Deep Learning with Neural Networks").has_value()) return false;
        if (!stmt.bind(3, "Neural networks are the foundation of deep learning. This guide explains "
                          "backpropagation, gradient descent, and common architectures.")
                 .has_value()) return false;
        if (!stmt.step().has_value()) return false;
        if (!stmt.reset().has_value()) return false;

        // Document 3
        if (!stmt.bind(1, 3).has_value()) return false;
        if (!stmt.bind(2, "Data Structures and Algorithms").has_value()) return false;
        if (!stmt.bind(3, "Comprehensive guide to data structures including arrays, lists, trees, and "
                          "graphs. Also covers sorting and searching algorithms.")
                 .has_value()) return false;
        if (!stmt.step().has_value()) return false;
        if (!stmt.reset().has_value()) return false;

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
        if (!metaStmtRes.has_value()) return false;
        auto metaStmt = std::move(metaStmtRes.value());

        // Doc 1 meta
        if (!metaStmt.bind(1, 1).has_value()) return false;
        if (!metaStmt.bind(2, "/docs/ml-basics.txt").has_value()) return false;
        if (!metaStmt.bind(3, "Machine Learning Basics").has_value()) return false;
        if (!metaStmt.bind(4, "txt").has_value()) return false;
        if (!metaStmt.bind(5, 1024).has_value()) return false;
        if (!metaStmt.bind(6, "hash1").has_value()) return false;
        if (!metaStmt.bind(7, "text/plain").has_value()) return false;
        if (!metaStmt.bind(8, nowSeconds).has_value()) return false;
        if (!metaStmt.bind(9, nowSeconds - 3600).has_value()) return false;
        if (!metaStmt.bind(10, nowSeconds - 1800).has_value()) return false;
        if (!metaStmt.bind(11, 1).has_value()) return false;
        if (!metaStmt.bind(12, "indexed").has_value()) return false;
        if (!metaStmt.execute().has_value()) return false;
        if (!metaStmt.reset().has_value()) return false;

        // Doc 2 meta
        if (!metaStmt.bind(1, 2).has_value()) return false;
        if (!metaStmt.bind(2, "/docs/deep-learning.txt").has_value()) return false;
        if (!metaStmt.bind(3, "Deep Learning with Neural Networks").has_value()) return false;
        if (!metaStmt.bind(4, "txt").has_value()) return false;
        if (!metaStmt.bind(5, 2048).has_value()) return false;
        if (!metaStmt.bind(6, "hash2").has_value()) return false;
        if (!metaStmt.bind(7, "text/plain").has_value()) return false;
        if (!metaStmt.bind(8, nowSeconds).has_value()) return false;
        if (!metaStmt.bind(9, nowSeconds - 7200).has_value()) return false;
        if (!metaStmt.bind(10, nowSeconds - 3600).has_value()) return false;
        if (!metaStmt.bind(11, 1).has_value()) return false;
        if (!metaStmt.bind(12, "indexed").has_value()) return false;
        if (!metaStmt.execute().has_value()) return false;
        if (!metaStmt.reset().has_value()) return false;

        // Doc 3 meta
        if (!metaStmt.bind(1, 3).has_value()) return false;
        if (!metaStmt.bind(2, "/docs/data-structures.pdf").has_value()) return false;
        if (!metaStmt.bind(3, "Data Structures and Algorithms").has_value()) return false;
        if (!metaStmt.bind(4, "pdf").has_value()) return false;
        if (!metaStmt.bind(5, 4096).has_value()) return false;
        if (!metaStmt.bind(6, "hash3").has_value()) return false;
        if (!metaStmt.bind(7, "application/pdf").has_value()) return false;
        if (!metaStmt.bind(8, nowSeconds).has_value()) return false;
        if (!metaStmt.bind(9, nowSeconds - 10800).has_value()) return false;
        if (!metaStmt.bind(10, nowSeconds - 5400).has_value()) return false;
        if (!metaStmt.bind(11, 0).has_value()) return false;
        if (!metaStmt.bind(12, "pending").has_value()) return false;
        if (!metaStmt.execute().has_value()) return false;

        return true;
    }

    bool skip_ = false;
    std::string skipReason_;
    std::string db_path_;
    std::shared_ptr<Database> database_;
    std::unique_ptr<SearchExecutor> executor_;
};

struct SearchFiltersFixture {
    SearchFiltersFixture() {
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
} // namespace

// ============================================================================
// SearchExecutor Tests
// ============================================================================

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - BasicSearch", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    auto result = executor_->search("machine learning");
    REQUIRE(result.has_value());

    const auto& results = result.value();
    CHECK(results.size() > 0u);
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchWithResults", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    auto result = executor_->search("learning");
    REQUIRE(result.has_value());

    const auto& results = result.value();
    const auto& items = results.getItems();
    CHECK(items.size() > 0u);

    // Check that results contain expected fields
    for (const auto& item : items) {
        CHECK_FALSE(item.title.empty());
        CHECK_FALSE(item.path.empty());
        CHECK_FALSE(item.contentType.empty());
        CHECK(item.relevanceScore >= 0.0f);
        CHECK(item.relevanceScore <= 1.0f);
    }
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchWithPagination", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    // Search with pagination
    auto result = executor_->search("learning", 0, 1); // Only 1 result
    REQUIRE(result.has_value());

    const auto& results = result.value();
    CHECK(results.size() <= 1u);
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchWithFilters", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    SearchRequest request;
    request.query = "learning";

    // Add content type filter
    ContentTypeFilter typeFilter;
    typeFilter.allowedTypes.insert("text/plain");
    request.filters.addContentTypeFilter(typeFilter);

    auto result = executor_->search(request);
    REQUIRE(result.has_value());

    const auto& items = result.value().getItems();

    // All results should be text/plain
    for (const auto& item : items) {
        CHECK(item.contentType == "text/plain");
    }
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchWithSorting", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    SearchRequest request;
    request.query = "learning";
    request.sortOrder = SearchConfig::SortOrder::TitleAsc;

    auto result = executor_->search(request);
    REQUIRE(result.has_value());

    const auto& items = result.value().getItems();

    if (items.size() > 1) {
        // Check that results are sorted by title
        for (size_t i = 1; i < items.size(); ++i) {
            CHECK(items[i - 1].title <= items[i].title);
        }
    }
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - EmptyQuery", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    auto result = executor_->search("");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - InvalidQuery", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    auto result = executor_->search("(unclosed parenthesis");

    // Should handle gracefully
    if (!result.has_value()) {
        CHECK(result.error().code != yams::ErrorCode::InternalError);
    }
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchSuggestions", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    auto result = executor_->getSuggestions("mach", 5);
    REQUIRE(result.has_value());

    const auto& suggestions = result.value();
    CHECK(suggestions.size() <= 5u);

    // Suggestions should start with the partial query
    for (const auto& suggestion : suggestions) {
        CHECK_FALSE(suggestion.empty());
        CHECK(suggestion.find("mach") != std::string::npos);
    }
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchFacets", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    std::vector<std::string> facetFields = {"contentType"};
    auto result = executor_->getFacets("learning", facetFields);
    REQUIRE(result.has_value());

    const auto& facets = result.value();

    // Should have contentType facet
    bool foundContentTypeFacet = false;
    for (const auto& facet : facets) {
        if (facet.name == "contentType") {
            foundContentTypeFacet = true;
            CHECK(facet.values.size() > 0u);

            // Check facet values
            for (const auto& value : facet.values) {
                CHECK_FALSE(value.value.empty());
                CHECK(value.count > 0u);
            }
        }
    }

    CHECK(foundContentTypeFacet);
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchWithHighlights", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    SearchRequest request;
    request.query = "machine learning";
    request.includeHighlights = true;

    auto result = executor_->search(request);
    REQUIRE(result.has_value());

    const auto& items = result.value().getItems();
    // Highlights may not always be present in this synthetic setup; if present, validate structure
    for (const auto& item : items) {
        for (const auto& highlight : item.highlights) {
            CHECK_FALSE(highlight.field.empty());
            CHECK_FALSE(highlight.snippet.empty());
            CHECK(highlight.startOffset <= highlight.endOffset);
        }
    }
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - SearchStatistics", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    auto result = executor_->search("learning");
    REQUIRE(result.has_value());

    const auto& results = result.value();
    const auto& stats = results.getStatistics();

    CHECK(stats.totalTime.count() >= 0);
    CHECK(stats.searchTime.count() >= 0);
    CHECK(stats.queryTime.count() >= 0);
    CHECK_FALSE(stats.originalQuery.empty());
    CHECK_FALSE(stats.executedQuery.empty());
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - ExecutorStatistics", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    // Perform several searches
    executor_->search("learning");
    executor_->search("neural networks");
    executor_->search("algorithms");

    auto stats = executor_->getStatistics();

    CHECK(stats.totalSearches >= 3u);
    CHECK(stats.avgSearchTime.count() >= 0);
    CHECK(stats.maxSearchTime.count() >= 0);
}

TEST_CASE_METHOD(SearchExecutorFixture, "SearchExecutor - ClearCache", "[search][executor][catch2]") {
    if (skip_) {
        SKIP(skipReason_);
    }

    // Cache has been removed - clearCache() is now a no-op for API compatibility
    // This test verifies the no-op does not crash

    // Perform search
    executor_->search("learning");

    // Clear cache (no-op)
    executor_->clearCache();

    // Verify executor still works after clearCache()
    auto stats = executor_->getStatistics();
    CHECK(stats.totalSearches >= 1u);
}

// ============================================================================
// SearchFilters Tests
// ============================================================================

TEST_CASE_METHOD(SearchFiltersFixture, "SearchFilters - ContentTypeFilter", "[search][filters][catch2]") {
    SearchFilters filters;
    ContentTypeFilter typeFilter;
    typeFilter.allowedTypes.insert("text/plain");
    filters.addContentTypeFilter(typeFilter);

    auto filtered = filters.apply(testItems_);

    CHECK(filtered.size() == 2u); // Only text/plain documents
}
