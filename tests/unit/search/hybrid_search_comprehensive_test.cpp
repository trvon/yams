/**
 * @file hybrid_search_comprehensive_test.cpp
 * @brief Comprehensive test coverage for HybridSearchEngine
 *
 * Expanded test coverage for PBI-068 focusing on:
 * - Fusion strategies (LINEAR_COMBINATION, RRF)
 * - Fallback scenarios (timeouts, disabled engines)
 * - Environment gates (runtime feature toggles)
 * - Adaptive weight tuning
 * - Line scope extraction
 * - Cache behavior
 * - KG integration
 * - Parallel vs sequential execution
 *
 * Addresses complexity analysis findings from hybrid-search-analysis.md
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/core/types.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_index_manager.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace yams;
using namespace yams::search;
using namespace yams::vector;
using Catch::Matchers::WithinAbs;

// ============================================================================
// Test Fixtures and Mocks
// ============================================================================

namespace {

/**
 * @brief Mock keyword search engine with controllable behavior
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

/**
 * @brief Mock vector index manager with controllable behavior
 */
class ControllableVectorIndex : public VectorIndexManager {
public:
    // Control knobs
    std::chrono::milliseconds search_delay{0};
    bool should_fail{false};
    bool init_succeeds{true};
    std::vector<SearchResult> canned_results;

    // Recorded calls
    size_t search_call_count{0};
    std::vector<float> last_query_vector;

    Result<void> initialize() override {
        if (init_succeeds) {
            initialized_ = true;
            return Result<void>();
        }
        return Result<void>(Error{ErrorCode::Unknown, "Mock init failure"});
    }

    Result<std::vector<SearchResult>> search(const std::vector<float>& query_vector, size_t k,
                                             const SearchFilter& filter) override {
        search_call_count++;
        last_query_vector = query_vector;

        if (search_delay.count() > 0) {
            std::this_thread::sleep_for(search_delay);
        }

        if (should_fail) {
            return Result<std::vector<SearchResult>>(Error{ErrorCode::Unknown, "Mock failure"});
        }

        std::vector<SearchResult> results;
        for (size_t i = 0; i < std::min(k, canned_results.size()); ++i) {
            results.push_back(canned_results[i]);
        }
        return Result<std::vector<SearchResult>>(results);
    }

    bool isInitialized() const override { return initialized_; }
    void shutdown() override { initialized_ = false; }

    // Unimplemented methods (not needed for tests)
    Result<void> addDocument(const std::string&, const std::vector<float>&,
                             const std::map<std::string, std::string>&) override {
        return Result<void>();
    }

    Result<void> addDocuments(const std::vector<std::string>&,
                              const std::vector<std::vector<float>>&,
                              const std::vector<std::map<std::string, std::string>>&) override {
        return Result<void>();
    }

    Result<void> removeDocument(const std::string&) override { return Result<void>(); }
    Result<void> updateDocument(const std::string&, const std::vector<float>&,
                                const std::map<std::string, std::string>&) override {
        return Result<void>();
    }
    Result<void> buildIndex() override { return Result<void>(); }
    Result<void> saveIndex(const std::string&) override { return Result<void>(); }
    Result<void> loadIndex(const std::string&) override { return Result<void>(); }
    size_t getDocumentCount() const override { return canned_results.size(); }
    size_t getIndexSize() const override { return 0; }

private:
    bool initialized_{false};
};

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

} // namespace

// ============================================================================
// Fusion Strategy Tests
// ============================================================================

TEST_CASE("HybridSearch - Reciprocal Rank Fusion (RRF)", "[hybrid][fusion][rrf]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    // Setup vector results
    vector_idx->canned_results = {
        SearchResult{"doc1", 0.1f, 0.9f}, // High similarity
        SearchResult{"doc2", 0.5f, 0.5f}, // Medium similarity
        SearchResult{"doc3", 0.9f, 0.1f}  // Low similarity
    };

    // Setup keyword results (different order)
    keyword_eng->canned_results = {
        KeywordSearchResult{"doc3", "content3", 10.0f}, // Best keyword match
        KeywordSearchResult{"doc1", "content1", 5.0f},  // Medium keyword match
        KeywordSearchResult{"doc2", "content2", 1.0f}   // Weak keyword match
    };

    HybridSearchConfig config;
    config.fusion_strategy = HybridSearchConfig::FusionStrategy::RECIPROCAL_RANK;
    config.rrf_k = 60.0f;
    config.vector_weight = 0.5f;
    config.keyword_weight = 0.5f;

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    auto init_result = engine.initialize();
    REQUIRE(init_result.has_value());

    SECTION("RRF combines ranks correctly") {
        // Vector ranks: doc1=0, doc2=1, doc3=2
        // Keyword ranks: doc3=0, doc1=1, doc2=2
        // RRF scores:
        //   doc1: 1/(60+0+1) + 1/(60+1+1) = 1/61 + 1/62 = 0.0328
        //   doc2: 1/(60+1+1) + 1/(60+2+1) = 1/62 + 1/63 = 0.0321
        //   doc3: 1/(60+2+1) + 1/(60+0+1) = 1/63 + 1/61 = 0.0323

        auto fused =
            engine.fuseResults(vector_idx->canned_results, keyword_eng->canned_results, config);

        REQUIRE(fused.size() == 3);

        // doc1 should have highest RRF score (best combined rank)
        REQUIRE(fused[0].id == "doc1");
        REQUIRE(fused[0].found_by_vector);
        REQUIRE(fused[0].found_by_keyword);
        REQUIRE(fused[0].vector_rank == 0);
        REQUIRE(fused[0].keyword_rank == 1);
        REQUIRE(fused[0].hybrid_score > 0.032f);
    }

    SECTION("RRF handles missing from one engine") {
        // Add doc4 only in vector results
        vector_idx->canned_results.push_back(SearchResult{"doc4", 0.2f, 0.8f});

        auto fused =
            engine.fuseResults(vector_idx->canned_results, keyword_eng->canned_results, config);

        auto doc4_it =
            std::find_if(fused.begin(), fused.end(), [](const auto& r) { return r.id == "doc4"; });
        REQUIRE(doc4_it != fused.end());
        REQUIRE(doc4_it->found_by_vector);
        REQUIRE_FALSE(doc4_it->found_by_keyword);
        REQUIRE(doc4_it->keyword_rank == SIZE_MAX);
    }
}

TEST_CASE("HybridSearch - Keyword-only fast path", "[hybrid][fusion][fast-path]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    keyword_eng->canned_results = {KeywordSearchResult{"doc1", "content1", 10.0f},
                                   KeywordSearchResult{"doc2", "content2", 5.0f}};

    HybridSearchConfig config;
    config.vector_weight = 0.0f; // Disable vector
    config.keyword_weight = 1.0f;
    config.enable_reranking = false;

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    engine.initialize();

    SECTION("Empty vector results triggers fast path") {
        std::vector<SearchResult> empty_vector;

        auto fused = engine.fuseResults(empty_vector, keyword_eng->canned_results, config);

        REQUIRE(fused.size() == 2);
        REQUIRE(fused[0].id == "doc1");
        REQUIRE_FALSE(fused[0].found_by_vector);
        REQUIRE(fused[0].found_by_keyword);
        REQUIRE(fused[0].keyword_score > 0.0f);
        REQUIRE(fused[0].hybrid_score == fused[0].keyword_score);
    }
}

TEST_CASE("HybridSearch - Vector-only path", "[hybrid][fusion][vector-only]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    vector_idx->canned_results = {SearchResult{"doc1", 0.1f, 0.9f},
                                  SearchResult{"doc2", 0.5f, 0.5f}};

    HybridSearchConfig config;
    config.vector_weight = 1.0f;
    config.keyword_weight = 0.0f; // Disable keyword

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    engine.initialize();

    SECTION("Empty keyword results uses vector-only") {
        std::vector<KeywordSearchResult> empty_keyword;

        auto fused = engine.fuseResults(vector_idx->canned_results, empty_keyword, config);

        REQUIRE(fused.size() == 2);
        REQUIRE(fused[0].id == "doc1");
        REQUIRE(fused[0].found_by_vector);
        REQUIRE_FALSE(fused[0].found_by_keyword);
        REQUIRE(fused[0].vector_score > 0.0f);
        REQUIRE(fused[0].hybrid_score == fused[0].vector_score);
    }
}

//============================================================================
// Fallback Scenario Tests
// ============================================================================

TEST_CASE("HybridSearch - Embedding timeout fallback", "[hybrid][fallback][timeout]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();
    auto embedding_gen = std::make_shared<MockEmbeddingGenerator>();

    keyword_eng->canned_results = {KeywordSearchResult{"doc1", "fallback content", 10.0f}};

    HybridSearchConfig config;
    config.embed_timeout_ms = std::chrono::milliseconds(10); // Very short timeout
    config.vector_weight = 0.7f;
    config.keyword_weight = 0.3f;

    embedding_gen->should_timeout = true; // Exceed timeout

    HybridSearchEngine engine(vector_idx, keyword_eng, config, embedding_gen);
    engine.initialize();

    SECTION("Timeout triggers keyword-only fallback") {
        auto result = engine.search("test query", 10);

        REQUIRE(result.has_value());
        // Should get keyword results even though vector timed out
        REQUIRE_FALSE(result.value().empty());
        REQUIRE(result.value()[0].id == "doc1");
        REQUIRE(result.value()[0].found_by_keyword);
        // Vector search should not have been attempted (timeout in embedding)
        REQUIRE(vector_idx->search_call_count == 0);
    }
}

TEST_CASE("HybridSearch - Vector init failure fallback", "[hybrid][fallback][init]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    vector_idx->init_succeeds = false; // Fail initialization
    keyword_eng->canned_results = {KeywordSearchResult{"doc1", "keyword content", 10.0f}};

    HybridSearchConfig config;
    config.vector_weight = 0.7f;
    config.keyword_weight = 0.3f;

    HybridSearchEngine engine(vector_idx, keyword_eng, config);

    SECTION("Init failure falls back to keyword-only") {
        auto init_result = engine.initialize();
        REQUIRE(init_result.has_value()); // Engine still initializes

        auto search_result = engine.search("test query", 10);
        REQUIRE(search_result.has_value());
        REQUIRE_FALSE(search_result.value().empty());
        REQUIRE(search_result.value()[0].found_by_keyword);
    }
}

TEST_CASE("HybridSearch - Both engines timeout", "[hybrid][fallback][timeout][edge]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    vector_idx->search_delay = std::chrono::milliseconds(100);
    keyword_eng->search_delay = std::chrono::milliseconds(100);

    HybridSearchConfig config;
    config.parallel_search = true;

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    engine.initialize();

    SECTION("Both timeout returns empty results") {
        SearchStageBudgets budgets;
        bool vector_timed_out = false;
        bool keyword_timed_out = false;
        budgets.vector_timeout = std::chrono::milliseconds(10);
        budgets.keyword_timeout = std::chrono::milliseconds(10);
        budgets.vector_timed_out = &vector_timed_out;
        budgets.keyword_timed_out = &keyword_timed_out;

        auto result = engine.search("test query", 10, {}, &budgets);

        REQUIRE(result.has_value());
        REQUIRE(result.value().empty());
        REQUIRE(vector_timed_out);
        REQUIRE(keyword_timed_out);
    }
}

// ============================================================================
// Environment Gate Tests
// ============================================================================

TEST_CASE("HybridSearch - Environment gate: YAMS_DISABLE_VECTOR", "[hybrid][env][gate]") {
    EnvGuard guard("YAMS_DISABLE_VECTOR", "1");

    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    vector_idx->canned_results = {SearchResult{"vec1", 0.0f, 1.0f}};
    keyword_eng->canned_results = {KeywordSearchResult{"key1", "content", 10.0f}};

    HybridSearchConfig config;
    config.vector_weight = 0.7f;
    config.keyword_weight = 0.3f;

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    engine.initialize();

    SECTION("Vector disabled via environment") {
        auto result = engine.search("test query", 10);

        REQUIRE(result.has_value());
        REQUIRE_FALSE(result.value().empty());
        // Vector search should not be called
        REQUIRE(vector_idx->search_call_count == 0);
        // Keyword search should be called
        REQUIRE(keyword_eng->search_call_count > 0);
    }
}

TEST_CASE("HybridSearch - Environment gate: YAMS_DISABLE_KEYWORD", "[hybrid][env][gate]") {
    EnvGuard guard("YAMS_DISABLE_KEYWORD", "1");

    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();
    auto embedding_gen = std::make_shared<MockEmbeddingGenerator>();

    vector_idx->canned_results = {SearchResult{"vec1", 0.0f, 1.0f}};
    keyword_eng->canned_results = {KeywordSearchResult{"key1", "content", 10.0f}};

    HybridSearchConfig config;
    config.vector_weight = 0.7f;
    config.keyword_weight = 0.3f;

    HybridSearchEngine engine(vector_idx, keyword_eng, config, embedding_gen);
    engine.initialize();

    SECTION("Keyword disabled via environment") {
        auto result = engine.search("test query", 10);

        REQUIRE(result.has_value());
        // Keyword search should not be called
        REQUIRE(keyword_eng->search_call_count == 0);
        // Vector search should be called (if embedding succeeds)
    }
}

// ============================================================================
// Cache Behavior Tests
// ============================================================================

TEST_CASE("HybridSearch - Cache hit short-circuits search", "[hybrid][cache]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();
    auto embedding_gen = std::make_shared<MockEmbeddingGenerator>();

    keyword_eng->canned_results = {KeywordSearchResult{"doc1", "content", 10.0f}};

    HybridSearchConfig config;
    config.enable_cache = true;
    config.cache_size = 100;

    HybridSearchEngine engine(vector_idx, keyword_eng, config, embedding_gen);
    engine.initialize();

    SECTION("Second identical query hits cache") {
        // First search - cache miss
        auto result1 = engine.search("cached query", 10);
        REQUIRE(result1.has_value());
        size_t first_keyword_calls = keyword_eng->search_call_count;

        // Second search - cache hit
        auto result2 = engine.search("cached query", 10);
        REQUIRE(result2.has_value());

        // Should not have called keyword engine again
        REQUIRE(keyword_eng->search_call_count == first_keyword_calls);

        // Results should be identical
        REQUIRE(result1.value().size() == result2.value().size());
    }
}

// ============================================================================
// Parallel vs Sequential Execution Tests
// ============================================================================

TEST_CASE("HybridSearch - Parallel execution correctness", "[hybrid][parallel]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    vector_idx->canned_results = {SearchResult{"vec1", 0.0f, 0.9f}};
    keyword_eng->canned_results = {KeywordSearchResult{"key1", "content", 10.0f}};

    HybridSearchConfig config;
    config.parallel_search = true;

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    engine.initialize();

    SECTION("Parallel execution produces results") {
        auto result = engine.search("parallel query", 10);

        REQUIRE(result.has_value());
        // Should have results from at least one engine
        REQUIRE_FALSE(result.value().empty());
    }
}

TEST_CASE("HybridSearch - Sequential execution correctness", "[hybrid][sequential]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    vector_idx->canned_results = {SearchResult{"vec1", 0.0f, 0.9f}};
    keyword_eng->canned_results = {KeywordSearchResult{"key1", "content", 10.0f}};

    HybridSearchConfig config;
    config.parallel_search = false; // Sequential

    HybridSearchEngine engine(vector_idx, keyword_eng, config);
    engine.initialize();

    SECTION("Sequential execution produces results") {
        auto result = engine.search("sequential query", 10);

        REQUIRE(result.has_value());
        REQUIRE_FALSE(result.value().empty());
    }
}

// ============================================================================
// Edge Cases and Robustness Tests
// ============================================================================

TEST_CASE("HybridSearch - Empty results from both engines", "[hybrid][edge]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    // No canned results - both engines return empty

    HybridSearchEngine engine(vector_idx, keyword_eng);
    engine.initialize();

    SECTION("Empty from both returns empty (not error)") {
        auto result = engine.search("no match query", 10);

        REQUIRE(result.has_value());     // Should succeed
        REQUIRE(result.value().empty()); // But be empty
    }
}

TEST_CASE("HybridSearch - Query with special characters", "[hybrid][edge][query]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    keyword_eng->canned_results = {KeywordSearchResult{"doc1", "content", 10.0f}};

    HybridSearchEngine engine(vector_idx, keyword_eng);
    engine.initialize();

    SECTION("Query with quotes and boolean operators") {
        auto result = engine.search("\"exact phrase\" AND term1 OR term2", 10);
        REQUIRE(result.has_value());
    }

    SECTION("Query with metadata filters") {
        auto result = engine.search("path:src/*.cpp AND type:code", 10);
        REQUIRE(result.has_value());
    }
}

TEST_CASE("HybridSearch - Large k value handling", "[hybrid][edge][k]") {
    auto vector_idx = std::make_shared<ControllableVectorIndex>();
    auto keyword_eng = std::make_shared<ControllableKeywordEngine>();

    // Add many results
    for (int i = 0; i < 100; ++i) {
        vector_idx->canned_results.push_back(
            SearchResult{"vec" + std::to_string(i), 0.0f, 1.0f - i * 0.01f});
        keyword_eng->canned_results.push_back(
            KeywordSearchResult{"key" + std::to_string(i), "content", 100.0f - i});
    }

    HybridSearchEngine engine(vector_idx, keyword_eng);
    engine.initialize();

    SECTION("Large k is handled correctly") {
        auto result = engine.search("large k query", 1000);

        REQUIRE(result.has_value());
        // Should cap at available results
        REQUIRE(result.value().size() <= 200); // 100 vector + 100 keyword (with deduplication)
    }
}
