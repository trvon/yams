#include <gtest/gtest.h>
#include <yams/core/types.h>
#include <yams/search/hybrid_search_engine.h>

#include <algorithm>
#include <cctype>
#include <locale>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace yams;
using namespace yams::search;
using namespace yams::vector;

namespace {

// Minimal mock keyword search engine that satisfies the interface and
// provides deterministic tokenization for expandQuery tests.
class MockKeywordSearchEngine final : public KeywordSearchEngine {
public:
    // Simple whitespace tokenizer with lowercase
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

    // Extract keywords (same as analyzeQuery for this mock)
    std::vector<std::string> extractKeywords(const std::string& text) const override {
        return analyzeQuery(text);
    }

    // The rest of the interface is not exercised by these tests; return trivial values.
    Result<std::vector<KeywordSearchResult>> search(const std::string&, size_t,
                                                    const vector::SearchFilter*) override {
        return Result<std::vector<KeywordSearchResult>>(std::vector<KeywordSearchResult>{});
    }

    Result<std::vector<std::vector<KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>&, size_t, const vector::SearchFilter*) override {
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
    Result<void> saveIndex(const std::string&) override {
        return Result<void>(Error{ErrorCode::InvalidOperation, "Not implemented"});
    }
    Result<void> loadIndex(const std::string&) override {
        return Result<void>(Error{ErrorCode::InvalidOperation, "Not implemented"});
    }

    size_t getDocumentCount() const override { return 0; }
    size_t getTermCount() const override { return 0; }
    size_t getIndexSize() const override { return 0; }
};

} // namespace

namespace {

class BlockingKeywordSearchEngine final : public KeywordSearchEngine {
public:
    explicit BlockingKeywordSearchEngine(std::chrono::milliseconds delay) : delay_(delay) {
        search::KeywordSearchResult r;
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

    Result<std::vector<search::KeywordSearchResult>> search(const std::string&, size_t,
                                                            const vector::SearchFilter*) override {
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

// Test that fuseResults performs linear combination correctly and sorts by hybrid score.
TEST(HybridSearchEngineTest, FuseResultsLinearCombination) {
    // We don't exercise vector search or keyword search here, so vector_index can be null.
    std::shared_ptr<VectorIndexManager> nullVectorIndex;
    auto keywordEngine = std::make_shared<MockKeywordSearchEngine>();

    HybridSearchConfig cfg;
    cfg.fusion_strategy = HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION;
    cfg.vector_weight = 0.7f;
    cfg.keyword_weight = 0.3f;
    cfg.normalizeWeights(); // Ensure weights sum to 1.0

    HybridSearchEngine engine(nullVectorIndex, keywordEngine, cfg);

    // Prepare vector results (already similarity scores in [0,1])
    std::vector<SearchResult> vres;
    vres.emplace_back("A", /*distance*/ 0.0f, /*similarity*/ 0.9f);
    vres.emplace_back("B", /*distance*/ 0.0f, /*similarity*/ 0.4f);

    // Prepare keyword results sorted by descending score (as expected by normalizeKeywordScore)
    std::vector<KeywordSearchResult> kres;
    {
        KeywordSearchResult rA;
        rA.id = "A";
        rA.content = "content A";
        rA.score = 2.0f; // max score
        kres.push_back(rA);
    }
    {
        KeywordSearchResult rB;
        rB.id = "B";
        rB.content = "content B";
        rB.score = 1.0f; // lower score -> normalized to 0.5
        kres.push_back(rB);
    }

    auto fused = engine.fuseResults(vres, kres, cfg);

    // Should have one entry per unique id
    ASSERT_EQ(fused.size(), 2u);

    // Sort order is descending by hybrid_score (operator< implements inverted compare)
    // Min/max normalization maps vector similarities {0.9, 0.4} -> {1.0, 0.0} while keyword
    // scores are normalized by the top keyword score, yielding {1.0, 0.5}.
    ASSERT_EQ(fused[0].id, "A");
    EXPECT_NEAR(fused[0].vector_score, 1.0f, 1e-5f);
    EXPECT_NEAR(fused[0].keyword_score, 1.0f, 1e-5f);
    const float expected_a = fusion::linearCombination(
        fused[0].vector_score, fused[0].keyword_score, cfg.vector_weight, cfg.keyword_weight);
    EXPECT_NEAR(fused[0].hybrid_score, expected_a, 1e-5f);

    ASSERT_EQ(fused[1].id, "B");
    EXPECT_NEAR(fused[1].vector_score, 0.0f, 1e-5f);
    EXPECT_NEAR(fused[1].keyword_score, 0.5f, 1e-5f);
    const float expected_b = fusion::linearCombination(
        fused[1].vector_score, fused[1].keyword_score, cfg.vector_weight, cfg.keyword_weight);
    EXPECT_NEAR(fused[1].hybrid_score, expected_b, 1e-5f);

    // Validate ranks and flags
    EXPECT_TRUE(fused[0].found_by_vector);
    EXPECT_TRUE(fused[0].found_by_keyword);
    EXPECT_EQ(fused[0].vector_rank, 0u);
    EXPECT_EQ(fused[0].keyword_rank, 0u);

    EXPECT_TRUE(fused[1].found_by_vector);
    EXPECT_TRUE(fused[1].found_by_keyword);
    EXPECT_EQ(fused[1].vector_rank, 1u);
    EXPECT_EQ(fused[1].keyword_rank, 1u);
}

// Test that rerankResults boosts exact query matches and keyword density.
TEST(HybridSearchEngineTest, RerankResultsBoostsExactAndKeywords) {
    std::shared_ptr<VectorIndexManager> nullVectorIndex;
    auto keywordEngine = std::make_shared<MockKeywordSearchEngine>();

    HybridSearchConfig cfg;
    cfg.fusion_strategy = HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION;
    cfg.vector_weight = 0.5f;
    cfg.keyword_weight = 0.5f;
    cfg.rerank_top_k = 10; // ensure both results considered

    HybridSearchEngine engine(nullVectorIndex, keywordEngine, cfg);

    std::vector<HybridSearchResult> results;
    HybridSearchResult good;
    good.id = "good";
    good.content = "This document talks about machine learning and more.";
    good.hybrid_score = 1.0f;
    good.matched_keywords = {"machine", "learning"};

    HybridSearchResult neutral;
    neutral.id = "neutral";
    neutral.content = "Irrelevant content.";
    neutral.hybrid_score = 1.0f;

    results.push_back(good);
    results.push_back(neutral);

    const std::string query = "machine learning";
    auto reranked = engine.rerankResults(results, query);

    // The "good" result should be boosted by:
    // exact match boost: *1.2
    // keyword density boost: * (1 + 0.05 * 2) = *1.10
    // total = 1.2 * 1.1 = 1.32
    // With the inverted comparator, the boosted result should be at index 0.
    ASSERT_EQ(reranked.size(), 2u);
    EXPECT_EQ(reranked[0].id, "good");
    EXPECT_NEAR(reranked[0].hybrid_score, 1.32f, 1e-5f);
    EXPECT_EQ(reranked[1].id, "neutral");
    EXPECT_NEAR(reranked[1].hybrid_score, 1.0f, 1e-5f);
}

TEST(HybridSearchEngineTest, KeywordStageTimeoutRaisesMetrics) {
    auto vectorManager = std::make_shared<VectorIndexManager>();
    auto initVec = vectorManager->initialize();
    ASSERT_TRUE(initVec.has_value()) << initVec.error().message;

    auto keywordEngine =
        std::make_shared<BlockingKeywordSearchEngine>(std::chrono::milliseconds(30));

    HybridSearchConfig cfg;
    cfg.vector_weight = 0.0f;
    cfg.keyword_weight = 1.0f;
    cfg.parallel_search = true;
    cfg.final_top_k = 5;
    cfg.keyword_top_k = 5;

    HybridSearchEngine engine(vectorManager, keywordEngine, cfg);
    auto initHybrid = engine.initialize();
    ASSERT_TRUE(initHybrid.has_value()) << initHybrid.error().message;

    SearchStageBudgets budgets;
    bool keywordTimedOut = false;
    bool vectorTimedOut = false;
    budgets.keyword_timeout = std::chrono::milliseconds(5);
    budgets.keyword_timed_out = &keywordTimedOut;
    budgets.vector_timeout = std::chrono::milliseconds(0);
    budgets.vector_timed_out = &vectorTimedOut;

    auto result = engine.search("timeout query", 5, {}, &budgets);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().empty());
    EXPECT_TRUE(keywordTimedOut);
    EXPECT_FALSE(vectorTimedOut);
}

// Test expandQuery pluralization/singularization behavior.
TEST(HybridSearchEngineTest, ExpandQueryPluralization) {
    std::shared_ptr<VectorIndexManager> nullVectorIndex;
    auto keywordEngine = std::make_shared<MockKeywordSearchEngine>();

    HybridSearchConfig cfg;
    cfg.expansion_terms = 10; // allow enough expansion terms

    HybridSearchEngine engine(nullVectorIndex, keywordEngine, cfg);

    const std::string query = "Cats and dog";
    auto expanded = engine.expandQuery(query);

    // For tokens: "cats" -> "cat", "and" -> "ands", "dog" -> "dogs"
    // We check for the more meaningful ones.
    auto contains = [&](const std::string& term) {
        return std::find(expanded.begin(), expanded.end(), term) != expanded.end();
    };

    EXPECT_TRUE(contains("cat"));
    EXPECT_TRUE(contains("dogs"));
    // Optional: ensure limit respected
    EXPECT_LE(expanded.size(), static_cast<size_t>(cfg.expansion_terms));
}
