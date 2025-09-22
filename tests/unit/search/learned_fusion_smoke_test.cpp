#include <gtest/gtest.h>
#include <yams/search/hybrid_search_engine.h>

using namespace yams;
using namespace yams::search;

namespace {
class DummyKeyword final : public KeywordSearchEngine {
public:
    Result<std::vector<KeywordSearchResult>> search(const std::string&, size_t,
                                                    const yams::vector::SearchFilter*) override {
        return std::vector<KeywordSearchResult>{};
    }
    Result<std::vector<std::vector<KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>&, size_t,
                const yams::vector::SearchFilter*) override {
        return std::vector<std::vector<KeywordSearchResult>>{};
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
    size_t getDocumentCount() const override { return 0; }
    size_t getTermCount() const override { return 0; }
    size_t getIndexSize() const override { return 0; }
    std::vector<std::string> analyzeQuery(const std::string& q) const override {
        std::vector<std::string> t;
        std::istringstream ss(q);
        std::string x;
        while (ss >> x)
            t.push_back(x);
        return t;
    }
    std::vector<std::string> extractKeywords(const std::string& t) const override {
        return analyzeQuery(t);
    }
};
} // namespace

static vector::SearchResult vr(const std::string& id, float sim, const char* cc = nullptr) {
    vector::SearchResult r;
    r.id = id;
    r.similarity = sim;
    if (cc)
        r.metadata["vector_contributing_chunks"] = cc;
    return r;
}

TEST(LearnedFusionSmoke, AppliesPenaltyAndProducesFiniteScore) {
    // Setup learned fusion
    HybridSearchConfig cfg;
    cfg.fusion_strategy = HybridSearchConfig::FusionStrategy::LEARNED_FUSION;
    cfg.normalize_scores = true;
    auto eng = std::make_shared<HybridSearchEngine>(nullptr, std::make_shared<DummyKeyword>(), cfg,
                                                    nullptr);

    // Two vector-only candidates; first has single contributing chunk (penalty applies)
    std::vector<vector::SearchResult> vrs{vr("A#0", 0.9f, "1"), vr("B#0", 0.8f, "2")};
    std::vector<KeywordSearchResult> krs;

    // Set strong penalty
    setenv("YAMS_OVERRELIANCE_PENALTY", "0.5", 1);
    auto fused = eng->fuseResults(vrs, krs, cfg);
    ASSERT_EQ(fused.size(), 2u);
    // Scores must be finite and within [0,1]
    for (const auto& r : fused) {
        EXPECT_TRUE(std::isfinite(r.hybrid_score));
        EXPECT_GE(r.hybrid_score, 0.0f);
        EXPECT_LE(r.hybrid_score, 1.0f);
    }
}
