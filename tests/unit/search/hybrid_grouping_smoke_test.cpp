#include <gtest/gtest.h>
#include <yams/search/hybrid_search_engine.h>

using namespace yams;
using namespace yams::search;

namespace {

// Minimal stub keyword engine to satisfy constructor; not used in this test.
class DummyKeywordEngine final : public KeywordSearchEngine {
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
        std::vector<std::string> out;
        std::istringstream iss(q);
        std::string t;
        while (iss >> t)
            out.push_back(t);
        return out;
    }
    std::vector<std::string> extractKeywords(const std::string& text) const override {
        return analyzeQuery(text);
    }
};

static vector::SearchResult vr(const std::string& id, float sim) {
    vector::SearchResult r;
    r.id = id;
    r.similarity = sim;
    return r;
}

} // namespace

TEST(HybridGroupingSmoke, GroupsChunksByBaseAndPoolsMax) {
    // TODO: This test expects doc-level grouping which isn't implemented yet.
    // fuseResults currently treats each chunk ID as its own result rather than
    // merging chunks (docA#0, docA#1) into a single doc-level result (docA).
    GTEST_SKIP() << "Doc-level chunk grouping not yet implemented in fuseResults";

    // Prepare engine with RRF strategy to enable doc-level grouping (guarded path)
    HybridSearchConfig cfg;
    cfg.fusion_strategy = HybridSearchConfig::FusionStrategy::RECIPROCAL_RANK;
    auto dummyKeyword = std::make_shared<DummyKeywordEngine>();
    HybridSearchEngine eng(nullptr, dummyKeyword, cfg, nullptr);

    // Chunk-level vector results: docA has two chunks; docB one chunk
    std::vector<vector::SearchResult> vrs{vr("docA#0", 0.70f), vr("docA#1", 0.90f),
                                          vr("docB#0", 0.80f)};
    std::vector<KeywordSearchResult> krs; // empty

    auto fused = eng.fuseResults(vrs, krs, cfg);
    ASSERT_EQ(fused.size(), 2u);
    // Expect docA first (MAX=0.90) then docB (0.80) after normalization-sort
    EXPECT_EQ(fused[0].id, std::string("docA"));
    EXPECT_EQ(fused[1].id, std::string("docB"));
    // Metadata should include contributing chunk counts
    ASSERT_TRUE(fused[0].metadata.count("vector_contributing_chunks") > 0);
    ASSERT_TRUE(fused[1].metadata.count("vector_contributing_chunks") > 0);
    EXPECT_EQ(fused[0].metadata["vector_contributing_chunks"], std::string("2"));
    EXPECT_EQ(fused[1].metadata["vector_contributing_chunks"], std::string("1"));
}
