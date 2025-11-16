#include <filesystem>
#include <memory>
#include <string>
#include <gtest/gtest.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/kg_scorer.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

// Forward-declare the simple scorer factory implemented in kg_scorer_simple.cpp
namespace yams::app::services {
class IGraphQueryService;
}

namespace yams::search {
std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store,
                   std::shared_ptr<yams::app::services::IGraphQueryService> graphService = nullptr);
}

namespace {
std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}
} // namespace

TEST(SimpleKGScorerTest, ScoresEntityAndStructuralOverlap) {
    auto dbPath = tempDbPath("kg_scorer_simple_");

    // Create KG store (runs migrations) and a repo on same DB
    KnowledgeGraphStoreConfig cfg{};
    auto sres = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
    ASSERT_TRUE(sres.has_value()) << (sres ? "" : sres.error().message);
    auto store = std::move(sres.value());

    ConnectionPoolConfig pcfg;
    auto pool = std::make_unique<ConnectionPool>(dbPath.string(), pcfg);
    auto init = pool->initialize();
    ASSERT_TRUE(init.has_value());
    auto repo = std::make_unique<MetadataRepository>(*pool);

    // Nodes A and B, alias for A, edge A->B
    auto ids = store->upsertNodes(
        {KGNode{.nodeKey = "ent:a", .label = std::string("A"), .type = std::string("entity")},
         KGNode{.nodeKey = "ent:b", .label = std::string("B"), .type = std::string("entity")}});
    ASSERT_TRUE(ids.has_value()) << ids.error().message;
    ASSERT_EQ(ids.value().size(), 2u);
    KGAlias aa{.nodeId = ids.value()[0],
               .alias = std::string("alpha"),
               .source = std::string("test"),
               .confidence = 1.0f};
    auto aaid = store->addAlias(aa);
    ASSERT_TRUE(aaid.has_value());
    KGEdge e{
        .srcNodeId = ids.value()[0], .dstNodeId = ids.value()[1], .relation = std::string("REL")};
    auto er = store->addEdge(e);
    ASSERT_TRUE(er.has_value());

    // Document 1 has entities A and B; Document 2 is empty/unrelated
    DocumentInfo d1;
    d1.filePath = "/tmp/doc1.txt";
    d1.fileName = "doc1.txt";
    d1.fileExtension = ".txt";
    d1.fileSize = 1;
    d1.sha256Hash = "hash1";
    d1.mimeType = "text/plain";
    d1.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d1.modifiedTime = d1.createdTime;
    d1.indexedTime = d1.createdTime;
    auto did1 = repo->insertDocument(d1);
    ASSERT_TRUE(did1.has_value());
    DocumentInfo d2 = d1;
    d2.fileName = "doc2.txt";
    d2.filePath = "/tmp/doc2.txt";
    d2.sha256Hash = "hash2";
    auto did2 = repo->insertDocument(d2);
    ASSERT_TRUE(did2.has_value());

    DocEntity de1{
        .documentId = did1.value(), .entityText = std::string("A"), .nodeId = ids.value()[0]};
    DocEntity de2{
        .documentId = did1.value(), .entityText = std::string("B"), .nodeId = ids.value()[1]};
    auto ar = store->addDocEntities({de1, de2});
    ASSERT_TRUE(ar.has_value());

    // Build scorer
    auto scorer = makeSimpleKGScorer(std::shared_ptr<KnowledgeGraphStore>(store.release()));
    ASSERT_NE(scorer, nullptr);
    KGScoringConfig scfg;
    scfg.max_neighbors = 16;
    scfg.max_hops = 1;
    scfg.budget = std::chrono::milliseconds(100);
    scorer->setConfig(scfg);

    // Score two candidates
    std::vector<std::string> cands = {std::to_string(did1.value()), std::to_string(did2.value()),
                                      std::string("abc")};
    auto res = scorer->score("Find Alpha please", cands);
    ASSERT_TRUE(res.has_value()) << (res ? "" : res.error().message);

    auto scores = res.value();
    ASSERT_EQ(scores.count(std::to_string(did1.value())), 1u);
    ASSERT_EQ(scores.count(std::to_string(did2.value())), 1u);
    ASSERT_EQ(scores.count("abc"), 1u);

    auto s1 = scores[std::to_string(did1.value())];
    // Expect both components > 0 due to alias match (A) and neighbor overlap (B)
    EXPECT_GT(s1.entity, 0.0f);
    EXPECT_GT(s1.structural, 0.0f);

    auto s2 = scores[std::to_string(did2.value())];
    EXPECT_EQ(s2.entity, 0.0f);
    EXPECT_EQ(s2.structural, 0.0f);

    auto s3 = scores["abc"]; // non-numeric id should produce zero scores, but still present
    EXPECT_EQ(s3.entity, 0.0f);
    EXPECT_EQ(s3.structural, 0.0f);

    // Explanations best-effort: should be available for the hit
    auto expl = scorer->getLastExplanations();
    EXPECT_FALSE(expl.empty());
}
