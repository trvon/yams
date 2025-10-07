#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;

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

class KGStoreAliasAndEntitiesTest : public ::testing::Test {
protected:
    void SetUp() override {
        dbPath_ = tempDbPath("kg_store_alias_entities_");

        KnowledgeGraphStoreConfig kgCfg{};
        auto sres = makeSqliteKnowledgeGraphStore(dbPath_.string(), kgCfg);
        ASSERT_TRUE(sres.has_value()) << (sres ? "" : sres.error().message);
        store_ = std::move(sres.value());
        ASSERT_NE(store_, nullptr);

        // Separate pool to exercise repository on same DB file
        ConnectionPoolConfig pcfg;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), pcfg);
        auto init = pool_->initialize();
        ASSERT_TRUE(init.has_value()) << (init ? "" : init.error().message);
        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }

    void TearDown() override {
        repo_.reset();
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        store_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<KnowledgeGraphStore> store_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
};

TEST_F(KGStoreAliasAndEntitiesTest, AliasExactAndFuzzyResolutionAndRemoval) {
    // Create a node and a couple of aliases
    KGNode n;
    n.nodeKey = "ent:alpha";
    n.label = std::string("Alpha");
    n.type = std::string("entity");
    auto nid = store_->upsertNode(n);
    ASSERT_TRUE(nid.has_value()) << nid.error().message;

    KGAlias a1;
    a1.nodeId = nid.value();
    a1.alias = "alpha";
    a1.source = std::string("test");
    a1.confidence = 0.9f;
    auto aid1 = store_->addAlias(a1);
    ASSERT_TRUE(aid1.has_value()) << aid1.error().message;

    // Exact should find it
    auto exact = store_->resolveAliasExact("alpha", 10);
    ASSERT_TRUE(exact.has_value()) << exact.error().message;
    ASSERT_FALSE(exact.value().empty());
    EXPECT_EQ(exact.value().front().nodeId, nid.value());

    // Fuzzy should also return it (either via FTS or fallback to exact)
    auto fuzzy = store_->resolveAliasFuzzy("alpha", 10);
    ASSERT_TRUE(fuzzy.has_value()) << fuzzy.error().message;
    ASSERT_FALSE(fuzzy.value().empty());
    EXPECT_EQ(fuzzy.value().front().nodeId, nid.value());

    // Remove aliases for node, expect exact lookup to be empty
    auto rm = store_->removeAliasesForNode(nid.value());
    ASSERT_TRUE(rm.has_value()) << rm.error().message;
    auto exactAfter = store_->resolveAliasExact("alpha", 10);
    ASSERT_TRUE(exactAfter.has_value()) << exactAfter.error().message;
    EXPECT_TRUE(exactAfter.value().empty());
}

TEST_F(KGStoreAliasAndEntitiesTest, NeighborsAndDocEntitiesRoundTrip) {
    // Two nodes with an edge
    auto ids = store_->upsertNodes(
        {KGNode{.nodeKey = "ent:a", .label = std::string("A"), .type = std::string("entity")},
         KGNode{.nodeKey = "ent:b", .label = std::string("B"), .type = std::string("entity")}});
    ASSERT_TRUE(ids.has_value()) << ids.error().message;
    ASSERT_EQ(ids.value().size(), 2u);
    KGEdge e;
    e.srcNodeId = ids.value()[0];
    e.dstNodeId = ids.value()[1];
    e.relation = "RELATED_TO";
    auto er = store_->addEdge(e);
    ASSERT_TRUE(er.has_value()) << er.error().message;

    auto nb = store_->neighbors(ids.value()[0], 16);
    ASSERT_TRUE(nb.has_value()) << nb.error().message;
    ASSERT_EQ(nb.value().size(), 1u);
    EXPECT_EQ(nb.value().front(), ids.value()[1]);

    // Create a document and attach entities
    DocumentInfo d;
    d.filePath = "/tmp/test-alpha.txt";
    d.fileName = "test-alpha.txt";
    d.fileExtension = ".txt";
    d.fileSize = 123;
    d.sha256Hash = "hash-alpha";
    d.mimeType = "text/plain";
    d.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.modifiedTime = d.createdTime;
    d.indexedTime = d.createdTime;
    auto did = repo_->insertDocument(d);
    ASSERT_TRUE(did.has_value()) << did.error().message;

    // Add two entities for the document
    DocEntity de1{.documentId = did.value(),
                  .entityText = std::string("A"),
                  .nodeId = ids.value()[0],
                  .startOffset = 0,
                  .endOffset = 1,
                  .confidence = 0.95f,
                  .extractor = std::string("test")};
    DocEntity de2{.documentId = did.value(),
                  .entityText = std::string("B"),
                  .nodeId = ids.value()[1],
                  .startOffset = 5,
                  .endOffset = 6,
                  .confidence = 0.85f,
                  .extractor = std::string("test")};
    auto ar = store_->addDocEntities({de1, de2});
    ASSERT_TRUE(ar.has_value()) << ar.error().message;

    // Retrieve
    auto got = store_->getDocEntitiesForDocument(did.value(), 100, 0);
    ASSERT_TRUE(got.has_value()) << got.error().message;
    ASSERT_EQ(got.value().size(), 2u);

    // Document id helpers
    auto byHash = store_->getDocumentIdByHash("hash-alpha");
    ASSERT_TRUE(byHash.has_value());
    ASSERT_TRUE(byHash.value().has_value());
    EXPECT_EQ(byHash.value().value(), did.value());

    auto byName = store_->getDocumentIdByName("test-alpha.txt");
    ASSERT_TRUE(byName.has_value());
    ASSERT_TRUE(byName.value().has_value());
    EXPECT_EQ(byName.value().value(), did.value());

    auto byPath = store_->getDocumentIdByPath("/tmp/test-alpha.txt");
    ASSERT_TRUE(byPath.has_value());
    ASSERT_TRUE(byPath.value().has_value());
    EXPECT_EQ(byPath.value().value(), did.value());

    // Cleanup entities
    auto del = store_->deleteDocEntitiesForDocument(did.value());
    ASSERT_TRUE(del.has_value()) << del.error().message;
    auto after = store_->getDocEntitiesForDocument(did.value(), 10, 0);
    ASSERT_TRUE(after.has_value());
    EXPECT_TRUE(after.value().empty());
}
