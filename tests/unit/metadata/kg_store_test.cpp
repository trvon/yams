#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/knowledge_graph_store.h>

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

class KGStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        dbPath_ = tempDbPath("kg_store_test_");
        KnowledgeGraphStoreConfig cfg{};
        auto storeRes = makeSqliteKnowledgeGraphStore(dbPath_.string(), cfg);
        ASSERT_TRUE(storeRes.has_value()) << (storeRes ? "" : storeRes.error().message);
        store_ = std::move(storeRes.value());
        ASSERT_NE(store_, nullptr);
    }
    void TearDown() override {
        store_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::unique_ptr<KnowledgeGraphStore> store_;
    std::filesystem::path dbPath_;
};

TEST_F(KGStoreTest, BatchUpsertNodesIsIdempotent) {
    std::vector<KGNode> nodes;
    nodes.push_back(KGNode{.id = 0,
                           .nodeKey = "tag:alpha",
                           .label = std::string("alpha"),
                           .type = std::string("tag")});
    nodes.push_back(KGNode{
        .id = 0, .nodeKey = "tag:beta", .label = std::string("beta"), .type = std::string("tag")});
    auto ids1 = store_->upsertNodes(nodes);
    ASSERT_TRUE(ids1.has_value()) << ids1.error().message;
    ASSERT_EQ(ids1.value().size(), 2u);
    EXPECT_GT(ids1.value()[0], 0);
    EXPECT_GT(ids1.value()[1], 0);

    // Call again; ids should remain stable
    auto ids2 = store_->upsertNodes(nodes);
    ASSERT_TRUE(ids2.has_value()) << ids2.error().message;
    ASSERT_EQ(ids2.value().size(), 2u);
    EXPECT_EQ(ids1.value()[0], ids2.value()[0]);
    EXPECT_EQ(ids1.value()[1], ids2.value()[1]);

    // getNodeByKey path
    auto n1 = store_->getNodeByKey("tag:alpha");
    ASSERT_TRUE(n1.has_value());
    ASSERT_TRUE(n1.value().has_value());
    EXPECT_EQ(n1.value()->id, ids1.value()[0]);
}

TEST_F(KGStoreTest, AddEdgesUniqueDeDupes) {
    // Prepare two nodes
    std::vector<KGNode> nodes;
    nodes.push_back(KGNode{
        .id = 0, .nodeKey = "doc:a", .label = std::string("a"), .type = std::string("document")});
    nodes.push_back(
        KGNode{.id = 0, .nodeKey = "tag:x", .label = std::string("x"), .type = std::string("tag")});
    auto ids = store_->upsertNodes(nodes);
    ASSERT_TRUE(ids.has_value()) << ids.error().message;
    ASSERT_EQ(ids.value().size(), 2u);
    auto src = ids.value()[0];
    auto dst = ids.value()[1];

    // Prepare duplicate edges
    KGEdge e;
    e.srcNodeId = src;
    e.dstNodeId = dst;
    e.relation = "HAS_TAG";
    std::vector<KGEdge> edges{e, e, e};

    // Insert twice via unique batch
    auto r1 = store_->addEdgesUnique(edges);
    ASSERT_TRUE(r1.has_value()) << r1.error().message;
    auto r2 = store_->addEdgesUnique(edges);
    ASSERT_TRUE(r2.has_value()) << r2.error().message;

    // Verify only one edge exists for (src,dst,relation)
    auto out = store_->getEdgesFrom(src, std::string_view("HAS_TAG"), 100, 0);
    ASSERT_TRUE(out.has_value()) << out.error().message;
    size_t count = 0;
    for (const auto& ed : out.value()) {
        if (ed.dstNodeId == dst && ed.relation == "HAS_TAG")
            ++count;
    }
    EXPECT_EQ(count, 1u);
}
