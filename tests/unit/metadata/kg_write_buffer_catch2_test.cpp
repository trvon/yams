/// @file kg_write_buffer_catch2_test.cpp
/// @brief Catch2 unit tests for KGWriteBuffer.
///
/// Covers: addEdge coalescing, addDocEntities, flush semantics,
/// auto-flush thresholds, disabled mode pass-through, and edge case merging.

#include <catch2/catch_test_macros.hpp>

#include <yams/core/types.h>
#include <yams/metadata/kg_write_buffer.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace yams::metadata;

namespace {

struct TestHarness {
    std::filesystem::path dbPath;
    std::unique_ptr<KnowledgeGraphStore> kgStore;
    std::unique_ptr<KGWriteBuffer> buffer;

    TestHarness() {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath = std::filesystem::temp_directory_path() /
                 ("kg_buffer_test_" + std::to_string(stamp) + ".db");
        std::filesystem::remove(dbPath);

        KnowledgeGraphStoreConfig cfg;
        cfg.enable_alias_fts = false;
        cfg.enable_wal = true;
        auto result = makeSqliteKnowledgeGraphStore(dbPath.string(), cfg);
        REQUIRE(result);
        kgStore = std::move(result.value());
        REQUIRE(kgStore);

        buffer = std::make_unique<KGWriteBuffer>(*kgStore);
    }

    ~TestHarness() {
        buffer.reset();
        kgStore.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    KGNode makeNode(int n) {
        KGNode node;
        node.nodeKey = "fn:test::func" + std::to_string(n);
        node.label = "func" + std::to_string(n);
        node.type = "function";
        return node;
    }

    KGEdge makeEdge(std::int64_t src, std::int64_t dst, std::string rel = "calls",
                    float weight = 1.0f) {
        KGEdge edge;
        edge.srcNodeId = src;
        edge.dstNodeId = dst;
        edge.relation = std::move(rel);
        edge.weight = weight;
        return edge;
    }

    DocEntity makeEntity(std::int64_t docId, std::int64_t nodeId,
                         const std::string& text = "entity") {
        DocEntity e;
        e.documentId = docId;
        e.entityText = text;
        e.nodeId = nodeId;
        e.confidence = 0.95f;
        return e;
    }

    /// Insert two nodes and return their IDs.
    std::pair<std::int64_t, std::int64_t> insertTwoNodes() {
        std::vector<KGNode> nodes = {makeNode(1), makeNode(2)};
        auto result = kgStore->upsertNodes(nodes);
        REQUIRE(result);
        auto ids = result.value();
        REQUIRE(ids.size() == 2);
        return {ids[0], ids[1]};
    }
};

// ---------------------------------------------------------------------------
// Edge coalescing
// ---------------------------------------------------------------------------

TEST_CASE("KGWriteBuffer coalesces duplicate edges by key", "[kg_write_buffer]") {
    TestHarness h;
    auto [a, b] = h.insertTwoNodes();

    auto e1 = h.makeEdge(a, b, "calls", 0.5f);
    auto e2 = h.makeEdge(a, b, "calls", 1.0f); // same key, higher weight

    REQUIRE(h.buffer->edgeCount() == 0);
    auto r1 = h.buffer->addEdge(e1);
    REQUIRE(r1);
    REQUIRE(h.buffer->edgeCount() == 1);
    REQUIRE(h.buffer->totalEdgesAdded() == 1);

    auto r2 = h.buffer->addEdge(e2);
    REQUIRE(r2);
    REQUIRE(h.buffer->edgeCount() == 1); // still one — coalesced
    REQUIRE(h.buffer->totalEdgesAdded() == 2);

    // Flush and verify only one edge was written with max weight.
    auto flushR = h.buffer->flush();
    REQUIRE(flushR);
    REQUIRE(h.buffer->totalEdgesFlushed() == 1);
    REQUIRE(h.buffer->edgeCount() == 0);

    // Verify the stored edge has weight 1.0 (max of 0.5 and 1.0).
    auto edges = h.kgStore->getEdgesFrom(a);
    REQUIRE(edges);
    REQUIRE(edges.value().size() == 1);
    REQUIRE(edges.value()[0].weight == 1.0f);
}

TEST_CASE("KGWriteBuffer does not coalesce different relations", "[kg_write_buffer]") {
    TestHarness h;
    auto [a, b] = h.insertTwoNodes();

    auto e1 = h.makeEdge(a, b, "calls", 1.0f);
    auto e2 = h.makeEdge(a, b, "inherits", 1.0f);

    REQUIRE(h.buffer->addEdge(e1));
    REQUIRE(h.buffer->addEdge(e2));
    REQUIRE(h.buffer->edgeCount() == 2);

    REQUIRE(h.buffer->flush());
    REQUIRE(h.buffer->totalEdgesFlushed() == 2);

    auto edges = h.kgStore->getEdgesFrom(a);
    REQUIRE(edges);
    REQUIRE(edges.value().size() == 2);
}

TEST_CASE("KGWriteBuffer merges edge properties correctly", "[kg_write_buffer]") {
    TestHarness h;
    auto [a, b] = h.insertTwoNodes();

    auto e1 = h.makeEdge(a, b, "calls", 0.3f);
    e1.properties = R"({"count": 1})";
    auto e2 = h.makeEdge(a, b, "calls", 0.9f);
    e2.properties = R"({"count": 5})"; // higher weight, should win

    REQUIRE(h.buffer->addEdge(e1));
    REQUIRE(h.buffer->addEdge(e2));
    REQUIRE(h.buffer->flush());

    auto edges = h.kgStore->getEdgesFrom(a);
    REQUIRE(edges);
    REQUIRE(edges.value().size() == 1);
    REQUIRE(edges.value()[0].weight == 0.9f);
    REQUIRE(edges.value()[0].properties == R"({"count": 5})");
}

// ---------------------------------------------------------------------------
// addDocEntities
// ---------------------------------------------------------------------------

TEST_CASE("KGWriteBuffer accumulates doc entities in memory", "[kg_write_buffer]") {
    TestHarness h;
    auto [a, b] = h.insertTwoNodes();

    REQUIRE(h.buffer->entityCount() == 0);
    REQUIRE(h.buffer->addDocEntity(h.makeEntity(1, a, "fn1")));
    REQUIRE(h.buffer->addDocEntity(h.makeEntity(2, b, "fn2")));
    REQUIRE(h.buffer->entityCount() == 2);
    // Entity flush requires a documents table (FK constraint).
    // In the isolated test harness, flush() may leave entities buffered
    // if the WriteBatch commit fails. This is expected behavior — callers
    // should handle flush errors and retry.
}

// ---------------------------------------------------------------------------
// Auto-flush thresholds
// ---------------------------------------------------------------------------

TEST_CASE("KGWriteBuffer auto-flushes on maxEdges threshold", "[kg_write_buffer]") {
    TestHarness h;
    KGWriteBufferConfig cfg;
    cfg.maxEdges = 3;
    cfg.autoFlush = true;
    h.buffer = std::make_unique<KGWriteBuffer>(*h.kgStore, cfg);

    auto nodes = h.kgStore->upsertNodes(
        {h.makeNode(1), h.makeNode(2), h.makeNode(3), h.makeNode(4), h.makeNode(5)});
    REQUIRE(nodes);
    auto ids = nodes.value();
    REQUIRE(ids.size() >= 4);

    // Add 3 unique edges — the 3rd should trigger auto-flush.
    REQUIRE(h.buffer->addEdge(h.makeEdge(ids[0], ids[1], "r1")));
    REQUIRE(h.buffer->edgeCount() == 1);
    REQUIRE(h.buffer->addEdge(h.makeEdge(ids[1], ids[2], "r2")));
    REQUIRE(h.buffer->edgeCount() == 2);
    // 3rd edge triggers auto-flush
    REQUIRE(h.buffer->addEdge(h.makeEdge(ids[2], ids[3], "r3")));
    REQUIRE(h.buffer->edgeCount() == 0); // flushed

    REQUIRE(h.buffer->totalEdgesFlushed() == 3);
}

TEST_CASE("KGWriteBuffer auto-flushes on document count threshold", "[kg_write_buffer]") {
    TestHarness h;
    KGWriteBufferConfig cfg;
    cfg.maxDocs = 3;
    cfg.autoFlush = true;
    h.buffer = std::make_unique<KGWriteBuffer>(*h.kgStore, cfg);

    auto [a, b] = h.insertTwoNodes();

    REQUIRE(h.buffer->docCount() == 0);
    REQUIRE(h.buffer->addEdge(h.makeEdge(a, b, "calls")));
    REQUIRE(h.buffer->edgeCount() == 1);

    h.buffer->incrementDocCount();
    REQUIRE(h.buffer->docCount() == 1);
    REQUIRE(h.buffer->edgeCount() == 1); // not flushed yet

    h.buffer->incrementDocCount();
    REQUIRE(h.buffer->docCount() == 2);
    REQUIRE(h.buffer->edgeCount() == 1); // still not flushed

    // 3rd doc triggers auto-flush
    h.buffer->incrementDocCount();
    REQUIRE(h.buffer->docCount() == 0);  // reset after flush
    REQUIRE(h.buffer->edgeCount() == 0); // flushed
    REQUIRE(h.buffer->totalEdgesFlushed() == 1);
}

// ---------------------------------------------------------------------------
// Disabled mode: pass-through
// ---------------------------------------------------------------------------

TEST_CASE("KGWriteBuffer disabled mode passes through directly", "[kg_write_buffer]") {
    TestHarness h;
    KGWriteBufferConfig cfg;
    cfg.enabled = false;
    h.buffer = std::make_unique<KGWriteBuffer>(*h.kgStore, cfg);

    auto [a, b] = h.insertTwoNodes();

    // In disabled mode, addEdge calls addEdgesUnique directly.
    auto r = h.buffer->addEdge(h.makeEdge(a, b, "calls"));
    REQUIRE(r);
    REQUIRE(h.buffer->edgeCount() == 0); // not buffered
    REQUIRE(h.buffer->totalEdgesAdded() == 1);

    // The edge should already be in the store (direct pass-through).
    auto edges = h.kgStore->getEdgesFrom(a);
    REQUIRE(edges);
    REQUIRE(edges.value().size() == 1);
}

// ---------------------------------------------------------------------------
// Edge cases: empty flush, counters
// ---------------------------------------------------------------------------

TEST_CASE("KGWriteBuffer flush with empty buffer is a no-op", "[kg_write_buffer]") {
    TestHarness h;
    REQUIRE(h.buffer->flush());
    REQUIRE(h.buffer->edgeCount() == 0);
    REQUIRE(h.buffer->totalEdgesFlushed() == 0);
}

TEST_CASE("KGWriteBuffer addEdges bulk accepts vector", "[kg_write_buffer]") {
    TestHarness h;
    auto [a, b] = h.insertTwoNodes();

    std::vector<KGEdge> batch = {
        h.makeEdge(a, b, "r1", 1.0f),
        h.makeEdge(a, b, "r2", 1.0f),
    };

    REQUIRE(h.buffer->addEdges(batch));
    REQUIRE(h.buffer->edgeCount() == 2);
    REQUIRE(h.buffer->totalEdgesAdded() == 2);

    REQUIRE(h.buffer->flush());
    REQUIRE(h.buffer->totalEdgesFlushed() == 2);
}

TEST_CASE("KGWriteBuffer resetCounters clears telemetry", "[kg_write_buffer]") {
    TestHarness h;
    auto [a, b] = h.insertTwoNodes();

    REQUIRE(h.buffer->addEdge(h.makeEdge(a, b, "calls")));
    REQUIRE(h.buffer->flush());
    REQUIRE(h.buffer->totalEdgesAdded() == 1);
    REQUIRE(h.buffer->totalEdgesFlushed() == 1);

    h.buffer->resetCounters();
    REQUIRE(h.buffer->totalEdgesAdded() == 0);
    REQUIRE(h.buffer->totalEdgesFlushed() == 0);
}

} // namespace
