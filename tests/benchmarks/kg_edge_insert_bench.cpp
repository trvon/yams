/// @file kg_edge_insert_bench.cpp
/// @brief KG edge/entity insert microbenchmark for the KG write buffer optimization path.
///
/// Benchmarks KnowledgeGraphStore insert paths that the LSM-inspired KGWriteBuffer (#2)
/// will optimize: addEdgesUnique, addEdges (no dedup), upsertNodes, and WriteBatch
/// throughput with varying batch sizes.
///
/// Registered as a Google Benchmark executable in tests/benchmarks/meson.build.
/// Micro-bench output: local JSON/results; daemon KPI ledger is docs/benchmarks/kpi_progress.md.

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>

using namespace yams::metadata;

namespace {

// ---------------------------------------------------------------------------
// Synthetic data generators
// ---------------------------------------------------------------------------

struct BenchContext {
    std::filesystem::path dbPath;
    std::unique_ptr<KnowledgeGraphStore> kgStore;
    std::vector<std::int64_t> nodeIds;
};

KGNode makeNode(int n) {
    KGNode node;
    node.nodeKey = "fn:bench::func" + std::to_string(n);
    node.label = "func" + std::to_string(n);
    node.type = "function";
    return node;
}

KGEdge makeEdge(std::int64_t src, std::int64_t dst) {
    KGEdge edge;
    edge.srcNodeId = src;
    edge.dstNodeId = dst;
    edge.relation = "calls";
    edge.weight = 1.0f;
    return edge;
}

BenchContext makeContext(int numNodes) {
    BenchContext ctx;
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    ctx.dbPath =
        std::filesystem::temp_directory_path() / ("kg_bench_" + std::to_string(stamp) + ".db");
    std::filesystem::remove(ctx.dbPath);

    KnowledgeGraphStoreConfig kgCfg;
    kgCfg.enable_alias_fts = false;
    kgCfg.enable_wal = true;
    auto kgResult = makeSqliteKnowledgeGraphStore(ctx.dbPath.string(), kgCfg);
    if (!kgResult) {
        throw std::runtime_error("makeSqliteKnowledgeGraphStore: " + kgResult.error().message);
    }
    ctx.kgStore = std::move(kgResult.value());

    std::vector<KGNode> nodes;
    nodes.reserve(static_cast<std::size_t>(numNodes));
    for (int i = 0; i < numNodes; ++i) {
        nodes.push_back(makeNode(i));
    }
    auto upsertResult = ctx.kgStore->upsertNodes(nodes);
    if (!upsertResult) {
        throw std::runtime_error("upsertNodes: " + upsertResult.error().message);
    }
    ctx.nodeIds = upsertResult.value();

    return ctx;
}

void destroyContext(BenchContext& ctx) {
    ctx.kgStore.reset();
    std::error_code ec;
    std::filesystem::remove(ctx.dbPath, ec);
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

class EdgeBulkFixture : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) override {
        const int nnodes = 256;
        totalEdges_ = static_cast<int>(state.range(0));
        ctx_ = makeContext(nnodes);

        // Deterministic pseudo-random edges so runs are comparable.
        edges_.reserve(static_cast<std::size_t>(totalEdges_));
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, nnodes - 1);
        for (int i = 0; i < totalEdges_; ++i) {
            int src = dist(rng);
            int dst = dist(rng);
            if (src != dst) {
                edges_.push_back(makeEdge(ctx_.nodeIds[static_cast<std::size_t>(src)],
                                          ctx_.nodeIds[static_cast<std::size_t>(dst)]));
            }
        }
    }

    void TearDown(const benchmark::State&) override { destroyContext(ctx_); }

protected:
    BenchContext ctx_;
    int totalEdges_ = 0;
    std::vector<KGEdge> edges_;
};

// ---------------------------------------------------------------------------
// Benchmarks: addEdgesUnique (with dedup) vs addEdges (no dedup)
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(EdgeBulkFixture, AddEdgesUnique_SingleTx)(benchmark::State& state) {
    for (auto _ : state) {
        // Benchmark: insert all edges in a single call (current bulk path).
        auto result = ctx_.kgStore->addEdgesUnique(edges_);
        benchmark::DoNotOptimize(result);
        if (!result) {
            state.SkipWithError(result.error().message.c_str());
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * edges_.size());
}

BENCHMARK_DEFINE_F(EdgeBulkFixture, AddEdgesUnique_OneByOne)(benchmark::State& state) {
    for (auto _ : state) {
        // Benchmark: insert edges one at a time (pre-buffer worst case).
        for (const auto& edge : edges_) {
            auto result = ctx_.kgStore->addEdgesUnique({edge});
            benchmark::DoNotOptimize(result);
            if (!result) {
                state.SkipWithError(result.error().message.c_str());
                return;
            }
        }
    }
    state.SetItemsProcessed(state.iterations() * edges_.size());
}

BENCHMARK_DEFINE_F(EdgeBulkFixture, AddEdges_NoDedup_Bulk)(benchmark::State& state) {
    // Generate deduplicated random edges per iteration to avoid the UNIQUE
    // constraint on (src_node_id, dst_node_id, relation). The rng seed
    // advances per iteration so each run measures fresh insert throughput.
    std::mt19937 rng(42);
    const int nnodes = 256;
    std::uniform_int_distribution<int> dist(0, nnodes - 1);
    for (auto _ : state) {
        std::vector<KGEdge> fresh;
        fresh.reserve(static_cast<std::size_t>(totalEdges_));
        // Dedup within batch via (src,dst,relation) key so addEdges() does
        // not hit the UNIQUE constraint on intra-batch duplicates.
        std::unordered_set<std::string> seen;
        seen.reserve(static_cast<std::size_t>(totalEdges_) * 2);
        while (fresh.size() < static_cast<std::size_t>(totalEdges_)) {
            int src = dist(rng);
            int dst = dist(rng);
            if (src == dst)
                continue;
            auto key = std::to_string(src) + ":" + std::to_string(dst) + ":calls";
            if (!seen.insert(key).second)
                continue;
            fresh.push_back(makeEdge(ctx_.nodeIds[static_cast<std::size_t>(src)],
                                     ctx_.nodeIds[static_cast<std::size_t>(dst)]));
        }
        auto result = ctx_.kgStore->addEdges(fresh);
        benchmark::DoNotOptimize(result);
        if (!result) {
            state.SkipWithError(result.error().message.c_str());
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * totalEdges_);
}

BENCHMARK_DEFINE_F(EdgeBulkFixture, WriteBatch_Edges)(benchmark::State& state) {
    for (auto _ : state) {
        // Benchmark: WriteBatch path with explicit begin/commit.
        auto batchResult = ctx_.kgStore->beginWriteBatch();
        if (!batchResult) {
            state.SkipWithError(batchResult.error().message.c_str());
            break;
        }
        auto& wb = *batchResult.value();
        auto er = wb.addEdgesUnique(edges_);
        if (!er) {
            state.SkipWithError(er.error().message.c_str());
            break;
        }
        auto cr = wb.commit();
        if (!cr) {
            state.SkipWithError(cr.error().message.c_str());
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * edges_.size());
}

// ---------------------------------------------------------------------------
// Benchmark: node upsert throughput (nodes/sec baseline)
// ---------------------------------------------------------------------------

class NodeUpsertFixture : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) override {
        totalNodes_ = static_cast<int>(state.range(0));
        ctx_ = makeContext(0); // empty DB

        nodes_.reserve(static_cast<std::size_t>(totalNodes_));
        for (int i = 0; i < totalNodes_; ++i) {
            nodes_.push_back(makeNode(i));
        }
    }

    void TearDown(const benchmark::State&) override { destroyContext(ctx_); }

protected:
    BenchContext ctx_;
    int totalNodes_ = 0;
    std::vector<KGNode> nodes_;
};

BENCHMARK_DEFINE_F(NodeUpsertFixture, UpsertNodes_Bulk)(benchmark::State& state) {
    for (auto _ : state) {
        auto result = ctx_.kgStore->upsertNodes(nodes_);
        benchmark::DoNotOptimize(result);
        if (!result) {
            state.SkipWithError(result.error().message.c_str());
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * nodes_.size());
}

BENCHMARK_DEFINE_F(NodeUpsertFixture, UpsertNodes_OneByOne)(benchmark::State& state) {
    for (auto _ : state) {
        for (const auto& node : nodes_) {
            auto result = ctx_.kgStore->upsertNode(node);
            benchmark::DoNotOptimize(result);
            if (!result) {
                state.SkipWithError(result.error().message.c_str());
                return;
            }
        }
    }
    state.SetItemsProcessed(state.iterations() * nodes_.size());
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

// Edge bulk: total-edges
BENCHMARK_REGISTER_F(EdgeBulkFixture, AddEdgesUnique_SingleTx)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(10);

BENCHMARK_REGISTER_F(EdgeBulkFixture, AddEdgesUnique_OneByOne)
    ->Arg(100)
    ->Arg(500)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(5);

BENCHMARK_REGISTER_F(EdgeBulkFixture, AddEdges_NoDedup_Bulk)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1); // Single-batch upper bound: cross-iteration collisions
                     // are inevitable in a 256² key space — Iterations(1)
                     // measures pure insert speed without dedup overhead.

BENCHMARK_REGISTER_F(EdgeBulkFixture, WriteBatch_Edges)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(10);

// Node upsert: total-nodes
BENCHMARK_REGISTER_F(NodeUpsertFixture, UpsertNodes_Bulk)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(10);

BENCHMARK_REGISTER_F(NodeUpsertFixture, UpsertNodes_OneByOne)
    ->Arg(100)
    ->Arg(500)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(5);

} // namespace

BENCHMARK_MAIN();
