#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/migration.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <random>
#include <thread>

using yams::daemon::AddDeferredEdgesOp;
using yams::daemon::AddSymSpellTermsOp;
using yams::daemon::DeferredEdgeOp;
using yams::daemon::DeleteOrphanedDocEntitiesOp;
using yams::daemon::DeleteOrphanedEdgesOp;
using yams::daemon::UpsertNodesOp;
using yams::daemon::WriteBatch;
using yams::daemon::WriteCoordinator;

namespace {

std::unique_ptr<WriteBatch> makeTermBatch(const std::string& term) {
    auto batch = std::make_unique<WriteBatch>();
    batch->source = "test/write_coordinator_capacity";
    batch->ops.emplace_back(AddSymSpellTermsOp{{term}});
    return batch;
}

struct KgCoordinatorFixture {
    explicit KgCoordinatorFixture(const std::string& prefix) {
        namespace fs = std::filesystem;
        dir = fs::temp_directory_path() / (prefix + std::to_string(std::random_device{}()));
        fs::create_directories(dir);
        dbPath = (dir / "kg.db").string();

        {
            yams::metadata::Database db;
            REQUIRE(db.open(dbPath, yams::metadata::ConnectionMode::Create));
            yams::metadata::MigrationManager mm(db);
            REQUIRE(mm.initialize());
            mm.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
            REQUIRE(mm.migrate());
            db.close();
        }

        yams::metadata::ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<yams::metadata::ConnectionPool>(dbPath, poolConfig);
        auto kgRes = yams::metadata::makeSqliteKnowledgeGraphStore(*pool);
        REQUIRE(kgRes);
        kg = std::move(kgRes.value());
    }

    ~KgCoordinatorFixture() {
        kg.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    std::filesystem::path dir;
    std::string dbPath;
    std::shared_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg;
};

struct EdgeRow {
    int64_t count = 0;
    double weight = 0.0;
    std::string properties;
};

EdgeRow queryEdgesForRelation(const std::string& dbPath, const std::string& relation) {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath, yams::metadata::ConnectionMode::ReadOnly));
    auto stmt = db.prepare("SELECT COUNT(*), COALESCE(MAX(weight), 0), "
                           "COALESCE(MAX(properties), '') FROM kg_edges WHERE relation = ?");
    REQUIRE(stmt);
    REQUIRE(stmt.value().bind(1, relation));
    auto row = stmt.value().step();
    REQUIRE(row);
    REQUIRE(row.value());
    EdgeRow result;
    result.count = stmt.value().getInt64(0);
    result.weight = stmt.value().getDouble(1);
    result.properties = stmt.value().getString(2);
    db.close();
    return result;
}

yams::metadata::KGNode makeNode(const std::string& key) {
    yams::metadata::KGNode node;
    node.nodeKey = key;
    node.type = "entity";
    return node;
}

DeferredEdgeOp makeDeferredEdge(const std::string& src, const std::string& dst,
                                const std::string& relation, float weight,
                                std::optional<std::string> properties = std::nullopt) {
    DeferredEdgeOp edge;
    edge.srcNodeKey = src;
    edge.dstNodeKey = dst;
    edge.relation = relation;
    edge.weight = weight;
    edge.properties = std::move(properties);
    return edge;
}

struct CoordinatorRunner {
    explicit CoordinatorRunner(KgCoordinatorFixture& fix, WriteCoordinator::Config config)
        : coordinator(io, fix.kg, {}, config) {
        coordinator.start();
        runner = std::thread([this] { io.run(); });
    }

    ~CoordinatorRunner() { stop(); }

    void stop() {
        if (!stopped) {
            coordinator.shutdown();
            io.stop();
            if (runner.joinable()) {
                runner.join();
            }
            stopped = true;
        }
    }

    boost::asio::io_context io;
    WriteCoordinator coordinator;
    std::thread runner;
    bool stopped = false;
};

WriteCoordinator::Config kgTestConfig(bool dedup) {
    WriteCoordinator::Config config;
    config.maxBatchSize = 16;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 64;
    config.kgDedupEnabled = dedup;
    return config;
}

void runDuplicateEdgeScenario(KgCoordinatorFixture& fix, bool dedup) {
    CoordinatorRunner run(fix, kgTestConfig(dedup));
    auto& coordinator = run.coordinator;

    auto nodeBatch = std::make_unique<WriteBatch>();
    nodeBatch->source = "test/kg_dedup_nodes";
    nodeBatch->ops.emplace_back(UpsertNodesOp{{makeNode("node:a"), makeNode("node:b")}});
    coordinator.enqueue(std::move(nodeBatch));

    for (int i = 0; i < 5; ++i) {
        auto edgeBatch = std::make_unique<WriteBatch>();
        edgeBatch->source = "test/kg_dedup_edges/" + std::to_string(i);
        edgeBatch->ops.emplace_back(AddDeferredEdgesOp{
            {makeDeferredEdge("node:a", "node:b", "dup_relation", 1.0f + static_cast<float>(i),
                              std::string("props-") + std::to_string(i))}});
        coordinator.enqueue(std::move(edgeBatch));
    }

    REQUIRE(coordinator.flush(std::chrono::seconds{10}).has_value());
    run.stop();
}

} // namespace

TEST_CASE("WriteCoordinator: tryEnqueue rejects batches at channel capacity",
          "[unit][daemon][write-coordinator]") {
    boost::asio::io_context io;
    WriteCoordinator::Config config;
    config.maxBatchSize = 8;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 2;
    WriteCoordinator coordinator(io, {}, {}, config);

    coordinator.enqueue(makeTermBatch("alpha"));
    coordinator.enqueue(makeTermBatch("beta"));
    REQUIRE((coordinator.queuedBatches() == 2));

    auto rejected = makeTermBatch("gamma");
    CHECK_FALSE(coordinator.tryEnqueue(rejected));
    REQUIRE((rejected != nullptr));
    CHECK((coordinator.queuedBatches() == 2));

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    REQUIRE((flushResult.has_value()));

    CHECK(coordinator.tryEnqueue(rejected));
    CHECK((rejected == nullptr));

    auto secondFlush = coordinator.flush(std::chrono::seconds{10});
    REQUIRE((secondFlush.has_value()));

    auto stats = coordinator.getStats();
    CHECK((stats.batchesEnqueued == 3));
    CHECK((stats.batchesCommitted == 3));

    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }
}

TEST_CASE("WriteCoordinator: shutdown drains queued batches and rejects new enqueue",
          "[unit][daemon][write-coordinator]") {
    boost::asio::io_context io;
    WriteCoordinator::Config config;
    config.maxBatchSize = 1;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 8;
    WriteCoordinator coordinator(io, {}, {}, config);
    CHECK_FALSE(coordinator.isShuttingDown());

    coordinator.enqueue(makeTermBatch("alpha"));
    coordinator.enqueue(makeTermBatch("beta"));
    REQUIRE((coordinator.queuedBatches() == 2));

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }

    CHECK((coordinator.queuedBatches() == 0));
    CHECK(coordinator.isShuttingDown());
    auto stats = coordinator.getStats();
    CHECK((stats.batchesEnqueued == 2));
    CHECK((stats.batchesCommitted == 2));

    auto lateBatch = makeTermBatch("gamma");
    CHECK_FALSE(coordinator.tryEnqueue(lateBatch));
    CHECK((lateBatch != nullptr));
    CHECK((coordinator.queuedBatches() == 0));
}

TEST_CASE("WriteCoordinator: orphan edge/doc-entity cleanup populates deletion stats",
          "[unit][daemon][write-coordinator][kg]") {
    // Regression for #7: RepairService::cleanOrphanedKgEntries reported edges=0 /
    // doc_entities=0 because it never read the deletion counts back. The counts are
    // available via WriteCoordinator::getStats() (edgesDeleted/docEntitiesDeleted),
    // which the repair path now snapshots before/after the flush. This proves the
    // stats source is actually populated by the orphan-cleanup ops.
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() /
               ("wc_kg_orphan_" + std::to_string(std::random_device{}()));
    fs::create_directories(dir);
    auto dbPath = (dir / "kg.db").string();

    {
        yams::metadata::Database db;
        auto open = db.open(dbPath, yams::metadata::ConnectionMode::Create);
        REQUIRE(open);
        yams::metadata::MigrationManager mm(db);
        REQUIRE(mm.initialize());
        mm.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
        REQUIRE(mm.migrate());
        db.close();
    }

    yams::metadata::ConnectionPoolConfig poolConfig;
    poolConfig.maxConnections = 2;
    auto pool = std::make_shared<yams::metadata::ConnectionPool>(dbPath, poolConfig);

    // Insert a genuinely orphaned edge directly. kg_edges has ON DELETE CASCADE on
    // both endpoints, so deleting a node also removes its edges — to create the
    // orphan condition the repair path is meant to clean up (an edge whose endpoint
    // node no longer exists), insert the edge with foreign keys disabled, pointing
    // at node ids that do not exist.
    {
        yams::metadata::Database db;
        auto open = db.open(dbPath, yams::metadata::ConnectionMode::ReadWrite);
        REQUIRE(open);
        REQUIRE(db.execute("PRAGMA foreign_keys = OFF"));
        REQUIRE(db.execute("INSERT INTO kg_edges (src_node_id, dst_node_id, relation) "
                           "VALUES (999001, 999002, 'related')"));
        db.close();
    }

    auto kgRes = yams::metadata::makeSqliteKnowledgeGraphStore(*pool);
    REQUIRE(kgRes);
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg = std::move(kgRes.value());

    boost::asio::io_context io;
    WriteCoordinator::Config config;
    config.maxBatchSize = 4;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 8;
    WriteCoordinator coordinator(io, kg, {}, config);

    coordinator.start();
    std::thread writerLoop([&io] { io.run(); });

    const auto before = coordinator.getStats();

    auto batch = std::make_unique<WriteBatch>();
    batch->source = "test/orphan_cleanup";
    batch->ops.emplace_back(DeleteOrphanedEdgesOp{});
    batch->ops.emplace_back(DeleteOrphanedDocEntitiesOp{});
    coordinator.enqueue(std::move(batch));

    auto flushResult = coordinator.flush(std::chrono::seconds{10});
    REQUIRE(flushResult.has_value());

    const auto after = coordinator.getStats();
    CHECK((after.edgesDeleted - before.edgesDeleted) == 1);

    coordinator.shutdown();
    io.stop();
    if (writerLoop.joinable()) {
        writerLoop.join();
    }

    kg.reset();
    pool->shutdown();
    pool.reset();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("WriteCoordinator: duplicate deferred edges coalesce to one row with merged weight",
          "[unit][daemon][write-coordinator][kg][dedup]") {
    KgCoordinatorFixture fix("wc_kg_dedup_");
    runDuplicateEdgeScenario(fix, true);

    auto row = queryEdgesForRelation(fix.dbPath, "dup_relation");
    CHECK((row.count == 1));
    CHECK((row.weight == 5.0));
    CHECK((row.properties == "props-4"));
}

TEST_CASE("WriteCoordinator: dedup disabled produces identical final edge state",
          "[unit][daemon][write-coordinator][kg][dedup]") {
    KgCoordinatorFixture withDedup("wc_kg_parity_on_");
    runDuplicateEdgeScenario(withDedup, true);
    auto dedupRow = queryEdgesForRelation(withDedup.dbPath, "dup_relation");

    KgCoordinatorFixture withoutDedup("wc_kg_parity_off_");
    runDuplicateEdgeScenario(withoutDedup, false);
    auto directRow = queryEdgesForRelation(withoutDedup.dbPath, "dup_relation");

    CHECK((dedupRow.count == directRow.count));
    CHECK((dedupRow.weight == directRow.weight));
    CHECK((dedupRow.properties == directRow.properties));
}

TEST_CASE("WriteCoordinator: coalescing stats populate",
          "[unit][daemon][write-coordinator][kg][dedup]") {
    KgCoordinatorFixture fix("wc_kg_dedup_stats_");
    CoordinatorRunner run(fix, kgTestConfig(true));
    auto& coordinator = run.coordinator;

    auto nodeBatch = std::make_unique<WriteBatch>();
    nodeBatch->source = "test/kg_stats_nodes";
    nodeBatch->ops.emplace_back(UpsertNodesOp{{makeNode("stat:a"), makeNode("stat:b")}});
    coordinator.enqueue(std::move(nodeBatch));
    REQUIRE(coordinator.flush(std::chrono::seconds{10}).has_value());

    auto edgeBatch = std::make_unique<WriteBatch>();
    edgeBatch->source = "test/kg_stats_edges";
    edgeBatch->ops.emplace_back(
        AddDeferredEdgesOp{{makeDeferredEdge("stat:a", "stat:b", "stat_rel", 1.0f),
                            makeDeferredEdge("stat:a", "stat:b", "stat_rel", 2.0f),
                            makeDeferredEdge("stat:a", "stat:b", "stat_rel", 3.0f)}});
    coordinator.enqueue(std::move(edgeBatch));
    REQUIRE(coordinator.flush(std::chrono::seconds{10}).has_value());

    auto stats = coordinator.getStats();
    CHECK((stats.edgesCoalesced == 2));
    CHECK((stats.edgesAdded == 1));
    CHECK((stats.nodeKeyLookupsBatched >= 2));
    run.stop();

    auto row = queryEdgesForRelation(fix.dbPath, "stat_rel");
    CHECK((row.count == 1));
    CHECK((row.weight == 3.0));
}

TEST_CASE("WriteCoordinator: deferred edges resolve prior-flush nodes via bulk prefetch",
          "[unit][daemon][write-coordinator][kg][dedup]") {
    constexpr int kNodeCount = 1000;
    KgCoordinatorFixture fix("wc_kg_prefetch_");
    CoordinatorRunner run(fix, kgTestConfig(true));
    auto& coordinator = run.coordinator;

    std::vector<yams::metadata::KGNode> nodes;
    nodes.reserve(kNodeCount);
    for (int i = 0; i < kNodeCount; ++i) {
        nodes.push_back(makeNode("bulk:" + std::to_string(i)));
    }
    auto nodeBatch = std::make_unique<WriteBatch>();
    nodeBatch->source = "test/kg_prefetch_nodes";
    nodeBatch->ops.emplace_back(UpsertNodesOp{std::move(nodes)});
    coordinator.enqueue(std::move(nodeBatch));
    REQUIRE(coordinator.flush(std::chrono::seconds{30}).has_value());

    std::vector<DeferredEdgeOp> edges;
    edges.reserve(kNodeCount / 2);
    for (int i = 0; i + 1 < kNodeCount; i += 2) {
        edges.push_back(makeDeferredEdge("bulk:" + std::to_string(i),
                                         "bulk:" + std::to_string(i + 1), "bulk_rel", 1.0f));
    }
    auto edgeBatch = std::make_unique<WriteBatch>();
    edgeBatch->source = "test/kg_prefetch_edges";
    edgeBatch->ops.emplace_back(AddDeferredEdgesOp{std::move(edges)});
    coordinator.enqueue(std::move(edgeBatch));
    REQUIRE(coordinator.flush(std::chrono::seconds{30}).has_value());

    auto stats = coordinator.getStats();
    CHECK((stats.nodeKeyLookupsBatched >= kNodeCount));
    run.stop();

    auto row = queryEdgesForRelation(fix.dbPath, "bulk_rel");
    CHECK((row.count == kNodeCount / 2));
}

TEST_CASE("WriteCoordinator: delete op mid-stream does not lose buffered edges",
          "[unit][daemon][write-coordinator][kg][dedup]") {
    KgCoordinatorFixture fix("wc_kg_delete_order_");
    CoordinatorRunner run(fix, kgTestConfig(true));
    auto& coordinator = run.coordinator;

    auto batch = std::make_unique<WriteBatch>();
    batch->source = "test/kg_delete_order";
    batch->ops.emplace_back(UpsertNodesOp{{makeNode("del:a"), makeNode("del:b")}});
    batch->ops.emplace_back(
        AddDeferredEdgesOp{{makeDeferredEdge("del:a", "del:b", "del_rel", 1.0f)}});
    batch->ops.emplace_back(DeleteOrphanedDocEntitiesOp{});
    batch->ops.emplace_back(
        AddDeferredEdgesOp{{makeDeferredEdge("del:b", "del:a", "del_rel", 2.0f)}});
    coordinator.enqueue(std::move(batch));
    REQUIRE(coordinator.flush(std::chrono::seconds{10}).has_value());
    run.stop();

    auto row = queryEdgesForRelation(fix.dbPath, "del_rel");
    CHECK((row.count == 2));
}
