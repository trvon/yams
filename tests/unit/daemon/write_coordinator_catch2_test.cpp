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

using yams::daemon::AddSymSpellTermsOp;
using yams::daemon::DeleteOrphanedDocEntitiesOp;
using yams::daemon::DeleteOrphanedEdgesOp;
using yams::daemon::WriteBatch;
using yams::daemon::WriteCoordinator;

namespace {

std::unique_ptr<WriteBatch> makeTermBatch(const std::string& term) {
    auto batch = std::make_unique<WriteBatch>();
    batch->source = "test/write_coordinator_capacity";
    batch->ops.emplace_back(AddSymSpellTermsOp{{term}});
    return batch;
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
