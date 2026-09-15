// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <yams/core/uuid.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include <chrono>
#include <thread>

using yams::daemon::EntityGraphService;

namespace {

struct EntityGraphFixture {
    EntityGraphFixture() {
        dir = std::filesystem::temp_directory_path() /
              yams::core::generateId("entity-graph-completion-test");
        std::filesystem::create_directories(dir);
        dbPath = (dir / "entity-graph.db").string();

        {
            yams::metadata::Database db;
            REQUIRE(db.open(dbPath, yams::metadata::ConnectionMode::Create));
            yams::metadata::MigrationManager migrations(db);
            REQUIRE(migrations.initialize());
            migrations.registerMigrations(
                yams::metadata::YamsMetadataMigrations::getAllMigrations());
            REQUIRE(migrations.migrate());
            db.close();
        }

        yams::metadata::ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<yams::metadata::ConnectionPool>(dbPath, poolConfig);
        auto kgResult = yams::metadata::makeSqliteKnowledgeGraphStore(*pool);
        REQUIRE(kgResult.has_value());
        kg = std::move(kgResult.value());
        metadata = std::make_shared<yams::metadata::MetadataRepository>(*pool);
    }

    ~EntityGraphFixture() {
        metadata.reset();
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
    std::shared_ptr<yams::metadata::MetadataRepository> metadata;
};

} // namespace

TEST_CASE("EntityGraphService: full channel reports failed admission", "[daemon][kg-intent]") {
    using namespace yams::daemon;
    DaemonConfig config;
    config.dataDir =
        std::filesystem::temp_directory_path() / yams::core::generateId("kg-admission-test");
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    } cleanup{config.dataDir};
    StateComponent state;
    DaemonLifecycleFsm lifecycle;
    ServiceManager services(config, state, lifecycle); // Not initialized; no corpus or daemon.
    EntityGraphService svc(&services, 1);              // Do not start the consumer.
    auto& bus = InternalEventBus::instance();
    auto channel =
        bus.get_or_create_channel<InternalEventBus::EntityGraphJob>("entity_graph_jobs", 4096);
    struct Drain {
        decltype(channel) queue;
        ~Drain() {
            InternalEventBus::EntityGraphJob job;
            while (queue->try_pop(job)) {
            }
        }
    } drain{channel};
    while (channel->try_push(InternalEventBus::EntityGraphJob{})) {
    }
    EntityGraphService::Job job;
    job.documentHash = "intent-hash";
    job.documentDbId = 7;
    job.knowledgeGraphToken = "intent-token";
    job.knowledgeGraphCompletion = std::make_shared<KnowledgeGraphCompletion>(true, true);
    auto rejected = svc.submitExtraction(job);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(svc.getStats().accepted == 0);
    InternalEventBus::EntityGraphJob buffered;
    while (channel->try_pop(buffered)) {
    }
    REQUIRE(svc.submitExtraction(job).has_value());
    REQUIRE(channel->try_pop(buffered));
    CHECK(buffered.documentDbId == 7);
    CHECK(buffered.knowledgeGraphToken == "intent-token");
    CHECK(buffered.knowledgeGraphCompletion == job.knowledgeGraphCompletion);
    CHECK(svc.getStats().accepted == 1);
}

TEST_CASE("EntityGraphService: graph no-op waits for title-NL completion",
          "[unit][daemon][kg-completion]") {
    using namespace yams::daemon;

    EntityGraphFixture fixture;
    DaemonConfig config;
    config.dataDir = fixture.dir / "daemon";
    StateComponent state;
    DaemonLifecycleFsm lifecycle;
    ServiceManager services(config, state, lifecycle);
    services.__test_setMetadataRepo(fixture.metadata);
    REQUIRE(services.getDatabaseManager() != nullptr);
    services.getDatabaseManager()->setKgStore(fixture.kg);

    yams::metadata::DocumentInfo doc;
    doc.filePath = "/kg/noop.txt";
    doc.fileName = "noop.txt";
    doc.sha256Hash = "entity-graph-noop-completion";
    auto id = fixture.metadata->insertDocument(doc);
    REQUIRE(id.has_value());

    yams::metadata::BatchContentEntry content;
    content.documentId = id.value();
    content.contentText = "natural language content";
    content.knowledgeGraphToken = "noop-shared-attempt";
    REQUIRE(fixture.metadata->batchInsertContentAndIndex({content}).has_value());

    auto completion = std::make_shared<KnowledgeGraphCompletion>(true, true);
    EntityGraphService::Job job;
    job.documentHash = doc.sha256Hash;
    job.filePath = doc.filePath;
    job.contentUtf8 = content.contentText;
    job.mimeType = "text/plain";
    job.documentDbId = id.value();
    job.knowledgeGraphToken = content.knowledgeGraphToken;
    job.knowledgeGraphCompletion = completion;

    EntityGraphService service(&services, 1);
    REQUIRE(service.testing_process(job));

    auto marker = fixture.metadata->getMetadata(id.value(), "yams:kg_enrichment");
    REQUIRE(marker.has_value());
    REQUIRE(marker.value().has_value());
    CHECK(marker.value()->value == "pending:" + content.knowledgeGraphToken);
    CHECK(completion->markCommitted(KnowledgeGraphCompletionStage::TitleNl));
}

TEST_CASE("EntityGraphService: queue and process without services", "[daemon]") {
    // Service with nullptr ServiceManager should not crash; processing will be counted as failed
    EntityGraphService svc(nullptr, 1);
    svc.start();

    EntityGraphService::Job j;
    j.documentHash = "deadbeef";
    j.filePath = "/tmp/file.cpp";
    j.contentUtf8 = "int main() { return 0; }";
    j.language = "cpp";

    auto r = svc.submitExtraction(j);
    // submitExtraction may fail immediately when ServiceManager is nullptr on some builds.
    // The key thing is it should not crash. If it succeeds, process will be counted as failed.
    if (r.has_value()) {
        // Give worker a short time slice
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        auto stats = svc.getStats();
        CHECK(stats.accepted >= 1u);
        CHECK(stats.processed >= 1u);
        CHECK(stats.failed >= 1u);
    } else {
        // Submission failed immediately - that's also acceptable behavior
        INFO("submitExtraction returned error: " << r.error().message);
        SUCCEED("submitExtraction rejected job with nullptr ServiceManager (expected behavior)");
    }

    svc.stop();
}
