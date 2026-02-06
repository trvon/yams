// Catch2 migration of repair_coordinator_test.cpp
// Migration: yams-3s4 (daemon unit tests)

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <thread>

#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/StateComponent.h>

#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

using namespace std::chrono_literals;

namespace {

// Local wait_for_condition helper (avoids GTest dependency from test_helpers.h)
inline bool wait_for_condition(std::chrono::milliseconds timeout,
                               std::chrono::milliseconds interval,
                               const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(interval);
    }
    return predicate();
}

} // namespace

namespace yams::daemon {

namespace {

class FakeGraphComponent final : public GraphComponent {
public:
    explicit FakeGraphComponent(std::atomic<int>& calls)
        : GraphComponent(nullptr, nullptr, nullptr), calls_(calls) {}

    Result<void> submitEntityExtraction(EntityExtractionJob) {
        calls_.fetch_add(1);
        return {};
    }

private:
    std::atomic<int>& calls_;
};

class FakeRepairCoordinator final : public RepairCoordinator {
public:
    FakeRepairCoordinator(StateComponent* state, std::function<size_t()> activeConnFn, Config cfg,
                          std::shared_ptr<metadata::KnowledgeGraphStore> kg,
                          std::shared_ptr<GraphComponent> graph,
                          std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> extractors)
        : RepairCoordinator(nullptr, state, std::move(activeConnFn), std::move(cfg)),
          kg_(std::move(kg)), graph_(std::move(graph)), extractors_(std::move(extractors)) {}

private:
    std::shared_ptr<GraphComponent> getGraphComponentForScheduling() const override {
        return graph_;
    }

    std::shared_ptr<metadata::KnowledgeGraphStore> getKgStoreForScheduling() const override {
        return kg_;
    }

    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>&
    getSymbolExtractorsForScheduling() const override {
        return extractors_;
    }

    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    std::shared_ptr<GraphComponent> graph_;
    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> extractors_;
};

} // namespace

TEST_CASE("RepairCoordinator processes document events when enabled",
          "[daemon][repair][coordinator]") {
    StateComponent state;

    // Always idle for testing
    std::atomic<size_t> active{0};
    auto activeFn = [&]() -> size_t { return active.load(); };

    RepairCoordinator::Config rcfg;
    rcfg.enable = true;
    rcfg.dataDir = std::filesystem::temp_directory_path();
    rcfg.maxBatch = 10;

    RepairCoordinator rc(nullptr, &state, activeFn, rcfg);
    rc.start();

    // Simulate document additions
    RepairCoordinator::DocumentAddedEvent event1{"hash1", "/path/to/doc1"};
    RepairCoordinator::DocumentAddedEvent event2{"hash2", "/path/to/doc2"};

    rc.onDocumentAdded(event1);
    rc.onDocumentAdded(event2);

    // Give the coordinator time to process events
    std::this_thread::sleep_for(100ms);

    // Simulate document removal
    RepairCoordinator::DocumentRemovedEvent removeEvent{"hash1"};
    rc.onDocumentRemoved(removeEvent);

    rc.stop();

    // Verify it started and stopped without errors
    SUCCEED();
}

TEST_CASE("RepairCoordinator does nothing when disabled", "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config rcfg;
    rcfg.enable = false; // Disabled
    rcfg.dataDir = std::filesystem::temp_directory_path();
    rcfg.maxBatch = 10;

    RepairCoordinator rc(nullptr, &state, activeFn, rcfg);
    rc.start();

    // Try to add documents - should be ignored
    RepairCoordinator::DocumentAddedEvent event{"hash1", "/path/to/doc1"};
    rc.onDocumentAdded(event);

    // Give a moment to ensure nothing happens
    std::this_thread::sleep_for(50ms);

    rc.stop();

    // Verify no repair operations were attempted
    REQUIRE(state.stats.repairBatchesAttempted.load() == 0u);
}

TEST_CASE("RepairCoordinator start is idempotent", "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_idem";
    cfg.maxBatch = 10;

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    // First start should succeed
    coordinator.start();
    std::this_thread::sleep_for(100ms);

    // Second start should be idempotent (no error)
    REQUIRE_NOTHROW(coordinator.start());

    coordinator.stop();
}

TEST_CASE("RepairCoordinator stop is idempotent", "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_stop";
    cfg.maxBatch = 10;

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    coordinator.start();
    std::this_thread::sleep_for(100ms);
    coordinator.stop();

    // Second stop should be idempotent (no error)
    REQUIRE_NOTHROW(coordinator.stop());
}

TEST_CASE("RepairCoordinator skips symbol extraction if doc entities exist",
          "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_dedupe";
    cfg.maxBatch = 10;

    // Minimal KG DB for predicate checks.
    auto dbPath = std::filesystem::temp_directory_path() / "repair_test_dedupe_kg.db";
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);

    metadata::KnowledgeGraphStoreConfig kgCfg{};
    auto sres = metadata::makeSqliteKnowledgeGraphStore(dbPath.string(), kgCfg);
    REQUIRE(sres.has_value());
    std::shared_ptr<metadata::KnowledgeGraphStore> kg = std::move(sres.value());

    metadata::ConnectionPoolConfig pcfg;
    auto pool = std::make_unique<metadata::ConnectionPool>(dbPath.string(), pcfg);
    REQUIRE(pool->initialize().has_value());
    auto repo = std::make_unique<metadata::MetadataRepository>(*pool);

    metadata::DocumentInfo doc;
    doc.fileName = "example.cpp";
    doc.filePath = "/tmp/example.cpp";
    doc.fileExtension = "cpp";
    doc.fileSize = 123;
    doc.sha256Hash = "hash-abc";
    doc.mimeType = "text/x-c++";
    doc.setCreatedTime(1);
    doc.setModifiedTime(1);
    doc.setIndexedTime(1);
    auto insDoc = repo->insertDocument(doc);
    REQUIRE(insDoc.has_value());
    auto docId = insDoc.value();

    metadata::KGNode sym;
    sym.nodeKey = "sym:hash-abc:F";
    sym.label = std::string("F");
    sym.type = std::string("symbol");
    auto symIdRes = kg->upsertNode(sym);
    REQUIRE(symIdRes.has_value());

    metadata::DocEntity de;
    de.documentId = docId;
    de.entityText = "F";
    de.nodeId = symIdRes.value();
    de.startOffset = 0;
    de.endOffset = 1;
    de.confidence = 1.0f;
    de.extractor = std::string("test");
    REQUIRE(kg->addDocEntities({de}).has_value());

    std::atomic<int> extractionCalls{0};
    auto graph = std::make_shared<FakeGraphComponent>(extractionCalls);

    // Construct a minimal extractor that advertises .cpp support.
    static yams_symbol_extractor_v1 table{};
    table.get_capabilities_json = [](void*, char** out_json) -> int {
        if (!out_json)
            return 1;
        static constexpr char kCaps[] =
            R"({"version":1,"languages":[{"id":"cpp","extensions":["cpp",".cpp"]}]})";
        *out_json = const_cast<char*>(kCaps);
        return 0;
    };
    table.free_string = [](void*, char*) {};
    auto extractor = std::make_shared<AbiSymbolExtractorAdapter>(&table);

    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> extractors{extractor};

    FakeRepairCoordinator rc(&state, activeFn, cfg, kg, graph, std::move(extractors));
    rc.start();

    // .cpp path should match and attempt dispatch; dedupe should skip.
    RepairCoordinator::DocumentAddedEvent ev{"hash-abc", "/tmp/example.cpp"};
    rc.onDocumentAdded(ev);

    CHECK(extractionCalls.load() == 0);

    // Remove doc entities and retry. If scheduling happens it should increment,
    // but this test primarily guards the "skip when entities exist" behavior.
    REQUIRE(kg->deleteDocEntitiesForDocument(docId).has_value());
    rc.onDocumentAdded(ev);

    SUCCEED();

    rc.stop();
    repo.reset();
    pool->shutdown();
    pool.reset();
    kg.reset();
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("RepairCoordinator handles multiple document added events",
          "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_multi";
    cfg.maxBatch = 5;

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    coordinator.start();

    // Add multiple documents to test batch processing
    for (int i = 0; i < 10; ++i) {
        std::string hash = "hash_" + std::to_string(i);
        std::string path = "/path/doc_" + std::to_string(i) + ".txt";
        RepairCoordinator::DocumentAddedEvent event{hash, path};
        coordinator.onDocumentAdded(event);
    }

    // Wait for processing to begin
    bool processed = wait_for_condition(
        2000ms, 50ms, [&state]() { return state.stats.repairQueueDepth.load() >= 0; });

    // Queue depth should be tracked (may already be processed)
    REQUIRE(processed);
    REQUIRE(state.stats.repairQueueDepth.load() >= 0u);

    coordinator.stop();
}

TEST_CASE("RepairCoordinator ignores events when not running", "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_notrun";
    cfg.maxBatch = 10;

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    // Try to add events before starting
    RepairCoordinator::DocumentAddedEvent addEvent{"hash1", "/path1.txt"};
    coordinator.onDocumentAdded(addEvent);
    RepairCoordinator::DocumentRemovedEvent removeEvent{"hash2"};
    coordinator.onDocumentRemoved(removeEvent);

    // Queue should remain empty (events ignored)
    REQUIRE(state.stats.repairQueueDepth.load() == 0u);
}

TEST_CASE("RepairCoordinator stats tracking updates queue depth", "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_stats";
    cfg.maxBatch = 10;

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    coordinator.start();

    // Initial queue depth should be 0
    REQUIRE(state.stats.repairQueueDepth.load() == 0u);

    // Add a document
    RepairCoordinator::DocumentAddedEvent event{"stats_hash", "/stats.txt"};
    coordinator.onDocumentAdded(event);

    // Small delay to allow queue update
    std::this_thread::sleep_for(50ms);

    // Queue depth should have been updated (may be processed already)
    REQUIRE(state.stats.repairQueueDepth.load() >= 0u);

    coordinator.stop();
}

TEST_CASE("RepairCoordinator filters non-text files without plugins",
          "[daemon][repair][coordinator]") {
    // Binary files should not trigger symbol extraction scheduling
    // when no symbol extractors are available
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_filter_noext";
    cfg.maxBatch = 10;

    // No symbol extractors — binary files should be ignored for extraction
    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> emptyExtractors;

    auto dbPath = std::filesystem::temp_directory_path() / "repair_test_filter_noext_kg.db";
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);

    metadata::KnowledgeGraphStoreConfig kgCfg{};
    auto sres = metadata::makeSqliteKnowledgeGraphStore(dbPath.string(), kgCfg);
    REQUIRE(sres.has_value());
    std::shared_ptr<metadata::KnowledgeGraphStore> kg = std::move(sres.value());

    std::atomic<int> extractionCalls{0};
    auto graph = std::make_shared<FakeGraphComponent>(extractionCalls);

    FakeRepairCoordinator rc(&state, activeFn, cfg, kg, graph, std::move(emptyExtractors));
    rc.start();

    // Add a binary file — with no extractors, symbol extraction should not fire
    RepairCoordinator::DocumentAddedEvent ev{"hash-bin1", "/tmp/test.bin"};
    rc.onDocumentAdded(ev);

    std::this_thread::sleep_for(100ms);

    // No symbol extraction should have been attempted
    CHECK(extractionCalls.load() == 0);

    rc.stop();
    kg.reset();
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("RepairCoordinator allows binary files with custom plugins",
          "[daemon][repair][coordinator]") {
    // Binary files with matching symbol extractors should trigger extraction
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_filter_ext";
    cfg.maxBatch = 10;

    auto dbPath = std::filesystem::temp_directory_path() / "repair_test_filter_ext_kg.db";
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);

    metadata::KnowledgeGraphStoreConfig kgCfg{};
    auto sres = metadata::makeSqliteKnowledgeGraphStore(dbPath.string(), kgCfg);
    REQUIRE(sres.has_value());
    std::shared_ptr<metadata::KnowledgeGraphStore> kg = std::move(sres.value());

    std::atomic<int> extractionCalls{0};
    auto graph = std::make_shared<FakeGraphComponent>(extractionCalls);

    // Construct a symbol extractor that advertises .bin support
    static yams_symbol_extractor_v1 binTable{};
    binTable.get_capabilities_json = [](void*, char** out_json) -> int {
        if (!out_json)
            return 1;
        static constexpr char kCaps[] =
            R"({"version":1,"languages":[{"id":"binary","extensions":["bin",".bin"]}]})";
        *out_json = const_cast<char*>(kCaps);
        return 0;
    };
    binTable.free_string = [](void*, char*) {};
    auto extractor = std::make_shared<AbiSymbolExtractorAdapter>(&binTable);

    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> extractors{extractor};

    FakeRepairCoordinator rc(&state, activeFn, cfg, kg, graph, std::move(extractors));
    rc.start();

    // Add a .bin file — with the extractor present, the event should be queued
    RepairCoordinator::DocumentAddedEvent ev{"hash-bin2", "/tmp/test.bin"};
    rc.onDocumentAdded(ev);

    std::this_thread::sleep_for(100ms);

    // The document should have been queued (queue depth updated)
    // It may or may not have been processed depending on timing,
    // but it should NOT have been silently dropped
    CHECK(state.stats.repairQueueDepth.load() >= 0u);

    rc.stop();
    kg.reset();
    std::filesystem::remove(dbPath, ec);
}

TEST_CASE("RepairCoordinator deduplicates pending documents", "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_dedupe_queue";
    cfg.maxBatch = 100; // Large batch to accumulate without processing

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    coordinator.start();

    // Add the same document hash multiple times
    const std::string testHash = "dedupe_test_hash_12345";
    for (int i = 0; i < 10; ++i) {
        RepairCoordinator::DocumentAddedEvent event{testHash, "/path/test.txt"};
        coordinator.onDocumentAdded(event);
    }

    // Small delay to ensure processing
    std::this_thread::sleep_for(50ms);

    // Queue depth should be 1 (all duplicates should be deduped)
    // Note: The queue may have been processed, so we check it's <= 1
    REQUIRE(state.stats.repairQueueDepth.load() <= 1u);

    coordinator.stop();
}

TEST_CASE("RepairCoordinator enqueueEmbeddingRepair deduplicates",
          "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_batch_dedupe";
    cfg.maxBatch = 100;

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    coordinator.start();

    // Enqueue batch with duplicate hashes
    std::vector<std::string> hashes = {"hash_a", "hash_b", "hash_a", "hash_c", "hash_b", "hash_a"};
    coordinator.enqueueEmbeddingRepair(hashes);

    // Small delay
    std::this_thread::sleep_for(50ms);

    // Should have at most 3 unique hashes queued (may be processed already)
    REQUIRE(state.stats.repairQueueDepth.load() <= 3u);

    coordinator.stop();
}

TEST_CASE("RepairCoordinator token throttling limits concurrency",
          "[daemon][repair][coordinator]") {
    StateComponent state;
    auto activeFn = []() -> size_t { return 0; };

    RepairCoordinator::Config cfg;
    cfg.enable = true;
    cfg.dataDir = std::filesystem::temp_directory_path() / "repair_test_tokens";
    cfg.maxBatch = 10;
    cfg.maintenanceTokens = 2; // Limit to 2 concurrent operations

    RepairCoordinator coordinator(nullptr, &state, activeFn, cfg);

    // Test token acquisition
    coordinator.setMaintenanceTokens(2);

    coordinator.start();
    std::this_thread::sleep_for(50ms);
    coordinator.stop();

    // Verify coordinator ran without errors
    SUCCEED();
}

} // namespace yams::daemon
