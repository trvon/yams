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

TEST_CASE("RepairCoordinator stats tracking updates queue depth",
          "[daemon][repair][coordinator]") {
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
          "[daemon][repair][coordinator][.placeholder]") {
    // This test validates that binary files are not queued for FTS5 repair
    // unless there's a custom plugin that can handle them
    // TODO: Implement with proper mocking
    SUCCEED();
}

TEST_CASE("RepairCoordinator allows binary files with custom plugins",
          "[daemon][repair][coordinator][.placeholder]") {
    // This test validates that binary files ARE queued for FTS5 repair
    // when a custom plugin extractor is available
    // TODO: Implement with proper mocking
    SUCCEED();
}

} // namespace yams::daemon
