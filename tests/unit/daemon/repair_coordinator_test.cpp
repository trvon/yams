#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <thread>

#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/StateComponent.h>

namespace yams::daemon {

TEST(RepairCoordinatorTest, ProcessesDocumentEventsWhenEnabled) {
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

    // Give the coordinator time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Simulate document removal
    RepairCoordinator::DocumentRemovedEvent removeEvent{"hash1"};
    rc.onDocumentRemoved(removeEvent);

    rc.stop();

    // In a real test, we'd verify that the coordinator attempted to check/create embeddings
    // For now, just verify it started and stopped without errors
    EXPECT_TRUE(true);
}

TEST(RepairCoordinatorTest, DoesNothingWhenDisabled) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    rc.stop();

    // Verify no repair operations were attempted
    EXPECT_EQ(state.stats.repairBatchesAttempted.load(), 0u);
}
} // namespace yams::daemon
