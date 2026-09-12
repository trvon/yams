#include <yams/daemon/components/MemorySyncCoordinator.h>
#include <yams/daemon/daemon.h>

#include <catch2/catch_test_macros.hpp>

TEST_CASE("Memory sync coordinator disabled and rejected startup are inert",
          "[daemon][memory-sync][coordinator]") {
    yams::daemon::DaemonConfig config;
    config.memorySync.enabled = false;
    // The configuration must outlive the coordinator; this is the owner's existing contract.
    yams::daemon::MemorySyncCoordinator::Dependencies deps;
    deps.config = &config;
    yams::daemon::MemorySyncCoordinator coordinator{std::move(deps)};

    SECTION("disabled lifecycle needs no store callbacks") {
        REQUIRE(coordinator.initializeMemorySync({}));
        REQUIRE(coordinator.configureMemorySyncApply());
        CHECK(coordinator.service() == nullptr);
        CHECK_FALSE(coordinator.publishMemorySync("key", "value"));
        CHECK_FALSE(coordinator.deleteMemorySync("key"));
        CHECK_FALSE(coordinator.readMemorySyncCached("key"));
        CHECK_FALSE(coordinator.getMemorySyncStatus());
    }
    SECTION("personal corpus is rejected before store creation") {
        config.memorySync.enabled = true;
        config.memorySync.corpusScope = yams::memory_sync::CorpusScope::Personal;
        const auto result = coordinator.initializeMemorySync({});
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
        CHECK(coordinator.service() == nullptr);
    }
    coordinator.shutdown();
    coordinator.shutdown();
    CHECK(coordinator.service() == nullptr);
}
