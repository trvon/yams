// SPDX-License-Identifier: GPL-3.0-or-later
#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include "../../common/service_manager_test_helper.h"
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams::daemon;

namespace {
// Dedicated process: one real ServiceManager and one separately owned tuning publisher.
struct TuningRegistryFixture {
    struct LogGuard {
        spdlog::level::level_enum previous{spdlog::get_level()};
        LogGuard() { spdlog::set_level(spdlog::level::warn); }
        ~LogGuard() { spdlog::set_level(previous); }
    } logGuard;
    yams::test::TempDirGuard directory{"tuning_registry_activity_"};
    const std::shared_ptr<const TuningSnapshot> savedSnapshot{
        TuningSnapshotRegistry::instance().get()};
    DaemonConfig config;
    StateComponent state;
    DaemonLifecycleFsm lifecycle;
    std::shared_ptr<ServiceManager> services;
    std::unique_ptr<TuningManager> manager;

    TuningRegistryFixture() {
        config.dataDir = directory.path();
        config.socketPath = directory.path() / "unused.sock";
        config.pidFile = directory.path() / "daemon.pid";
        config.logFile = directory.path() / "daemon.log";
        config.enableModelProvider = false;
        config.modelProviderRequired = false;
        config.useMockModelProvider = false;
        config.autoLoadPlugins = false;
        config.enableAutoRepair = false;
        config.autoRebuildOnDimMismatch = false;
        config.embeddedOneShot = true;
        config.searchMaintenance.automaticRebuildsEnabled = false;
        config.searchMaintenance.automaticRebuildsSource = "typed:test";
        config.memorySync.enabled = false;
        config.downloadPolicy.enable = false;
    }
    ~TuningRegistryFixture() {
        // stop() posts to the coordinator strand and waits: never stop that executor first.
        if (manager)
            manager->stop();
        // Destroy its strand/executor while the coordinator's context still exists.
        // ServiceManager::shutdown() resets the coordinator, not just its worker threads.
        manager.reset();
        state.stats.activeConnections.store(0);
        state.stats.healthCheckConnections.store(0);
        state.stats.activeNonHealthRequests.store(0);
        if (services)
            services->shutdown();
        services.reset();
        TuningSnapshotRegistry::instance().set(savedSnapshot);
    }
    void initialize() {
        // Run after fixture construction so failed REQUIREs still shut down real services.
        services = std::make_shared<ServiceManager>(config, state, lifecycle);
        REQUIRE(
            yams::test::initializeServiceManagerFully(services, std::chrono::seconds{30}, false));
        REQUIRE(state.readiness.metadataRepoReady.load());
        auto repo = services->getMetadataRepo();
        REQUIRE(repo != nullptr);
        REQUIRE(repo->getFts5IndexedRowIdSet().has_value()); // Real schema/provider control.
        auto* coordinator = services->getWorkCoordinator();
        REQUIRE(coordinator != nullptr);
        REQUIRE(coordinator->isRunning());
        REQUIRE(coordinator->getWorkerCount() >= 2);
        manager = std::make_unique<TuningManager>(services.get(), &state, coordinator);
    }
    std::shared_ptr<const TuningSnapshot> publishSnapshot(bool idle) {
        // Drain the prior tick before taking the publication baseline. A fresh pointer alone
        // cannot distinguish an old-input tick that finishes after a counter changes.
        // These controls test real classification across public restarts, not continuous retuning.
        manager->stop();
        const auto previous = TuningSnapshotRegistry::instance().get();
        manager->start(); // Synchronous real tick, followed by the asynchronous loop.
        TuningManager::notifyWakeup();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{4};
        do {
            auto snapshot = TuningSnapshotRegistry::instance().get();
            // Keep previous alive: address reuse cannot masquerade as a new publication.
            if (snapshot && snapshot.get() != previous.get() && snapshot->daemonIdle == idle)
                return snapshot;
            // The registry has no subscription API. Bound observation across the fallback
            // cadence; this is not a notification latency or CPU-idleness assertion.
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        } while (std::chrono::steady_clock::now() < deadline);
        return nullptr;
    }
};
} // namespace

TEST_CASE_METHOD(
    TuningRegistryFixture,
    "TuningManager: published idle state follows executing requests, not socket occupancy",
    "[daemon][tuning][registry][activity][slow][regression]") {
    initialize();
    REQUIRE(state.stats.activeConnections.load() == 0);
    REQUIRE(state.stats.healthCheckConnections.load() == 0);
    REQUIRE(state.stats.activeNonHealthRequests.load() == 0);
    auto quiet = publishSnapshot(true); // No readiness/private-tick hook.
    REQUIRE(quiet != nullptr); // Actual ready services and quiet queues published a fresh snapshot.

    // Controlled inputs exercise the real manager/registry, NOT SocketServer acceptance or
    // RequestHandler guard wiring. Those paths require separate lifecycle/socket tests.
    state.stats.activeConnections.store(1);
    auto connectedIdle = publishSnapshot(true);
    REQUIRE(connectedIdle != nullptr);

    state.stats.activeNonHealthRequests.store(1);
    auto busy = publishSnapshot(false);
    REQUIRE(busy != nullptr);

    // Health and non-health work may overlap on the SAME multiplexed connection.
    state.stats.healthCheckConnections.store(1);
    REQUIRE(state.stats.activeConnections.load() == 1);
    auto healthOverlap = publishSnapshot(false);
    REQUIRE(healthOverlap != nullptr);

    state.stats.healthCheckConnections.store(0);
    state.stats.activeNonHealthRequests.store(0);
    auto returnedIdle = publishSnapshot(true);
    REQUIRE(returnedIdle != nullptr);
    CHECK(state.stats.activeConnections.load() == 1);

    manager->stop(); // Called from this test thread while the coordinator still services posts.
    CHECK(services->getWorkCoordinator()->isRunning());
}
