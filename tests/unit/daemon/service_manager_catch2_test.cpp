// Catch2 migration of service_manager_test.cpp
// Migration: yams-3s4 (daemon unit tests)
// Unit tests for ServiceManager component - construction, initialization, and service access

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <system_error>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;

namespace yams::daemon::test {

// Test fixture for ServiceManager tests
struct ServiceManagerFixture {
    yams::test::ScopedEnvVar disableWatcher_{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    fs::path testDir_;

    ServiceManagerFixture() {
        // Create isolated test directory
        testDir_ = fs::temp_directory_path() /
                   ("sm_test_" + std::to_string(::getpid()) + "_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        // Setup basic config
        config_.dataDir = testDir_ / "data";
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";

        fs::create_directories(config_.dataDir);
    }

    ~ServiceManagerFixture() {
        // Cleanup test directory
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }
};

struct ScopedCurrentPath {
    fs::path original_;

    explicit ScopedCurrentPath(const fs::path& target) : original_(fs::current_path()) {
        fs::current_path(target);
    }

    ~ScopedCurrentPath() {
        std::error_code ec;
        fs::current_path(original_, ec);
    }
};

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager construction succeeds",
                 "[daemon][service_manager]") {
    REQUIRE_NOTHROW(ServiceManager(config_, state_, lifecycleFsm_));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager getName returns correct component name",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    REQUIRE(std::string(sm.getName()) == "ServiceManager");
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager service accessors after construction",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    // PBI-057: Vector DB initialization is deferred to async phase
    REQUIRE(sm.getVectorDatabase() == nullptr);

    // Other services are initialized during initialize(), not in constructor
    REQUIRE(sm.getContentStore() == nullptr);
    REQUIRE(sm.getMetadataRepo() == nullptr);
    REQUIRE(sm.getModelProvider() == nullptr);
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager multiple construction is idempotent",
                 "[daemon][service_manager]") {
    // Create two instances sequentially - should not throw
    ServiceManager sm1(config_, state_, lifecycleFsm_);
    ServiceManager sm2(config_, state_, lifecycleFsm_);
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager construction with missing data directory",
                 "[daemon][service_manager]") {
    fs::remove_all(config_.dataDir);
    REQUIRE_NOTHROW(ServiceManager(config_, state_, lifecycleFsm_));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager destructor handles cleanup",
                 "[daemon][service_manager]") {
    auto sm = std::make_unique<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE_NOTHROW(sm.reset());
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager getConfig returns configuration",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    const auto& cfg = sm.getConfig();
    REQUIRE(cfg.dataDir == config_.dataDir);
    REQUIRE(cfg.socketPath == config_.socketPath);
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager PostIngestQueue accessor before init",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    // May be null before initialization
    auto piq = sm.getPostIngestQueue();
    (void)piq;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager tuning config getter doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    const auto& tuning = sm.getTuningConfig();
    (void)tuning;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager set tuning config doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    TuningConfig tc;
    tc.postIngestCapacity = 1000;
    tc.postIngestThreadsMin = 2;
    tc.postIngestThreadsMax = 4;

    REQUIRE_NOTHROW(sm.setTuningConfig(tc));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager getWorkerQueueDepth doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto depth = sm.getWorkerQueueDepth();
    (void)depth;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager enqueuePostIngest doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    REQUIRE_NOTHROW(sm.enqueuePostIngest("test_hash", "text/plain"));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager search engine snapshot doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto snapshot = sm.getSearchEngineFsmSnapshot();
    (void)snapshot.buildReason;
    (void)snapshot.vectorEnabled;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager cached search engine access doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto* engine = sm.getCachedSearchEngine();
    (void)engine; // May be null, that's OK
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager memory cleanup verification",
                 "[daemon][service_manager][.slow]") {
    // Note: This test rapidly creates/destroys ServiceManagers which may be flaky
    // on some platforms due to timing-sensitive cleanup
    for (int i = 0; i < 3; ++i) {
        auto sm = std::make_unique<ServiceManager>(config_, state_, lifecycleFsm_);
        sm.reset();
    }
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager restart creates fresh io_context (PBI-066-38)",
                 "[daemon][service_manager]") {
    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);

    // First init
    auto r1 = sm->initialize();
    REQUIRE(r1);

    // Stop services
    sm->shutdown();

    // Second init should succeed and not throw bad executor
    auto r2 = sm->initialize();
    REQUIRE(r2);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager initializes WAL under configured data directory",
                 "[daemon][service_manager][wal]") {
    const auto runRoot = testDir_ / "cwd";
    fs::create_directories(runRoot);
    ScopedCurrentPath cwdGuard(runRoot);

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);

    const auto expectedWalDir = config_.dataDir / "wal";
    REQUIRE(fs::exists(expectedWalDir));
    REQUIRE(fs::is_directory(expectedWalDir));

    const auto misplacedWalDir = runRoot / "wal";
    CHECK_FALSE(fs::exists(misplacedWalDir));
}

} // namespace yams::daemon::test
