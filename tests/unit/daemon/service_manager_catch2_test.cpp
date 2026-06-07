// Catch2 migration of service_manager_test.cpp
// Migration: yams-3s4 (daemon unit tests)
// Unit tests for ServiceManager component - construction, initialization, and service access

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <system_error>
#include <thread>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/repair/repair_health_probe.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/database.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;

namespace yams::daemon::test {
namespace {

fs::path metadataDbPath(const DaemonConfig& config) {
    return config.dataDir / "yams.db";
}

void seedMetadataDb(const fs::path& dbPath, const std::string& value = "seed") {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
    REQUIRE(
        db.execute("CREATE TABLE IF NOT EXISTS startup_probe(id INTEGER PRIMARY KEY, value TEXT)"));
    REQUIRE(db.execute("DELETE FROM startup_probe"));
    REQUIRE(db.execute("INSERT INTO startup_probe(value) VALUES('" + value + "')"));
    db.close();
}

void writeCorruptDb(const fs::path& dbPath) {
    std::ofstream out(dbPath, std::ios::binary | std::ios::trunc);
    REQUIRE(out.good());
    static constexpr char kSqliteHeader[] = "SQLite format 3";
    out.write(kSqliteHeader, static_cast<std::streamsize>(sizeof(kSqliteHeader)));
    const std::string garbage(4096, '\xff');
    out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
    out.close();
}

std::optional<fs::path> findQuarantinedFile(const fs::path& dataDir) {
    std::error_code ec;
    if (!fs::exists(dataDir, ec)) {
        return std::nullopt;
    }
    const std::string prefix = "yams.db.corrupt-";
    for (fs::directory_iterator it(dataDir, ec), end; it != end; it.increment(ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        if (it->path().filename().string().rfind(prefix, 0) == 0 &&
            it->path().extension() != ".sentinel") {
            return it->path();
        }
    }
    return std::nullopt;
}

std::size_t startupProbeRowCount(const fs::path& dbPath) {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite));
    auto stmtR = db.prepare("SELECT COUNT(*) FROM startup_probe");
    REQUIRE(stmtR);
    auto& stmt = stmtR.value();
    REQUIRE(stmt.step());
    auto count = static_cast<std::size_t>(stmt.getInt(0));
    db.close();
    return count;
}

void seedDummyWalSidecar(const fs::path& dbPath, const std::string& payload = "stale-wal") {
    std::ofstream out(dbPath.string() + "-wal", std::ios::binary | std::ios::trunc);
    REQUIRE(out.good());
    out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    out.close();
    REQUIRE(fs::exists(dbPath.string() + "-wal"));
}

bool walSidecarCleared(const fs::path& dbPath) {
    std::error_code ec;
    const fs::path walPath(dbPath.string() + "-wal");
    if (!fs::exists(walPath, ec)) {
        return true;
    }
    return fs::file_size(walPath, ec) == 0;
}

bool walSidecarStillMatchesPayload(const fs::path& dbPath, const std::string& payload) {
    const fs::path walPath(dbPath.string() + "-wal");
    std::error_code ec;
    if (!fs::exists(walPath, ec) || fs::file_size(walPath, ec) != payload.size()) {
        return false;
    }
    std::ifstream in(walPath, std::ios::binary);
    std::string actual(payload.size(), '\0');
    in.read(actual.data(), static_cast<std::streamsize>(actual.size()));
    return in.good() && actual == payload;
}

void requireReadyDatabaseState(const StateComponent& state) {
    CHECK(state.readiness.databaseReady.load());
    std::lock_guard<std::mutex> lk(state.readiness.recoveryMutex);
    CHECK((state.readiness.databasePhase == "ready"));
}

} // namespace

// Test fixture for ServiceManager tests
struct ServiceManagerFixture {
    yams::test::ScopedEnvVar disableWatcher_{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    fs::path testDir_;

    ServiceManagerFixture() {
        // Create isolated test directory
        testDir_ =
            fs::temp_directory_path() /
            ("sm_test_" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) +
             "_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
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
    REQUIRE((std::string(sm.getName()) == "ServiceManager"));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager service accessors after construction",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    // PBI-057: Vector DB initialization is deferred to async phase
    REQUIRE((sm.getVectorDatabase() == nullptr));

    // Other services are initialized during initialize(), not in constructor
    REQUIRE((sm.getContentStore() == nullptr));
    REQUIRE((sm.getMetadataRepo() == nullptr));
    REQUIRE((sm.getModelProvider() == nullptr));
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
    REQUIRE((cfg.dataDir == config_.dataDir));
    REQUIRE((cfg.socketPath == config_.socketPath));
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

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager async init starts only once per initialize cycle",
                 "[daemon][service_manager][startup][async_init]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);

    std::promise<void> firstBarrierPromise;
    auto firstBarrierFuture = firstBarrierPromise.get_future();
    std::atomic<bool> firstBarrierSet{false};
    sm->startAsyncInit(&firstBarrierPromise, &firstBarrierSet);

    REQUIRE((firstBarrierFuture.wait_for(std::chrono::seconds(2)) == std::future_status::ready));
    CHECK(firstBarrierSet.load(std::memory_order_acquire));

    std::promise<void> secondBarrierPromise;
    auto secondBarrierFuture = secondBarrierPromise.get_future();
    std::atomic<bool> secondBarrierSet{false};
    sm->startAsyncInit(&secondBarrierPromise, &secondBarrierSet);

    CHECK((secondBarrierFuture.wait_for(std::chrono::milliseconds(200)) ==
           std::future_status::timeout));
    CHECK_FALSE(secondBarrierSet.load(std::memory_order_acquire));

    const auto snapshot = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((snapshot.state == ServiceManagerState::Ready));
    requireReadyDatabaseState(state_);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager records recovery provenance for corrupt metadata DB startup",
                 "[daemon][service_manager][startup][recovery]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    writeCorruptDb(dbPath);
    seedDummyWalSidecar(dbPath, "corrupt-sidecar");

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((smSnap.state == ServiceManagerState::Ready));

    requireReadyDatabaseState(state_);
    std::optional<fs::path> quarantinedPath;
    {
        std::lock_guard<std::mutex> lk(state_.readiness.recoveryMutex);
        REQUIRE_FALSE(state_.readiness.databaseRecoveredFrom.empty());
        quarantinedPath = findQuarantinedFile(config_.dataDir);
        REQUIRE(quarantinedPath.has_value());
        CHECK((state_.readiness.databaseRecoveredFrom == quarantinedPath->string()));
    }
    CHECK(fs::exists(*quarantinedPath));
    CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, "corrupt-sidecar"));

    sm->shutdown();
    CHECK(walSidecarCleared(dbPath));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager clears stale metadata WAL during startup",
                 "[daemon][service_manager][wal][startup]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    seedMetadataDb(dbPath, "stale_wal_seed");
    const std::string staleWalPayload = "stale-wal";
    seedDummyWalSidecar(dbPath, staleWalPayload);

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((smSnap.state == ServiceManagerState::Ready));

    requireReadyDatabaseState(state_);
    {
        std::lock_guard<std::mutex> lk(state_.readiness.recoveryMutex);
        CHECK(state_.readiness.databaseRecoveredFrom.empty());
    }
    CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, staleWalPayload));
    CHECK((startupProbeRowCount(dbPath) == 1U));

    sm->shutdown();
    CHECK(walSidecarCleared(dbPath));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown remains idempotent after async init reaches ready",
                 "[daemon][service_manager][shutdown][idempotent]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    sm->shutdown();
    const auto firstShutdown = sm->getServiceManagerFsmSnapshot();
    CHECK((firstShutdown.state == ServiceManagerState::Stopped));
    CHECK((sm->__test_getAbiHost() == nullptr));

    REQUIRE_NOTHROW(sm->shutdown());
    const auto secondShutdown = sm->getServiceManagerFsmSnapshot();
    CHECK((secondShutdown.state == ServiceManagerState::Stopped));
    CHECK((sm->getMetadataRepo() == nullptr));
    CHECK((sm->getContentStore() == nullptr));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown waits for an in-flight worker task",
                 "[daemon][service_manager][shutdown][regression]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;
    config_.enableAutoRepair = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool allowWorkerExit = false;
    std::atomic<bool> workerEntered{false};
    std::atomic<bool> workerFinished{false};
    std::atomic<bool> shutdownReturned{false};

    boost::asio::post(sm->getWorkerExecutor(), [&]() {
        {
            std::lock_guard<std::mutex> lk(gateMutex);
            workerEntered.store(true, std::memory_order_release);
        }
        gateCv.notify_all();

        std::unique_lock<std::mutex> lk(gateMutex);
        gateCv.wait(lk, [&] { return allowWorkerExit; });
        workerFinished.store(true, std::memory_order_release);
    });

    {
        std::unique_lock<std::mutex> lk(gateMutex);
        REQUIRE(gateCv.wait_for(lk, std::chrono::seconds(5),
                                [&] { return workerEntered.load(std::memory_order_acquire); }));
    }

    std::thread shutdownThread([&]() {
        sm->shutdown();
        shutdownReturned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK_FALSE(shutdownReturned.load(std::memory_order_acquire));
    CHECK_FALSE(workerFinished.load(std::memory_order_acquire));

    {
        std::lock_guard<std::mutex> lk(gateMutex);
        allowWorkerExit = true;
    }
    gateCv.notify_all();

    {
        std::unique_lock<std::mutex> lk(gateMutex);
        REQUIRE(gateCv.wait_for(lk, std::chrono::seconds(5),
                                [&] { return workerFinished.load(std::memory_order_acquire); }));
    }

    shutdownThread.join();
    CHECK(shutdownReturned.load(std::memory_order_acquire));
    CHECK((sm->getServiceManagerFsmSnapshot().state == ServiceManagerState::Stopped));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown interrupts blocked auto-repair graph health probe",
                 "[daemon][service_manager][shutdown][auto_repair][regression]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;
    config_.enableAutoRepair = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    auto writePool = sm->getWriteConnectionPool();
    REQUIRE((writePool != nullptr));

    std::vector<std::unique_ptr<metadata::PooledConnection>> heldConnections;
    heldConnections.reserve(32);
    for (std::size_t i = 0; i < 128; ++i) {
        auto conn = writePool->acquire(std::chrono::milliseconds(0));
        if (!conn) {
            break;
        }
        heldConnections.emplace_back();
        std::swap(heldConnections.back(), conn.value());
    }
    REQUIRE_FALSE(heldConnections.empty());
    REQUIRE_FALSE(writePool->acquire(std::chrono::milliseconds(0)).has_value());

    std::atomic<bool> probeStarted{false};
    std::atomic<bool> probeFinished{false};
    std::atomic<bool> shutdownReturned{false};

    boost::asio::post(sm->getWorkerExecutor(), [sm, &probeStarted, &probeFinished]() {
        probeStarted.store(true, std::memory_order_release);
        repair::RepairHealthProbe probe(sm->getMetadataRepo(), sm->getVectorDatabase(),
                                        sm->getGraphComponent(), sm->getKgStore());
        repair::RepairHealthOptions opts;
        opts.checkFts5 = false;
        opts.checkEmbeddings = false;
        opts.checkGraph = true;
        opts.scanDocuments = false;
        (void)probe.probe(opts);
        probeFinished.store(true, std::memory_order_release);
    });

    REQUIRE(yams::test::wait_for_condition(
        std::chrono::seconds(5), std::chrono::milliseconds(5),
        [&]() { return probeStarted.load(std::memory_order_acquire); }));

    std::thread shutdownThread([&]() {
        sm->shutdown();
        shutdownReturned.store(true, std::memory_order_release);
    });

    REQUIRE(yams::test::wait_for_condition(
        std::chrono::seconds(5), std::chrono::milliseconds(5), [&]() {
            return shutdownReturned.load(std::memory_order_acquire) &&
                   probeFinished.load(std::memory_order_acquire);
        }));

    shutdownThread.join();
    CHECK(shutdownReturned.load(std::memory_order_acquire));
    CHECK(probeFinished.load(std::memory_order_acquire));
    CHECK((sm->getServiceManagerFsmSnapshot().state == ServiceManagerState::Stopped));

    heldConnections.clear();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager restart cycles recover freshly seeded stale WAL",
                 "[daemon][service_manager][wal][restart]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    seedMetadataDb(dbPath, "restart_cycle_seed");

    for (int cycle = 0; cycle < 3; ++cycle) {
        StateComponent cycleState;
        DaemonLifecycleFsm cycleLifecycle;
        const std::string staleWalPayload = "wal-cycle-" + std::to_string(cycle);
        seedDummyWalSidecar(dbPath, staleWalPayload);

        auto sm = std::make_shared<ServiceManager>(config_, cycleState, cycleLifecycle);
        auto init = sm->initialize();
        REQUIRE(init);
        sm->startAsyncInit();
        auto smSnap = sm->waitForServiceManagerTerminalState(30);
        REQUIRE((smSnap.state == ServiceManagerState::Ready));

        INFO("cycle=" << cycle);
        requireReadyDatabaseState(cycleState);
        CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, staleWalPayload));
        CHECK((startupProbeRowCount(dbPath) == 1U));

        sm->shutdown();
        CHECK(walSidecarCleared(dbPath));
    }
}

} // namespace yams::daemon::test
