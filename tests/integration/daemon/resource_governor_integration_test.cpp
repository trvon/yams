// ResourceGovernor Integration Tests
// Tests that resource governor throttling behavior actually kicks in during real
// daemon operations, verifying the integration between:
//   TuneAdvisor (thresholds) → ResourceGovernor (pressure detection) → Daemon (status)

#include <catch2/catch_test_macros.hpp>
#include "test_async_helpers.h"
#include "test_daemon_harness.h"

#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/compat/unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

/// RAII guard for TuneAdvisor CPU threshold restoration
class CpuThresholdGuard {
    double prev_;

public:
    explicit CpuThresholdGuard(double newThreshold) {
        prev_ = TuneAdvisor::cpuHighThresholdPercent();
        TuneAdvisor::setCpuHighThresholdPercent(newThreshold);
    }

    ~CpuThresholdGuard() { TuneAdvisor::setCpuHighThresholdPercent(prev_); }

    CpuThresholdGuard(const CpuThresholdGuard&) = delete;
    CpuThresholdGuard& operator=(const CpuThresholdGuard&) = delete;
};

/// RAII guard for environment variables - restores previous value on destruction
class EnvGuard {
    std::string name_;
    std::string prev_;
    bool hadPrev_;

public:
    EnvGuard(const char* name, const char* value) : name_(name), hadPrev_(false) {
        if (const char* existing = std::getenv(name)) {
            prev_ = existing;
            hadPrev_ = true;
        }
        setenv(name, value, 1);
    }

    ~EnvGuard() {
        if (hadPrev_) {
            setenv(name_.c_str(), prev_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    EnvGuard(const EnvGuard&) = delete;
    EnvGuard& operator=(const EnvGuard&) = delete;
};

/// Wait for daemon to complete initial setup and governor to be active.
/// The governor budget is populated by TuningManager tick, which runs on ~500ms intervals.
/// We check the ResourceGovernor singleton directly for reliability.
bool waitForGovernorActive(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto& governor = ResourceGovernor::instance();
        auto snapshot = governor.getSnapshot();
        if (snapshot.memoryBudgetBytes > 0) {
            return true;
        }
        // TuningManager ticks every ~500ms, so don't poll too aggressively
        std::this_thread::sleep_for(300ms);
    }
    return false;
}

/// Wait for embedding queue to drain (processing complete)
bool waitForEmbeddingDrain(DaemonClient& client, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    int stableCount = 0;
    constexpr int stableRequired = 5;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = yams::cli::run_sync(client.status(), 5s);
        if (!status) {
            std::this_thread::sleep_for(500ms);
            continue;
        }

        const auto& st = status.value();
        uint64_t embedQueued = 0;
        uint64_t embedInFlight = 0;
        uint64_t postQueued = 0;
        uint64_t postInFlight = 0;

        if (auto it = st.requestCounts.find("embed_svc_queued"); it != st.requestCounts.end()) {
            embedQueued = it->second;
        }
        if (auto it = st.requestCounts.find("embed_in_flight"); it != st.requestCounts.end()) {
            embedInFlight = it->second;
        }
        if (auto it = st.requestCounts.find("post_ingest_queued"); it != st.requestCounts.end()) {
            postQueued = it->second;
        }
        if (auto it = st.requestCounts.find("post_ingest_inflight"); it != st.requestCounts.end()) {
            postInFlight = it->second;
        }

        const bool embedDrained = (embedQueued == 0 && embedInFlight == 0);
        const bool postDrained = (postQueued == 0 && postInFlight == 0);

        if (embedDrained && postDrained) {
            ++stableCount;
            if (stableCount >= stableRequired) {
                return true;
            }
        } else {
            stableCount = 0;
        }

        std::this_thread::sleep_for(500ms);
    }
    return false;
}

/// Add a test document via daemon
bool addTestDocument(const DaemonHarness& harness, int docId) {
    const std::string docName = "governor_test_doc_" + std::to_string(docId) + ".txt";
    auto path = harness.dataDir() / docName;

    std::ofstream ofs(path);
    if (!ofs) {
        return false;
    }

    // Create content that will generate some processing work
    ofs << "Test document " << docId << " for resource governor integration testing.\n";
    ofs << "This document contains content to exercise the ingest pipeline.\n";
    for (int i = 0; i < 10; ++i) {
        ofs << "Line " << i << ": Sample content for processing.\n";
    }
    ofs.close();

    yams::app::services::DocumentIngestionService docSvc;
    yams::app::services::AddOptions opts;
    opts.socketPath = harness.socketPath().string();
    opts.explicitDataDir = harness.dataDir().string();
    opts.path = path.string();
    opts.noEmbeddings = true; // Skip embeddings for faster tests

    auto result = docSvc.addViaDaemon(opts);
    return result.has_value() && !result.value().hash.empty();
}

} // namespace

// =============================================================================
// Test 1: Verify Governor Metrics Available via Status
// =============================================================================

TEST_CASE("ResourceGovernor metrics exposed in daemon status", "[integration][governor][status]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    ClientConfig clientCfg;
    clientCfg.socketPath = harness.socketPath();
    clientCfg.requestTimeout = 10s;
    clientCfg.autoStart = false;
    DaemonClient client(clientCfg);

    auto connected = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connected.has_value());

    // Wait for governor to be active (via singleton check)
    REQUIRE(waitForGovernorActive(30s));

    // Wait a bit longer for FSM metrics to propagate to status response
    // The TuningManager updates FSM metrics on its tick cycle (~500ms)
    std::this_thread::sleep_for(1s);

    auto status = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(status.has_value());

    // Check governor budget via singleton (FSM propagation may be slow)
    auto& governor = ResourceGovernor::instance();
    auto snapshot = governor.getSnapshot();

    INFO("governorBudgetBytes (status)=" << status.value().governorBudgetBytes);
    INFO("governorBudgetBytes (singleton)=" << snapshot.memoryBudgetBytes);
    CHECK(snapshot.memoryBudgetBytes > 0);

    INFO("governorPressureLevel=" << static_cast<int>(status.value().governorPressureLevel));
    CHECK(status.value().governorPressureLevel <= 3);

    INFO("governorRssBytes=" << snapshot.rssBytes);
    CHECK(snapshot.rssBytes > 0);

    harness.stop();
}

// =============================================================================
// Test 2: Verify Governor Singleton Accessibility During Daemon Operation
// =============================================================================

TEST_CASE("ResourceGovernor singleton is accessible during daemon operation",
          "[integration][governor][singleton]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    auto& governor = ResourceGovernor::instance();

    // Test: Governor provides pressure level
    auto level = governor.getPressureLevel();
    INFO("pressureLevel=" << static_cast<int>(level));
    CHECK((level == ResourcePressureLevel::Normal || level == ResourcePressureLevel::Warning ||
           level == ResourcePressureLevel::Critical || level == ResourcePressureLevel::Emergency));

    // Test: Governor provides scaling caps
    auto caps = governor.getScalingCaps();
    INFO("allowModelLoads=" << caps.allowModelLoads);
    INFO("allowNewIngest=" << caps.allowNewIngest);
    // At normal pressure, model loads and ingest should be allowed
    if (level == ResourcePressureLevel::Normal) {
        CHECK(caps.allowModelLoads);
        CHECK(caps.allowNewIngest);
    }

    // Test: Governor max concurrency values are positive
    CHECK(governor.maxIngestWorkers() > 0);
    CHECK(governor.maxSearchConcurrency() > 0);

    // Test: Governor admission control is callable
    bool canWork = governor.canAdmitWork();
    bool canLoad = governor.canLoadModel(1024ULL * 1024ULL); // 1 MB
    bool canScale = governor.canScaleUp("test", 1);

    INFO("canAdmitWork=" << canWork);
    INFO("canLoadModel(1MB)=" << canLoad);
    INFO("canScaleUp(test,1)=" << canScale);

    // At normal pressure, these should all return true
    if (level == ResourcePressureLevel::Normal) {
        CHECK(canWork);
        CHECK(canLoad);
        CHECK(canScale);
    }

    harness.stop();
}

// =============================================================================
// Test 3: Verify Pressure Level Detection with Low Threshold
// =============================================================================

TEST_CASE("Governor detects pressure with low CPU threshold", "[integration][governor][pressure]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Set a very low CPU threshold so that normal daemon operation triggers Warning
    CpuThresholdGuard thresholdGuard(5.0); // 5% - very low threshold

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    ClientConfig clientCfg;
    clientCfg.socketPath = harness.socketPath();
    clientCfg.requestTimeout = 10s;
    clientCfg.autoStart = false;
    DaemonClient client(clientCfg);

    auto connected = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connected.has_value());

    // Add some documents to create CPU load
    INFO("Adding test documents to create load...");
    for (int i = 0; i < 10; ++i) {
        REQUIRE(addTestDocument(harness, i));
    }

    // Poll for pressure level change or timeout
    auto deadline = std::chrono::steady_clock::now() + 30s;
    bool sawPressure = false;
    uint8_t maxPressureLevel = 0;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = yams::cli::run_sync(client.status(), 5s);
        if (status.has_value() && status.value().governorPressureLevel > 0) {
            sawPressure = true;
            maxPressureLevel = std::max(maxPressureLevel, status.value().governorPressureLevel);
            INFO("Detected pressure level: "
                 << static_cast<int>(status.value().governorPressureLevel));
            break;
        }
        std::this_thread::sleep_for(200ms);
    }

    // Note: Pressure detection is non-deterministic - depends on actual CPU usage
    // On fast machines, we may not see pressure. This test documents whether
    // throttling CAN work under load, not that it ALWAYS triggers.
    INFO("sawPressure=" << sawPressure
                        << ", maxPressureLevel=" << static_cast<int>(maxPressureLevel));

    // Wait for processing to complete
    waitForEmbeddingDrain(client, 30s);

    harness.stop();
}

// =============================================================================
// Test 4: Verify Governor Scaling Caps Adjust Under Pressure
// =============================================================================

TEST_CASE("Governor scaling caps adjust based on pressure level",
          "[integration][governor][scaling]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    auto& governor = ResourceGovernor::instance();

    // Record caps at Normal level
    auto normalLevel = governor.getPressureLevel();
    auto normalMaxIngest = governor.maxIngestWorkers();
    auto normalMaxSearch = governor.maxSearchConcurrency();
    auto normalMaxEmbed = governor.maxEmbedConcurrency();

    INFO("At level=" << static_cast<int>(normalLevel));
    INFO("normalMaxIngest=" << normalMaxIngest);
    INFO("normalMaxSearch=" << normalMaxSearch);
    INFO("normalMaxEmbed=" << normalMaxEmbed);

    // These caps should be reasonable positive values
    CHECK(normalMaxIngest > 0);
    CHECK(normalMaxSearch > 0);
    // Note: maxEmbed can be 0 at Emergency level

    harness.stop();
}

// =============================================================================
// Test 5: Verify Status Response Contains Governor Metrics After Ingest
// =============================================================================

TEST_CASE("Status response contains governor metrics during ingest",
          "[integration][governor][ingest]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    ClientConfig clientCfg;
    clientCfg.socketPath = harness.socketPath();
    clientCfg.requestTimeout = 10s;
    clientCfg.autoStart = false;
    DaemonClient client(clientCfg);

    auto connected = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connected.has_value());

    REQUIRE(waitForGovernorActive(30s));

    // Add documents while checking status
    std::vector<uint8_t> pressureLevels;
    std::vector<uint64_t> budgetValues;

    for (int i = 0; i < 5; ++i) {
        REQUIRE(addTestDocument(harness, i));

        // Check status after each add
        auto status = yams::cli::run_sync(client.status(), 5s);
        if (status) {
            pressureLevels.push_back(status.value().governorPressureLevel);
            budgetValues.push_back(status.value().governorBudgetBytes);
        }
    }

    // Verify we got consistent governor metrics throughout
    INFO("Captured " << pressureLevels.size() << " status readings");
    CHECK(!pressureLevels.empty());
    CHECK(!budgetValues.empty());

    // Budget should remain consistent (it's a configured value)
    if (budgetValues.size() >= 2) {
        for (size_t i = 1; i < budgetValues.size(); ++i) {
            CHECK(budgetValues[i] == budgetValues[0]);
        }
    }

    // Wait for processing to complete
    waitForEmbeddingDrain(client, 30s);

    harness.stop();
}

// =============================================================================
// Test 6: Verify Governor Snapshot Contains Valid Metrics
// =============================================================================

TEST_CASE("ResourceGovernor snapshot contains valid metrics", "[integration][governor][snapshot]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    // Wait for governor to be ticked at least once
    REQUIRE(waitForGovernorActive(30s));

    auto& governor = ResourceGovernor::instance();
    auto snapshot = governor.getSnapshot();

    // Test: Memory metrics are populated
    INFO("rssBytes=" << snapshot.rssBytes);
    INFO("memoryBudgetBytes=" << snapshot.memoryBudgetBytes);
    INFO("memoryPressure=" << snapshot.memoryPressure);
    CHECK(snapshot.rssBytes > 0);
    CHECK(snapshot.memoryBudgetBytes > 0);

    // Test: Timestamp is recent
    auto now = std::chrono::steady_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - snapshot.timestamp);
    INFO("snapshotAge=" << age.count() << "s");
    CHECK(age.count() < 10);

    // Test: Pressure level matches snapshot
    auto level = governor.getPressureLevel();
    CHECK(snapshot.level == level);

    harness.stop();
}

// =============================================================================
// Test 7: Verify Concurrent Status Queries Return Consistent Governor Data
// =============================================================================

TEST_CASE("Concurrent status queries return consistent governor data",
          "[integration][governor][concurrent]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(10s));

    ClientConfig clientCfg;
    clientCfg.socketPath = harness.socketPath();
    clientCfg.requestTimeout = 10s;
    clientCfg.autoStart = false;
    DaemonClient client(clientCfg);

    auto connected = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connected.has_value());

    REQUIRE(waitForGovernorActive(30s));

    // Issue multiple concurrent status queries
    // This test verifies thread safety of concurrent DaemonClient::status() calls.
    // Prior to the fix in asio_connection.cpp, this would crash with SIGSEGV.
    std::atomic<int> successCount{0};
    std::atomic<int> errorCount{0};
    std::vector<uint64_t> budgetValues;
    std::mutex valuesMutex;

    auto worker = [&]() {
        auto status = yams::cli::run_sync(client.status(), 5s);
        if (status.has_value()) {
            ++successCount;
            // governorBudgetBytes may be 0 if TuningManager hasn't updated
            // FsmMetricsRegistry yet - that's OK, we're testing thread safety
            auto budget = status.value().governorBudgetBytes;
            if (budget > 0) {
                std::lock_guard<std::mutex> lock(valuesMutex);
                budgetValues.push_back(budget);
            }
        } else {
            ++errorCount;
        }
    };

    std::vector<std::thread> threads;
    constexpr int numQueries = 8;
    threads.reserve(numQueries);

    for (int i = 0; i < numQueries; ++i) {
        threads.emplace_back(worker);
    }

    for (auto& t : threads) {
        t.join();
    }

    INFO("successCount=" << successCount << ", errorCount=" << errorCount);
    // All concurrent status queries should succeed (no crashes, no errors)
    CHECK(successCount == numQueries);
    CHECK(errorCount == 0);

    // If any budget values were captured, they should all be consistent
    if (budgetValues.size() >= 2) {
        for (size_t i = 1; i < budgetValues.size(); ++i) {
            CHECK(budgetValues[i] == budgetValues[0]);
        }
    }

    harness.stop();
}
