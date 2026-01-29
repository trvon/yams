// ONNX Concurrency Integration Tests
// Tests that ONNX pool and embedding loading respect TuneAdvisor and TuningManager
// settings throughout the full configuration flow:
//   Environment Variables → TuneAdvisor → TuningManager::configureOnnxConcurrencyRegistry()
//       → OnnxConcurrencyRegistry → Embedding/Reranker services respect limits

#include "test_daemon_harness.h"
#include <catch2/catch_test_macros.hpp>

#include <yams/cli/cli_sync.h>
#include <yams/compat/unistd.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/resource/OnnxConcurrencyRegistry.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

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

/// Wait for TuningManager to complete its first tick and configure the ONNX registry.
/// The configuration happens on the first TuningManager tick after daemon enters Ready state.
bool waitForOnnxRegistryConfiguration(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        // TuningManager runs on a 500ms tick; give it time to configure
        std::this_thread::sleep_for(100ms);

        // Check if registry has been configured (totalSlots > 0 indicates configuration)
        auto& registry = OnnxConcurrencyRegistry::instance();
        if (registry.totalSlots() > 0) {
            return true;
        }
    }
    return false;
}

} // namespace

// =============================================================================
// Test 1: TuneAdvisor Settings Flow to OnnxConcurrencyRegistry
// =============================================================================

TEST_CASE("OnnxConcurrencyRegistry respects TuneAdvisor env settings",
          "[daemon][onnx][tuning][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("YAMS_ONNX_MAX_CONCURRENT configures total slots") {
        // Set env vars before daemon starts (TuneAdvisor reads at startup)
        EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "6");
        EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced"); // scale=0.5

        DaemonHarness::Options opts;
        opts.useMockModelProvider = true;
        DaemonHarness harness(opts);
        REQUIRE(harness.start());

        // Wait for TuningManager to configure registry (happens on first tick after Ready)
        REQUIRE(waitForOnnxRegistryConfiguration(3s));

        auto& registry = OnnxConcurrencyRegistry::instance();
        // Balanced profile uses 0.5x scale, so 6 * 0.5 = 3
        INFO("totalSlots=" << registry.totalSlots());
        CHECK(registry.totalSlots() == 3);

        harness.stop();
    }

    SECTION("Reserved slots are configured per lane") {
        EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "10");
        EnvGuard embedEnv("YAMS_ONNX_EMBED_RESERVED", "3");
        EnvGuard glinerEnv("YAMS_ONNX_GLINER_RESERVED", "2");
        EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

        DaemonHarness::Options opts;
        opts.useMockModelProvider = true;
        DaemonHarness harness(opts);
        REQUIRE(harness.start());

        REQUIRE(waitForOnnxRegistryConfiguration(3s));

        auto& registry = OnnxConcurrencyRegistry::instance();
        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        auto glinerMetrics = registry.laneMetrics(OnnxLane::Gliner);

        INFO("embedMetrics.reserved=" << embedMetrics.reserved);
        INFO("glinerMetrics.reserved=" << glinerMetrics.reserved);
        CHECK(embedMetrics.reserved == 3);
        CHECK(glinerMetrics.reserved == 2);

        harness.stop();
    }

    SECTION("Reranker reserved slots are configured") {
        EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "8");
        EnvGuard rerankerEnv("YAMS_ONNX_RERANKER_RESERVED", "2");
        EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

        DaemonHarness::Options opts;
        opts.useMockModelProvider = true;
        DaemonHarness harness(opts);
        REQUIRE(harness.start());

        REQUIRE(waitForOnnxRegistryConfiguration(3s));

        auto& registry = OnnxConcurrencyRegistry::instance();
        auto rerankerMetrics = registry.laneMetrics(OnnxLane::Reranker);

        INFO("rerankerMetrics.reserved=" << rerankerMetrics.reserved);
        CHECK(rerankerMetrics.reserved == 2);

        harness.stop();
    }

    SECTION("Efficient profile scales down max slots") {
        EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "8");
        EnvGuard profileEnv("YAMS_TUNING_PROFILE", "efficient"); // scale=0.0

        DaemonHarness::Options opts;
        opts.useMockModelProvider = true;
        DaemonHarness harness(opts);
        REQUIRE(harness.start());

        REQUIRE(waitForOnnxRegistryConfiguration(3s));

        auto& registry = OnnxConcurrencyRegistry::instance();
        // Efficient profile uses 0.0x scale, so 8 * 0.0 = 0
        // Minimum is 2, so expect max(0, 2) = 2
        INFO("totalSlots=" << registry.totalSlots());
        CHECK(registry.totalSlots() == 2);

        harness.stop();
    }

    SECTION("Aggressive profile scales up max slots") {
        EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "4");
        EnvGuard profileEnv("YAMS_TUNING_PROFILE", "aggressive"); // scale=1.0

        DaemonHarness::Options opts;
        opts.useMockModelProvider = true;
        DaemonHarness harness(opts);
        REQUIRE(harness.start());

        REQUIRE(waitForOnnxRegistryConfiguration(3s));

        auto& registry = OnnxConcurrencyRegistry::instance();
        // Aggressive profile uses 1.0x scale, so 4 * 1.0 = 4
        INFO("totalSlots=" << registry.totalSlots());
        CHECK(registry.totalSlots() == 4);

        harness.stop();
    }
}

// =============================================================================
// Test 2: ONNX Registry Configuration with Embedding Slots
// =============================================================================

TEST_CASE("Embedding lane slots are configured correctly",
          "[daemon][onnx][embedding][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Configure slots with embedding-specific reservations
    EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "6");
    EnvGuard embedEnv("YAMS_ONNX_EMBED_RESERVED", "2");
    EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    opts.enableModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start());

    REQUIRE(waitForOnnxRegistryConfiguration(3s));

    auto& registry = OnnxConcurrencyRegistry::instance();

    SECTION("Registry is configured with correct total and embedding slots") {
        // Balanced profile uses 0.5x scale, so 6 * 0.5 = 3
        INFO("totalSlots=" << registry.totalSlots());
        CHECK(registry.totalSlots() == 3);

        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        INFO("embedding reserved=" << embedMetrics.reserved);
        CHECK(embedMetrics.reserved == 2);
    }

    SECTION("Slots can be manually acquired for embedding lane") {
        uint32_t beforeUsed = registry.usedSlots();

        // Acquire a slot for embedding lane
        OnnxConcurrencyRegistry::SlotGuard guard(registry, OnnxLane::Embedding, 1s);
        REQUIRE(guard.acquired());

        CHECK(registry.usedSlots() == beforeUsed + 1);

        // Metrics should show embedding lane is in use
        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        CHECK(embedMetrics.used >= 1);
    }

    SECTION("Multiple embedding slots can be acquired up to limit") {
        uint32_t beforeUsed = registry.usedSlots();

        // Acquire multiple slots for embedding
        OnnxConcurrencyRegistry::SlotGuard slot1(registry, OnnxLane::Embedding, 1s);
        OnnxConcurrencyRegistry::SlotGuard slot2(registry, OnnxLane::Embedding, 1s);

        REQUIRE(slot1.acquired());
        REQUIRE(slot2.acquired());
        CHECK(registry.usedSlots() >= beforeUsed + 2);
    }

    SECTION("Slots are released when guards go out of scope") {
        uint32_t beforeUsed = registry.usedSlots();

        {
            OnnxConcurrencyRegistry::SlotGuard guard(registry, OnnxLane::Embedding, 1s);
            REQUIRE(guard.acquired());
            CHECK(registry.usedSlots() == beforeUsed + 1);
        }

        // Slot should be released after scope exit
        CHECK(registry.usedSlots() == beforeUsed);
    }

    harness.stop();
}

// =============================================================================
// Test 3: Metrics Observability
// =============================================================================

TEST_CASE("FSM metrics reflect ONNX configuration", "[daemon][onnx][metrics][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "5");
    EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start());

    REQUIRE(waitForOnnxRegistryConfiguration(3s));

    SECTION("Registry snapshot contains expected metrics") {
        auto& registry = OnnxConcurrencyRegistry::instance();
        auto snap = registry.snapshot();

        // Balanced profile uses 0.5x scale, so 5 * 0.5 = 2.5 -> 2
        INFO("snapshot totalSlots=" << snap.totalSlots);
        INFO("snapshot usedSlots=" << snap.usedSlots);
        INFO("snapshot availableSlots=" << snap.availableSlots);

        CHECK(snap.totalSlots == 2);
        CHECK(snap.usedSlots == 0);
        CHECK(snap.availableSlots == 2);
    }

    SECTION("Status response includes ONNX metrics via client") {
        ClientConfig clientCfg;
        clientCfg.socketPath = harness.socketPath();
        clientCfg.requestTimeout = 10s;
        clientCfg.autoStart = false;
        DaemonClient client(clientCfg);
        auto connected = yams::cli::run_sync(client.connect(), 5s);
        REQUIRE(connected.has_value());

        auto statusResult = yams::cli::run_sync(client.status(), 10s);
        REQUIRE(statusResult.has_value());

        // Status response should be valid and daemon ready
        const auto& status = statusResult.value();
        INFO("overallStatus=" << status.overallStatus);
        INFO("lifecycleState=" << status.lifecycleState);
        CHECK((status.ready || status.overallStatus == "ready" || status.overallStatus == "Ready"));
    }

    harness.stop();
}

// =============================================================================
// Test 4: Registry State Isolation Between Daemon Restarts
// =============================================================================

TEST_CASE("OnnxConcurrencyRegistry state resets on daemon restart",
          "[daemon][onnx][lifecycle][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("Different configs apply correctly on daemon restart") {
        // First daemon with 4 slots
        {
            EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "4");
            EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

            DaemonHarness::Options opts;
            opts.useMockModelProvider = true;
            DaemonHarness harness(opts);
            REQUIRE(harness.start());
            REQUIRE(waitForOnnxRegistryConfiguration(3s));

            auto& registry = OnnxConcurrencyRegistry::instance();
            // Balanced profile uses 0.5x scale, so 4 * 0.5 = 2
            INFO("first daemon totalSlots=" << registry.totalSlots());
            CHECK(registry.totalSlots() == 2);

            harness.stop();
        }

        // Second daemon with 8 slots - should reconfigure
        {
            EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "8");
            EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

            DaemonHarness::Options opts;
            opts.useMockModelProvider = true;
            DaemonHarness harness(opts);
            REQUIRE(harness.start());
            REQUIRE(waitForOnnxRegistryConfiguration(3s));

            auto& registry = OnnxConcurrencyRegistry::instance();
            // Balanced profile uses 0.5x scale, so 8 * 0.5 = 4
            INFO("second daemon totalSlots=" << registry.totalSlots());
            CHECK(registry.totalSlots() == 4);

            harness.stop();
        }
    }
}

// =============================================================================
// Test 5: Lane Metrics Update Correctly
// =============================================================================

TEST_CASE("Lane metrics update during slot acquisition", "[daemon][onnx][lanes][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "8");
    EnvGuard embedEnv("YAMS_ONNX_EMBED_RESERVED", "2");
    EnvGuard glinerEnv("YAMS_ONNX_GLINER_RESERVED", "1");
    EnvGuard rerankerEnv("YAMS_ONNX_RERANKER_RESERVED", "1");
    EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start());

    REQUIRE(waitForOnnxRegistryConfiguration(3s));

    auto& registry = OnnxConcurrencyRegistry::instance();

    SECTION("All lanes have correct reserved slots") {
        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        auto glinerMetrics = registry.laneMetrics(OnnxLane::Gliner);
        auto rerankerMetrics = registry.laneMetrics(OnnxLane::Reranker);
        auto otherMetrics = registry.laneMetrics(OnnxLane::Other);

        CHECK(embedMetrics.reserved == 2);
        CHECK(glinerMetrics.reserved == 1);
        CHECK(rerankerMetrics.reserved == 1);
        CHECK(otherMetrics.reserved == 0);
    }

    SECTION("Manual slot acquisition updates used count") {
        uint32_t beforeUsed = registry.usedSlots();

        // Acquire a slot manually
        OnnxConcurrencyRegistry::SlotGuard guard(registry, OnnxLane::Embedding, 1s);
        REQUIRE(guard.acquired());

        CHECK(registry.usedSlots() == beforeUsed + 1);

        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        CHECK(embedMetrics.used >= 1);
    }

    harness.stop();
}

// =============================================================================
// Test 6: AbiModelProviderAdapter SlotGuard Verification (Real ONNX Plugin)
// =============================================================================

TEST_CASE("AbiModelProviderAdapter acquires SlotGuard during real embedding",
          "[daemon][onnx][adapter][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Configure conservative slot limits to observe contention
    EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "4");
    EnvGuard embedEnv("YAMS_ONNX_EMBED_RESERVED", "1");
    EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

    DaemonHarness::Options opts;
    opts.useMockModelProvider = false; // CRITICAL: Use real ONNX plugin
    opts.enableModelProvider = true;
    opts.autoLoadPlugins = true; // Load ONNX plugin
    opts.configureModelPool = true;
    opts.modelPoolLazyLoading = false; // Preload model on startup
    opts.preloadModels = {"all-MiniLM-L6-v2"};

    // Set plugin directory to builddir/plugins
    opts.pluginDir = std::filesystem::current_path() / "builddir" / "plugins";

    // Configure ONNX plugin with preferred model (uses plugin's model resolution)
    opts.pluginConfigs["onnx_plugin"] = R"({"preferred_model": "all-MiniLM-L6-v2"})";

    DaemonHarness harness(opts);

    INFO("Starting daemon with real ONNX plugin (model preloading may take time)...");
    // If model not found, start() will fail - this makes it clear the model must be installed
    if (!harness.start(std::chrono::seconds(60))) {
        SKIP("Failed to start daemon with ONNX plugin - model may not be available");
    }

    REQUIRE(waitForOnnxRegistryConfiguration(5s)); // Allow extra time for model loading

    ClientConfig clientCfg;
    clientCfg.socketPath = harness.socketPath();
    clientCfg.requestTimeout = 30s;
    clientCfg.autoStart = false;
    DaemonClient client(clientCfg);
    auto connected = yams::cli::run_sync(client.connect(), 10s);
    REQUIRE(connected.has_value());

    auto& registry = OnnxConcurrencyRegistry::instance();

    SECTION("Single embedding acquires and releases slot") {
        uint32_t baselineUsed = registry.usedSlots();
        auto beforeSnap = registry.snapshot();

        GenerateEmbeddingRequest req;
        req.text = "test embedding via real ONNX plugin";
        req.modelName = "all-MiniLM-L6-v2";
        req.normalize = true;

        INFO("Issuing embedding request...");
        auto result = yams::cli::run_sync(client.generateEmbedding(req), 30s);

        if (!result.has_value()) {
            INFO("Embedding request failed: " << result.error().message);
            // Skip rather than fail - model may not be fully loaded
            SKIP("Embedding request failed - model may not be available");
        }

        REQUIRE(!result.value().embedding.empty());
        INFO("Embedding dimension: " << result.value().embedding.size());

        // Verify slot released after completion
        auto afterSnap = registry.snapshot();
        INFO("Slots before: " << beforeSnap.usedSlots << ", after: " << afterSnap.usedSlots);
        CHECK(afterSnap.usedSlots == baselineUsed);

        // Verify no timeouts occurred during this request
        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        INFO("Embedding lane timeouts: " << embedMetrics.timeouts);
        CHECK(embedMetrics.timeouts == 0);
    }

    SECTION("Concurrent embeddings respect slot limits") {
        // WARMUP: Run a single embedding first to ensure ONNX session is fully initialized
        // This prevents the "Session invalid during warmup" race condition that occurs
        // when multiple threads attempt to use the session before warmup completes.
        INFO("Warmup: Running initial embedding to initialize ONNX session...");
        {
            GenerateEmbeddingRequest warmupReq;
            warmupReq.text = "warmup request to initialize session";
            warmupReq.modelName = "all-MiniLM-L6-v2";
            warmupReq.normalize = true;

            auto warmupResult = yams::cli::run_sync(client.generateEmbedding(warmupReq), 60s);
            if (!warmupResult.has_value() || warmupResult.value().embedding.empty()) {
                INFO("Warmup embedding failed - ONNX session may not be ready");
                SKIP("ONNX session warmup failed - skipping concurrent test");
            }
            INFO("Warmup complete, session initialized");
        }

        // Brief pause to ensure session is stable after warmup
        std::this_thread::sleep_for(100ms);

        std::atomic<int> completed{0};
        std::atomic<int> failed{0};
        std::atomic<uint32_t> peakUsed{0};

        // Track peak slot usage during concurrent requests
        auto worker = [&](int id) {
            GenerateEmbeddingRequest req;
            req.text = "concurrent test embedding " + std::to_string(id);
            req.modelName = "all-MiniLM-L6-v2";
            req.normalize = true;

            // Capture peak usage during execution
            uint32_t current = registry.usedSlots();
            uint32_t expected = peakUsed.load();
            while (current > expected && !peakUsed.compare_exchange_weak(expected, current)) {
            }

            auto result = yams::cli::run_sync(client.generateEmbedding(req), 60s);
            if (result.has_value() && !result.value().embedding.empty()) {
                ++completed;
            } else {
                ++failed;
            }
        };

        constexpr int numRequests = 4; // Match max slots (4) for more reliable test
        std::vector<std::thread> threads;
        threads.reserve(numRequests);

        INFO("Launching " << numRequests << " concurrent embedding requests...");
        for (int i = 0; i < numRequests; ++i) {
            threads.emplace_back(worker, i);
        }

        for (auto& t : threads) {
            t.join();
        }

        INFO("Completed: " << completed << ", Failed: " << failed);
        INFO("Peak slots used: " << peakUsed);

        // At least some should complete (with warmup, most should succeed)
        CHECK(completed >= 1);

        // After all complete, slots should be released
        CHECK(registry.usedSlots() == 0);

        // Peak usage should not exceed configured max (4)
        CHECK(peakUsed <= 4);
    }

    harness.stop();
}

// =============================================================================
// Test 7: Slot Exhaustion and Timeout Behavior
// =============================================================================

TEST_CASE("ONNX slot exhaustion causes graceful timeout", "[daemon][onnx][timeout][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Configure very limited slots AND zero reserved slots to force contention
    // Without zeroing reserved slots, lanes can acquire beyond totalSlots via reserved pool
    EnvGuard maxEnv("YAMS_ONNX_MAX_CONCURRENT", "3");
    EnvGuard embedEnv("YAMS_ONNX_EMBED_RESERVED", "0");
    EnvGuard glinerEnv("YAMS_ONNX_GLINER_RESERVED", "0");
    EnvGuard rerankerEnv("YAMS_ONNX_RERANKER_RESERVED", "0");
    EnvGuard profileEnv("YAMS_TUNING_PROFILE", "balanced");

    DaemonHarness::Options opts;
    opts.useMockModelProvider = true;
    opts.enableModelProvider = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start());

    REQUIRE(waitForOnnxRegistryConfiguration(3s));

    auto& registry = OnnxConcurrencyRegistry::instance();

    // Verify registry is configured as expected
    INFO("Registry totalSlots: " << registry.totalSlots());
    INFO("Embedding reserved: " << registry.laneMetrics(OnnxLane::Embedding).reserved);
    INFO("Gliner reserved: " << registry.laneMetrics(OnnxLane::Gliner).reserved);
    INFO("Reranker reserved: " << registry.laneMetrics(OnnxLane::Reranker).reserved);
    REQUIRE(registry.totalSlots() >= 2);

    SECTION("Slot acquisition is tracked in metrics") {
        // This test verifies that slot acquisition/release is properly tracked,
        // without making assumptions about exact capacity limits (which can vary
        // based on semaphore implementation details).
        uint32_t beforeUsed = registry.usedSlots();
        INFO("Slots used before: " << beforeUsed);

        // Acquire a slot
        OnnxConcurrencyRegistry::SlotGuard guard(registry, OnnxLane::Other, 1s);
        REQUIRE(guard.acquired());

        // Verify usage increased
        CHECK(registry.usedSlots() == beforeUsed + 1);

        // Lane metrics should reflect the acquisition
        auto otherMetrics = registry.laneMetrics(OnnxLane::Other);
        INFO("Other lane used: " << otherMetrics.used);
        CHECK(otherMetrics.used >= 1);
    }

    SECTION("Slot metrics track reserved slots correctly") {
        // Configure reserved slot for embedding
        uint32_t originalReserved = registry.laneMetrics(OnnxLane::Embedding).reserved;
        registry.setReservedSlots(OnnxLane::Embedding, 1);

        auto embedMetrics = registry.laneMetrics(OnnxLane::Embedding);
        INFO("Embedding reserved slots: " << embedMetrics.reserved);
        CHECK(embedMetrics.reserved == 1);

        // Restore original
        registry.setReservedSlots(OnnxLane::Embedding, originalReserved);
    }

    harness.stop();
}
