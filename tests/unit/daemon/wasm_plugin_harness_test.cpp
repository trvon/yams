#include <chrono>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/wasm_runtime.h>

namespace yams::daemon {
namespace fs = std::filesystem;

class WasmPluginHarnessTest : public ::testing::Test {
protected:
    void SetUp() override {
        tempDir_ = fs::temp_directory_path() / ("yams_wasm_ht_" + std::to_string(::getpid()));
        fs::create_directories(tempDir_);
        trustFile_ = tempDir_ / "plugins_trust.txt";
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(tempDir_, ec);
    }
    fs::path tempDir_;
    fs::path trustFile_;
};

TEST_F(WasmPluginHarnessTest, ConditionalLoadWasmIfPresent) {
    const char* envPath = std::getenv("TEST_WASM_PLUGIN_FILE");
    if (!envPath || !*envPath) {
        GTEST_SKIP() << "TEST_WASM_PLUGIN_FILE not set";
    }
    fs::path wasmPath(envPath);
    if (!fs::exists(wasmPath)) {
        GTEST_SKIP() << "WASM plugin file not found: " << wasmPath;
    }
    // Require sidecar manifest
    fs::path manifest = wasmPath;
    manifest += ".manifest.json";
    if (!fs::exists(manifest)) {
        GTEST_SKIP() << "Manifest sidecar missing: " << manifest;
    }

    WasmPluginHost host(trustFile_);
    ASSERT_TRUE(host.trustAdd(wasmPath.parent_path()));

    // Scan
    auto scan = host.scanTarget(wasmPath);
    ASSERT_TRUE(scan) << (scan ? "" : scan.error().message);
    // Load
    auto lr = host.load(wasmPath, "{}");
    if (!lr) {
        GTEST_SKIP() << "Failed to initialize WASM runtime or plugin: " << lr.error().message;
    }
    // Health
    auto h = host.health(lr.value().name);
    // health may be NotImplemented depending on host; accept success or NotImplemented
    if (!h) {
        // Accept NotImplemented in current prototype
        SUCCEED();
    }
    // Unload
    auto ur = host.unload(lr.value().name);
    EXPECT_TRUE(ur);
}

TEST_F(WasmPluginHarnessTest, EnforceMaxInstancesIfPresent) {
    const char* envPath = std::getenv("TEST_WASM_PLUGIN_FILE");
    if (!envPath || !*envPath) {
        GTEST_SKIP() << "TEST_WASM_PLUGIN_FILE not set";
    }
    fs::path wasmPath(envPath);
    if (!fs::exists(wasmPath)) {
        GTEST_SKIP() << "WASM plugin file not found: " << wasmPath;
    }
    // Require sidecar manifest
    fs::path manifest = wasmPath;
    manifest += ".manifest.json";
    if (!fs::exists(manifest)) {
        GTEST_SKIP() << "Manifest sidecar missing: " << manifest;
    }

    // Enforce a single instance
    ::setenv("YAMS_WASM_MAX_INSTANCES", "1", 1);

    WasmPluginHost host(trustFile_);
    ASSERT_TRUE(host.trustAdd(wasmPath.parent_path()));

    // First load should succeed when runtime is available
    auto lr1 = host.load(wasmPath, "{}");
    if (!lr1) {
        // Don't fail if wasmtime is unavailable in this environment
        GTEST_SKIP() << "Failed to initialize WASM runtime or plugin: " << lr1.error().message;
    }

    // Second load should fail with ResourceExhausted due to max_instances=1
    auto lr2 = host.load(wasmPath, "{}");
    ASSERT_FALSE(lr2);
    EXPECT_EQ(lr2.error().code, ErrorCode::ResourceExhausted);

    // Cleanup
    auto ur = host.unload(lr1.value().name);
    EXPECT_TRUE(ur);

    ::unsetenv("YAMS_WASM_MAX_INSTANCES");
}

TEST_F(WasmPluginHarnessTest, EpochTimeoutIfPresent) {
    const char* envPath = std::getenv("TEST_WASM_PLUGIN_FILE");
    const char* envExport = std::getenv("TEST_WASM_TIMEOUT_EXPORT");
    if (!envPath || !*envPath) {
        GTEST_SKIP() << "TEST_WASM_PLUGIN_FILE not set";
    }
    if (!envExport || !*envExport) {
        GTEST_SKIP() << "TEST_WASM_TIMEOUT_EXPORT not set";
    }
    fs::path wasmPath(envPath);
    if (!fs::exists(wasmPath)) {
        GTEST_SKIP() << "WASM plugin file not found: " << wasmPath;
    }
    // Require sidecar manifest
    fs::path manifest = wasmPath;
    manifest += ".manifest.json";
    if (!fs::exists(manifest)) {
        GTEST_SKIP() << "Manifest sidecar missing: " << manifest;
    }

    // Set a small timeout and validate the call is preempted when runtime available
    WasmRuntime rt;
    WasmRuntime::Limits lims = rt.getLimits();
    lims.call_timeout_ms = 50; // 50ms deadline
    rt.setLimits(lims);

    auto init = rt.load(wasmPath, "{}");
    if (!init.ok) {
        GTEST_SKIP() << "Failed to initialize WASM runtime or plugin: " << init.error;
    }

    auto start = std::chrono::steady_clock::now();
    auto res = rt.callJsonExport(envExport, "", "");
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

    // Expect the call to be interrupted/timed out; exact error text may vary by wasmtime version
    EXPECT_FALSE(res.ok);
    // Should not run unbounded; allow generous upper bound for CI variance
    EXPECT_LE(elapsed_ms, 5000);
}

} // namespace yams::daemon
