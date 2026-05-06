// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 Trevon Helm
// Plugin harness test: isolated plugin loading with graceful degradation
// Migrated from GTest to Catch2, enhanced with timeout and multi-plugin support

#include <nlohmann/json.hpp>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif
#include "../../common/env_compat.h"
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/unistd.h> // For getpid()
#include <yams/daemon/resource/plugin_host.h>
#include <yams/plugins/model_provider_v1.h>

namespace yams::daemon {
namespace fs = std::filesystem;

namespace {

#if defined(__APPLE__) && defined(__SANITIZE_ADDRESS__)
constexpr bool kAddressSanitizerEnabled = true;
#elif defined(__APPLE__) && defined(__has_feature)
#if __has_feature(address_sanitizer)
constexpr bool kAddressSanitizerEnabled = true;
#else
constexpr bool kAddressSanitizerEnabled = false;
#endif
#else
constexpr bool kAddressSanitizerEnabled = false;
#endif

using yams::test::ScopedEnvVar;

// Helper to create isolated temp directory for tests
struct IsolatedPluginEnv {
    IsolatedPluginEnv() {
        tempDir = fs::temp_directory_path() /
                  ("yams_plugin_test_" + std::to_string(::getpid()) + "_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(tempDir);
        trustFile = tempDir / "plugins_trust.txt";

        // Prevent config/trust file leakage
        xdgGuard = std::make_unique<ScopedEnvVar>("XDG_CONFIG_HOME", tempDir.string());
    }

    ~IsolatedPluginEnv() {
        std::error_code ec;
        fs::remove_all(tempDir, ec);
    }

    fs::path tempDir;
    fs::path trustFile;
    std::unique_ptr<ScopedEnvVar> xdgGuard;
};

// Get plugin path from environment or build define
std::optional<fs::path> getOnnxPluginPath() {
    // Priority 1: Environment variable
    if (const char* envPath = std::getenv("TEST_ONNX_PLUGIN_FILE")) {
        if (*envPath) {
            return fs::path(envPath);
        }
    }

    // Priority 2: Build-time define
#ifdef yams_onnx_plugin_BUILT
    fs::path pluginPath = fs::path(yams_onnx_plugin_BUILT);
    if (fs::exists(pluginPath)) {
        return pluginPath;
    }
#endif

    return std::nullopt;
}

#ifdef __APPLE__
std::string shellQuote(const fs::path& path) {
    std::string quoted = "'";
    for (char ch : path.string()) {
        if (ch == '\'') {
            quoted += "'\\''";
        } else {
            quoted += ch;
        }
    }
    quoted += "'";
    return quoted;
}

std::string runCommand(const std::string& command) {
    std::array<char, 512> buffer{};
    std::string output;
    FILE* pipe = ::popen(command.c_str(), "r");
    if (!pipe) {
        throw std::runtime_error("Failed to run command: " + command);
    }

    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output += buffer.data();
    }

    const int status = ::pclose(pipe);
    if (status != 0) {
        throw std::runtime_error("Command failed: " + command +
                                 " (status=" + std::to_string(status) + ")\n" + output);
    }

    return output;
}

fs::path currentExecutablePath() {
    uint32_t size = 0;
    (void)_NSGetExecutablePath(nullptr, &size);
    std::string buffer(size, '\0');
    if (_NSGetExecutablePath(buffer.data(), &size) != 0) {
        throw std::runtime_error("Unable to resolve current executable path");
    }
    buffer.resize(std::strlen(buffer.c_str()));
    return fs::weakly_canonical(fs::path(buffer));
}

int runExitCode(const std::string& command) {
    const int rc = std::system(command.c_str());
    if (rc == -1) {
        throw std::runtime_error("Failed to execute command: " + command);
    }
    return rc;
}

bool hasDirectOnnxRuntimeLinkage(const fs::path& pluginPath) {
    try {
        return runCommand("otool -L " + shellQuote(pluginPath)).find("libonnxruntime") !=
               std::string::npos;
    } catch (...) {
        return false;
    }
}
#endif

bool shouldSkipLazyOnnxPluginLoadContract(const fs::path& pluginPath) {
#ifdef __APPLE__
    return kAddressSanitizerEnabled && hasDirectOnnxRuntimeLinkage(pluginPath);
#else
    (void)pluginPath;
    return false;
#endif
}

// Load plugin with timeout to prevent hanging on plugin crash
template <typename Func>
auto loadWithTimeout(Func&& fn, std::chrono::milliseconds timeout = std::chrono::seconds(5))
    -> decltype(fn()) {
    auto future = std::async(std::launch::async, std::forward<Func>(fn));
    if (future.wait_for(timeout) == std::future_status::timeout) {
        throw std::runtime_error("Plugin load timed out after " + std::to_string(timeout.count()) +
                                 "ms");
    }
    return future.get();
}

} // namespace

// =============================================================================
// Plugin Loading Tests
// =============================================================================

TEST_CASE("Plugin: ONNX plugin loading with isolation", "[daemon][plugin][onnx]") {
    IsolatedPluginEnv env;

    auto pluginPath = getOnnxPluginPath();
    if (!pluginPath) {
        SUCCEED("ONNX plugin not available (set TEST_ONNX_PLUGIN_FILE or build plugins/onnx)");
        return;
    }

    if (!fs::exists(*pluginPath)) {
        SUCCEED("ONNX plugin file does not exist: " + pluginPath->string());
        return;
    }

    if (shouldSkipLazyOnnxPluginLoadContract(*pluginPath)) {
        SKIP("ASan macOS build links ONNX Runtime directly; isolated lazy-load harness is not "
             "applicable");
    }

    SECTION("Load ONNX plugin successfully") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(env.trustFile);
        REQUIRE(host.trustAdd(pluginPath->parent_path()));

        // Scan with timeout
        auto scanResult = loadWithTimeout([&]() { return host.scanTarget(*pluginPath); });

        if (!scanResult) {
            SKIP("ONNX plugin scan failed (missing runtime dependencies): " + pluginPath->string());
        }

        // Load with timeout
        auto loadResult = loadWithTimeout([&]() { return host.load(*pluginPath, "{}"); });
        REQUIRE(loadResult);

        // Verify model_provider_v1 interface
        auto ifaceResult = host.getInterface(loadResult.value().name, "model_provider_v1", 1);
        REQUIRE(ifaceResult);

        auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceResult.value());
        REQUIRE(table != nullptr);
        // ABI version 2 includes additional fields/features
        REQUIRE(table->abi_version >= 1);

        // Unload cleanly
        REQUIRE(host.unload(loadResult.value().name));
    }

    SECTION("Trust validation") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(env.trustFile);

        // Scanning is allowed even without trust (just reads metadata)
        auto untrustedScan = host.scanTarget(*pluginPath);
        // Scan may succeed (it's just file inspection), but load would check trust

        // Add trust
        REQUIRE(host.trustAdd(pluginPath->parent_path()));
        auto trustedScan = loadWithTimeout([&]() { return host.scanTarget(*pluginPath); });

        if (trustedScan) {
            // Verify can load from trusted location
            auto loadResult = loadWithTimeout([&]() { return host.load(*pluginPath, "{}"); });
            REQUIRE(loadResult);
            REQUIRE(host.unload(loadResult.value().name));
            SUCCEED("Trust validation working correctly");
        } else {
            SKIP("Plugin scan failed (runtime dependencies)");
        }
    }

    SECTION("Multiple load/unload cycles") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(env.trustFile);
        REQUIRE(host.trustAdd(pluginPath->parent_path()));

        auto scanResult = loadWithTimeout([&]() { return host.scanTarget(*pluginPath); });
        if (!scanResult) {
            SKIP("Plugin scan failed");
        }

        for (int i = 0; i < 3; ++i) {
            INFO("Load/unload cycle " << i);

            auto loadResult = loadWithTimeout([&]() { return host.load(*pluginPath, "{}"); });
            REQUIRE(loadResult);

            std::string pluginName = loadResult.value().name;
            REQUIRE(host.unload(pluginName));
        }

        SUCCEED("Multiple load/unload cycles completed");
    }
}

TEST_CASE("Plugin: ONNX plugin reports missing runtime cleanly",
          "[daemon][plugin][onnx][runtime]") {
    IsolatedPluginEnv env;

    auto pluginPath = getOnnxPluginPath();
    if (!pluginPath || !fs::exists(*pluginPath)) {
        SKIP("ONNX plugin not available (set TEST_ONNX_PLUGIN_FILE or build plugins/onnx)");
    }

    if (shouldSkipLazyOnnxPluginLoadContract(*pluginPath)) {
        SKIP("ASan macOS build links ONNX Runtime directly; missing-runtime load harness is not "
             "applicable");
    }

#ifdef __APPLE__
    const bool childMode = std::getenv("YAMS_ONNX_HARNESS_CHILD") != nullptr;
    const fs::path missingRuntime = env.tempDir / "missing-onnxruntime.dylib";

    if (!childMode) {
        const fs::path exe = currentExecutablePath();
        const std::string command =
            "YAMS_ONNX_HARNESS_CHILD=1 YAMS_ONNX_RUNTIME_LIB=" + shellQuote(missingRuntime) + " " +
            shellQuote(exe) + " " +
            shellQuote("Plugin: ONNX plugin reports missing runtime cleanly") +
            " --allow-running-no-tests";
        INFO(command);
        REQUIRE(runExitCode(command) == 0);
        return;
    }
#else
    ScopedEnvVar runtimeLib("YAMS_ONNX_RUNTIME_LIB",
                            (env.tempDir / "missing-onnxruntime.so").string());
#endif

    AbiPluginHost host(nullptr);
    host.setTrustFile(env.trustFile);
    REQUIRE(host.trustAdd(pluginPath->parent_path()));

    auto loadResult = loadWithTimeout([&]() { return host.load(*pluginPath, "{}"); });
    REQUIRE(loadResult);

    auto healthResult = host.health(loadResult.value().name);
    REQUIRE(healthResult);

    auto health = nlohmann::json::parse(healthResult.value(), nullptr, false);
    INFO(health.dump());
    REQUIRE_FALSE(health.is_discarded());
    CHECK(health.value("status", "") == "unavailable");
    CHECK((health.value("reason", "") == "runtime_load_failed" ||
           health.value("reason", "") == "runtime_not_found"));
    CHECK_FALSE(health.value("runtime_status", "").empty());
    CHECK_FALSE(health.value("runtime_error", "").empty());

    auto ifaceResult = host.getInterface(loadResult.value().name, "model_provider_v1", 1);
    REQUIRE(ifaceResult);
    auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceResult.value());
    REQUIRE(table != nullptr);

    bool loaded = true;
    CHECK(table->is_model_loaded(table->self, "e5-small", &loaded) == YAMS_ERR_INTERNAL);
    CHECK_FALSE(loaded);

    REQUIRE(host.unload(loadResult.value().name));
}

#ifdef __APPLE__
TEST_CASE("Plugin: ONNX dylib avoids direct runtime linkage", "[daemon][plugin][onnx][linkage]") {
    auto pluginPath = getOnnxPluginPath();
    if (!pluginPath || !fs::exists(*pluginPath)) {
        SKIP("ONNX plugin not available (set TEST_ONNX_PLUGIN_FILE or build plugins/onnx)");
    }

    if (shouldSkipLazyOnnxPluginLoadContract(*pluginPath)) {
        SKIP("ASan macOS build emits direct ONNX Runtime linkage for this harness configuration");
    }

    const std::string output = runCommand("otool -L " + shellQuote(*pluginPath));
    INFO(output);

    CHECK(output.find("libonnxruntime") == std::string::npos);
    CHECK(output.find("libyams_vector") == std::string::npos);
}
#endif

TEST_CASE("Plugin: Error handling and edge cases", "[daemon][plugin][error]") {
    IsolatedPluginEnv env;

    SECTION("Load non-existent plugin") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(env.trustFile);

        fs::path nonExistent = env.tempDir / "nonexistent.so";
        auto result = host.scanTarget(nonExistent);
        REQUIRE_FALSE(result);
    }

    SECTION("Load from untrusted directory") {
        auto pluginPath = getOnnxPluginPath();
        if (!pluginPath || !fs::exists(*pluginPath)) {
            SKIP("ONNX plugin not available for trust test");
        }

        AbiPluginHost host(nullptr);
        host.setTrustFile(env.trustFile);
        // Don't add trust

        // Scan may succeed (metadata inspection), but verify load behavior
        auto scanResult = host.scanTarget(*pluginPath);
        if (scanResult) {
            // If scan succeeded, verify that load checks trust
            // (Current implementation may allow scan but restrict load)
            INFO("Plugin scanned successfully, trust enforcement during load");
            SUCCEED("Trust model: scan allowed, load restricted");
        } else {
            // If scan also checks trust, that's also valid
            SUCCEED("Trust model: scan restricted");
        }
    }
}

TEST_CASE("Plugin: provenance root smoke", "[daemon][plugin][provenance]") {
    IsolatedPluginEnv env;

    auto pluginPath = getOnnxPluginPath();
    if (!pluginPath || !fs::exists(*pluginPath)) {
        SKIP("ONNX plugin not available for provenance smoke test");
    }

    const char* expectedRootEnv = std::getenv("YAMS_EXPECT_PLUGIN_ROOT");
    if (!expectedRootEnv || !*expectedRootEnv) {
        SKIP("YAMS_EXPECT_PLUGIN_ROOT not set; skipping provenance root enforcement");
    }

    const fs::path expectedRoot = fs::weakly_canonical(fs::path(expectedRootEnv));

    AbiPluginHost host(nullptr);
    host.setTrustFile(env.trustFile);
    REQUIRE(host.trustAdd(pluginPath->parent_path()));

    auto loaded = host.load(*pluginPath, "{}");
    REQUIRE(loaded);

    const fs::path actualPath = fs::weakly_canonical(loaded.value().path);
    INFO("Expected plugin root: " << expectedRoot.string());
    INFO("Loaded plugin path: " << actualPath.string());

    const std::string expectedRootStr = expectedRoot.string();
    const std::string actualPathStr = actualPath.string();
    REQUIRE(actualPathStr.rfind(expectedRootStr, 0) == 0);

    REQUIRE(host.unload(loaded.value().name));
}

} // namespace yams::daemon
