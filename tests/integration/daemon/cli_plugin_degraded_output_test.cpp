#include <chrono>
#include <string>
#include <thread>
#include <gtest/gtest.h>

#include <yams/daemon/components/ServiceManager.h> // For test helper methods on ServiceManager
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;

// Minimal local implementation of run_cmd (duplicated from external_plugin_integration_test.cpp)
// to avoid undefined symbol at link time. If a shared test utility is later introduced,
// this can be refactored out.
namespace {
std::string run_cmd(const std::string& cmd) {
    std::string out;
    FILE* fp = popen(cmd.c_str(), "r");
    if (!fp)
        return out;
    char buf[4096];
    while (fgets(buf, sizeof(buf), fp)) {
        out.append(buf);
    }
    pclose(fp);
    return out;
}
} // namespace

// CLI-level test to verify degraded/provider tags and error text are printed by
// `yams plugin list` when the daemon marks the provider as degraded.
//
// This test is resilient to environments without a loaded ONNX plugin. It injects
// degraded state for a commonly used provider name ("onnx") and then:
// - If the CLI output lists a plugin named "onnx", it asserts the tags and error are shown.
// - Otherwise, it skips with an explanatory message.
TEST(CliPluginListDegraded, ShowsDegradedTagsWhenPresent) {
    // Start the daemon in-process.
    yams::daemon::DaemonConfig cfg;
    cfg.workerThreads = 2;
    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());

    // Give the daemon a moment to initialize.
    std::this_thread::sleep_for(150ms);

#ifdef YAMS_TESTING
    // Inject degraded state for the adopted provider plugin.
    // We choose "onnx" because it's the typical provider name in deployments.
    if (auto* sm = d.getServiceManager()) {
        sm->__test_setAdoptedProviderPluginName("onnx");
        sm->__test_setModelProviderDegraded(true, "unit-test simulated degraded provider");
    }
#endif

    // Allow stats/plugins_json to refresh.
    std::this_thread::sleep_for(100ms);

    // Invoke the CLI.
    std::string out;
    try {
        out = run_cmd("yams plugin list");
    } catch (...) {
        // If the CLI cannot be executed, we skip rather than fail hard in integration.
        GTEST_SKIP() << "Unable to execute 'yams' CLI in this environment.";
    }

    // If the plugin name appears, verify the degraded/provider tags and error text.
    if (out.find("onnx") != std::string::npos) {
        // Expect tags and error to be visible in the CLI output.
        EXPECT_NE(out.find("[provider]"), std::string::npos) << "Expected [provider] tag";
        EXPECT_NE(out.find("[degraded]"), std::string::npos) << "Expected [degraded] tag";
        // Error message should be present in quotes, but accept any occurrence of 'error='.
        EXPECT_NE(out.find("error="), std::string::npos) << "Expected error=... field";
    } else {
        GTEST_SKIP() << "No 'onnx' plugin entry found in CLI output; skipping tag assertions.\n"
                        "Output was:\n"
                     << out;
    }

    ASSERT_TRUE(d.stop());
}