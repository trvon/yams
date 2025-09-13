#include <filesystem>
#include <gtest/gtest.h>
#include <yams/daemon/daemon.h>

// This test requires a built ONNX plugin and will dlopen it. Guarded by env
// YAMS_RUN_DLOPEN_TEST=1 to skip on CI/macOS.

TEST(DaemonPlugins, AutoloadDlopenLoadsPlugin) {
    if (const char* run = std::getenv("YAMS_RUN_DLOPEN_TEST"); !run || std::string(run) != "1") {
        GTEST_SKIP() << "Skipping dlopen test (set YAMS_RUN_DLOPEN_TEST=1 to run)";
    }
#ifndef yams_onnx_plugin_BUILT
    GTEST_SKIP() << "ONNX plugin build path not available";
#else
    namespace fs = std::filesystem;
    fs::path pluginPath = yams_onnx_plugin_BUILT;
    if (!fs::exists(pluginPath)) {
        GTEST_SKIP() << "Built plugin path missing: " << pluginPath.string();
    }
    fs::path pluginDir = pluginPath.parent_path();
    // Configure daemon to trust and scan the plugin dir
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = fs::temp_directory_path() / "yams_dlopen_it";
    fs::create_directories(cfg.dataDir);
    cfg.socketPath = cfg.dataDir / "daemon.sock";
    cfg.pidFile = cfg.dataDir / "daemon.pid";
    cfg.logFile = cfg.dataDir / "daemon.log";
    cfg.enableModelProvider = true;
    cfg.autoLoadPlugins = false; // we'll call autoload explicitly
    cfg.pluginDir = pluginDir;
    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());
    auto loaded = d.autoloadPluginsNow();
    ASSERT_TRUE(loaded);
    EXPECT_GT(loaded.value(), 0u) << "Expected at least one plugin loaded from " << pluginDir;
    (void)d.stop();
#endif
}
