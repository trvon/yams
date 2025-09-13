#include <filesystem>
#include <fstream>
#include <string>
#include <gtest/gtest.h>
#include <yams/daemon/daemon.h>

TEST(DaemonPlugins, AdoptionLogsIncludePath) {
    if (const char* run = std::getenv("YAMS_RUN_DLOPEN_TEST"); !run || std::string(run) != "1") {
        GTEST_SKIP() << "Skipping dlopen test (set YAMS_RUN_DLOPEN_TEST=1 to run)";
    }
#ifndef yams_onnx_plugin_BUILT
    GTEST_SKIP() << "ONNX plugin build path not available";
#else
    namespace fs = std::filesystem;
    fs::path pluginPath = yams_onnx_plugin_BUILT;
    ASSERT_TRUE(fs::exists(pluginPath)) << pluginPath.string();
    fs::path pluginDir = pluginPath.parent_path();

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = fs::temp_directory_path() / "yams_dlopen_adopt_log";
    fs::create_directories(cfg.dataDir);
    cfg.socketPath = cfg.dataDir / "daemon.sock";
    cfg.pidFile = cfg.dataDir / "daemon.pid";
    cfg.logFile = cfg.dataDir / "daemon.log";
    cfg.enableModelProvider = true;
    cfg.autoLoadPlugins = false; // call autoload explicitly
    cfg.pluginDir = pluginDir;

    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());
    auto loaded = d.autoloadPluginsNow();
    ASSERT_TRUE(loaded);
    EXPECT_GT(loaded.value(), 0u);

    // Read logs and verify adoption line contains plugin path
    std::ifstream in(cfg.logFile);
    ASSERT_TRUE(in.good());
    std::string line;
    bool found = false;
    while (std::getline(in, line)) {
        if (line.find("Adopted model provider from plugin:") != std::string::npos &&
            line.find(pluginPath.string()) != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Expected adoption log to include plugin path: " << pluginPath;
    (void)d.stop();
#endif
}
