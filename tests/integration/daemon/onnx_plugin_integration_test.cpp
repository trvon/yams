// Integration: load ONNX plugin via ABI host and run an embedding end-to-end
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static std::optional<fs::path> get_plugin_path() {
    if (const char* env = std::getenv("TEST_ONNX_PLUGIN_FILE")) {
        fs::path p(env);
        if (fs::exists(p))
            return p;
    }
#ifdef yams_onnx_plugin_BUILT
    {
        fs::path p = fs::path(yams_onnx_plugin_BUILT);
        if (fs::exists(p))
            return p;
    }
#endif
    return std::nullopt;
}

static bool find_model_root(std::string& out_root) {
    const char* home = std::getenv("HOME");
    if (!home)
        return false;
    fs::path root = fs::path(home) / ".yams" / "models";
    fs::path model = root / "all-MiniLM-L6-v2" / "model.onnx";
    if (fs::exists(model)) {
        out_root = root.string();
        return true;
    }
    return false;
}

TEST(OnnxPluginIT, LoadPluginAndGenerateEmbedding) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    auto pluginPath = get_plugin_path();
    if (!pluginPath) {
        GTEST_SKIP() << "ONNX plugin not built in this configuration";
    }
    std::string models_root;
    if (!find_model_root(models_root)) {
        GTEST_SKIP() << "Model not found under ~/.yams/models/all-MiniLM-L6-v2/model.onnx";
    }

    // Temp layout
    fs::path tmp = fs::temp_directory_path() / ("yams_onx_it_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    // Prepare config.toml for plugin-side resolver
    fs::path xdg = tmp / "cfg";
    fs::create_directories(xdg / "yams");
    fs::path cfgFile = xdg / "yams" / "config.toml";
    {
        std::ofstream out(cfgFile);
        out << "[daemon]\n";
        out << "plugin_dir = \"" << pluginPath->parent_path().string() << "\"\n\n";
        out << "[embeddings]\n";
        out << "preferred_model = \"all-MiniLM-L6-v2\"\n";
        out << "model_path = \"" << models_root << "\"\n";
        out << "keep_model_hot = true\n";
    }
    // Trust file will live under XDG_CONFIG_HOME/yams/plugins_trust.txt
    ::setenv("XDG_CONFIG_HOME", xdg.string().c_str(), 1);

    // Daemon config (temp socket/pid/log/data)
    fs::path sock = tmp / "d.sock";
    fs::path pid = tmp / "d.pid";
    fs::path log = tmp / "d.log";
    fs::path data = tmp / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true;
    // Let plugin/provider read from config.toml; keep pool config defaults

    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << "Failed to start daemon: " << started.error().message;
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 15s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait ready-ish
    bool ready = false;
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && (st.value().ready || st.value().overallStatus == "Ready")) {
            ready = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    ASSERT_TRUE(ready) << "Daemon not ready";

    // Trust plugin directory then load plugin
    yams::daemon::PluginTrustAddRequest tr;
    tr.path = pluginPath->parent_path().string();
    auto trr = yams::cli::run_sync(client.call(tr), 5s);
    ASSERT_TRUE(trr) << "Trust add failed: " << trr.error().message;

    yams::daemon::PluginLoadRequest pl;
    pl.pathOrName = pluginPath->string();
    pl.dryRun = false;
    auto plr = yams::cli::run_sync(client.call(pl), 10s);
    ASSERT_TRUE(plr) << "Plugin load failed: " << plr.error().message;
    EXPECT_EQ(plr.value().loaded, true);

    // Wait for model provider adoption
    bool mp_ready = false;
    for (int i = 0; i < 60; ++i) {
        auto st = yams::cli::run_sync(client.status(), 1s);
        if (st && st.value().readinessStates.count("model_provider") &&
            st.value().readinessStates.at("model_provider")) {
            mp_ready = true;
            break;
        }
        std::this_thread::sleep_for(500ms);
    }
    ASSERT_TRUE(mp_ready) << "Model provider was not adopted from plugin in time";

    // Load model via daemon API (longer timeout; model may be large)
    yams::daemon::ClientConfig cc2 = cc;
    cc2.requestTimeout = 90s;
    yams::daemon::DaemonClient client2(cc2);
    yams::daemon::LoadModelRequest lm;
    lm.modelName = "all-MiniLM-L6-v2";
    lm.preload = true;
    auto lmr = yams::cli::run_sync(client2.call(lm), 90s);
    ASSERT_TRUE(lmr) << "LoadModel failed: " << lmr.error().message;

    // Generate a simple embedding
    yams::daemon::GenerateEmbeddingRequest ge;
    ge.text = "test embedding";
    ge.modelName = "all-MiniLM-L6-v2";
    ge.normalize = true;
    auto ger = yams::cli::run_sync(client2.call(ge), 60s);
    ASSERT_TRUE(ger) << "GenerateEmbedding failed: " << ger.error().message;
    EXPECT_GT(ger.value().embedding.size(), 0u);

    // Cleanup temp artifacts
    cleanup();
}

TEST(OnnxPluginIT, LoadAllModelsInModelsRoot) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }
    auto pluginPath = get_plugin_path();
    if (!pluginPath) {
        GTEST_SKIP() << "ONNX plugin file not provided";
    }
    std::string models_root;
    if (!find_model_root(models_root)) {
        GTEST_SKIP() << "No models under ~/.yams/models";
    }

    // Temp layout
    fs::path tmp = fs::temp_directory_path() / ("yams_onx_it_all_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };
    fs::path xdg = tmp / "cfg";
    fs::create_directories(xdg / "yams");
    {
        std::ofstream out(xdg / "yams" / "config.toml");
        out << "[embeddings]\npreferred_model=\"all-MiniLM-L6-v2\"\nmodel_path=\"" << models_root
            << "\"\nkeep_model_hot=true\n";
    }
    ::setenv("XDG_CONFIG_HOME", xdg.string().c_str(), 1);

    fs::path sock = tmp / "d.sock";
    fs::path pid = tmp / "d.pid";
    fs::path log = tmp / "d.log";
    fs::path data = tmp / "data";
    fs::create_directories(data);
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true;
    yams::daemon::YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start());
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 120s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Request a scan of configured plugin_dir; daemon will adopt provider if available
    yams::daemon::PluginScanRequest scan;
    (void)yams::cli::run_sync(client.call(scan), 10s);

    // Wait for model provider readiness
    bool mp_ready = false;
    for (int i = 0; i < 60; ++i) {
        auto st = yams::cli::run_sync(client.status(), 1s);
        if (st && st.value().readinessStates.count("model_provider") &&
            st.value().readinessStates.at("model_provider")) {
            mp_ready = true;
            break;
        }
        std::this_thread::sleep_for(500ms);
    }
    ASSERT_TRUE(mp_ready);

    // Enumerate models root and try to load each directory containing model.onnx
    size_t attempted = 0, loaded_ok = 0;
    for (const auto& e : fs::directory_iterator(models_root)) {
        if (!e.is_directory())
            continue;
        fs::path mp = e.path() / "model.onnx";
        if (!fs::exists(mp))
            continue;
        std::string name = e.path().filename().string();
        ++attempted;
        yams::daemon::LoadModelRequest lm;
        lm.modelName = name;
        lm.preload = true;
        auto lmr = yams::cli::run_sync(client.call(lm), 120s);
        if (lmr)
            ++loaded_ok;
    }
    // At least 1 should load; if none, surface failure to flag potential model loading issue
    ASSERT_GT(attempted, 0u) << "No models found under " << models_root;
    EXPECT_EQ(loaded_ok, attempted) << "Some models failed to load";
    cleanup();
}
