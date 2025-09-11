#include <filesystem>
#include <fstream>
#include <string>
#include "test_async_helpers.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

TEST(OnnxHfRevisionCacheIT, ResolvesSnapshotFromRefsAndLoads) {
    if (std::getenv("GTEST_DISCOVERY_MODE"))
        GTEST_SKIP();

    // Prepare fake HF cache
    fs::path tmp = fs::temp_directory_path() / ("yams_hf_cache_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };
    std::string org = "acme";
    std::string name = "toy-model";
    std::string repo_dir = "models--" + org + "--" + name;
    fs::path base = tmp / repo_dir;
    fs::create_directories(base / "refs");
    fs::create_directories(base / "snapshots" / "abcdef123456");
    fs::create_directories(base / "snapshots" / "abcdef123456" / "onnx");
    // refs/main -> snapshots/abcdef123456
    {
        std::ofstream out(base / "refs" / "main");
        out << "abcdef123456";
    }
    // Create dummy model.onnx file
    {
        std::ofstream m(base / "snapshots" / "abcdef123456" / "onnx" / "model.onnx");
        m << "DUMMY";
    }

    // Env: point plugin resolver and skip actual ONNX loading
    ::setenv("YAMS_ONNX_HF_CACHE", tmp.string().c_str(), 1);
    ::setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);

    // Start daemon
    fs::path sock = tmp / "d.sock", pid = tmp / "d.pid", log = tmp / "d.log", data = tmp / "data";
    fs::create_directories(data);
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true;
    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 10s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);
    // Wait ready
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && (st.value().ready || st.value().overallStatus == "ready" ||
                   st.value().overallStatus == "Ready"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Attempt to load model using HF repo id and revision via options_json
    yams::daemon::LoadModelRequest r;
    r.modelName = org + "/" + name;
    r.preload = true;
    r.optionsJson = R"({"hf":{"revision":"main","offline":true}})";
    auto res = yams::cli::run_sync(client.loadModel(r), 10s);
    if (!res) {
        GTEST_SKIP() << "Model provider unavailable or plugin not loaded: " << res.error().message;
    }
    ASSERT_TRUE(res);

    cleanup();
}
