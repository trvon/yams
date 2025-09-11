#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

TEST(AutoEmbedOnAddIT, TriggersEmbeddingsAndIncreasesIndex) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during discovery";
    }

    fs::path tmp = fs::temp_directory_path() / ("yams_auto_embed_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    // Write config to enable auto_on_add
    fs::path xdg = tmp / "cfg";
    fs::create_directories(xdg / "yams");
    std::ofstream(xdg / "yams" / "config.toml") << "[embeddings]\nauto_on_add=\"true\"\n";
    ::setenv("XDG_CONFIG_HOME", xdg.string().c_str(), 1);

    fs::path sock = tmp / "d.sock", pid = tmp / "d.pid", log = tmp / "d.log", data = tmp / "data";
    fs::create_directories(data);
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = false;
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
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && (st.value().ready || st.value().overallStatus == "ready" ||
                   st.value().overallStatus == "Ready"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Baseline vector index size
    auto s0 = yams::cli::run_sync(client.getStats(yams::daemon::GetStatsRequest{}), 5s);
    ASSERT_TRUE(s0);
    size_t base = s0.value().vectorIndexSize;

    // Add a simple text doc; auto_on_add should queue embedding and persist
    yams::daemon::AddDocumentRequest add;
    add.path = "";
    add.content = "auto embed content";
    add.name = "auto_embed.txt";
    add.mimeType = "text/plain";
    add.noEmbeddings = false;
    auto ar = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
    ASSERT_TRUE(ar);

    // Wait for vector index to grow (readiness wait loop)
    bool grew = false;
    for (int i = 0; i < 50; ++i) {
        auto s1 = yams::cli::run_sync(client.getStats(yams::daemon::GetStatsRequest{}), 5s);
        if (s1 && s1.value().vectorIndexSize > base) {
            grew = true;
            break;
        }
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_TRUE(grew) << "Vector index did not grow after auto_on_add";

    cleanup();
}
