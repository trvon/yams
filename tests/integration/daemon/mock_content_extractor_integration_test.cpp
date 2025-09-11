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

TEST(MockContentExtractorIT, IndexesViaPluginAndSearchable) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during discovery";
    }

    // Locate built plugin artifact directory using YAMS_PLUGIN_DIR override
    fs::path pluginDir;
#ifdef CMAKE_BINARY_DIR
    pluginDir = fs::path(CMAKE_BINARY_DIR) / "tests" / "plugins" / "mock_content_extractor";
#else
    // If CMAKE_BINARY_DIR is not defined in this build context, force skip below
    pluginDir.clear();
#endif
    if (!fs::exists(pluginDir)) {
        GTEST_SKIP() << "mock_content_extractor plugin not built";
    }
    ::setenv("YAMS_PLUGIN_DIR", pluginDir.string().c_str(), 1);

    fs::path tmp = fs::temp_directory_path() / ("yams_mock_ce_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

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
    // wait ready
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && (st.value().ready || st.value().overallStatus == "ready" ||
                   st.value().overallStatus == "Ready"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Add a .mock document; plugin should extract MOCK_EXTRACTED_TEXT
    yams::daemon::AddDocumentRequest add;
    add.path = "";
    add.content = "ignored";
    add.name = "plugin_test.mock";
    add.mimeType = "application/x-mock";
    auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
    ASSERT_TRUE(addRes);

    // Search for the plugin-provided text
    yams::daemon::SearchRequest s;
    s.query = "MOCK_EXTRACTED_TEXT";
    s.limit = 5;
    s.searchType = "keyword";
    s.verbose = true;
    bool found = false;
    for (int i = 0; i < 30; ++i) {
        auto r = yams::cli::run_sync(client.search(s), 2s);
        if (r && r.value().totalCount > 0) {
            found = true;
            break;
        }
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_TRUE(found) << "Expected keyword hit from mock content extractor";

    cleanup();
}
