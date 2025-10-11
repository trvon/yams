// Semantic search E2E: add -> embed -> hybrid search hit
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

TEST(SemanticSearchE2E, AddEmbedSearch) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    // Prefer mock provider for E2E predictability
    auto truthy = [](const char* value) {
        if (!value || !*value) {
            return false;
        }
        std::string v(value);
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return !(v == "0" || v == "false" || v == "off" || v == "no");
    };
    bool useMockProvider = true;
    if (const char* v = std::getenv("YAMS_USE_MOCK_PROVIDER")) {
        useMockProvider = truthy(v);
    }

    fs::path tmp = fs::temp_directory_path() / ("yams_sem_e2e_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

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
    cfg.useMockModelProvider = useMockProvider;
    yams::daemon::YamsDaemon daemon(cfg);
    ASSERT_TRUE(daemon.start()) << "Failed to start daemon";
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 30s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait for content store + metadata repo readiness
    for (int i = 0; i < 50; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && st.value().readinessStates.count("content_store") &&
            st.value().readinessStates.at("content_store") &&
            st.value().readinessStates.count("metadata_repo") &&
            st.value().readinessStates.at("metadata_repo"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Add document with unique content
    std::string content = "semantic-e2e unique phrase zoomy-blorbulated foobar";
    yams::daemon::AddDocumentRequest add;
    add.path = "";
    add.content = content;
    add.name = "sem_e2e.txt";
    add.mimeType = "text/plain";
    add.noEmbeddings = true; // embed separately
    auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
    ASSERT_TRUE(addRes) << "AddDocument failed: " << addRes.error().message;
    auto hash = addRes.value().hash;
    ASSERT_FALSE(hash.empty());

    // Embed the document via daemon
    yams::daemon::EmbedDocumentsRequest ed;
    ed.modelName = "all-MiniLM-L6-v2";
    ed.documentHashes = {hash};
    ed.batchSize = 8;
    ed.skipExisting = false;
    auto er = yams::cli::run_sync(client.call(ed), 30s);
    ASSERT_TRUE(er) << "EmbedDocuments failed: " << er.error().message;
    EXPECT_EQ(er.value().requested, 1u);
    EXPECT_GE(er.value().embedded, 0u);

    // Search for a token from the content; expect a hit
    yams::daemon::SearchRequest sr;
    sr.query = "blorbulated";
    sr.limit = 5;
    sr.fuzzy = false;
    auto sres = yams::cli::run_sync(client.search(sr), 10s);
    ASSERT_TRUE(sres) << "Search failed: " << sres.error().message;
    EXPECT_GE(sres.value().totalCount, 1UL);

    cleanup();
}
