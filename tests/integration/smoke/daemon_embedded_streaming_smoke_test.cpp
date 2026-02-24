#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>

#include "common/test_helpers.h"

#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

template <typename Func>
bool wait_until(Func&& predicate, std::chrono::milliseconds timeout,
                std::chrono::milliseconds interval = 100ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(interval);
    }
    return predicate();
}

} // namespace

TEST(IntegrationSmoke, DaemonClientEmbeddedStreamingSearchAndList) {
    const fs::path dataDir = yams::test::make_temp_dir("yams_embedded_stream_smoke_");

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar syncAdd("YAMS_SYNC_SINGLE_FILE_ADD", std::string("1"));

    yams::daemon::ClientConfig cfg;
    cfg.autoStart = false;
    cfg.transportMode = yams::daemon::ClientTransportMode::Auto;

    yams::daemon::DaemonClient client(cfg);

    auto connectRes = yams::cli::run_sync(client.connect(), 20s);
    ASSERT_TRUE(connectRes.has_value()) << connectRes.error().message;
    ASSERT_TRUE(client.isConnected());

    auto addDoc = [&](const std::string& name, const std::string& content) {
        yams::daemon::AddDocumentRequest req;
        req.name = name;
        req.content = content;
        req.tags = {"embedded-streaming-smoke"};
        req.noEmbeddings = true;
        req.waitForProcessing = true;
        req.waitTimeoutSeconds = 10;
        return yams::cli::run_sync(client.streamingAddDocument(req), 15s);
    };

    auto add1 =
        addDoc("embedded-stream-doc-1.txt", "alpha_embedded_stream_token first document content");
    ASSERT_TRUE(add1.has_value()) << add1.error().message;

    auto add2 =
        addDoc("embedded-stream-doc-2.txt", "second document with beta_embedded_stream_token");
    ASSERT_TRUE(add2.has_value()) << add2.error().message;

    yams::daemon::ListRequest listReq;
    listReq.limit = 100;
    listReq.tags = {"embedded-streaming-smoke"};
    listReq.matchAllTags = true;

    yams::Result<yams::daemon::ListResponse> lastList =
        yams::Error{yams::ErrorCode::NotFound, "list not executed"};
    const bool listReady = wait_until(
        [&]() {
            lastList = yams::cli::run_sync(client.streamingList(listReq), 10s);
            return lastList.has_value() && lastList.value().items.size() >= 2;
        },
        15s);

    ASSERT_TRUE(listReady) << (lastList ? "list returned too few items"
                                        : std::string("list failed: ") + lastList.error().message);

    yams::daemon::SearchRequest searchReq;
    searchReq.query = "alpha_embedded_stream_token";
    searchReq.limit = 20;
    searchReq.searchType = "keyword";
    searchReq.tags = {"embedded-streaming-smoke"};
    searchReq.matchAllTags = true;

    yams::Result<yams::daemon::SearchResponse> lastSearch =
        yams::Error{yams::ErrorCode::NotFound, "search not executed"};
    const bool searchReady = wait_until(
        [&]() {
            lastSearch = yams::cli::run_sync(client.streamingSearch(searchReq), 10s);
            return lastSearch.has_value() && !lastSearch.value().results.empty();
        },
        15s);

    ASSERT_TRUE(searchReady) << (lastSearch
                                     ? "search returned no results"
                                     : std::string("search failed: ") + lastSearch.error().message);
    EXPECT_GE(lastSearch.value().totalCount, 1u);

    client.disconnect();

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}
