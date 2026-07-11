#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <thread>

#include "common/test_helpers_catch2.h"

#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/in_process_transport.h>
#include <yams/daemon/embedded_service_host.h>
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

TEST_CASE("IntegrationSmoke.DaemonClientEmbeddedStreamingSearchAndList",
          "[smoke][integrationsmoke]") {
    const fs::path dataDir = yams::test::make_temp_dir("yams_embedded_stream_smoke_");

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar syncAdd("YAMS_SYNC_SINGLE_FILE_ADD", std::string("1"));

    yams::daemon::EmbeddedServiceHost::Options hostOpts;
    hostOpts.dataDir = dataDir;
    hostOpts.ioThreads = 1;
    hostOpts.enableAutoRepair = false;
    hostOpts.autoLoadPlugins = false;
    hostOpts.enableModelProvider = false;
    hostOpts.initTimeoutSeconds = 30;

    auto hostRes = yams::daemon::EmbeddedServiceHost::getOrCreate(hostOpts);
    INFO((hostRes.has_value() ? std::string{} : hostRes.error().message));
    REQUIRE(hostRes.has_value());
    auto host = hostRes.value();
    const bool hostCreated = host != nullptr;
    REQUIRE(hostCreated);

    yams::daemon::InProcessTransport transport(host);
    auto call = [&](const yams::daemon::Request& request) {
        return yams::cli::run_sync(transport.send_request(request), 15s);
    };

    auto addDoc =
        [&](const std::string& name,
            const std::string& content) -> yams::Result<yams::daemon::AddDocumentResponse> {
        yams::daemon::AddDocumentRequest req;
        req.name = name;
        req.content = content;
        req.tags = {"embedded-streaming-smoke"};
        req.noEmbeddings = true;
        req.waitForProcessing = true;
        req.waitTimeoutSeconds = 10;
        auto response =
            call(yams::daemon::Request{std::in_place_type<yams::daemon::AddDocumentRequest>, req});
        if (!response) {
            return response.error();
        }
        if (const auto* error = std::get_if<yams::daemon::ErrorResponse>(&response.value())) {
            return yams::Error{error->code, error->message};
        }
        if (const auto* add = std::get_if<yams::daemon::AddDocumentResponse>(&response.value())) {
            return *add;
        }
        return yams::Error{yams::ErrorCode::InvalidData, "unexpected add response type"};
    };

    auto add1 =
        addDoc("embedded-stream-doc-1.txt", "alpha_embedded_stream_token first document content");
    INFO((add1.has_value() ? std::string{} : add1.error().message));
    REQUIRE(add1.has_value());

    auto add2 =
        addDoc("embedded-stream-doc-2.txt", "second document with beta_embedded_stream_token");
    INFO((add2.has_value() ? std::string{} : add2.error().message));
    REQUIRE(add2.has_value());

    yams::daemon::ListRequest listReq;
    listReq.limit = 100;
    listReq.tags = {"embedded-streaming-smoke"};
    listReq.matchAllTags = true;

    yams::Result<yams::daemon::ListResponse> lastList =
        yams::Error{yams::ErrorCode::NotFound, "list not executed"};
    const bool listReady = wait_until(
        [&]() {
            auto response =
                call(yams::daemon::Request{std::in_place_type<yams::daemon::ListRequest>, listReq});
            if (!response) {
                lastList = response.error();
                return false;
            }
            if (const auto* error = std::get_if<yams::daemon::ErrorResponse>(&response.value())) {
                lastList = yams::Error{error->code, error->message};
                return false;
            }
            if (const auto* list = std::get_if<yams::daemon::ListResponse>(&response.value())) {
                lastList = *list;
                return lastList.value().items.size() >= 2;
            }
            lastList = yams::Error{yams::ErrorCode::InvalidData, "unexpected list response type"};
            return false;
        },
        15s);

    INFO((lastList ? "list returned too few items"
                   : std::string("list failed: ") + lastList.error().message));
    REQUIRE(listReady);

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
            auto response = call(
                yams::daemon::Request{std::in_place_type<yams::daemon::SearchRequest>, searchReq});
            if (!response) {
                lastSearch = response.error();
                return false;
            }
            if (const auto* error = std::get_if<yams::daemon::ErrorResponse>(&response.value())) {
                lastSearch = yams::Error{error->code, error->message};
                return false;
            }
            if (const auto* search = std::get_if<yams::daemon::SearchResponse>(&response.value())) {
                lastSearch = *search;
                return !lastSearch.value().results.empty();
            }
            lastSearch =
                yams::Error{yams::ErrorCode::InvalidData, "unexpected search response type"};
            return false;
        },
        15s);

    INFO((lastSearch ? "search returned no results"
                     : std::string("search failed: ") + lastSearch.error().message));
    REQUIRE(searchReady);
    const bool hasSearchResults = lastSearch.value().totalCount >= 1u;
    CHECK(hasSearchResults);

    auto shutdownRes = host->shutdown();
    REQUIRE(shutdownRes.has_value());

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}
