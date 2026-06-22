// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_topology_presenter.h>
#include <yams/cli/plugin_helpers.h>
#include <yams/cli/plugin_util.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/search_runner.h>
#include <yams/cli/session_store.h>
#include <yams/cli/tune_runner.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

#include "../../common/test_helpers_catch2.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace yams::cli::test {
namespace {

// Catch2 decomposes comparison expressions inside CHECK/REQUIRE; this trips the
// chained-comparison lint even for ordinary assertions.
// NOLINTBEGIN(bugprone-chained-comparison)

using ScopedEnv = yams::test::ScopedEnvVar;

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_;
};

class CaptureStderr {
public:
    CaptureStderr() : old_(std::cerr.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStderr() { std::cerr.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_;
};

template <typename T> yams::Result<T> runAwait(boost::asio::awaitable<yams::Result<T>> aw) {
    boost::asio::io_context ioc;
    auto wrapper =
        [aw = std::move(aw)]() mutable -> boost::asio::awaitable<std::optional<yams::Result<T>>> {
        try {
            auto v = co_await std::move(aw);
            co_return std::optional<yams::Result<T>>(std::move(v));
        } catch (...) {
            co_return std::optional<yams::Result<T>>{};
        }
    };
    auto fut = boost::asio::co_spawn(ioc, wrapper(), boost::asio::use_future);
    ioc.run();
    auto opt = fut.get();
    if (opt) {
        return std::move(*opt);
    }
    return yams::Result<T>(yams::Error{yams::ErrorCode::InternalError, "Awaitable failed"});
}

std::filesystem::path tempRoot(const char* prefix) {
    auto root = std::filesystem::temp_directory_path() /
                (std::string(prefix) +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root);
    return root;
}

} // namespace

TEST_CASE("ProgressIndicator renders and clears each public style", "[cli][progress]") {
    CaptureStdout capture;

    {
        ProgressIndicator indicator(ProgressIndicator::Style::Percentage);
        CHECK_FALSE(indicator.isActive());
        indicator.start("indexing");
        CHECK(indicator.isActive());
        indicator.setUpdateInterval(0);
        indicator.update(5, 10);
        indicator.stop();
        CHECK_FALSE(indicator.isActive());
    }

    {
        ProgressIndicator indicator(ProgressIndicator::Style::Dots, true);
        CHECK(indicator.isActive());
        indicator.setUpdateInterval(0);
        indicator.setMessage("waiting");
        indicator.tick();
        indicator.stop();
    }

    {
        ProgressIndicator indicator(ProgressIndicator::Style::Bar);
        indicator.setShowCount(false);
        indicator.start("bar");
        indicator.setUpdateInterval(0);
        indicator.update(2, 4);
        indicator.stop();
    }

    const auto output = capture.str();
    CHECK(output.find("indexing") != std::string::npos);
    CHECK(output.find("50%") != std::string::npos);
    CHECK(output.find("waiting") != std::string::npos);
    CHECK(output.find("bar") != std::string::npos);
    CHECK(output.find("\033[K") != std::string::npos);
}

TEST_CASE("search_runner retries fuzzy and literal-text paths", "[cli][search-runner]") {
    using yams::cli::search_runner::DaemonSearchOptions;
    using yams::cli::search_runner::DaemonSearchResult;
    using yams::cli::search_runner::detail::DaemonSearchCaller;

    std::vector<daemon::SearchRequest> calls;
    DaemonSearchCaller unary = [&calls](const daemon::SearchRequest& req)
        -> boost::asio::awaitable<Result<daemon::SearchResponse>> {
        calls.push_back(req);
        daemon::SearchResponse response;
        if (calls.size() == 2) {
            response.totalCount = 1;
            response.results.push_back({.id = "id", .path = "/tmp/hit.txt", .score = 1.0});
        }
        co_return response;
    };
    DaemonSearchCaller streaming =
        [](const daemon::SearchRequest&) -> boost::asio::awaitable<Result<daemon::SearchResponse>> {
        co_return Error{ErrorCode::InternalError, "unused"};
    };

    DaemonSearchOptions opts;
    opts.query = "needle";
    auto fuzzy =
        runAwait<DaemonSearchResult>(yams::cli::search_runner::detail::daemon_search_with_callers(
            opts, false, unary, streaming));
    REQUIRE(fuzzy.has_value());
    CHECK(fuzzy.value().attempts == 2);
    CHECK(fuzzy.value().usedFuzzyRetry);
    REQUIRE(calls.size() == 2);
    CHECK_FALSE(calls[0].fuzzy);
    CHECK(calls[1].fuzzy);

    std::vector<daemon::SearchRequest> parseCalls;
    DaemonSearchCaller parseUnary = [&parseCalls](const daemon::SearchRequest& req)
        -> boost::asio::awaitable<Result<daemon::SearchResponse>> {
        parseCalls.push_back(req);
        if (parseCalls.size() == 1) {
            co_return Error{ErrorCode::InvalidArgument, "FTS5 syntax near quote"};
        }
        daemon::SearchResponse response;
        response.totalCount = 1;
        response.results.push_back({.id = "literal", .path = "/tmp/literal.txt", .score = 0.5});
        co_return response;
    };
    auto literal =
        runAwait<DaemonSearchResult>(yams::cli::search_runner::detail::daemon_search_with_callers(
            opts, false, parseUnary, streaming));
    REQUIRE(literal.has_value());
    CHECK(literal.value().attempts == 2);
    CHECK(literal.value().usedLiteralTextRetry);
    REQUIRE(parseCalls.size() == 2);
    CHECK_FALSE(parseCalls[0].literalText);
    CHECK(parseCalls[1].literalText);
}

TEST_CASE("session_store reads current session selectors and legacy pins", "[cli][session-store]") {
    auto root = tempRoot("yams_cli_session_store_");
    ScopedEnv stateHome("XDG_STATE_HOME", root.string());
    ScopedEnv current("YAMS_SESSION_CURRENT", std::nullopt);

    auto sessions = root / "yams" / "sessions";
    std::filesystem::create_directories(sessions);
    {
        std::ofstream index(sessions / "index.json");
        index << R"({"current":"work"})";
    }
    {
        std::ofstream session(sessions / "work.json");
        session
            << R"({"uuid":"uuid-1","selectors":[{"path":"src"},{"path":"*.md"}],"materialized":[{"path":"docs"},{"name":"named-selection"}]})";
    }
    std::filesystem::create_directories("src");
    std::filesystem::create_directories("docs");

    CHECK(session_store::current_session() == "work");
    CHECK(session_store::current_session_uuid() == "uuid-1");
    auto patterns = session_store::active_include_patterns(std::nullopt);
    CHECK(patterns.size() == 4);
    CHECK(patterns[0] == "src");
    CHECK(patterns[1] == "*.md");
    CHECK(patterns[3] == "named-selection");

    ScopedEnv envCurrent("YAMS_SESSION_CURRENT", "env-session");
    CHECK(session_store::current_session() == "env-session");

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST_CASE("plugin helpers parse status JSON and print readiness details", "[cli][plugin]") {
    const std::string pluginsJson =
        R"([{"name":"glint","type":"model_provider","interfaces":["model_provider_v1","search_provider_v1"]},{"name":"bad","interfaces":[7]}])";
    auto interfaces = plugin::parse_plugins_json_interfaces(pluginsJson);
    REQUIRE(interfaces.contains("glint"));
    CHECK(interfaces["glint"].size() == 2);
    auto types = plugin::parse_plugins_json_types(pluginsJson);
    CHECK(types["glint"] == "model_provider");
    CHECK(plugin::parse_plugins_json_interfaces("not json").empty());

    daemon::StatusResponse status;
    status.lifecycleState = "Starting";
    status.lastError = "warming";
    status.readinessStates["plugins"] = false;
    status.providers.push_back({.name = "glint",
                                .ready = false,
                                .degraded = true,
                                .error = "cold",
                                .modelsLoaded = 2,
                                .isProvider = true});
    status.skippedPlugins.push_back({.path = "/tmp/plugin.so", .reason = "untrusted"});

    CaptureStdout capture;
    plugin::print_plugin_not_ready_hint(status);
    plugin::print_plugin_list(status, interfaces, true);
    plugin::print_skipped_plugins(status.skippedPlugins);
    plugin::print_loaded_plugins_hint(status.providers);
    const auto output = capture.str();
    CHECK(output.find("readiness.plugins=false") != std::string::npos);
    CHECK(output.find("Loaded plugins (1)") != std::string::npos);
    CHECK(output.find("interfaces=model_provider_v1") != std::string::npos);
    CHECK(output.find("Skipped plugins (1)") != std::string::npos);
}

TEST_CASE("plugin utility path helpers are deterministic", "[cli][plugin]") {
    std::vector<std::filesystem::path> roots{"./plugins", "plugins/../plugins", "/tmp/yams"};
    auto deduped = plugin::dedupePluginRoots(std::move(roots));
    REQUIRE(deduped.size() == 2);
    CHECK(deduped[0].lexically_normal() == std::filesystem::path("plugins"));
    CHECK_FALSE(plugin::getPluginExtension().empty());
    CHECK(plugin::hasPluginExtension(std::filesystem::path("example") /
                                     ("plugin" + plugin::getPluginExtension())));
    CHECK_FALSE(plugin::hasPluginExtension("plugin.txt"));
}

TEST_CASE("topology snapshot presenter renders empty and populated snapshots", "[cli][topology]") {
    TopologyCommandRenderOptions options;
    options.requestedSnapshotId = "missing";

    std::ostringstream plain;
    REQUIRE(renderTopologySnapshots(plain, std::nullopt, options).has_value());
    CHECK(plain.str().find("No topology snapshot available") != std::string::npos);

    options.jsonOutput = true;
    std::ostringstream jsonOut;
    REQUIRE(renderTopologySnapshots(jsonOut, std::nullopt, options).has_value());
    CHECK(jsonOut.str().find("requested_snapshot_id") != std::string::npos);

    topology::TopologyArtifactBatch batch;
    batch.snapshotId = "snap-1";
    batch.algorithm = "connected";
    batch.inputKind = topology::TopologyInputKind::Hybrid;
    batch.generatedAtUnixSeconds = 42;
    batch.topologyEpoch = 7;
    batch.clusters.push_back({.clusterId = "cluster-a", .memberCount = 3});
    batch.memberships.push_back({.documentHash = "hash-a", .clusterId = "cluster-a"});

    std::ostringstream populated;
    REQUIRE(renderTopologySnapshots(populated, batch, options).has_value());
    CHECK(populated.str().find("snap-1") != std::string::npos);
    CHECK(populated.str().find("cluster_count") != std::string::npos);
}

TEST_CASE("tune runner noninteractive/status/reset/replay paths are fail-soft", "[cli][tune]") {
    auto data = tempRoot("yams_cli_tune_");
    ScopedEnv dataDir("YAMS_DATA_DIR", data.string());

    CaptureStdout out;
    TuneOptions opts;
    opts.nonInteractive = true;
    runInteractiveTune(nullptr, opts);
    runTuneStatus(nullptr, true);
    CHECK(runTuneReset(nullptr, "rules", true) == 0);
    runTuneReplayStub();

    CaptureStderr err;
    CHECK(runTuneReset(nullptr, "bogus", true) == 2);

    const auto stdoutText = out.str();
    CHECK(stdoutText.find("Non-interactive mode") != std::string::npos);
    CHECK(stdoutText.find("state_present") != std::string::npos);
    CHECK(stdoutText.find("R6 offline replay harness") != std::string::npos);
    CHECK(err.str().find("Unknown policy") != std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(data, ec);
}

// NOLINTEND(bugprone-chained-comparison)

} // namespace yams::cli::test
