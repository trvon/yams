// Characterization tests for the `yams daemon status` renderers.
//
// The renderers are pure StatusResponse -> text, so a synthetic response drives every branch
// the CLI used to reach only through a live daemon RPC.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/daemon_status_render.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/metric_keys.h>

#include <filesystem>
#include <sstream>
#include <string>

namespace {

using yams::cli::DaemonStatusRenderContext;
using yams::daemon::MemorySyncResponse;
using yams::daemon::StatusResponse;

struct PlainColors {
    yams::cli::ui::ColorMode saved{yams::cli::ui::color_mode()};
    PlainColors() { yams::cli::ui::set_colors_enabled_override(false); }
    ~PlainColors() { yams::cli::ui::set_color_mode(saved); }
};

StatusResponse readyDaemon() {
    StatusResponse s;
    s.running = true;
    s.ready = true;
    s.version = "9.9.9";
    s.uptimeSeconds = 125;
    s.requestsProcessed = 42;
    s.lifecycleState = "ready";
    s.memoryUsageMb = 512;
    s.embeddingAvailable = true;
    s.embeddingModel = "all-MiniLM-L6-v2";
    s.contentStoreRoot = "/srv/yams/storage";
    s.vectorDbInitAttempted = true;
    s.vectorDbReady = true;
    s.readinessStates[std::string(yams::daemon::readiness::kDatabase)] = true;
    s.requestCounts["repair_running"] = 1;
    return s;
}

std::string renderBrief(const StatusResponse& s, const DaemonStatusRenderContext& ctx) {
    std::ostringstream os;
    yams::cli::renderDaemonStatusBrief(s, ctx, os);
    return os.str();
}

std::string renderDetailed(const StatusResponse& s, const DaemonStatusRenderContext& ctx) {
    std::ostringstream os;
    yams::cli::renderDaemonStatusDetailed(s, ctx, os);
    return os.str();
}

} // namespace

TEST_CASE("daemon status brief renders the overview from a synthetic response",
          "[cli][daemon][status][catch2]") {
    PlainColors plain;
    DaemonStatusRenderContext ctx;
    ctx.configuredDataDir = "/srv/yams";

    const std::string out = renderBrief(readyDaemon(), ctx);

    CHECK(out.find("YAMS Daemon") != std::string::npos);
    CHECK(out.find("Ready") != std::string::npos);
    CHECK(out.find("v9.9.9") != std::string::npos);
    CHECK(out.find("42 requests") != std::string::npos);
    CHECK(out.find("all-MiniLM-L6-v2") != std::string::npos);
    CHECK(out.find("Running") != std::string::npos);
    CHECK(out.find("Data dir warning") == std::string::npos);
    CHECK(out.find("Waiting on:") == std::string::npos);
    CHECK(out.find("Error:") == std::string::npos);
    CHECK(out.find("Memory Sync") == std::string::npos);
    CHECK(out.find("yams daemon status -d") != std::string::npos);
}

TEST_CASE("daemon status brief surfaces blockers, errors, and data-dir drift",
          "[cli][daemon][status][catch2]") {
    PlainColors plain;
    StatusResponse s = readyDaemon();
    s.ready = false;
    s.lifecycleState = "initializing";
    s.lastError = "vector index rebuild failed";
    s.readinessStates["content_store"] = false;
    s.initProgress["content_store"] = 40;

    DaemonStatusRenderContext ctx;
    ctx.configuredDataDir = "/elsewhere/yams";

    const std::string out = renderBrief(s, ctx);
    CHECK(out.find("Initializing") != std::string::npos);
    CHECK(out.find("Waiting on: Content Store (40%)") != std::string::npos);
    CHECK(out.find("Error: vector index rebuild failed") != std::string::npos);
    CHECK(out.find("Daemon data dir differs from current CLI/config") != std::string::npos);

    SECTION("an explicit --socket makes the data-dir difference expected") {
        ctx.explicitCustomSocket = true;
        const std::string quiet = renderBrief(s, ctx);
        CHECK(quiet.find("Daemon data dir differs") == std::string::npos);
    }
}

TEST_CASE("daemon status brief warns when the daemon serves a temporary data directory",
          "[cli][daemon][status][catch2]") {
    PlainColors plain;
    StatusResponse s = readyDaemon();
    s.contentStoreRoot =
        (std::filesystem::temp_directory_path() / "yams-ephemeral" / "storage").string();
    DaemonStatusRenderContext ctx;
    ctx.configuredDataDir = std::filesystem::temp_directory_path() / "yams-ephemeral";

    const std::string out = renderBrief(s, ctx);
    CHECK(out.find("Daemon is serving a temporary data directory") != std::string::npos);
    CHECK(out.find("differs from current CLI/config") == std::string::npos);
}

TEST_CASE("daemon status names the active repair operation from the shared code table",
          "[cli][daemon][status][catch2]") {
    PlainColors plain;
    StatusResponse s = readyDaemon();
    s.requestCounts[std::string(yams::daemon::metrics::kRepairCurrentOperationCode)] =
        yams::daemon::metrics::repairOperationCodeForName("block_refs");
    s.requestCounts[std::string(yams::daemon::metrics::kRepairCurrentOperationElapsedMs)] = 65000;

    DaemonStatusRenderContext ctx;
    const std::string brief = renderBrief(s, ctx);
    CHECK(brief.find("Running · block refs") != std::string::npos);

    const std::string detailed = renderDetailed(s, ctx);
    CHECK(detailed.find("block refs") != std::string::npos);
}

TEST_CASE("daemon status detailed renders every section from a synthetic response",
          "[cli][daemon][status][catch2]") {
    PlainColors plain;
    StatusResponse s = readyDaemon();
    StatusResponse::ProviderInfo provider;
    provider.name = "onnx";
    provider.ready = true;
    provider.modelsLoaded = 2;
    provider.isProvider = true;
    s.providers.push_back(provider);
    StatusResponse::ModelInfo model;
    model.name = "all-MiniLM-L6-v2";
    model.type = "onnx";
    model.memoryMb = 128;
    model.requestCount = 4;
    s.models.push_back(model);
    StatusResponse::PluginSkipInfo skipped;
    skipped.path = "/plugins/legacy_plugin.so";
    skipped.reason = "abi mismatch";
    s.skippedPlugins.push_back(skipped);

    DaemonStatusRenderContext ctx;
    ctx.configuredDataDir = "/srv/yams";
    const std::string out = renderDetailed(s, ctx);

    for (const char* header : {"YAMS Daemon — Detailed", "Resources", "Transport", "Search",
                               "Post-Ingest Pipeline", "Internal Processing", "Repair Service",
                               "Storage & Embeddings", "Providers", "Skipped Plugins", "Models"}) {
        INFO(header);
        CHECK(out.find(header) != std::string::npos);
    }
    CHECK(out.find("/plugins/legacy_plugin.so") != std::string::npos);
    CHECK(out.find("abi mismatch") != std::string::npos);
    CHECK(out.find("128 MB · 4 req") != std::string::npos);
    CHECK(out.find("2 models · active") != std::string::npos);
    CHECK(out.find("Components Not Ready") == std::string::npos);
    CHECK(out.find("Data Directory Warnings") == std::string::npos);
}

TEST_CASE("memory sync section renders only when the loop has started",
          "[cli][daemon][status][catch2]") {
    PlainColors plain;

    SECTION("no response") {
        std::ostringstream os;
        yams::cli::renderMemorySyncSection(nullptr, os);
        CHECK(os.str().empty());
    }

    MemorySyncResponse m;
    m.backend = "direct";
    m.mode = "writer";
    m.corpusId = "corpus-1";
    m.corpusEpoch = 3;
    m.nodeId = "node-a";
    m.trustMode = "authenticated-writers";
    m.peerCount = 2;
    m.records = 10;
    m.quarantinedRecords = 1;
    m.successfulCycles = 5;
    m.failedCycles = 1;
    m.lastSuccessAgeMs = 1000;

    SECTION("not started") {
        m.started = false;
        std::ostringstream os;
        yams::cli::renderMemorySyncSection(&m, os);
        CHECK(os.str().empty());
    }

    SECTION("started") {
        m.started = true;
        std::ostringstream os;
        yams::cli::renderMemorySyncSection(&m, os);
        const std::string out = os.str();
        CHECK(out.find("Memory Sync") != std::string::npos);
        CHECK(out.find("corpus-1 (epoch 3)") != std::string::npos);
        CHECK(out.find("authenticated-writers") != std::string::npos);
        CHECK(out.find("yams p2p peers for details") != std::string::npos);
        CHECK(out.find("check daemon log for reasons") != std::string::npos);
        CHECK(out.find("5 ok · 1 failed") != std::string::npos);
    }
}
