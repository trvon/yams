/*
  repair_ability_bench.cpp - KPI 4 repair ability harness

  Injects documents that skip embeddings (fault surface), runs daemon
  RepairRequest operations, and emits JSON metrics for xplan.

  Env:
    YAMS_TEST_SAFE_SINGLE_INSTANCE=1  required
    YAMS_BENCH_FAULT_DOCS=N           documents to inject (default 20)
    YAMS_BENCH_DOC_SIZE=N             bytes per doc (default 512)
    YAMS_BENCH_FAULT_KIND=embed|fts5|graph|orphans|all
    YAMS_BENCH_OUTPUT=path            JSON output path (default stdout)
    YAMS_BENCH_REPAIR_TIMEOUT_S=N     callRepair timeout (default 120)
*/

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

int envInt(const char* name, int def) {
    if (const char* v = std::getenv(name); v && *v) {
        try {
            return std::stoi(v);
        } catch (...) {
            return def;
        }
    }
    return def;
}

std::string envStr(const char* name, const std::string& def) {
    if (const char* v = std::getenv(name); v && *v)
        return std::string(v);
    return def;
}

yams::daemon::RepairRequest buildRequest(const std::string& kind) {
    yams::daemon::RepairRequest req;
    req.foreground = true;
    req.verbose = false;
    req.force = true;
    if (kind == "embed" || kind == "embeddings") {
        req.repairEmbeddings = true;
    } else if (kind == "fts5" || kind == "fts") {
        req.repairFts5 = true;
    } else if (kind == "graph" || kind == "kg") {
        req.repairGraph = true;
    } else if (kind == "orphans" || kind == "orphan") {
        req.repairOrphans = true;
    } else {
        // "all" or unknown: embeddings + fts5 + graph
        req.repairEmbeddings = true;
        req.repairFts5 = true;
        req.repairGraph = true;
    }
    return req;
}

std::string makeDoc(int i, int size) {
    std::string body = "repair ability fixture doc " + std::to_string(i) + "\n";
    body += "topic authentication database network parsing\n";
    while (static_cast<int>(body.size()) < size)
        body += " token" + std::to_string(i % 17);
    if (static_cast<int>(body.size()) > size)
        body.resize(static_cast<size_t>(size));
    return body;
}

} // namespace

int main(int /*argc*/, char** /*argv*/) {
    const char* logLevel = std::getenv("YAMS_LOG_LEVEL");
    if (logLevel && std::string(logLevel) == "debug")
        spdlog::set_level(spdlog::level::debug);
    else
        spdlog::set_level(spdlog::level::info);

    const int faultDocs = envInt("YAMS_BENCH_FAULT_DOCS", 20);
    const int docSize = envInt("YAMS_BENCH_DOC_SIZE", 512);
    const std::string faultKind = envStr("YAMS_BENCH_FAULT_KIND", "embed");
    const int repairTimeoutS = envInt("YAMS_BENCH_REPAIR_TIMEOUT_S", 120);
    const char* outputPath = std::getenv("YAMS_BENCH_OUTPUT");

    json out;
    out["test"] = "repair_ability";
    out["fault_kind"] = faultKind;
    out["fault_docs"] = faultDocs;
    out["doc_size"] = docSize;

    yams::test::DaemonHarnessOptions opts;
    opts.useMockModelProvider = true;
    opts.autoLoadPlugins = false;
    // RepairService is only constructed when auto-repair is enabled; we still
    // drive on-demand RepairRequest for measurement (not background auto-scan).
    opts.enableAutoRepair = true;
    opts.enableModelProvider = true;

    yams::test::DaemonHarness harness(opts);
    if (!harness.start(std::chrono::seconds(45))) {
        out["status"] = "failed";
        out["error"] = "daemon harness start failed";
        const std::string dumped = out.dump(2);
        if (outputPath) {
            std::ofstream(outputPath) << dumped << "\n";
        } else {
            std::cout << dumped << "\n";
        }
        return 2;
    }

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = harness.socketPath();
    ccfg.autoStart = false;
    ccfg.requestTimeout = std::chrono::seconds(30);
    yams::daemon::DaemonClient client(ccfg);

    auto connect = yams::cli::run_sync(client.connect(), 10s);
    if (!connect) {
        out["status"] = "failed";
        out["error"] = "connect failed: " + connect.error().message;
        const std::string dumped = out.dump(2);
        if (outputPath)
            std::ofstream(outputPath) << dumped << "\n";
        else
            std::cout << dumped << "\n";
        return 2;
    }

    // Inject documents that skip embeddings — creates repair surface for embed kind.
    int injected = 0;
    int injectFailed = 0;
    const auto injectStart = std::chrono::steady_clock::now();
    for (int i = 0; i < faultDocs; ++i) {
        yams::daemon::AddDocumentRequest req;
        req.name = "repair_fault_" + std::to_string(i) + ".md";
        req.content = makeDoc(i, docSize);
        req.tags = {"bench", "repair-ability", faultKind};
        req.collection = "repair_ability_bench";
        req.noEmbeddings = true;

        auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
        if (res)
            ++injected;
        else
            ++injectFailed;
    }
    const auto injectEnd = std::chrono::steady_clock::now();
    out["faults_injected"] = injected;
    out["inject_failures"] = injectFailed;
    out["inject_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(injectEnd - injectStart)
                           .count();

    // Brief settle for post-ingest metadata commit.
    std::this_thread::sleep_for(500ms);

    auto repairReq = buildRequest(faultKind);
    uint64_t eventCount = 0;
    uint64_t lastSucceeded = 0;
    uint64_t lastFailed = 0;
    uint64_t lastSkipped = 0;
    std::string lastOp;

    const auto repairStart = std::chrono::steady_clock::now();
    auto repairResult = yams::cli::run_sync(
        client.callRepair(repairReq,
                          [&](const yams::daemon::RepairEvent& ev) {
                              ++eventCount;
                              lastSucceeded = ev.succeeded;
                              lastFailed = ev.failed;
                              lastSkipped = ev.skipped;
                              lastOp = ev.operation;
                          }),
        std::chrono::seconds(repairTimeoutS));
    const auto repairEnd = std::chrono::steady_clock::now();
    const auto repairMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(repairEnd - repairStart).count();

    out["repair_wall_ms"] = repairMs;
    out["detect_latency_ms"] = repairMs; // first-cut: wall covers detect+repair for foreground
    out["repair_event_count"] = eventCount;
    out["last_operation"] = lastOp;

    if (!repairResult) {
        out["status"] = "failed";
        out["error"] = repairResult.error().message;
        out["faults_repaired"] = 0;
        out["residual_faults"] = injected;
        out["repair_docs_per_s"] = 0.0;
    } else {
        const auto& resp = repairResult.value();
        out["status"] = resp.success ? "ok" : "failed";
        out["repair_success"] = resp.success;
        out["total_operations"] = resp.totalOperations;
        out["total_succeeded"] = resp.totalSucceeded;
        out["total_failed"] = resp.totalFailed;
        out["total_skipped"] = resp.totalSkipped;
        out["errors"] = resp.errors;

        json ops = json::array();
        uint64_t repaired = 0;
        for (const auto& op : resp.operationResults) {
            ops.push_back({{"operation", op.operation},
                           {"succeeded", op.succeeded},
                           {"failed", op.failed},
                           {"skipped", op.skipped}});
            repaired += op.succeeded;
        }
        if (repaired == 0 && lastSucceeded > 0)
            repaired = lastSucceeded;
        out["operation_results"] = ops;
        out["faults_repaired"] = repaired;
        // Residual is a lower bound: inject failures + unrepaired.
        const int64_t residual =
            static_cast<int64_t>(injected) - static_cast<int64_t>(repaired);
        out["residual_faults"] = residual > 0 ? residual : 0;
        out["repair_docs_per_s"] =
            repairMs > 0 ? (1000.0 * static_cast<double>(repaired) / static_cast<double>(repairMs))
                         : 0.0;
        if (!resp.success)
            out["error"] = "repair response success=false";
    }

    const std::string dumped = out.dump(2);
    if (outputPath) {
        std::ofstream(outputPath) << dumped << "\n";
        spdlog::info("repair_ability results -> {}", outputPath);
    } else {
        std::cout << dumped << "\n";
    }

    return out.value("status", std::string("failed")) == "ok" ? 0 : 1;
}
