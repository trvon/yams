// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/doctor/benchmark.h>
#include <yams/cli/doctor/repairs/dedupe.h>
#include <yams/cli/doctor/repairs/embedding_progress.h>
#include <yams/cli/doctor/repairs/fts5_event_renderer.h>
#include <yams/cli/doctor/repairs/fts5_progress.h>
#include <yams/cli/doctor/repairs/vector_fix.h>
#include <yams/cli/doctor_checks.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

#include "../../common/test_helpers_catch2.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace yams::cli::doctor::test {
namespace {

// Catch2 decomposes comparison expressions inside CHECK/REQUIRE; this trips the
// chained-comparison lint even for ordinary assertions.
// NOLINTBEGIN(bugprone-chained-comparison)

std::filesystem::path tempRoot(const char* prefix) {
    auto root = std::filesystem::temp_directory_path() /
                (std::string(prefix) +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root);
    return root;
}

TEST_CASE("doctor_checks public helpers handle cached and unavailable contexts",
          "[cli][doctor][checks]") {
    auto root = tempRoot("yams_doctor_checks_");
    yams::test::ScopedEnvVar dataDir("YAMS_DATA_DIR", root.string());
    yams::test::ScopedEnvVar home("HOME", root.string());

    std::optional<daemon::StatusResponse> status;
    status.emplace();
    status->ready = true;
    status->overallStatus = "ready";
    status->lifecycleState = "Running";
    status->version = "test-version";
    status->activeConnections = 3;
    status->memoryUsageMb = 12.5;
    status->cpuUsagePercent = 4.5;
    status->readinessStates["vector_embeddings_available"] = true;
    status->readinessStates["vector_scoring_enabled"] = false;
    status->readinessStates["vector_db"] = true;
    status->requestCounts["worker_threads"] = 4;
    status->requestCounts["worker_active"] = 1;
    status->requestCounts["worker_queued"] = 2;
    status->vectorDbReady = true;
    status->vectorDbDim = 384;
    status->embeddingAvailable = true;
    status->embeddingDim = 768;

    auto daemon = checkDaemon(nullptr, status);
    CHECK(daemon.running);
    CHECK(daemon.ready);
    CHECK(daemon.lifecycleState == "Running");
    CHECK(daemon.workerThreads == 4);
    CHECK(daemon.vectorEmbeddingsAvailable);

    auto dimensions = checkDimensions(nullptr, status);
    CHECK(dimensions.dbReady);
    CHECK(dimensions.dbDimension == 384);
    CHECK_FALSE(dimensions.matches);
    CHECK_FALSE(dimensions.recommendations.empty());

    auto modelsPath = root / "yams" / "models" / "all-MiniLM-L6-v2";
    std::filesystem::create_directories(modelsPath);
    std::ofstream(modelsPath / "model.onnx") << "placeholder";
    std::ofstream(modelsPath / "config.json") << "{}";
    std::ofstream(modelsPath / "tokenizer.json") << "{}";
    auto models = checkInstalledModels(nullptr);
    CHECK(models.currentPath.filename() == "models");

    auto vectorDb = checkVectorDb(nullptr);
    CHECK_FALSE(vectorDb.exists);
    CHECK(vectorDb.schemaType == "none");

    auto kg = checkKnowledgeGraph(nullptr);
    CHECK_FALSE(kg.available);
    CHECK_FALSE(kg.issues.empty());
}

TEST_CASE("BenchmarkCommand and DedupeCommand fail softly without CLI context",
          "[cli][doctor][repair]") {
    auto root = tempRoot("yams_doctor_commands_");
    yams::test::ScopedEnvVar dataDir("YAMS_DATA_DIR", root.string());

    BenchmarkCommand::Config benchCfg;
    BenchmarkCommand benchmark(nullptr, benchCfg);
    std::ostringstream benchOut;
    benchmark.execute(benchOut);
    CHECK(benchOut.str().find("CLI context unavailable") != std::string::npos);

    std::ostringstream historyOut;
    benchmark.printHistory(historyOut, false, 5);
    CHECK(historyOut.str().find("Benchmark History") != std::string::npos);

    std::ostringstream historyJson;
    benchmark.printHistory(historyJson, true, 5);
    CHECK(historyJson.str().find("[") != std::string::npos);

    DedupeCommand dedupe;
    DedupeCommand::Config dedupeCfg;
    std::ostringstream dedupeOut;
    dedupe.execute(dedupeOut, nullptr, dedupeCfg);
    CHECK(dedupeOut.str().find("CLI context unavailable") != std::string::npos);
}

TEST_CASE("EmbeddingProgressRenderer throttles non-tty output and finishes tty lines",
          "[cli][doctor][repair][progress]") {
    std::ostringstream nonTty;
    auto start = std::chrono::steady_clock::now() - std::chrono::seconds(3);
    auto last = std::chrono::steady_clock::time_point{};
    EmbeddingProgressRenderer renderer(nonTty, false, last, start);

    renderer.render(1, 10, "skip this non tty increment");
    CHECK(nonTty.str().empty());

    renderer.render(200, 400, std::string(100, 'x'));
    const auto text = nonTty.str();
    CHECK(text.find("Embeddings") != std::string::npos);
    CHECK(text.find("docs") != std::string::npos);
    CHECK(text.find("elapsed") != std::string::npos);

    std::ostringstream tty;
    EmbeddingProgressRenderer ttyRenderer(tty, true, last, start);
    ttyRenderer.finish();
    CHECK(tty.str() == "\n");
}

TEST_CASE("Fts5EventRenderer groups operations and reports error/completion",
          "[cli][doctor][repair][fts5]") {
    std::ostringstream out;
    std::string lastOperation;
    uint64_t lastProcessed = 0;
    auto lastPrint = std::chrono::steady_clock::time_point{};
    Fts5EventRenderer renderer(out, false, lastOperation, lastProcessed, lastPrint);

    daemon::RepairEvent repairing;
    repairing.phase = "repairing";
    repairing.operation = "fts5";
    repairing.processed = 10;
    repairing.total = 20;
    renderer.onRepairEvent(repairing);

    daemon::RepairEvent error;
    error.phase = "error";
    error.operation = "fts5";
    error.message = "bad row";
    renderer.onRepairEvent(error);

    daemon::RepairEvent completed;
    completed.phase = "completed";
    completed.operation = "fts5";
    completed.processed = 20;
    completed.succeeded = 19;
    completed.failed = 1;
    renderer.onRepairEvent(completed);

    const auto text = out.str();
    CHECK(text.find("Daemon: fts5") != std::string::npos);
    CHECK(text.find("bad row") != std::string::npos);
    CHECK(text.find("Daemon FTS5 repair done") != std::string::npos);
    CHECK(text.find("failed=1") != std::string::npos);
}

TEST_CASE("Fts5ProgressRenderer renders forced newline progress", "[cli][doctor][repair][fts5]") {
    std::ostringstream out;
    size_t current = 25;
    auto started = std::chrono::steady_clock::now() - std::chrono::seconds(5);
    auto lastPrint = std::chrono::steady_clock::time_point{};
    Fts5ProgressRenderer renderer(out, false, current, 100, started, lastPrint);

    renderer.render(true);
    const auto text = out.str();
    CHECK(text.find("FTS5") != std::string::npos);
    CHECK(text.find("docs") != std::string::npos);
    CHECK(text.find("elapsed") != std::string::npos);
}

TEST_CASE("VectorFixRepair render covers no-vector ok mismatch and fixed states",
          "[cli][doctor][repair][vector]") {
    VectorFixRepair::Result noVectors;
    noVectors.error = "No vectors.db found or no vectors stored yet";
    std::ostringstream noVectorsOut;
    VectorFixRepair::render(noVectorsOut, noVectors, false);
    CHECK(noVectorsOut.str().find("Nothing to fix") != std::string::npos);

    VectorFixRepair::Result ok;
    ok.dbDim = 384;
    ok.modelDim = 384;
    ok.modelName = "all-MiniLM-L6-v2";
    std::ostringstream okJson;
    VectorFixRepair::render(okJson, ok, true);
    CHECK(okJson.str().find("\"status\":\"ok\"") != std::string::npos);

    VectorFixRepair::Result mismatch;
    mismatch.dbDim = 384;
    mismatch.modelDim = 768;
    mismatch.modelName = "wrong-model";
    mismatch.mismatch = true;
    mismatch.error = "No installed model matches 384-dim";
    std::ostringstream mismatchOut;
    VectorFixRepair::render(mismatchOut, mismatch, false);
    CHECK(mismatchOut.str().find("No installed model matches") != std::string::npos);
    CHECK(mismatchOut.str().find("Options:") != std::string::npos);

    VectorFixRepair::Result fixed;
    fixed.dbDim = 384;
    fixed.modelDim = 768;
    fixed.modelName = "wrong-model";
    fixed.matchingModel = "all-MiniLM-L6-v2";
    fixed.mismatch = true;
    fixed.fixed = true;
    std::ostringstream fixedJson;
    VectorFixRepair::render(fixedJson, fixed, true);
    CHECK(fixedJson.str().find("\"status\":\"fixed\"") != std::string::npos);
    CHECK(fixedJson.str().find("all-MiniLM-L6-v2") != std::string::npos);
}

// NOLINTEND(bugprone-chained-comparison)

} // namespace
} // namespace yams::cli::doctor::test
