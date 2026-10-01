// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// `yams p2p status` rendering of memory-sync apply health. The formatter is pure, so a synthetic
// reply drives every branch the CLI otherwise reaches only through a live daemon.

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <string>

#include <yams/cli/p2p_status_format.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace {

yams::daemon::MemorySyncResponse stuckMeshNode() {
    yams::daemon::MemorySyncResponse m;
    m.started = true;
    m.backend = "direct";
    m.nodeId = "net-research";
    m.corpusId = "corpus";
    m.corpusEpoch = 1;
    m.mode = "persistent";
    m.trustMode = "mutual-tls-operator-pinned";
    m.peerCount = 2;
    m.records = 219;
    m.successfulCycles = 1268;
    m.applyCycles = 1268;
    m.applyFailedCycles = 1268;
    m.deferredVector = 3;
    m.deferredTopology = 1;
    m.oldestDeferralAgeMs = 754'000;
    m.applyFailuresTopology = 1268;
    m.lastApplyFailureStage = "topology";
    m.lastApplyFailure = "topology edge identity does not match logical key";
    m.lastApplyFailureAgeMs = 5'000;
    m.publishSkippedCycles = 2;
    m.publishFailedCycles = 1;
    return m;
}

} // namespace

TEST_CASE("p2p status text reports apply health as key=value tokens",
          "[cli][p2p][status][memory-sync]") {
    const auto rendered = yams::cli::formatP2pStatus(stuckMeshNode(), false);
    REQUIRE(rendered.has_value());
    const auto& text = rendered.value();
    const auto firstLine = text.substr(0, text.find('\n'));
    // The first line stays one line of tokens, so existing scrapers keep working.
    CHECK(firstLine.find("records=219") != std::string::npos);
    CHECK(firstLine.find("failed_cycles=0") != std::string::npos);
    CHECK(firstLine.find("apply_cycles=1268") != std::string::npos);
    CHECK(firstLine.find("apply_failed_cycles=1268") != std::string::npos);
    CHECK(firstLine.find(" deferred=4 ") != std::string::npos);
    CHECK(firstLine.find("oldest_deferral_age_ms=754000") != std::string::npos);
    CHECK(firstLine.find("publish_skipped_cycles=2") != std::string::npos);
    CHECK(firstLine.find("publish_failed_cycles=1") != std::string::npos);
    CHECK(text.find("\napply deferred_content=0 deferred_metadata=0 deferred_vector=3 "
                    "deferred_topology=1 failures_content=0 failures_metadata=0 "
                    "failures_vector=0 failures_topology=1268") != std::string::npos);
    CHECK(text.find("\nlast apply failure (5s ago) at topology: topology edge identity") !=
          std::string::npos);
    CHECK(text.find("last inbound failure") == std::string::npos);
}

TEST_CASE("p2p status text omits the apply failure line on a healthy node",
          "[cli][p2p][status][memory-sync]") {
    yams::daemon::MemorySyncResponse m;
    m.started = true;
    m.applyCycles = 10;
    const auto rendered = yams::cli::formatP2pStatus(m, false);
    REQUIRE(rendered.has_value());
    CHECK(rendered.value().find(" deferred=0 ") != std::string::npos);
    CHECK(rendered.value().find("last apply failure") == std::string::npos);
}

TEST_CASE("p2p status JSON nests apply health", "[cli][p2p][status][memory-sync][json]") {
    const auto rendered = yams::cli::formatP2pStatus(stuckMeshNode(), true);
    REQUIRE(rendered.has_value());
    const auto parsed = nlohmann::json::parse(rendered.value());
    REQUIRE(parsed.contains("apply"));
    const auto& apply = parsed.at("apply");
    CHECK(apply.at("cycles") == 1268);
    CHECK(apply.at("failed_cycles") == 1268);
    CHECK(apply.at("deferred").at("total") == 4);
    CHECK(apply.at("deferred").at("vector") == 3);
    CHECK(apply.at("deferred").at("topology") == 1);
    CHECK(apply.at("oldest_deferral_age_ms") == 754'000);
    CHECK(apply.at("failures").at("topology") == 1268);
    CHECK(apply.at("last_failure").at("stage") == "topology");
    CHECK(apply.at("publish_skipped_cycles") == 2);
    CHECK(apply.at("publish_failed_cycles") == 1);
    CHECK(parsed.at("failed_cycles") == 0);
}

TEST_CASE("p2p status refuses a stopped memory sync service", "[cli][p2p][status]") {
    yams::daemon::MemorySyncResponse m;
    m.started = false;
    CHECK_FALSE(yams::cli::formatP2pStatus(m, false).has_value());
}
