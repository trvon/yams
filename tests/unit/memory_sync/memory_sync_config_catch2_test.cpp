// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync_config.h>
#include <yams/memory_sync/memory_sync_service.h>

using namespace yams::memory_sync;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

} // namespace

TEST_CASE("MemorySyncDaemonConfig parses the [memory_sync] TOML section", "[memory-sync][config]") {
    const std::map<std::string, std::string> flat = {
        {"memory_sync.enabled", "true"},         {"memory_sync.node_id", "node-a"},
        {"memory_sync.backend", "filesystem"},   {"memory_sync.path", "/tmp/mem"},
        {"memory_sync.sync_interval_ms", "250"},
    };
    const auto cfg = MemorySyncDaemonConfig::fromToml(flat);
    CHECK(cfg.enabled);
    CHECK(cfg.nodeId == "node-a");
    CHECK(cfg.backend == "filesystem");
    CHECK(cfg.path == "/tmp/mem");
    CHECK(cfg.syncIntervalMs == 250);
}

TEST_CASE("MemorySyncDaemonConfig defaults to disabled", "[memory-sync][config]") {
    const auto cfg = MemorySyncDaemonConfig::fromToml({});
    CHECK_FALSE(cfg.enabled);
    CHECK(cfg.backend == "filesystem");
    CHECK(cfg.syncIntervalMs == 5000);
}

TEST_CASE("createMemorySyncService rejects disabled config", "[memory-sync][config]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = false;
    const auto svc = createMemorySyncService(cfg);
    CHECK_FALSE(svc.has_value());
}

TEST_CASE("createMemorySyncService rejects unknown backend", "[memory-sync][config]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.backend = "ftp";
    cfg.path = "ftp://example.com";
    const auto svc = createMemorySyncService(cfg);
    CHECK_FALSE(svc.has_value());
}

TEST_CASE("createMemorySyncService rejects s3 with a non-s3 URL", "[memory-sync][config]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.backend = "s3";
    cfg.path = "/not/an/s3/url";
    const auto svc = createMemorySyncService(cfg);
    CHECK_FALSE(svc.has_value());
}

TEST_CASE("createMemorySyncService builds a working filesystem service", "[memory-sync][config]") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams-memory-sync-config-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);

    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.nodeId = "node-a";
    cfg.backend = "filesystem";
    cfg.path = dir.string();
    cfg.syncIntervalMs = 50;

    auto svc = createMemorySyncService(cfg);
    REQUIRE(svc.has_value());

    REQUIRE(svc.value()->publish("key", bytes("value")).has_value());
    const auto value = svc.value()->read("key");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "value");

    svc.value()->stop();

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
