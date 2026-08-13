// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include <yams/memory_sync/memory_sync_cli.h>
#include <yams/memory_sync/memory_sync_config.h>
#include <yams/memory_sync/memory_sync_service.h>

using namespace yams::memory_sync;

namespace {

std::unique_ptr<MemorySyncService> makeService(const std::filesystem::path& dir) {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.nodeId = "node-a";
    cfg.backend = "filesystem";
    cfg.path = dir.string();
    cfg.syncIntervalMs = 50;
    auto svc = createMemorySyncService(cfg);
    REQUIRE(svc.has_value());
    return std::move(svc.value());
}

struct TempDir {
    std::filesystem::path path;
    TempDir() {
        path = std::filesystem::temp_directory_path() /
               ("yams-p2p-cli-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

} // namespace

TEST_CASE("p2p publish/read round-trips a literal value", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    auto pub = publishValue(*svc, "greeting", "hello-world", /*json=*/false);
    REQUIRE(pub.has_value());
    CHECK(pub.value().find("published greeting") != std::string::npos);

    auto rd = readValue(*svc, "greeting", /*json=*/false);
    REQUIRE(rd.has_value());
    CHECK(rd.value() == "hello-world");
}

TEST_CASE("p2p publish reads a @file value", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    const auto file = dir.path / "payload.bin";
    {
        std::ofstream out(file, std::ios::binary);
        out << "file-bytes";
    }

    auto pub = publishValue(*svc, "blob", "@" + file.string(), /*json=*/false);
    REQUIRE(pub.has_value());

    auto rd = readValue(*svc, "blob", /*json=*/false);
    REQUIRE(rd.has_value());
    CHECK(rd.value() == "file-bytes");
}

TEST_CASE("p2p publish/read emit JSON", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    auto pub = publishValue(*svc, "k", "v", /*json=*/true);
    REQUIRE(pub.has_value());
    CHECK(pub.value().find("\"published\": true") != std::string::npos);

    auto rd = readValue(*svc, "k", /*json=*/true);
    REQUIRE(rd.has_value());
    CHECK(rd.value().find("\"value_hex\": \"76\"") != std::string::npos);
}

TEST_CASE("p2p read of missing key returns error", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    auto rd = readValue(*svc, "nope", /*json=*/false);
    CHECK_FALSE(rd.has_value());
}

TEST_CASE("p2p status reports backend + merged record count", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    REQUIRE(publishValue(*svc, "a", "1", false).has_value());
    REQUIRE(publishValue(*svc, "b", "2", false).has_value());

    auto st = statusSummary(*svc, "filesystem", "node-a", /*json=*/false);
    REQUIRE(st.has_value());
    CHECK(st.value().find("backend=filesystem") != std::string::npos);
    CHECK(st.value().find("node_id=node-a") != std::string::npos);
    CHECK(st.value().find("records=2") != std::string::npos);

    auto stJson = statusSummary(*svc, "filesystem", "node-a", /*json=*/true);
    REQUIRE(stJson.has_value());
    CHECK(stJson.value().find("\"records\": 2") != std::string::npos);
}

TEST_CASE("p2p read JSON hex-encodes binary bytes", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    // 0xff is invalid UTF-8 and would make nlohmann::json::dump throw if embedded raw.
    const std::vector<std::byte> binary = {std::byte{0xff}, std::byte{0x00}, std::byte{0x7f}};
    REQUIRE(svc->publish("bin", binary).has_value());

    auto rd = readValue(*svc, "bin", /*json=*/true);
    REQUIRE(rd.has_value());
    CHECK(rd.value().find("\"value_hex\": \"ff007f\"") != std::string::npos);
    CHECK(rd.value().find("\"size\": 3") != std::string::npos);
}

TEST_CASE("p2p publish/read an empty value", "[memory-sync][cli]") {
    TempDir dir;
    auto svc = makeService(dir.path);

    auto pub = publishValue(*svc, "empty", "", /*json=*/false);
    REQUIRE(pub.has_value());
    CHECK(pub.value().find("0 bytes") != std::string::npos);

    auto rd = readValue(*svc, "empty", /*json=*/false);
    REQUIRE(rd.has_value());
    CHECK(rd.value().empty());
}
