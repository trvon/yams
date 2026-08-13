// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_backend.h>

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

std::unique_ptr<yams::storage::FilesystemBackend> makeBackend(const std::filesystem::path& dir) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = dir;
    auto backend = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

struct TempDirGuard {
    TempDirGuard() {
        path = std::filesystem::temp_directory_path() /
               ("yams-memory-sync-service-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::filesystem::path path;
};

} // namespace

TEST_CASE("MemorySyncService start/stop is idempotent and clean", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};

    REQUIRE(service.start().has_value());
    CHECK(service.started());
    REQUIRE(service.start().has_value()); // idempotent

    service.stop();
    CHECK_FALSE(service.started());
    service.stop(); // idempotent, noexcept
    CHECK_FALSE(service.started());
}

TEST_CASE("MemorySyncService publish and read round-trips", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService service{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};

    REQUIRE(service.publish("slot", bytes("hello service")).has_value());
    const auto value = service.read("slot");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "hello service");
}

TEST_CASE("MemorySyncService two nodes converge", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService a{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};
    MemorySyncService b{makeBackend(tmp.path), MemorySyncConfig{"B", 50}};

    REQUIRE(a.publish("a-key", bytes("from-a")).has_value());
    REQUIRE(b.publish("b-key", bytes("from-b")).has_value());

    const auto mergedA = a.syncOnce();
    const auto mergedB = b.syncOnce();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());

    CHECK(mergedA.value().count("a-key") == 1);
    CHECK(mergedB.value().count("b-key") == 1);
    CHECK(mergedA.value().at("a-key").entryHash == mergedB.value().at("a-key").entryHash);
}

TEST_CASE("MemorySyncService periodic worker syncs without hanging", "[memory-sync][service]") {
    TempDirGuard tmp;
    MemorySyncService a{makeBackend(tmp.path), MemorySyncConfig{"A", 50}};
    MemorySyncService b{makeBackend(tmp.path), MemorySyncConfig{"B", 50}};

    REQUIRE(a.start().has_value());
    REQUIRE(b.start().has_value());

    REQUIRE(a.publish("periodic-key", bytes("via-periodic-sync")).has_value());

    // B's periodic worker should converge within a bounded deadline.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    bool converged = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto value = b.read("periodic-key");
        if (value.has_value() && text(value.value()) == "via-periodic-sync") {
            converged = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    CHECK(converged);

    a.stop();
    b.stop();
    CHECK_FALSE(a.started());
    CHECK_FALSE(b.started());
}
