// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync.h>
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

struct BackendFixture {
    explicit BackendFixture(const std::string& name) {
        dir = std::filesystem::temp_directory_path() /
              ("yams-memory-sync-" + name + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir);
        config.type = "filesystem";
        config.localPath = dir;
        REQUIRE(backend.initialize(config).has_value());
    }
    ~BackendFixture() {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    std::filesystem::path dir;
    yams::storage::BackendConfig config;
    yams::storage::FilesystemBackend backend;
};

} // namespace

TEST_CASE("MemorySyncLoop publish and read round-trips", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"roundtrip"};
    MemorySyncLoop loop{fixture.backend, "A"};

    REQUIRE(loop.publish("slot", bytes("hello memory")).has_value());
    const auto value = loop.read("slot");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "hello memory");
}

TEST_CASE("MemorySyncLoop converges across two nodes", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"converge"};
    MemorySyncLoop a{fixture.backend, "A"};
    MemorySyncLoop b{fixture.backend, "B"};

    REQUIRE(a.publish("a-key", bytes("from-a")).has_value());
    REQUIRE(b.publish("b-key", bytes("from-b")).has_value());

    const auto mergedA = a.sync();
    const auto mergedB = b.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());

    CHECK(mergedA.value().size() == 2);
    CHECK(mergedB.value().size() == 2);
    CHECK(mergedA.value().count("a-key") == 1);
    CHECK(mergedA.value().count("b-key") == 1);
    CHECK(mergedB.value().count("a-key") == 1);
    CHECK(mergedB.value().count("b-key") == 1);

    // Both nodes see the same winning entry hashes (convergence).
    CHECK(mergedA.value().at("a-key").entryHash == mergedB.value().at("a-key").entryHash);
    CHECK(mergedA.value().at("b-key").entryHash == mergedB.value().at("b-key").entryHash);
}

TEST_CASE("MemorySyncLoop resolves concurrent writes deterministically",
          "[memory-sync][sync-loop]") {
    BackendFixture fixture{"concurrent"};
    MemorySyncLoop a{fixture.backend, "A"};
    MemorySyncLoop b{fixture.backend, "B"};

    REQUIRE(a.publish("contended", bytes("write-a")).has_value());
    REQUIRE(b.publish("contended", bytes("write-b")).has_value());

    const auto mergedA = a.sync();
    const auto mergedB = b.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());

    // Exactly one winner, and both nodes agree on it (deterministic LWW).
    REQUIRE(mergedA.value().count("contended") == 1);
    REQUIRE(mergedB.value().count("contended") == 1);
    CHECK(mergedA.value().at("contended").entryHash == mergedB.value().at("contended").entryHash);
}

TEST_CASE("MemorySyncLoop prefers causally-later writes", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"causal"};
    MemorySyncLoop a{fixture.backend, "A"};
    MemorySyncLoop b{fixture.backend, "B"};

    // A writes first.
    REQUIRE(a.publish("causal-key", bytes("first")).has_value());

    // B observes A's write, then writes a causally-later value.
    REQUIRE(b.sync().has_value());
    REQUIRE(b.publish("causal-key", bytes("second")).has_value());

    // A syncs and must observe B's causally-later value (dominance beats timestamp).
    const auto mergedA = a.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedA.value().count("causal-key") == 1);

    const auto value = a.read("causal-key");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "second");
}
