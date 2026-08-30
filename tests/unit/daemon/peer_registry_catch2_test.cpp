// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/p2p/peer_registry.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <latch>
#include <memory>
#include <string>
#include <vector>

using yams::daemon::p2p::PeerRegistry;

namespace {

struct TempDatabase {
    explicit TempDatabase(std::string name) {
        directory = std::filesystem::temp_directory_path() /
                    ("yams-peer-registry-" + std::move(name) + "-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(directory);
        path = directory / "metadata.db";
    }
    ~TempDatabase() {
        std::error_code error;
        std::filesystem::remove_all(directory, error);
    }
    std::filesystem::path directory;
    std::filesystem::path path;
};

} // namespace

TEST_CASE("P2P peer registry migrates legacy trust schema", "[daemon][p2p][registry][sqlite]") {
    TempDatabase database{"migration"};
    {
        yams::metadata::Database legacy;
        REQUIRE(legacy.open(database.path.string(), yams::metadata::ConnectionMode::Create)
                    .has_value());
        REQUIRE(legacy
                    .execute(R"sql(
CREATE TABLE p2p_peers (
    node_id TEXT PRIMARY KEY NOT NULL,
    spki_hash TEXT NOT NULL,
    corpus_id TEXT NOT NULL DEFAULT '',
    corpus_epoch INTEGER NOT NULL DEFAULT 0,
    last_seen_vv TEXT NOT NULL DEFAULT '{"counters_":{}}',
    last_connected_ms INTEGER NOT NULL DEFAULT 0,
    pinned_by_operator INTEGER NOT NULL DEFAULT 0
)
)sql")
                    .has_value());
        auto insert =
            legacy.prepare("INSERT INTO p2p_peers(node_id, spki_hash, pinned_by_operator) "
                           "VALUES('legacy-peer', ?, 1)");
        REQUIRE(insert.has_value());
        auto statement = std::move(insert.value());
        REQUIRE(statement.bind(1, std::string(64, 'a')).has_value());
        REQUIRE(statement.execute().has_value());
    }

    auto opened = PeerRegistry::open(database.path, 8);
    REQUIRE(opened.has_value());
    auto peers = opened.value()->listPeers();
    REQUIRE(peers.has_value());
    REQUIRE(peers.value().size() == 1);
    CHECK(peers.value().front().endpoint.empty());
    CHECK_FALSE(peers.value().front().remembered);
    CHECK_FALSE(peers.value().front().pinnedByOperator);
    auto legacyTrust = opened.value()->verifyOrPin("legacy-peer", std::string(64, 'a'), false);
    REQUIRE(legacyTrust.has_value());
    CHECK_FALSE(legacyTrust.value().firstContactPinned);
    CHECK_FALSE(legacyTrust.value().pinnedByOperator);
    REQUIRE(opened.value()->enrollOperatorPeer("legacy-peer", std::string(64, 'a')).has_value());
    auto reenrolled = opened.value()->verifyOrPin("legacy-peer", std::string(64, 'a'), false);
    REQUIRE(reenrolled.has_value());
    CHECK(reenrolled.value().pinnedByOperator);
    yams::memory_sync::VersionVector version;
    REQUIRE(opened.value()
                ->updatePeerState("legacy-peer", "corpus", 1, version, 7, "host:9721", true)
                .has_value());
}

TEST_CASE("P2P peer registry persists operator enrollment",
          "[daemon][p2p][registry][sqlite][security]") {
    TempDatabase database{"operator-enrollment"};
    auto opened = PeerRegistry::open(database.path, 1);
    REQUIRE(opened.has_value());
    auto& registry = *opened.value();
    const std::string pin(64, 'a');

    REQUIRE(registry.enrollOperatorPeer("peer-node", std::string(64, 'A')).has_value());
    REQUIRE(registry.enrollOperatorPeer("peer-node", pin).has_value());
    auto replacement = registry.enrollOperatorPeer("peer-node", std::string(64, 'b'));
    REQUIRE_FALSE(replacement.has_value());
    CHECK(replacement.error().code == yams::ErrorCode::Unauthorized);
    auto full = registry.enrollOperatorPeer("other-node", std::string(64, 'c'));
    REQUIRE_FALSE(full.has_value());
    CHECK(full.error().code == yams::ErrorCode::ResourceExhausted);

    auto peers = registry.listPeers();
    REQUIRE(peers.has_value());
    REQUIRE(peers.value().size() == 1);
    CHECK(peers.value().front().nodeId == "peer-node");
    CHECK(peers.value().front().spkiPin == pin);
    CHECK(peers.value().front().pinnedByOperator);
    CHECK_FALSE(peers.value().front().remembered);
    auto trusted = registry.verifyOrPin("peer-node", pin, false);
    REQUIRE(trusted.has_value());
    CHECK(trusted.value().pinnedByOperator);
}

TEST_CASE("P2P peer registry persists trust and causal state across restart",
          "[daemon][p2p][registry][sqlite]") {
    TempDatabase database{"restart"};
    const std::string pin(64, 'a');
    yams::memory_sync::VersionVector version;
    REQUIRE(version.increment("peer-node"));
    REQUIRE(version.increment("peer-node"));

    {
        auto opened = PeerRegistry::open(database.path, 8);
        REQUIRE(opened.has_value());
        auto& registry = *opened.value();
        auto unknown = registry.verifyOrPin("peer-node", pin, false);
        REQUIRE_FALSE(unknown.has_value());
        CHECK(unknown.error().code == yams::ErrorCode::Unauthorized);
        auto first = registry.verifyOrPin("peer-node", std::string(64, 'A'), true);
        REQUIRE(first.has_value());
        CHECK(first.value().firstContactPinned);
        CHECK_FALSE(first.value().pinnedByOperator);
        REQUIRE(registry.enrollOperatorPeer("peer-node", pin).has_value());
        auto promoted = registry.verifyOrPin("peer-node", pin, false);
        REQUIRE(promoted.has_value());
        CHECK(promoted.value().pinnedByOperator);
        auto firstUpdate =
            registry.updatePeerState("peer-node", "corpus", 7, version, 123456, {}, false);
        const std::string firstUpdateError =
            firstUpdate ? std::string{} : firstUpdate.error().message;
        INFO(firstUpdateError);
        REQUIRE(firstUpdate.has_value());
        // A later non-operator update must not downgrade the pin source.
        REQUIRE(registry
                    .updatePeerState("peer-node", "corpus", 7, version, 123999,
                                     "ghost.example:9721", true)
                    .has_value());
    }

    auto reopened = PeerRegistry::open(database.path, 8);
    REQUIRE(reopened.has_value());
    auto& registry = *reopened.value();
    CHECK(registry.pinnedPin("peer-node") == pin);
    auto known = registry.verifyOrPin("peer-node", pin, false);
    REQUIRE(known.has_value());
    CHECK_FALSE(known.value().firstContactPinned);
    CHECK(known.value().pinnedByOperator);
    auto changed = registry.verifyOrPin("peer-node", std::string(64, 'b'), true);
    REQUIRE_FALSE(changed.has_value());
    CHECK(changed.error().code == yams::ErrorCode::Unauthorized);

    auto peers = registry.listPeers();
    REQUIRE(peers.has_value());
    REQUIRE(peers.value().size() == 1);
    const auto& peer = peers.value().front();
    CHECK(peer.nodeId == "peer-node");
    CHECK(peer.spkiPin == pin);
    CHECK(peer.corpusId == "corpus");
    CHECK(peer.corpusEpoch == 7);
    CHECK(peer.lastSeenVersion.get("peer-node") == 2);
    CHECK(peer.lastConnectedMs == 123999);
    CHECK(peer.endpoint == "ghost.example:9721");
    CHECK(peer.remembered);
    CHECK(peer.pinnedByOperator);
}

TEST_CASE("P2P peer registry enforces capacity and supports removal",
          "[daemon][p2p][registry][sqlite]") {
    TempDatabase database{"capacity"};
    auto opened = PeerRegistry::open(database.path, 2);
    REQUIRE(opened.has_value());
    auto& registry = *opened.value();
    REQUIRE(registry.verifyOrPin("one", std::string(64, '1'), true).has_value());
    REQUIRE(registry.verifyOrPin("two", std::string(64, '2'), true).has_value());
    auto full = registry.verifyOrPin("three", std::string(64, '3'), true);
    REQUIRE_FALSE(full.has_value());
    CHECK(full.error().code == yams::ErrorCode::ResourceExhausted);

    REQUIRE(registry.removePeer("one").has_value());
    CHECK_FALSE(registry.pinnedPin("one").has_value());
    REQUIRE(registry.verifyOrPin("three", std::string(64, '3'), true).has_value());
    auto peers = registry.listPeers();
    REQUIRE(peers.has_value());
    CHECK(peers.value().size() == 2);
}

TEST_CASE("P2P peer registry serializes concurrent first contact",
          "[daemon][p2p][registry][sqlite][concurrency]") {
    TempDatabase database{"concurrency"};
    auto opened = PeerRegistry::open(database.path, 8);
    REQUIRE(opened.has_value());
    auto& registry = *opened.value();
    const std::string pin(64, 'c');
    constexpr std::size_t kThreads = 8;
    std::latch start(1);
    std::atomic<std::size_t> successes{0};
    std::atomic<std::size_t> firstContacts{0};
    std::vector<yams::compat::jthread> workers;
    workers.reserve(kThreads);
    for (std::size_t index = 0; index < kThreads; ++index) {
        workers.emplace_back([&] {
            start.wait();
            auto result = registry.verifyOrPin("shared-peer", pin, true);
            if (result) {
                successes.fetch_add(1, std::memory_order_relaxed);
                if (result.value().firstContactPinned) {
                    firstContacts.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    start.count_down();
    workers.clear();

    CHECK(successes.load(std::memory_order_relaxed) == kThreads);
    CHECK(firstContacts.load(std::memory_order_relaxed) == 1);
    auto peers = registry.listPeers();
    REQUIRE(peers.has_value());
    CHECK(peers.value().size() == 1);
}
