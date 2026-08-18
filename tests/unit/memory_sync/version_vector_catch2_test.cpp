// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <limits>

#include <yams/memory_sync/version_vector.h>

using namespace yams::memory_sync;

TEST_CASE("VersionVector merges element-wise max", "[memory-sync][version-vector]") {
    VersionVector a;
    a.increment("A");
    a.increment("A");

    VersionVector b;
    b.increment("B");
    b.increment("B");
    b.increment("B");

    a.merge(b);
    CHECK(a.get("A") == 2);
    CHECK(a.get("B") == 3);

    // Merging a vector that is strictly behind is a no-op for the union.
    VersionVector c;
    c.increment("B");
    a.merge(c);
    CHECK(a.get("B") == 3);
}

TEST_CASE("VersionVector dominates detects causal ordering", "[memory-sync][version-vector]") {
    VersionVector first;
    first.increment("A");

    VersionVector later;
    later.merge(first);
    later.increment("B");

    CHECK(later.dominates(first));
    CHECK_FALSE(first.dominates(later));
    CHECK_FALSE(later.concurrent(first));
    CHECK_FALSE(first.concurrent(later));
}

TEST_CASE("VersionVector concurrent detects independent writes", "[memory-sync][version-vector]") {
    VersionVector a;
    a.increment("A");

    VersionVector b;
    b.increment("B");

    CHECK_FALSE(a.dominates(b));
    CHECK_FALSE(b.dominates(a));
    CHECK(a.concurrent(b));
}

TEST_CASE("VersionVector equal vectors neither dominate nor are concurrent-before-merge",
          "[memory-sync][version-vector]") {
    VersionVector a;
    a.increment("A");

    VersionVector b;
    b.increment("A");

    CHECK_FALSE(a.dominates(b));
    CHECK_FALSE(b.dominates(a));
}

TEST_CASE("lwwWins prefers causally-later writes over timestamp", "[memory-sync][lww]") {
    MemoryIndexRecord older;
    older.entryHash = std::string(64, 'a');
    older.ts.physicalMs = 9999; // newer wall clock, but causally behind
    older.origin = "A";
    older.vv.increment("A");

    MemoryIndexRecord later;
    later.entryHash = std::string(64, 'b');
    later.ts.physicalMs = 1; // older wall clock, but causally after
    later.origin = "B";
    later.vv.merge(older.vv);
    later.vv.increment("B");

    // Causality beats the timestamp: `later` dominates `older`.
    CHECK(lwwWins(later, older));
    CHECK_FALSE(lwwWins(older, later));
}

TEST_CASE("lwwWins resolves concurrent writes deterministically by timestamp",
          "[memory-sync][lww]") {
    MemoryIndexRecord a;
    a.entryHash = std::string(64, 'a');
    a.ts.physicalMs = 100;
    a.origin = "A";
    a.vv.increment("A");

    MemoryIndexRecord b;
    b.entryHash = std::string(64, 'b');
    b.ts.physicalMs = 200;
    b.origin = "B";
    b.vv.increment("B");

    // Concurrent (neither dominates): higher timestamp wins.
    CHECK(lwwWins(b, a));
    CHECK_FALSE(lwwWins(a, b));
}

TEST_CASE("lwwWins breaks equal timestamps by origin deterministically", "[memory-sync][lww]") {
    MemoryIndexRecord a;
    a.entryHash = std::string(64, 'a');
    a.ts.physicalMs = 100;
    a.ts.logical = 0;
    a.origin = "A";
    a.vv.increment("A");

    MemoryIndexRecord b;
    b.entryHash = std::string(64, 'b');
    b.ts.physicalMs = 100;
    b.ts.logical = 0;
    b.origin = "B";
    b.vv.increment("B");

    // Equal (physical, logical): higher origin string wins, deterministically.
    CHECK(lwwWins(b, a));
    CHECK_FALSE(lwwWins(a, b));
}

TEST_CASE("LWW resolver rejects equal-vector payload forks", "[memory-sync][lww][identity]") {
    MemoryIndexRecord a;
    a.entryHash = std::string(64, 'a');
    a.ts.physicalMs = 100;
    a.origin = "writer";
    a.vv.increment("writer");

    MemoryIndexRecord b = a;
    b.entryHash = std::string(64, 'b');
    b.ts.physicalMs = 200;

    CHECK(resolveLww(a, b) == LwwDecision::Fork);
    CHECK(resolveLww(b, a) == LwwDecision::Fork);
}

TEST_CASE("LWW resolver gives concurrent tombstones delete-wins semantics",
          "[memory-sync][lww][tombstone]") {
    MemoryIndexRecord update;
    update.entryHash = std::string(64, 'a');
    update.recordKind = std::string(kMemoryValueRecordKind);
    update.origin = "update-writer";
    update.vv.increment(update.origin);

    MemoryIndexRecord deletion;
    deletion.recordKind = std::string(kMemoryTombstoneRecordKind);
    deletion.origin = "delete-writer";
    deletion.vv.increment(deletion.origin);

    CHECK(resolveLww(deletion, update) == LwwDecision::First);
    CHECK(resolveLww(update, deletion) == LwwDecision::Second);
}

TEST_CASE("VersionVector rejects counter overflow", "[memory-sync][version-vector][identity]") {
    const nlohmann::json encoded = {
        {"counters_", {{"writer", std::numeric_limits<std::uint64_t>::max()}}},
    };
    auto vector = encoded.get<VersionVector>();

    CHECK_FALSE(vector.increment("writer"));
    CHECK(vector.get("writer") == std::numeric_limits<std::uint64_t>::max());
}

TEST_CASE("MemoryIndexRecord serializes its content-addressed causal envelope",
          "[memory-sync][serialization]") {
    MemoryIndexRecord record;
    record.entryHash = std::string(64, 'a');
    record.ts.physicalMs = 1700000000000ULL;
    record.ts.logical = 7;
    record.origin = "node-1";
    record.vv.increment("node-1");
    record.vv.increment("node-1");
    record.corpusId = "corpus-a";
    record.corpusEpoch = 7;
    record.logicalKey = "user/slot";
    record.recordKind = "value";
    record.operationId = "node-1:2";

    const nlohmann::json json = record;
    const auto decoded = json.get<MemoryIndexRecord>();

    CHECK(json.at("schemaVersion") == 3);
    CHECK(json.contains("entryHash"));
    CHECK(json.contains("ts"));
    CHECK(json.contains("origin"));
    CHECK(json.contains("vv"));
    CHECK(json.at("corpusId") == "corpus-a");
    CHECK(json.at("corpusEpoch") == 7);
    CHECK(json.at("logicalKey") == "user/slot");
    CHECK(json.at("recordKind") == "value");
    CHECK(json.at("operationId") == "node-1:2");
    CHECK_FALSE(json.contains("signatureAlgorithm"));
    CHECK_FALSE(json.contains("signingKeyId"));
    CHECK_FALSE(json.contains("signature"));
    CHECK(decoded.entryHash == record.entryHash);
    CHECK(decoded.ts == record.ts);
    CHECK(decoded.origin == record.origin);
    CHECK(decoded.vv.get("node-1") == 2);
    CHECK(decoded.hasValidIdentity("corpus-a", 7, "user/slot"));
}

TEST_CASE("MemoryIndexRecord rejects mismatched origin vector and operation identity",
          "[memory-sync][serialization][identity]") {
    MemoryIndexRecord record;
    record.entryHash = std::string(64, 'a');
    record.origin = "writer-a";
    record.vv.increment("writer-b");
    record.corpusId = "corpus-a";
    record.corpusEpoch = 1;
    record.logicalKey = "user/key";
    record.recordKind = "value";
    record.operationId = "writer-a:1";

    CHECK_FALSE(record.hasValidIdentity("corpus-a", 1, "user/key"));
    REQUIRE_THROWS_AS(nlohmann::json(record), std::invalid_argument);
}

TEST_CASE("MemoryIndexRecord decodes legacy and unversioned envelope shapes",
          "[memory-sync][serialization][compatibility]") {
    const auto digest = std::string(64, 'a');
    const nlohmann::json legacy = {
        {"entryHash", digest},
        {"ts", {{"physicalMs", 123}, {"logical", 4}, {"origin", "legacy-node"}}},
        {"version", {{"counters_", {{"legacy-node", 7}}}}},
    };
    const auto decodedLegacy = legacy.get<MemoryIndexRecord>();
    CHECK(decodedLegacy.entryHash == digest);
    CHECK(decodedLegacy.ts == HybridTs{123, 4});
    CHECK(decodedLegacy.origin == "legacy-node");
    CHECK(decodedLegacy.vv.get("legacy-node") == 7);

    const nlohmann::json unversioned = {
        {"entryHash", digest},
        {"ts", {{"physicalMs", 456}, {"logical", 8}}},
        {"origin", "worktree-node"},
        {"vv", {{"counters_", {{"worktree-node", 9}}}}},
    };
    const auto decodedUnversioned = unversioned.get<MemoryIndexRecord>();
    CHECK(decodedUnversioned.ts == HybridTs{456, 8});
    CHECK(decodedUnversioned.origin == "worktree-node");
    CHECK(decodedUnversioned.vv.get("worktree-node") == 9);
}

TEST_CASE("MemoryIndexRecord rejects unknown schema versions",
          "[memory-sync][serialization][compatibility]") {
    const nlohmann::json unknown = {
        {"schemaVersion", 99},
        {"entryHash", std::string(64, 'a')},
        {"ts", {{"physicalMs", 1}, {"logical", 0}}},
        {"origin", "node-1"},
        {"vv", {{"counters_", {{"node-1", 1}}}}},
    };
    REQUIRE_THROWS_AS(unknown.get<MemoryIndexRecord>(), std::invalid_argument);
}

TEST_CASE("MemoryIndexRecord rejects non-sha256 blob keys", "[memory-sync][serialization]") {
    nlohmann::json invalid = {
        {"entryHash", "not-a-sha256"},
        {"ts", {{"physicalMs", 1}, {"logical", 0}}},
        {"origin", "node-1"},
        {"vv", {{"counters_", {{"node-1", 1}}}}},
    };

    REQUIRE_THROWS_AS(invalid.get<MemoryIndexRecord>(), std::invalid_argument);
}

TEST_CASE("lwwWins fails closed instead of ordering equal-vector payloads",
          "[memory-sync][lww][identity]") {
    MemoryIndexRecord a;
    a.entryHash = std::string(63, 'a') + "b";
    a.ts = {.physicalMs = 100, .logical = 7};
    a.origin = "node";
    a.vv.increment("node");

    MemoryIndexRecord b = a;
    b.entryHash = std::string(63, 'a') + "c";

    CHECK(resolveLww(a, b) == LwwDecision::Fork);
    CHECK(resolveLww(b, a) == LwwDecision::Fork);
    CHECK_FALSE(lwwWins(a, b));
    CHECK_FALSE(lwwWins(b, a));
}
