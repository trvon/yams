// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

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
    older.entryHash = "older";
    older.ts.physicalMs = 9999; // newer wall clock, but causally behind
    older.ts.origin = "A";
    older.version.increment("A");

    MemoryIndexRecord later;
    later.entryHash = "later";
    later.ts.physicalMs = 1; // older wall clock, but causally after
    later.ts.origin = "B";
    later.version.merge(older.version);
    later.version.increment("B");

    // Causality beats the timestamp: `later` dominates `older`.
    CHECK(lwwWins(later, older));
    CHECK_FALSE(lwwWins(older, later));
}

TEST_CASE("lwwWins resolves concurrent writes deterministically by timestamp",
          "[memory-sync][lww]") {
    MemoryIndexRecord a;
    a.entryHash = "a";
    a.ts.physicalMs = 100;
    a.ts.origin = "A";
    a.version.increment("A");

    MemoryIndexRecord b;
    b.entryHash = "b";
    b.ts.physicalMs = 200;
    b.ts.origin = "B";
    b.version.increment("B");

    // Concurrent (neither dominates): higher timestamp wins.
    CHECK(lwwWins(b, a));
    CHECK_FALSE(lwwWins(a, b));
}

TEST_CASE("lwwWins breaks equal timestamps by origin deterministically", "[memory-sync][lww]") {
    MemoryIndexRecord a;
    a.entryHash = "a";
    a.ts.physicalMs = 100;
    a.ts.logical = 0;
    a.ts.origin = "A";
    a.version.increment("A");

    MemoryIndexRecord b;
    b.entryHash = "b";
    b.ts.physicalMs = 100;
    b.ts.logical = 0;
    b.ts.origin = "B";
    b.version.increment("B");

    // Equal (physical, logical): higher origin string wins, deterministically.
    CHECK(lwwWins(b, a));
    CHECK_FALSE(lwwWins(a, b));
}

TEST_CASE("MemoryIndexRecord serializes round-trip", "[memory-sync][serialization]") {
    MemoryIndexRecord record;
    record.entryHash = "deadbeef";
    record.ts.physicalMs = 1700000000000ULL;
    record.ts.logical = 7;
    record.ts.origin = "node-1";
    record.version.increment("node-1");
    record.version.increment("node-1");

    const nlohmann::json json = record;
    const auto decoded = json.get<MemoryIndexRecord>();

    CHECK(decoded.entryHash == record.entryHash);
    CHECK(decoded.ts == record.ts);
    CHECK(decoded.version.get("node-1") == 2);
}
