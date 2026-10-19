// SPDX-License-Identifier: GPL-3.0-or-later
#include <catch2/catch_test_macros.hpp>

#include "../../../src/memory_sync/sweep_store.h"

#include <array>
#include <limits>
#include <stdexcept>
#include <vector>

using yams::memory_sync::detail::SweepStore;

TEST_CASE("Sweep spill retains exact fingerprints and every equal-vector fork",
          "[memory-sync][spill][security]") {
    auto created = SweepStore::create(1024 * 1024, {});
    REQUIRE(created.has_value());
    auto store = created.value();
    REQUIRE(store->observeOperation("op", "hash-a", "correct").has_value());
    REQUIRE(store->observeOperation("op", "hash-a", "misplaced").has_value());
    auto original = store->operation("op");
    REQUIRE(original.has_value());
    REQUIRE(original.value().has_value());
    CHECK(original.value()->first == "hash-a");
    CHECK(original.value()->second == "correct");
    auto forked = store->forked("op");
    REQUIRE(forked.has_value());
    CHECK(forked.value());
    for (const auto& id : {"a", "b", "c"}) {
        REQUIRE(store->observeVector("same-vector", std::string("hash-") + id, id).has_value());
    }
    for (const auto& id : {"a", "b", "c"}) {
        auto conflict = store->forked(id);
        REQUIRE(conflict.has_value());
        CHECK(conflict.value());
    }
    const std::string first{"vv\0a", 4};
    const std::string second{"vv\0b", 4};
    REQUIRE(store->observeVector(first, "one", "nul-a").has_value());
    REQUIRE(store->observeVector(second, "two", "nul-b").has_value());
    auto distinct = store->forked("nul-a");
    REQUIRE(distinct.has_value());
    CHECK_FALSE(distinct.value());
    const std::string opA{"op\0a", 4};
    const std::string opB{"op\0b", 4};
    REQUIRE(store->observeOperation(opA, "one", "key-a").has_value());
    REQUIRE(store->observeOperation(opB, "two", "key-b").has_value());
    auto exact = store->operation(opB);
    REQUIRE(exact.has_value());
    REQUIRE(exact.value().has_value());
    CHECK(exact.value()->second == "key-b");
}

TEST_CASE("Sweep spill streams full unsigned counter order and permits nested lookups",
          "[memory-sync][spill][ordering]") {
    auto created = SweepStore::create(1024 * 1024, {});
    REQUIRE(created.has_value());
    auto store = created.value();
    const std::array<std::uint64_t, 5> counters{UINT64_MAX, 1, std::uint64_t{1} << 63,
                                                (std::uint64_t{1} << 63) - 1, 0};
    const std::array<std::byte, 1> payload{std::byte{42}};
    REQUIRE(store->observeOperation("nested", "hash", "logical").has_value());
    for (std::size_t i = 0; i < counters.size(); ++i) {
        REQUIRE(
            store
                ->addCandidate(std::to_string(i), "logical", "hash", "writer", counters[i], payload)
                .has_value());
    }
    std::vector<std::string> seen;
    auto visited =
        store->forEach(true,
                       [&](std::string_view index, std::string_view logical, std::string_view hash,
                           std::span<const std::byte> encoded) -> yams::Result<void> {
                           const auto saved = std::string(index);
                           auto lookup = store->operation("nested");
                           REQUIRE(lookup.has_value());
                           REQUIRE(lookup.value().has_value());
                           auto conflict = store->forked("nested");
                           REQUIRE(conflict.has_value());
                           CHECK_FALSE(conflict.value());
                           CHECK(index == saved);
                           CHECK(logical == "logical");
                           CHECK(hash == "hash");
                           REQUIRE(encoded.size() == 1);
                           CHECK(encoded[0] == std::byte{42});
                           seen.emplace_back(index);
                           return {};
                       });
    REQUIRE(visited.has_value());
    CHECK(seen == std::vector<std::string>{"4", "1", "3", "2", "0"});
    seen.clear();
    REQUIRE(store
                ->forEach(false,
                          [&](std::string_view index, auto, auto, auto) -> yams::Result<void> {
                              seen.emplace_back(index);
                              return {};
                          })
                .has_value());
    CHECK(seen == std::vector<std::string>{"0", "1", "2", "3", "4"});
}

TEST_CASE("Sweep spill admission failure poisons only its disposable generation",
          "[memory-sync][spill][failure]") {
    bool admitted = true;
    auto check = [&]() -> yams::Result<void> {
        return admitted ? yams::Result<void>{}
                        : yams::Result<void>{yams::Error{yams::ErrorCode::ResourceExhausted,
                                                         "test admission denied"}};
    };
    auto created = SweepStore::create(1024 * 1024, check);
    REQUIRE(created.has_value());
    auto store = created.value();
    REQUIRE(store->observeOperation("old", "hash", "logical").has_value());
    admitted = false;
    auto denied = store->operation("old");
    REQUIRE_FALSE(denied.has_value());
    CHECK(denied.error().code == yams::ErrorCode::ResourceExhausted);
    admitted = true;
    CHECK_FALSE(store->operation("old").has_value());
    auto fresh = SweepStore::create(1024 * 1024, check);
    REQUIRE(fresh.has_value());
    auto absent = fresh.value()->operation("old");
    REQUIRE(absent.has_value());
    CHECK_FALSE(absent.value().has_value());
}

TEST_CASE("Sweep spill row streaming preserves cancellation and thrown-check errors",
          "[memory-sync][spill][failure]") {
    bool cancel = false;
    bool throws = false;
    auto check = [&]() -> yams::Result<void> {
        if (throws) {
            throw std::runtime_error("injected admission exception");
        }
        return cancel ? yams::Result<void>{yams::Error{yams::ErrorCode::OperationCancelled,
                                                       "injected cancellation"}}
                      : yams::Result<void>{};
    };
    for (const bool useException : {false, true}) {
        cancel = false;
        throws = false;
        auto created = SweepStore::create(1024 * 1024, check);
        REQUIRE(created.has_value());
        const std::array<std::byte, 1> payload{};
        REQUIRE(created.value()->addCandidate("a", "k", "h", "w", 1, payload).has_value());
        REQUIRE(created.value()->addCandidate("b", "k", "h", "w", 2, payload).has_value());
        int rows = 0;
        auto interrupted =
            created.value()->forEach(false, [&](auto, auto, auto, auto) -> yams::Result<void> {
                ++rows;
                throws = useException;
                cancel = !useException;
                return {};
            });
        REQUIRE_FALSE(interrupted.has_value());
        CHECK(rows == 1);
        CHECK(interrupted.error().code == (useException ? yams::ErrorCode::InternalError
                                                        : yams::ErrorCode::OperationCancelled));
    }
}

TEST_CASE("Sweep spill disk bound fails closed and does not survive a new generation",
          "[memory-sync][spill][failure]") {
    auto created = SweepStore::create(1, {});
    REQUIRE(created.has_value());
    std::vector<std::byte> payload(64 * 1024, std::byte{1});
    bool exhausted = false;
    for (std::size_t i = 0; i < 128; ++i) {
        auto stored = created.value()->addCandidate(std::to_string(i), "k", "h", "w", i, payload);
        if (!stored) {
            CHECK(stored.error().code == yams::ErrorCode::ResourceExhausted);
            exhausted = true;
            break;
        }
    }
    REQUIRE(exhausted);
    CHECK_FALSE(created.value()->operation("anything").has_value());
    created.value().reset();
    auto fresh = SweepStore::create(1, {});
    REQUIRE(fresh.has_value());
    auto missing = fresh.value()->operation("anything");
    REQUIRE(missing.has_value());
    CHECK_FALSE(missing.value().has_value());
}
