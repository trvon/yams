// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// The repair operation registry replaces a 13-way switch and two name tables. These cases pin
// what the switch guaranteed implicitly: every wire code has exactly one operation, names are
// the canonical metric names, and the async entry point of an operation without a coroutine
// body is its synchronous body.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/repair/repair_operation.h>
#include <yams/daemon/metric_keys.h>

#include <set>
#include <string>

using yams::daemon::repair::repairOperationForCode;
using yams::daemon::repair::repairOperations;

TEST_CASE("repair operation registry covers every wire code exactly once",
          "[daemon][repair][registry][catch2]") {
    const auto& ops = repairOperations();
    REQUIRE(ops.size() == yams::daemon::metrics::kRepairOperationCodes.size());

    std::set<std::uint64_t> codes;
    std::set<std::string> names;
    for (const auto& op : ops) {
        INFO(op->name());
        CHECK(codes.insert(op->code()).second);
        CHECK(names.insert(std::string(op->name())).second);
        CHECK(op->name() == yams::daemon::metrics::repairOperationNameForCode(op->code()));
        CHECK(repairOperationForCode(op->code()) == op.get());
    }
    for (const auto& entry : yams::daemon::metrics::kRepairOperationCodes) {
        INFO(entry.name);
        REQUIRE(repairOperationForCode(entry.code) != nullptr);
        CHECK(repairOperationForCode(entry.code)->name() == entry.name);
    }
    CHECK(repairOperationForCode(0) == nullptr);
    CHECK(repairOperationForCode(99) == nullptr);
}

TEST_CASE("repair operations are registered in wire-code order",
          "[daemon][repair][registry][catch2]") {
    std::uint64_t expected = 1;
    for (const auto& op : repairOperations()) {
        CHECK(op->code() == expected++);
    }
}
