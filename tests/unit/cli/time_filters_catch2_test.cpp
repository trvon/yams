// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/time_filters.h>

#include <chrono>

namespace {

yams::metadata::DocumentInfo makeDoc(std::int64_t createdSec, std::int64_t modifiedSec,
                                     std::int64_t indexedSec) {
    yams::metadata::DocumentInfo doc;
    doc.createdTime = std::chrono::sys_seconds{std::chrono::seconds{createdSec}};
    doc.modifiedTime = std::chrono::sys_seconds{std::chrono::seconds{modifiedSec}};
    doc.indexedTime = std::chrono::sys_seconds{std::chrono::seconds{indexedSec}};
    return doc;
}

TEST_CASE("applyDocTimeFilters passes when no filters are set", "[cli][filter][time]") {
    yams::cli::DocTimeFilters filters;
    const auto doc = makeDoc(1'000'000, 2'000'000, 3'000'000);
    CHECK(yams::cli::applyDocTimeFilters(doc, filters));
}

TEST_CASE("applyDocTimeFilters filters on created time", "[cli][filter][time]") {
    const auto doc = makeDoc(1'700'000'000, 1'700'000'000, 1'700'000'000);

    yams::cli::DocTimeFilters after;
    after.createdAfter = "2000-01-01"; // far in the past: doc passes
    CHECK(yams::cli::applyDocTimeFilters(doc, after));

    yams::cli::DocTimeFilters afterFuture;
    afterFuture.createdAfter = "2030-01-01"; // future: doc fails
    CHECK_FALSE(yams::cli::applyDocTimeFilters(doc, afterFuture));

    yams::cli::DocTimeFilters before;
    before.createdBefore = "2000-01-01"; // doc is newer: fails
    CHECK_FALSE(yams::cli::applyDocTimeFilters(doc, before));

    yams::cli::DocTimeFilters beforeFuture;
    beforeFuture.createdBefore = "2030-01-01"; // doc is older: passes
    CHECK(yams::cli::applyDocTimeFilters(doc, beforeFuture));
}

TEST_CASE("applyDocTimeFilters filters on modified and indexed time", "[cli][filter][time]") {
    const auto doc = makeDoc(1'700'000'000, 1'700'000'000, 1'700'000'000);

    yams::cli::DocTimeFilters modified;
    modified.modifiedAfter = "2030-01-01";
    CHECK_FALSE(yams::cli::applyDocTimeFilters(doc, modified));

    yams::cli::DocTimeFilters indexed;
    indexed.indexedBefore = "2000-01-01";
    CHECK_FALSE(yams::cli::applyDocTimeFilters(doc, indexed));
}

TEST_CASE("applyDocTimeFilters does not filter on invalid time strings", "[cli][filter][time]") {
    const auto doc = makeDoc(1'700'000'000, 1'700'000'000, 1'700'000'000);

    yams::cli::DocTimeFilters invalid;
    invalid.createdAfter = "not-a-time";
    CHECK(yams::cli::applyDocTimeFilters(doc, invalid)); // warn + keep
}

} // namespace
