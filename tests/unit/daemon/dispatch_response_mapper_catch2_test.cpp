// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/services.hpp>
#include <yams/daemon/components/dispatch_response.hpp>

using namespace yams::app::services;

TEST_CASE("SearchResultMapper preserves service metadata", "[unit][daemon][mapper]") {
    SearchItem item;
    item.id = 42;
    item.title = "main.cpp";
    item.path = "/repo/main.cpp";
    item.hash = "abc123";
    item.score = 0.91;
    item.snippet = "snippet";
    item.metadata["relation_count"] = "3";
    item.metadata["relation_summary"] = "defines(2), has_version(1)";

    auto mapped = yams::daemon::dispatch::SearchResultMapper::fromServiceItem(item, false);

    REQUIRE(mapped.metadata.contains("relation_count"));
    CHECK(mapped.metadata.at("relation_count") == "3");
    REQUIRE(mapped.metadata.contains("relation_summary"));
    CHECK(mapped.metadata.at("relation_summary") == "defines(2), has_version(1)");

    REQUIRE(mapped.metadata.contains("hash"));
    CHECK(mapped.metadata.at("hash") == "abc123");
    REQUIRE(mapped.metadata.contains("path"));
    CHECK(mapped.metadata.at("path") == "/repo/main.cpp");
}

TEST_CASE("SearchResultMapper paths-only mode omits metadata", "[unit][daemon][mapper]") {
    SearchItem item;
    item.id = 7;
    item.title = "util.cpp";
    item.path = "/repo/util.cpp";
    item.hash = "def456";
    item.score = 0.77;
    item.metadata["relation_summary"] = "calls(1)";

    auto mapped = yams::daemon::dispatch::SearchResultMapper::fromServiceItem(item, true);
    CHECK(mapped.metadata.empty());
}
