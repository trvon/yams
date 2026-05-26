// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_helpers.h>

#include <filesystem>
#include <string>

TEST_CASE("Graph helpers: candidate list includes absolute path", "[cli][graph]") {
    std::string input = "src/cli/commands/graph_command.cpp";
    auto candidates = yams::cli::build_graph_file_node_candidates(input);

    REQUIRE_FALSE(candidates.empty());
    CHECK(candidates.front() == input);

    auto abs = std::filesystem::absolute(input).lexically_normal().string();
    auto has_abs = std::find(candidates.begin(), candidates.end(), abs) != candidates.end();
    CHECK(has_abs);
}

TEST_CASE("Graph helpers: absolute path is preserved", "[cli][graph]") {
    auto abs = std::filesystem::absolute("src").lexically_normal().string();
    auto candidates = yams::cli::build_graph_file_node_candidates(abs);

    REQUIRE_FALSE(candidates.empty());
    CHECK(candidates.front() == abs);
}

TEST_CASE("Graph helpers: low-signal relations are omitted from explore hints", "[cli][graph]") {
    const std::string path = "src/cli/commands/search_command.cpp";

    CHECK(yams::cli::buildGraphExploreHint(path, "blob_at_path", 2) ==
          "yams graph --name \"src/cli/commands/search_command.cpp\" --depth 2");
    CHECK(yams::cli::buildGraphExploreHint(path, "has_version", 2) ==
          "yams graph --name \"src/cli/commands/search_command.cpp\" --depth 2");
    CHECK(yams::cli::buildGraphExploreHint(path, "calls", 2) ==
          "yams graph --name \"src/cli/commands/search_command.cpp\" -r calls --depth 2");
}

TEST_CASE("Graph helpers: file presentation bundles display path and hint", "[cli][graph]") {
    const auto cwd = std::filesystem::current_path();
    const auto path = (cwd / "src" / "cli" / "commands" / "search_command.cpp").string();

    const auto presentation = yams::cli::describeFileForCli(path, "calls(3), includes(2)", cwd);

    CHECK(presentation.rawPath == path);
    CHECK(presentation.displayPath == "src/cli/commands/search_command.cpp");
    CHECK(presentation.relationSummary == "calls(3), includes(2)");
    CHECK(presentation.graphExploreHint ==
          "yams graph --name \"src/cli/commands/search_command.cpp\" -r calls --depth 2");
}
