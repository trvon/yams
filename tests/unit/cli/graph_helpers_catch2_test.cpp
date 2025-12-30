// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

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
