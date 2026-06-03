// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_helpers.h>
#include <yams/daemon/ipc/ipc_protocol.h>

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

TEST_CASE("Graph helpers: explore hints use agent-oriented graph explore", "[cli][graph]") {
    const std::string path = "src/cli/commands/search_command.cpp";

    CHECK(yams::cli::buildGraphExploreHint(path, "blob_at_path", 2) ==
          "yams graph --explore \"src/cli/commands/search_command.cpp\"");
    CHECK(yams::cli::buildGraphExploreHint(path, "has_version", 2) ==
          "yams graph --explore \"src/cli/commands/search_command.cpp\"");
    CHECK(yams::cli::buildGraphExploreHint(path, "calls", 2) ==
          "yams graph --explore \"src/cli/commands/search_command.cpp\"");
}

TEST_CASE("Graph helpers: file presentation bundles display path and hint", "[cli][graph]") {
    const auto cwd = std::filesystem::current_path();
    const auto path = (cwd / "src" / "cli" / "commands" / "search_command.cpp").string();

    const auto presentation = yams::cli::describeFileForCli(path, "calls(3), includes(2)", cwd);

    CHECK(presentation.rawPath == path);
    CHECK(presentation.displayPath == "src/cli/commands/search_command.cpp");
    CHECK(presentation.relationSummary == "calls(3), includes(2)");
    CHECK(presentation.graphExploreHint ==
          "yams graph --explore \"src/cli/commands/search_command.cpp\"");
}

TEST_CASE("Graph helpers: label search hint uses filename stem", "[cli][graph]") {
    const auto cwd = std::filesystem::current_path();
    const auto path = (cwd / "src" / "app" / "services" / "grep_service.cpp").string();

    CHECK(yams::cli::buildGraphSearchHint(path, cwd) == "yams graph --search \"*grep_service*\"");
    CHECK(yams::cli::buildGraphSearchHint("grep", cwd) == "yams graph --search \"*grep*\"");
}

TEST_CASE("Graph helpers: node presentation prefers symbolic labels over snap paths",
          "[cli][graph]") {
    yams::daemon::GraphNode node;
    node.label = "ActiveGrepRequestGuard";
    node.type = "function_version";
    node.properties = R"({"path":"snap:71503b..."})";

    const auto presentation = yams::cli::describeGraphNodeForCli(node);

    CHECK(presentation.displayLabel == "ActiveGrepRequestGuard");
    CHECK(presentation.displayType == "function");
    CHECK_FALSE(presentation.hideByDefault);
}

TEST_CASE("Graph helpers: field version nodes are hidden by default", "[cli][graph]") {
    yams::daemon::GraphNode node;
    node.label = "worker_";
    node.type = "field_version";

    const auto presentation = yams::cli::describeGraphNodeForCli(node);

    CHECK(presentation.displayLabel == "worker_");
    CHECK(presentation.displayType == "field");
    CHECK(presentation.hideByDefault);
}
