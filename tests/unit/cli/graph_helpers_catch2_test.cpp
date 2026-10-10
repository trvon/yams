// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_helpers.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

TEST_CASE("Graph helpers: --name tries the stored path under the cwd first", "[cli][graph]") {
    // #280 follow-up: a relative --name must name the document under the client cwd, as
    // ingestion stored it, before the daemon falls back to a suffix match.
    const auto cwd = std::filesystem::weakly_canonical(std::filesystem::temp_directory_path()) /
                     "yams_graph_name_candidates";
    std::filesystem::create_directories(cwd / "include");
    {
        std::ofstream(cwd / "include" / "x.hpp") << "x\n";
    }
    {
        std::ofstream(cwd / "notes.md") << "n\n";
    }
    const auto stored = [&](const std::string& rel) { return (cwd / rel).generic_string(); };

    CHECK(yams::cli::buildGraphDocumentNameCandidates("include/x.hpp", cwd) ==
          std::vector<std::string>{stored("include/x.hpp"), "include/x.hpp"});
    CHECK(yams::cli::buildGraphDocumentNameCandidates("./include/../include/x.hpp", cwd) ==
          std::vector<std::string>{stored("include/x.hpp"), "./include/../include/x.hpp"});
    // A deleted file is still indexed under its path.
    CHECK(yams::cli::buildGraphDocumentNameCandidates("src/gone.cpp", cwd) ==
          std::vector<std::string>{stored("src/gone.cpp"), "src/gone.cpp"});
    // A bare name that is a file under the cwd is a path too.
    CHECK(yams::cli::buildGraphDocumentNameCandidates("notes.md", cwd) ==
          std::vector<std::string>{stored("notes.md"), "notes.md"});
    // Otherwise a bare name is only a file-name match.
    CHECK(yams::cli::buildGraphDocumentNameCandidates("bm25.hpp", cwd) ==
          std::vector<std::string>{"bm25.hpp"});
    // An absolute stored path is tried once.
    CHECK(yams::cli::buildGraphDocumentNameCandidates(stored("include/x.hpp"), cwd) ==
          std::vector<std::string>{stored("include/x.hpp")});
    CHECK(yams::cli::buildGraphDocumentNameCandidates("", cwd).empty());

    std::error_code ec;
    std::filesystem::remove_all(cwd, ec);
}

#ifndef _WIN32
TEST_CASE("Graph helpers: --name resolves a symlinked cwd like ingestion", "[cli][graph]") {
    const auto base = std::filesystem::weakly_canonical(std::filesystem::temp_directory_path()) /
                      "yams_graph_name_symlink";
    std::filesystem::create_directories(base / "real" / "include");
    {
        std::ofstream(base / "real" / "include" / "x.hpp") << "x\n";
    }
    std::error_code ec;
    std::filesystem::create_directory_symlink(base / "real", base / "link", ec);
    if (!ec) {
        CHECK(yams::cli::buildGraphDocumentNameCandidates("include/x.hpp", base / "link").front() ==
              (base / "real" / "include" / "x.hpp").generic_string());
    }
    std::filesystem::remove_all(base, ec);
}
#endif

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
