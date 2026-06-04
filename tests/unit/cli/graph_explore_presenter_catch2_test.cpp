// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_explore_presenter.h>

#include <filesystem>
#include <sstream>

TEST_CASE("Graph explore presenter maps daemon response to app response", "[cli][graph]") {
    yams::daemon::GraphExploreResponse in;
    in.query = "processTask";
    in.totalSymbolsConsidered = 2;
    in.totalFilesConsidered = 1;
    in.emittedChars = 32;
    in.kgAvailable = true;
    in.truncated = false;
    in.warnings.push_back("fallback warning");

    yams::daemon::GraphExploreSymbol symbol;
    symbol.nodeKey = "function:demo::processTask@/repo/src/process.cpp";
    symbol.label = "processTask";
    symbol.qualifiedName = "demo::processTask";
    symbol.kind = "function";
    symbol.filePath = "/repo/src/process.cpp";
    symbol.startLine = 10;
    symbol.endLine = 12;
    symbol.score = 99.0;
    symbol.exactMatch = true;
    in.entrySymbols.push_back(symbol);

    yams::daemon::GraphExploreSnippet omittedSnippet;
    omittedSnippet.filePath = symbol.filePath;
    omittedSnippet.language = "cpp";
    omittedSnippet.mode = "omitted";
    omittedSnippet.startLine = 10;
    omittedSnippet.endLine = 12;
    omittedSnippet.heading = "process.cpp";
    omittedSnippet.content = "";
    omittedSnippet.truncated = true;
    omittedSnippet.symbols.push_back(symbol);
    in.files.push_back(omittedSnippet);

    yams::daemon::GraphExploreSnippet fullSnippet;
    fullSnippet.filePath = "/repo/src/helper.cpp";
    fullSnippet.language = "cpp";
    fullSnippet.mode = "full";
    fullSnippet.startLine = 20;
    fullSnippet.endLine = 24;
    fullSnippet.heading = "helper.cpp";
    fullSnippet.content = "20\tint helper() {\n21\t    return 0;\n22\t}\n";
    in.files.push_back(fullSnippet);

    yams::daemon::GraphExploreRelation relation;
    relation.relation = "calls";
    relation.sourceNodeKey = symbol.nodeKey;
    relation.sourceLabel = "processTask";
    relation.targetNodeKey = "function:demo::helper@/repo/src/helper.cpp";
    relation.targetLabel = "helper";
    relation.weight = 1.0F;
    relation.confidence = 1.0;
    relation.provenance = "treesitter";
    in.relationships.push_back(relation);

    const auto out = yams::cli::mapGraphExploreResponseFromDaemon(in);

    CHECK(out.query == in.query);
    REQUIRE(out.entrySymbols.size() == 1);
    CHECK(out.entrySymbols.front().qualifiedName == "demo::processTask");
    REQUIRE(out.files.size() == 2);
    CHECK(out.files.front().mode == yams::app::services::GraphContextSnippetMode::Omitted);
    CHECK(out.files.back().mode == yams::app::services::GraphContextSnippetMode::Full);
    REQUIRE(out.relationships.size() == 1);
    REQUIRE(out.relationships.front().provenance.has_value());
    CHECK(*out.relationships.front().provenance == "treesitter");
}

TEST_CASE("Graph explore presenter renders json and markdown", "[cli][graph]") {
    yams::app::services::GraphExploreResponse response;
    response.query = "processTask";

    yams::app::services::GraphContextSymbol symbol;
    const auto cwd = std::filesystem::current_path();
    symbol.nodeKey = "function:demo::processTask@" +
                     (cwd / "src" / "cli" / "commands" / "graph_command.cpp").string();
    symbol.label = "processTask";
    symbol.qualifiedName = "demo::processTask";
    symbol.kind = "function";
    symbol.filePath = (cwd / "src" / "cli" / "commands" / "graph_command.cpp").string();
    symbol.startLine = 1;
    symbol.endLine = 2;
    symbol.exactMatch = true;
    response.entrySymbols.push_back(symbol);

    yams::app::services::GraphContextSnippet snippet;
    snippet.filePath = symbol.filePath;
    snippet.language = "cpp";
    snippet.mode = yams::app::services::GraphContextSnippetMode::Full;
    snippet.startLine = 1;
    snippet.endLine = 2;
    snippet.heading = "graph_command.cpp";
    snippet.content = "1\tint processTask() {\n2\t    return 1;\n";
    snippet.symbols.push_back(symbol);
    response.files.push_back(snippet);

    yams::app::services::GraphContextRelation relation;
    relation.relation = "calls";
    relation.sourceLabel = "processTask";
    relation.targetLabel = "helper";
    response.relationships.push_back(relation);

    const auto json = yams::cli::makeGraphExploreJson(response);
    CHECK(json["files"][0]["mode"] == "full");
    CHECK(json["entrySymbols"][0]["label"] == "processTask");

    std::ostringstream out;
    yams::cli::renderGraphExploreMarkdown(out, response, cwd);
    const auto rendered = out.str();
    CHECK(rendered.find("Graph Explore") != std::string::npos);
    CHECK(rendered.find("processTask --calls--> helper") != std::string::npos);
    CHECK(rendered.find("src/cli/commands/graph_command.cpp") != std::string::npos);
}
