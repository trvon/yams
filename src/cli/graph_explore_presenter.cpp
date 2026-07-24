#include <yams/cli/graph_explore_presenter.h>

#include <yams/cli/graph_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/core/assert.hpp>

#include <iostream>
#include <string>

namespace yams::cli {

namespace {

std::string snippetModeLabel(app::services::GraphContextSnippetMode mode) {
    using app::services::GraphContextSnippetMode;
    switch (mode) {
        case GraphContextSnippetMode::Full:
            return "full";
        case GraphContextSnippetMode::Omitted:
            return "omitted";
    }
    YAMS_UNREACHABLE("unknown graph context snippet mode");
}

app::services::GraphContextSnippetMode snippetModeFromLabel(const std::string& mode) {
    if (mode == "full") {
        return app::services::GraphContextSnippetMode::Full;
    }
    if (mode == "omitted") {
        return app::services::GraphContextSnippetMode::Omitted;
    }
    YAMS_UNREACHABLE("unexpected graph explore snippet mode label from daemon response");
}

app::services::GraphContextSymbol
toAppGraphContextSymbol(const daemon::GraphExploreSymbol& symbol) {
    app::services::GraphContextSymbol out;
    out.nodeKey = symbol.nodeKey;
    out.label = symbol.label;
    out.qualifiedName = symbol.qualifiedName;
    out.kind = symbol.kind;
    out.filePath = symbol.filePath;
    out.startLine = symbol.startLine;
    out.endLine = symbol.endLine;
    out.score = symbol.score;
    out.exactMatch = symbol.exactMatch;
    out.generatedOrCache = symbol.generatedOrCache;
    out.testFile = symbol.testFile;
    return out;
}

} // namespace

app::services::GraphExploreResponse
mapGraphExploreResponseFromDaemon(const yams::daemon::GraphExploreResponse& response) {
    app::services::GraphExploreResponse out;
    out.query = response.query;
    out.totalSymbolsConsidered = static_cast<std::size_t>(response.totalSymbolsConsidered);
    out.totalFilesConsidered = static_cast<std::size_t>(response.totalFilesConsidered);
    out.emittedChars = static_cast<std::size_t>(response.emittedChars);
    out.snippetRenderMicros = static_cast<std::int64_t>(response.snippetRenderMicros);
    out.snippetsRendered = static_cast<std::size_t>(response.snippetsRendered);
    out.kgAvailable = response.kgAvailable;
    out.truncated = response.truncated;
    out.warnings = response.warnings;

    out.entrySymbols.reserve(response.entrySymbols.size());
    for (const auto& symbol : response.entrySymbols) {
        out.entrySymbols.push_back(toAppGraphContextSymbol(symbol));
    }

    out.files.reserve(response.files.size());
    for (const auto& file : response.files) {
        app::services::GraphContextSnippet snippet;
        snippet.filePath = file.filePath;
        snippet.language = file.language;
        snippet.mode = snippetModeFromLabel(file.mode);
        snippet.startLine = file.startLine;
        snippet.endLine = file.endLine;
        snippet.heading = file.heading;
        snippet.content = file.content;
        snippet.truncated = file.truncated;
        snippet.symbols.reserve(file.symbols.size());
        for (const auto& symbol : file.symbols) {
            snippet.symbols.push_back(toAppGraphContextSymbol(symbol));
        }
        out.files.push_back(std::move(snippet));
    }

    out.relationships.reserve(response.relationships.size());
    for (const auto& relation : response.relationships) {
        app::services::GraphContextRelation outRelation;
        outRelation.relation = relation.relation;
        outRelation.sourceNodeKey = relation.sourceNodeKey;
        outRelation.sourceLabel = relation.sourceLabel;
        outRelation.targetNodeKey = relation.targetNodeKey;
        outRelation.targetLabel = relation.targetLabel;
        outRelation.weight = relation.weight;
        outRelation.confidence = relation.confidence;
        if (!relation.provenance.empty()) {
            outRelation.provenance = relation.provenance;
        }
        out.relationships.push_back(std::move(outRelation));
    }
    return out;
}

nlohmann::json makeGraphExploreJson(const app::services::GraphExploreResponse& response) {
    nlohmann::json out;
    out["query"] = response.query;
    out["kgAvailable"] = response.kgAvailable;
    out["truncated"] = response.truncated;
    out["totalSymbolsConsidered"] = response.totalSymbolsConsidered;
    out["totalFilesConsidered"] = response.totalFilesConsidered;
    out["emittedChars"] = response.emittedChars;
    out["snippetRenderMicros"] = response.snippetRenderMicros;
    out["snippetsRendered"] = response.snippetsRendered;
    out["warnings"] = response.warnings;

    out["entrySymbols"] = nlohmann::json::array();
    for (const auto& symbol : response.entrySymbols) {
        nlohmann::json entry;
        entry["nodeKey"] = symbol.nodeKey;
        entry["label"] = symbol.label;
        entry["qualifiedName"] = symbol.qualifiedName;
        entry["kind"] = symbol.kind;
        entry["filePath"] = symbol.filePath;
        entry["startLine"] =
            symbol.startLine ? nlohmann::json(*symbol.startLine) : nlohmann::json(nullptr);
        entry["endLine"] =
            symbol.endLine ? nlohmann::json(*symbol.endLine) : nlohmann::json(nullptr);
        entry["score"] = symbol.score;
        entry["exactMatch"] = symbol.exactMatch;
        out["entrySymbols"].push_back(std::move(entry));
    }

    out["files"] = nlohmann::json::array();
    for (const auto& file : response.files) {
        nlohmann::json item;
        item["filePath"] = file.filePath;
        item["language"] = file.language;
        item["mode"] = snippetModeLabel(file.mode);
        item["startLine"] =
            file.startLine ? nlohmann::json(*file.startLine) : nlohmann::json(nullptr);
        item["endLine"] = file.endLine ? nlohmann::json(*file.endLine) : nlohmann::json(nullptr);
        item["heading"] = file.heading;
        item["content"] = file.content;
        item["truncated"] = file.truncated;
        item["symbols"] = nlohmann::json::array();
        for (const auto& symbol : file.symbols) {
            nlohmann::json fileSymbol;
            fileSymbol["label"] = symbol.label;
            fileSymbol["qualifiedName"] = symbol.qualifiedName;
            fileSymbol["kind"] = symbol.kind;
            fileSymbol["startLine"] =
                symbol.startLine ? nlohmann::json(*symbol.startLine) : nlohmann::json(nullptr);
            fileSymbol["endLine"] =
                symbol.endLine ? nlohmann::json(*symbol.endLine) : nlohmann::json(nullptr);
            item["symbols"].push_back(std::move(fileSymbol));
        }
        out["files"].push_back(std::move(item));
    }

    out["relationships"] = nlohmann::json::array();
    for (const auto& relation : response.relationships) {
        out["relationships"].push_back({{"relation", relation.relation},
                                        {"sourceNodeKey", relation.sourceNodeKey},
                                        {"sourceLabel", relation.sourceLabel},
                                        {"targetNodeKey", relation.targetNodeKey},
                                        {"targetLabel", relation.targetLabel},
                                        {"weight", relation.weight},
                                        {"confidence", relation.confidence}});
    }
    return out;
}

void renderGraphExploreMarkdown(std::ostream& out,
                                const app::services::GraphExploreResponse& response,
                                const std::filesystem::path& cwd) {
    out << yams::cli::ui::section_header("Graph Explore") << "\n\n";
    out << yams::cli::ui::key_value("Query", response.query) << "\n";
    out << yams::cli::ui::key_value("Files", std::to_string(response.files.size())) << "\n";
    out << yams::cli::ui::key_value("Symbols", std::to_string(response.entrySymbols.size()))
        << "\n";
    if (response.truncated) {
        out << yams::cli::ui::status_warning("Output truncated to graph explore budget") << "\n";
    }
    for (const auto& warning : response.warnings) {
        out << yams::cli::ui::status_warning(warning) << "\n";
    }
    out << "\n";

    if (!response.relationships.empty()) {
        out << yams::cli::ui::subsection_header("Relationships") << "\n";
        std::size_t shown = 0;
        for (const auto& relation : response.relationships) {
            if (shown++ >= 12) {
                out << yams::cli::ui::bullet("... more relationships omitted", 2) << "\n";
                break;
            }
            const auto lhs =
                relation.sourceLabel.empty() ? relation.sourceNodeKey : relation.sourceLabel;
            const auto rhs =
                relation.targetLabel.empty() ? relation.targetNodeKey : relation.targetLabel;
            out << yams::cli::ui::bullet(lhs + " --" + relation.relation + "--> " + rhs, 2) << "\n";
        }
        out << "\n";
    }

    for (const auto& file : response.files) {
        const auto displayPath = projectPathForCli(file.filePath, cwd);
        out << yams::cli::ui::subsection_header(displayPath) << "\n";
        if (!file.symbols.empty()) {
            std::string symbolList;
            for (const auto& symbol : file.symbols) {
                if (!symbolList.empty()) {
                    symbolList += ", ";
                }
                symbolList += symbol.label.empty() ? symbol.qualifiedName : symbol.label;
            }
            out << yams::cli::ui::key_value("Symbols", symbolList) << "\n";
        }
        if (file.content.empty()) {
            out << yams::cli::ui::status_info("Source omitted or unavailable") << "\n\n";
            continue;
        }
        out << "```" << file.language << "\n" << file.content << "```\n";
        if (file.truncated) {
            out << yams::cli::ui::status_warning(
                       "File snippet truncated; run graph --explore with a narrower query")
                << "\n";
        }
        out << "\n";
    }

    if (!response.files.empty()) {
        out << yams::cli::ui::colorize(
                   "Shown snippets are line-numbered and read-equivalent; prefer another `yams "
                   "graph --explore` for follow-up context.",
                   yams::cli::ui::Ansi::DIM)
            << "\n";
    }
}

} // namespace yams::cli
