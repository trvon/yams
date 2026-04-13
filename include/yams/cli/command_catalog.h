#pragma once

#include <array>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli {

struct CommandCatalogEntry {
    std::string_view name;
    std::string_view description;
};

#ifdef YAMS_BUILD_MCP_SERVER
inline constexpr std::array<CommandCatalogEntry, 30> kTopLevelCommands = {{
#else
inline constexpr std::array<CommandCatalogEntry, 29> kTopLevelCommands = {{
#endif
    {"init", "Initialize YAMS storage and configuration"},
    {"add", "Add files or content to YAMS"},
    {"get", "Retrieve content by hash"},
    {"restore", "Restore deleted content"},
    {"cat", "Display document content to stdout"},
    {"delete", "Delete content from YAMS"},
    {"list", "List stored documents"},
    {"tree", "Show a tree view of stored content"},
    {"search", "Search documents by query"},
    {"grep", "Search document content with grep-style matching"},
    {"config", "Manage YAMS configuration settings"},
    {"auth", "Manage authentication keys and tokens"},
    {"status", "Show quick system status and health overview"},
    {"stats", "Show quick system status and health overview"},
    {"uninstall", "Remove YAMS from your system"},
    {"migrate", "Migrate YAMS data and configuration"},
    {"update", "Update stored content"},
    {"download", "Download and ingest remote content"},
    {"session", "Manage YAMS sessions"},
    {"watch", "Watch files and ingest changes automatically"},
    {"completion", "Generate shell completion scripts"},
    {"repair", "Repair and maintain storage integrity"},
    {"model", "Manage embedding models"},
    {"daemon", "Manage YAMS daemon process"},
    {"plugin", "Manage plugins and plugin trust"},
    {"doctor", "Run diagnostics and health checks"},
    {"dr", "Disaster Recovery (gated by plugins)"},
    {"graph", "Explore the knowledge graph"},
    {"diff", "Compare snapshots and paths"},
#ifdef YAMS_BUILD_MCP_SERVER
    {"serve", "Start MCP server"},
#endif
}};

inline std::vector<std::string> topLevelCommandNames() {
    std::vector<std::string> commands;
    commands.reserve(kTopLevelCommands.size());
    for (const auto& entry : kTopLevelCommands) {
        commands.emplace_back(entry.name);
    }
    return commands;
}

inline std::string_view topLevelCommandDescription(std::string_view name) {
    for (const auto& entry : kTopLevelCommands) {
        if (entry.name == name) {
            return entry.description;
        }
    }
    return "YAMS command";
}

} // namespace yams::cli
