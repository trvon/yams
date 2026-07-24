#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams::metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace yams::metadata

namespace yams::app::services {

/**
 * Agent-oriented graph context service.
 *
 * GraphQueryService exposes low-level KG traversal. GraphContextService composes
 * symbol lookup, graph traversal, source hydration, ranking, and output budgets
 * into read-equivalent context blocks for CLI/MCP/agent surfaces.
 */

enum class GraphContextSnippetMode {
    Full,    // Include the selected source span in full, subject to budgets.
    Omitted, // Mention the symbol/file but omit source due to budget or policy.
};

struct GraphContextBudget {
    std::size_t maxFiles{8};
    std::size_t maxSymbols{32};
    std::size_t maxTotalChars{24'000};
    std::size_t maxCharsPerFile{7'000};
    std::size_t maxSnippetLines{160};
    bool includeLineNumbers{true};
    bool includeRelationships{true};
};

struct GraphContextRelation {
    std::string relation;
    std::string sourceNodeKey;
    std::string sourceLabel;
    std::string targetNodeKey;
    std::string targetLabel;
    float weight{1.0F};
    double confidence{1.0};
    std::optional<std::string> provenance;
};

struct GraphContextSymbol {
    std::string nodeKey;
    std::string label;
    std::string qualifiedName;
    std::string kind;
    std::string filePath;
    std::optional<std::int32_t> startLine;
    std::optional<std::int32_t> endLine;
    double score{0.0};
    bool exactMatch{false};
    bool generatedOrCache{false};
    bool testFile{false};
};

struct GraphContextSnippet {
    std::string filePath;
    std::string language;
    GraphContextSnippetMode mode{GraphContextSnippetMode::Full};
    std::optional<std::int32_t> startLine;
    std::optional<std::int32_t> endLine;
    std::string heading;
    std::string content;
    std::vector<GraphContextSymbol> symbols;
    bool truncated{false};
};

struct GraphExploreRequest {
    std::string query;
    GraphContextBudget budget{};
    bool includeCode{true};
    bool includeTests{false};
};

struct GraphExploreResponse {
    std::string query;
    std::vector<GraphContextSymbol> entrySymbols;
    std::vector<GraphContextSnippet> files;
    std::vector<GraphContextRelation> relationships;
    std::vector<std::string> warnings;
    std::size_t totalSymbolsConsidered{0};
    std::size_t totalFilesConsidered{0};
    std::size_t emittedChars{0};
    std::int64_t snippetRenderMicros{0};
    std::size_t snippetsRendered{0};
    bool kgAvailable{true};
    bool truncated{false};
};

struct GraphSymbolLookupRequest {
    std::string symbol;
    std::optional<std::string> file;
    std::optional<std::int32_t> line;
    GraphContextBudget budget{};
    bool includeCode{false};
};

struct GraphSymbolLookupResponse {
    std::string symbol;
    std::vector<GraphContextSymbol> matches;
    std::vector<GraphContextSnippet> snippets;
    std::vector<GraphContextRelation> trail;
    std::vector<std::string> warnings;
    std::int64_t snippetRenderMicros{0};
    std::size_t snippetsRendered{0};
    bool ambiguous{false};
    bool truncated{false};
};

struct GraphTraceRequest {
    std::string from;
    std::string to;
    std::size_t maxDepth{6};
    GraphContextBudget budget{};
};

struct GraphTraceResponse {
    std::string from;
    std::string to;
    std::vector<GraphContextRelation> path;
    std::vector<GraphContextSnippet> snippets;
    std::vector<std::string> warnings;
    std::int64_t snippetRenderMicros{0};
    std::size_t snippetsRendered{0};
    bool found{false};
    bool truncated{false};
};

struct GraphImpactRequest {
    std::string symbol;
    std::size_t depth{2};
    GraphContextBudget budget{};
};

struct GraphImpactResponse {
    std::string symbol;
    std::vector<GraphContextSymbol> affectedSymbols;
    std::vector<GraphContextRelation> relationships;
    std::vector<std::string> warnings;
    bool truncated{false};
};

struct GraphAffectedTestsRequest {
    std::vector<std::string> changedFiles;
    std::size_t depth{5};
    std::string testPathPattern;
    GraphContextBudget budget{};
};

struct GraphAffectedTestsResponse {
    std::vector<std::string> changedFiles;
    std::vector<std::string> affectedTests;
    std::vector<GraphContextRelation> relationships;
    std::vector<std::string> warnings;
    bool truncated{false};
};

class IGraphContextService {
public:
    virtual ~IGraphContextService() = default;

    virtual Result<GraphExploreResponse> explore(const GraphExploreRequest& req) = 0;
    virtual Result<GraphSymbolLookupResponse> lookupSymbol(const GraphSymbolLookupRequest& req) = 0;
    virtual Result<GraphTraceResponse> trace(const GraphTraceRequest& req) = 0;
    virtual Result<GraphImpactResponse> impact(const GraphImpactRequest& req) = 0;
    virtual Result<GraphAffectedTestsResponse>
    affectedTests(const GraphAffectedTestsRequest& req) = 0;
};

std::shared_ptr<IGraphContextService>
makeGraphContextService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                        std::shared_ptr<metadata::MetadataRepository> metadataRepo);

} // namespace yams::app::services
