#include <yams/app/services/graph_context_service.hpp>

#include <yams/core/assert.hpp>
#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace yams::app::services {

namespace {

std::string lowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

bool containsToken(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    auto h = lowerAscii(std::string(haystack));
    auto n = lowerAscii(std::string(needle));
    return h.find(n) != std::string::npos;
}

bool isWordChar(unsigned char c) {
    return std::isalnum(c) != 0 || c == '_' || c == ':' || c == '.' || c == '/' || c == '-';
}

std::vector<std::string> extractQueryTerms(const std::string& query) {
    static const std::unordered_set<std::string> kStopWords = {
        "the",   "and",    "for",  "with", "from", "this",    "that",  "what",   "where",
        "when",  "why",    "how",  "does", "work", "works",   "code",  "symbol", "function",
        "class", "method", "file", "find", "show", "explain", "trace", "call",   "calls"};

    std::vector<std::string> terms;
    std::unordered_set<std::string> seen;
    std::string current;
    for (unsigned char c : query) {
        if (isWordChar(c)) {
            current.push_back(static_cast<char>(c));
            continue;
        }
        if (!current.empty()) {
            auto lowered = lowerAscii(current);
            if (current.size() >= 3 && !kStopWords.contains(lowered) &&
                seen.insert(current).second) {
                terms.push_back(current);
            }
            current.clear();
        }
    }
    if (!current.empty()) {
        auto lowered = lowerAscii(current);
        if (current.size() >= 3 && !kStopWords.contains(lowered) && seen.insert(current).second) {
            terms.push_back(current);
        }
    }
    if (terms.empty() && !query.empty()) {
        terms.push_back(query);
    }
    return terms;
}

bool isGeneratedOrCachePath(std::string_view path) {
    return path.find("/.cache/") != std::string_view::npos ||
           path.find("/build/") != std::string_view::npos ||
           path.find("/node_modules/") != std::string_view::npos ||
           path.find("/third_party/") != std::string_view::npos ||
           path.find("/.git/") != std::string_view::npos ||
           path.find("/generated/") != std::string_view::npos;
}

bool isTestPath(std::string_view path) {
    auto lowered = lowerAscii(std::string(path));
    return lowered.find("/test/") != std::string::npos ||
           lowered.find("/tests/") != std::string::npos ||
           lowered.find("_test.") != std::string::npos ||
           lowered.find("_catch2_test.") != std::string::npos ||
           lowered.find(".spec.") != std::string::npos;
}

bool queryMentionsTests(const std::string& query) {
    auto lowered = lowerAscii(query);
    return lowered.find("test") != std::string::npos || lowered.find("spec") != std::string::npos;
}

bool looksLikePathQuery(const std::string& query) {
    if (query.empty()) {
        return false;
    }
    if (query.find('/') != std::string::npos || query.find('\\') != std::string::npos) {
        return true;
    }
    if (query.find_first_of(" \t\n\r") != std::string::npos) {
        return false;
    }
    const auto ext = std::filesystem::path(query).extension().string();
    return !ext.empty() && ext.size() <= 10;
}

std::string languageFromPath(const std::string& path) {
    const auto ext = lowerAscii(std::filesystem::path(path).extension().string());
    if (ext == ".cpp" || ext == ".cc" || ext == ".cxx" || ext == ".hpp" || ext == ".h")
        return "cpp";
    if (ext == ".rs")
        return "rust";
    if (ext == ".py")
        return "python";
    if (ext == ".ts" || ext == ".tsx")
        return "typescript";
    if (ext == ".js" || ext == ".jsx")
        return "javascript";
    if (ext == ".go")
        return "go";
    if (ext == ".java")
        return "java";
    return ext.empty() ? "text" : ext.substr(1);
}

std::vector<std::string> readLines(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        return {};
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(input, line)) {
        lines.push_back(std::move(line));
    }
    return lines;
}

std::size_t cappedProduct(std::size_t value, std::size_t factor, std::size_t cap) {
    if (value == 0 || factor == 0 || cap == 0) {
        return 0;
    }
    std::size_t total = 0;
    for (std::size_t i = 0; i < factor; ++i) {
        const auto next = total + value;
        if (next < total || next > cap) {
            return cap;
        }
        total = next;
    }
    return total;
}

std::optional<std::string> tryExtractStringProperty(const metadata::KGNode& node,
                                                    std::string_view key) {
    if (!node.properties.has_value() || node.properties->empty()) {
        return std::nullopt;
    }
    try {
        const auto props = nlohmann::json::parse(*node.properties);
        if (!props.contains(std::string(key)) || !props[std::string(key)].is_string()) {
            return std::nullopt;
        }
        return props[std::string(key)].get<std::string>();
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<std::int32_t> tryExtractIntProperty(const metadata::KGNode& node,
                                                  std::string_view key) {
    if (!node.properties.has_value() || node.properties->empty()) {
        return std::nullopt;
    }
    try {
        const auto props = nlohmann::json::parse(*node.properties);
        if (!props.contains(std::string(key)) || !props[std::string(key)].is_number_integer()) {
            return std::nullopt;
        }
        return static_cast<std::int32_t>(props[std::string(key)].get<std::int64_t>());
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<std::string> filePathFromNodeKey(std::string_view nodeKey) {
    auto at = nodeKey.rfind('@');
    if (at == std::string_view::npos || at + 1 >= nodeKey.size()) {
        return std::nullopt;
    }
    auto suffix = std::string(nodeKey.substr(at + 1));
    if (suffix.rfind("snap:", 0) == 0) {
        return std::nullopt;
    }
    return suffix;
}

std::optional<std::string> qualifiedNameFromNodeKey(std::string_view nodeKey) {
    const auto colon = nodeKey.find(':');
    const auto at = nodeKey.rfind('@');
    if (colon == std::string_view::npos || at == std::string_view::npos) {
        return std::nullopt;
    }
    const auto start = colon + 1;
    if (at <= start) {
        return std::nullopt;
    }
    return std::string(nodeKey.begin() + static_cast<std::ptrdiff_t>(start),
                       nodeKey.begin() + static_cast<std::ptrdiff_t>(at));
}

GraphContextSymbol makeContextSymbol(const metadata::KGNode& node, const std::string& query,
                                     double baseScore) {
    GraphContextSymbol out;
    out.nodeKey = node.nodeKey;
    out.label = node.label.value_or(std::string{});
    out.kind = node.type.value_or("symbol");
    static constexpr std::string_view kVersionSuffix = "_version";
    if (out.kind.size() >= kVersionSuffix.size() && out.kind.ends_with(kVersionSuffix)) {
        const auto suffixPos = out.kind.rfind(kVersionSuffix);
        if (suffixPos != std::string::npos) {
            out.kind = out.kind.substr(0, suffixPos);
        }
    }
    auto qualifiedName = tryExtractStringProperty(node, "qualified_name");
    if (!qualifiedName.has_value()) {
        qualifiedName = qualifiedNameFromNodeKey(node.nodeKey);
    }
    out.qualifiedName = qualifiedName.value_or(out.label);

    auto filePath = tryExtractStringProperty(node, "file_path");
    if (!filePath.has_value()) {
        filePath = filePathFromNodeKey(node.nodeKey);
    }
    out.filePath = filePath.value_or(std::string{});
    out.startLine = tryExtractIntProperty(node, "start_line");
    out.endLine = tryExtractIntProperty(node, "end_line");
    out.exactMatch = lowerAscii(out.label) == lowerAscii(query) ||
                     lowerAscii(out.qualifiedName) == lowerAscii(query);
    out.generatedOrCache = isGeneratedOrCachePath(out.filePath);
    out.testFile = isTestPath(out.filePath);
    out.score = baseScore;
    if (out.exactMatch) {
        out.score += 20.0;
    }
    if (containsToken(out.qualifiedName, query)) {
        out.score += 10.0;
    }
    if (out.generatedOrCache) {
        out.score -= 30.0;
    }
    if (out.testFile && !queryMentionsTests(query)) {
        out.score -= 20.0;
    }
    return out;
}

std::string lineNumberedContent(const std::vector<std::string>& lines, std::size_t startLine,
                                std::size_t endLine, bool includeLineNumbers, std::size_t maxChars,
                                bool& truncated) {
    if (lines.empty()) {
        return {};
    }
    startLine = std::max<std::size_t>(startLine, 1);
    endLine = std::max(endLine, startLine);

    std::size_t beginIndex = 0;
    if (startLine > 1) {
        beginIndex = startLine - 1;
    }
    const std::size_t endIndexExclusive = std::min(endLine, lines.size());

    std::ostringstream out;
    std::size_t emitted = 0;
    for (std::size_t index = beginIndex; index < endIndexExclusive; ++index) {
        const std::size_t lineNo = index + 1;
        std::string rendered;
        if (includeLineNumbers) {
            rendered = std::to_string(lineNo) + "\t" + lines[index] + "\n";
        } else {
            rendered = lines[index] + "\n";
        }
        if (emitted + rendered.size() > maxChars) {
            truncated = true;
            break;
        }
        out << rendered;
        emitted += rendered.size();
    }
    return out.str();
}

GraphContextSymbol makeContextSymbol(const metadata::SymbolMetadata& sym,
                                     const std::string& query) {
    GraphContextSymbol out;
    out.label = sym.symbolName;
    out.qualifiedName = sym.qualifiedName;
    out.kind = sym.kind;
    out.filePath = sym.filePath;
    out.startLine = sym.startLine;
    out.endLine = sym.endLine;
    out.nodeKey = sym.kind + ":" + sym.qualifiedName + "@" + sym.filePath;
    out.exactMatch = lowerAscii(sym.symbolName) == lowerAscii(query) ||
                     lowerAscii(sym.qualifiedName) == lowerAscii(query);
    out.generatedOrCache = isGeneratedOrCachePath(sym.filePath);
    out.testFile = isTestPath(sym.filePath);
    out.score = out.exactMatch ? 100.0 : 50.0;
    if (containsToken(sym.qualifiedName, query)) {
        out.score += 20.0;
    }
    if (out.generatedOrCache) {
        out.score -= 30.0;
    }
    if (out.testFile && !queryMentionsTests(query)) {
        out.score -= 20.0;
    }
    return out;
}

Result<std::vector<GraphContextSymbol>>
lookupFallbackNodeSymbols(metadata::KnowledgeGraphStore& kgStore,
                          const std::vector<std::string>& terms, const std::string& query,
                          bool includeTests, std::size_t limit) {
    std::vector<GraphContextSymbol> symbols;
    std::unordered_set<std::string> seenKeys;
    symbols.reserve(limit);

    const auto maybeAddNode = [&](const metadata::KGNode& node, double baseScore) {
        if (seenKeys.size() >= limit) {
            return;
        }
        auto symbol = makeContextSymbol(node, query, baseScore);
        if ((!includeTests && symbol.testFile && !queryMentionsTests(query)) ||
            symbol.filePath.empty()) {
            return;
        }
        if (seenKeys.insert(symbol.nodeKey).second) {
            symbols.push_back(std::move(symbol));
        }
    };

    const auto hydrateAliases = [&](const Result<std::vector<metadata::AliasResolution>>& aliases,
                                    double baseScore) -> Result<void> {
        if (!aliases) {
            return aliases.error();
        }
        for (const auto& alias : aliases.value()) {
            auto node = kgStore.getNodeById(alias.nodeId);
            if (!node) {
                return node.error();
            }
            if (!node.value().has_value()) {
                continue;
            }
            maybeAddNode(*node.value(), baseScore * alias.score);
            if (symbols.size() >= limit) {
                return Result<void>();
            }
        }
        return Result<void>();
    };

    for (const auto& term : terms) {
        auto exactAliases = hydrateAliases(kgStore.resolveAliasExact(term, limit), 95.0);
        if (!exactAliases) {
            return exactAliases.error();
        }
        if (symbols.size() >= limit) {
            break;
        }

        auto labelMatches = kgStore.searchNodesByLabel(term, limit, 0);
        if (!labelMatches) {
            return labelMatches.error();
        }
        for (const auto& node : labelMatches.value()) {
            maybeAddNode(node, term.find(' ') != std::string::npos ? 90.0 : 80.0);
            if (symbols.size() >= limit) {
                break;
            }
        }
        if (symbols.size() >= limit) {
            break;
        }

        auto fuzzyAliases = hydrateAliases(kgStore.resolveAliasFuzzy(term, limit), 70.0);
        if (!fuzzyAliases) {
            return fuzzyAliases.error();
        }
        if (symbols.size() >= limit) {
            break;
        }
    }

    std::stable_sort(symbols.begin(), symbols.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.score != rhs.score) {
            return lhs.score > rhs.score;
        }
        if (lhs.filePath != rhs.filePath) {
            return lhs.filePath < rhs.filePath;
        }
        return lhs.qualifiedName < rhs.qualifiedName;
    });
    return symbols;
}

} // namespace

class GraphContextService : public IGraphContextService {
public:
    GraphContextService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                        std::shared_ptr<metadata::MetadataRepository> metadataRepo)
        : kgStore_(std::move(kgStore)), metadataRepo_(std::move(metadataRepo)) {}

    Result<GraphExploreResponse> explore(const GraphExploreRequest& req) override {
        GraphExploreResponse response;
        response.query = req.query;
        response.kgAvailable = kgStore_ != nullptr;

        if (!kgStore_) {
            response.warnings.push_back("Knowledge graph store not available");
            return response;
        }
        if (req.query.empty()) {
            return Error{ErrorCode::InvalidArgument, "graph explore query is required"};
        }

        const auto terms = extractQueryTerms(req.query);
        std::vector<GraphContextSymbol> symbols;
        std::unordered_set<std::string> seenKeys;
        const auto perTermLimit = std::max<std::size_t>(req.budget.maxSymbols, 16);
        const bool pathQuery = looksLikePathQuery(req.query);
        const auto addSymbols = [&](const std::vector<metadata::SymbolMetadata>& matches,
                                    double filePathBoost) {
            for (const auto& sym : matches) {
                auto contextSymbol = makeContextSymbol(sym, req.query);
                if (pathQuery && containsToken(sym.filePath, req.query)) {
                    contextSymbol.score += filePathBoost;
                    contextSymbol.exactMatch = lowerAscii(sym.filePath) == lowerAscii(req.query);
                }
                if (!req.includeTests && contextSymbol.testFile && !queryMentionsTests(req.query)) {
                    continue;
                }
                if (seenKeys.insert(contextSymbol.nodeKey).second) {
                    symbols.push_back(std::move(contextSymbol));
                }
            }
        };

        if (pathQuery) {
            auto filePathResult = kgStore_->querySymbolMetadata(req.query, std::nullopt,
                                                                std::nullopt, perTermLimit, 0);
            if (!filePathResult) {
                return filePathResult.error();
            }
            addSymbols(filePathResult.value(), 60.0);
        }

        if (!pathQuery || symbols.empty()) {
            for (const auto& term : terms) {
                auto symResult = kgStore_->querySymbolMetadata(std::nullopt, std::nullopt, term,
                                                               perTermLimit, 0);
                if (!symResult) {
                    return symResult.error();
                }
                addSymbols(symResult.value(), 0.0);
            }
        }

        std::stable_sort(symbols.begin(), symbols.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.score != rhs.score) {
                return lhs.score > rhs.score;
            }
            if (lhs.filePath != rhs.filePath) {
                return lhs.filePath < rhs.filePath;
            }
            return lhs.qualifiedName < rhs.qualifiedName;
        });

        response.totalSymbolsConsidered = symbols.size();
        if (symbols.empty()) {
            auto fallbackSymbols = lookupFallbackNodeSymbols(
                *kgStore_, terms, req.query, req.includeTests, req.budget.maxSymbols);
            if (!fallbackSymbols) {
                return fallbackSymbols.error();
            }
            if (fallbackSymbols.value().empty()) {
                response.warnings.push_back(
                    "No matching symbols found in symbol metadata or graph node labels");
                response.warnings.push_back(
                    "Graph explore depends on extracted symbol metadata; re-index the file or "
                    "rerun extraction if results look incomplete");
                return response;
            }
            response.warnings.push_back(
                "Symbol metadata missing or stale; falling back to graph node labels and aliases");
            symbols = std::move(fallbackSymbols.value());
            response.totalSymbolsConsidered = symbols.size();
        }
        if (symbols.size() > req.budget.maxSymbols) {
            symbols.resize(req.budget.maxSymbols);
            response.truncated = true;
        }
        response.entrySymbols = symbols;
        YAMS_DCHECK(response.entrySymbols.size() <= req.budget.maxSymbols,
                    "graph explore entry symbols must respect maxSymbols budget");

        addRelationships(symbols, response, req.budget);
        addSnippets(symbols, response, req);
        YAMS_DCHECK(response.emittedChars <= req.budget.maxTotalChars,
                    "graph explore emitted chars must respect maxTotalChars budget");
        return response;
    }

    Result<GraphSymbolLookupResponse> lookupSymbol(const GraphSymbolLookupRequest&) override {
        return Error{ErrorCode::NotImplemented, "graph symbol lookup is not implemented yet"};
    }

    Result<GraphTraceResponse> trace(const GraphTraceRequest&) override {
        return Error{ErrorCode::NotImplemented, "graph trace is not implemented yet"};
    }

    Result<GraphImpactResponse> impact(const GraphImpactRequest&) override {
        return Error{ErrorCode::NotImplemented, "graph impact is not implemented yet"};
    }

    Result<GraphAffectedTestsResponse> affectedTests(const GraphAffectedTestsRequest&) override {
        return Error{ErrorCode::NotImplemented, "affected test discovery is not implemented yet"};
    }

private:
    void addRelationships(const std::vector<GraphContextSymbol>& symbols,
                          GraphExploreResponse& response, const GraphContextBudget& budget) {
        YAMS_PRECONDITION(kgStore_ != nullptr,
                          "GraphContextService::addRelationships requires a knowledge graph store");
        if (!budget.includeRelationships || symbols.empty()) {
            return;
        }

        std::vector<std::string> nodeKeys;
        nodeKeys.reserve(symbols.size());
        for (const auto& symbol : symbols) {
            if (!symbol.nodeKey.empty()) {
                nodeKeys.push_back(symbol.nodeKey);
            }
        }
        if (nodeKeys.empty()) {
            return;
        }

        auto nodesResult = kgStore_->getNodesByKeys(nodeKeys);
        if (!nodesResult) {
            return;
        }

        std::unordered_map<std::string, std::int64_t> nodeIdsByKey;
        nodeIdsByKey.reserve(nodesResult.value().size());
        for (const auto& node : nodesResult.value()) {
            nodeIdsByKey.emplace(node.nodeKey, node.id);
        }
        if (nodeIdsByKey.empty()) {
            return;
        }

        const auto symbolCount = symbols.size();
        YAMS_PRECONDITION(symbolCount > 0,
                          "GraphContextService::addRelationships requires at least one symbol");
        if (symbolCount == 0) {
            return;
        }
        static constexpr std::size_t kRelationshipBudgetCap = 128;
        const auto relationshipBudget = std::clamp<std::size_t>(
            std::max<std::size_t>(cappedProduct(symbolCount, 4, kRelationshipBudgetCap),
                                  cappedProduct(budget.maxFiles, 6, kRelationshipBudgetCap)),
            8, kRelationshipBudgetCap);
        const auto fetchShare = relationshipBudget / symbolCount;
        const auto fetchRemainder = relationshipBudget % symbolCount;
        const auto fetchLimitPerSymbol =
            std::clamp<std::size_t>(fetchShare + (fetchRemainder == 0 ? 2 : 3), 4, 24);
        YAMS_DCHECK(
            relationshipBudget >= symbolCount || relationshipBudget == kRelationshipBudgetCap,
            "graph explore relationship budget must cover the entry symbol set unless capped");
        YAMS_DCHECK(fetchLimitPerSymbol > 0,
                    "graph explore relationship fetch budget must stay positive");

        std::unordered_set<std::int64_t> seenEdgeIds;
        std::vector<metadata::KGEdge> hydratedEdges;
        hydratedEdges.reserve(relationshipBudget);
        bool relationshipLimitHit = false;

        for (const auto& symbol : symbols) {
            const auto nodeIt = nodeIdsByKey.find(symbol.nodeKey);
            if (nodeIt == nodeIdsByKey.end()) {
                continue;
            }

            auto edgesResult =
                kgStore_->getEdgesBidirectional(nodeIt->second, std::nullopt, fetchLimitPerSymbol);
            if (!edgesResult) {
                continue;
            }
            for (const auto& edge : edgesResult.value()) {
                if (!seenEdgeIds.insert(edge.id).second) {
                    continue;
                }
                if (hydratedEdges.size() >= relationshipBudget) {
                    relationshipLimitHit = true;
                    break;
                }
                hydratedEdges.push_back(edge);
            }
            if (relationshipLimitHit) {
                break;
            }
        }

        YAMS_DCHECK(hydratedEdges.size() <= relationshipBudget,
                    "graph explore hydrated edge count must respect relationship budget");
        if (hydratedEdges.empty()) {
            return;
        }

        std::vector<std::int64_t> endpointIds;
        endpointIds.reserve(hydratedEdges.size());
        std::unordered_set<std::int64_t> seenEndpointIds;
        for (const auto& edge : hydratedEdges) {
            if (seenEndpointIds.insert(edge.srcNodeId).second) {
                endpointIds.push_back(edge.srcNodeId);
            }
            if (seenEndpointIds.insert(edge.dstNodeId).second) {
                endpointIds.push_back(edge.dstNodeId);
            }
        }

        std::unordered_map<std::int64_t, metadata::KGNode> nodesById;
        if (!endpointIds.empty()) {
            auto endpointNodesResult = kgStore_->getNodesByIds(endpointIds);
            if (endpointNodesResult) {
                nodesById.reserve(endpointNodesResult.value().size());
                for (auto& node : endpointNodesResult.value()) {
                    nodesById.emplace(node.id, std::move(node));
                }
            }
        }

        response.relationships.reserve(response.relationships.size() + hydratedEdges.size());
        for (const auto& edge : hydratedEdges) {
            GraphContextRelation relation;
            relation.relation = metadata::normalizeRelationName(edge.relation);
            relation.sourceNodeKey = std::to_string(edge.srcNodeId);
            relation.targetNodeKey = std::to_string(edge.dstNodeId);
            relation.weight = edge.weight;
            relation.confidence = std::clamp(static_cast<double>(edge.weight), 0.0, 1.0);

            if (const auto srcIt = nodesById.find(edge.srcNodeId); srcIt != nodesById.end()) {
                relation.sourceNodeKey = srcIt->second.nodeKey;
                relation.sourceLabel = srcIt->second.label.value_or(srcIt->second.nodeKey);
            }
            if (const auto dstIt = nodesById.find(edge.dstNodeId); dstIt != nodesById.end()) {
                relation.targetNodeKey = dstIt->second.nodeKey;
                relation.targetLabel = dstIt->second.label.value_or(dstIt->second.nodeKey);
            }

            response.relationships.push_back(std::move(relation));
        }

        if (relationshipLimitHit) {
            response.truncated = true;
        }
    }

    void addSnippets(const std::vector<GraphContextSymbol>& symbols, GraphExploreResponse& response,
                     const GraphExploreRequest& req) {
        std::unordered_map<std::string, std::vector<GraphContextSymbol>> byFile;
        for (const auto& symbol : symbols) {
            if (symbol.filePath.empty()) {
                continue;
            }
            byFile[symbol.filePath].push_back(symbol);
        }

        std::vector<std::string> files;
        files.reserve(byFile.size());
        for (const auto& [file, _] : byFile) {
            files.push_back(file);
        }
        std::sort(files.begin(), files.end(), [&](const auto& lhs, const auto& rhs) {
            const auto& lhsSyms = byFile.at(lhs);
            const auto& rhsSyms = byFile.at(rhs);
            const double lhsScore = lhsSyms.empty() ? 0.0 : lhsSyms.front().score;
            const double rhsScore = rhsSyms.empty() ? 0.0 : rhsSyms.front().score;
            if (lhsScore != rhsScore) {
                return lhsScore > rhsScore;
            }
            return lhs < rhs;
        });

        response.totalFilesConsidered = files.size();
        std::size_t totalChars = 0;
        for (const auto& file : files) {
            if (response.files.size() >= req.budget.maxFiles) {
                response.truncated = true;
                break;
            }
            if (totalChars >= req.budget.maxTotalChars) {
                response.truncated = true;
                break;
            }

            auto lines = req.includeCode ? readLines(file) : std::vector<std::string>{};
            GraphContextSnippet snippet;
            snippet.filePath = file;
            snippet.language = languageFromPath(file);
            snippet.symbols = byFile[file];
            snippet.heading = std::filesystem::path(file).filename().string();

            if (!req.includeCode) {
                snippet.mode = GraphContextSnippetMode::Omitted;
                response.files.push_back(std::move(snippet));
                continue;
            }
            if (lines.empty()) {
                snippet.mode = GraphContextSnippetMode::Omitted;
                snippet.truncated = true;
                response.warnings.push_back("Could not read source file: " + file);
                response.files.push_back(std::move(snippet));
                continue;
            }

            auto startLine = std::numeric_limits<std::size_t>::max();
            std::size_t endLine = 0;
            for (const auto& symbol : snippet.symbols) {
                if (symbol.startLine.has_value()) {
                    startLine = std::min(startLine, static_cast<std::size_t>(*symbol.startLine));
                }
                if (symbol.endLine.has_value()) {
                    endLine = std::max(endLine, static_cast<std::size_t>(*symbol.endLine));
                }
            }
            if (startLine == std::numeric_limits<std::size_t>::max() || startLine == 0) {
                startLine = 1;
            }
            YAMS_DCHECK(
                startLine >= 1,
                "graph explore snippet start line must be normalized to one-based indexing");
            const auto snippetLineBudget = std::max<std::size_t>(req.budget.maxSnippetLines, 1);
            std::size_t snippetLineSpan = snippetLineBudget;
            if (snippetLineSpan > 0) {
                --snippetLineSpan;
            }
            const auto maxEndLine = std::min(lines.size(), startLine + snippetLineSpan);
            if (endLine == 0 || endLine < startLine) {
                endLine = maxEndLine;
            }
            endLine = std::min(endLine, maxEndLine);
            endLine = std::min(endLine, lines.size());
            YAMS_DCHECK(endLine >= startLine,
                        "graph explore snippet end line must not precede start line");

            const auto remaining = req.budget.maxTotalChars - totalChars;
            const auto fileBudget = std::min(req.budget.maxCharsPerFile, remaining);
            snippet.content =
                lineNumberedContent(lines, startLine, endLine, req.budget.includeLineNumbers,
                                    fileBudget, snippet.truncated);
            snippet.startLine = static_cast<std::int32_t>(startLine);
            snippet.endLine = static_cast<std::int32_t>(endLine);
            totalChars += snippet.content.size();
            YAMS_DCHECK(totalChars <= req.budget.maxTotalChars,
                        "graph explore snippet aggregation must respect maxTotalChars budget");
            response.emittedChars = totalChars;
            if (snippet.truncated) {
                response.truncated = true;
            }
            response.files.push_back(std::move(snippet));
        }
    }

    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
};

std::shared_ptr<IGraphContextService>
makeGraphContextService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                        std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    if (!kgStore) {
        return nullptr;
    }
    return std::make_shared<GraphContextService>(std::move(kgStore), std::move(metadataRepo));
}

} // namespace yams::app::services
