#include <yams/app/services/graph_context_service.hpp>

#include <yams/core/assert.hpp>
#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

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

std::string simpleNameOf(const std::string& qualified) {
    const auto pos = qualified.rfind("::");
    if (pos == std::string::npos || pos + 2 >= qualified.size()) {
        return qualified;
    }
    return qualified.substr(pos + 2);
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

const std::unordered_set<std::string>& impactRelations() {
    static const std::unordered_set<std::string> kRelations = {"calls", "references", "inherits",
                                                               "implements"};
    return kRelations;
}

GraphContextRelation
makeRelationFromEdge(const metadata::KGEdge& edge,
                     const std::unordered_map<std::int64_t, metadata::KGNode>& nodesById) {
    GraphContextRelation relation;
    relation.relation = metadata::normalizeRelationName(edge.relation);
    relation.sourceNodeKey = std::to_string(edge.srcNodeId);
    relation.targetNodeKey = std::to_string(edge.dstNodeId);
    relation.weight = edge.weight;
    relation.confidence = std::clamp(static_cast<double>(edge.weight), 0.0, 1.0);
    if (const auto it = nodesById.find(edge.srcNodeId); it != nodesById.end()) {
        relation.sourceNodeKey = it->second.nodeKey;
        relation.sourceLabel = it->second.label.value_or(it->second.nodeKey);
    }
    if (const auto it = nodesById.find(edge.dstNodeId); it != nodesById.end()) {
        relation.targetNodeKey = it->second.nodeKey;
        relation.targetLabel = it->second.label.value_or(it->second.nodeKey);
    }
    return relation;
}

} // namespace

class GraphContextService : public IGraphContextService {
public:
    GraphContextService(std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                        std::shared_ptr<metadata::MetadataRepository> metadataRepo)
        : kgStore_(std::move(kgStore)), metadataRepo_(std::move(metadataRepo)) {}

    Result<GraphExploreResponse> explore(const GraphExploreRequest& req) override {
        YAMS_ZONE_SCOPED_N("graph_context::explore");
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

    Result<GraphSymbolLookupResponse> lookupSymbol(const GraphSymbolLookupRequest& req) override {
        YAMS_ZONE_SCOPED_N("graph_context::lookupSymbol");
        GraphSymbolLookupResponse response;
        response.symbol = req.symbol;
        if (!kgStore_) {
            response.warnings.push_back("Knowledge graph store not available");
            return response;
        }
        if (req.symbol.empty()) {
            return Error{ErrorCode::InvalidArgument, "graph symbol lookup requires a symbol"};
        }

        auto matches = resolveEntrySymbols(req.symbol, req.file, req.line, req.budget.maxSymbols);
        if (!matches) {
            return matches.error();
        }
        response.matches = std::move(matches.value());
        if (response.matches.empty()) {
            response.warnings.push_back("No symbol matching '" + req.symbol +
                                        "' found in the knowledge graph");
            return response;
        }
        response.ambiguous = response.matches.size() > 1;

        std::vector<std::string> nodeKeys;
        nodeKeys.reserve(response.matches.size());
        for (const auto& match : response.matches) {
            if (!match.nodeKey.empty()) {
                nodeKeys.push_back(match.nodeKey);
            }
        }
        auto nodes = kgStore_->getNodesByKeys(nodeKeys);
        if (nodes) {
            std::unordered_map<std::int64_t, metadata::KGNode> nodesById;
            std::vector<std::int64_t> seedIds;
            for (auto& node : nodes.value()) {
                seedIds.push_back(node.id);
                nodesById.emplace(node.id, std::move(node));
            }
            std::unordered_set<std::int64_t> seenEdge;
            std::vector<metadata::KGEdge> edges;
            for (const auto id : seedIds) {
                auto edgeResult = kgStore_->getEdgesBidirectional(id, std::nullopt, 16);
                if (!edgeResult) {
                    continue;
                }
                for (const auto& edge : edgeResult.value()) {
                    if (seenEdge.insert(edge.id).second) {
                        edges.push_back(edge);
                    }
                }
            }
            std::unordered_set<std::int64_t> missing;
            for (const auto& edge : edges) {
                if (!nodesById.contains(edge.srcNodeId)) {
                    missing.insert(edge.srcNodeId);
                }
                if (!nodesById.contains(edge.dstNodeId)) {
                    missing.insert(edge.dstNodeId);
                }
            }
            if (!missing.empty()) {
                std::vector<std::int64_t> missingVec(missing.begin(), missing.end());
                auto more = kgStore_->getNodesByIds(missingVec);
                if (more) {
                    for (auto& node : more.value()) {
                        nodesById.emplace(node.id, std::move(node));
                    }
                }
            }
            response.trail.reserve(edges.size());
            for (const auto& edge : edges) {
                response.trail.push_back(makeRelationFromEdge(edge, nodesById));
            }
        }

        if (req.includeCode) {
            bool truncated = false;
            std::size_t totalFiles = 0;
            buildSnippets(response.matches, req.budget, true, response.snippets, response.warnings,
                          truncated, totalFiles);
            if (truncated) {
                response.truncated = true;
            }
        }
        YAMS_PLOT("graph_context::lookup_matches",
                  static_cast<int64_t>(response.matches.size()));
        return response;
    }

    Result<GraphTraceResponse> trace(const GraphTraceRequest& req) override {
        YAMS_ZONE_SCOPED_N("graph_context::trace");
        GraphTraceResponse response;
        response.from = req.from;
        response.to = req.to;
        if (!kgStore_) {
            response.warnings.push_back("Knowledge graph store not available");
            return response;
        }
        if (req.from.empty() || req.to.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "graph trace requires both from and to symbols"};
        }

        auto fromSyms = resolveEntrySymbols(req.from, std::nullopt, std::nullopt, 4);
        if (!fromSyms) {
            return fromSyms.error();
        }
        auto toSyms = resolveEntrySymbols(req.to, std::nullopt, std::nullopt, 4);
        if (!toSyms) {
            return toSyms.error();
        }
        if (fromSyms.value().empty() || toSyms.value().empty()) {
            response.warnings.push_back("Could not resolve both endpoints in the knowledge graph");
            return response;
        }

        std::vector<std::int64_t> fromIds;
        for (const auto& sym : fromSyms.value()) {
            auto id = nodeIdForSymbol(sym);
            if (!id) {
                return id.error();
            }
            if (id.value().has_value()) {
                fromIds.push_back(*id.value());
            }
        }
        std::unordered_set<std::int64_t> targets;
        for (const auto& sym : toSyms.value()) {
            auto id = nodeIdForSymbol(sym);
            if (!id) {
                return id.error();
            }
            if (id.value().has_value()) {
                targets.insert(*id.value());
            }
        }
        if (fromIds.empty() || targets.empty()) {
            response.warnings.push_back(
                "Endpoints have no graph nodes; re-run extraction to populate edges");
            return response;
        }

        const auto depth = std::clamp<std::size_t>(req.maxDepth == 0 ? 6 : req.maxDepth, 1, 8);
        bool found = false;
        for (const auto fromId : fromIds) {
            response.path.clear();
            if (auto r = shortestPathToAny(fromId, targets, depth, response.path, found); !r) {
                return r.error();
            }
            if (found) {
                break;
            }
        }
        response.found = found;
        if (!found) {
            response.path.clear();
            response.warnings.push_back("No path found within depth " + std::to_string(depth));
            return response;
        }

        std::vector<std::string> pathKeys;
        std::unordered_set<std::string> seenKeys;
        for (const auto& relation : response.path) {
            if (seenKeys.insert(relation.sourceNodeKey).second) {
                pathKeys.push_back(relation.sourceNodeKey);
            }
            if (seenKeys.insert(relation.targetNodeKey).second) {
                pathKeys.push_back(relation.targetNodeKey);
            }
        }
        auto pathSyms = symbolsFromNodeKeys(pathKeys);
        if (pathSyms) {
            bool truncated = false;
            std::size_t totalFiles = 0;
            buildSnippets(pathSyms.value(), req.budget, true, response.snippets, response.warnings,
                          truncated, totalFiles);
            if (truncated) {
                response.truncated = true;
            }
        }
        YAMS_PLOT("graph_context::trace_path_hops", static_cast<int64_t>(response.path.size()));
        return response;
    }

    Result<GraphImpactResponse> impact(const GraphImpactRequest& req) override {
        YAMS_ZONE_SCOPED_N("graph_context::impact");
        GraphImpactResponse response;
        response.symbol = req.symbol;
        if (!kgStore_) {
            response.warnings.push_back("Knowledge graph store not available");
            return response;
        }
        if (req.symbol.empty()) {
            return Error{ErrorCode::InvalidArgument, "graph impact requires a symbol"};
        }

        auto entry = resolveEntrySymbols(req.symbol, std::nullopt, std::nullopt, req.budget.maxSymbols);
        if (!entry) {
            return entry.error();
        }

        std::vector<std::int64_t> seedIds;
        std::unordered_set<std::int64_t> seedSeen;
        // Seed from the literal query name first: callers reference the call-site surface form
        // (`symbol_ref:<name>`), which does not always match the substring-resolved definition.
        if (auto r = collectCallTargetNodeIds(simpleNameOf(req.symbol), req.symbol, seedSeen,
                                              seedIds);
            !r) {
            return r.error();
        }
        for (const auto& sym : entry.value()) {
            auto id = nodeIdForSymbol(sym);
            if (!id) {
                return id.error();
            }
            if (id.value().has_value() && seedSeen.insert(*id.value()).second) {
                seedIds.push_back(*id.value());
            }
            if (auto r = collectCallTargetNodeIds(sym.label, sym.qualifiedName, seedSeen, seedIds);
                !r) {
                return r.error();
            }
        }
        if (seedIds.empty()) {
            response.warnings.push_back("No symbol matching '" + req.symbol +
                                        "' found in the knowledge graph");
            return response;
        }

        std::vector<metadata::KGNode> affected;
        const auto depth = std::clamp<std::size_t>(req.depth == 0 ? 2 : req.depth, 1, 5);
        if (auto r = collectReverseDependents(seedIds, depth, req.budget.maxSymbols, affected,
                                              response.relationships);
            !r) {
            return r.error();
        }
        YAMS_PLOT("graph_context::impact_affected",
                  static_cast<int64_t>(affected.size()));
        response.affectedSymbols.reserve(affected.size());
        for (const auto& node : affected) {
            response.affectedSymbols.push_back(makeContextSymbol(node, req.symbol, 0.0));
        }
        std::stable_sort(response.affectedSymbols.begin(), response.affectedSymbols.end(),
                         [](const auto& lhs, const auto& rhs) {
                             if (lhs.filePath != rhs.filePath) {
                                 return lhs.filePath < rhs.filePath;
                             }
                             return lhs.qualifiedName < rhs.qualifiedName;
                         });
        if (response.affectedSymbols.size() >= req.budget.maxSymbols) {
            response.truncated = true;
        }
        return response;
    }

    Result<GraphAffectedTestsResponse> affectedTests(const GraphAffectedTestsRequest& req) override {
        YAMS_ZONE_SCOPED_N("graph_context::affectedTests");
        GraphAffectedTestsResponse response;
        response.changedFiles = req.changedFiles;
        if (!kgStore_) {
            response.warnings.push_back("Knowledge graph store not available");
            return response;
        }
        if (req.changedFiles.empty()) {
            return Error{ErrorCode::InvalidArgument, "affected tests requires changed files"};
        }

        std::vector<std::int64_t> seedIds;
        std::unordered_set<std::int64_t> seedSeen;
        for (const auto& file : req.changedFiles) {
            auto syms = kgStore_->querySymbolMetadata(file, std::nullopt, std::nullopt, 500, 0);
            if (!syms) {
                return syms.error();
            }
            for (const auto& sym : syms.value()) {
                auto contextSymbol = makeContextSymbol(sym, std::string{});
                auto id = nodeIdForSymbol(contextSymbol);
                if (!id) {
                    return id.error();
                }
                if (id.value().has_value() && seedSeen.insert(*id.value()).second) {
                    seedIds.push_back(*id.value());
                }
                if (auto r = collectCallTargetNodeIds(sym.symbolName, sym.qualifiedName, seedSeen,
                                                      seedIds);
                    !r) {
                    return r.error();
                }
            }
        }
        if (seedIds.empty()) {
            response.warnings.push_back(
                "No indexed symbols found for the changed files; re-run extraction");
            return response;
        }

        std::vector<metadata::KGNode> affected;
        const auto depth = std::clamp<std::size_t>(req.depth == 0 ? 5 : req.depth, 1, 5);
        if (auto r = collectReverseDependents(seedIds, depth, 256, affected, response.relationships);
            !r) {
            return r.error();
        }

        std::unordered_set<std::string> testFiles;
        for (const auto& node : affected) {
            auto sym = makeContextSymbol(node, std::string{}, 0.0);
            if (sym.filePath.empty()) {
                continue;
            }
            const bool matchesPattern =
                !req.testPathPattern.empty() &&
                sym.filePath.find(req.testPathPattern) != std::string::npos;
            if (sym.testFile || matchesPattern) {
                testFiles.insert(sym.filePath);
            }
        }
        response.affectedTests.assign(testFiles.begin(), testFiles.end());
        std::sort(response.affectedTests.begin(), response.affectedTests.end());
        YAMS_PLOT("graph_context::affected_tests",
                  static_cast<int64_t>(response.affectedTests.size()));
        return response;
    }

private:
    void addRelationships(const std::vector<GraphContextSymbol>& symbols,
                          GraphExploreResponse& response, const GraphContextBudget& budget) {
        YAMS_ZONE_SCOPED_N("graph_context::addRelationships");
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
        bool truncated = false;
        std::size_t totalFilesConsidered = 0;
        const auto emitted = buildSnippets(symbols, req.budget, req.includeCode, response.files,
                                           response.warnings, truncated, totalFilesConsidered);
        response.totalFilesConsidered = totalFilesConsidered;
        response.emittedChars = emitted;
        if (truncated) {
            response.truncated = true;
        }
    }

    std::size_t buildSnippets(const std::vector<GraphContextSymbol>& symbols,
                              const GraphContextBudget& budget, bool includeCode,
                              std::vector<GraphContextSnippet>& outFiles,
                              std::vector<std::string>& warnings, bool& truncated,
                              std::size_t& totalFilesConsidered) {
        YAMS_ZONE_SCOPED_N("graph_context::buildSnippets");
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

        totalFilesConsidered = files.size();
        std::size_t totalChars = 0;
        for (const auto& file : files) {
            if (outFiles.size() >= budget.maxFiles) {
                truncated = true;
                break;
            }
            if (totalChars >= budget.maxTotalChars) {
                truncated = true;
                break;
            }

            auto lines = includeCode ? readLines(file) : std::vector<std::string>{};
            GraphContextSnippet snippet;
            snippet.filePath = file;
            snippet.language = languageFromPath(file);
            snippet.symbols = byFile[file];
            snippet.heading = std::filesystem::path(file).filename().string();

            if (!includeCode) {
                snippet.mode = GraphContextSnippetMode::Omitted;
                outFiles.push_back(std::move(snippet));
                continue;
            }
            if (lines.empty()) {
                snippet.mode = GraphContextSnippetMode::Omitted;
                snippet.truncated = true;
                warnings.push_back("Could not read source file: " + file);
                outFiles.push_back(std::move(snippet));
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
            const auto snippetLineBudget = std::max<std::size_t>(budget.maxSnippetLines, 1);
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

            const auto remaining = budget.maxTotalChars - totalChars;
            const auto fileBudget = std::min(budget.maxCharsPerFile, remaining);
            snippet.content = lineNumberedContent(lines, startLine, endLine,
                                                  budget.includeLineNumbers, fileBudget,
                                                  snippet.truncated);
            snippet.startLine = static_cast<std::int32_t>(startLine);
            snippet.endLine = static_cast<std::int32_t>(endLine);
            totalChars += snippet.content.size();
            YAMS_DCHECK(totalChars <= budget.maxTotalChars,
                        "graph explore snippet aggregation must respect maxTotalChars budget");
            if (snippet.truncated) {
                truncated = true;
            }
            outFiles.push_back(std::move(snippet));
        }
        return totalChars;
    }

    Result<std::optional<std::int64_t>> nodeIdForSymbol(const GraphContextSymbol& symbol) {
        if (symbol.nodeKey.empty()) {
            return std::optional<std::int64_t>{};
        }
        auto node = kgStore_->getNodeByKey(symbol.nodeKey);
        if (!node) {
            return node.error();
        }
        if (!node.value().has_value()) {
            return std::optional<std::int64_t>{};
        }
        return std::optional<std::int64_t>{node.value()->id};
    }

    Result<std::vector<GraphContextSymbol>> resolveEntrySymbols(const std::string& name,
                                                                const std::optional<std::string>& file,
                                                                std::optional<std::int32_t> line,
                                                                std::size_t limit) {
        YAMS_ZONE_SCOPED_N("graph_context::resolveEntrySymbols");
        std::vector<GraphContextSymbol> out;
        std::unordered_set<std::string> seen;
        auto byName = kgStore_->querySymbolMetadata(std::nullopt, std::nullopt, name,
                                                    std::max<std::size_t>(limit * 2, 16), 0);
        if (!byName) {
            return byName.error();
        }
        for (const auto& sym : byName.value()) {
            if (file.has_value() && sym.filePath.find(*file) == std::string::npos) {
                continue;
            }
            if (line.has_value() && sym.startLine.has_value() && sym.endLine.has_value() &&
                (*line < *sym.startLine || *line > *sym.endLine)) {
                continue;
            }
            auto contextSymbol = makeContextSymbol(sym, name);
            if (seen.insert(contextSymbol.nodeKey).second) {
                out.push_back(std::move(contextSymbol));
            }
        }
        if (out.empty()) {
            auto fallback = lookupFallbackNodeSymbols(*kgStore_, extractQueryTerms(name), name,
                                                      true, limit);
            if (!fallback) {
                return fallback.error();
            }
            out = std::move(fallback.value());
        }
        std::stable_sort(out.begin(), out.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.score != rhs.score) {
                return lhs.score > rhs.score;
            }
            if (lhs.filePath != rhs.filePath) {
                return lhs.filePath < rhs.filePath;
            }
            return lhs.qualifiedName < rhs.qualifiedName;
        });
        if (out.size() > limit) {
            out.resize(limit);
        }
        return out;
    }

    Result<std::vector<GraphContextSymbol>>
    symbolsFromNodeKeys(const std::vector<std::string>& keys) {
        std::vector<GraphContextSymbol> out;
        if (keys.empty()) {
            return out;
        }
        auto nodes = kgStore_->getNodesByKeys(keys);
        if (!nodes) {
            return nodes.error();
        }
        out.reserve(nodes.value().size());
        for (const auto& node : nodes.value()) {
            out.push_back(makeContextSymbol(node, std::string{}, 0.0));
        }
        return out;
    }

    // Cross-file calls land on `symbol_reference` placeholder nodes (and the per-snapshot
    // version nodes), not the canonical definition node. To answer "who depends on X" we must
    // seed reverse traversal from every node a caller could have referenced for X.
    Result<void> collectCallTargetNodeIds(const std::string& simpleName,
                                          const std::string& qualifiedName,
                                          std::unordered_set<std::int64_t>& seedSet,
                                          std::vector<std::int64_t>& seedIds) {
        if (simpleName.empty()) {
            return Result<void>();
        }
        auto matches = kgStore_->searchNodesByLabel(simpleName, 200, 0);
        if (!matches) {
            return matches.error();
        }
        const auto lowerSimple = lowerAscii(simpleName);
        const auto lowerQualified = lowerAscii(qualifiedName);
        const auto suffix = "::" + lowerSimple;
        for (const auto& node : matches.value()) {
            const auto& type = node.type;
            const bool callTargetType =
                type == "symbol_reference" || type == "function" || type == "method" ||
                type == "function_version" || type == "method_version" || type == "class" ||
                type == "struct";
            if (!callTargetType) {
                continue;
            }
            const auto label = lowerAscii(node.label.value_or(std::string{}));
            const bool nameMatch =
                label == lowerSimple || (!lowerQualified.empty() && label == lowerQualified) ||
                (label.size() > suffix.size() && label.ends_with(suffix));
            if (!nameMatch) {
                continue;
            }
            if (seedSet.insert(node.id).second) {
                seedIds.push_back(node.id);
            }
            // If a reconciliation pass linked placeholders to this definition, seed those
            // deterministically (avoids relying solely on surface-form name matching).
            const bool isDefinition = type == "function" || type == "method" ||
                                      type == "class" || type == "struct";
            if (isDefinition) {
                auto resolved = kgStore_->getEdgesTo(node.id, std::string_view("resolves_to"), 64, 0);
                if (resolved) {
                    for (const auto& edge : resolved.value()) {
                        if (seedSet.insert(edge.srcNodeId).second) {
                            seedIds.push_back(edge.srcNodeId);
                        }
                    }
                }
            }
        }
        return Result<void>();
    }

    Result<void> collectReverseDependents(const std::vector<std::int64_t>& seedIds,
                                          std::size_t depth, std::size_t maxNodes,
                                          std::vector<metadata::KGNode>& affectedNodes,
                                          std::vector<GraphContextRelation>& relationships) {
        YAMS_ZONE_SCOPED_N("graph_context::collectReverseDependents");
        if (seedIds.empty() || depth == 0 || maxNodes == 0) {
            return Result<void>();
        }
        std::unordered_set<std::int64_t> visited(seedIds.begin(), seedIds.end());
        std::unordered_set<std::int64_t> discovered;
        std::unordered_set<std::int64_t> bridgeSeen(seedIds.begin(), seedIds.end());
        std::vector<std::int64_t> frontier = seedIds;
        std::vector<metadata::KGEdge> keptEdges;
        const auto& relations = impactRelations();
        static constexpr std::size_t kPerNode = 64;
        std::size_t edgesExamined = 0;
        std::size_t hopsReached = 0;

        for (std::size_t d = 0; d < depth && !frontier.empty(); ++d) {
            hopsReached = d + 1;
            auto incoming = kgStore_->getEdgesToBatch(frontier, std::nullopt, kPerNode);
            if (!incoming) {
                return incoming.error();
            }
            std::vector<std::int64_t> callersThisHop;
            for (const auto& [dstId, edges] : incoming.value()) {
                for (const auto& edge : edges) {
                    ++edgesExamined;
                    if (!relations.contains(metadata::normalizeRelationName(edge.relation))) {
                        continue;
                    }
                    keptEdges.push_back(edge);
                    const auto srcId = edge.srcNodeId;
                    if (!visited.contains(srcId) && discovered.size() < maxNodes) {
                        visited.insert(srcId);
                        discovered.insert(srcId);
                        callersThisHop.push_back(srcId);
                    }
                }
            }
            // A caller is a version/canonical node that itself carries no incoming calls — its
            // callers point at its `symbol_reference` placeholder. Bridge each discovered caller
            // to its placeholders so the next hop can find who calls it.
            std::vector<std::int64_t> nextFrontier;
            if (d + 1 < depth && !callersThisHop.empty() && discovered.size() < maxNodes) {
                auto callerNodes = kgStore_->getNodesByIds(callersThisHop);
                if (!callerNodes) {
                    return callerNodes.error();
                }
                for (const auto& node : callerNodes.value()) {
                    const auto simple = node.label.value_or(std::string{});
                    const auto qualified =
                        tryExtractStringProperty(node, "qualified_name").value_or(simple);
                    if (auto r =
                            collectCallTargetNodeIds(simple, qualified, bridgeSeen, nextFrontier);
                        !r) {
                        return r.error();
                    }
                }
            }
            frontier = std::move(nextFrontier);
        }

        YAMS_PLOT("graph_context::impact_seeds", static_cast<int64_t>(seedIds.size()));
        YAMS_PLOT("graph_context::impact_edges_examined", static_cast<int64_t>(edgesExamined));
        YAMS_PLOT("graph_context::impact_hops_reached", static_cast<int64_t>(hopsReached));

        std::vector<std::int64_t> idsToHydrate(discovered.begin(), discovered.end());
        idsToHydrate.insert(idsToHydrate.end(), seedIds.begin(), seedIds.end());
        std::unordered_map<std::int64_t, metadata::KGNode> nodesById;
        if (!idsToHydrate.empty()) {
            auto nodes = kgStore_->getNodesByIds(idsToHydrate);
            if (!nodes) {
                return nodes.error();
            }
            for (auto& node : nodes.value()) {
                nodesById.emplace(node.id, std::move(node));
            }
        }
        affectedNodes.reserve(affectedNodes.size() + discovered.size());
        for (const auto id : discovered) {
            if (const auto it = nodesById.find(id); it != nodesById.end()) {
                affectedNodes.push_back(it->second);
            }
        }
        std::unordered_set<std::int64_t> seenEdge;
        relationships.reserve(relationships.size() + keptEdges.size());
        for (const auto& edge : keptEdges) {
            if (!seenEdge.insert(edge.id).second) {
                continue;
            }
            relationships.push_back(makeRelationFromEdge(edge, nodesById));
        }
        return Result<void>();
    }

    Result<void> shortestPathToAny(std::int64_t fromId,
                                   const std::unordered_set<std::int64_t>& targets,
                                   std::size_t maxDepth,
                                   std::vector<GraphContextRelation>& path, bool& found) {
        YAMS_ZONE_SCOPED_N("graph_context::shortestPathToAny");
        found = false;
        if (targets.contains(fromId)) {
            found = true;
            return Result<void>();
        }
        if (maxDepth == 0) {
            return Result<void>();
        }
        std::unordered_map<std::int64_t, metadata::KGEdge> parentEdge;
        std::unordered_set<std::int64_t> visited{fromId};
        std::vector<std::int64_t> frontier{fromId};
        std::int64_t reachedTarget = 0;
        bool reached = false;
        static constexpr std::size_t kPerNode = 128;

        for (std::size_t d = 0; d < maxDepth && !frontier.empty() && !reached; ++d) {
            std::vector<std::int64_t> next;
            for (const auto nodeId : frontier) {
                auto edges = kgStore_->getEdgesBidirectional(nodeId, std::nullopt, kPerNode);
                if (!edges) {
                    return edges.error();
                }
                for (const auto& edge : edges.value()) {
                    const auto neighbor =
                        (edge.srcNodeId == nodeId) ? edge.dstNodeId : edge.srcNodeId;
                    if (visited.contains(neighbor)) {
                        continue;
                    }
                    visited.insert(neighbor);
                    parentEdge.emplace(neighbor, edge);
                    if (targets.contains(neighbor)) {
                        reached = true;
                        reachedTarget = neighbor;
                        break;
                    }
                    next.push_back(neighbor);
                }
                if (reached) {
                    break;
                }
            }
            frontier = std::move(next);
        }
        if (!reached) {
            return Result<void>();
        }
        found = true;

        std::vector<metadata::KGEdge> chain;
        std::int64_t cur = reachedTarget;
        while (cur != fromId) {
            const auto it = parentEdge.find(cur);
            if (it == parentEdge.end()) {
                break;
            }
            chain.push_back(it->second);
            cur = (it->second.srcNodeId == cur) ? it->second.dstNodeId : it->second.srcNodeId;
        }
        std::reverse(chain.begin(), chain.end());

        std::unordered_set<std::int64_t> ids;
        for (const auto& edge : chain) {
            ids.insert(edge.srcNodeId);
            ids.insert(edge.dstNodeId);
        }
        std::unordered_map<std::int64_t, metadata::KGNode> nodesById;
        if (!ids.empty()) {
            std::vector<std::int64_t> idVec(ids.begin(), ids.end());
            auto nodes = kgStore_->getNodesByIds(idVec);
            if (!nodes) {
                return nodes.error();
            }
            for (auto& node : nodes.value()) {
                nodesById.emplace(node.id, std::move(node));
            }
        }
        path.reserve(path.size() + chain.size());
        for (const auto& edge : chain) {
            path.push_back(makeRelationFromEdge(edge, nodesById));
        }
        return Result<void>();
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
