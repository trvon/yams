#include <yams/app/services/graph_context_service.hpp>

#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
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

std::string lineNumberedContent(const std::vector<std::string>& lines, std::size_t startLine,
                                std::size_t endLine, bool includeLineNumbers, std::size_t maxChars,
                                bool& truncated) {
    std::ostringstream out;
    std::size_t emitted = 0;
    for (std::size_t lineNo = startLine; lineNo <= endLine && lineNo <= lines.size(); ++lineNo) {
        std::string rendered;
        if (includeLineNumbers) {
            rendered = std::to_string(lineNo) + "\t" + lines[lineNo - 1] + "\n";
        } else {
            rendered = lines[lineNo - 1] + "\n";
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
        for (const auto& term : terms) {
            auto symResult =
                kgStore_->querySymbolMetadata(std::nullopt, std::nullopt, term, perTermLimit, 0);
            if (!symResult) {
                return symResult.error();
            }
            for (const auto& sym : symResult.value()) {
                auto contextSymbol = makeContextSymbol(sym, req.query);
                if (!req.includeTests && contextSymbol.testFile && !queryMentionsTests(req.query)) {
                    continue;
                }
                if (seenKeys.insert(contextSymbol.nodeKey).second) {
                    symbols.push_back(std::move(contextSymbol));
                }
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
            response.warnings.push_back("No matching symbols found in symbol metadata");
            return response;
        }
        if (symbols.size() > req.budget.maxSymbols) {
            symbols.resize(req.budget.maxSymbols);
            response.truncated = true;
        }
        response.entrySymbols = symbols;

        addRelationships(symbols, response, req.budget);
        addSnippets(symbols, response, req);
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
        if (!budget.includeRelationships) {
            return;
        }
        std::unordered_set<std::int64_t> seenEdgeIds;
        for (const auto& symbol : symbols) {
            auto nodeResult = kgStore_->getNodeByKey(symbol.nodeKey);
            if (!nodeResult || !nodeResult.value().has_value()) {
                continue;
            }
            auto edgesResult =
                kgStore_->getEdgesBidirectional(nodeResult.value()->id, std::nullopt, 16);
            if (!edgesResult) {
                continue;
            }
            for (const auto& edge : edgesResult.value()) {
                if (!seenEdgeIds.insert(edge.id).second) {
                    continue;
                }
                GraphContextRelation relation;
                relation.relation = metadata::normalizeRelationName(edge.relation);
                relation.sourceNodeKey = std::to_string(edge.srcNodeId);
                relation.targetNodeKey = std::to_string(edge.dstNodeId);
                relation.weight = edge.weight;
                relation.confidence = std::clamp(static_cast<double>(edge.weight), 0.0, 1.0);
                if (auto src = kgStore_->getNodeById(edge.srcNodeId); src && src.value()) {
                    relation.sourceNodeKey = src.value()->nodeKey;
                    relation.sourceLabel = src.value()->label.value_or(src.value()->nodeKey);
                }
                if (auto dst = kgStore_->getNodeById(edge.dstNodeId); dst && dst.value()) {
                    relation.targetNodeKey = dst.value()->nodeKey;
                    relation.targetLabel = dst.value()->label.value_or(dst.value()->nodeKey);
                }
                response.relationships.push_back(std::move(relation));
                if (response.relationships.size() >= budget.maxSymbols) {
                    response.truncated = true;
                    return;
                }
            }
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
            if (startLine == std::numeric_limits<std::size_t>::max()) {
                startLine = 1;
            }
            if (endLine == 0 || endLine < startLine) {
                endLine = std::min(startLine + req.budget.maxSnippetLines - 1, lines.size());
            }
            endLine = std::min(endLine, startLine + req.budget.maxSnippetLines - 1);
            endLine = std::min(endLine, lines.size());

            const auto remaining = req.budget.maxTotalChars - totalChars;
            const auto fileBudget = std::min(req.budget.maxCharsPerFile, remaining);
            snippet.content =
                lineNumberedContent(lines, startLine, endLine, req.budget.includeLineNumbers,
                                    fileBudget, snippet.truncated);
            snippet.startLine = static_cast<std::int32_t>(startLine);
            snippet.endLine = static_cast<std::int32_t>(endLine);
            totalChars += snippet.content.size();
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
