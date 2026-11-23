#include <yams/app/services/graph_context_service.hpp>
#include <yams/app/services/retrieval_path_policy.hpp>

#include <yams/common/fs_utils.h>
#include <yams/core/assert.hpp>
#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/node_key_utils.h>
#include <yams/profiling.h>

#include <nlohmann/json.hpp>
#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <numeric>
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

bool pathWithinScope(std::string_view candidatePath, std::string_view scopePathPrefix) {
    if (scopePathPrefix.empty()) {
        return true;
    }
    return utils::isPathWithinScope(candidatePath, scopePathPrefix);
}

bool relationEndpointWithinScope(std::string_view nodeKey, std::string_view scopePathPrefix) {
    if (scopePathPrefix.empty()) {
        return true;
    }
    if (const auto sourcePath = metadata::sourcePathFromNodeKey(nodeKey)) {
        return pathWithinScope(*sourcePath, scopePathPrefix);
    }
    auto normalizedScope = common::canonicalizePathForComparison(scopePathPrefix);
    std::string normalizedNodeKey(nodeKey);
#ifdef _WIN32
    normalizedScope = lowerAscii(normalizedScope);
    normalizedNodeKey = lowerAscii(normalizedNodeKey);
#endif
    const auto offset = normalizedNodeKey.find(normalizedScope);
    if (offset == std::string_view::npos) {
        return false;
    }
    if (offset != 0 && normalizedNodeKey[offset - 1] != '@' &&
        normalizedNodeKey[offset - 1] != '/' && normalizedNodeKey[offset - 1] != ':') {
        return false;
    }
    const auto end = offset + normalizedScope.size();
    return end == normalizedNodeKey.size() || normalizedNodeKey[end] == '/' ||
           normalizedNodeKey[end] == '@';
}

bool isWordChar(unsigned char c) {
    // '\\' and '~' keep native Windows paths, including 8.3 short names, as one query term.
    return std::isalnum(c) != 0 || c == '_' || c == ':' || c == '.' || c == '/' || c == '-' ||
           c == '\\' || c == '~';
}

bool isIdentifierChar(unsigned char c) {
    return std::isalnum(c) != 0 || c == '_';
}

bool containsIdentifier(std::string_view line, std::string_view identifier) {
    if (identifier.empty()) {
        return false;
    }
    std::size_t offset = 0;
    auto pos = line.find(identifier, offset);
    while (pos != std::string_view::npos) {
        const bool leftBoundary =
            pos == 0 || !isIdentifierChar(static_cast<unsigned char>(line[pos - 1]));
        const auto end = pos + identifier.size();
        const bool rightBoundary =
            end == line.size() || !isIdentifierChar(static_cast<unsigned char>(line[end]));
        if (leftBoundary && rightBoundary) {
            return true;
        }
        offset = pos + 1;
        pos = line.find(identifier, offset);
    }
    return false;
}

std::optional<std::size_t> findNearestIdentifierLine(const std::vector<std::string>& lines,
                                                     std::string_view identifier,
                                                     std::size_t preferredLine) {
    std::optional<std::size_t> bestLine;
    std::size_t bestDistance = std::numeric_limits<std::size_t>::max();
    for (std::size_t index = 0; index < lines.size(); ++index) {
        if (!containsIdentifier(lines[index], identifier)) {
            continue;
        }
        const auto line = index + 1;
        const auto distance = line > preferredLine ? line - preferredLine : preferredLine - line;
        if (!bestLine.has_value() || distance < bestDistance) {
            bestLine = line;
            bestDistance = distance;
        }
    }
    return bestLine;
}

std::vector<std::string> extractQueryTerms(const std::string& query) {
    static const std::unordered_set<std::string> kStopWords = {
        "the",    "and",  "for",  "with", "from",    "this",  "that",   "what",    "where",
        "when",   "why",  "how",  "does", "work",    "works", "code",   "symbol",  "class",
        "method", "file", "find", "show", "explain", "trace", "call",   "calls",   "add",
        "get",    "set",  "make", "use",  "uses",    "using", "result", "results", "function"};

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
        filePath = tryExtractStringProperty(node, "source_file");
    }
    if (!filePath.has_value()) {
        // Path nodes written at ingest carry the file under "path"; they let file-path queries
        // resolve now that code-symbol nodes are no longer produced.
        filePath = tryExtractStringProperty(node, "path");
    }
    if (!filePath.has_value()) {
        filePath = filePathFromNodeKey(node.nodeKey);
    }
    out.filePath = filePath.value_or(std::string{});
    out.startLine = tryExtractIntProperty(node, "start_line");
    out.endLine = tryExtractIntProperty(node, "end_line");
    out.exactMatch =
        lowerAscii(out.label) == lowerAscii(query) ||
        lowerAscii(out.qualifiedName) == lowerAscii(query) ||
        (!out.filePath.empty() &&
         lowerAscii(std::filesystem::path(out.filePath).filename().string()) == lowerAscii(query));
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

    const auto lookupTerm = [&](const std::string& lookupTerm) -> Result<void> {
        auto exactAliases = hydrateAliases(kgStore.resolveAliasExact(lookupTerm, limit), 95.0);
        if (!exactAliases) {
            return exactAliases.error();
        }
        if (symbols.size() >= limit) {
            return Result<void>();
        }

        auto labelMatches = kgStore.searchNodesByLabel(lookupTerm, limit, 0);
        if (!labelMatches) {
            return labelMatches.error();
        }
        for (const auto& node : labelMatches.value()) {
            maybeAddNode(node, lookupTerm.find(' ') != std::string::npos ? 90.0 : 80.0);
            if (symbols.size() >= limit) {
                break;
            }
        }
        if (symbols.size() >= limit) {
            return Result<void>();
        }

        auto fuzzyAliases = hydrateAliases(kgStore.resolveAliasFuzzy(lookupTerm, limit), 70.0);
        if (!fuzzyAliases) {
            return fuzzyAliases.error();
        }
        return Result<void>();
    };

    for (const auto& term : terms) {
        if (auto res = lookupTerm(term); !res) {
            return res.error();
        }
        if (symbols.size() >= limit) {
            break;
        }
        // Indexed symbols may carry internal qualifiers (e.g. Ns::Impl::method) that a
        // shortened C++ qualified query (Ns::method) cannot match verbatim; retry with
        // the simple name so qualified queries resolve like unqualified ones.
        const std::string simpleName = simpleNameOf(term);
        if (simpleName != term) {
            if (auto res = lookupTerm(simpleName); !res) {
                return res.error();
            }
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

bool isWholeFileContext(const GraphContextSymbol& symbol) {
    return (symbol.kind == "file" || symbol.kind == "path" || symbol.kind == "document") &&
           !symbol.startLine.has_value() && !symbol.endLine.has_value();
}

// Node types every store gets from ingest, versioning and topology. A label match on one of
// these is a file or bookkeeping node, never a definition.
constexpr std::array<std::string_view, 12> kStructuralNodeTypes{"path",
                                                                "blob",
                                                                "document",
                                                                "file",
                                                                "directory",
                                                                "text_segment",
                                                                "topology_cluster",
                                                                "topology_snapshot",
                                                                "topology_snapshot_pointer",
                                                                "topology_road_snapshot",
                                                                "topology_route_fit_snapshot",
                                                                "topology_payload_chunk"};

const std::string& symbolNavigationUnavailableMessage() {
    static const std::string kMessage =
        "This store has only file and version nodes: code-symbol extraction was removed in "
        "v0.20, so graph lookup and trace have nothing to resolve. Ingest nodes through the MCP "
        "`graph` tool, use `yams graph --explore <file>` for files related by directory, tag and "
        "semantic similarity, or `yams grep`/`yams search` to find a definition.";
    return kMessage;
}

// Lookup and trace resolve named nodes: pre-v0.20 code symbols, or nodes added by graph ingest
// or entity extraction. A store written since v0.20 with neither holds only structural nodes,
// whose label matches would pass for definitions, so callers fail instead.
Result<bool> storeHasNavigableNodes(metadata::KnowledgeGraphStore& kgStore) {
    auto types = kgStore.listNodeTypes();
    if (!types) {
        return types.error();
    }
    for (const auto& type : types.value()) {
        if (std::find(kStructuralNodeTypes.begin(), kStructuralNodeTypes.end(), type) ==
            kStructuralNodeTypes.end()) {
            return true;
        }
    }
    return false;
}

// Candidate pool for explore relative to the symbol budget, before coverage ranking.
constexpr std::size_t kExploreCandidatePoolFactor = 8;
constexpr std::size_t kExploreMinCandidatePool = 64;

// Score weights for multi-term explore ranking. A query term covered anywhere in a file's
// candidates outweighs the lookup tier gap (alias 95 / label 80 / fuzzy 70), so the file that
// answers most of the query wins; a term in the file name counts more than one in a directory.
constexpr double kFileCoverageWeight = 10.0;
constexpr double kBasenameTermWeight = 3.0;
constexpr double kSymbolCoverageWeight = 1.0;

// Fold query-term coverage into each candidate's score, then order by score. Lookup returns
// label matches alphabetically, so without this, equal-score files came back in path order.
void rankByQueryCoverage(std::vector<GraphContextSymbol>& symbols,
                         const std::vector<std::string>& terms) {
    if (terms.size() < 2 || symbols.size() < 2) {
        return;
    }
    const auto limit = std::min<std::size_t>(terms.size(), 64);
    const auto termMask = [&](const GraphContextSymbol& symbol) {
        std::uint64_t mask = 0;
        for (std::size_t i = 0; i < limit; ++i) {
            if (containsToken(symbol.label, terms[i]) ||
                containsToken(symbol.qualifiedName, terms[i]) ||
                containsToken(symbol.filePath, terms[i])) {
                mask |= std::uint64_t{1} << i;
            }
        }
        return mask;
    };
    const auto basenameMask = [&](const std::string& filePath) {
        std::uint64_t mask = 0;
        const auto name = std::filesystem::path(filePath).filename().string();
        for (std::size_t i = 0; i < limit; ++i) {
            if (containsToken(name, terms[i])) {
                mask |= std::uint64_t{1} << i;
            }
        }
        return mask;
    };
    std::vector<std::uint64_t> masks;
    masks.reserve(symbols.size());
    std::unordered_map<std::string, std::uint64_t> fileMasks;
    for (const auto& symbol : symbols) {
        masks.push_back(termMask(symbol));
        fileMasks[symbol.filePath] |= masks.back();
    }
    std::unordered_map<std::string, int> basenameHits;
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        auto& symbol = symbols[i];
        auto [it, inserted] = basenameHits.try_emplace(symbol.filePath, 0);
        if (inserted) {
            it->second = std::popcount(basenameMask(symbol.filePath));
        }
        symbol.score += kFileCoverageWeight * std::popcount(fileMasks[symbol.filePath]) +
                        kBasenameTermWeight * it->second +
                        kSymbolCoverageWeight * std::popcount(masks[i]);
    }
    std::stable_sort(symbols.begin(), symbols.end(),
                     [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
}

bool isFilePathQuery(const std::string& query, const std::vector<std::string>& terms) {
    if (terms.size() != 1 || terms.front() != query) {
        return false;
    }
    const std::filesystem::path queryPath(query);
    return queryPath.has_extension() || queryPath.has_parent_path();
}

// A whole-file candidate is the file the query names: the same absolute path, the query
// resolved against the scope, or (unscoped relative query) a path ending in the query.
bool filePathMatchesQuery(std::string_view filePath, const std::string& query,
                          std::string_view scopePathPrefix) {
    if (filePath.empty()) {
        return false;
    }
    const auto candidate = std::filesystem::path(std::string(filePath)).lexically_normal();
    const auto queryPath = std::filesystem::path(query).lexically_normal();
    if (queryPath.is_absolute()) {
        return candidate == queryPath;
    }
    if (!scopePathPrefix.empty()) {
        return candidate ==
               (std::filesystem::path(std::string(scopePathPrefix)) / queryPath).lexically_normal();
    }
    const auto candidateText = candidate.generic_string();
    const auto suffix = queryPath.generic_string();
    if (suffix.empty() || suffix.starts_with("..")) {
        return false;
    }
    return candidateText == suffix ||
           (candidateText.size() > suffix.size() && candidateText.ends_with(suffix) &&
            candidateText[candidateText.size() - suffix.size() - 1] == '/');
}

// Relations a file-seeded explore follows, in metadata-first order.
constexpr std::string_view kRelatedSameDirectory = "same_directory";
constexpr std::string_view kRelatedSharedTag = "shared_tag";
constexpr std::string_view kRelatedSemanticNeighbor = "semantic_neighbor";
// Rows scanned per source (directory listing, tag members, neighbor edges) before ranking.
constexpr std::size_t kRelatedScanLimit = 256;
constexpr std::size_t kRelatedSemanticEdgeLimit = 64;
constexpr std::size_t kRelatedSeedTagLimit = 32;

struct RelatedFileCandidate {
    std::string filePath;
    bool sameDirectory{false};
    std::vector<std::string> sharedTags;
    std::optional<float> semanticWeight;
    double score{0.0};
};

// Shared leading characters of two file stems, relative to the longer stem: links
// foo.cpp, foo.h and foo_test.cpp within a candidate group.
double stemAffinity(std::string_view lhsPath, std::string_view rhsPath) {
    const auto lhs = lowerAscii(std::filesystem::path(std::string(lhsPath)).stem().string());
    const auto rhs = lowerAscii(std::filesystem::path(std::string(rhsPath)).stem().string());
    const auto longest = std::max(lhs.size(), rhs.size());
    if (longest == 0) {
        return 0.0;
    }
    std::size_t common = 0;
    while (common < lhs.size() && common < rhs.size() && lhs[common] == rhs[common]) {
        ++common;
    }
    return static_cast<double>(common) / static_cast<double>(longest);
}

// Leading directory components two paths share.
std::size_t sharedDirectoryDepth(std::string_view lhsPath, std::string_view rhsPath) {
    const auto lhs = std::filesystem::path(std::string(lhsPath)).parent_path();
    const auto rhs = std::filesystem::path(std::string(rhsPath)).parent_path();
    std::size_t depth = 0;
    for (auto l = lhs.begin(), r = rhs.begin(); l != lhs.end() && r != rhs.end() && *l == *r;
         ++l, ++r) {
        ++depth;
    }
    return depth;
}

// One score ranks candidates inside every relation group: each other signal that also links
// the candidate to the seed counts, then file-name affinity, then directory proximity.
double relatedFileScore(const RelatedFileCandidate& candidate, std::string_view seedPath) {
    double score = 0.0;
    if (candidate.semanticWeight) {
        score += 2.0 + std::clamp(static_cast<double>(*candidate.semanticWeight), 0.0, 1.0);
    }
    score += static_cast<double>(candidate.sharedTags.size());
    score += 1.5 * stemAffinity(candidate.filePath, seedPath);
    score += 0.1 * static_cast<double>(std::min<std::size_t>(
                       sharedDirectoryDepth(candidate.filePath, seedPath), 10));
    return score;
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
        // Label lookup fills its limit term by term, so an early broad term can crowd out nodes
        // that match more of the query. Gather a wider pool, scope it, rank it by query-term
        // coverage, then apply the symbol budget.
        const std::size_t candidatePool = std::max<std::size_t>(
            req.budget.maxSymbols * kExploreCandidatePoolFactor, kExploreMinCandidatePool);
        auto matched =
            lookupFallbackNodeSymbols(*kgStore_, terms, req.query, req.includeTests, candidatePool);
        if (!matched) {
            return matched.error();
        }
        auto symbols = std::move(matched.value());
        // Global label/alias limits can be exhausted by foreign mentions before scope
        // filtering. An explicit file query can resolve its stable path key directly.
        const std::filesystem::path queryPath(req.query);
        if (terms.size() == 1 && terms.front() == req.query &&
            (queryPath.has_extension() || queryPath.has_parent_path()) &&
            (queryPath.is_absolute() || !req.scopePathPrefix.empty())) {
            const auto target =
                (queryPath.is_absolute() ? queryPath
                                         : std::filesystem::path(req.scopePathPrefix) / queryPath)
                    .lexically_normal();
            if (pathWithinScope(target.generic_string(), req.scopePathPrefix)) {
                for (const auto* prefix : {"path:file:", "path:logical:"}) {
                    auto node = kgStore_->getNodeByKey(prefix + target.generic_string());
                    if (!node) {
                        return node.error();
                    }
                    if (!node.value()) {
                        continue;
                    }
                    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
                    auto symbol = makeContextSymbol(*node.value(), req.query, 95.0);
                    if (std::filesystem::path(symbol.filePath).lexically_normal() != target ||
                        (!req.includeTests && symbol.testFile && !queryMentionsTests(req.query))) {
                        continue;
                    }
                    if (!symbol.exactMatch) {
                        symbol.exactMatch = true;
                        symbol.score += 20.0;
                    }
                    symbols.insert(symbols.begin(), std::move(symbol));
                    break;
                }
            }
        }
        std::erase_if(symbols, [&](const auto& symbol) {
            return !pathWithinScope(symbol.filePath, req.scopePathPrefix);
        });
        std::stable_sort(symbols.begin(), symbols.end(),
                         [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
        // Deduplicate hydration, not graph identity: historical file nodes can have
        // different incident edges. Segments and explicit source spans stay distinct.
        std::unordered_map<std::string, std::vector<std::string>> fileNodeKeys;
        std::unordered_set<std::string> seenFiles;
        std::erase_if(symbols, [&](const auto& symbol) {
            if (!isWholeFileContext(symbol)) {
                return false;
            }
            if (req.budget.includeRelationships) {
                fileNodeKeys[symbol.filePath].push_back(symbol.nodeKey);
            }
            return !seenFiles.insert(symbol.filePath).second;
        });
        rankByQueryCoverage(symbols, terms);
        response.totalSymbolsConsidered = symbols.size();
        if (symbols.empty()) {
            response.warnings.push_back("No graph node labels or aliases matched the query");
            return response;
        }
        if (symbols.size() > req.budget.maxSymbols) {
            symbols.resize(req.budget.maxSymbols);
            response.truncated = true;
        }
        // A query naming one file seeds navigation from that file: put it first and expand to
        // related documents below.
        std::optional<GraphContextSymbol> fileSeed;
        if (isFilePathQuery(req.query, terms)) {
            const auto seedIt = std::ranges::find_if(symbols, [&](const auto& symbol) {
                return isWholeFileContext(symbol) &&
                       filePathMatchesQuery(symbol.filePath, req.query, req.scopePathPrefix);
            });
            if (seedIt != symbols.end()) {
                if (!seedIt->exactMatch) {
                    seedIt->exactMatch = true;
                    seedIt->score += 20.0;
                }
                std::rotate(symbols.begin(), seedIt, seedIt + 1);
                fileSeed = symbols.front();
            }
        }
        response.entrySymbols = symbols;
        YAMS_DCHECK(response.entrySymbols.size() <= req.budget.maxSymbols,
                    "graph explore entry symbols must respect maxSymbols budget");

        std::vector<std::string> relationshipNodeKeys;
        std::unordered_set<std::string> seenNodeKeys;
        if (req.budget.includeRelationships) {
            const auto addKey = [&](const std::string& key) {
                if (!key.empty() && seenNodeKeys.insert(key).second) {
                    relationshipNodeKeys.push_back(key);
                }
            };
            for (const auto& symbol : symbols) {
                if (isWholeFileContext(symbol)) {
                    for (const auto& key : fileNodeKeys.at(symbol.filePath)) {
                        addKey(key);
                    }
                } else {
                    addKey(symbol.nodeKey);
                }
            }
        }
        addRelationships(relationshipNodeKeys, response, req.budget);
        if (!req.scopePathPrefix.empty()) {
            std::erase_if(response.relationships, [&](const auto& relation) {
                return !relationEndpointWithinScope(relation.sourceNodeKey, req.scopePathPrefix) ||
                       !relationEndpointWithinScope(relation.targetNodeKey, req.scopePathPrefix);
            });
        }
        if (fileSeed) {
            addRelatedFiles(*fileSeed, req, symbols, response);
        }
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
        if (auto hasSymbols = storeHasNavigableNodes(*kgStore_); !hasSymbols) {
            return hasSymbols.error();
        } else if (!hasSymbols.value()) {
            return Error{ErrorCode::NotSupported, symbolNavigationUnavailableMessage()};
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
                          truncated, totalFiles, response.snippetRenderMicros,
                          response.snippetsRendered);
            if (truncated) {
                response.truncated = true;
            }
        }
        YAMS_PLOT("graph_context::lookup_matches", static_cast<int64_t>(response.matches.size()));
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
        if (auto hasSymbols = storeHasNavigableNodes(*kgStore_); !hasSymbols) {
            return hasSymbols.error();
        } else if (!hasSymbols.value()) {
            return Error{ErrorCode::NotSupported, symbolNavigationUnavailableMessage()};
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
                // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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
                // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
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
                          truncated, totalFiles, response.snippetRenderMicros,
                          response.snippetsRendered);
            if (truncated) {
                response.truncated = true;
            }
        }
        YAMS_PLOT("graph_context::trace_path_hops", static_cast<int64_t>(response.path.size()));
        return response;
    }

private:
    // Expand a file seed to related documents in metadata-first order: same directory, shared
    // tags, then semantic neighbors. Each relation gets a share of the remaining file budget;
    // unused shares pass to the next relation. Every added file carries a relationship from
    // the seed that names the relation it came from.
    void addRelatedFiles(const GraphContextSymbol& seed, const GraphExploreRequest& req,
                         std::vector<GraphContextSymbol>& symbols, GraphExploreResponse& response) {
        YAMS_ZONE_SCOPED_N("graph_context::addRelatedFiles");
        std::unordered_set<std::string> shownFiles;
        for (const auto& symbol : symbols) {
            shownFiles.insert(symbol.filePath);
        }
        if (req.budget.maxFiles <= shownFiles.size()) {
            return;
        }
        const auto budget = req.budget.maxFiles - shownFiles.size();
        const auto& seedPath = seed.filePath;

        std::unordered_map<std::string, RelatedFileCandidate> candidates;
        const auto candidateFor = [&](const std::string& path) -> RelatedFileCandidate* {
            if (path.empty() || shownFiles.contains(path) ||
                !pathWithinScope(path, req.scopePathPrefix)) {
                return nullptr;
            }
            auto [it, inserted] = candidates.try_emplace(path);
            if (inserted) {
                it->second.filePath = path;
            }
            return &it->second;
        };

        std::optional<metadata::DocumentInfo> seedDoc;
        if (metadataRepo_) {
            if (auto found = metadataRepo_->findDocumentByExactPath(seedPath); found) {
                seedDoc = std::move(found.value());
            }
            metadata::DocumentQueryOptions siblings;
            siblings.pathPrefix = std::filesystem::path(seedPath).parent_path().generic_string();
            siblings.prefixIsDirectory = true;
            siblings.includeSubdirectories = false;
            // Match path locations: a document row keeps the first path its content was seen
            // at, so identical files elsewhere would hide this directory's members.
            siblings.usePathLocations = true;
            siblings.currentPathsOnly = true;
            siblings.limit = static_cast<int>(kRelatedScanLimit);
            if (auto docs = metadataRepo_->queryDocuments(siblings); docs) {
                for (const auto& doc : docs.value()) {
                    // The exact-path lookup normalizes symlinks; the stored path may not be.
                    if (!seedDoc && doc.filePath == seedPath) {
                        seedDoc = doc;
                    }
                    if (auto* candidate = candidateFor(doc.filePath)) {
                        candidate->sameDirectory = true;
                    }
                }
            }
        }

        std::optional<std::int64_t> seedDocNodeId;
        if (seedDoc && !seedDoc->sha256Hash.empty()) {
            if (auto node = kgStore_->getNodeByKey("doc:" + seedDoc->sha256Hash);
                node && node.value()) {
                // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
                seedDocNodeId = node.value()->id;
            }
        }

        const auto docNodePath = [&](const metadata::KGNode& node) -> std::string {
            if (node.label && !node.label->empty()) {
                return *node.label;
            }
            if (metadataRepo_ && node.nodeKey.starts_with("doc:")) {
                if (auto doc = metadataRepo_->getDocumentByHash(node.nodeKey.substr(4));
                    doc && doc.value()) {
                    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
                    return doc.value()->filePath;
                }
            }
            return {};
        };
        const auto hydrateNodes = [&](const std::vector<std::int64_t>& ids) {
            std::unordered_map<std::int64_t, metadata::KGNode> out;
            if (ids.empty()) {
                return out;
            }
            if (auto nodes = kgStore_->getNodesByIds(ids); nodes) {
                for (auto& node : nodes.value()) {
                    out.emplace(node.id, std::move(node));
                }
            }
            return out;
        };

        // Shared tags: graph has_tag edges when the store has them, else the tag metadata.
        bool tagsFromGraph = false;
        if (seedDocNodeId) {
            auto tagEdges = kgStore_->getEdgesFrom(*seedDocNodeId, std::string_view("has_tag"),
                                                   kRelatedSeedTagLimit);
            if (tagEdges && !tagEdges.value().empty()) {
                tagsFromGraph = true;
                std::vector<std::int64_t> tagIds;
                for (const auto& edge : tagEdges.value()) {
                    tagIds.push_back(edge.dstNodeId);
                }
                auto tagNodes = hydrateNodes(tagIds);
                for (const auto tagId : tagIds) {
                    const auto tagIt = tagNodes.find(tagId);
                    const std::string tagName =
                        tagIt == tagNodes.end()
                            ? std::to_string(tagId)
                            : tagIt->second.label.value_or(tagIt->second.nodeKey);
                    auto members =
                        kgStore_->getEdgesTo(tagId, std::string_view("has_tag"), kRelatedScanLimit);
                    if (!members) {
                        continue;
                    }
                    std::vector<std::int64_t> memberIds;
                    for (const auto& edge : members.value()) {
                        if (edge.srcNodeId != *seedDocNodeId) {
                            memberIds.push_back(edge.srcNodeId);
                        }
                    }
                    for (const auto& [id, node] : hydrateNodes(memberIds)) {
                        if (auto* candidate = candidateFor(docNodePath(node))) {
                            candidate->sharedTags.push_back(tagName);
                        }
                    }
                }
            }
        }
        if (!tagsFromGraph && seedDoc && metadataRepo_) {
            if (auto tags = metadataRepo_->getDocumentTags(seedDoc->id); tags) {
                auto seedTags = std::move(tags.value());
                if (seedTags.size() > kRelatedSeedTagLimit) {
                    seedTags.resize(kRelatedSeedTagLimit);
                }
                for (const auto& tag : seedTags) {
                    metadata::DocumentQueryOptions tagged;
                    tagged.tags = {tag};
                    tagged.usePathLocations = true;
                    tagged.currentPathsOnly = true;
                    tagged.limit = static_cast<int>(kRelatedScanLimit);
                    auto docs = metadataRepo_->queryDocuments(tagged);
                    if (!docs) {
                        continue;
                    }
                    for (const auto& doc : docs.value()) {
                        if (auto* candidate = candidateFor(doc.filePath)) {
                            candidate->sharedTags.push_back(tag);
                        }
                    }
                }
            }
        }

        if (seedDocNodeId) {
            auto edges = kgStore_->getEdgesBidirectional(
                *seedDocNodeId, std::string_view("semantic_neighbor"), kRelatedSemanticEdgeLimit);
            if (edges) {
                std::unordered_map<std::int64_t, float> weights;
                for (const auto& edge : edges.value()) {
                    const auto other =
                        edge.srcNodeId == *seedDocNodeId ? edge.dstNodeId : edge.srcNodeId;
                    if (other == *seedDocNodeId) {
                        continue;
                    }
                    auto [it, inserted] = weights.try_emplace(other, edge.weight);
                    if (!inserted) {
                        it->second = std::max(it->second, edge.weight);
                    }
                }
                std::vector<std::int64_t> ids;
                ids.reserve(weights.size());
                for (const auto& [id, _] : weights) {
                    ids.push_back(id);
                }
                for (const auto& [id, node] : hydrateNodes(ids)) {
                    if (auto* candidate = candidateFor(docNodePath(node))) {
                        candidate->semanticWeight = weights.at(id);
                    }
                }
            }
        }

        // Group each candidate under the first relation that reaches it, then rank inside it.
        std::array<std::vector<const RelatedFileCandidate*>, 3> groups;
        for (auto& [path, candidate] : candidates) {
            candidate.score = relatedFileScore(candidate, seedPath);
            if (candidate.sameDirectory) {
                groups[0].push_back(&candidate);
            } else if (!candidate.sharedTags.empty()) {
                groups[1].push_back(&candidate);
            } else if (candidate.semanticWeight) {
                groups[2].push_back(&candidate);
            }
        }
        for (auto& group : groups) {
            std::ranges::sort(group, [](const auto* lhs, const auto* rhs) {
                if (lhs->score != rhs->score) {
                    return lhs->score > rhs->score;
                }
                return lhs->filePath < rhs->filePath;
            });
        }
        static constexpr std::array<std::string_view, 3> kGroupRelation{
            kRelatedSameDirectory, kRelatedSharedTag, kRelatedSemanticNeighbor};

        const auto share = std::max<std::size_t>(1, (budget + groups.size() - 1) / groups.size());
        std::array<std::size_t, 3> taken{};
        std::vector<std::pair<std::size_t, const RelatedFileCandidate*>> picked;
        for (std::size_t g = 0; g < groups.size() && picked.size() < budget; ++g) {
            while (taken[g] < groups[g].size() && taken[g] < share && picked.size() < budget) {
                picked.emplace_back(g, groups[g][taken[g]++]);
            }
        }
        for (std::size_t g = 0; g < groups.size() && picked.size() < budget; ++g) {
            while (taken[g] < groups[g].size() && picked.size() < budget) {
                picked.emplace_back(g, groups[g][taken[g]++]);
            }
        }
        if (picked.empty()) {
            // Say so only for a stored document; a bare graph node has nothing to expand.
            if (seedDoc) {
                response.warnings.push_back(
                    "No related files for " + seedPath +
                    ": no other documents in its directory, no shared tags, no semantic neighbors");
            }
            return;
        }

        std::vector<GraphContextRelation> relations;
        relations.reserve(picked.size());
        for (std::size_t rank = 0; rank < picked.size(); ++rank) {
            const auto [group, candidate] = picked[rank];
            GraphContextSymbol related;
            related.nodeKey = "path:logical:" + candidate->filePath;
            related.label = candidate->filePath;
            related.qualifiedName = candidate->filePath;
            related.kind = "document";
            related.filePath = candidate->filePath;
            related.generatedOrCache = isGeneratedOrCachePath(candidate->filePath);
            related.testFile = isTestPath(candidate->filePath);
            // Below the seed, in pick order.
            related.score = seed.score - 1.0 - static_cast<double>(rank) * 1e-3;
            symbols.push_back(std::move(related));

            if (!req.budget.includeRelationships) {
                continue;
            }
            GraphContextRelation relation;
            relation.relation = std::string(kGroupRelation[group]);
            relation.sourceNodeKey = seed.nodeKey;
            relation.sourceLabel = seedPath;
            relation.targetNodeKey = "path:logical:" + candidate->filePath;
            relation.targetLabel = candidate->filePath;
            relation.weight = candidate->semanticWeight.value_or(1.0F);
            relation.confidence = std::clamp(static_cast<double>(relation.weight), 0.0, 1.0);
            std::string provenance = "explore:" + relation.relation;
            if (group == 1) {
                provenance += ":";
                for (std::size_t i = 0; i < candidate->sharedTags.size(); ++i) {
                    provenance += (i == 0 ? "" : ",") + candidate->sharedTags[i];
                }
            }
            relation.provenance = std::move(provenance);
            relations.push_back(std::move(relation));
        }
        response.relationships.insert(response.relationships.begin(),
                                      std::make_move_iterator(relations.begin()),
                                      std::make_move_iterator(relations.end()));
    }

    void addRelationships(const std::vector<std::string>& nodeKeys, GraphExploreResponse& response,
                          const GraphContextBudget& budget) {
        YAMS_ZONE_SCOPED_N("graph_context::addRelationships");
        YAMS_PRECONDITION(kgStore_ != nullptr,
                          "GraphContextService::addRelationships requires a knowledge graph store");
        if (!budget.includeRelationships || nodeKeys.empty()) {
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

        const auto symbolCount = nodeKeys.size();
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

        for (const auto& nodeKey : nodeKeys) {
            const auto nodeIt = nodeIdsByKey.find(nodeKey);
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
                                           response.warnings, truncated, totalFilesConsidered,
                                           response.snippetRenderMicros, response.snippetsRendered);
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
                              std::size_t& totalFilesConsidered, std::int64_t& renderMicros,
                              std::size_t& snippetsRendered) {
        YAMS_ZONE_SCOPED_N("graph_context::buildSnippets");
        renderMicros = 0;
        snippetsRendered = 0;
        std::unordered_map<std::string, std::vector<GraphContextSymbol>> byFile;
        for (const auto& symbol : symbols) {
            if (symbol.filePath.empty()) {
                continue;
            }
            byFile[symbol.filePath].push_back(symbol);
        }

        // Callers pass symbols already ranked. Order files by their best score and keep that
        // ranked order among ties; sorting ties by path made equal-score files alphabetical.
        std::vector<std::string> files;
        files.reserve(byFile.size());
        std::unordered_map<std::string, double> bestScore;
        for (const auto& symbol : symbols) {
            if (symbol.filePath.empty()) {
                continue;
            }
            auto [it, inserted] = bestScore.try_emplace(symbol.filePath, symbol.score);
            if (inserted) {
                files.push_back(symbol.filePath);
            } else {
                it->second = std::max(it->second, symbol.score);
            }
        }
        std::stable_sort(files.begin(), files.end(), [&](const auto& lhs, const auto& rhs) {
            return bestScore.at(lhs) > bestScore.at(rhs);
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
                truncated = true;
                warnings.push_back("Could not read source file: " + file);
                outFiles.push_back(std::move(snippet));
                continue;
            }

            auto startLine = std::numeric_limits<std::size_t>::max();
            std::size_t endLine = 0;
            std::size_t relocatedSymbols = 0;
            for (const auto& symbol : snippet.symbols) {
                auto symbolStart = symbol.startLine.has_value() && *symbol.startLine > 0
                                       ? static_cast<std::size_t>(*symbol.startLine)
                                       : std::size_t{0};
                auto symbolEnd = symbol.endLine.has_value() && *symbol.endLine > 0
                                     ? static_cast<std::size_t>(*symbol.endLine)
                                     : symbolStart;
                bool locationContainsSymbol = false;
                if (symbolStart >= 1 && symbolStart <= lines.size() && !symbol.label.empty()) {
                    const auto checkedEnd =
                        std::min(lines.size(), std::max(symbolStart, symbolEnd));
                    for (auto line = symbolStart; line <= checkedEnd; ++line) {
                        if (containsIdentifier(lines[line - 1], symbol.label)) {
                            locationContainsSymbol = true;
                            break;
                        }
                    }
                }
                if (!locationContainsSymbol && symbolStart >= 1 && symbolStart <= lines.size() &&
                    !symbol.label.empty()) {
                    const auto relocated =
                        findNearestIdentifierLine(lines, symbol.label, symbolStart);
                    if (relocated.has_value()) {
                        const auto originalSpan =
                            symbolEnd >= symbolStart ? symbolEnd - symbolStart : std::size_t{0};
                        symbolStart = *relocated;
                        symbolEnd = std::min(lines.size(), symbolStart + originalSpan);
                        ++relocatedSymbols;
                    }
                }
                if (symbolStart != 0) {
                    startLine = std::min(startLine, symbolStart);
                }
                if (symbolEnd != 0) {
                    endLine = std::max(endLine, symbolEnd);
                }
            }
            if (relocatedSymbols != 0) {
                warnings.push_back("Relocated " + std::to_string(relocatedSymbols) +
                                   " stale symbol location(s) while hydrating: " + file);
            }
            if (startLine == std::numeric_limits<std::size_t>::max() || startLine == 0) {
                startLine = 1;
            }
            if (startLine > lines.size()) {
                snippet.mode = GraphContextSnippetMode::Omitted;
                snippet.truncated = true;
                truncated = true;
                warnings.push_back("Omitted snippet for stale source location: " + file);
                outFiles.push_back(std::move(snippet));
                continue;
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
                endLine = lines.size();
            }
            if (endLine > maxEndLine) {
                snippet.truncated = true;
            }
            endLine = std::min(endLine, maxEndLine);
            endLine = std::min(endLine, lines.size());
            YAMS_DCHECK(endLine >= startLine,
                        "graph explore snippet end line must not precede start line");

            const auto remaining = budget.maxTotalChars - totalChars;
            const auto fileBudget = std::min(budget.maxCharsPerFile, remaining);
            const auto renderStart = std::chrono::steady_clock::now();
            {
                YAMS_ZONE_SCOPED_N("graph_context::snippet_render");
                snippet.content =
                    lineNumberedContent(lines, startLine, endLine, budget.includeLineNumbers,
                                        fileBudget, snippet.truncated);
            }
            renderMicros += std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() - renderStart)
                                .count();
            ++snippetsRendered;
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
        YAMS_PLOT("graph_context::snippet_render_us", renderMicros);
        YAMS_PLOT("graph_context::snippets_rendered", static_cast<std::int64_t>(snippetsRendered));
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
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access): checked above
        return std::optional<std::int64_t>{node.value()->id};
    }

    Result<std::vector<GraphContextSymbol>>
    resolveEntrySymbols(const std::string& name, const std::optional<std::string>& file,
                        std::optional<std::int32_t> line, std::size_t limit,
                        std::string_view scopePathPrefix = {}) {
        YAMS_ZONE_SCOPED_N("graph_context::resolveEntrySymbols");
        auto matched =
            lookupFallbackNodeSymbols(*kgStore_, extractQueryTerms(name), name, true, limit);
        if (!matched) {
            return matched.error();
        }
        auto out = std::move(matched.value());
        std::erase_if(out, [&](const auto& symbol) {
            if (!pathWithinScope(symbol.filePath, scopePathPrefix)) {
                return true;
            }
            if (file.has_value() && symbol.filePath.find(*file) == std::string::npos) {
                return true;
            }
            return line.has_value() && symbol.startLine.has_value() && symbol.endLine.has_value() &&
                   (*line < *symbol.startLine || *line > *symbol.endLine);
        });
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

    Result<void> shortestPathToAny(std::int64_t fromId,
                                   const std::unordered_set<std::int64_t>& targets,
                                   std::size_t maxDepth, std::vector<GraphContextRelation>& path,
                                   bool& found) {
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
