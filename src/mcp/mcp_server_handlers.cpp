#include <yams/common/fs_utils.h>
#include <yams/core/uuid.h>
#include <yams/mcp/error_handling.h>
#include <yams/mcp/mcp_server.h>

#if !defined(YAMS_WASI)
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/graph_helpers.h>
#include <yams/compression/compression_header.h>
#include <yams/compression/compressor_interface.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/core/task.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/socket_utils.h>
#include <yams/downloader/downloader.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>
#endif

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#if !defined(YAMS_WASI)
#include <boost/asio/local/stream_protocol.hpp>
#endif
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/this_coro.hpp>

#include <cmath>
#include <future>
#include <iomanip>
#include <mutex>
#include <thread>

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

// Platform-specific includes for non-blocking I/O
#if defined(_WIN32)
#include <conio.h>
#include <fcntl.h>
#include <io.h>
#include <windows.h>
// Windows implementation of setenv
inline int setenv(const char* name, const char* value, int overwrite) {
    if (!overwrite) {
        size_t envsize = 0;
        const int errcode = getenv_s(&envsize, NULL, 0, name);
        if (errcode || envsize)
            return errcode;
    }
    return _putenv_s(name, value);
}
#elif !defined(YAMS_WASI)
#include <poll.h>
#include <unistd.h>
#endif

namespace yams::mcp {

namespace {
MCPDownloadJobResponse makeMcpDownloadJobResponse(const yams::daemon::DownloadResponse& resp) {
    MCPDownloadJobResponse out;
    out.jobId = resp.jobId;
    out.state = resp.state;
    out.success = resp.success;
    out.url = resp.url;
    out.hash = resp.hash;
    out.storedPath = resp.localPath;
    out.sizeBytes = resp.size;
    out.createdAtMs = resp.createdAtMs;
    out.updatedAtMs = resp.updatedAtMs;
    out.error = resp.error;
    return out;
}

json makeSnapshotJson(const yams::daemon::SnapshotInfo& snap) {
    return json{{"id", snap.id},
                {"label", snap.label},
                {"created_at", snap.createdAt},
                {"document_count", snap.documentCount}};
}

MCPUpdateMetadataResponse
makeUpdateMetadataResponse(const yams::daemon::UpdateDocumentResponse& ur) {
    MCPUpdateMetadataResponse out;
    out.success = ur.metadataUpdated || ur.tagsUpdated || ur.contentUpdated;
    out.updated = out.success ? 1 : 0;
    out.matched = 1;
    if (!ur.hash.empty()) {
        out.updatedHashes.push_back(ur.hash);
    }
    out.message = out.success ? "Update successful" : "No changes applied";
    return out;
}

app::services::RetrievalOptions
makeMcpRetrievalOptions(const yams::daemon::ClientConfig& daemonClientConfig) {
    yams::app::services::RetrievalOptions ropts;
    ropts.socketPath = daemonClientConfig.socketPath;
    ropts.requestTimeoutMs = 15000;
    ropts.headerTimeoutMs = 10000;
    ropts.bodyTimeoutMs = 60000;
    return ropts;
}

std::string normalizedTokenString(std::string value) {
    for (char& c : value) {
        if (!std::isalnum(static_cast<unsigned char>(c))) {
            c = ' ';
        } else {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
    }

    std::ostringstream out;
    std::istringstream in(value);
    std::string token;
    bool first = true;
    while (in >> token) {
        if (!first) {
            out << ' ';
        }
        out << token;
        first = false;
    }
    return out.str();
}

bool containsNormalizedToken(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return false;
    }
    if (haystack == needle) {
        return true;
    }
    std::string paddedHaystack;
    paddedHaystack.reserve(haystack.size() + 2);
    paddedHaystack.push_back(' ');
    paddedHaystack.append(haystack);
    paddedHaystack.push_back(' ');

    std::string paddedNeedle;
    paddedNeedle.reserve(needle.size() + 2);
    paddedNeedle.push_back(' ');
    paddedNeedle.append(needle);
    paddedNeedle.push_back(' ');
    return paddedHaystack.find(paddedNeedle) != std::string::npos;
}

#if !defined(YAMS_WASI)
struct SuggestContextSuppressionState {
    std::unordered_set<std::string> snapshotIds;
    std::unordered_set<std::string> resultIds;
};

SuggestContextSuppressionState
loadSuggestContextSuppressionState(metadata::MetadataRepository& repo, std::string_view sessionName,
                                   std::string_view normalizedQuery) {
    SuggestContextSuppressionState state;
    if (sessionName.empty() || normalizedQuery.empty()) {
        return state;
    }

    auto recent = repo.getRecentFeedbackEvents(25);
    if (!recent) {
        return state;
    }

    for (const auto& event : recent.value()) {
        if (event.eventType != "suggest_context_served") {
            continue;
        }

        auto payload = nlohmann::json::parse(event.payloadJson, nullptr, false);
        if (payload.is_discarded() || !payload.is_object()) {
            continue;
        }

        if (payload.value("session_name", std::string{}) != sessionName) {
            continue;
        }

        const auto payloadQuery = normalizedTokenString(
            payload.value("normalized_query", payload.value("query", std::string{})));
        if (payloadQuery != normalizedQuery) {
            continue;
        }

        if (auto it = payload.find("snapshot_ids"); it != payload.end() && it->is_array()) {
            for (const auto& snapshotId : *it) {
                if (snapshotId.is_string()) {
                    state.snapshotIds.insert(snapshotId.get<std::string>());
                }
            }
        }
        if (auto it = payload.find("served_result_ids"); it != payload.end() && it->is_array()) {
            for (const auto& resultId : *it) {
                if (resultId.is_string()) {
                    state.resultIds.insert(resultId.get<std::string>());
                }
            }
        }
        break;
    }

    return state;
}

#endif

static std::filesystem::path findGitRoot(const std::filesystem::path& start) {
    std::error_code ec;
    std::filesystem::path cur = std::filesystem::absolute(start, ec);
    if (ec)
        cur = start;
    while (!cur.empty()) {
        auto candidate = cur / ".git";
        if (std::filesystem::exists(candidate, ec)) {
            return cur;
        }
        auto parent = cur.parent_path();
        if (parent == cur)
            break;
        cur = parent;
    }
    return {};
}

static std::string sanitizeName(std::string s) {
    if (s.empty())
        return "project";
    for (auto& c : s) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_')) {
            c = '-';
        } else {
            c = static_cast<char>(std::tolower(c));
        }
    }
    return s;
}

static std::unordered_map<int64_t, std::string>
buildTraversalRelationHints(const yams::daemon::GraphQueryResponse& resp) {
    std::unordered_map<int64_t, std::string> hints;
    if (resp.connectedNodes.empty() || resp.edges.empty()) {
        return hints;
    }

    std::unordered_set<int64_t> connectedIds;
    connectedIds.reserve(resp.connectedNodes.size());

    std::unordered_map<int64_t, int32_t> distanceByNode;
    distanceByNode.reserve(resp.connectedNodes.size() + 1);
    for (const auto& node : resp.connectedNodes) {
        connectedIds.insert(node.nodeId);
        distanceByNode[node.nodeId] = node.distance;
    }
    distanceByNode[resp.originNode.nodeId] = 0;

    std::unordered_map<int64_t, std::unordered_map<std::string, std::size_t>> preferred;
    std::unordered_map<int64_t, std::unordered_map<std::string, std::size_t>> fallback;

    auto accumulate = [&](int64_t nodeId, int64_t otherNodeId, const std::string& relationLabel) {
        if (connectedIds.find(nodeId) == connectedIds.end()) {
            return;
        }

        fallback[nodeId][relationLabel] += 1;

        auto distIt = distanceByNode.find(nodeId);
        auto otherDistIt = distanceByNode.find(otherNodeId);
        if (distIt == distanceByNode.end() || otherDistIt == distanceByNode.end()) {
            return;
        }
        if (distIt->second > 0 && otherDistIt->second == distIt->second - 1) {
            preferred[nodeId][relationLabel] += 1;
        }
    };

    for (const auto& edge : resp.edges) {
        static const std::string kDefaultEdge = "edge";
        const std::string& relationLabel = edge.relation.empty() ? kDefaultEdge : edge.relation;
        accumulate(edge.srcNodeId, edge.dstNodeId, relationLabel);
        accumulate(edge.dstNodeId, edge.srcNodeId, relationLabel);
    }

    hints.reserve(resp.connectedNodes.size());
    for (const auto& node : resp.connectedNodes) {
        if (auto it = preferred.find(node.nodeId); it != preferred.end() && !it->second.empty()) {
            hints[node.nodeId] = yams::cli::formatRelationCounts(it->second);
            continue;
        }
        if (auto it = fallback.find(node.nodeId); it != fallback.end() && !it->second.empty()) {
            hints[node.nodeId] = yams::cli::formatRelationCounts(it->second);
            continue;
        }
        hints[node.nodeId] = "-";
    }

    return hints;
}

// Async variant to be used once MCP handlers become coroutine-based.

// No-op async bridge here; MCP dispatch below uses a detached thread
// to run yams::Task<> to completion off the io thread.

} // namespace

// In-band logging helper (level + message variant) - YAMS extension, not standard MCP
static nlohmann::json createLogNotification(const std::string& level, const std::string& message) {
    // NOTE: This is a YAMS-specific extension. Standard MCP only supports notifications/log from
    // client->server Wrap simple textual message inside a data object for consistency with the
    // (level,data,logger) overload
    nlohmann::json params = {{"level", level}, {"data", nlohmann::json{{"message", message}}}};
    return {{"jsonrpc", "2.0"}, {"method", "notifications/message"}, {"params", params}};
}

// Modern C++20 tool handler implementations
boost::asio::awaitable<Result<MCPSearchResponse>>
MCPServer::handleSearchDocuments(const MCPSearchRequest& req) {
#if defined(YAMS_WASI)
    (void)req;
    co_return Error{ErrorCode::NotSupported, "search is not supported on WASI build"};
#else
    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();
    yams::daemon::SearchRequest dreq;
    // Preserve the user's query as-is; rely on dedicated fields for filters
    dreq.query = req.query;
    // Heuristic: enable literal-text for code-like queries to avoid FTS parse issues
    {
        const std::string& q = dreq.query;
        bool punct = false;
        for (char c : q) {
            if (c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '"' ||
                c == '\'' || c == '\\' || c == '`' || c == ';') {
                punct = true;
                break;
            }
        }
        if (punct || q.find("::") != std::string::npos || q.find("->") != std::string::npos ||
            q.find("#include") != std::string::npos || q.find("std::") != std::string::npos) {
            dreq.literalText = true;
        }
    }
    // Pass through engine-level filters directly instead of injecting into the query
    std::string pathPattern = req.pathPattern;
    if (!pathPattern.empty()) {
        auto normalized = yams::app::services::utils::normalizeLookupPath(pathPattern);
        if (!normalized.hasWildcards && normalized.changed) {
            pathPattern = normalized.normalized;
        }
    }
    dreq.pathPattern = pathPattern;

    // Populate pathPatterns for multi-pattern server-side filtering
    if (!req.includePatterns.empty()) {
        dreq.pathPatterns = req.includePatterns;
    } else if (!pathPattern.empty()) {
        dreq.pathPatterns.push_back(pathPattern);
    }

    // CWD scoping: inject glob patterns to restrict results to a directory
    if (!req.cwd.empty()) {
        auto cwdPatterns = yams::app::services::utils::buildCwdScopePatterns(req.cwd);
        dreq.pathPatterns.insert(dreq.pathPatterns.end(), cwdPatterns.begin(), cwdPatterns.end());
        spdlog::debug("[MCP] Scoping search to CWD: {} ({} patterns)", req.cwd, cwdPatterns.size());
    }

    dreq.tags = req.tags;
    dreq.matchAllTags = req.matchAllTags;
    dreq.limit = req.limit;
    // Mirror CLI default: enable fuzzy matching by default
    dreq.fuzzy = true;
    dreq.similarity = (req.similarity > 0.0f) ? static_cast<double>(req.similarity) : 0.7;
    // Pass-through hash when present to enable hash-first search
    dreq.hashQuery = req.hash;
    dreq.searchType = req.type.empty() ? std::string("hybrid") : req.type;
    dreq.verbose = req.verbose;
    // Always request full hits from daemon; MCP can project paths-only reliably.
    // This avoids transport/result-shape inconsistencies seen with daemon pathsOnly mode.
    dreq.pathsOnly = false;
    dreq.showLineNumbers = req.lineNumbers;
    dreq.beforeContext = req.beforeContext;
    dreq.afterContext = req.afterContext;
    dreq.context = req.context;
    dreq.symbolRank = req.symbolRank;

    // Send early progress notification
    sendProgress("search", 0.0, "search started");

    // If keyword search was explicitly requested, skip vector scoring entirely
    if (dreq.searchType == "keyword") {
        dreq.similarity = 0.0; // disable vector path
    } else {
        // Note: removed pre-flight status check to avoid extra IPC round-trip.
        // The daemon handles degradation internally; this matches CLI behavior.
    }

    MCPSearchResponse out;
    // Propagate session explicitly (avoid env var coupling)
    std::string sessionName;
    if (!req.sessionName.empty()) {
        sessionName = req.sessionName;
    } else if (req.useSession) {
        auto sessionSvc = app::services::makeSessionService(nullptr);
        sessionName = sessionSvc->current().value_or("");
    }
    if (!sessionName.empty()) {
        spdlog::debug("[MCP] search: using session '{}'", sessionName);
        dreq.useSession = true;
        dreq.sessionName = sessionName;
        // Inject instance tag for session-scoped isolation (unless globalSearch)
        if (!req.tags.empty() || !instanceId_.empty()) {
            if (!instanceId_.empty() && !dreq.globalSearch) {
                dreq.tags.push_back("inst:" + instanceId_);
            }
        }
    }

    // Optional fast-first strategy: quick keyword preview before full hybrid
    if (dreq.searchType == "hybrid") {
        if (const char* ff = std::getenv("YAMS_MCP_SEARCH_FAST_FIRST"); ff && *ff && ff[0] != '0') {
            yams::daemon::SearchRequest kreq = dreq;
            kreq.searchType = "keyword";
            kreq.fuzzy = false;    // keep preview snappy
            kreq.similarity = 0.0; // skip vector
            kreq.limit = std::min<size_t>(dreq.limit > 0 ? dreq.limit : 10, 10);
            auto kres = co_await daemon_client_->streamingSearch(kreq);
            if (kres) {
                const auto& kr = kres.value();
                // Notify clients about quick keyword candidates
                json partial;
                partial["jsonrpc"] = "2.0";
                partial["method"] = "notifications/search_partial"; // YAMS extension
                json params;
                params["query"] = dreq.query;
                params["type"] = "keyword";
                params["total"] = kr.totalCount;
                if (dreq.pathsOnly) {
                    json paths = json::array();
                    for (const auto& item : kr.results) {
                        std::string path = !item.path.empty() ? item.path
                                                              : (item.metadata.count("path")
                                                                     ? item.metadata.at("path")
                                                                     : std::string());
                        if (path.empty())
                            path = item.id;
                        paths.push_back(path);
                    }
                    params["paths"] = std::move(paths);
                }
                partial["params"] = std::move(params);
                sendResponse(partial);
                sendProgress("search", 25.0, "keyword candidates ready");
            }
        }
    }

    // Streaming-only path for search to match CLI and reduce protocol complexity.
    // Direct co_await avoids the blocking promise/future pattern that stalls the IO thread.
    Result<yams::daemon::SearchResponse> res = co_await daemon_client_->streamingSearch(dreq);
    if (!res) {
        co_return res.error();
    }
    const auto& r = res.value();
    out.total = r.totalCount;
    out.type = "daemon";
    out.executionTimeMs = r.elapsed.count();
    out.traceId = r.traceId;
    out.queryInfo = r.queryInfo;
    out.searchStats = r.searchStats;

    std::optional<yams::daemon::ListResponse> tagListSnapshot;
    if (!req.tags.empty()) {
        yams::daemon::ListRequest tagReq;
        tagReq.tags = req.tags;
        tagReq.matchAllTags = req.matchAllTags;
        tagReq.limit = 10000;
        auto tagRes = co_await daemon_client_->list(tagReq);
        if (tagRes) {
            tagListSnapshot = tagRes.value();
        }
    }
    auto tagAllowed = [&](const yams::daemon::SearchResult& item) {
        if (!tagListSnapshot.has_value()) {
            return true;
        }
        static const std::string kEmpty;
        const std::string& hash = item.metadata.count("hash") ? item.metadata.at("hash") : kEmpty;
        const std::string& path =
            !item.path.empty() ? item.path
                               : (item.metadata.count("path") ? item.metadata.at("path") : kEmpty);
        for (const auto& it : tagListSnapshot->items) {
            if (!hash.empty() && it.hash == hash) {
                return true;
            }
            if (!path.empty() && it.path == path) {
                return true;
            }
        }
        return false;
    };
    // When pathsOnly was requested by the MCP client, populate the 'paths' field
    // to mirror CLI behavior and make it easy for clients to consume.
    if (req.pathsOnly) {
        out.pathsOnly = true;
        out.paths.reserve(r.results.size());
        for (const auto& item : r.results) {
            if (!tagAllowed(item)) {
                continue;
            }
            std::string path =
                !item.path.empty()
                    ? item.path
                    : (item.metadata.count("path") ? item.metadata.at("path") : std::string());
            if (path.empty())
                path = item.id; // last-resort fallback
            out.paths.push_back(std::move(path));
        }
        out.total = out.paths.size();
        sendProgress("search", 100.0, "done");
        co_return out;
    }
    // Full result objects (with robust path fallback)
    auto parseMetaFloat = [](const std::map<std::string, std::string>& md,
                             const std::string& key) -> std::optional<float> {
        auto it = md.find(key);
        if (it == md.end() || it->second.empty()) {
            return std::nullopt;
        }
        try {
            return std::stof(it->second);
        } catch (...) {
            return std::nullopt;
        }
    };
    auto parseMetaUint = [](const std::map<std::string, std::string>& md,
                            const std::string& key) -> std::optional<uint64_t> {
        auto it = md.find(key);
        if (it == md.end() || it->second.empty()) {
            return std::nullopt;
        }
        try {
            return static_cast<uint64_t>(std::stoull(it->second));
        } catch (...) {
            return std::nullopt;
        }
    };
    auto parseMetaBool = [](const std::map<std::string, std::string>& md,
                            const std::string& key) -> bool {
        auto it = md.find(key);
        if (it == md.end()) {
            return false;
        }
        auto v = it->second;
        std::transform(v.begin(), v.end(), v.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return v == "1" || v == "true" || v == "yes";
    };

    for (const auto& item : r.results) {
        if (!tagAllowed(item)) {
            continue;
        }
        MCPSearchResponse::Result m;
        m.id = item.id;
        m.hash = item.metadata.count("hash") ? item.metadata.at("hash") : "";
        m.title = item.title;
        // Fallback to metadata.path when daemon omitted direct path field
        m.path = !item.path.empty()
                     ? item.path
                     : (item.metadata.count("path") ? item.metadata.at("path") : std::string());
        m.score = static_cast<float>(item.score);
        m.snippet = item.snippet;
        m.vectorScore = parseMetaFloat(item.metadata, "vector_score");
        m.keywordScore = parseMetaFloat(item.metadata, "keyword_score");
        m.kgEntityScore = parseMetaFloat(item.metadata, "kg_entity_score");
        m.structuralScore = parseMetaFloat(item.metadata, "structural_score");
        m.lineStart = parseMetaUint(item.metadata, "line_start");
        if (!m.lineStart.has_value()) {
            m.lineStart = parseMetaUint(item.metadata, "line");
        }
        m.lineEnd = parseMetaUint(item.metadata, "line_end");
        m.charStart = parseMetaUint(item.metadata, "char_start");
        m.charEnd = parseMetaUint(item.metadata, "char_end");
        m.snippetTruncated = parseMetaBool(item.metadata, "snippet_truncated") ||
                             parseMetaBool(item.metadata, "truncated");
        out.results.push_back(std::move(m));
    }
    if (!req.tags.empty()) {
        out.total = out.results.size();
    }
    // Optional diff parity: when includeDiff=true and pathPattern is a local file, attach a
    // structured diff to the matching search result.
    if (req.includeDiff && !req.pathPattern.empty()) {
        auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(req.pathPattern);
        if (resolved.isLocalFile && resolved.absPath.has_value()) {
            try {
                // Find a matching result by filename equality
                std::string base = std::filesystem::path(*resolved.absPath).filename().string();
                size_t idx = static_cast<size_t>(-1);
                for (size_t i = 0; i < out.results.size(); ++i) {
                    const auto& rr = out.results[i];
                    if (!rr.path.empty() &&
                        std::filesystem::path(rr.path).filename().string() == base) {
                        idx = i;
                        break;
                    }
                }
                if (idx != static_cast<size_t>(-1)) {
                    // Prefer using known hash if present
                    std::string hash = out.results[idx].hash;
                    if (hash.empty()) {
                        // best-effort resolve by name if missing
                        auto appContext = app::services::AppContext{};
                        (void)appContext;
                    }
                    // Retrieve indexed content by hash when available
                    std::string indexedContent;
                    if (!hash.empty()) {
                        daemon::GetRequest greq;
                        greq.hash = hash;
                        greq.metadataOnly = false;
                        auto gr = co_await daemon_client_->get(greq);
                        if (gr)
                            indexedContent = gr.value().content;
                    }
                    // Load local content (limit ~1MB)
                    std::ifstream ifs(*resolved.absPath);
                    if (ifs) {
                        std::string local((std::istreambuf_iterator<char>(ifs)),
                                          std::istreambuf_iterator<char>());
                        if (!indexedContent.empty()) {
                            auto toLines = [](const std::string& s) {
                                std::vector<std::string> lines;
                                std::stringstream ss(s);
                                std::string line;
                                while (std::getline(ss, line))
                                    lines.push_back(line);
                                return lines;
                            };
                            auto a = toLines(local);
                            auto b = toLines(indexedContent);
                            std::vector<std::string> added;
                            std::vector<std::string> removed;
                            size_t i = 0, j = 0, shown = 0, maxShown = 200;
                            while ((i < a.size() || j < b.size()) && shown < maxShown) {
                                const std::string* la = (i < a.size()) ? &a[i] : nullptr;
                                const std::string* lb = (j < b.size()) ? &b[j] : nullptr;
                                if (la && lb && *la == *lb) {
                                    ++i;
                                    ++j;
                                    continue;
                                }
                                if (la) {
                                    removed.push_back(*la);
                                    ++i;
                                    ++shown;
                                }
                                if (lb && shown < maxShown) {
                                    added.push_back(*lb);
                                    ++j;
                                    ++shown;
                                }
                            }
                            bool truncated = (i < a.size() || j < b.size());
                            if (!added.empty() || !removed.empty()) {
                                out.results[idx].diff = json{{"added", added},
                                                             {"removed", removed},
                                                             {"truncated", truncated}};
                                out.results[idx].localInputFile = *resolved.absPath;
                            }
                        }
                    }
                }
            } catch (...) {
            }
        }
    }
    sendProgress("search", 100.0, "done");
    co_return out;
}

#endif

    boost::asio::awaitable<Result<MCPGrepResponse>> MCPServer::handleGrepDocuments(
        const MCPGrepRequest& req) {
#if defined(YAMS_WASI)
        (void)req;
        co_return Error{ErrorCode::NotSupported, "grep is not supported on WASI build"};
#else
    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();
    daemon::GrepRequest dreq;
    dreq.pattern = req.pattern;
    dreq.paths = req.paths;
    dreq.caseInsensitive = req.ignoreCase;
    dreq.wholeWord = req.word;
    dreq.invertMatch = req.invert;
    dreq.showLineNumbers = req.lineNumbers;
    dreq.showFilename = req.withFilename;
    dreq.countOnly = req.count;
    dreq.filesOnly = req.filesWithMatches;
    dreq.filesWithoutMatch = req.filesWithoutMatch;
    dreq.afterContext = req.afterContext;
    dreq.beforeContext = req.beforeContext;
    dreq.contextLines = req.context;
    dreq.colorMode = req.color;
    if (req.maxCount)
        dreq.maxMatches = *req.maxCount;

    // Pass include patterns to daemon and enable recursive by default
    if (!req.includePatterns.empty()) {
        dreq.includePatterns = req.includePatterns;
        dreq.recursive = true;
    }

    // CWD scoping: inject glob patterns to restrict results to a directory
    if (!req.cwd.empty()) {
        auto cwdPatterns = yams::app::services::utils::buildCwdScopePatterns(req.cwd);
        dreq.includePatterns.insert(dreq.includePatterns.end(), cwdPatterns.begin(),
                                    cwdPatterns.end());
        dreq.recursive = true;
        spdlog::debug("[MCP] Scoping grep to CWD: {} ({} patterns)", req.cwd, cwdPatterns.size());
    }

    std::vector<std::string> initial_paths = req.paths;
    if (!req.name.empty()) {
        initial_paths.push_back(req.name);
    }

    std::unordered_set<std::string> final_paths;
    for (const auto& p : initial_paths) {
        if (p.empty())
            continue;

        // Add original and normalized paths
        final_paths.insert(p);
        auto normalized = yams::app::services::utils::normalizeLookupPath(p);
        if (normalized.changed) {
            final_paths.insert(normalized.normalized);
        }

        // Add suffix match for non-wildcard paths
        const bool has_wild =
            (p.find('*') != std::string::npos) || (p.find('?') != std::string::npos);
        if (!has_wild) {
            if (req.subpath) {
                final_paths.insert(std::string("*") + p);
                if (normalized.changed) {
                    final_paths.insert(std::string("*") + normalized.normalized);
                }
            }
            // Basename fallback
            std::string base = p;
            try {
                base = std::filesystem::path(p).filename().string();
            } catch (...) {
            }
            if (!base.empty() && base != p) {
                final_paths.insert(std::string("*") + base);
            }
        }
    }
    dreq.paths.assign(final_paths.begin(), final_paths.end());

    // Pass user-provided tags through to the grep engine
    if (!req.tags.empty()) {
        dreq.filterTags.insert(dreq.filterTags.end(), req.tags.begin(), req.tags.end());
        if (req.matchAllTags) {
            dreq.matchAllTags = true;
        }
    }

    // Session scoping for grep follows the CLI contract: selectors become include filters,
    // while useSession only enables the hot path. Existing watched corpus documents do not
    // necessarily carry MCP instance tags, so avoid narrowing grep by instance here.
    if (req.useSession) {
        auto sess = app::services::makeSessionService(nullptr);
        auto pats = sess->activeIncludePatterns(req.sessionName.empty()
                                                    ? std::optional<std::string>{}
                                                    : std::optional<std::string>{req.sessionName});
        if (!pats.empty()) {
            for (const auto& p : pats) {
                dreq.includePatterns.push_back(p);
                const bool hasWild = p.find_first_of("*?") != std::string::npos;
                if (!hasWild && !p.empty()) {
                    dreq.includePatterns.push_back(p + "/**");
                    dreq.includePatterns.push_back(p + "/**/*");
                }
            }
            dreq.recursive = true;
        }
    }

    // Fast-first path: emit early semantic suggestions and return immediately if requested
    if (req.fastFirst) {
        try {
            if (transport_) {
                sendResponse(createLogNotification(
                    "info", "grep fast-first: returning semantic semantic suggestions"));
            }
        } catch (...) {
            // best-effort notification
        }
        yams::daemon::SearchRequest sreq;
        sreq.query = req.pattern;
        sreq.limit = 10;
        sreq.fuzzy = true;
        sreq.searchType = "hybrid";
        sreq.pathsOnly = false;
        auto sres = co_await daemon_client_->streamingSearch(sreq);
        if (sres) {
            const auto& sr = sres.value();
            MCPGrepResponse early;
            std::ostringstream oss_;
            for (const auto& item : sr.results) {
                const auto& p = !item.path.empty() ? item.path : item.title;
                if (!p.empty()) {
                    oss_ << "[S] " << p << "\n";
                }
            }
            early.output = oss_.str();
            early.matchCount = 0;
            early.fileCount = sr.results.size();
            co_return early;
        }
        // If semantic burst failed, fall through to standard grep
    }

    MCPGrepResponse out;
    // Propagate session explicitly (avoid env var coupling)
    std::string sessionName;
    if (!req.sessionName.empty()) {
        sessionName = req.sessionName;
    } else if (req.useSession) {
        auto sessionSvc = app::services::makeSessionService(nullptr);
        sessionName = sessionSvc->current().value_or("");
    }
    if (!sessionName.empty()) {
        spdlog::debug("[MCP] grep: using session '{}'", sessionName);
        dreq.useSession = true;
        dreq.sessionName = std::move(sessionName);
    }
    // Use shared daemon client directly — avoids creating a new DaemonClient + sync bridge
    auto res = co_await daemon_client_->streamingGrep(dreq);
    if (!res)
        co_return res.error();
    const auto& r = res.value();
    out.matchCount = r.totalMatches;
    out.fileCount = r.filesSearched;
    out.regexMatches = r.regexMatches;
    out.semanticMatches = r.semanticMatches;
    out.executionTimeMs = r.executionTimeMs;
    out.queryInfo = r.queryInfo;
    out.searchStats = r.searchStats;
    std::unordered_map<std::string, size_t> fileMatchCounts;
    for (const auto& m : r.matches) {
        if (!m.file.empty()) {
            fileMatchCounts[m.file]++;
        }
    }

    std::ostringstream oss;
    size_t maxOutputBytes = 16 * 1024;
    if (const char* env = std::getenv("YAMS_MCP_GREP_MAX_OUTPUT_BYTES")) {
        try {
            maxOutputBytes = std::max<size_t>(1024, static_cast<size_t>(std::stoul(env)));
        } catch (...) {
            maxOutputBytes = 16 * 1024;
        }
    }
    out.outputMaxBytes = maxOutputBytes;
    auto estimateEscapedSize = [](const std::string& s) -> size_t {
        size_t outSize = 0;
        for (unsigned char ch : s) {
            outSize += 1;
            if (ch == '"' || ch == '\\' || ch < 0x20) {
                outSize += 1;
            }
        }
        return outSize;
    };
    size_t outputBytes = 0;
    std::unordered_set<std::string> seenFiles;
    size_t matchIdx = 0;
    for (const auto& m : r.matches) {
        ++matchIdx;
        if (!m.file.empty())
            seenFiles.insert(m.file);

        MCPGrepResponse::Match sm;
        sm.file = m.file;
        sm.lineNumber = m.lineNumber;
        sm.lineText = m.line;
        sm.contextBefore = m.contextBefore;
        sm.contextAfter = m.contextAfter;
        sm.matchType = m.matchType;
        sm.confidence = m.confidence;
        sm.fileMatches = m.file.empty() ? 0 : fileMatchCounts[m.file];
        sm.matchId = (m.file.empty() ? std::string("unknown") : m.file) + ":" +
                     std::to_string(m.lineNumber) + ":" + std::to_string(matchIdx);
        out.matches.push_back(std::move(sm));

        std::string line;
        if (out.fileCount > 1 || req.withFilename) {
            if (!m.file.empty())
                line.append(m.file).append(":");
        }
        if (req.lineNumbers)
            line.append(std::to_string(m.lineNumber)).append(":");
        line.append(m.line).append("\n");

        const size_t escaped = estimateEscapedSize(line);
        if (outputBytes + escaped > maxOutputBytes) {
            out.outputTruncated = true;
            break;
        }
        oss << line;
        outputBytes += escaped;
    }
    if (out.fileCount == 0 && !seenFiles.empty()) {
        out.fileCount = seenFiles.size();
    }
    if (out.outputTruncated) {
        oss << "[truncated: output exceeded max bytes]\n";
    }
    out.output = oss.str();
    co_return out;
#endif
    }

    boost::asio::awaitable<Result<MCPDownloadResponse>> MCPServer::handleDownload(
        const MCPDownloadRequest& req) {
#if defined(YAMS_WASI)
        (void)req;
        co_return Error{ErrorCode::NotSupported, "download is not supported on WASI build"};
#else
    const bool verbose =
        (std::getenv("YAMS_POOL_VERBOSE") && std::string(std::getenv("YAMS_POOL_VERBOSE")) != "0" &&
         std::string(std::getenv("YAMS_POOL_VERBOSE")) != "false");
    if (verbose) {
        spdlog::debug("[MCP] download: url='{}' post_index={} store_only={} export='{}'", req.url,
                      req.postIndex, req.storeOnly, req.exportPath);
    }
    // Perform download locally using downloader manager (store into CAS), then optionally
    // post-index.
    MCPDownloadResponse mcp_response;

    // Build downloader request from MCP request
    yams::downloader::DownloadRequest dreq;
    dreq.url = req.url;
    dreq.concurrency = std::max(1, req.concurrency);
    dreq.chunkSizeBytes = req.chunkSizeBytes;
    dreq.timeout = std::chrono::milliseconds{req.timeoutMs};
    dreq.resume = req.resume;
    dreq.followRedirects = req.followRedirects;
    dreq.storeOnly = req.storeOnly;

    // Optional proxy
    if (!req.proxy.empty()) {
        dreq.proxy = req.proxy;
    }

    // Optional export path (only honored when not storeOnly)
    if (!req.exportPath.empty()) {
        dreq.exportPath = std::filesystem::path(req.exportPath);
    }

    // Overwrite policy
    if (req.overwrite == "always") {
        dreq.overwrite = yams::downloader::OverwritePolicy::Always;
    } else if (req.overwrite == "if-different-etag") {
        dreq.overwrite = yams::downloader::OverwritePolicy::IfDifferentEtag;
    } else {
        dreq.overwrite = yams::downloader::OverwritePolicy::Never;
    }

    // Headers
    for (const auto& h : req.headers) {
        auto pos = h.find(':');
        if (pos != std::string::npos) {
            yams::downloader::Header hdr;
            hdr.name = std::string(h.begin(), h.begin() + static_cast<std::ptrdiff_t>(pos));
            // skip possible space after colon
            std::string val = h.substr(pos + 1);
            if (!val.empty() && val.front() == ' ')
                val.erase(0, 1);
            hdr.value = std::move(val);
            dreq.headers.push_back(std::move(hdr));
        }
    }

    // Expected checksum (format "algo:hex")
    if (!req.checksum.empty()) {
        auto colon = req.checksum.find(':');
        if (colon != std::string::npos) {
            std::string algo = req.checksum.substr(0, colon);
            std::string hex = req.checksum.substr(colon + 1);
            yams::downloader::Checksum sum;
            if (algo == "sha256") {
                sum.algo = yams::downloader::HashAlgo::Sha256;
            } else if (algo == "sha512") {
                sum.algo = yams::downloader::HashAlgo::Sha512;
            } else if (algo == "md5") {
                sum.algo = yams::downloader::HashAlgo::Md5;
            }
            sum.hex = std::move(hex);
            dreq.checksum = std::move(sum);
        }
    }

    // Construct manager with defaults from config (fallback to request values)
    yams::downloader::StorageConfig storage{};
    yams::downloader::DownloaderConfig cfg{};

    // Read config and resolve storage path to match CLI behavior
    std::filesystem::path resolvedDataRoot;
    try {
        namespace fs = std::filesystem;

        // Load config.toml if present
        fs::path configPath;
        if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            configPath = fs::path(xdgConfigHome) / "yams" / "config.toml";
        } else if (const char* homeEnv = std::getenv("HOME")) {
            configPath = fs::path(homeEnv) / ".config" / "yams" / "config.toml";
        }

        std::map<std::string, std::map<std::string, std::string>> toml;
        if (!configPath.empty() && fs::exists(configPath)) {
            yams::config::ConfigMigrator migrator;
            if (auto parsed = migrator.parseTomlConfig(configPath)) {
                toml = std::move(parsed.value());
            }
        }

        // Downloader defaults from config
        if (auto it = toml.find("downloader"); it != toml.end()) {
            const auto& dl = it->second;
            if (auto f = dl.find("default_concurrency"); f != dl.end()) {
                try {
                    cfg.defaultConcurrency = std::stoi(f->second);
                } catch (...) {
                }
            }
            if (auto f = dl.find("default_chunk_size_bytes"); f != dl.end()) {
                try {
                    cfg.defaultChunkSizeBytes = static_cast<std::size_t>(std::stoull(f->second));
                } catch (...) {
                }
            }
            if (auto f = dl.find("default_timeout_ms"); f != dl.end()) {
                try {
                    cfg.defaultTimeout = std::chrono::milliseconds(std::stoll(f->second));
                } catch (...) {
                }
            }
            if (auto f = dl.find("follow_redirects"); f != dl.end()) {
                cfg.followRedirects = (f->second == "true");
            }
            if (auto f = dl.find("resume"); f != dl.end()) {
                cfg.resume = (f->second == "true");
            }
            if (auto f = dl.find("store_only"); f != dl.end()) {
                cfg.storeOnly = (f->second == "true");
            }
            if (auto f = dl.find("max_file_bytes"); f != dl.end()) {
                try {
                    cfg.maxFileBytes = static_cast<std::uint64_t>(std::stoull(f->second));
                } catch (...) {
                }
            }
        }

        // Determine data root (env > core.data_dir > XDG_DATA_HOME > ~/.local/share/yams)
        fs::path dataRoot;
        if (const char* envStorage = std::getenv("YAMS_STORAGE")) {
            if (envStorage && *envStorage)
                dataRoot = fs::path(envStorage);
        }
        if (dataRoot.empty()) {
            if (auto it = toml.find("core"); it != toml.end()) {
                const auto& core = it->second;
                if (auto f = core.find("data_dir"); f != core.end() && !f->second.empty()) {
                    std::string p = f->second;
                    if (!p.empty() && p.front() == '~') {
                        if (const char* home = std::getenv("HOME")) {
                            p = std::string(home) + p.substr(1);
                        }
                    }
                    dataRoot = fs::path(p);
                }
            }
        }
        if (dataRoot.empty()) {
            if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME")) {
                dataRoot = fs::path(xdgDataHome) / "yams";
            } else if (const char* homeEnv = std::getenv("HOME")) {
                dataRoot = fs::path(homeEnv) / ".local" / "share" / "yams";
            } else {
                dataRoot = fs::current_path() / "yams_data";
            }
        }
        resolvedDataRoot = std::move(dataRoot);

        // Allow explicit overrides via [storage] objects_dir/staging_dir
        fs::path objectsDir;
        fs::path stagingDir;
        if (auto it = toml.find("storage"); it != toml.end()) {
            const auto& st = it->second;
            if (auto f = st.find("objects_dir"); f != st.end() && !f->second.empty()) {
                objectsDir = fs::path(f->second);
            }
            if (auto f = st.find("staging_dir"); f != st.end() && !f->second.empty()) {
                stagingDir = fs::path(f->second);
            }
        }
        if (objectsDir.empty())
            objectsDir = dataRoot / "storage" / "objects";
        if (stagingDir.empty())
            stagingDir = dataRoot / "storage" / "staging";

        // Relative storage paths in config are resolved against data root.
        if (!resolvedDataRoot.empty()) {
            if (!objectsDir.empty() && objectsDir.is_relative()) {
                objectsDir = resolvedDataRoot / objectsDir;
            }
            if (!stagingDir.empty() && stagingDir.is_relative()) {
                stagingDir = resolvedDataRoot / stagingDir;
            }
        }
        storage.objectsDir = std::move(objectsDir);
        storage.stagingDir = std::move(stagingDir);

    } catch (...) {
        // Use defaults silently if config parsing fails
    }

    // Apply request-level overrides (request has priority)
    if (dreq.concurrency > 0)
        cfg.defaultConcurrency = dreq.concurrency;
    if (dreq.chunkSizeBytes > 0)
        cfg.defaultChunkSizeBytes = dreq.chunkSizeBytes;
    if (dreq.timeout.count() > 0)
        cfg.defaultTimeout = dreq.timeout;
    cfg.followRedirects = dreq.followRedirects;
    cfg.resume = dreq.resume;
    cfg.storeOnly = dreq.storeOnly;

    // Align downloader CAS root with daemon content store root so returned hashes are
    // retrievable by daemon get/cat operations.
    try {
        auto sres = co_await fetchDaemonStatus(DaemonStatusFetchMode::BestEffort);
        if (sres && sres.value()) {
            const auto& s = *sres.value();
            if (!s.contentStoreRoot.empty()) {
                namespace fs = std::filesystem;
                storage.objectsDir = fs::path(s.contentStoreRoot);
                if (storage.stagingDir.empty()) {
                    storage.stagingDir = fs::path(s.contentStoreRoot).parent_path() / "staging";
                }
                if (verbose) {
                    spdlog::debug("[MCP] download: using daemon content store root for CAS: '{}'",
                                  storage.objectsDir.string());
                }
            }
        }
    } catch (...) {
        // Best-effort alignment only; fall back to config/env-derived storage roots.
    }

    // Resolve and ensure staging directory exists to avoid regression
    try {
        namespace fs = std::filesystem;
        auto ensure_dir = [](const fs::path& p) -> bool {
            if (p.empty())
                return false;
            std::error_code ec;
            yams::common::ensureDirectories(p);
            return !ec && fs::exists(p);
        };

        if (storage.stagingDir.empty()) {
            // Prefer XDG_STATE_HOME, then HOME, then /tmp
            fs::path staging;
            if (const char* xdgState = std::getenv("XDG_STATE_HOME")) {
                staging = fs::path(xdgState) / "yams" / "staging";
            } else if (const char* homeEnv = std::getenv("HOME")) {
                staging = fs::path(homeEnv) / ".local" / "state" / "yams" / "staging";
            } else {
                staging = fs::path("/tmp") / "yams" / "staging";
            }
            storage.stagingDir = staging;
        }

        // Resolve late relative staging path against data root if available.
        if (storage.stagingDir.is_relative() && !resolvedDataRoot.empty()) {
            storage.stagingDir = resolvedDataRoot / storage.stagingDir;
        }

        if (!ensure_dir(storage.stagingDir)) {
            // Harden against invalid/unwritable config by falling back to a known writable root.
            fs::path fallbackStaging;
            if (!resolvedDataRoot.empty()) {
                fallbackStaging = resolvedDataRoot / "storage" / "staging";
            }
            if (fallbackStaging.empty()) {
                if (const char* xdgState = std::getenv("XDG_STATE_HOME"); xdgState && *xdgState) {
                    fallbackStaging = fs::path(xdgState) / "yams" / "staging";
                } else if (const char* homeEnv = std::getenv("HOME"); homeEnv && *homeEnv) {
                    fallbackStaging = fs::path(homeEnv) / ".local" / "state" / "yams" / "staging";
                } else {
                    fallbackStaging = fs::path("/tmp") / "yams" / "staging";
                }
            }
            if (!ensure_dir(fallbackStaging)) {
                co_return Error{ErrorCode::InternalError,
                                std::string("Failed to create staging dir: ") +
                                    storage.stagingDir.string()};
            }
            if (verbose) {
                spdlog::warn("[MCP] download: configured staging dir '{}' is not writable; "
                             "falling back to '{}'",
                             storage.stagingDir.string(), fallbackStaging.string());
            }
            storage.stagingDir = std::move(fallbackStaging);
        }

        // Optionally ensure objectsDir if provided
        if (!storage.objectsDir.empty()) {
            (void)ensure_dir(storage.objectsDir);
        }
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::InternalError,
                        std::string("Failed to prepare staging dir: ") + e.what()};
    }

    if (verbose) {
        spdlog::debug("[MCP] download: starting manager (conc={}, chunk={}, timeout_ms={}, "
                      "follow_redirects={}, resume={}, store_only={})",
                      cfg.defaultConcurrency, cfg.defaultChunkSizeBytes, cfg.defaultTimeout.count(),
                      cfg.followRedirects, cfg.resume, cfg.storeOnly);
        spdlog::debug("[MCP] download: staging_dir='{}' objects_dir='{}'",
                      storage.stagingDir.string(), storage.objectsDir.string());
    }
    auto manager = yams::downloader::makeDownloadManager(storage, cfg);
    auto dlRes = manager->download(dreq);
    if (!dlRes.ok()) {
        if (verbose) {
            spdlog::debug("[MCP] download: failed for url='{}' error='{}'", req.url,
                          dlRes.error().message);
        }
        co_return Error{ErrorCode::InternalError, dlRes.error().message};
    }

    const auto& final = dlRes.value();
    mcp_response.url = final.url;
    mcp_response.hash = final.hash;
    mcp_response.storedPath = final.storedPath.string();
    mcp_response.sizeBytes = final.sizeBytes;
    mcp_response.success = final.success;
    if (final.httpStatus)
        mcp_response.httpStatus = *final.httpStatus;
    if (final.etag)
        mcp_response.etag = *final.etag;
    if (final.lastModified)
        mcp_response.lastModified = *final.lastModified;
    if (final.checksumOk)
        mcp_response.checksumOk = *final.checksumOk;
    if (final.contentType)
        mcp_response.contentType = *final.contentType;
    if (final.suggestedName)
        mcp_response.suggestedName = *final.suggestedName;

    if (verbose) {
        spdlog::debug(
            "[MCP] download: success url='{}' hash='{}' stored='{}' size={} http={} etag='{}' "
            "lm='{}' checksum_ok={}",
            mcp_response.url, mcp_response.hash, mcp_response.storedPath, mcp_response.sizeBytes,
            (mcp_response.httpStatus ? *mcp_response.httpStatus : 0),
            (mcp_response.etag ? *mcp_response.etag : ""),
            (mcp_response.lastModified ? *mcp_response.lastModified : ""),
            (mcp_response.checksumOk ? (*mcp_response.checksumOk ? "true" : "false") : "n/a"));
    }
    // Optionally post-index the artifact via daemon
    // Optionally post-index the artifact via daemon
    if (mcp_response.success && req.postIndex) {
        if (verbose) {
            spdlog::debug("[MCP] post-index: starting for path='{}' collection='{}' "
                          "snapshot_id='{}' snapshot_label='{}'",
                          mcp_response.storedPath, req.collection, req.snapshotId,
                          req.snapshotLabel);
        }
        daemon::AddDocumentRequest addReq;
        // Resolve stored path to absolute under daemon-resolved content store root if relative
        std::filesystem::path absPath = std::filesystem::path(mcp_response.storedPath);
        if (absPath.is_relative()) {
            std::filesystem::path baseDir;
            try {
                auto sres = co_await fetchDaemonStatus(DaemonStatusFetchMode::BestEffort);
                if (sres && sres.value()) {
                    const auto& s = *sres.value();
                    if (!s.contentStoreRoot.empty()) {
                        baseDir = std::filesystem::path(s.contentStoreRoot).parent_path();
                    }
                }
            } catch (...) {
            }
            if (baseDir.empty()) {
                if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME");
                    xdgDataHome && *xdgDataHome) {
                    baseDir = std::filesystem::path(xdgDataHome) / "yams";
                } else if (const char* homeEnv = std::getenv("HOME"); homeEnv && *homeEnv) {
                    baseDir = std::filesystem::path(homeEnv) / ".local" / "share" / "yams";
                } else {
                    baseDir = std::filesystem::current_path();
                }
            }
            absPath = baseDir / absPath;
        }
        std::error_code canonEc;
        auto canonPath = std::filesystem::weakly_canonical(absPath, canonEc);
        if (!canonEc && !canonPath.empty()) {
            absPath = std::move(canonPath);
        }
        if (verbose) {
            spdlog::debug("[MCP] post-index: resolved stored path: '{}' -> '{}'",
                          mcp_response.storedPath, absPath.string());
        }
        addReq.path = absPath.string(); // normalized absolute path

        // Preserve a human-friendly name for name-based retrieval
        // Prefer Content-Disposition filename when available; fallback to URL basename
        try {
            std::string fname;
            if (final.suggestedName && !final.suggestedName->empty()) {
                fname = *final.suggestedName;
            } else {
                auto lastSlash = req.url.find_last_of('/');
                fname = (lastSlash == std::string::npos) ? req.url : req.url.substr(lastSlash + 1);
                auto q = fname.find('?');
                if (q != std::string::npos) {
                    fname.resize(q);
                }
            }
            if (fname.empty())
                fname = "downloaded_file";
            addReq.name = std::move(fname);
        } catch (...) {
            addReq.name = "downloaded_file";
        }
        if (final.contentType && !final.contentType->empty()) {
            addReq.mimeType = *final.contentType;
        }
        addReq.collection = req.collection;
        addReq.snapshotId = req.snapshotId;
        addReq.snapshotLabel = req.snapshotLabel;

        // Tags and metadata enrichment
        // 1) Default tag
        addReq.tags.clear();
        addReq.tags.push_back("downloaded");

        // 2) Derived tags: host:..., scheme:..., status:2xx/4xx/5xx
        auto extract_host = [](const std::string& url) -> std::string {
            auto p = url.find("://");
            if (p == std::string::npos)
                return {};
            auto rest = url.substr(p + 3);
            auto slash = rest.find('/');
            return (slash == std::string::npos) ? rest : rest.substr(0, slash);
        };
        auto extract_scheme = [](const std::string& url) -> std::string {
            auto p = url.find("://");
            return (p == std::string::npos) ? std::string{} : url.substr(0, p);
        };
        auto host = extract_host(req.url);
        auto scheme = extract_scheme(req.url);
        if (!host.empty())
            addReq.tags.push_back("host:" + host);
        if (!scheme.empty())
            addReq.tags.push_back("scheme:" + scheme);
        if (mcp_response.httpStatus) {
            int code = *mcp_response.httpStatus;
            std::string bucket = (code >= 200 && code < 300)   ? "2xx"
                                 : (code >= 400 && code < 500) ? "4xx"
                                                               : "5xx";
            addReq.tags.push_back("status:" + bucket);
        }

        // Include user tags at the end
        for (const auto& t : req.tags)
            addReq.tags.push_back(t);

        // 3) Provenance metadata
        addReq.metadata["extract_text"] = "true";
        addReq.metadata["raw_content"] = "false";
        addReq.metadata["source_url"] = req.url;
        if (mcp_response.httpStatus)
            addReq.metadata["http_status"] = std::to_string(*mcp_response.httpStatus);
        if (mcp_response.etag)
            addReq.metadata["etag"] = *mcp_response.etag;
        if (mcp_response.lastModified)
            addReq.metadata["last_modified"] = *mcp_response.lastModified;
        if (mcp_response.checksumOk)
            addReq.metadata["checksum_ok"] = *mcp_response.checksumOk ? "true" : "false";
        // RFC3339-like timestamp (best-effort)
        {
            auto now = std::chrono::system_clock::now();
            auto t = std::chrono::system_clock::to_time_t(now);
            std::stringstream ss;
            ss << std::put_time(std::localtime(&t), "%FT%T%z");
            addReq.metadata["downloaded_at"] = ss.str();
        }
        // Merge user metadata
        for (const auto& [k, v] : req.metadata)
            addReq.metadata[k] = v;

        // Call daemon to add/index the downloaded document via ingestion service
        app::services::AddOptions postOpts;
        postOpts.path = std::move(addReq.path);
        postOpts.name = std::move(addReq.name);
        postOpts.tags = std::move(addReq.tags);
        postOpts.metadata = std::move(addReq.metadata);
        postOpts.collection = std::move(addReq.collection);
        postOpts.snapshotId = std::move(addReq.snapshotId);
        postOpts.snapshotLabel = std::move(addReq.snapshotLabel);
        postOpts.noEmbeddings = addReq.noEmbeddings;
        postOpts.retries = 2;
        postOpts.timeoutMs = 30000;
        postOpts.backoffMs = 250;
        auto addres = co_await ingestion_svc_->addViaDaemonAsync(postOpts);
        if (!addres) {
            if (addres.error().code == ErrorCode::NotInitialized ||
                addres.error().code == ErrorCode::InvalidState) {
                spdlog::warn("[MCP] post-index: daemon not ready for path='{}' error='{}'",
                             addReq.path, addres.error().message);
            } else {
                spdlog::error("[MCP] post-index: daemon add failed for path='{}' error='{}'",
                              addReq.path, addres.error().message);
            }
            mcp_response.indexed = false;
            // Do not expose downloader-local hash as retrievable when indexing failed.
            // Agents commonly follow download -> get(hash); returning a non-indexed hash causes
            // repeated "File not found" lookups in daemon CAS.
            mcp_response.hash.clear();
        } else {
            mcp_response.indexed = true;
            const auto& addok = addres.value();
            spdlog::info("[MCP] post-index: indexed path='{}' hash='{}'", addReq.path, addok.hash);
            mcp_response.hash = addok.hash;
        }
    }

    co_return mcp_response;
}

#endif

        boost::asio::awaitable<Result<MCPDownloadJobsResponse>> MCPServer::handleDownloadJobs(
            const MCPDownloadJobsRequest& req) {
#if defined(YAMS_WASI)
            (void)req;
            co_return Error{ErrorCode::NotSupported,
                            "download_jobs is not supported on WASI build"};
#else
    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();
    auto* client = clientRes.value();

    MCPDownloadJobsResponse out;
    out.action = req.action;

    if ((req.action == "status" || req.action == "cancel") && req.jobId.empty()) {
        co_return Error{ErrorCode::InvalidArgument,
                        "job_id is required for action '" + req.action + "'"};
    }

    if (req.action == "status") {
        auto dres = co_await client->downloadStatus(yams::daemon::DownloadStatusRequest{req.jobId});
        if (!dres)
            co_return dres.error();
        out.job = makeMcpDownloadJobResponse(dres.value());
    } else if (req.action == "list") {
        auto dres = co_await client->listDownloadJobs(yams::daemon::ListDownloadJobsRequest{});
        if (!dres)
            co_return dres.error();
        out.jobs.reserve(dres.value().jobs.size());
        for (const auto& job : dres.value().jobs) {
            out.jobs.push_back(makeMcpDownloadJobResponse(job));
        }
    } else if (req.action == "cancel") {
        auto dres =
            co_await client->cancelDownloadJob(yams::daemon::CancelDownloadJobRequest{req.jobId});
        if (!dres)
            co_return dres.error();
        out.job = makeMcpDownloadJobResponse(dres.value());
    } else {
        co_return Error{ErrorCode::InvalidArgument, "Unknown action: " + req.action};
    }

    co_return out;
#endif
        }

        boost::asio::awaitable<Result<MCPStoreDocumentResponse>> MCPServer::handleStoreDocument(
            const MCPStoreDocumentRequest& req) {
#if defined(YAMS_WASI)
            (void)req;
            co_return Error{ErrorCode::NotSupported, "add is not supported on WASI build"};
#else
    if (!req.inputError.empty()) {
        co_return Error{ErrorCode::InvalidArgument, req.inputError};
    }

    // Fast path: reject completely empty inputs before contacting the daemon
    if ((req.path.empty() || req.path == "") && (req.name.empty() || req.name == "") &&
        (req.content.empty() || req.content == "")) {
        co_return Error{ErrorCode::InvalidArgument,
                        "Provide 'path' (a single file or directory) or inline 'content' with "
                        "'name'. For code-mode execute/add, use one add operation per path "
                        "instead of a 'paths' array."};
    }

    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();

    // Lightweight throttle: limit concurrent add/store operations to avoid stressing the IPC FSM
    {
        using namespace std::chrono_literals;
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer{exec};
        const int maxConcurrent = 2;
        const auto throttleDeadline = 2s;
        auto start = std::chrono::steady_clock::now();
        while (addInFlight_.load(std::memory_order_relaxed) >= maxConcurrent) {
            if (std::chrono::steady_clock::now() - start >= throttleDeadline) {
                co_return Error{ErrorCode::Timeout,
                                "Add busy: too many concurrent operations. Please retry."};
            }
            timer.expires_after(10ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        addInFlight_.fetch_add(1, std::memory_order_relaxed);
    }
    struct ScopeExit {
        std::function<void()> fn;
        ~ScopeExit() {
            if (fn)
                fn();
        }
    } _decr{[this]() noexcept { addInFlight_.fetch_sub(1, std::memory_order_relaxed); }};
    bool modelReadyFlag = true;
    // Convert MCP request → AddOptions, then route through ingestion service
    app::services::AddOptions aopts;

    // Resolve path: prefer explicit path; else treat name as path if it points to a file
    std::string candidatePath = req.path;
    std::string resolvedName;
    if (candidatePath.empty() && !req.name.empty()) {
        std::string tmp = req.name;
        if (!tmp.empty()) {
            tmp.erase(std::remove_if(tmp.begin(), tmp.end(),
                                     [](unsigned char c) { return c == '\n' || c == '\r'; }),
                      tmp.end());
            auto ltrim = [](std::string& s) {
                s.erase(s.begin(),
                        std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
            };
            auto rtrim = [](std::string& s) {
                s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); })
                            .base(),
                        s.end());
            };
            ltrim(tmp);
            rtrim(tmp);
        }
        if (!tmp.empty()) {
            namespace fs = std::filesystem;
            std::error_code ec;
            fs::path p(tmp);
            if (!p.is_absolute()) {
                if (const char* pwd = std::getenv("PWD")) {
                    fs::path cand = fs::path(pwd) / p;
                    if (fs::exists(cand, ec))
                        p = cand;
                }
            }
            if (fs::exists(p, ec) && fs::is_regular_file(p, ec)) {
                candidatePath = p.string();
                try {
                    resolvedName = p.filename().string();
                } catch (...) {
                }
            }
        }
    }

    // Normalize path
    {
        std::string _p = candidatePath;
        if (!_p.empty()) {
            std::string cleaned;
            cleaned.reserve(_p.size());
            for (char c : _p) {
                if (c == '\0')
                    break;
                if (c == '\r' || c == '\n')
                    continue;
                cleaned.push_back(c);
            }
            auto start = cleaned.find_first_not_of(" \t");
            if (start == std::string::npos) {
                cleaned.clear();
            } else {
                auto end = cleaned.find_last_not_of(" \t");
                cleaned = cleaned.substr(start, end - start + 1);
            }
            if (cleaned.size() > 1 && cleaned.back() == '/') {
                cleaned.pop_back();
            }
            _p = std::move(cleaned);
        }
        if (_p.rfind("file://", 0) == 0) {
            _p = _p.substr(7);
        }
        if (!_p.empty() && _p.front() == '~') {
            if (const char* home = std::getenv("HOME")) {
                _p = std::string(home) + _p.substr(1);
            }
        }
        if (!_p.empty() && _p.front() != '/') {
            std::vector<std::filesystem::path> bases;
            if (const char* pwd = std::getenv("PWD"); pwd && *pwd) {
                bases.emplace_back(pwd);
            }
            bases.emplace_back(std::filesystem::current_path());
            std::filesystem::path chosen = _p;
            bool resolved = false;
            for (const auto& base : bases) {
                std::filesystem::path cand = base / (_p.rfind("./", 0) == 0 ? _p.substr(2) : _p);
                std::error_code ec;
                if (std::filesystem::exists(cand, ec)) {
                    chosen = std::move(cand);
                    resolved = true;
                    break;
                }
            }
            _p = resolved ? chosen.string() : (_p.rfind("./", 0) == 0 ? _p.substr(2) : _p);
        }
        if (!_p.empty()) {
            std::error_code canonEc;
            auto canonPath = std::filesystem::weakly_canonical(_p, canonEc);
            if (!canonEc && !canonPath.empty()) {
                _p = canonPath.string();
            }
        }
        aopts.path = std::move(_p);
    }
    aopts.content = req.content;
    aopts.name = resolvedName.empty() ? req.name : resolvedName;
    aopts.mimeType = req.mimeType;
    aopts.disableAutoMime = req.disableAutoMime;
    aopts.noEmbeddings = req.noEmbeddings || !modelReadyFlag;
    aopts.collection = req.collection;
    aopts.snapshotId = req.snapshotId;
    aopts.snapshotLabel = req.snapshotLabel;
    aopts.recursive = req.recursive;
    aopts.includePatterns = req.includePatterns;
    aopts.excludePatterns = req.excludePatterns;
    aopts.tags = req.tags;
    // Inject instance and session tags for isolation
    if (!instanceId_.empty()) {
        aopts.tags.push_back("inst:" + instanceId_);
    }
    {
        auto sessionSvc = app::services::makeSessionService(nullptr);
        auto curSession = sessionSvc->current().value_or("");
        if (!curSession.empty()) {
            aopts.tags.push_back("session:" + curSession);
            aopts.metadata["session_uuid"] = "";
            if (auto sessInfo = sessionSvc->getSessionInfo(curSession);
                sessInfo && !sessInfo->uuid.empty()) {
                aopts.metadata["session_uuid"] = sessInfo->uuid;
            }
            aopts.metadata["instance_id"] = instanceId_;
        }
    }
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) {
            aopts.metadata[key] = value.get<std::string>();
        } else {
            aopts.metadata[key] = value.dump();
        }
    }
    aopts.retries = 2; // 3 attempts total
    aopts.timeoutMs = 30000;
    aopts.backoffMs = 250;

    // Validate before contacting daemon
    if (aopts.path.empty()) {
        if (aopts.content.empty()) {
            co_return Error{ErrorCode::InvalidArgument,
                            "Provide 'path' (a single file or directory) or inline 'content' + "
                            "'name'. For code-mode execute/add, use one add operation per path "
                            "instead of a 'paths' array."};
        }
        if (aopts.name.empty()) {
            co_return Error{ErrorCode::InvalidArgument,
                            "Provide 'name' when sending inline 'content' to add."};
        }
    }

    // Route through the shared ingestion service
    auto addRes = co_await ingestion_svc_->addViaDaemonAsync(aopts);
    if (!addRes) {
        co_return addRes.error();
    }

    if (!aopts.recursive) {
        yams::daemon::GetRequest probe;
        if (!aopts.name.empty()) {
            probe.name = std::move(aopts.name);
            probe.byName = true;
        } else {
            probe.hash = addRes.value().hash;
        }
        probe.metadataOnly = true;

        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer{exec};
        constexpr int kProbeAttempts = 200;
        constexpr auto kProbeDelay = std::chrono::milliseconds(25);
        for (int i = 0; i < kProbeAttempts; ++i) {
            auto probeRes = co_await daemon_client_->get(probe);
            if (probeRes) {
                break;
            }
            timer.expires_after(kProbeDelay);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    MCPStoreDocumentResponse out;
    // For directory adds, return empty hash to signal multi-file op
    bool hasContent = !aopts.content.empty();
    std::error_code ec;
    if (!hasContent && !aopts.path.empty() && aopts.recursive &&
        std::filesystem::is_directory(aopts.path, ec)) {
        co_return out;
    }
    out.hash = std::move(addRes.value().hash);
    out.bytesStored = 0;
    out.bytesDeduped = 0;
    co_return out;
}

boost::asio::awaitable<Result<MCPRetrieveDocumentResponse>>
MCPServer::handleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
#if defined(YAMS_WASI)
    (void)req;
    co_return Error{ErrorCode::NotSupported, "get is not supported on WASI build"};
#else
    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();
    // Convert MCP request to daemon request
    daemon::GetRequest daemon_req;
    daemon_req.hash = req.hash;
    daemon_req.name = req.name;
    daemon_req.byName = !req.name.empty();
    daemon_req.outputPath = req.outputPath;
    daemon_req.showGraph = req.graph;
    daemon_req.graphDepth = req.depth;
    daemon_req.metadataOnly = !req.includeContent;
    daemon_req.acceptCompressed = true;

    // Use shared daemon client directly — avoids creating a new DaemonClient + sync bridge
    auto dres = co_await clientRes.value()->get(daemon_req);
    if (!dres)
        co_return dres.error();

    MCPRetrieveDocumentResponse mcp_response;
    const auto& resp = dres.value();
    mcp_response.hash = resp.hash;
    mcp_response.path = resp.path;
    mcp_response.name = resp.name;
    mcp_response.size = resp.size;
    mcp_response.mimeType = resp.mimeType;
    mcp_response.compressed = resp.compressed;
    if (resp.compressionAlgorithm.has_value()) {
        mcp_response.compressionAlgorithm = resp.compressionAlgorithm.value();
    }
    if (resp.compressionLevel.has_value()) {
        mcp_response.compressionLevel = resp.compressionLevel.value();
    }
    if (resp.uncompressedSize.has_value()) {
        mcp_response.uncompressedSize = resp.uncompressedSize.value();
    }
    if (resp.compressedCrc32.has_value()) {
        mcp_response.compressedCrc32 = resp.compressedCrc32.value();
    }
    if (resp.uncompressedCrc32.has_value()) {
        mcp_response.uncompressedCrc32 = resp.uncompressedCrc32.value();
    }
    if (!resp.compressionHeader.empty()) {
        std::ostringstream oss;
        for (uint8_t byte : resp.compressionHeader) {
            oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
        }
        mcp_response.compressionHeader = oss.str();
    }
    if (resp.hasContent) {
        // Handle compressed content - need to decompress or strip header for JSON response
        constexpr size_t headerSize = compression::CompressionHeader::SIZE;
        if (resp.compressed && resp.content.size() > headerSize) {
            // Parse the compression header
            std::span<const std::byte> headerSpan(
                reinterpret_cast<const std::byte*>(resp.content.data()), headerSize);
            auto headerRes = compression::CompressionHeader::parse(headerSpan);
            if (headerRes && headerRes.value().validate()) {
                const auto& header = headerRes.value();
                auto algo = static_cast<compression::CompressionAlgorithm>(header.algorithm);

                if (algo == compression::CompressionAlgorithm::None) {
                    // Content stored uncompressed with header - strip header and return raw data
                    mcp_response.content = std::string(resp.content.data() + headerSize,
                                                       resp.content.size() - headerSize);
                } else {
                    // Content is compressed - decompress it
                    auto compressor =
                        compression::CompressionRegistry::instance().createCompressor(algo);
                    if (compressor && header.compressedSize > 0 &&
                        headerSize + header.compressedSize <= resp.content.size()) {
                        std::span<const std::byte> compressedSpan(
                            reinterpret_cast<const std::byte*>(resp.content.data() + headerSize),
                            header.compressedSize);
                        auto decompRes =
                            compressor->decompress(compressedSpan, header.uncompressedSize);
                        if (decompRes) {
                            const auto& decompData = decompRes.value();
                            mcp_response.content =
                                std::string(reinterpret_cast<const char*>(decompData.data()),
                                            decompData.size());
                        } else {
                            spdlog::warn("[MCP] Decompression failed: {}",
                                         decompRes.error().message);
                            mcp_response.content = resp.content;
                        }
                    } else {
                        spdlog::warn("[MCP] Invalid header sizes or no compressor for algo={}",
                                     static_cast<int>(algo));
                        mcp_response.content = resp.content;
                    }
                }
            } else {
                // Header invalid - return content as-is
                mcp_response.content = resp.content;
            }
        } else {
            mcp_response.content = resp.content;
        }

        size_t maxContentBytes = 32 * 1024;
        if (const char* env = std::getenv("YAMS_MCP_GET_MAX_CONTENT_BYTES")) {
            try {
                maxContentBytes = std::max<size_t>(1024, static_cast<size_t>(std::stoul(env)));
            } catch (...) {
                maxContentBytes = 32 * 1024;
            }
        }
        mcp_response.contentMaxBytes = static_cast<uint64_t>(maxContentBytes);
        if (mcp_response.content.has_value()) {
            if (mcp_response.content->size() > maxContentBytes) {
                mcp_response.content->resize(maxContentBytes);
                mcp_response.contentTruncated = true;
            }
            mcp_response.contentBytes = static_cast<uint64_t>(mcp_response.content->size());
        }
    }
    mcp_response.metadata = resp.metadata;
    mcp_response.graphEnabled = resp.graphEnabled;
    for (const auto& rel : resp.related) {
        json relatedJson = {{"hash", rel.hash}, {"path", rel.path}, {"distance", rel.distance}};
        mcp_response.related.push_back(relatedJson);
    }
    co_return mcp_response;
}

boost::asio::awaitable<Result<MCPListDocumentsResponse>>
MCPServer::handleListDocuments(const MCPListDocumentsRequest& req) {
#if defined(YAMS_WASI)
    (void)req;
    co_return Error{ErrorCode::NotSupported, "list is not supported on WASI build"};
#else
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    daemon::ListRequest daemon_req;
    // Map MCP filters to daemon ListRequest
    // Prioritize pattern, but fall back to name.
    if (!req.pattern.empty()) {
        auto normalized = yams::app::services::utils::normalizeLookupPath(req.pattern);
        if (normalized.changed && !normalized.hasWildcards) {
            daemon_req.namePattern = normalized.normalized;
        } else {
            daemon_req.namePattern = req.pattern;
        }
    } else if (!req.name.empty()) {
        auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(req.name);
        daemon_req.namePattern = resolved.pattern.empty() ? req.name : resolved.pattern;
    }
    if (req.useSession && daemon_req.namePattern.empty()) {
        auto sess = app::services::makeSessionService(nullptr);
        auto pats = sess->activeIncludePatterns(req.sessionName.empty()
                                                    ? std::optional<std::string>{}
                                                    : std::optional<std::string>{req.sessionName});
        if (!pats.empty()) {
            daemon_req.namePattern = pats.front();
        }
    }
    daemon_req.tags = req.tags;
    daemon_req.matchAllTags = req.matchAllTags;
    // Inject instance tag for session-scoped list
    if (req.useSession && !instanceId_.empty()) {
        daemon_req.tags.push_back("inst:" + instanceId_);
        daemon_req.matchAllTags = true;
    }
    daemon_req.fileType = req.type;
    daemon_req.mimeType = req.mime;
    daemon_req.extensions = req.extension;
    daemon_req.binaryOnly = req.binary;
    daemon_req.textOnly = req.text;
    daemon_req.pathsOnly = req.pathsOnly;
    daemon_req.recentCount = req.recent > 0 ? req.recent : 0;
    daemon_req.limit = req.limit > 0 ? static_cast<size_t>(req.limit) : daemon_req.limit;
    daemon_req.offset = req.offset > 0 ? req.offset : 0;
    daemon_req.sortBy = req.sortBy.empty() ? daemon_req.sortBy : req.sortBy;
    daemon_req.reverse =
        (req.sortOrder == "asc") ? true : false; // ascending means reverse order in server

    // Propagate session to services/daemon via environment for this handler
    std::string sessionName;
    if (!req.sessionName.empty()) {
        sessionName = req.sessionName;
    } else if (req.useSession) {
        auto sessionSvc = app::services::makeSessionService(nullptr);
        sessionName = sessionSvc->current().value_or("");
    }
    if (!sessionName.empty()) {
        spdlog::debug("[MCP] list: using session '{}'", sessionName);
        daemon_req.sessionId = std::move(sessionName);
    }
    // Use shared daemon client directly — avoids creating a new DaemonClient + sync bridge
    // per request (was the primary cause of ~70x MCP list slowdown vs daemon IPC).
    auto dres = co_await daemon_client_->streamingList(daemon_req);
    if (!dres)
        co_return dres.error();
    MCPListDocumentsResponse out;
    const auto& lr = dres.value();
    out.total = lr.totalCount;
    out.queryInfo = lr.queryInfo;
    out.listStats = lr.listStats;
    for (const auto& item : lr.items) {
        json docJson;
        docJson["hash"] = item.hash;
        docJson["path"] = item.path;
        docJson["name"] = item.name;
        docJson["size"] = item.size;
        docJson["mime_type"] = item.mimeType;
        docJson["created"] = item.created;
        docJson["modified"] = item.modified;
        docJson["indexed"] = item.indexed;
        // Synthetic tag when caller provided a concrete local file and it matches by suffix
        if (!req.name.empty()) {
            auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(req.name);
            if (resolved.isLocalFile && !item.path.empty()) {
                // If item.path ends with local abs path's filename, annotate
                try {
                    std::string base = std::filesystem::path(*resolved.absPath).filename().string();
                    if (std::filesystem::path(item.path).filename().string() == base) {
                        docJson["local_input_file"] = *resolved.absPath;
                    }
                } catch (...) {
                }
            }
        }
        // Optional diff block when caller asked for it and provided a local file path
        if (req.includeDiff && !req.name.empty()) {
            auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(req.name);
            if (resolved.isLocalFile && resolved.absPath.has_value()) {
                try {
                    // Read local content (limit ~1MB)
                    std::ifstream ifs(*resolved.absPath);
                    if (ifs) {
                        std::string local((std::istreambuf_iterator<char>(ifs)),
                                          std::istreambuf_iterator<char>());
                        // Get indexed content via shared daemon client (not a throwaway service)
                        daemon::GetRequest greq;
                        greq.hash = item.hash;
                        greq.metadataOnly = false;
                        auto gr = co_await daemon_client_->get(greq);
                        if (gr) {
                            const auto& resp = gr.value();
                            auto toLines = [](const std::string& s) {
                                std::vector<std::string> lines;
                                std::stringstream ss(s);
                                std::string line;
                                while (std::getline(ss, line))
                                    lines.push_back(line);
                                return lines;
                            };
                            auto a = toLines(local);
                            auto b = toLines(resp.content);
                            std::vector<std::string> added;
                            std::vector<std::string> removed;
                            size_t i = 0, j = 0, shown = 0, maxShown = 200;
                            while ((i < a.size() || j < b.size()) && shown < maxShown) {
                                const std::string* la = (i < a.size()) ? &a[i] : nullptr;
                                const std::string* lb = (j < b.size()) ? &b[j] : nullptr;
                                if (la && lb && *la == *lb) {
                                    ++i;
                                    ++j;
                                    continue;
                                }
                                if (la) {
                                    removed.push_back(*la);
                                    ++i;
                                    ++shown;
                                }
                                if (lb && shown < maxShown) {
                                    added.push_back(*lb);
                                    ++j;
                                    ++shown;
                                }
                            }
                            bool truncated = (i < a.size() || j < b.size());
                            if (!added.empty() || !removed.empty()) {
                                docJson["diff"] = json{{"added", added},
                                                       {"removed", removed},
                                                       {"truncated", truncated}};
                            }
                        }
                    }
                } catch (...) {
                }
            }
        }
        out.documents.push_back(std::move(docJson));
    }
    co_return out;
#endif
}

boost::asio::awaitable<Result<MCPStatsResponse>>
MCPServer::handleGetStats(const MCPStatsRequest& req) {
#if defined(YAMS_WASI)
    (void)req;
    co_return Error{ErrorCode::NotSupported, "stats is not supported on WASI build"};
#else
    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();
    auto* client = clientRes.value();
    daemon::GetStatsRequest daemon_req;
    daemon_req.showFileTypes = req.fileTypes;
    daemon_req.detailed = req.verbose;
    auto dres = co_await client->getStats(daemon_req);
    if (!dres)
        co_return dres.error();
    MCPStatsResponse out;
    const auto& resp = dres.value();
    out.totalObjects = resp.totalDocuments;
    out.totalBytes = resp.totalSize;
    out.uniqueHashes = resp.additionalStats.count("unique_hashes")
                           ? std::stoull(resp.additionalStats.at("unique_hashes"))
                           : 0;
    out.deduplicationSavings = resp.additionalStats.count("deduplicated_bytes")
                                   ? std::stoull(resp.additionalStats.at("deduplicated_bytes"))
                                   : 0;
    for (const auto& [key, value] : resp.documentsByType) {
        json ftJson;
        ftJson["extension"] = key;
        ftJson["count"] = value;
        out.fileTypes.push_back(ftJson);
    }
    out.additionalStats = resp.additionalStats;
    co_return out;
#endif
}

boost::asio::awaitable<Result<MCPStatusResponse>>
MCPServer::handleGetStatus(const MCPStatusRequest& req) {
#if defined(YAMS_WASI)
    (void)req;
    MCPStatusResponse out;
    out.running = true;
    out.ready = true;
    out.overallStatus = "wasi";
    out.lifecycleState = "in_process";
    out.lastError = "";
    out.version = "";
    out.uptimeSeconds = 0;
    out.requestsProcessed = 0;
    out.activeConnections = 0;
    out.memoryUsageMb = 0;
    out.cpuUsagePercent = 0.0;
    out.counters = json::object();
    out.readinessStates = json::object();
    out.initProgress = json::object();
    co_return out;
#else
    (void)req;
    auto sres = co_await fetchDaemonStatus(DaemonStatusFetchMode::Required);
    if (!sres)
        co_return sres.error();
    const auto& s = *sres.value();
    MCPStatusResponse out;
    out.running = s.running;
    out.ready = s.ready;
    out.overallStatus = s.overallStatus;
    out.lifecycleState = s.lifecycleState;
    out.lastError = s.lastError;
    out.version = s.version;
    out.uptimeSeconds = s.uptimeSeconds;
    out.requestsProcessed = s.requestsProcessed;
    out.activeConnections = s.activeConnections;
    out.memoryUsageMb = s.memoryUsageMb;
    out.cpuUsagePercent = s.cpuUsagePercent;
    out.counters = s.requestCounts;
    out.readinessStates = s.readinessStates;
    out.initProgress = s.initProgress;
    co_return out;
#endif
}

boost::asio::awaitable<Result<MCPAddDirectoryResponse>>
MCPServer::handleAddDirectory(const MCPAddDirectoryRequest& req) {
    // Daemon-first: prefer dispatcher AddDocument(path=dir, recursive=true)
    // Remove dependency on local appContext_.store to avoid "Content store not available" race.

    // Normalize and validate the directory path
    std::filesystem::path dir_path;
    try {
        std::string path_str = req.directoryPath;
        // Sanitize accidental newlines/CRs from JSON inputs and trim whitespace
        if (!path_str.empty()) {
            path_str.erase(std::remove_if(path_str.begin(), path_str.end(),
                                          [](unsigned char c) { return c == '\n' || c == '\r'; }),
                           path_str.end());
            auto ltrim = [](std::string& s) {
                s.erase(s.begin(),
                        std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
            };
            auto rtrim = [](std::string& s) {
                s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); })
                            .base(),
                        s.end());
            };
            ltrim(path_str);
            rtrim(path_str);
        }
        if (path_str.rfind("file://", 0) == 0) {
            path_str = path_str.substr(7);
        }
        if (!path_str.empty() && path_str.front() == '~') {
            if (const char* home = std::getenv("HOME")) {
                path_str = std::string(home) + path_str.substr(1);
            }
        }
        dir_path = std::filesystem::path(path_str);
        if (dir_path.is_relative()) {
            dir_path = std::filesystem::current_path() / dir_path;
        }
        dir_path = std::filesystem::weakly_canonical(dir_path);
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::InvalidArgument,
                        std::string("Invalid directory path: ") + e.what()};
    }

    std::error_code ec;
    if (!std::filesystem::is_directory(dir_path, ec) || ec) {
        co_return Error{ErrorCode::InvalidArgument,
                        "Path is not a directory: " + dir_path.string()};
    }

    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }

    // Build AddOptions and route through ingestion service
    app::services::AddOptions aopts;
    aopts.path = dir_path.string();
    aopts.recursive = true;
    aopts.tags = req.tags;
    for (const auto& [k, v] : req.metadata.items()) {
        if (v.is_string())
            aopts.metadata[k] = v.get<std::string>();
        else
            aopts.metadata[k] = v.dump();
    }
    aopts.includePatterns = req.includePatterns;
    aopts.excludePatterns = req.excludePatterns;
    aopts.collection = req.collection;
    aopts.retries = 2;
    aopts.timeoutMs = 30000;
    aopts.backoffMs = 250;

    auto addRes = co_await ingestion_svc_->addViaDaemonAsync(aopts);
    if (!addRes) {
        if (addRes.error().code == ErrorCode::NotInitialized) {
            co_return Error{ErrorCode::NotInitialized,
                            "Daemon: content store initializing; please retry shortly"};
        }
        co_return addRes.error();
    }

    MCPAddDirectoryResponse out;
    out.directoryPath = req.directoryPath;
    out.collection = req.collection;
    out.filesProcessed = 0;
    out.filesIndexed = 0;
    out.filesSkipped = 0;
    out.filesFailed = 0;
    out.results.clear();
    co_return out;
#endif
}

boost::asio::awaitable<Result<MCPDoctorResponse>>
MCPServer::handleDoctor(const MCPDoctorRequest& req) {
    (void)req;
    // Resolve socket and probe connectivity first so we can return structured info even if daemon
    // is unreachable.
    const auto& sock = daemon_client_config_.socketPath;
    if (sock.empty()) {
        try {
            daemon_client_config_.socketPath =
                yams::daemon::socket_utils::resolve_socket_path_config_first();
        } catch (...) {
        }
    }
    bool socketExists = false;
    bool connectable = false;
    try {
        std::error_code ec;
        socketExists = !sock.empty() && std::filesystem::exists(sock, ec) && !ec;
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::local::stream_protocol::socket probe(exec);
        if (!sock.empty()) {
            boost::system::error_code bec;
            probe.connect(boost::asio::local::stream_protocol::endpoint(sock.string()), bec);
            connectable = !bec;
            probe.close();
        }
    } catch (...) {
    }

    // Try daemon status; tolerate failure and still return a response with diagnostics
    yams::daemon::StatusResponse s{};
    bool haveStatus = false;
    if (auto sres = co_await fetchDaemonStatus(DaemonStatusFetchMode::BestEffort);
        sres && sres.value()) {
        s = std::move(*sres.value());
        haveStatus = true;
    }

    MCPDoctorResponse out;
    std::vector<std::string> issues;
    json details;
    details["overallStatus"] = haveStatus ? std::move(s.overallStatus) : std::string("unknown");
    details["lifecycleState"] = haveStatus ? std::move(s.lifecycleState) : std::string("unknown");
    details["lastError"] = haveStatus ? std::move(s.lastError) : std::string("unreachable");
    if (haveStatus)
        details["readiness"] = s.readinessStates;
    if (haveStatus)
        details["counters"] = s.requestCounts;
    details["socketPath"] = sock.empty() ? std::string("") : sock.string();
    details["socketExists"] = socketExists;
    details["connectable"] = connectable;

    if (!haveStatus || !s.running) {
        issues.push_back("daemon_not_running");
    }
    if (haveStatus && !s.ready) {
        issues.push_back("daemon_not_ready");
        for (const auto& [k, v] : s.readinessStates) {
            if (!v)
                issues.push_back(std::string("subsystem_not_ready:") + k);
        }
    }
    if (haveStatus && s.requestCounts.count("post_ingest_queued") &&
        s.requestCounts.at("post_ingest_queued") > 1000) {
        issues.push_back("post_ingest_backlog_high");
    }
    if (haveStatus && s.requestCounts.count("worker_queued") &&
        s.requestCounts.at("worker_queued") > 1000) {
        issues.push_back("worker_queue_high");
    }
    if (haveStatus && s.readinessStates.count("vector_embeddings_available") &&
        !s.readinessStates.at("vector_embeddings_available")) {
        issues.push_back("vector_embeddings_unavailable");
    }

    // Suggestions
    std::vector<std::string> suggestions;
    for (const auto& iss : issues) {
        if (iss == "vector_embeddings_unavailable") {
            suggestions.push_back(
                "Load or initialize an embedding model; check plugins and model provider logs.");
        } else if (iss.rfind("subsystem_not_ready:", 0) == 0) {
            suggestions.push_back(
                "Review logs for the listed subsystem and ensure dependencies are initialized.");
        } else if (iss == "post_ingest_backlog_high") {
            suggestions.push_back("Increase post-ingest threads via TuneAdvisor or pause adds "
                                  "until the queue drains.");
        } else if (iss == "worker_queue_high") {
            suggestions.push_back(
                "Reduce parallel requests or increase worker threads via TuneAdvisor.");
        } else if (iss == "daemon_not_ready") {
            suggestions.push_back(
                "Wait for initialization to complete or investigate lifecycle lastError.");
        } else if (iss == "daemon_not_running") {
            suggestions.push_back("Start or restart the daemon.");
        }
    }

    // Build summary
    std::string summary;
    if (issues.empty()) {
        summary = "All systems nominal.";
    } else {
        summary = std::to_string(issues.size()) + " issue(s) detected.";
    }
    details["suggestions"] = suggestions;
    out.summary = std::move(summary);
    out.issues = std::move(issues);
    out.details = std::move(details);
    co_return out;
}

boost::asio::awaitable<Result<MCPUpdateMetadataResponse>>
MCPServer::handleUpdateMetadata(const MCPUpdateMetadataRequest& req) {
    auto waitForTagVisibility =
        [this, &req](const std::string& expectedHash) -> boost::asio::awaitable<void> {
        if (expectedHash.empty() || req.tags.empty()) {
            co_return;
        }
        daemon::ListRequest probe;
        probe.tags = req.tags;
        probe.matchAllTags = true;
        probe.limit = 16;
        if (!req.name.empty()) {
            probe.namePattern = req.name;
        }
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer{exec};
        constexpr int kProbeAttempts = 80;
        constexpr auto kProbeDelay = std::chrono::milliseconds(25);
        for (int i = 0; i < kProbeAttempts; ++i) {
            auto probeRes = co_await daemon_client_->list(probe);
            if (probeRes) {
                const auto& lr = probeRes.value();
                auto it = std::find_if(lr.items.begin(), lr.items.end(),
                                       [&](const auto& item) { return item.hash == expectedHash; });
                if (it != lr.items.end()) {
                    break;
                }
            }
            timer.expires_after(kProbeDelay);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        co_return;
    };

    // Fast path: explicit single-hash update goes through daemon (if available)
    if (!req.hash.empty() && req.name.empty() && req.path.empty() && req.pattern.empty() &&
        req.names.empty()) {
        daemon::UpdateDocumentRequest daemon_req;
        daemon_req.hash = req.hash;
        daemon_req.name = req.name;
        daemon_req.addTags = req.tags;
        daemon_req.removeTags = req.removeTags;
        for (const auto& [key, value] : req.metadata.items()) {
            if (value.is_string())
                daemon_req.metadata[key] = value.get<std::string>();
            else
                daemon_req.metadata[key] = value.dump();
        }
        auto updateClientRes = requireDaemonClient();
        if (!updateClientRes)
            co_return updateClientRes.error();
        auto dres = co_await updateClientRes.value()->updateDocument(daemon_req);
        if (!dres)
            co_return dres.error();
        auto out = makeUpdateMetadataResponse(dres.value());
        if (out.success) {
            co_await waitForTagVisibility(dres.value().hash);
        }
        co_return out;
    }

    // Name-first single-target path: reuse robust name resolution from get (name path).
    // When a single name is provided (without other selectors), resolve to a single document
    // using RetrievalService::getByNameSmart and then perform a hash-based update via daemon.
    if (req.hash.empty() && !req.name.empty() && req.path.empty() && req.pattern.empty() &&
        req.names.empty()) {
        // Resolve name -> hash using the same strategy as handleGet (smart + fallback).
        // Normalize: expand leading '~' to HOME to allow user-friendly paths.
        std::string normName = req.name;
        try {
            if (!normName.empty() && normName.front() == '~') {
                if (const char* home = std::getenv("HOME")) {
                    normName = std::string(home) + normName.substr(1);
                }
            }
        } catch (...) {
        }
        // Reuse shared daemon client to avoid per-request connection overhead
        auto clientRes = requireDaemonClient();
        if (!clientRes)
            co_return clientRes.error();
        auto& rsvc = *retrieval_svc_;
        auto ropts = makeMcpRetrievalOptions(daemon_client_config_);
        auto docService = documentService_;
        if (!docService) {
            docService = app::services::makeDocumentService(appContext_);
            if (docService) {
                documentService_ = docService;
            }
        }
        auto resolver = [docService](const std::string& nm) -> Result<std::string> {
            if (docService)
                return docService->resolveNameToHash(nm);
            return Error{ErrorCode::NotInitialized, "Document service not available"};
        };

        // Prefer latest when disambiguating unless caller explicitly asked for oldest.
        const bool pickOldest = req.oldest;
        bool useSession = req.useSession; // allow client to bypass session filters
        auto grr = rsvc.getByNameSmart(normName, pickOldest, /*allowFuzzy*/ true,
                                       /*useSession*/ useSession, req.sessionName, ropts, resolver);
        // Rescue pass: if session-scoped lookup failed and caller allowed sessions, retry without
        // session to avoid false negatives when the active session excludes the target.
        if (!grr && useSession) {
            grr = rsvc.getByNameSmart(normName, pickOldest, /*allowFuzzy*/ true,
                                      /*useSession*/ false, std::string{}, ropts, resolver);
        }
        if (!grr) {
            // Fall back to simple basename-list matching similar to handleGet
            auto tryList =
                [&](const std::string& pat) -> std::optional<yams::daemon::ListResponse> {
                yams::app::services::ListOptions lreq;
                lreq.namePattern = pat;
                lreq.limit = 500;
                lreq.pathsOnly = false;
                auto lres = rsvc.list(lreq, ropts);
                if (lres && !lres.value().items.empty())
                    return lres.value();
                return std::nullopt;
            };
            auto bestMatch = [&](const std::vector<yams::daemon::ListEntry>& items)
                -> std::optional<yams::daemon::ListEntry> {
                if (items.empty())
                    return std::nullopt;
                if (req.latest || req.oldest) {
                    const yams::daemon::ListEntry* chosen = nullptr;
                    for (const auto& it : items) {
                        if (!chosen) {
                            chosen = &it;
                        } else if (req.oldest) {
                            if (it.indexed < chosen->indexed)
                                chosen = &it;
                        } else {
                            if (it.indexed > chosen->indexed)
                                chosen = &it;
                        }
                    }
                    return chosen ? std::optional<yams::daemon::ListEntry>(*chosen) : std::nullopt;
                }
                auto scoreName = [&](const std::string& base) -> int {
                    if (base == normName)
                        return 1000;
                    if (base.size() >= normName.size() && base.rfind(normName, 0) == 0)
                        return 800;
                    if (base.find(normName) != std::string::npos)
                        return 600;
                    int dl = static_cast<int>(std::abs((long)(base.size() - normName.size())));
                    return 400 - std::min(200, dl * 10);
                };
                int bestScore = -1;
                const yams::daemon::ListEntry* chosen = nullptr;
                for (const auto& it : items) {
                    std::string b;
                    try {
                        b = std::filesystem::path(it.path).filename().string();
                    } catch (...) {
                        b = it.name;
                    }
                    int sc = scoreName(b);
                    if (sc > bestScore) {
                        bestScore = sc;
                        chosen = &it;
                    }
                }
                return chosen ? std::optional<yams::daemon::ListEntry>(*chosen) : std::nullopt;
            };

            std::optional<yams::daemon::ListResponse> lr;
            lr = tryList(std::string("%/") + normName);
            if (!lr) {
                std::string stem = normName;
                try {
                    stem = std::filesystem::path(normName).stem().string();
                } catch (...) {
                }
                lr = tryList(std::string("%/") + stem + "%");
            }
            if (!lr)
                lr = tryList(std::string("%") + normName + "%");
            if (!lr || lr->items.empty()) {
                co_return Error{ErrorCode::NotFound, "No matching documents"};
            }
            auto cand = bestMatch(lr->items);
            if (!cand)
                co_return Error{ErrorCode::NotFound, "No matching documents"};
            // Build a synthetic GetResponse equivalent
            yams::app::services::GetOptions greq;
            greq.hash = cand->hash;
            greq.metadataOnly = true;
            auto grres = rsvc.get(greq, ropts);
            if (!grres)
                co_return grres.error();
            // Use resolved hash for daemon fast update
            daemon::UpdateDocumentRequest daemon_req;
            daemon_req.hash = std::move(cand->hash);
            daemon_req.addTags = req.tags;
            daemon_req.removeTags = req.removeTags;
            for (const auto& [key, value] : req.metadata.items()) {
                if (value.is_string())
                    daemon_req.metadata[key] = value.get<std::string>();
                else
                    daemon_req.metadata[key] = value.dump();
            }
            auto clientRes = requireDaemonClient();
            if (!clientRes)
                co_return clientRes.error();
            auto dres = co_await clientRes.value()->updateDocument(daemon_req);
            if (!dres)
                co_return dres.error();
            auto out = makeUpdateMetadataResponse(dres.value());
            if (out.success) {
                co_await waitForTagVisibility(dres.value().hash);
            }
            co_return out;
        }

        const auto& gr = grr.value();
        daemon::UpdateDocumentRequest daemon_req;
        daemon_req.hash = gr.hash;
        daemon_req.addTags = req.tags;
        daemon_req.removeTags = req.removeTags;
        for (const auto& [key, value] : req.metadata.items()) {
            if (value.is_string())
                daemon_req.metadata[key] = value.get<std::string>();
            else
                daemon_req.metadata[key] = value.dump();
        }
        auto updateClientRes = requireDaemonClient();
        if (!updateClientRes)
            co_return updateClientRes.error();
        auto dres = co_await updateClientRes.value()->updateDocument(daemon_req);
        if (!dres)
            co_return dres.error();
        auto out = makeUpdateMetadataResponse(dres.value());
        if (out.success) {
            co_await waitForTagVisibility(dres.value().hash);
        }
        co_return out;
    }

    // General path: resolve selectors via document service and apply updates (batch-safe)
    auto docService = app::services::makeDocumentService(appContext_);
    if (!docService) {
        co_return Error{ErrorCode::NotInitialized, "Document service not available"};
    }

    std::vector<app::services::DocumentEntry> targets;

    auto append_list = [&](const std::string& pat) {
        app::services::ListDocumentsRequest lreq;
        lreq.pattern = pat;
        lreq.limit = 10000;
        lreq.pathsOnly = false;
        auto lr = docService->list(lreq);
        if (lr && !lr.value().documents.empty()) {
            for (const auto& d : lr.value().documents)
                targets.push_back(d);
        }
    };

    // pattern selector
    if (!req.pattern.empty())
        append_list(req.pattern);
    // path selector (treat as exact first, then suffix match)
    if (!req.path.empty()) {
        append_list(req.path);
        append_list("%/" + req.path);
    }
    // explicit name selector
    if (!req.name.empty()) {
        append_list("%/" + req.name);
        append_list(req.name);
    }
    // multiple names
    for (const auto& n : req.names) {
        append_list("%/" + n);
        append_list(n);
    }

    // De-duplicate by hash
    std::sort(targets.begin(), targets.end(),
              [](const auto& a, const auto& b) { return a.hash < b.hash; });
    targets.erase(std::unique(targets.begin(), targets.end(),
                              [](const auto& a, const auto& b) { return a.hash == b.hash; }),
                  targets.end());

    // Disambiguation for single-name with multiple matches
    if ((req.latest || req.oldest) && !targets.empty()) {
        std::sort(targets.begin(), targets.end(), [](const auto& a, const auto& b) {
            return a.modified < b.modified; // ascending
        });
        app::services::DocumentEntry pick = req.latest ? targets.back() : targets.front();
        targets.clear();
        targets.push_back(std::move(pick));
    }

    MCPUpdateMetadataResponse out;
    out.matched = targets.size();
    if (targets.empty()) {
        out.success = false;
        out.message = "No matching documents";
        co_return out;
    }

    if (req.dryRun) {
        out.success = true;
        out.updated = 0;
        out.message = "Dry-run: would update " + std::to_string(out.matched) + " document(s)";
        co_return out;
    }

    std::size_t updated = 0;
    for (const auto& d : targets) {
        app::services::UpdateMetadataRequest u;
        u.name = d.name; // prefer stable name path
        // metadata
        if (req.metadata.is_object()) {
            for (auto it = req.metadata.begin(); it != req.metadata.end(); ++it) {
                if (it->is_string()) {
                    u.keyValues[it.key()] = it->get<std::string>();
                } else {
                    u.keyValues[it.key()] = it->dump();
                }
            }
        }
        // tags
        u.addTags = req.tags;
        u.removeTags = req.removeTags;
        u.atomic = true;
        auto ur = docService->updateMetadata(u);
        if (ur && ur.value().success) {
            ++updated;
            if (!ur.value().hash.empty())
                out.updatedHashes.push_back(ur.value().hash);
        }
    }
    out.updated = updated;
    out.success = updated > 0;
    out.message = (updated > 0) ? ("Updated " + std::to_string(updated) + " of " +
                                   std::to_string(out.matched) + " document(s)")
                                : "No changes applied";
    co_return out;
}

boost::asio::awaitable<Result<MCPSessionStartResponse>>
MCPServer::handleSessionStart(const MCPSessionStartRequest& req) {
    // Initialize or select session using JSON-backed service
    auto sessionSvc = app::services::makeSessionService(nullptr);
    if (!req.name.empty()) {
        if (!sessionSvc->exists(req.name)) {
            sessionSvc->init(req.name, req.description);
        } else {
            sessionSvc->use(req.name);
        }
    }

    uint64_t warmed = 0;
    if (req.warm) {
        // Prefer daemon offload
        yams::daemon::PrepareSessionRequest dreq;
        dreq.sessionName = req.name; // empty ok => current
        dreq.cores = req.cores;
        dreq.memoryGb = req.memoryGb;
        dreq.timeMs = req.timeMs;
        dreq.aggressive = req.aggressive;
        dreq.limit = static_cast<std::size_t>(req.limit);
        dreq.snippetLen = static_cast<std::size_t>(req.snippetLen);
        bool needFallback = true;
        if (auto ensure = ensureDaemonClient(); ensure) {
            auto resp = co_await daemon_client_->call<yams::daemon::PrepareSessionRequest>(dreq);
            if (resp) {
                warmed = resp.value().warmedCount;
                needFallback = false;
            }
        }
        if (needFallback) {
            // Fallback to local prepare (will be no-op without app context)
            app::services::PrepareBudget b{req.cores, req.memoryGb, req.timeMs, req.aggressive};
            warmed = sessionSvc->prepare(b, static_cast<std::size_t>(req.limit),
                                         static_cast<std::size_t>(req.snippetLen));
        }
    }

    MCPSessionStartResponse out;
    out.name = !req.name.empty() ? req.name : sessionSvc->current().value_or("");
    out.warmedCount = warmed;
    co_return out;
}

boost::asio::awaitable<Result<MCPSessionStopResponse>>
MCPServer::handleSessionStop(const MCPSessionStopRequest& req) {
    auto sessionSvc = app::services::makeSessionService(nullptr);
    if (!req.name.empty()) {
        if (sessionSvc->exists(req.name))
            sessionSvc->use(req.name);
    }
    if (req.clear)
        sessionSvc->clearMaterialized();
    MCPSessionStopResponse out;
    out.name = !req.name.empty() ? req.name : sessionSvc->current().value_or("");
    out.cleared = req.clear;
    co_return out;
}

boost::asio::awaitable<Result<MCPSessionPinResponse>>
MCPServer::handleSessionPin(const MCPSessionPinRequest& req) {
    // Validate inputs
    if (req.path.empty()) {
        co_return Error{ErrorCode::InvalidArgument, "path is required"};
    }
    // Create a document service bound to the MCP app context
    auto docService = app::services::makeDocumentService(appContext_);
    if (!docService) {
        co_return Error{ErrorCode::NotInitialized, "Document service not available"};
    }

    // List documents matching the provided pattern
    app::services::ListDocumentsRequest lreq;
    lreq.pattern = req.path;
    lreq.limit = 10000; // reasonable safeguard
    auto lres = docService->list(lreq);
    if (!lres) {
        co_return lres.error();
    }

    // Update each matched document: add 'pinned' tag and provided tags/metadata
    std::size_t updated = 0;
    for (const auto& d : lres.value().documents) {
        app::services::UpdateMetadataRequest u;
        u.name = d.name;

        // Add 'pinned' tag plus any user-specified tags
        u.addTags = req.tags;
        if (std::find(u.addTags.begin(), u.addTags.end(), "pinned") == u.addTags.end()) {
            u.addTags.push_back("pinned");
        }

        // Metadata: copy string values as-is; non-strings are serialized
        if (req.metadata.is_object()) {
            for (auto it = req.metadata.begin(); it != req.metadata.end(); ++it) {
                if (it->is_string()) {
                    u.keyValues[it.key()] = it->get<std::string>();
                } else {
                    u.keyValues[it.key()] = it->dump();
                }
            }
        }
        // Explicitly set pinned=true metadata for discoverability
        u.keyValues["pinned"] = "true";

        auto ur = docService->updateMetadata(u);
        if (ur && ur.value().success) {
            ++updated;
        }
    }

    MCPSessionPinResponse out;
    out.updated = updated;
    co_return out;
}

boost::asio::awaitable<Result<MCPSessionUnpinResponse>>
MCPServer::handleSessionUnpin(const MCPSessionUnpinRequest& req) {
    // Validate inputs
    if (req.path.empty()) {
        co_return Error{ErrorCode::InvalidArgument, "path is required"};
    }
    // Create a document service bound to the MCP app context
    auto docService = app::services::makeDocumentService(appContext_);
    if (!docService) {
        co_return Error{ErrorCode::NotInitialized, "Document service not available"};
    }

    // List documents matching the provided pattern
    app::services::ListDocumentsRequest lreq;
    lreq.pattern = req.path;
    lreq.limit = 10000; // reasonable safeguard
    auto lres = docService->list(lreq);
    if (!lres) {
        co_return lres.error();
    }

    // Update each matched document: remove 'pinned' tag and set pinned=false metadata
    std::size_t updated = 0;
    for (const auto& d : lres.value().documents) {
        app::services::UpdateMetadataRequest u;
        u.name = d.name;

        u.removeTags.push_back("pinned");
        u.keyValues["pinned"] = "false";

        auto ur = docService->updateMetadata(u);
        if (ur && ur.value().success) {
            ++updated;
        }
    }

    MCPSessionUnpinResponse out;
    out.updated = updated;
    co_return out;
}

boost::asio::awaitable<Result<MCPSessionWatchResponse>>
MCPServer::handleSessionWatch(const MCPSessionWatchRequest& req) {
    auto sessionSvc = app::services::makeSessionService(nullptr);
    std::error_code ec;
    std::filesystem::path cwd = std::filesystem::current_path(ec);
    if (ec) {
        co_return Error{ErrorCode::InvalidArgument, "Failed to resolve current working directory"};
    }

    std::filesystem::path root;
    if (!req.root.empty()) {
        root = std::filesystem::path(req.root);
    } else {
        root = findGitRoot(cwd);
        if (root.empty())
            root = std::move(cwd);
    }
    auto absRoot = std::filesystem::absolute(root, ec);
    if (!ec)
        root = std::move(absRoot);
    const std::string rootStr = root.string();

    std::string targetSession;
    if (!req.session.empty()) {
        targetSession = req.session;
    } else if (auto cur = sessionSvc->current(); cur && !cur->empty()) {
        targetSession = *cur;
    } else {
        std::string base = root.filename().string();
        if (base.empty())
            base = "project";
        targetSession = "proj-" + sanitizeName(base) + "-" + yams::core::shortHash(rootStr);
    }

    bool created = false;
    if (!sessionSvc->exists(targetSession)) {
        if (!req.allowCreate) {
            co_return Error{ErrorCode::NotFound, "Session not found: " + targetSession};
        }
        sessionSvc->init(targetSession, "auto: " + rootStr);
        created = true;
    }

    std::optional<std::string> previousSession;
    bool changedCurrent = false;
    auto ensureCurrent = [&]() {
        if (req.setCurrent) {
            sessionSvc->use(targetSession);
            changedCurrent = true;
            return;
        }
        previousSession = sessionSvc->current();
        if (!previousSession || *previousSession != targetSession) {
            sessionSvc->use(targetSession);
            changedCurrent = true;
        }
    };

    bool selectorAdded = false;
    if (req.addSelector) {
        ensureCurrent();
        auto selectors = sessionSvc->listPathSelectors(targetSession);
        if (std::find(selectors.begin(), selectors.end(), rootStr) == selectors.end()) {
            sessionSvc->addPathSelector(rootStr, {}, {});
            selectorAdded = true;
        }
    } else if (req.setCurrent) {
        ensureCurrent();
    }

    if (req.intervalMs > 0)
        sessionSvc->setWatchIntervalMs(req.intervalMs, targetSession);
    sessionSvc->enableWatch(req.enable, targetSession);

    if (!req.setCurrent && changedCurrent) {
        if (previousSession && *previousSession != targetSession) {
            sessionSvc->use(*previousSession);
        } else if (!previousSession) {
            sessionSvc->close();
        }
    }

    MCPSessionWatchResponse out;
    out.session = targetSession;
    out.root = rootStr;
    out.enabled = sessionSvc->watchEnabled(targetSession);
    out.intervalMs = sessionSvc->watchIntervalMs(targetSession);
    out.created = created;
    out.selectorAdded = selectorAdded;
    co_return out;
}

boost::asio::awaitable<Result<MCPGraphResponse>>
MCPServer::handleGraphQuery(const MCPGraphRequest& req) {
    // Dispatch ingest action to the dedicated handler
    if (req.action == "ingest") {
        co_return co_await handleKgIngest(req);
    }

    const auto symbolToJson = [](const auto& s) {
        json sj{{"node_key", s.nodeKey},
                {"label", s.label},
                {"qualified_name", s.qualifiedName},
                {"kind", s.kind},
                {"file_path", s.filePath}};
        if (s.startLine.has_value())
            sj["start_line"] = *s.startLine;
        if (s.endLine.has_value())
            sj["end_line"] = *s.endLine;
        return sj;
    };
    const auto relationToJson = [](const auto& r) {
        return json{{"relation", r.relation},
                    {"source", r.sourceLabel},
                    {"source_node_key", r.sourceNodeKey},
                    {"target", r.targetLabel},
                    {"target_node_key", r.targetNodeKey}};
    };

    if (req.action == "lookup") {
        auto clientRes = requireDaemonClient();
        if (!clientRes)
            co_return clientRes.error();
        yams::daemon::GraphSymbolLookupRequest dreq;
        dreq.symbol = req.symbol.empty() ? req.name : req.symbol;
        if (!req.file.empty()) {
            dreq.hasFile = true;
            dreq.file = req.file;
        }
        if (req.line >= 0) {
            dreq.hasLine = true;
            dreq.line = req.line;
        }
        auto res = co_await clientRes.value()->call<yams::daemon::GraphSymbolLookupRequest>(dreq);
        if (!res)
            co_return res.error();
        const auto& resp = res.value();
        MCPGraphResponse out;
        out.action = "lookup";
        json nav;
        nav["symbol"] = resp.symbol;
        nav["ambiguous"] = resp.ambiguous;
        nav["truncated"] = resp.truncated;
        nav["matches"] = json::array();
        for (const auto& m : resp.matches)
            nav["matches"].push_back(symbolToJson(m));
        nav["trail"] = json::array();
        for (const auto& r : resp.trail)
            nav["trail"].push_back(relationToJson(r));
        nav["warnings"] = resp.warnings;
        out.navResult = std::move(nav);
        co_return out;
    }

    if (req.action == "impact") {
        auto clientRes = requireDaemonClient();
        if (!clientRes)
            co_return clientRes.error();
        yams::daemon::GraphImpactRequest dreq;
        dreq.symbol = req.symbol.empty() ? req.name : req.symbol;
        dreq.depth = static_cast<uint64_t>(req.depth);
        auto res = co_await clientRes.value()->call<yams::daemon::GraphImpactRequest>(dreq);
        if (!res)
            co_return res.error();
        const auto& resp = res.value();
        MCPGraphResponse out;
        out.action = "impact";
        json nav;
        nav["symbol"] = resp.symbol;
        nav["truncated"] = resp.truncated;
        nav["affected_symbols"] = json::array();
        for (const auto& s : resp.affectedSymbols)
            nav["affected_symbols"].push_back(symbolToJson(s));
        nav["relationships"] = json::array();
        for (const auto& r : resp.relationships)
            nav["relationships"].push_back(relationToJson(r));
        nav["warnings"] = resp.warnings;
        out.navResult = std::move(nav);
        co_return out;
    }

    if (req.action == "trace") {
        auto clientRes = requireDaemonClient();
        if (!clientRes)
            co_return clientRes.error();
        yams::daemon::GraphTraceRequest dreq;
        dreq.from = req.fromSymbol;
        dreq.to = req.toSymbol;
        if (req.depth > 1) {
            dreq.maxDepth = static_cast<uint64_t>(req.depth);
        }
        auto res = co_await clientRes.value()->call<yams::daemon::GraphTraceRequest>(dreq);
        if (!res)
            co_return res.error();
        const auto& resp = res.value();
        MCPGraphResponse out;
        out.action = "trace";
        json nav;
        nav["from"] = resp.from;
        nav["to"] = resp.to;
        nav["found"] = resp.found;
        nav["truncated"] = resp.truncated;
        nav["path"] = json::array();
        for (const auto& r : resp.path)
            nav["path"].push_back(relationToJson(r));
        nav["warnings"] = resp.warnings;
        out.navResult = std::move(nav);
        co_return out;
    }

    if (req.action == "affected_tests") {
        auto clientRes = requireDaemonClient();
        if (!clientRes)
            co_return clientRes.error();
        yams::daemon::GraphAffectedTestsRequest dreq;
        dreq.changedFiles = req.changedFiles;
        if (req.depth > 1) {
            dreq.depth = static_cast<uint64_t>(req.depth);
        }
        dreq.testPathPattern = req.testPattern;
        auto res = co_await clientRes.value()->call<yams::daemon::GraphAffectedTestsRequest>(dreq);
        if (!res)
            co_return res.error();
        const auto& resp = res.value();
        MCPGraphResponse out;
        out.action = "affected_tests";
        json nav;
        nav["changed_files"] = resp.changedFiles;
        nav["affected_tests"] = resp.affectedTests;
        nav["truncated"] = resp.truncated;
        nav["relationships"] = json::array();
        for (const auto& r : resp.relationships)
            nav["relationships"].push_back(relationToJson(r));
        nav["warnings"] = resp.warnings;
        out.navResult = std::move(nav);
        co_return out;
    }

    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();

    // Build daemon GraphQueryRequest from MCPGraphRequest
    yams::daemon::GraphQueryRequest dreq;
    dreq.documentHash = req.hash;
    dreq.documentName = req.name;
    dreq.nodeKey = req.nodeKey;
    dreq.nodeId = req.nodeId;

    // Mode flags
    dreq.listTypes = req.listTypes;
    dreq.listByType = !req.listType.empty();
    dreq.nodeType = req.listType;
    dreq.isolatedMode = req.isolated;
    if (!req.relationFilters.empty()) {
        dreq.isolatedRelation = req.relationFilters.front();
    }

    // Traversal options
    dreq.relationFilters = req.relationFilters;
    if (!req.relation.empty() && dreq.relationFilters.empty()) {
        dreq.relationFilters.push_back(req.relation);
    }
    dreq.maxDepth = req.depth;
    dreq.limit = static_cast<uint32_t>(req.limit);
    dreq.offset = static_cast<uint32_t>(req.offset);
    dreq.reverseTraversal = req.reverse;

    // Output control
    dreq.includeNodeProperties = req.includeNodeProperties;
    dreq.includeEdgeProperties = req.includeEdgeProperties;
    dreq.hydrateFully = req.hydrateFully;

    // Snapshot scoping
    dreq.scopeToSnapshot = req.scopeSnapshot;

    // Send the request
    auto res = co_await clientRes.value()->call<yams::daemon::GraphQueryRequest>(dreq);
    if (!res) {
        co_return res.error();
    }

    const auto& resp = res.value();
    const auto traversalHints = buildTraversalRelationHints(resp);

    // Build MCP response
    MCPGraphResponse out;

    // Convert origin node to JSON
    out.origin = json::object();
    out.origin["nodeId"] = resp.originNode.nodeId;
    out.origin["type"] = resp.originNode.type;
    out.origin["nodeKey"] = resp.originNode.nodeKey;
    out.origin["documentHash"] = resp.originNode.documentHash;
    out.origin["documentPath"] = resp.originNode.documentPath;
    if (!resp.originNode.label.empty()) {
        out.origin["label"] = resp.originNode.label;
    }
    if (!resp.originNode.snapshotId.empty()) {
        out.origin["snapshotId"] = resp.originNode.snapshotId;
    }
    if (!resp.originNode.properties.empty()) {
        out.origin["properties"] = json::parse(resp.originNode.properties, nullptr, false);
    }

    // Convert connected nodes to JSON array
    out.connectedNodes = json::array();
    for (const auto& node : resp.connectedNodes) {
        json jnode;
        jnode["nodeId"] = node.nodeId;
        jnode["type"] = node.type;
        jnode["nodeKey"] = node.nodeKey;
        jnode["documentHash"] = node.documentHash;
        jnode["documentPath"] = node.documentPath;
        jnode["distance"] = node.distance;
        if (auto hintIt = traversalHints.find(node.nodeId); hintIt != traversalHints.end()) {
            jnode["via"] = hintIt->second;
        }
        if (!node.label.empty()) {
            jnode["label"] = node.label;
        }
        if (!node.snapshotId.empty()) {
            jnode["snapshotId"] = node.snapshotId;
        }
        if (!node.properties.empty()) {
            jnode["properties"] = json::parse(node.properties, nullptr, false);
        }
        out.connectedNodes.push_back(std::move(jnode));
    }

    // Convert node type counts
    out.nodeTypeCounts = json::object();
    for (const auto& [type, count] : resp.nodeTypeCounts) {
        out.nodeTypeCounts[type] = count;
    }

    // Copy statistics
    out.totalNodesFound = resp.totalNodesFound;
    out.totalEdgesTraversed = resp.totalEdgesTraversed;
    out.truncated = resp.truncated;
    out.maxDepthReached = resp.maxDepthReached;
    out.queryTimeMs = resp.queryTimeMs;
    out.kgAvailable = resp.kgAvailable;
    out.warning = resp.warning;

    co_return out;
}

boost::asio::awaitable<Result<MCPGraphResponse>>
MCPServer::handleKgIngest(const MCPGraphRequest& req) {
    auto clientRes = requireDaemonClient();
    if (!clientRes)
        co_return clientRes.error();

    // Translate MCP DTO → daemon KgIngestRequest
    yams::daemon::KgIngestRequest dreq;
    dreq.documentHash = req.documentHash;
    dreq.skipExistingNodes = req.skipExistingNodes;
    dreq.skipExistingEdges = req.skipExistingEdges;

    dreq.nodes.reserve(req.nodes.size());
    for (const auto& n : req.nodes) {
        yams::daemon::KgIngestNode dn;
        dn.nodeKey = n.nodeKey;
        dn.label = n.label;
        dn.type = n.type;
        if (!n.properties.is_null())
            dn.properties = n.properties.dump();
        dreq.nodes.push_back(std::move(dn));
    }

    dreq.edges.reserve(req.edges.size());
    for (const auto& e : req.edges) {
        yams::daemon::KgIngestEdge de;
        de.srcNodeKey = e.srcNodeKey;
        de.dstNodeKey = e.dstNodeKey;
        de.relation = e.relation;
        de.weight = e.weight;
        if (!e.properties.is_null())
            de.properties = e.properties.dump();
        dreq.edges.push_back(std::move(de));
    }

    dreq.aliases.reserve(req.aliases.size());
    for (const auto& a : req.aliases) {
        yams::daemon::KgIngestAlias da;
        da.nodeKey = a.nodeKey;
        da.alias = a.alias;
        da.source = a.source;
        da.confidence = a.confidence;
        dreq.aliases.push_back(std::move(da));
    }

    auto res = co_await clientRes.value()->call<yams::daemon::KgIngestRequest>(dreq);
    if (!res) {
        co_return res.error();
    }

    const auto& resp = res.value();

    MCPGraphResponse out;
    out.action = "ingest";
    out.nodesInserted = resp.nodesInserted;
    out.nodesSkipped = resp.nodesSkipped;
    out.edgesInserted = resp.edgesInserted;
    out.edgesSkipped = resp.edgesSkipped;
    out.aliasesInserted = resp.aliasesInserted;
    out.aliasesSkipped = resp.aliasesSkipped;
    out.errors = resp.errors;
    out.success = resp.success;

    co_return out;
}

#endif

        } // namespace yams::mcp
