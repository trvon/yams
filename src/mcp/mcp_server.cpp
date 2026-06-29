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

MessageResult MCPServer::handleRequest(const json& request) {
    auto id = request.value("id", json{});
    registerCancelable(id);
    try {
        // Extract method and params
        std::string method = request.value("method", "");
        json params = request.value("params", json::object());
        const auto& id2 = id; // Reuse already extracted id

        spdlog::debug("MCP server handling method: '{}' with id: {}", method, id.dump());

        if (auto routed = dispatchCoreMethod(id2, method, params); routed.has_value()) {
            return std::move(*routed);
        }

        // Unknown method - return proper JSON-RPC error
        return json{{"jsonrpc", protocol::JSONRPC_VERSION},
                    {"error", {{"code", -32601}, {"message", "Method not found: " + method}}},
                    {"id", id}};
    } catch (const json::exception& e) {
        return Error{ErrorCode::InvalidArgument, std::string("JSON error: ") + e.what()};
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Internal error: ") + e.what()};
    }
}

boost::asio::awaitable<MessageResult> MCPServer::handleRequestAsync(const json& request) {
    auto id = request.value("id", json{});
    registerCancelable(id);
    try {
        std::string method = request.value("method", "");
        json params = request.value("params", json::object());

        spdlog::debug("MCP server handling async method: '{}' with id: {}", method, id.dump());

        if (method == "tools/call") {
            recordEarlyFeatureUse();
            const auto toolName = params.value("name", "");
            const auto toolArgs = params.value("arguments", json::object());
            spdlog::debug("MCP async tool call: '{}' with args: {}", toolName, toolArgs.dump());
            sendProgress("tool", 0.0, std::string("calling ") + toolName);

            json raw = co_await callToolAsync(toolName, toolArgs);
            if (raw.is_object() && raw.contains("error")) {
                const auto& err = raw["error"];
                sendProgress("tool", 100.0, std::string("completed ") + toolName);
                co_return json{{"jsonrpc", protocol::JSONRPC_VERSION}, {"error", err}, {"id", id}};
            }

            sendProgress("tool", 100.0, std::string("completed ") + toolName);
            co_return createResponse(id, raw);
        }

        if (auto routed = dispatchCoreMethod(id, method, params); routed.has_value()) {
            co_return std::move(*routed);
        }

        co_return json{{"jsonrpc", protocol::JSONRPC_VERSION},
                       {"error", {{"code", -32601}, {"message", "Method not found: " + method}}},
                       {"id", id}};
    } catch (const json::exception& e) {
        co_return Error{ErrorCode::InvalidArgument, std::string("JSON error: ") + e.what()};
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::InternalError, std::string("Internal error: ") + e.what()};
    }
}

json MCPServer::listTools() {
    if (!toolRegistry_) {
        return json{{"tools", json::array()}};
    }

    json out = toolRegistry_->listTools();

    // Protocol-aware shaping for interoperability with strict clients.
    // - annotations were introduced in 2024-11-05
    // - title in tool metadata became available in 2025-03-26
    const bool supportsAnnotations = negotiatedProtocolVersion_ >= "2024-11-05";
    const bool supportsTitle = negotiatedProtocolVersion_ >= "2025-03-26";

    if (out.is_object() && out.contains("tools") && out["tools"].is_array()) {
        json shapedTools = json::array();
        shapedTools.get_ref<json::array_t&>().reserve(out["tools"].size());

        for (auto& tool : out["tools"]) {
            if (!tool.is_object()) {
                continue;
            }
            if (!supportsAnnotations) {
                tool.erase("annotations");
            }
            if (!supportsTitle) {
                tool.erase("title");
            }

            shapedTools.push_back(std::move(tool));
        }

        out["tools"] = std::move(shapedTools);
    }

    // MCP Apps: when UI is negotiated, link select tools to UI resources.
    // Tests currently expect nested `_meta.ui.resourceUri`.
    if (mcpAppsSupported_.load() && out.is_object() && out.contains("tools") &&
        out["tools"].is_array()) {
        // Ensure UI resources exist before we reference their URIs.
        ensureUiResourcesInitialized();

        for (auto& tool : out["tools"]) {
            if (!tool.is_object() || !tool.contains("name") || !tool["name"].is_string()) {
                continue;
            }
            const std::string name = tool["name"].get<std::string>();

            // Minimal Phase 1 linkage: provide at least one tool with an app UI.
            // Keep this conservative: the tool remains callable from the model.
            // In code mode, "query" is the composite tool covering search/grep/graph.
            if (name == "search" || name == "grep" || name == "graph" || name == "query") {
                // Compatibility: tests expect `_meta.ui.resourceUri`.
                tool["_meta"]["ui"]["resourceUri"] = "ui://yams/dashboard";
                tool["_meta"]["ui"]["visibility"] = json::array({"model", "app"});

                // Spec-style keys are often expressed as a single string key containing '/'.
                // Emit these too so hosts that key off the canonical extension path still work.
                tool["_meta"]["ui/resourceUri"] = "ui://yams/dashboard";
                tool["_meta"]["ui/visibility"] = json::array({"model", "app"});
            }
        }
    }

    return out;
}

// Tool calls live in mcp_tool_calls.cpp


        // Code Mode composite tool helpers/handlers live in src/mcp/mcp_code_mode.cpp

        json MCPServer::createResponse(const json& id, const json& result) {
            return json{{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
        }

        json MCPServer::createError(const json& id, int code, const std::string& message) {
            spdlog::warn("MCP server creating error response: code={}, message='{}'", code,
                         message);
            return json{
                {"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}};
        }

        void MCPServer::recordEarlyFeatureUse() {
            if (!initializedNotificationSeen_.load(std::memory_order_acquire)) {
                earlyFeatureUse_.store(true, std::memory_order_relaxed);
            }
        }

        // Helper: create a structured MCP logging notification (optional, in-band logging)
        // Removed duplicate unused createLogNotification overload (previously: level + data +
        // logger)

        /* Removed misplaced logging/setLevel block (should reside inside MCPServer::handleRequest)
         */

        boost::asio::awaitable<Result<MCPGetByNameResponse>> MCPServer::handleGetByName(
            const MCPGetByNameRequest& req) {
            // Path-first resolution: if explicit path provided or name includes a subpath,
            // use document service to resolve exact path or suffix, then retrieve by hash.
            if (!req.path.empty() || (req.name.find('/') != std::string::npos ||
                                      req.name.find('\\') != std::string::npos)) {
                auto docService = documentService_;
                if (!docService) {
                    docService = app::services::makeDocumentService(appContext_);
                    if (docService) {
                        documentService_ = docService;
                    }
                }
                if (!docService) {
                    co_return Error{ErrorCode::NotInitialized, "Document service not available"};
                }

                const std::string wanted = !req.path.empty() ? req.path : req.name;
                std::vector<app::services::DocumentEntry> matches;

                auto try_list = [&](const std::string& pat) {
                    app::services::ListDocumentsRequest lreq;
                    lreq.pattern = pat;
                    lreq.limit = 10000;
                    lreq.pathsOnly = false;
                    auto lr = docService->list(lreq);
                    if (lr && !lr.value().documents.empty()) {
                        for (const auto& d : lr.value().documents)
                            matches.push_back(d);
                    }
                };

                // Exact path first
                try_list(wanted);
                // Suffix match if allowed and no exact match
                if (matches.empty() && req.subpath) {
                    try_list(std::string("%/") + wanted);
                }
                // Contains anywhere as a last resort before returning NotFound
                if (matches.empty()) {
                    try_list(std::string("%") + wanted + "%");
                }

                if (matches.empty()) {
                    // If a path-like query yields no results, fail explicitly instead of falling
                    // through. This makes the behavior more predictable for callers.
                    co_return Error{ErrorCode::NotFound, "document not found by path: " + wanted};
                } else {
                    // Disambiguate
                    if (req.latest || req.oldest) {
                        std::sort(matches.begin(), matches.end(), [](const auto& a, const auto& b) {
                            return a.indexed < b.indexed;
                        });
                        const auto pick = req.latest ? matches.back() : matches.front();
                        // Retrieve content by hash via shared daemon client
                        if (auto ensure = ensureDaemonClient(); !ensure) {
                            co_return ensure.error();
                        }
                        yams::app::services::RetrievalOptions ropts;
                        ropts.socketPath = daemon_client_config_.socketPath;
                        yams::app::services::GetOptions greq;
                        greq.hash = pick.hash;
                        greq.metadataOnly = false;
                        auto grres = retrieval_svc_->get(greq, ropts);
                        if (!grres)
                            co_return grres.error();
                        auto gr = std::move(grres).value();
                        MCPGetByNameResponse out;
                        out.size = gr.size;
                        out.hash = std::move(gr.hash);
                        out.name = std::move(gr.name);
                        out.path = std::move(gr.path);
                        out.mimeType = std::move(gr.mimeType);
                        if (!gr.content.empty()) {
                            constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
                            out.content = gr.content.size() <= MAX_BYTES
                                              ? std::move(gr.content)
                                              : gr.content.substr(0, MAX_BYTES);
                        }
                        co_return out;
                    }

                    // If not choosing latest/oldest, prefer exact match by path equality
                    auto exactIt = std::find_if(matches.begin(), matches.end(),
                                                [&](const auto& d) { return d.path == wanted; });
                    const auto chosen = (exactIt != matches.end()) ? *exactIt : matches.front();

                    // Retrieve content by hash via shared daemon client
                    if (auto ensure = ensureDaemonClient(); !ensure) {
                        co_return ensure.error();
                    }
                    yams::app::services::RetrievalOptions ropts;
                    ropts.socketPath = daemon_client_config_.socketPath;
                    yams::app::services::GetOptions greq;
                    greq.hash = chosen.hash;
                    greq.metadataOnly = false;
                    auto grres = retrieval_svc_->get(greq, ropts);
                    if (!grres)
                        co_return grres.error();
                    auto gr = std::move(grres).value();
                    MCPGetByNameResponse out;
                    out.size = gr.size;
                    out.hash = std::move(gr.hash);
                    out.name = std::move(gr.name);
                    out.path = std::move(gr.path);
                    out.mimeType = std::move(gr.mimeType);
                    if (!gr.content.empty()) {
                        constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
                        out.content = gr.content.size() <= MAX_BYTES
                                          ? std::move(gr.content)
                                          : gr.content.substr(0, MAX_BYTES);
                    }
                    co_return out;
                }
            }

            // Try smart retrieval first, then fallback to base-name list + fuzzy selection
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

            auto r = rsvc.getByNameSmart(req.name, req.oldest, true, /*useSession*/ false,
                                         std::string{}, ropts, resolver);

            yams::daemon::GetResponse gr;
            if (r) {
                gr = std::move(r).value();
            } else {
                // Fallback path: search by base filename using list + simple fuzzy
                auto tryList =
                    [&](const std::string& pat) -> std::optional<yams::daemon::ListResponse> {
                    yams::app::services::ListOptions lreq;
                    lreq.namePattern = pat; // SQL LIKE pattern
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
                        return chosen ? std::optional<yams::daemon::ListEntry>(*chosen)
                                      : std::nullopt;
                    }
                    auto scoreName = [&](const std::string& base) -> int {
                        if (base == req.name)
                            return 1000;
                        if (base.size() >= req.name.size() && base.rfind(req.name, 0) == 0)
                            return 800;
                        if (base.find(req.name) != std::string::npos)
                            return 600;
                        int dl = static_cast<int>(std::abs((long)(base.size() - req.name.size())));
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
                // Exact base-name
                lr = tryList(std::string("%/") + req.name);
                // Stem match
                if (!lr) {
                    std::string stem = req.name;
                    try {
                        stem = std::filesystem::path(req.name).stem().string();
                    } catch (...) {
                    }
                    lr = tryList(std::string("%/") + stem + "%");
                }
                // Anywhere contains
                if (!lr)
                    lr = tryList(std::string("%") + req.name + "%");

                if (!lr || lr->items.empty())
                    co_return Error{ErrorCode::NotFound, "document not found by name"};
                auto cand = bestMatch(lr->items);
                if (!cand)
                    co_return Error{ErrorCode::NotFound, "document not found by name"};
                yams::app::services::GetOptions greq;
                greq.hash = std::move(cand->hash);
                greq.metadataOnly = false;
                auto grres = rsvc.get(greq, ropts);
                if (!grres)
                    co_return grres.error();
                gr = std::move(grres).value();
            }

            MCPGetByNameResponse out;
            out.size = gr.size;
            out.hash = std::move(gr.hash);
            out.name = std::move(gr.name);
            out.path = std::move(gr.path);
            out.mimeType = std::move(gr.mimeType);
            if (!gr.content.empty()) {
                constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
                out.content = gr.content.size() <= MAX_BYTES ? std::move(gr.content)
                                                             : gr.content.substr(0, MAX_BYTES);
            }
            co_return out;
        }

        boost::asio::awaitable<Result<MCPDeleteByNameResponse>> MCPServer::handleDeleteByName(
            const MCPDeleteByNameRequest& req) {
            daemon::DeleteRequest daemon_req;
            daemon_req.name = req.name;
            daemon_req.names = req.names;
            daemon_req.pattern = req.pattern;
            daemon_req.dryRun = req.dryRun;
            auto clientRes = requireDaemonClient();
            if (!clientRes)
                co_return clientRes.error();
            auto dres = co_await clientRes.value()->remove(daemon_req);
            if (!dres)
                co_return dres.error();
            MCPDeleteByNameResponse out;
            out.count = dres.value().successCount;
            out.dryRun = req.dryRun;
            for (const auto& result : dres.value().results) {
                if (result.success) {
                    out.deleted.push_back(result.name.empty() ? result.hash : result.name);
                }
            }
            co_return out;
        }

        boost::asio::awaitable<yams::Result<yams::mcp::MCPCatDocumentResponse>>
        yams::mcp::MCPServer::handleCatDocument(const yams::mcp::MCPCatDocumentRequest& req) {
            // Reuse shared daemon client to avoid per-request connection overhead
            auto clientRes = requireDaemonClient();
            if (!clientRes)
                co_return clientRes.error();
            auto ropts = makeMcpRetrievalOptions(daemon_client_config_);

            yams::app::services::GetOptions dreq;
            dreq.hash = req.hash;
            dreq.name = req.name;
            dreq.byName = !req.name.empty();
            dreq.raw = req.rawContent;
            dreq.extract = req.extractText;
            dreq.latest = req.latest;
            dreq.oldest = req.oldest;
            dreq.metadataOnly = false; // cat always needs content

            auto gres = retrieval_svc_->get(dreq, ropts);
            if (!gres) {
                co_return gres.error();
            }

            const auto& r = gres.value();
            MCPCatDocumentResponse out;
            out.size = r.size;
            out.hash = r.hash;
            out.name = r.name;
            if (r.hasContent) {
                out.content = r.content;
            }
            co_return out;
        }

        // Implementation of collection restore
        boost::asio::awaitable<yams::Result<yams::mcp::MCPRestoreCollectionResponse>>
        yams::mcp::MCPServer::handleRestoreCollection(
            const yams::mcp::MCPRestoreCollectionRequest& req) {
            try {
                if (!metadataRepo_) {
                    co_return Error{ErrorCode::NotInitialized,
                                    "Metadata repository not initialized"};
                }

                if (!store_) {
                    co_return Error{ErrorCode::NotInitialized, "Content store not initialized"};
                }

                if (req.collection.empty()) {
                    co_return Error{ErrorCode::InvalidArgument, "Collection name is required"};
                }

                spdlog::debug("MCP handleRestoreCollection: restoring collection '{}'",
                              req.collection);

                // Get documents from collection using generic metadata query
                metadata::DocumentQueryOptions queryOpts;
                queryOpts.metadataFilters.emplace_back("collection", req.collection);
                queryOpts.orderByIndexedDesc = true;
                auto docsResult = metadataRepo_->queryDocuments(queryOpts);
                if (!docsResult) {
                    co_return Error{ErrorCode::InternalError,
                                    "Failed to find collection documents: " +
                                        docsResult.error().message};
                }

                const auto& documents = docsResult.value();
                if (documents.empty()) {
                    MCPRestoreCollectionResponse response;
                    response.filesRestored = 0;
                    response.dryRun = req.dryRun;
                    spdlog::info(
                        "MCP handleRestoreCollection: no documents found in collection '{}'",
                        req.collection);
                    co_return response;
                }

                MCPRestoreCollectionResponse response;
                response.dryRun = req.dryRun;

                // Create output directory if needed
                std::filesystem::path outputDir(req.outputDirectory);
                if (!req.dryRun && req.createDirs) {
                    std::error_code ec;
                    yams::common::ensureDirectories(outputDir);
                    if (ec) {
                        co_return Error{ErrorCode::IOError,
                                        "Failed to create output directory: " + ec.message()};
                    }
                }

                // Process each document
                for (const auto& doc : documents) {
                    // Apply include/exclude filters
                    bool shouldInclude = true;

                    // Check include patterns
                    if (!req.includePatterns.empty()) {
                        shouldInclude = false;
                        for (const auto& pattern : req.includePatterns) {
                            // Simple wildcard matching (convert * to .*)
                            std::string regexPattern = pattern;
                            size_t pos = 0;
                            while ((pos = regexPattern.find("*", pos)) != std::string::npos) {
                                regexPattern.replace(pos, 1, ".*");
                                pos += 2;
                            }

                            std::regex rx(regexPattern);
                            if (std::regex_match(doc.fileName, rx)) {
                                shouldInclude = true;
                                break;
                            }
                        }
                    }

                    // Check exclude patterns
                    if (shouldInclude && !req.excludePatterns.empty()) {
                        for (const auto& pattern : req.excludePatterns) {
                            std::string regexPattern = pattern;
                            size_t pos = 0;
                            while ((pos = regexPattern.find("*", pos)) != std::string::npos) {
                                regexPattern.replace(pos, 1, ".*");
                                pos += 2;
                            }

                            std::regex rx(regexPattern);
                            if (std::regex_match(doc.fileName, rx)) {
                                shouldInclude = false;
                                break;
                            }
                        }
                    }

                    if (!shouldInclude) {
                        continue;
                    }

                    // Expand layout template
                    std::string outputPath = req.layoutTemplate;

                    // Replace {path} with original file path
                    size_t pos = outputPath.find("{path}");
                    if (pos != std::string::npos) {
                        outputPath.replace(pos, 6, doc.filePath);
                    }

                    // Replace {name} with file name
                    pos = outputPath.find("{name}");
                    if (pos != std::string::npos) {
                        outputPath.replace(pos, 6, doc.fileName);
                    }

                    // Replace {hash} with content hash
                    pos = outputPath.find("{hash}");
                    if (pos != std::string::npos) {
                        outputPath.replace(pos, 6, doc.sha256Hash);
                    }

                    // Replace {collection} with collection name
                    pos = outputPath.find("{collection}");
                    if (pos != std::string::npos) {
                        outputPath.replace(pos, 12, req.collection);
                    }

                    std::filesystem::path fullOutputPath = outputDir / outputPath;

                    // Check if file exists and handle overwrite
                    if (!req.dryRun && !req.overwrite && std::filesystem::exists(fullOutputPath)) {
                        spdlog::debug("MCP handleRestoreCollection: skipping existing file '{}'",
                                      fullOutputPath.string());
                        continue;
                    }

                    if (req.dryRun) {
                        response.restoredPaths.push_back(fullOutputPath.string());
                        response.filesRestored++;
                        spdlog::info(
                            "MCP handleRestoreCollection: [DRY-RUN] would restore '{}' to '{}'",
                            doc.fileName, fullOutputPath.string());
                    } else {
                        // Retrieve content
                        auto contentResult = store_->retrieveBytes(doc.sha256Hash);
                        if (!contentResult) {
                            spdlog::error("MCP handleRestoreCollection: failed to retrieve content "
                                          "for '{}': {}",
                                          doc.fileName, contentResult.error().message);
                            continue;
                        }

                        // Create parent directories
                        std::error_code ec;
                        yams::common::ensureDirectories(fullOutputPath.parent_path());
                        if (ec) {
                            spdlog::error("MCP handleRestoreCollection: failed to create directory "
                                          "for '{}': {}",
                                          fullOutputPath.string(), ec.message());
                            continue;
                        }

                        // Write file
                        std::ofstream outFile(fullOutputPath, std::ios::binary);
                        if (!outFile) {
                            spdlog::error(
                                "MCP handleRestoreCollection: failed to open output file '{}'",
                                fullOutputPath.string());
                            continue;
                        }

                        const auto& data = contentResult.value();
                        outFile.write(reinterpret_cast<const char*>(data.data()), data.size());
                        outFile.close();

                        response.restoredPaths.push_back(fullOutputPath.string());
                        response.filesRestored++;
                        spdlog::info("MCP handleRestoreCollection: restored '{}' to '{}'",
                                     doc.fileName, fullOutputPath.string());
                    }
                }

                spdlog::info(
                    "MCP handleRestoreCollection: restored {} files from collection '{}'{}",
                    response.filesRestored, req.collection, req.dryRun ? " [DRY-RUN]" : "");

                co_return response;
            } catch (const std::exception& e) {
                spdlog::error("MCP handleRestoreCollection exception: {}", e.what());
                co_return Error{ErrorCode::InternalError,
                                std::string("Restore collection failed: ") + e.what()};
            }
        }

        boost::asio::awaitable<yams::Result<yams::mcp::MCPRestoreSnapshotResponse>>
        yams::mcp::MCPServer::handleRestoreSnapshot(
            const yams::mcp::MCPRestoreSnapshotRequest& req) {
            auto clientRes = requireDaemonClient();
            if (!clientRes)
                co_return clientRes.error();
            auto* client = clientRes.value();

            std::string snapshotId = req.snapshotId;

            // Resolve label to ID if needed
            if (snapshotId.empty() && !req.snapshotLabel.empty()) {
                yams::daemon::ListSnapshotsRequest listReq;
                auto listResult = co_await client->listSnapshots(listReq);
                if (!listResult) {
                    co_return listResult.error();
                }
                for (const auto& snap : listResult.value().snapshots) {
                    if (snap.label == req.snapshotLabel) {
                        snapshotId = snap.id;
                        break;
                    }
                }
                if (snapshotId.empty()) {
                    co_return Error{ErrorCode::NotFound,
                                    "Snapshot with label '" + req.snapshotLabel + "' not found"};
                }
            }

            if (snapshotId.empty()) {
                co_return Error{ErrorCode::InvalidArgument,
                                "Either snapshotId or snapshotLabel must be provided"};
            }

            yams::daemon::RestoreSnapshotRequest daemonReq;
            daemonReq.snapshotId = std::move(snapshotId);
            daemonReq.outputDirectory = req.outputDirectory;
            daemonReq.layoutTemplate = req.layoutTemplate;
            daemonReq.includePatterns = req.includePatterns;
            daemonReq.excludePatterns = req.excludePatterns;
            daemonReq.overwrite = req.overwrite;
            daemonReq.createDirs = req.createDirs;
            daemonReq.dryRun = req.dryRun;

            auto result = co_await client->restoreSnapshot(daemonReq);
            if (!result) {
                co_return result.error();
            }

            MCPRestoreSnapshotResponse response;
            response.filesRestored = result.value().filesRestored;
            response.dryRun = result.value().dryRun;
            for (const auto& file : result.value().files) {
                response.restoredPaths.push_back(file.path);
            }
            co_return response;
        }

        boost::asio::awaitable<yams::Result<yams::mcp::MCPRestoreResponse>>
        yams::mcp::MCPServer::handleRestore(const yams::mcp::MCPRestoreRequest& req) {
            // Delegate to collection or snapshot restore based on which fields are populated
            if (!req.snapshotId.empty() || !req.snapshotLabel.empty()) {
                MCPRestoreSnapshotRequest snapReq;
                snapReq.snapshotId = req.snapshotId;
                snapReq.snapshotLabel = req.snapshotLabel;
                snapReq.outputDirectory = req.outputDirectory;
                snapReq.overwrite = req.overwrite;
                snapReq.createDirs = req.createDirs;
                snapReq.dryRun = req.dryRun;
                auto result = co_await handleRestoreSnapshot(snapReq);
                if (!result) {
                    co_return Error{result.error().code, result.error().message};
                }
                MCPRestoreResponse response;
                response.filesRestored = result.value().filesRestored;
                response.restoredPaths = result.value().restoredPaths;
                response.dryRun = result.value().dryRun;
                co_return response;
            } else if (!req.collection.empty()) {
                MCPRestoreCollectionRequest colReq;
                colReq.collection = req.collection;
                colReq.outputDirectory = req.outputDirectory;
                colReq.layoutTemplate = req.layoutTemplate;
                colReq.includePatterns = req.includePatterns;
                colReq.excludePatterns = req.excludePatterns;
                colReq.overwrite = req.overwrite;
                colReq.createDirs = req.createDirs;
                colReq.dryRun = req.dryRun;
                auto result = co_await handleRestoreCollection(colReq);
                if (!result) {
                    co_return Error{result.error().code, result.error().message};
                }
                MCPRestoreResponse response;
                response.filesRestored = result.value().filesRestored;
                response.restoredPaths = result.value().restoredPaths;
                response.dryRun = result.value().dryRun;
                co_return response;
            }
            co_return Error{ErrorCode::InvalidArgument,
                            "Either collection or snapshotId/snapshotLabel must be provided"};
        }

        boost::asio::awaitable<Result<MCPListCollectionsResponse>> MCPServer::handleListCollections(
            const MCPListCollectionsRequest& req) {
            (void)req; // Currently no parameters

            // Try local metadata repo first (embedded mode)
            if (metadataRepo_) {
                metadata::DocumentQueryOptions opts;
                auto result = metadataRepo_->getMetadataValueCounts({"collection"}, opts);
                if (!result) {
                    co_return result.error();
                }

                MCPListCollectionsResponse response;
                auto it = result.value().find("collection");
                if (it != result.value().end()) {
                    for (const auto& vc : it->second) {
                        response.collections.push_back(vc.value);
                    }
                }
                co_return response;
            }

            // Client mode: route through daemon client
            auto clientRes = requireDaemonClient();
            if (!clientRes)
                co_return clientRes.error();
            auto* client = clientRes.value();

            daemon::MetadataValueCountsRequest dreq;
            dreq.keys = {"collection"};

            auto result = co_await client->call<daemon::MetadataValueCountsRequest>(dreq);
            if (!result) {
                co_return result.error();
            }

            MCPListCollectionsResponse response;
            auto it = result.value().valueCounts.find("collection");
            if (it != result.value().valueCounts.end()) {
                for (const auto& [value, count] : it->second) {
                    response.collections.push_back(value);
                }
            }
            co_return response;
        }

        boost::asio::awaitable<Result<MCPListSnapshotsResponse>> MCPServer::handleListSnapshots(
            const MCPListSnapshotsRequest& req) {
            (void)req; // Daemon request has no filter fields yet

            auto clientRes = requireDaemonClient();
            if (!clientRes)
                co_return clientRes.error();
            auto* client = clientRes.value();

            daemon::ListSnapshotsRequest daemonReq;
            auto result = co_await client->listSnapshots(daemonReq);

            if (!result) {
                co_return result.error();
            }

            MCPListSnapshotsResponse response;
            for (const auto& snap : result.value().snapshots) {
                response.snapshots.push_back(makeSnapshotJson(snap));
            }
            co_return response;
        }

        boost::asio::awaitable<Result<MCPSuggestContextResponse>> MCPServer::handleSuggestContext(
            const MCPSuggestContextRequest& req) {
#if defined(YAMS_WASI)
            (void)req;
            co_return Error{ErrorCode::NotSupported,
                            "suggest_context is not supported on WASI build"};
#else
    auto query = normalizedTokenString(req.query);
    if (query.empty()) {
        co_return Error{ErrorCode::InvalidArgument, "query is required"};
    }

    MCPSuggestContextResponse response;
    response.query = req.query;

    std::shared_ptr<metadata::ConnectionPool> suggestContextPool;
    std::unique_ptr<metadata::MetadataRepository> suggestContextRepo;
    auto ensureSuggestContextRepo = [&](bool required) -> Result<metadata::MetadataRepository*> {
        if (suggestContextRepo) {
            return suggestContextRepo.get();
        }

        auto dataDir = daemon_client_config_.dataDir;
        if (dataDir.empty()) {
            dataDir = yams::config::get_data_dir();
        }
        metadata::ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        poolConfig.readOnly = false;
        suggestContextPool =
            std::make_shared<metadata::ConnectionPool>((dataDir / "yams.db").string(), poolConfig);
        auto init = suggestContextPool->initialize();
        if (!init) {
            if (!required) {
                suggestContextPool.reset();
                return static_cast<metadata::MetadataRepository*>(nullptr);
            }
            return Error{ErrorCode::DatabaseError,
                         "Failed to initialize metadata pool: " + init.error().message};
        }

        suggestContextRepo = std::make_unique<metadata::MetadataRepository>(*suggestContextPool);
        return suggestContextRepo.get();
    };

    const auto suppressionQuery = query;

    if (!testSuggestContextOverrides_.empty()) {
        SuggestContextSuppressionState suppression;
        if (!req.sessionName.empty()) {
            auto repoResult = ensureSuggestContextRepo(false);
            if (repoResult && repoResult.value() != nullptr) {
                suppression = loadSuggestContextSuppressionState(*repoResult.value(),
                                                                 req.sessionName, suppressionQuery);
            }
        }

        for (const auto& overrideEntry : testSuggestContextOverrides_) {
            if (suppression.snapshotIds.contains(overrideEntry.snapshotId)) {
                continue;
            }

            MCPSuggestContextResponse::Suggestion suggestion;
            suggestion.snapshotId = overrideEntry.snapshotId;
            suggestion.label = overrideEntry.label;
            suggestion.directoryPath = overrideEntry.directoryPath;
            for (const auto& supporting : overrideEntry.supportingResults) {
                if (suppression.resultIds.contains(supporting.id)) {
                    continue;
                }
                suggestion.score += supporting.score;
                suggestion.supportingResults.push_back(MCPSuggestContextResponse::SupportingResult{
                    supporting.id, supporting.hash, supporting.title, supporting.path,
                    supporting.score, supporting.snippet});
            }
            suggestion.supportingResultCount = suggestion.supportingResults.size();
            if (suggestion.supportingResultCount == 0) {
                continue;
            }
            response.suggestions.push_back(std::move(suggestion));
        }
        std::sort(response.suggestions.begin(), response.suggestions.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
        if (response.suggestions.size() > req.limit) {
            response.suggestions.resize(req.limit);
        }
        response.total = response.suggestions.size();

        co_return response;
    }

    MCPSearchRequest searchReq;
    searchReq.query = req.query;
    searchReq.limit = std::max<size_t>(req.limit * 8, size_t{20});
    searchReq.type = "hybrid";
    searchReq.useSession = req.useSession;
    searchReq.sessionName = req.sessionName;

    auto searchResult = co_await handleSearchDocuments(searchReq);
    if (!searchResult) {
        co_return searchResult.error();
    }

    struct SnapshotAccumulator {
        std::string snapshotId;
        std::string label;
        std::string directoryPath;
        double score{0.0};
        std::vector<MCPSuggestContextResponse::SupportingResult> supportingResults;
    };

    std::vector<std::string> hashes;
    hashes.reserve(searchResult.value().results.size());
    for (const auto& result : searchResult.value().results) {
        if (!result.hash.empty()) {
            hashes.push_back(result.hash);
        }
    }

    std::unordered_map<std::string, metadata::DocumentInfo> docsByHash;
    std::unordered_map<int64_t, std::unordered_map<std::string, metadata::MetadataValue>>
        metadataById;
    SuggestContextSuppressionState suppression;
    if (!hashes.empty() || !req.sessionName.empty()) {
        auto repoResult = ensureSuggestContextRepo(!hashes.empty());
        if (!repoResult) {
            co_return repoResult.error();
        }
        auto* repo = repoResult.value();
        if (repo != nullptr && !req.sessionName.empty()) {
            suppression =
                loadSuggestContextSuppressionState(*repo, req.sessionName, suppressionQuery);
        }
        if (repo == nullptr || hashes.empty()) {
            goto suggest_context_metadata_loaded;
        }

        auto docsResult = repo->batchGetDocumentsByHash(hashes);
        if (!docsResult) {
            co_return Error{ErrorCode::DatabaseError,
                            "Failed to load documents for suggest_context: " +
                                docsResult.error().message};
        }
        docsByHash = std::move(docsResult.value());

        std::vector<int64_t> docIds;
        docIds.reserve(docsByHash.size());
        for (const auto& [hash, doc] : docsByHash) {
            (void)hash;
            docIds.push_back(doc.id);
        }
        auto mdResult = repo->getMetadataForDocuments(docIds);
        if (!mdResult) {
            co_return Error{ErrorCode::DatabaseError,
                            "Failed to load metadata for suggest_context: " +
                                mdResult.error().message};
        }
        metadataById = std::move(mdResult.value());
    }

suggest_context_metadata_loaded:

    std::unordered_map<std::string, SnapshotAccumulator> accumulators;
    for (const auto& result : searchResult.value().results) {
        if (suppression.resultIds.contains(result.id)) {
            continue;
        }

        std::string snapshotId;
        std::string snapshotLabel;
        std::string directoryPath;

        if (!result.hash.empty()) {
            auto docIt = docsByHash.find(result.hash);
            if (docIt != docsByHash.end()) {
                auto mdIt = metadataById.find(docIt->second.id);
                if (mdIt != metadataById.end()) {
                    auto snapshotIt = mdIt->second.find("snapshot_id");
                    if (snapshotIt != mdIt->second.end()) {
                        snapshotId = snapshotIt->second.asString();
                    }
                    auto labelIt = mdIt->second.find("snapshot_label");
                    if (labelIt != mdIt->second.end()) {
                        snapshotLabel = labelIt->second.asString();
                    }
                    auto dirIt = mdIt->second.find("directory_path");
                    if (dirIt != mdIt->second.end()) {
                        directoryPath = dirIt->second.asString();
                    }
                }
                if (directoryPath.empty()) {
                    directoryPath = docIt->second.filePath;
                }
            }
        }

        if (snapshotId.empty()) {
            continue;
        }
        if (suppression.snapshotIds.contains(snapshotId)) {
            continue;
        }

        auto& acc = accumulators[snapshotId];
        if (acc.snapshotId.empty()) {
            acc.snapshotId = snapshotId;
        }
        if (acc.label.empty()) {
            acc.label = snapshotLabel;
        }
        if (acc.directoryPath.empty()) {
            acc.directoryPath = directoryPath;
        }

        double score = static_cast<double>(result.score);
        const auto labelNorm = normalizedTokenString(acc.label);
        const auto dirNorm = normalizedTokenString(acc.directoryPath);
        if (containsNormalizedToken(labelNorm, query)) {
            score += 0.35;
        }
        if (containsNormalizedToken(dirNorm, query)) {
            score += 0.2;
        }
        if (!result.title.empty() &&
            containsNormalizedToken(normalizedTokenString(result.title), query)) {
            score += 0.15;
        }
        if (!std::isfinite(score)) {
            score = 0.0;
        }

        acc.score += score;
        if (acc.supportingResults.size() < 3) {
            acc.supportingResults.push_back(MCPSuggestContextResponse::SupportingResult{
                result.id, result.hash, result.title, result.path, static_cast<float>(score),
                result.snippet});
        }
    }

    std::vector<MCPSuggestContextResponse::Suggestion> suggestions;
    suggestions.reserve(accumulators.size());
    for (auto& [snapshotId, acc] : accumulators) {
        (void)snapshotId;
        std::sort(acc.supportingResults.begin(), acc.supportingResults.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
        suggestions.push_back(MCPSuggestContextResponse::Suggestion{
            acc.snapshotId, acc.label, acc.directoryPath, acc.score, acc.supportingResults.size(),
            std::move(acc.supportingResults)});
    }

    std::sort(suggestions.begin(), suggestions.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
    if (suggestions.size() > req.limit) {
        suggestions.resize(req.limit);
    }

    response.total = suggestions.size();
    response.suggestions = std::move(suggestions);
    co_return response;
#endif
        }

        boost::asio::awaitable<Result<MCPSemanticDedupeResponse>> MCPServer::handleSemanticDedupe(
            const MCPSemanticDedupeRequest& req) {
#if defined(YAMS_WASI)
            (void)req;
            co_return Error{ErrorCode::NotSupported,
                            "semantic_dedupe is not supported on WASI build"};
#else
    auto dataDir = daemon_client_config_.dataDir;
    if (dataDir.empty()) {
        dataDir = yams::config::get_data_dir();
    }
    metadata::ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    poolConfig.readOnly = true;
    auto pool =
        std::make_shared<metadata::ConnectionPool>((dataDir / "yams.db").string(), poolConfig);
    auto init = pool->initialize();
    if (!init) {
        co_return Error{ErrorCode::DatabaseError,
                        "Failed to initialize metadata pool: " + init.error().message};
    }

    metadata::MetadataRepository repo(*pool);
    MCPSemanticDedupeResponse response;

    std::vector<metadata::SemanticDuplicateGroupDetail> details;
    if (!req.groupKey.empty()) {
        auto groupResult = repo.getSemanticDuplicateGroupByKey(req.groupKey);
        if (!groupResult) {
            co_return Error{ErrorCode::DatabaseError, "Failed to load semantic duplicate group: " +
                                                          groupResult.error().message};
        }
        if (groupResult.value().has_value()) {
            std::vector<int64_t> docIds;
            if (groupResult.value()->canonicalDocumentId.has_value()) {
                docIds.push_back(*groupResult.value()->canonicalDocumentId);
            }
            auto grouped = repo.getSemanticDuplicateGroupsForDocuments(docIds);
            if (!grouped) {
                co_return Error{ErrorCode::DatabaseError,
                                "Failed to load semantic duplicate members: " +
                                    grouped.error().message};
            }
            auto it = grouped.value().find(docIds.empty() ? 0 : docIds.front());
            if (it != grouped.value().end()) {
                details.push_back(it->second);
            } else {
                details.push_back(
                    metadata::SemanticDuplicateGroupDetail{std::move(*groupResult.value()), {}});
            }
        }
    } else if (!req.documentIds.empty()) {
        auto grouped = repo.getSemanticDuplicateGroupsForDocuments(req.documentIds);
        if (!grouped) {
            co_return Error{ErrorCode::DatabaseError,
                            "Failed to load semantic duplicate groups: " + grouped.error().message};
        }
        std::unordered_set<std::string> seen;
        for (auto& [docId, detail] : grouped.value()) {
            (void)docId;
            if (seen.insert(detail.group.groupKey).second) {
                details.push_back(std::move(detail));
            }
        }
    } else {
        auto groupsResult = repo.listSemanticDuplicateGroups(static_cast<int>(req.limit));
        if (!groupsResult) {
            co_return Error{ErrorCode::DatabaseError, "Failed to list semantic duplicate groups: " +
                                                          groupsResult.error().message};
        }
        std::vector<int64_t> canonicalIds;
        canonicalIds.reserve(groupsResult.value().size());
        for (const auto& group : groupsResult.value()) {
            if (group.canonicalDocumentId.has_value()) {
                canonicalIds.push_back(*group.canonicalDocumentId);
            }
        }
        auto grouped = repo.getSemanticDuplicateGroupsForDocuments(canonicalIds);
        if (!grouped) {
            co_return Error{ErrorCode::DatabaseError,
                            "Failed to load semantic duplicate group members: " +
                                grouped.error().message};
        }
        std::unordered_set<std::string> seen;
        for (const auto& group : groupsResult.value()) {
            if (group.canonicalDocumentId.has_value()) {
                auto it = grouped.value().find(*group.canonicalDocumentId);
                if (it != grouped.value().end() && seen.insert(it->second.group.groupKey).second) {
                    details.push_back(it->second);
                    continue;
                }
            }
            if (seen.insert(group.groupKey).second) {
                details.push_back(metadata::SemanticDuplicateGroupDetail{group, {}});
            }
        }
    }

    std::sort(details.begin(), details.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.group.groupKey < rhs.group.groupKey;
    });
    if (details.size() > req.limit) {
        details.resize(req.limit);
    }

    response.total = details.size();
    response.groups.reserve(details.size());
    for (const auto& detail : details) {
        MCPSemanticDedupeResponse::Group group;
        group.groupKey = detail.group.groupKey;
        group.algorithmVersion = detail.group.algorithmVersion;
        group.status = detail.group.status;
        group.reviewState = detail.group.reviewState;
        group.canonicalDocumentId = detail.group.canonicalDocumentId.value_or(0);
        group.memberCount = detail.group.memberCount;
        group.maxPairScore = detail.group.maxPairScore;
        group.threshold = detail.group.threshold.value_or(0.0);
        group.evidenceJson = detail.group.evidenceJson;
        for (const auto& member : detail.members) {
            group.members.push_back(MCPSemanticDedupeResponse::Member{
                member.documentId, member.role, member.decision, member.reason,
                member.similarityToCanonical.value_or(0.0), member.titleOverlap.value_or(0.0),
                member.pathOverlap.value_or(0.0), member.pairScore.value_or(0.0)});
        }
        response.groups.push_back(std::move(group));
    }

    co_return response;
#endif
        }

        // ---------------- Lifecycle helper implementations ----------------

        bool MCPServer::isMethodAllowedBeforeInitialization(const std::string& method) const {
            static const std::unordered_set<std::string> allowed = {
                "initialize", "exit",
                // readonly discovery before full init
                "tools/list", "resources/list", "resources/read", "prompts/list", "prompts/get",
                // logging notifications (client -> server)
                "notifications/log"};
            return allowed.count(method) > 0;
        }

        void MCPServer::markClientInitialized() {
            spdlog::info("MCP marking client as initialized");
            initializedNotificationSeen_.store(true);
            initialized_.store(true);
        }

        void MCPServer::handleExitRequest() {
            spdlog::info("MCP server received 'exit' request");
            exitRequested_.store(true);
            running_.store(false);
            if (transport_) {
                transport_->close();
            }
        }

        json MCPServer::buildServerCapabilities() const {
            // Per MCP 2025-06-18 spec: advertise listChanged:true since we support the notification
            // even though tools are primarily static (registered at startup)
            json caps = {{"tools", {{"listChanged", true}}},
                         {"prompts", {{"listChanged", false}}},
                         {"resources", {{"subscribe", false}, {"listChanged", false}}},
                         {"logging", json::object()},
                         {"experimental", json::object()}};

            // OpenCode validates `capabilities.experimental.*` against a schema.
            // In practice it expects these entries to be objects (not booleans).
            // We support cancel + progress (when tokens are supplied), so advertise as empty
            // objects.
            if (cancellationSupported_) {
                caps["experimental"]["cancellation"] = json::object();
            }
            if (progressSupported_) {
                caps["experimental"]["progress"] = json::object();
            }

            // Add MCP Apps extension capability if supported by client
            if (mcpAppsSupported_.load()) {
                caps["extensions"] = {{"io.modelcontextprotocol/ui",
                                       {{"mimeTypes", json::array({mcpAppsMimeType_})}}}};
            }

            return caps;
        }

        void MCPServer::scheduleAutoReady() {
            // Intentionally no-op: readiness requires explicit client lifecycle messages.
        }

        bool MCPServer::shouldAutoInitialize() const {
            return false;
        }

    } // namespace yams::mcp
