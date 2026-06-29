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

void MCPServer::initializeToolRegistry() {
    toolRegistry_ = std::make_unique<ToolRegistry>();

    // Define annotation presets for different tool categories (MCP 2024-11-05)
    ToolAnnotation readOnlyAnnotation{.readOnlyHint = true,
                                      .destructiveHint = false,
                                      .idempotentHint = true,
                                      .openWorldHint = false};
    ToolAnnotation addAnnotation{.readOnlyHint = false,
                                 .destructiveHint = false,
                                 .idempotentHint = false,
                                 .openWorldHint = false};
    ToolAnnotation deleteAnnotation{.readOnlyHint = false,
                                    .destructiveHint = true,
                                    .idempotentHint = false,
                                    .openWorldHint = false};
    ToolAnnotation updateAnnotation{.readOnlyHint = false,
                                    .destructiveHint = true,
                                    .idempotentHint = true,
                                    .openWorldHint = false};
    ToolAnnotation sessionAnnotation{.readOnlyHint = false,
                                     .destructiveHint = false,
                                     .idempotentHint = false,
                                     .openWorldHint = false};

    // Non-daemon tool used for protocol feature validation.
    // This stays fully in-process so unit tests can exercise tool result shaping
    // (content + structuredContent gating by negotiated protocol version).
    auto makeEchoHandler = [this]() {
        return [this](const json& args) mutable -> boost::asio::awaitable<json> {
            try {
                std::string text;
                if (args.is_object() && args.contains("text") && args["text"].is_string()) {
                    text = args["text"].get<std::string>();
                }

                json data = json{{"text", text}, {"length", text.size()}};

                std::optional<json> structured;
                if (negotiatedProtocolVersion_ >= "2024-11-05") {
                    structured = json{{"type", "tool_result"}, {"data", data}};
                }

                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("echo: ") + text)}),
                    std::move(structured),
                    /*is_error=*/false);
            } catch (const json::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("JSON error: ") + e.what())}),
                    std::nullopt, true);
            } catch (const std::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt,
                    true);
            }
        };
    };

    toolRegistry_->registerRawTool(
        "mcp.echo", makeEchoHandler(),
        json{{"type", "object"}, {"properties", {{"text", json{{"type", "string"}}}}}},
        "Echo input for MCP protocol testing", "Echo", readOnlyAnnotation);

#if defined(YAMS_WASI)
    // WASI builds currently don't support the daemon-backed tool surface.
    // Keep the server usable for MCP handshake + tool discovery.
    toolRegistry_->registerTool<MCPStatusRequest, MCPStatusResponse>(
        "status", [this](const MCPStatusRequest& req) { return handleGetStatus(req); },
        json{{"type", "object"},
             {"properties",
              {{"detailed",
                {{"type", "boolean"},
                 {"description", "Include verbose metrics"},
                 {"default", false}}}}}},
        "Get status for the in-process WASI MCP server", "Get Status", readOnlyAnnotation);
    return;
#endif

    // Always register standard MCP tools
    toolRegistry_->registerRawTool(
        "search",
        [this](const json& args) mutable -> boost::asio::awaitable<json> {
            try {
                auto req = MCPSearchRequest::fromJson(args);
                auto result = co_await handleSearchDocuments(req);

                if (!result) {
                    co_return yams::mcp::wrapToolResultStructured(
                        json::array(
                            {content::text(std::string("Error: ") + result.error().message)}),
                        std::nullopt, true);
                }

                const auto& out = result.value();

                std::ostringstream summary;
                summary << "Search";
                if (!req.query.empty()) {
                    summary << " for '" << req.query << "'";
                }
                if (!req.type.empty()) {
                    summary << " (type=" << req.type << ")";
                }
                summary << ": total=" << out.total;
                if (!out.paths.empty()) {
                    summary << ", paths=" << out.paths.size();
                } else {
                    summary << ", results=" << out.results.size();
                }
                if (out.executionTimeMs > 0) {
                    summary << ", time=" << out.executionTimeMs << "ms";
                }

                std::optional<json> structured;
                if (negotiatedProtocolVersion_ >= "2024-11-05") {
                    structured = json{{"type", "tool_result"}, {"data", out.toJson()}};
                }
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(summary.str())}), std::move(structured),
                    /*is_error=*/false);
            } catch (const json::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("JSON error: ") + e.what())}),
                    std::nullopt, true);
            } catch (const std::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt,
                    true);
            }
        },
        json{
            {"type", "object"},
            {"properties",
             {{"query", {{"type", "string"}, {"description", "Search query"}}},
              {"limit", {{"type", "integer"}, {"description", "Maximum results"}, {"default", 10}}},
              {"fuzzy",
               {{"type", "boolean"}, {"description", "Enable fuzzy search"}, {"default", false}}},
              {"similarity",
               {{"type", "number"}, {"description", "Similarity threshold"}, {"default", 0.7}}},
              {"type", {{"type", "string"}, {"description", "Search type"}, {"default", "hybrid"}}},
              {"paths_only",
               {{"type", "boolean"}, {"description", "Return only paths"}, {"default", false}}},
              {"path_pattern",
               {{"type", "string"},
                {"description", "Single path pattern (glob) to filter results"}}},
              {"include_patterns",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "Multiple path patterns (glob) to filter results (OR logic). "
                                "Preferred over path_pattern for multiple patterns."}}},
              {"tags",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "Filter by tags (YAMS extension)"}}},
              {"match_all_tags",
               {{"type", "boolean"},
                {"description", "Require all tags to match (AND logic)"},
                {"default", false}}},
              {"cwd",
               {{"type", "string"},
                {"description",
                 "Scope search to files under this directory path (absolute or relative)"}}}}},
            {"required", json::array({"query"})}},
        "Search documents using hybrid search (vector + full-text + knowledge graph)",
        "Search Documents", readOnlyAnnotation);

    toolRegistry_->registerRawTool(
        "grep",
        [this](const json& args) mutable -> boost::asio::awaitable<json> {
            try {
                auto req = MCPGrepRequest::fromJson(args);
                auto result = co_await handleGrepDocuments(req);

                if (!result) {
                    co_return yams::mcp::wrapToolResultStructured(
                        json::array(
                            {content::text(std::string("Error: ") + result.error().message)}),
                        std::nullopt, true);
                }

                const auto& out = result.value();
                std::ostringstream summary;
                summary << "Grep";
                if (!req.pattern.empty()) {
                    summary << " for '" << req.pattern << "'";
                }
                summary << ": matches=" << out.matchCount << ", files=" << out.fileCount;

                std::optional<json> structured;
                if (negotiatedProtocolVersion_ >= "2024-11-05") {
                    structured = json{{"type", "tool_result"}, {"data", out.toJson()}};
                }
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(summary.str())}), std::move(structured),
                    /*is_error=*/false);
            } catch (const json::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("JSON error: ") + e.what())}),
                    std::nullopt, true);
            } catch (const std::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt,
                    true);
            }
        },
        json{{"type", "object"},
             {"properties",
              {{"pattern", {{"type", "string"}, {"description", "Regex pattern to search"}}},
               {"name",
                {{"type", "string"},
                 {"description", "Optional file name or subpath to scope search"}}},
               {"paths",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Paths to search"}}},
               {"include_patterns",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "File include globs (e.g., '*.md')"}}},
               {"subpath",
                {{"type", "boolean"},
                 {"description", "Allow suffix match for path-like names"},
                 {"default", true}}},
               {"ignore_case",
                {{"type", "boolean"},
                 {"description", "Case insensitive search"},
                 {"default", false}}},
               {"line_numbers",
                {{"type", "boolean"}, {"description", "Show line numbers"}, {"default", false}}},
               {"context", {{"type", "integer"}, {"description", "Context lines"}, {"default", 0}}},
               {"fast_first",
                {{"type", "boolean"},
                 {"description", "Return a fast semantic-first burst"},
                 {"default", false}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Filter by tags"}}},
               {"match_all_tags",
                {{"type", "boolean"},
                 {"description", "Require all tags to match (AND logic)"},
                 {"default", false}}},
               {"cwd",
                {{"type", "string"},
                 {"description",
                  "Scope grep to files under this directory path (absolute or relative)"}}}}},
             {"required", json::array({"pattern"})}},
        "Search documents using regular expressions with grep-like functionality", "Grep Search",
        readOnlyAnnotation);

    toolRegistry_->registerTool<MCPDownloadRequest, MCPDownloadResponse>(
        "download", [this](const MCPDownloadRequest& req) { return handleDownload(req); },
        json{
            {"type", "object"},
            {"properties",
             {{"url", {{"type", "string"}, {"description", "URL to download"}}},
              {"headers",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "HTTP headers"}}},
              {"checksum", {{"type", "string"}, {"description", "Expected checksum (algo:hex)"}}},
              {"concurrency",
               {{"type", "integer"},
                {"description", "Number of concurrent connections"},
                {"default", 4}}},
              {"chunk_size_bytes",
               {{"type", "integer"}, {"description", "Chunk size in bytes"}, {"default", 8388608}}},
              {"timeout_ms",
               {{"type", "integer"},
                {"description", "Per-connection timeout in milliseconds"},
                {"default", 60000}}},
              {"resume",
               {{"type", "boolean"},
                {"description", "Attempt to resume interrupted downloads"},
                {"default", true}}},
              {"proxy", {{"type", "string"}, {"description", "Proxy URL (optional)"}}},
              {"follow_redirects",
               {{"type", "boolean"}, {"description", "Follow HTTP redirects"}, {"default", true}}},
              {"store_only",
               {{"type", "boolean"},
                {"description", "Store only in CAS without writing export path"},
                {"default", true}}},
              {"export_path",
               {{"type", "string"}, {"description", "Export path (when store_only is false)"}}},
              {"overwrite",
               {{"type", "string"},
                {"description", "Overwrite policy: never|if-different-etag|always"},
                {"default", "never"}}},
              {"post_index",
               {{"type", "boolean"},
                {"description", "Index the downloaded artifact after storing"},
                {"default", true}}},
              {"tags",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "Tags to apply when indexing"}}},
              {"metadata",
               {{"type", "object"}, {"description", "Metadata key/value pairs for indexing"}}},
              {"collection", {{"type", "string"}, {"description", "Collection name for indexing"}}},
              {"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID for indexing"}}},
              {"snapshot_label",
               {{"type", "string"}, {"description", "Snapshot label for indexing"}}}}},
            {"required", json::array({"url"})}},
        "Download files from URLs and store them in YAMS content-addressed storage; optionally "
        "post-index the artifact.",
        "Download Files", addAnnotation);

    toolRegistry_->registerTool<MCPDownloadJobsRequest, MCPDownloadJobsResponse>(
        "download_jobs",
        [this](const MCPDownloadJobsRequest& req) { return handleDownloadJobs(req); },
        json{{"type", "object"},
             {"properties",
              {{"action",
                {{"type", "string"},
                 {"enum", json::array({"status", "list", "cancel"})},
                 {"default", "status"}}},
               {"job_id", {{"type", "string"}, {"description", "Job id for status/cancel"}}}}}},
        "Manage daemon download jobs (status, list, or cancel)", "Download Jobs",
        readOnlyAnnotation);

    toolRegistry_->registerTool<MCPStoreDocumentRequest, MCPStoreDocumentResponse>(
        "add", [this](const MCPStoreDocumentRequest& req) { return handleStoreDocument(req); },
        json{{"type", "object"},
             {"properties",
              {{"path",
                {{"type", "string"},
                 {"description", "Single file or directory path for this add call"}}},
               {"content", {{"type", "string"}, {"description", "Inline document content"}}},
               {"name", {{"type", "string"}, {"description", "Document name (for stdin/content)"}}},
               {"mime_type", {{"type", "string"}, {"description", "MIME type override"}}},
               {"disable_auto_mime",
                {{"type", "boolean"}, {"description", "Disable automatic MIME detection"}}},
               {"no_embeddings",
                {{"type", "boolean"},
                 {"description", "Disable automatic embedding generation"},
                 {"default", false}}},
               {"collection", {{"type", "string"}, {"description", "Collection name"}}},
               {"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID"}}},
               {"snapshot_label", {{"type", "string"}, {"description", "Snapshot label"}}},
               {"recursive",
                {{"type", "boolean"},
                 {"description", "Recursively add files from directories"},
                 {"default", false}}},
               {"include",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Include patterns for recursive adds"}}},
               {"exclude",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Exclude patterns for recursive adds"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Document tags"}}},
               {"metadata", {{"type", "object"}, {"description", "Metadata key/value pairs"}}}}}},
        "Store one file or directory per call with deduplication; for multiple files use "
        "repeated add calls or a recursive directory path; mirrors CLI add",
        "Add Documents", addAnnotation);

    toolRegistry_->registerTool<MCPRetrieveDocumentRequest, MCPRetrieveDocumentResponse>(
        "get",
        [this](const MCPRetrieveDocumentRequest& req) { return handleRetrieveDocument(req); },
        json{{"type", "object"},
             {"properties",
              {{"hash", {{"type", "string"}, {"description", "Document hash"}}},
               {"name", {{"type", "string"}, {"description", "Document name (optional)"}}},
               {"output_path", {{"type", "string"}, {"description", "Output file path"}}},
               {"include_content",
                {{"type", "boolean"},
                 {"description", "Include content in response"},
                 {"default", true}}},
               {"use_session",
                {{"type", "boolean"},
                 {"description", "Use current session scope for name resolution"},
                 {"default", true}}},
               {"session", {{"type", "string"}, {"description", "Session name override"}}}}}},
        "Retrieve documents from storage by hash with optional knowledge graph expansion",
        "Get Documents", readOnlyAnnotation);

    toolRegistry_->registerRawTool(
        "list",
        [this](const json& args) mutable -> boost::asio::awaitable<json> {
            try {
                auto req = MCPListDocumentsRequest::fromJson(args);
                auto result = co_await handleListDocuments(req);

                if (!result) {
                    co_return yams::mcp::wrapToolResultStructured(
                        json::array(
                            {content::text(std::string("Error: ") + result.error().message)}),
                        std::nullopt, true);
                }

                const auto& out = result.value();
                std::ostringstream summary;
                summary << "List documents";
                if (!req.pattern.empty()) {
                    summary << " (pattern='" << req.pattern << "')";
                } else if (!req.name.empty()) {
                    summary << " (name='" << req.name << "')";
                }
                summary << ": total=" << out.total << ", returned=" << out.documents.size();
                if (req.limit > 0) {
                    summary << ", limit=" << req.limit;
                }
                if (req.offset > 0) {
                    summary << ", offset=" << req.offset;
                }

                std::optional<json> structured;
                if (negotiatedProtocolVersion_ >= "2024-11-05") {
                    structured = json{{"type", "tool_result"}, {"data", out.toJson()}};
                }
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(summary.str())}), std::move(structured),
                    /*is_error=*/false);
            } catch (const json::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("JSON error: ") + e.what())}),
                    std::nullopt, true);
            } catch (const std::exception& e) {
                co_return yams::mcp::wrapToolResultStructured(
                    json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt,
                    true);
            }
        },
        json{{"type", "object"},
             {"properties",
              {{"pattern", {{"type", "string"}, {"description", "Name pattern filter"}}},
               {"name", {{"type", "string"}, {"description", "Exact name filter (optional)"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Filter by tags"}}},
               {"match_all_tags",
                {{"type", "boolean"},
                 {"description", "Require all tags to match (AND logic)"},
                 {"default", false}}},
               {"recent", {{"type", "integer"}, {"description", "Show N most recent documents"}}},
               {"paths_only",
                {{"type", "boolean"},
                 {"description", "Output only file paths"},
                 {"default", false}}},
               {"include_diff",
                {{"type", "boolean"},
                 {"description", "Include structured diff when 'name' is a local file"},
                 {"default", false}}},
               {"limit",
                {{"type", "integer"},
                 {"description", "Maximum number of results"},
                 {"default", 100}}},
               {"offset",
                {{"type", "integer"}, {"description", "Offset for pagination"}, {"default", 0}}}}}},
        "List documents with filtering by pattern, tags, type, or recency", "List Documents",
        readOnlyAnnotation);

    toolRegistry_->registerTool<MCPStatusRequest, MCPStatusResponse>(
        "status", [this](const MCPStatusRequest& req) { return handleGetStatus(req); },
        json{{"type", "object"},
             {"properties",
              {{"detailed",
                {{"type", "boolean"},
                 {"description", "Include verbose metrics"},
                 {"default", false}}}}}},
        "Get daemon status, readiness, and metrics", "Get Status", readOnlyAnnotation);

    toolRegistry_->registerTool<MCPDoctorRequest, MCPDoctorResponse>(
        "doctor", [this](const MCPDoctorRequest& req) { return handleDoctor(req); },
        json{{"type", "object"},
             {"properties",
              {{"verbose",
                {{"type", "boolean"},
                 {"description", "Include detailed subsystem diagnostics"},
                 {"default", true}}}}}},
        "Diagnose daemon IPC connectivity, socket path, and readiness", "Doctor",
        readOnlyAnnotation);

    toolRegistry_->registerTool<MCPDeleteByNameRequest, MCPDeleteByNameResponse>(
        "delete_by_name",
        [this](const MCPDeleteByNameRequest& req) { return handleDeleteByName(req); },
        json{
            {"type", "object"},
            {"properties",
             {{"name", {{"type", "string"}, {"description", "Single document name to delete"}}},
              {"names",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "Multiple document names to delete"}}},
              {"pattern", {{"type", "string"}, {"description", "Glob pattern for matching names"}}},
              {"dry_run",
               {{"type", "boolean"},
                {"description", "Preview what would be deleted"},
                {"default", false}}}}}},
        "Delete documents by name, names array, or pattern", "Delete Documents", deleteAnnotation);

    toolRegistry_->registerTool<MCPUpdateMetadataRequest, MCPUpdateMetadataResponse>(
        "update", [this](const MCPUpdateMetadataRequest& req) { return handleUpdateMetadata(req); },
        json{{"type", "object"},
             {"properties",
              {{"hash", {{"type", "string"}, {"description", "Document hash"}}},
               {"name", {{"type", "string"}, {"description", "Document name"}}},
               {"path", {{"type", "string"}, {"description", "Explicit path to match"}}},
               {"names",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Multiple document names to update"}}},
               {"pattern", {{"type", "string"}, {"description", "Glob-like pattern"}}},
               {"latest",
                {{"type", "boolean"},
                 {"description", "Select newest when ambiguous"},
                 {"default", false}}},
               {"oldest",
                {{"type", "boolean"},
                 {"description", "Select oldest when ambiguous"},
                 {"default", false}}},
               {"metadata",
                {{"type", "object"}, {"description", "Metadata key-value pairs to update"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Tags to add"}}},
               {"remove_tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Tags to remove"}}},
               {"dry_run",
                {{"type", "boolean"},
                 {"description", "Preview changes only"},
                 {"default", false}}}}}},
        "Update metadata/tags by hash, name, path, names[], or pattern", "Update Metadata",
        updateAnnotation);

    // Session start/stop (simplified)
    // YAMS-specific session management tools
    if (areYamsExtensionsEnabled()) {
        // session_start
        toolRegistry_->registerTool<MCPSessionStartRequest, MCPSessionStartResponse>(
            "session_start",
            [this](const MCPSessionStartRequest& req) { return handleSessionStart(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Session name (optional)"}}},
                   {"description", {{"type", "string"}, {"description", "Session description"}}},
                   {"warm", {{"type", "boolean"}, {"default", true}}},
                   {"limit", {{"type", "integer"}, {"default", 200}}},
                   {"snippet_len", {{"type", "integer"}, {"default", 160}}},
                   {"cores", {{"type", "integer"}, {"default", -1}}},
                   {"memory_gb", {{"type", "integer"}, {"default", -1}}},
                   {"time_ms", {{"type", "integer"}, {"default", -1}}},
                   {"aggressive", {{"type", "boolean"}, {"default", false}}}}}},
            "Start (and optionally warm) a session with default budgets", "Start Session",
            sessionAnnotation);

        // session_stop
        toolRegistry_->registerTool<MCPSessionStopRequest, MCPSessionStopResponse>(
            "session_stop",
            [this](const MCPSessionStopRequest& req) { return handleSessionStop(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Session name (optional)"}}},
                   {"clear", {{"type", "boolean"}, {"default", true}}}}}},
            "Stop session (clear materialized cache)", "Stop Session", sessionAnnotation);

        // session_pin
        toolRegistry_->registerTool<MCPSessionPinRequest, MCPSessionPinResponse>(
            "session_pin",
            [this](const MCPSessionPinRequest& req) { return handleSessionPin(req); },
            json{
                {"type", "object"},
                {"properties",
                 {{"path", {{"type", "string"}, {"description", "Path glob pattern to pin"}}},
                  {"tags",
                   {{"type", "array"},
                    {"items", {{"type", "string"}}},
                    {"description", "Additional tags"}}},
                  {"metadata", {{"type", "object"}, {"description", "Metadata key/value pairs"}}}}},
                {"required", json::array({"path"})}},
            "Pin documents by path pattern (adds 'pinned' tag and updates repo)", "Pin Documents",
            sessionAnnotation);

        // session_unpin
        toolRegistry_->registerTool<MCPSessionUnpinRequest, MCPSessionUnpinResponse>(
            "session_unpin",
            [this](const MCPSessionUnpinRequest& req) { return handleSessionUnpin(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"path", {{"type", "string"}, {"description", "Path glob pattern to unpin"}}}}},
                 {"required", json::array({"path"})}},
            "Unpin documents by path pattern by removing 'pinned' tag", "Unpin Documents",
            sessionAnnotation);

        // session_watch
        toolRegistry_->registerTool<MCPSessionWatchRequest, MCPSessionWatchResponse>(
            "watch", [this](const MCPSessionWatchRequest& req) { return handleSessionWatch(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"session", {{"type", "string"}, {"description", "Session name override"}}},
                   {"root", {{"type", "string"}, {"description", "Project root to watch"}}},
                   {"interval_ms", {{"type", "integer"}, {"description", "Polling interval (ms)"}}},
                   {"enable",
                    {{"type", "boolean"},
                     {"description", "Enable watch (false disables)"},
                     {"default", true}}},
                   {"add_selector",
                    {{"type", "boolean"},
                     {"description", "Add root as a session selector"},
                     {"default", true}}},
                   {"set_current",
                    {{"type", "boolean"},
                     {"description", "Set session as current"},
                     {"default", true}}},
                   {"allow_create",
                    {{"type", "boolean"},
                     {"description", "Create session if missing"},
                     {"default", true}}}}}},
            "Enable or disable auto-ingest for a project session", "Watch Project",
            sessionAnnotation);
    }

    // Collection/Snapshot tools (consider these as standard or extension based on use case)
    toolRegistry_->registerTool<MCPRestoreRequest, MCPRestoreResponse>(
        "restore", [this](const MCPRestoreRequest& req) { return handleRestore(req); },
        json{{"type", "object"},
             {"properties",
              {{"collection", {{"type", "string"}, {"description", "Collection name"}}},
               {"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID"}}},
               {"snapshot_label",
                {{"type", "string"},
                 {"description", "Snapshot label (alternative to snapshot_id)"}}},
               {"output_directory", {{"type", "string"}, {"description", "Output directory"}}},
               {"layout_template",
                {{"type", "string"},
                 {"description", "Layout template (e.g., {collection}/{path})"},
                 {"default", "{path}"}}},
               {"include_patterns",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Only restore files matching these patterns"}}},
               {"exclude_patterns",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Exclude files matching these patterns"}}},
               {"overwrite",
                {{"type", "boolean"},
                 {"description", "Overwrite existing files"},
                 {"default", false}}},
               {"create_dirs",
                {{"type", "boolean"},
                 {"description", "Create parent directories if needed"},
                 {"default", true}}},
               {"dry_run",
                {{"type", "boolean"},
                 {"description", "Preview without writing"},
                 {"default", false}}}}},
             {"required", json::array({"output_directory"})}},
        "Restore documents from a collection or snapshot", "Restore Documents", updateAnnotation);

    if (areYamsExtensionsEnabled()) {
        toolRegistry_->registerTool<MCPListCollectionsRequest, MCPListCollectionsResponse>(
            "list_collections",
            [this](const MCPListCollectionsRequest& req) { return handleListCollections(req); },
            json{{"type", "object"}}, "List available collections", "List Collections",
            readOnlyAnnotation);

        toolRegistry_->registerTool<MCPListSnapshotsRequest, MCPListSnapshotsResponse>(
            "list_snapshots",
            [this](const MCPListSnapshotsRequest& req) { return handleListSnapshots(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"collection", {{"type", "string"}, {"description", "Filter by collection"}}},
                   {"with_labels",
                    {{"type", "boolean"},
                     {"description", "Include snapshot labels"},
                     {"default", true}}}}}},
            "List available snapshots", "List Snapshots", readOnlyAnnotation);

        toolRegistry_->registerTool<MCPSuggestContextRequest, MCPSuggestContextResponse>(
            "suggest_context",
            [this](const MCPSuggestContextRequest& req) { return handleSuggestContext(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"query", {{"type", "string"}, {"description", "Draft topic or query"}}},
                   {"limit",
                    {{"type", "integer"},
                     {"description", "Maximum snapshot suggestions"},
                     {"default", 5}}},
                   {"use_session",
                    {{"type", "boolean"},
                     {"description", "Scope to the current or provided session"},
                     {"default", false}}},
                   {"session",
                    {{"type", "string"},
                     {"description", "Specific session name when use_session is enabled"}}},
                   {"global_search",
                    {{"type", "boolean"},
                     {"description", "Bypass session isolation when session scoping is used"},
                     {"default", false}}}}},
                 {"required", json::array({"query"})}},
            "Suggest relevant snapshots for a draft topic", "Suggest Context", readOnlyAnnotation);

        toolRegistry_->registerTool<MCPSemanticDedupeRequest, MCPSemanticDedupeResponse>(
            "semantic_dedupe",
            [this](const MCPSemanticDedupeRequest& req) { return handleSemanticDedupe(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"group_key",
                    {{"type", "string"}, {"description", "Specific semantic group key"}}},
                   {"document_ids",
                    {{"type", "array"},
                     {"items", {{"type", "integer"}}},
                     {"description", "Filter groups by member document ids"}}},
                   {"limit",
                    {{"type", "integer"},
                     {"description", "Maximum groups to return"},
                     {"default", 25}}}}}},
            "Inspect persisted semantic duplicate groups", "Semantic Dedupe", readOnlyAnnotation);

        toolRegistry_->registerTool<MCPGraphRequest, MCPGraphResponse>(
            "graph", [this](const MCPGraphRequest& req) { return handleGraphQuery(req); },
            json{
                {"type", "object"},
                {"properties",
                 {{"action",
                   {{"type", "string"},
                    {"description",
                     "Operation: 'query' (default) to traverse/explore the graph, 'ingest' to "
                     "insert nodes/edges/aliases, 'lookup' to resolve a symbol definition, "
                     "'impact' for reverse dependents (blast radius), 'trace' for a path between "
                     "two symbols, 'affected_tests' to map changed files to affected tests"},
                    {"enum", json::array({"query", "ingest", "lookup", "impact", "trace",
                                          "affected_tests"})},
                    {"default", "query"}}},
                  // ── Navigation parameters (lookup/impact/trace/affected_tests) ──
                  {"symbol",
                   {{"type", "string"},
                    {"description", "Target symbol for action=lookup or action=impact"}}},
                  {"file",
                   {{"type", "string"},
                    {"description", "File path substring to disambiguate action=lookup"}}},
                  {"line",
                   {{"type", "integer"},
                    {"description", "Line number to disambiguate action=lookup"}}},
                  {"from", {{"type", "string"}, {"description", "Source symbol for action=trace"}}},
                  {"to", {{"type", "string"}, {"description", "Target symbol for action=trace"}}},
                  {"changed_files",
                   {{"type", "array"},
                    {"items", {{"type", "string"}}},
                    {"description", "Changed file paths for action=affected_tests"}}},
                  {"test_pattern",
                   {{"type", "string"},
                    {"description", "Path substring identifying test files for "
                                    "action=affected_tests"}}},
                  // ── Query parameters ──
                  {"hash", {{"type", "string"}, {"description", "Document hash to query from"}}},
                  {"name", {{"type", "string"}, {"description", "Document name to query from"}}},
                  {"node_key",
                   {{"type", "string"},
                    {"description", "Direct node key lookup (e.g., fn:abc:0x1000)"}}},
                  {"node_id", {{"type", "integer"}, {"description", "Direct node ID lookup"}}},
                  {"list_types",
                   {{"type", "boolean"},
                    {"description", "List available node types with counts"},
                    {"default", false}}},
                  {"list_type",
                   {{"type", "string"},
                    {"description", "List nodes of specific type (e.g., binary.function)"}}},
                  {"isolated",
                   {{"type", "boolean"},
                    {"description", "Find isolated nodes (no incoming edges)"},
                    {"default", false}}},
                  {"relation",
                   {{"type", "string"},
                    {"description", "Filter by relation type (e.g., calls, imports)"}}},
                  {"depth",
                   {{"type", "integer"},
                    {"description", "BFS traversal depth (1-5)"},
                    {"default", 1},
                    {"minimum", 1},
                    {"maximum", 5}}},
                  {"limit",
                   {{"type", "integer"}, {"description", "Maximum results"}, {"default", 100}}},
                  {"offset",
                   {{"type", "integer"}, {"description", "Pagination offset"}, {"default", 0}}},
                  {"reverse",
                   {{"type", "boolean"},
                    {"description", "Traverse incoming edges instead of outgoing"},
                    {"default", false}}},
                  {"include_properties",
                   {{"type", "boolean"},
                    {"description", "Include node and edge properties"},
                    {"default", false}}},
                  {"scope_snapshot",
                   {{"type", "string"}, {"description", "Scope results to specific snapshot"}}},
                  // ── Ingest parameters (used when action == "ingest") ──
                  {"nodes",
                   {{"type", "array"},
                    {"description", "Nodes to ingest (action=ingest)"},
                    {"items",
                     {{"type", "object"},
                      {"properties",
                       {{"node_key",
                         {{"type", "string"},
                          {"description",
                           "Unique logical key (e.g., 'strongs:H1234', 'lemma:λόγος')"}}},
                        {"label", {{"type", "string"}, {"description", "Human-readable name"}}},
                        {"type",
                         {{"type", "string"},
                          {"description",
                           "Node type (e.g., 'token', 'strongs', 'lemma', 'verse')"}}},
                        {"properties",
                         {{"type", "object"},
                          {"description", "Arbitrary properties as JSON object"}}}}},
                      {"required", json::array({"node_key", "type"})}}}}},
                  {"edges",
                   {{"type", "array"},
                    {"description", "Edges/relationships to ingest (action=ingest)"},
                    {"items",
                     {{"type", "object"},
                      {"properties",
                       {{"src_node_key", {{"type", "string"}, {"description", "Source node key"}}},
                        {"dst_node_key",
                         {{"type", "string"}, {"description", "Destination node key"}}},
                        {"relation",
                         {{"type", "string"},
                          {"description",
                           "Relation type (e.g., 'HAS_STRONGS', 'HAS_LEMMA', 'CONTAINS')"}}},
                        {"weight",
                         {{"type", "number"}, {"description", "Edge weight"}, {"default", 1.0}}},
                        {"properties",
                         {{"type", "object"}, {"description", "Arbitrary edge properties"}}}}},
                      {"required", json::array({"src_node_key", "dst_node_key", "relation"})}}}}},
                  {"aliases",
                   {{"type", "array"},
                    {"description", "Aliases/surface forms to ingest (action=ingest)"},
                    {"items",
                     {{"type", "object"},
                      {"properties",
                       {{"node_key",
                         {{"type", "string"}, {"description", "Node key to attach alias to"}}},
                        {"alias",
                         {{"type", "string"}, {"description", "Surface form / alias text"}}},
                        {"source",
                         {{"type", "string"},
                          {"description", "Origin system (e.g., 'symphony', 'manual')"}}},
                        {"confidence",
                         {{"type", "number"},
                          {"description", "Confidence [0,1]"},
                          {"default", 1.0}}}}},
                      {"required", json::array({"node_key", "alias"})}}}}},
                  {"document_hash",
                   {{"type", "string"},
                    {"description",
                     "Associate ingested entities with this document (action=ingest)"}}},
                  {"skip_existing_nodes",
                   {{"type", "boolean"},
                    {"description", "Skip nodes that already exist by node_key (action=ingest)"},
                    {"default", true}}},
                  {"skip_existing_edges",
                   {{"type", "boolean"},
                    {"description",
                     "Skip edges that already exist by src/dst/relation (action=ingest)"},
                    {"default", true}}}}}},
            "Query or mutate the knowledge graph. Use action='query' (default) to explore "
            "relationships, or action='ingest' to bulk-insert nodes, edges, and aliases "
            "(e.g., token-to-Strong's/lemma graphs).",
            "Knowledge Graph", addAnnotation);
    }

            // ── Code Mode: composite tool surface ──
            // Replace individual tools with 3 composite tools (query/execute/session).
            // Individual tools remain in internalRegistry_ for dispatch by the composite handlers.
            {
                spdlog::info("MCP: registering composite query/execute/session tools");

                // Save a reference to the fully-populated registry for internal dispatch
                auto fullRegistry = std::move(toolRegistry_);
                toolRegistry_ = std::make_unique<ToolRegistry>();

                // Re-register mcp.echo (always available)
                toolRegistry_->registerRawTool(
                    "mcp.echo",
                    [fullReg = fullRegistry.get()](
                        const json& args) mutable -> boost::asio::awaitable<json> {
                        co_return co_await fullReg->callTool("mcp.echo", args);
                    },
                    json{{"type", "object"}, {"properties", {{"text", json{{"type", "string"}}}}}},
                    "Echo input for MCP protocol testing", "Echo", readOnlyAnnotation);

                // Re-register doctor (always available in code mode)
                toolRegistry_->registerRawTool(
                    "doctor",
                    [fullReg = fullRegistry.get()](
                        const json& args) mutable -> boost::asio::awaitable<json> {
                        co_return co_await fullReg->callTool("doctor", args);
                    },
                    json{{"type", "object"},
                         {"properties",
                          {{"verbose",
                            {{"type", "boolean"},
                             {"description", "Include detailed subsystem diagnostics"},
                             {"default", true}}}}}},
                    "Diagnose daemon IPC connectivity, socket path, and readiness", "Doctor",
                    readOnlyAnnotation);

                // 1. query — read-only pipeline
                toolRegistry_->registerRawTool(
                    "query",
                    [this](const json& args) mutable -> boost::asio::awaitable<json> {
                        co_return co_await handlePipelineQuery(args);
                    },
                    json{{"type", "object"},
                         {"properties",
                          {{"steps",
                            {{"type", "array"},
                             {"description",
                              "Ordered pipeline steps. Each step's result is available as $prev in "
                              "subsequent steps."},
                             {"items",
                              {{"type", "object"},
                               {"properties",
                                {{"op",
                                  {{"type", "string"},
                                   {"enum", json::array({"search", "grep", "list",
                                                         "list_collections", "list_snapshots",
                                                         "graph", "get", "status", "describe"})}}},
                                 {"params",
                                  {{"type", "object"},
                                   {"description",
                                    "Operation-specific parameters. Use describe op to discover "
                                    "schemas."}}}}},
                               {"required", json::array({"op"})}}}}}}},
                         {"required", json::array({"steps"})}},
                    "Read-only operations: search, grep, list, list_collections, list_snapshots, "
                    "graph, "
                    "get, status. Supports multi-step pipelines where each step can reference "
                    "$prev "
                    "results. Use {\"op\":\"describe\"} to discover available operations and their "
                    "parameter schemas.",
                    "Query YAMS", readOnlyAnnotation);

                // 2. execute — write batch
                toolRegistry_->registerRawTool(
                    "execute",
                    [this](const json& args) mutable -> boost::asio::awaitable<json> {
                        co_return co_await handleBatchExecute(args);
                    },
                    json{
                        {"type", "object"},
                        {"properties",
                         {{"operations",
                           {{"type", "array"},
                            {"description", "Ordered write operations. Executed sequentially; "
                                            "stops on first error "
                                            "unless continueOnError is true."},
                            {"items",
                             {{"type", "object"},
                              {"properties",
                               {{"op",
                                 {{"type", "string"},
                                  {"enum", json::array({"add", "update", "delete", "restore",
                                                        "download"})}}},
                                {"params",
                                 {{"type", "object"},
                                  {"description",
                                   "Operation-specific parameters. Use query tool's describe op to "
                                   "discover schemas."}}}}},
                              {"required", json::array({"op", "params"})}}}}},
                          {"continueOnError",
                           {{"type", "boolean"},
                            {"description",
                             "If true, continue executing remaining operations after a failure."},
                            {"default", false}}}}},
                        {"required", json::array({"operations"})}},
                    "Write operations: add, update, delete, restore, download. Supports batching "
                    "multiple "
                    "operations in a single call.",
                    "Execute YAMS Operations",
                    ToolAnnotation{.readOnlyHint = false,
                                   .destructiveHint = true,
                                   .idempotentHint = false,
                                   .openWorldHint = false});

                // 3. session — session lifecycle
                if (areYamsExtensionsEnabled()) {
                    toolRegistry_->registerRawTool(
                        "session",
                        [this](const json& args) mutable -> boost::asio::awaitable<json> {
                            co_return co_await handleSessionAction(args);
                        },
                        json{
                            {"type", "object"},
                            {"properties",
                             {{"action",
                               {{"type", "string"},
                                {"enum", json::array({"start", "stop", "pin", "unpin", "watch"})}}},
                              {"params",
                               {{"type", "object"},
                                {"description", "Action-specific parameters. Use query tool's "
                                                "describe op to discover "
                                                "schemas."}}}}},
                            {"required", json::array({"action"})}},
                        "Session lifecycle: start, stop, pin, unpin, watch.", "Manage Sessions",
                        sessionAnnotation);
                }

                // Transfer ownership: keep fullRegistry alive as internalRegistry_ for dispatch
                internalRegistry_ = std::move(fullRegistry);
            }
        }
} // namespace yams::mcp
