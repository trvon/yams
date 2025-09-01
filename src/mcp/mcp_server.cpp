#include <yams/cli/asio_client_pool.hpp>
#include <yams/config/config_migration.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/downloader/downloader.hpp>
#include <yams/mcp/error_handling.h>
#include <yams/mcp/mcp_server.h>

#include <mutex>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <errno.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <regex>

// Platform-specific includes for non-blocking I/O
#ifdef _WIN32
#include <conio.h>
#include <windows.h>
#else
#include <poll.h>
#include <unistd.h>
#endif

namespace yams::mcp {

// Define static mutex for StdioTransport
std::mutex StdioTransport::io_mutex_;

// StdioTransport implementation
StdioTransport::StdioTransport() {
    // Ensure predictable stdio behavior. In tests, avoid changing global iostream
    // configuration so that rdbuf redirection in unit tests works as expected.
#ifndef YAMS_TESTING
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
#endif
    // Capture current stream buffers so we honor caller redirections (tests set rdbuf)
    outbuf_ = std::cout.rdbuf();
    inbuf_ = std::cin.rdbuf();
}

void StdioTransport::send(const json& message) {
    auto currentState = state_.load();
    if (currentState == TransportState::Connected) {
        // Lock mutex to ensure atomic write
        std::lock_guard<std::mutex> lock(io_mutex_);
        const std::string payload = message.dump();
        if (outbuf_) {
            std::ostream out(outbuf_);
            out << payload << "\n";
            out.flush();
        }
    }
}

bool StdioTransport::isInputAvailable(int timeoutMs) const {
#ifdef _WIN32
    // Windows implementation using WaitForSingleObject
    HANDLE stdinHandle = GetStdHandle(STD_INPUT_HANDLE);
    DWORD waitResult = WaitForSingleObject(stdinHandle, timeoutMs);
    return waitResult == WAIT_OBJECT_0;
#else
    // Unix/Linux/macOS implementation using poll
    struct pollfd fds;
    fds.fd = STDIN_FILENO;
    fds.events = POLLIN;
    fds.revents = 0;

    int result = poll(&fds, 1, timeoutMs);

    if (result == -1) {
        if (errno == EINTR) {
            // Signal interrupted, check shutdown
            if (externalShutdown_ && *externalShutdown_) {
                return false;
            }
        }
        return false;
    } else if (result == 0) {
        // Timeout - check shutdown
        if (externalShutdown_ && *externalShutdown_) {
            return false;
        }
    }

    return result > 0 && (fds.revents & POLLIN);
#endif
}

MessageResult StdioTransport::receive() {
    auto currentState = state_.load();
    if (currentState == TransportState::Closing || currentState == TransportState::Disconnected) {
        return Error{ErrorCode::NetworkError, "Transport is closed or disconnected"};
    }

    // MCP stdio transport: newline-delimited JSON messages
    // Use captured input buffer to respect redirections
    std::istream in(inbuf_);

    while (state_.load() != TransportState::Closing) {
        // Check for input availability
        std::streamsize avail = in.rdbuf() ? in.rdbuf()->in_avail() : 0;

        // Check for EOF first
        if (in.eof()) {
            state_.store(TransportState::Disconnected);
            return Error{ErrorCode::NetworkError, "End of file reached on stdin"};
        }

        if (isInputAvailable(100) || avail > 0) {
            std::string line;
            in.clear();

            // Lock mutex for thread-safe getline
            {
                std::lock_guard<std::mutex> lock(io_mutex_);
                if (!std::getline(in, line)) {
                    if (in.eof()) {
                        spdlog::debug("EOF on stdin, closing transport");
                        state_.store(TransportState::Disconnected);
                        return Error{ErrorCode::NetworkError, "End of file reached on stdin"};
                    }
                    // Clear error and retry if we should
                    in.clear();
                    if (!shouldRetryAfterError()) {
                        state_.store(TransportState::Error);
                        return Error{ErrorCode::NetworkError, "Too many consecutive I/O errors"};
                    }
                    continue;
                }
            }

            // Handle CRLF: strip trailing '\r' if present
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            // Skip empty lines but check for EOF after
            if (line.empty()) {
                // If we got an empty line and there's no more input, check for EOF
                if (in.eof() || (!in.rdbuf() || in.rdbuf()->in_avail() == 0)) {
                    // No more input after empty line
                    if (!isInputAvailable(10)) { // Short timeout for empty line check
                        state_.store(TransportState::Disconnected);
                        return Error{ErrorCode::NetworkError, "No more input after empty line"};
                    }
                }
                continue;
            }

            // Parse JSON message using safe parser
            auto parseResult = json_utils::parse_json(line);
            if (!parseResult) {
                recordError();
                spdlog::debug("JSON parse error: {}", parseResult.error().message);

                // In test environments or when we shouldn't retry, return error immediately
                // This prevents hanging when there's no more input after an error
                if (!shouldRetryAfterError() || in.eof()) {
                    state_.store(TransportState::Error);
                    return parseResult.error();
                }

                // Check if there's more input available before continuing
                if (!isInputAvailable(10)) { // Short timeout
                    // No more input available, return the error
                    return parseResult.error();
                }
                continue; // Try next line for recoverable errors
            }

            // Reset error count on successful parse
            resetErrorCount();
            return parseResult.value();
        }

        // Check if external shutdown was requested
        if (externalShutdown_ && *externalShutdown_) {
            spdlog::debug("External shutdown requested, closing transport");
            state_.store(TransportState::Closing);
            return Error{ErrorCode::NetworkError, "External shutdown requested"};
        }
    }

    return Error{ErrorCode::NetworkError, "Transport closed during receive"};
}

bool StdioTransport::shouldRetryAfterError() const noexcept {
    constexpr size_t MAX_CONSECUTIVE_ERRORS = 5;
    return errorCount_.load() < MAX_CONSECUTIVE_ERRORS;
}

void StdioTransport::recordError() noexcept {
    errorCount_.fetch_add(1);
}

void StdioTransport::resetErrorCount() noexcept {
    errorCount_.store(0);
}

// MCPServer implementation
MCPServer::MCPServer(std::unique_ptr<ITransport> transport, std::atomic<bool>* externalShutdown)
    : transport_(std::move(transport)), externalShutdown_(externalShutdown) {
    // Set external shutdown flag on StdioTransport if applicable
    if (auto* stdioTransport = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdioTransport->setShutdownFlag(externalShutdown_);
    }

    // Initialize the daemon client pool configuration (new DaemonClientPool)
    yams::cli::DaemonClientPool::Config pool_config;
    pool_config.max_clients = 10; // Cap clients to the daemon
    // MCP: prefer unary (non-streaming) responses to avoid partial stream handling issues
    pool_config.client_config.enableChunkedResponses = false;
    // Keep connections pooled for efficiency
    pool_config.client_config.singleUseConnections = false;
    // Be conservative with timeouts for MCP tools
    pool_config.client_config.requestTimeout = std::chrono::seconds(10);
    pool_config.client_config.headerTimeout = std::chrono::seconds(5);
    pool_config.client_config.bodyTimeout = std::chrono::seconds(60);
    pool_config.idle_timeout = std::chrono::minutes(1);

    // Construct pooled request managers; internal DaemonClientPool handles clients
    search_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::SearchRequest, yams::daemon::SearchResponse>>(
        pool_config);

    grep_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::GrepRequest, yams::daemon::GrepResponse>>(
        pool_config);

    download_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::DownloadRequest, yams::daemon::DownloadResponse>>(
        pool_config);

    store_req_manager_ =
        std::make_unique<cli::PooledRequestManager<yams::daemon::AddDocumentRequest,
                                                   yams::daemon::AddDocumentResponse>>(pool_config);

    retrieve_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::GetRequest, yams::daemon::GetResponse>>(
        pool_config);

    list_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::ListRequest, yams::daemon::ListResponse>>(
        pool_config);

    stats_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::GetStatsRequest, yams::daemon::GetStatsResponse>>(
        pool_config);

    delete_req_manager_ = std::make_unique<
        cli::PooledRequestManager<yams::daemon::DeleteRequest, yams::daemon::DeleteResponse>>(
        pool_config);

    update_req_manager_ = std::make_unique<cli::PooledRequestManager<
        yams::daemon::UpdateDocumentRequest, yams::daemon::UpdateDocumentResponse>>(pool_config);

    // Quick daemon health probe via Asio pool (prefer ping; fallback to status)
    {
        yams::cli::AsioClientPool asio_pool{};
        auto pong = asio_pool.ping();
        if (pong) {
            spdlog::info("Daemon reachable via Asio client pool (ping)");
        } else {
            auto st = asio_pool.status();
            if (st) {
                spdlog::info("Daemon reachable via Asio client pool (status)");
            } else {
                spdlog::debug("Asio probe failed: {}", pong ? "" : pong.error().message);
            }
        }
    }

    // Initialize the tool registry with modern handlers
    initializeToolRegistry();
}

MCPServer::~MCPServer() {
    stop();
}

void MCPServer::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }

    spdlog::info("MCP server started");

    // Main message loop with modern error handling
    while (running_ && (!externalShutdown_ || !*externalShutdown_)) {
        auto messageResult = transport_->receive();

        if (!messageResult) {
            const auto& error = messageResult.error();

            // Handle different error types
            switch (error.code) {
                case ErrorCode::NetworkError:
                    spdlog::info("Transport closed: {}", error.message);
                    running_ = false;
                    break;

                case ErrorCode::InvalidData:
                    spdlog::debug("Invalid JSON received: {}", error.message);
                    // Continue processing - client may send valid messages
                    continue;

                default:
                    spdlog::error("Unexpected transport error: {}", error.message);
                    continue;
            }
            continue;
        }

        // Process valid message
        auto response = handleRequest(messageResult.value());
        if (response) {
            const auto& resp = response.value();
            // Do not send a response for notifications (no id)
            if (!(resp.contains("id") && resp["id"].is_null())) {
                transport_->send(resp);
            } else {
                spdlog::debug("Notification processed without response");
            }
        } else {
            // Send error response for protocol violations
            const auto& error = response.error();
            json errorResponse = {
                {"jsonrpc", protocol::JSONRPC_VERSION},
                {"error", {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                {"id", nullptr}};
            transport_->send(errorResponse);
        }
    }

    running_ = false;
    spdlog::info("MCP server stopped");
}

void MCPServer::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }

    if (transport_) {
        transport_->close();
    }
}

MessageResult MCPServer::handleRequest(const json& request) {
    try {
        // Extract method and params
        std::string method = request.value("method", "");
        json params = request.value("params", json::object());
        auto id = request.value("id", json{});

        json response;

        // Route to appropriate handler
        if (method == "initialize") {
            response = initialize(params);
        } else if (method == "tools/list") {
            response = listTools();
        } else if (method == "tools/call") {
            std::string toolName = params.value("name", "");
            json toolArgs = params.value("arguments", json::object());
            response = callTool(toolName, toolArgs);
        } else if (method == "resources/list") {
            response = listResources();
        } else if (method == "resources/read") {
            std::string uri = params.value("uri", "");
            response = readResource(uri);
        } else if (method == "prompts/list") {
            response = listPrompts();
        } else {
            // Unknown method
            return createError(id, -32601, "Method not found: " + method);
        }

        // Create JSON-RPC response
        return createResponse(id, response);

    } catch (const json::exception& e) {
        return Error{ErrorCode::InvalidArgument, std::string("JSON error: ") + e.what()};
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Internal error: ") + e.what()};
    }
}

json MCPServer::initialize(const json& params) {
    // Store client info if provided
    if (params.contains("clientInfo")) {
        auto info = params["clientInfo"];
        clientInfo_.name = info.value("name", "unknown");
        clientInfo_.version = info.value("version", "unknown");
        spdlog::info("MCP client connected: {} {}", clientInfo_.name, clientInfo_.version);
    }

    // Compose capabilities and negotiated protocol version (minimal, conservative defaults)
    std::string negotiatedVersion = "2024-11-05";
    if (params.contains("protocolVersion") && params["protocolVersion"].is_string()) {
        negotiatedVersion = params["protocolVersion"].get<std::string>();
    }
    json capabilities = {{"tools", json({{"listChanged", false}})},
                         {"prompts", json({{"listChanged", false}})},
                         {"resources", json({{"subscribe", false}, {"listChanged", false}})},
                         {"logging", json::object()}};
    json result = {{"protocolVersion", negotiatedVersion},
                   {"capabilities", capabilities},
                   {"serverInfo", {{"name", "YAMS MCP Server"}, {"version", "0.0.2"}}},
                   {"instructions", "YAMS MCP server is ready. Use tools/list to discover tools."}};

    negotiatedProtocolVersion_ = negotiatedVersion;
    return result;
}

json MCPServer::listResources() {
    json resources = json::array();

    // Add a resource for the YAMS storage statistics
    resources.push_back({{"uri", "yams://stats"},
                         {"name", "Storage Statistics"},
                         {"description", "Current YAMS storage statistics and health status"},
                         {"mimeType", "application/json"}});

    // Add a resource for recent documents
    resources.push_back({{"uri", "yams://recent"},
                         {"name", "Recent Documents"},
                         {"description", "Recently added documents in YAMS storage"},
                         {"mimeType", "application/json"}});

    return {{"resources", resources}};
}

json MCPServer::readResource(const std::string& uri) {
    if (uri == "yams://stats") {
        // Get storage statistics
        auto stats = store_->getStats();
        auto health = store_->checkHealth();

        return {{"contents",
                 {{{"uri", uri},
                   {"mimeType", "application/json"},
                   {"text", json({{"storage",
                                   {{"totalObjects", stats.totalObjects},
                                    {"totalBytes", stats.totalBytes},
                                    {"uniqueBlocks", stats.uniqueBlocks},
                                    {"deduplicatedBytes", stats.deduplicatedBytes}}},
                                  {"health",
                                   {{"isHealthy", health.isHealthy},
                                    {"status", health.status},
                                    {"warnings", health.warnings},
                                    {"errors", health.errors}}}})
                                .dump()}}}}};
    } else if (uri == "yams://recent") {
        // Get recent documents
        auto docsResult = metadataRepo_->findDocumentsByPath("%");
        if (!docsResult) {
            return {{"contents", {{"text", "Failed to list documents"}}}};
        }
        auto docs = docsResult.value();
        // Limit to 20 most recent
        if (docs.size() > 20) {
            docs.resize(20);
        }

        json docList = json::array();
        for (const auto& doc : docs) {
            docList.push_back({{"hash", doc.sha256Hash},
                               {"name", doc.fileName},
                               {"size", doc.fileSize},
                               {"mimeType", doc.mimeType}});
        }

        return {{"contents",
                 {{{"uri", uri},
                   {"mimeType", "application/json"},
                   {"text", json({{"documents", docList}}).dump()}}}}};
    } else {
        throw std::runtime_error("Unknown resource URI: " + uri);
    }
}

json MCPServer::listTools() {
    json tools = json::array();

    // Helper lambdas
    auto makeProp = [](const std::string& type, const std::string& desc) {
        json j;
        j["type"] = type;
        if (!desc.empty())
            j["description"] = desc;
        return j;
    };

    // search
    {
        json tool;
        tool["name"] = "search";
        tool["description"] = "Search for documents using keywords, fuzzy matching, or similarity";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["query"] = makeProp("string", "Search query (keywords, phrases, or hash)");
        props["limit"] = makeProp("integer", "Maximum number of results");
        props["limit"]["default"] = 10;
        props["fuzzy"] = makeProp("boolean", "Enable fuzzy matching");
        props["fuzzy"]["default"] = false;
        props["similarity"] = makeProp("number", "Minimum similarity threshold (0-1)");
        props["similarity"]["default"] = 0.7;
        props["hash"] =
            makeProp("string", "Search by file hash (full or partial, minimum 8 characters)");
        props["verbose"] = makeProp("boolean", "Enable verbose output");
        props["verbose"]["default"] = false;
        props["type"] = makeProp("string", "Search type: keyword, semantic, hybrid");
        props["type"]["default"] = "hybrid";
        props["paths_only"] = makeProp("boolean", "Return only file paths (LLM-friendly)");
        props["paths_only"]["default"] = false;
        props["line_numbers"] = makeProp("boolean", "Include line numbers in content");
        props["line_numbers"]["default"] = false;
        props["after_context"] = makeProp("integer", "Lines of context after matches");
        props["after_context"]["default"] = 0;
        props["before_context"] = makeProp("integer", "Lines of context before matches");
        props["before_context"]["default"] = 0;
        props["context"] = makeProp("integer", "Lines of context around matches");
        props["context"]["default"] = 0;
        props["color"] =
            makeProp("string", "Color highlighting for matches (values: always, never, auto)");
        props["color"]["default"] = "auto";
        props["path_pattern"] =
            makeProp("string", "Glob-like filename/path filter (e.g., **/*.md or substring)");
        props["path"] =
            makeProp("string", "Alias for path_pattern (substring or glob-like filter)");
        props["tags"] =
            json{{"type", "array"},
                 {"items", json{{"type", "string"}}},
                 {"description", "Filter by tags (presence-based, matches any by default)"}};
        props["match_all_tags"] = makeProp("boolean", "Require all specified tags to be present");
        props["match_all_tags"]["default"] = false;
        schema["properties"] = props;
        schema["required"] = json::array({"query"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // grep
    {
        json tool;
        tool["name"] = "grep";
        tool["description"] = "Search document contents using regular expressions";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["pattern"] = makeProp("string", "Regular expression pattern");
        props["paths"] = json{{"type", "array"},
                              {"items", json{{"type", "string"}}},
                              {"description", "Specific paths to search (optional)"}};
        props["ignore_case"] = makeProp("boolean", "Case-insensitive search");
        props["ignore_case"]["default"] = false;
        props["word"] = makeProp("boolean", "Match whole words only");
        props["word"]["default"] = false;
        props["invert"] = makeProp("boolean", "Invert match (show non-matching lines)");
        props["invert"]["default"] = false;
        props["line_numbers"] = makeProp("boolean", "Show line numbers");
        props["line_numbers"]["default"] = false;
        props["with_filename"] = makeProp("boolean", "Show filename with matches");
        props["with_filename"]["default"] = true;
        props["count"] = makeProp("boolean", "Count matches instead of showing them");
        props["count"]["default"] = false;
        props["files_with_matches"] = makeProp("boolean", "Show only filenames with matches");
        props["files_with_matches"]["default"] = false;
        props["files_without_match"] = makeProp("boolean", "Show only filenames without matches");
        props["files_without_match"]["default"] = false;
        props["after_context"] = makeProp("integer", "Lines after match");
        props["after_context"]["default"] = 0;
        props["before_context"] = makeProp("integer", "Lines before match");
        props["before_context"]["default"] = 0;
        props["context"] = makeProp("integer", "Lines around match");
        props["context"]["default"] = 0;
        props["max_count"] = makeProp("integer", "Maximum matches per file");
        props["color"] = makeProp("string", "Color highlighting (values: always, never, auto)");
        props["color"]["default"] = "auto";
        schema["properties"] = props;
        schema["required"] = json::array({"pattern"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // download
    {
        json tool;
        tool["name"] = "download";
        tool["description"] =
            "Robust downloader: store into CAS (store-only by default) with optional export";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["url"] = makeProp("string", "Source URL");
        props["headers"] = json{{"type", "array"},
                                {"items", json{{"type", "string"}}},
                                {"description", "Custom headers"}};
        props["checksum"] = makeProp("string", "Expected checksum '<algo>:<hex>'");
        props["concurrency"] = makeProp("integer", "Parallel connections");
        props["concurrency"]["default"] = 4;
        props["chunk_size_bytes"] = makeProp("integer", "Chunk size in bytes");
        props["chunk_size_bytes"]["default"] = 8388608;
        props["timeout_ms"] = makeProp("integer", "Per-connection timeout (ms)");
        props["timeout_ms"]["default"] = 60000;
        props["resume"] = makeProp("boolean", "");
        props["resume"]["default"] = true;
        props["proxy"] = makeProp("string", "");
        props["follow_redirects"] = makeProp("boolean", "");
        props["follow_redirects"]["default"] = true;
        props["store_only"] = makeProp("boolean", "");
        props["store_only"]["default"] = true;
        props["export_path"] = makeProp("string", "Optional export path");
        props["overwrite"] = makeProp("string", "Overwrite policy: never|if-different-etag|always");
        props["overwrite"]["default"] = "never";
        schema["properties"] = props;
        schema["required"] = json::array({"url"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // add (store single)
    {
        json tool;
        tool["name"] = "add";
        tool["description"] = "Store a document in YAMS";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["path"] = makeProp("string", "File path to store");
        props["content"] = makeProp("string", "Document content");
        props["name"] = makeProp("string", "Document name/filename");
        props["mime_type"] = makeProp("string", "MIME type of the content");
        props["collection"] = makeProp("string", "Collection name for grouping");
        props["tags"] = json{{"type", "array"},
                             {"items", json{{"type", "string"}}},
                             {"description", "Tags for the document"}};
        props["metadata"] = makeProp("object", "Additional metadata key-value pairs");
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // get
    {
        json tool;
        tool["name"] = "get";
        tool["description"] = "Retrieve a document by hash or name";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["hash"] = makeProp("string", "Document SHA-256 hash");
        props["name"] = makeProp("string", "Document name");
        props["outputPath"] = makeProp("string", "Output file path for retrieved content");
        props["graph"] = makeProp("boolean", "Include knowledge graph relationships");
        props["graph"]["default"] = false;
        props["depth"] = makeProp("integer", "Graph traversal depth (1-5)");
        props["depth"]["default"] = 1;
        props["include_content"] = makeProp("boolean", "Include full content in graph results");
        props["include_content"]["default"] = false;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // delete_by_name
    {
        json tool;
        tool["name"] = "delete_by_name";
        tool["description"] = "Delete documents by name with pattern support";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["name"] = makeProp("string", "Document name");
        props["names"] = json{{"type", "array"},
                              {"items", json{{"type", "string"}}},
                              {"description", "Multiple document names"}};
        props["pattern"] = makeProp("string", "Glob pattern for matching names");
        props["dry_run"] = makeProp("boolean", "Preview what would be deleted");
        props["dry_run"]["default"] = false;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // update
    {
        json tool;
        tool["name"] = "update";
        tool["description"] = "Update document metadata";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["hash"] = makeProp("string", "Document SHA-256 hash");
        props["name"] = makeProp("string", "Document name (alternative to hash)");
        props["type"] = makeProp("string", "Update target discriminator (e.g., metadata, tags)");
        props["metadata"] = makeProp("object", "Metadata key-value pairs to update");
        props["tags"] = json{{"type", "array"},
                             {"items", json{{"type", "string"}}},
                             {"description", "Tags to add or update"}};
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // list
    {
        json tool;
        tool["name"] = "list";
        tool["description"] = "List documents with optional filtering";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["limit"] = makeProp("integer", "Maximum number of results");
        props["limit"]["default"] = 100;
        props["offset"] = makeProp("integer", "Offset for pagination");
        props["offset"]["default"] = 0;
        props["name"] = makeProp("string", "Exact name filter (optional)");
        props["pattern"] = makeProp("string", "Glob pattern for filtering names");
        props["tags"] = json{{"type", "array"},
                             {"items", json{{"type", "string"}}},
                             {"description", "Filter by tags"}};
        props["metadata"] = makeProp("object", "Metadata key/value filter (optional)");
        props["type"] = makeProp("string", "Filter by file type category");
        props["mime"] = makeProp("string", "Filter by MIME type pattern");
        props["extension"] = makeProp("string", "Filter by file extension");
        props["binary"] = makeProp("boolean", "Filter binary files");
        props["text"] = makeProp("boolean", "Filter text files");
        props["created_after"] = makeProp("string", "ISO 8601 timestamp or relative time");
        props["created_before"] = makeProp("string", "ISO 8601 timestamp or relative time");
        props["modified_after"] = makeProp("string", "ISO 8601 timestamp or relative time");
        props["modified_before"] = makeProp("string", "ISO 8601 timestamp or relative time");
        props["indexed_after"] = makeProp("string", "ISO 8601 timestamp or relative time");
        props["indexed_before"] = makeProp("string", "ISO 8601 timestamp or relative time");
        props["recent"] = makeProp("integer", "Get N most recent documents");
        props["sort_by"] =
            makeProp("string", "Sort field (values: name, size, created, modified, indexed)");
        props["sort_by"]["default"] = "indexed";
        props["sort_order"] = makeProp("string", "Sort order (values: asc, desc)");
        props["sort_order"]["default"] = "desc";
        props["with_labels"] = makeProp("boolean", "Include snapshot labels in results");
        props["with_labels"]["default"] = false;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // stats
    {
        json tool;
        tool["name"] = "stats";
        tool["description"] = "Get storage statistics and health status";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["detailed"] = makeProp("boolean", "");
        props["detailed"]["default"] = false;
        props["file_types"] = makeProp("boolean", "Include file type breakdown");
        props["file_types"]["default"] = false;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // get_by_name
    {
        json tool;
        tool["name"] = "get_by_name";
        tool["description"] = "Retrieve document content by name";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["name"] = makeProp("string", "Document name");
        props["raw_content"] = makeProp("boolean", "Return raw content without text extraction");
        props["raw_content"]["default"] = false;
        props["extract_text"] = makeProp("boolean", "Extract text from HTML/PDF files");
        props["extract_text"]["default"] = true;
        schema["properties"] = props;
        schema["required"] = json::array({"name"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // cat
    {
        json tool;
        tool["name"] = "cat";
        tool["description"] = "Display document content (like cat command)";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["hash"] = makeProp("string", "Document SHA-256 hash");
        props["name"] = makeProp("string", "Document name");
        props["raw_content"] = makeProp("boolean", "Return raw content without text extraction");
        props["raw_content"]["default"] = false;
        props["extract_text"] = makeProp("boolean", "Extract text from HTML/PDF files");
        props["extract_text"]["default"] = true;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // add_directory
    {
        json tool;
        tool["name"] = "add_directory";
        tool["description"] = "Add all files from a directory";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["directory_path"] = makeProp("string", "Directory path");
        props["recursive"] = makeProp("boolean", "Recursively add subdirectories");
        props["recursive"]["default"] = false;
        props["collection"] = makeProp("string", "Collection name for grouping");
        props["snapshot_id"] = makeProp("string", "Snapshot ID for versioning");
        props["snapshot_label"] = makeProp("string", "Human-readable snapshot label");
        props["include_patterns"] = json{{"type", "array"},
                                         {"items", json{{"type", "string"}}},
                                         {"description", "Include patterns (e.g., *.txt)"}};
        props["exclude_patterns"] = json{{"type", "array"},
                                         {"items", json{{"type", "string"}}},
                                         {"description", "Exclude patterns"}};
        props["tags"] = json{{"type", "array"},
                             {"items", json{{"type", "string"}}},
                             {"description", "Tags to add to each stored document"}};
        props["metadata"] =
            makeProp("object", "Additional metadata key-value pairs applied to each document");
        schema["properties"] = props;
        schema["required"] = json::array({"directory_path"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // restore_collection
    {
        json tool;
        tool["name"] = "restore_collection";
        tool["description"] = "Restore all documents from a collection";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["collection"] = makeProp("string", "Collection name");
        props["output_directory"] = makeProp("string", "Output directory");
        props["layout_template"] =
            makeProp("string", "Layout template (e.g., {collection}/{path})");
        props["layout_template"]["default"] = "{path}";
        props["include_patterns"] =
            json{{"type", "array"},
                 {"items", json{{"type", "string"}}},
                 {"description", "Only restore files matching these patterns"}};
        props["exclude_patterns"] = json{{"type", "array"},
                                         {"items", json{{"type", "string"}}},
                                         {"description", "Exclude files matching these patterns"}};
        props["overwrite"] = makeProp("boolean", "Overwrite files if they already exist");
        props["overwrite"]["default"] = false;
        props["create_dirs"] = makeProp("boolean", "Create parent directories if needed");
        props["create_dirs"]["default"] = true;
        props["dry_run"] = makeProp("boolean", "Show what would be restored without writing files");
        props["dry_run"]["default"] = false;
        schema["properties"] = props;
        schema["required"] = json::array({"collection", "output_directory"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // restore_snapshot
    {
        json tool;
        tool["name"] = "restore_snapshot";
        tool["description"] = "Restore all documents from a snapshot";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["snapshot_id"] = makeProp("string", "Snapshot ID");
        props["snapshot_label"] = makeProp("string", "Snapshot label (alternative to snapshot_id)");
        props["output_directory"] = makeProp("string", "Output directory");
        props["layout_template"] = makeProp("string", "Layout template");
        props["layout_template"]["default"] = "{path}";
        props["include_patterns"] =
            json{{"type", "array"},
                 {"items", json{{"type", "string"}}},
                 {"description", "Only restore files matching these patterns"}};
        props["exclude_patterns"] = json{{"type", "array"},
                                         {"items", json{{"type", "string"}}},
                                         {"description", "Exclude files matching these patterns"}};
        props["overwrite"] = makeProp("boolean", "Overwrite files if they already exist");
        props["overwrite"]["default"] = false;
        props["create_dirs"] = makeProp("boolean", "Create parent directories if needed");
        props["create_dirs"]["default"] = true;
        props["dry_run"] = makeProp("boolean", "Show what would be restored without writing files");
        props["dry_run"]["default"] = false;
        schema["properties"] = props;
        schema["required"] = json::array({"snapshot_id", "output_directory"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // restore (combined)
    {
        json tool;
        tool["name"] = "restore";
        tool["description"] = "Restore documents from a collection or snapshot";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["collection"] = makeProp("string", "Collection name");
        props["snapshot_id"] = makeProp("string", "Snapshot ID");
        props["output_directory"] = makeProp("string", "Output directory");
        props["layout_template"] =
            makeProp("string", "Layout template (e.g., {collection}/{path})");
        props["layout_template"]["default"] = "{path}";
        props["include_patterns"] =
            json{{"type", "array"},
                 {"items", json{{"type", "string"}}},
                 {"description", "Only restore files matching these patterns"}};
        props["exclude_patterns"] = json{{"type", "array"},
                                         {"items", json{{"type", "string"}}},
                                         {"description", "Exclude files matching these patterns"}};
        props["overwrite"] = makeProp("boolean", "Overwrite existing files");
        props["overwrite"]["default"] = false;
        props["create_dirs"] = makeProp("boolean", "Create parent directories if needed");
        props["create_dirs"]["default"] = true;
        props["dry_run"] = makeProp("boolean", "Show what would be restored without writing files");
        props["dry_run"]["default"] = false;
        schema["properties"] = props;
        schema["required"] = json::array({"output_directory"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // list_collections
    {
        json tool;
        tool["name"] = "list_collections";
        tool["description"] = "List available collections";
        json schema;
        schema["type"] = "object";
        schema["properties"] = json::object();
        schema["required"] = json::array();
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // list_snapshots
    {
        json tool;
        tool["name"] = "list_snapshots";
        tool["description"] = "List available snapshots";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["collection"] = makeProp("string", "Filter by collection");
        props["with_labels"] = makeProp("boolean", "Include snapshot labels");
        props["with_labels"]["default"] = true;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    return json{{"tools", tools}};
}

json yams::mcp::MCPServer::listPrompts() {
    return {{"prompts",
             json::array(
                 {{{"name", "search_codebase"},
                   {"description", "Search for code patterns in the codebase"},
                   {"arguments",
                    json::array({{{"name", "pattern"},
                                  {"description", "Code pattern to search for"},
                                  {"required", true}},
                                 {{"name", "file_type"},
                                  {"description", "Filter by file type (e.g., cpp, py, js)"},
                                  {"required", false}}})}},
                  {{"name", "summarize_document"},
                   {"description", "Generate a summary of a document"},
                   {"arguments", json::array({{{"name", "document_name"},
                                               {"description", "Name of the document to summarize"},
                                               {"required", true}},
                                              {{"name", "max_length"},
                                               {"description", "Maximum summary length in words"},
                                               {"required", false}}})}}})}};
}

// Pure O(1) callTool implementation
json MCPServer::callTool(const std::string& name, const json& arguments) {
    spdlog::debug("MCP callTool: name='{}', arguments={}", name, arguments.dump());

    if (!toolRegistry_) {
        return {{"error", "Tool registry not initialized"}};
    }

    auto toolResult = toolRegistry_->callTool(name, arguments);
    // Wrap non-MCP-shaped results into MCP content format for compatibility with MCP clients
    if (toolResult.contains("content") && toolResult["content"].is_array()) {
        return toolResult;
    }
    json wrapped = {
        {"content", json::array({json{{"type", "text"}, {"text", toolResult.dump(2)}}})}};
    return wrapped;
}

// Modern C++20 tool handler implementations
Result<MCPSearchResponse> MCPServer::handleSearchDocuments(const MCPSearchRequest& req) {
    // This function will now be a client to the daemon.
    // It converts the MCP request to a daemon request, sends it, and converts the response.

    // 1. Convert MCP request to daemon IPC request
    daemon::SearchRequest daemon_req;
    daemon_req.query = req.query;
    daemon_req.limit = req.limit;
    daemon_req.fuzzy = req.fuzzy;
    daemon_req.similarity = static_cast<double>(req.similarity);
    daemon_req.hashQuery = req.hash;
    daemon_req.searchType = req.type;
    daemon_req.verbose = req.verbose;
    daemon_req.pathsOnly = req.pathsOnly;
    daemon_req.showLineNumbers = req.lineNumbers;
    daemon_req.beforeContext = req.beforeContext;
    daemon_req.afterContext = req.afterContext;
    daemon_req.context = req.context;

    // daemon_req.tags = req.tags; // Tags need to be handled if protocol supports it
    // daemon_req.matchAllTags = req.matchAllTags;

    // 2. Execute request via the pooled manager
    MCPSearchResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::SearchResponse& resp) -> Result<void> {
        // 3. Convert daemon response to MCP response
        mcp_response.total = resp.totalCount;
        mcp_response.type = "daemon"; // Indicate response came from daemon
        mcp_response.executionTimeMs = resp.elapsed.count();

        for (const auto& item : resp.results) {
            MCPSearchResponse::Result mcp_result;
            mcp_result.id = item.id;
            mcp_result.hash = item.metadata.count("hash") ? item.metadata.at("hash") : "";
            mcp_result.title = item.title;
            mcp_result.path = item.path;
            mcp_result.score = item.score;
            mcp_result.snippet = item.snippet;
            mcp_response.results.push_back(mcp_result);
        }
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error = Error{ErrorCode::NetworkError,
                                "Daemon not available or failed to respond. Try restarting the "
                                "daemon (yams daemon start) and re-run the command."};
        return Result<void>();
    };

    auto result = search_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPGrepResponse> MCPServer::handleGrepDocuments(const MCPGrepRequest& req) {
    // Convert MCP request to daemon request
    daemon::GrepRequest daemon_req;
    daemon_req.pattern = req.pattern;
    daemon_req.paths = req.paths;
    daemon_req.caseInsensitive = req.ignoreCase;
    daemon_req.wholeWord = req.word;
    daemon_req.invertMatch = req.invert;
    daemon_req.showLineNumbers = req.lineNumbers;
    daemon_req.showFilename = req.withFilename;
    daemon_req.countOnly = req.count;
    daemon_req.filesOnly = req.filesWithMatches;
    daemon_req.filesWithoutMatch = req.filesWithoutMatch;
    daemon_req.afterContext = req.afterContext;
    daemon_req.beforeContext = req.beforeContext;
    daemon_req.contextLines = req.context;
    daemon_req.colorMode = req.color;
    if (req.maxCount) {
        daemon_req.maxMatches = *req.maxCount;
    }

    MCPGrepResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::GrepResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.matchCount = resp.totalMatches;
        mcp_response.fileCount = resp.filesSearched;

        std::ostringstream oss;
        for (const auto& match : resp.matches) {
            if (mcp_response.fileCount > 1 || req.withFilename) {
                oss << match.file << ":";
            }
            if (req.lineNumbers) {
                oss << match.lineNumber << ":";
            }
            oss << match.line << "\n";
        }
        mcp_response.output = oss.str();

        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = grep_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPDownloadResponse> MCPServer::handleDownload(const MCPDownloadRequest& req) {
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

    // Reuse existing ConfigMigrator to read config.v2 values (downloader.*)
    try {
        namespace fs = std::filesystem;
        // Resolve default config path like daemon does
        std::string cfgPath;
        if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            cfgPath = (fs::path(xdgConfigHome) / "yams" / "config.toml").string();
        } else if (const char* homeEnv = std::getenv("HOME")) {
            cfgPath = (fs::path(homeEnv) / ".config" / "yams" / "config.toml").string();
        }

        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            yams::config::ConfigMigrator migrator;
            auto parsed = migrator.parseTomlConfig(cfgPath);
            if (parsed) {
                const auto& toml = parsed.value();
                if (toml.find("downloader") != toml.end()) {
                    const auto& dl = toml.at("downloader");
                    if (dl.find("default_concurrency") != dl.end()) {
                        try {
                            cfg.defaultConcurrency = std::stoi(dl.at("default_concurrency"));
                        } catch (...) {
                        }
                    }
                    if (dl.find("default_chunk_size_bytes") != dl.end()) {
                        try {
                            cfg.defaultChunkSizeBytes = static_cast<std::size_t>(
                                std::stoull(dl.at("default_chunk_size_bytes")));
                        } catch (...) {
                        }
                    }
                    if (dl.find("default_timeout_ms") != dl.end()) {
                        try {
                            cfg.defaultTimeout =
                                std::chrono::milliseconds(std::stoll(dl.at("default_timeout_ms")));
                        } catch (...) {
                        }
                    }
                    if (dl.find("follow_redirects") != dl.end()) {
                        cfg.followRedirects = (dl.at("follow_redirects") == "true");
                    }
                    if (dl.find("resume") != dl.end()) {
                        cfg.resume = (dl.at("resume") == "true");
                    }
                    if (dl.find("store_only") != dl.end()) {
                        cfg.storeOnly = (dl.at("store_only") == "true");
                    }
                    if (dl.find("max_file_bytes") != dl.end()) {
                        try {
                            cfg.maxFileBytes =
                                static_cast<std::uint64_t>(std::stoull(dl.at("max_file_bytes")));
                        } catch (...) {
                        }
                    }
                    // rate limits and checksum algo are present in config; if needed later, map
                    // similarly
                }
                if (toml.find("storage") != toml.end()) {
                    const auto& st = toml.at("storage");
                    if (st.find("objects_dir") != st.end()) {
                        storage.objectsDir = fs::path(st.at("objects_dir"));
                    }
                    if (st.find("staging_dir") != st.end()) {
                        storage.stagingDir = fs::path(st.at("staging_dir"));
                    }
                }
            }
        }
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

    // Resolve and ensure staging directory exists to avoid regression
    try {
        namespace fs = std::filesystem;
        auto ensure_dir = [](const fs::path& p) -> bool {
            if (p.empty())
                return false;
            std::error_code ec;
            fs::create_directories(p, ec);
            return !ec && fs::exists(p);
        };

        if (storage.stagingDir.empty()) {
            // Prefer XDG_STATE_HOME, then HOME, then /tmp
            fs::path staging;
            if (const char* xdgState = std::getenv("XDG_STATE_HOME")) {
                staging = fs::path(xdgState) / "yams" / "staging" / "downloader";
            } else if (const char* homeEnv = std::getenv("HOME")) {
                staging =
                    fs::path(homeEnv) / ".local" / "state" / "yams" / "staging" / "downloader";
            } else {
                staging = fs::path("/tmp") / "yams" / "staging" / "downloader";
            }
            storage.stagingDir = staging;
        }
        if (!ensure_dir(storage.stagingDir)) {
            return Error{ErrorCode::InternalError, std::string("Failed to create staging dir: ") +
                                                       storage.stagingDir.string()};
        }

        // Optionally ensure objectsDir if provided
        if (!storage.objectsDir.empty()) {
            (void)ensure_dir(storage.objectsDir);
        }
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
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
        return Error{ErrorCode::InternalError, dlRes.error().message};
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
        addReq.path = mcp_response.storedPath; // index by stored path
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

        std::optional<Error> add_error;
        auto renderAdd = [&](const daemon::AddDocumentResponse& resp) -> Result<void> {
            (void)resp;
            return Result<void>();
        };
        auto fallbackAdd = [&]() -> Result<void> {
            add_error = Error{ErrorCode::NetworkError, "Daemon not available during post-index"};
            return Result<void>();
        };
        if (auto res = store_req_manager_->execute(addReq, fallbackAdd, renderAdd); !res) {
            if (verbose) {
                spdlog::debug("[MCP] post-index: daemon call failed error='{}'",
                              res.error().message);
            }
            return res.error();
        }
        if (add_error) {
            if (verbose) {
                spdlog::debug("[MCP] post-index: fallback error='{}'", add_error->message);
            }
            return add_error.value();
        }
    }

    return mcp_response;
}

Result<MCPStoreDocumentResponse>
MCPServer::handleStoreDocument(const MCPStoreDocumentRequest& req) {
    // Convert MCP request to daemon request
    daemon::AddDocumentRequest daemon_req;
    daemon_req.path = req.path;
    daemon_req.content = req.content;
    daemon_req.name = req.name;
    daemon_req.mimeType = req.mimeType;
    daemon_req.tags = req.tags;
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) {
            daemon_req.metadata[key] = value.get<std::string>();
        } else {
            daemon_req.metadata[key] = value.dump();
        }
    }

    MCPStoreDocumentResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::AddDocumentResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.hash = resp.hash;
        // Note: The daemon AddDocumentResponse doesn't have bytesStored/bytesDeduped.
        // We can leave them as 0 or enhance the daemon protocol later.
        mcp_response.bytesStored = 0;
        mcp_response.bytesDeduped = 0;
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = store_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPRetrieveDocumentResponse>
MCPServer::handleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
    // Convert MCP request to daemon request
    daemon::GetRequest daemon_req;
    daemon_req.hash = req.hash;
    daemon_req.outputPath = req.outputPath;
    daemon_req.showGraph = req.graph;
    daemon_req.graphDepth = req.depth;
    daemon_req.metadataOnly = !req.includeContent;

    MCPRetrieveDocumentResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::GetResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.hash = resp.hash;
        mcp_response.path = resp.path;
        mcp_response.name = resp.name;
        mcp_response.size = resp.size;
        mcp_response.mimeType = resp.mimeType;
        if (resp.hasContent) {
            mcp_response.content = resp.content;
        }
        mcp_response.graphEnabled = resp.graphEnabled;
        for (const auto& rel : resp.related) {
            json relatedJson = {{"hash", rel.hash}, {"path", rel.path}, {"distance", rel.distance}};
            mcp_response.related.push_back(std::move(relatedJson));
        }
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = retrieve_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPListDocumentsResponse>
MCPServer::handleListDocuments(const MCPListDocumentsRequest& req) {
    // Convert MCP request to daemon request
    daemon::ListRequest daemon_req;
    daemon_req.namePattern = req.pattern;
    daemon_req.tags = req.tags;
    daemon_req.fileType = req.type;
    daemon_req.mimeType = req.mime;
    daemon_req.extensions = req.extension;
    daemon_req.binaryOnly = req.binary;
    daemon_req.textOnly = req.text;
    daemon_req.recentCount = req.recent;
    daemon_req.sortBy = req.sortBy;
    daemon_req.reverse = (req.sortOrder == "desc");
    daemon_req.limit = req.limit;
    daemon_req.offset = req.offset;

    MCPListDocumentsResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::ListResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.total = resp.totalCount;
        for (const auto& item : resp.items) {
            json docJson = {{"hash", item.hash},          {"path", item.path},
                            {"name", item.name},          {"size", item.size},
                            {"mime_type", item.mimeType}, {"created", item.created},
                            {"modified", item.modified},  {"indexed", item.indexed}};
            mcp_response.documents.push_back(std::move(docJson));
        }
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = list_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPStatsResponse> MCPServer::handleGetStats(const MCPStatsRequest& req) {
    // Convert MCP request to daemon request
    daemon::GetStatsRequest daemon_req;
    daemon_req.showFileTypes = req.fileTypes;
    daemon_req.detailed = req.verbose;

    MCPStatsResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::GetStatsResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.totalObjects = resp.totalDocuments;
        mcp_response.totalBytes = resp.totalSize;
        mcp_response.uniqueHashes = resp.additionalStats.count("unique_hashes")
                                        ? std::stoull(resp.additionalStats.at("unique_hashes"))
                                        : 0;
        mcp_response.deduplicationSavings =
            resp.additionalStats.count("deduplicated_bytes")
                ? std::stoull(resp.additionalStats.at("deduplicated_bytes"))
                : 0;

        for (const auto& [key, value] : resp.documentsByType) {
            json ftJson;
            ftJson["extension"] = key;
            ftJson["count"] = value;
            mcp_response.fileTypes.push_back(ftJson);
        }

        mcp_response.additionalStats = resp.additionalStats;
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = stats_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPAddDirectoryResponse> MCPServer::handleAddDirectory(const MCPAddDirectoryRequest& req) {
    // Convert MCP request to daemon request
    daemon::AddDocumentRequest daemon_req;
    daemon_req.path = req.directoryPath;
    daemon_req.collection = req.collection;
    daemon_req.includePatterns = req.includePatterns;
    daemon_req.excludePatterns = req.excludePatterns;
    daemon_req.recursive = req.recursive;
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) {
            daemon_req.metadata[key] = value.get<std::string>();
        } else {
            daemon_req.metadata[key] = value.dump();
        }
    }

    MCPAddDirectoryResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::AddDocumentResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.directoryPath = resp.path;
        mcp_response.collection =
            daemon_req.collection; // Not in daemon response, use request value
        mcp_response.filesIndexed = resp.documentsAdded;
        // Other fields are not in the daemon response, so we leave them default.
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = store_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPUpdateMetadataResponse>
MCPServer::handleUpdateMetadata(const MCPUpdateMetadataRequest& req) {
    // Convert MCP request to daemon request
    daemon::UpdateDocumentRequest daemon_req;
    daemon_req.hash = req.hash;
    daemon_req.name = req.name;
    daemon_req.addTags = req.tags;
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) {
            daemon_req.metadata[key] = value.get<std::string>();
        } else {
            daemon_req.metadata[key] = value.dump();
        }
    }

    MCPUpdateMetadataResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::UpdateDocumentResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.success = resp.metadataUpdated || resp.tagsUpdated || resp.contentUpdated;
        mcp_response.message = "Update successful";
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = update_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

void MCPServer::initializeToolRegistry() {
    toolRegistry_ = std::make_unique<ToolRegistry>();

    toolRegistry_->registerTool<MCPSearchRequest, MCPSearchResponse>(
        "search", [this](const MCPSearchRequest& req) { return handleSearchDocuments(req); },
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
              {"tags",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "Filter by tags"}}}}},
            {"required", json::array({"query"})}},
        "Search documents using hybrid search (vector + full-text + knowledge graph)");

    toolRegistry_->registerTool<MCPGrepRequest, MCPGrepResponse>(
        "grep", [this](const MCPGrepRequest& req) { return handleGrepDocuments(req); },
        json{{"type", "object"},
             {"properties",
              {{"pattern", {{"type", "string"}, {"description", "Regex pattern to search"}}},
               {"paths",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Paths to search"}}},
               {"ignore_case",
                {{"type", "boolean"},
                 {"description", "Case insensitive search"},
                 {"default", false}}},
               {"line_numbers",
                {{"type", "boolean"}, {"description", "Show line numbers"}, {"default", false}}},
               {"context",
                {{"type", "integer"}, {"description", "Context lines"}, {"default", 0}}}}},
             {"required", json::array({"pattern"})}},
        "Search documents using regular expressions with grep-like functionality");

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
        "post-index the artifact.");

    toolRegistry_->registerTool<MCPStoreDocumentRequest, MCPStoreDocumentResponse>(
        "add", [this](const MCPStoreDocumentRequest& req) { return handleStoreDocument(req); },
        json{{"type", "object"},
             {"properties",
              {{"path", {{"type", "string"}, {"description", "File path to store"}}},
               {"content", {{"type", "string"}, {"description", "Document content"}}},
               {"name", {{"type", "string"}, {"description", "Document name"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Document tags"}}}}}},
        "Store documents with deduplication and content-based addressing");

    toolRegistry_->registerTool<MCPRetrieveDocumentRequest, MCPRetrieveDocumentResponse>(
        "get",
        [this](const MCPRetrieveDocumentRequest& req) { return handleRetrieveDocument(req); },
        json{{"type", "object"},
             {"properties",
              {{"hash", {{"type", "string"}, {"description", "Document hash"}}},
               {"output_path", {{"type", "string"}, {"description", "Output file path"}}},
               {"include_content",
                {{"type", "boolean"},
                 {"description", "Include content in response"},
                 {"default", false}}}}},
             {"required", json::array({"hash"})}},
        "Retrieve documents from storage by hash with optional knowledge graph expansion");

    toolRegistry_->registerTool<MCPListDocumentsRequest, MCPListDocumentsResponse>(
        "list", [this](const MCPListDocumentsRequest& req) { return handleListDocuments(req); },
        json{{"type", "object"},
             {"properties",
              {{"pattern", {{"type", "string"}, {"description", "Name pattern filter"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Filter by tags"}}},
               {"recent", {{"type", "integer"}, {"description", "Show N most recent documents"}}},
               {"limit",
                {{"type", "integer"},
                 {"description", "Maximum number of results"},
                 {"default", 100}}},
               {"offset",
                {{"type", "integer"}, {"description", "Offset for pagination"}, {"default", 0}}}}}},
        "List documents with filtering by pattern, tags, type, or recency");

    toolRegistry_->registerTool<MCPStatsRequest, MCPStatsResponse>(
        "stats", [this](const MCPStatsRequest& req) { return handleGetStats(req); },
        json{{"type", "object"},
             {"properties",
              {{"file_types",
                {{"type", "boolean"},
                 {"description", "Include file type breakdown"},
                 {"default", false}}},
               {"verbose",
                {{"type", "boolean"},
                 {"description", "Include verbose statistics"},
                 {"default", false}}}}}},
        "Get storage statistics including deduplication savings and file type breakdown");

    toolRegistry_->registerTool<MCPAddDirectoryRequest, MCPAddDirectoryResponse>(
        "add_directory",
        [this](const MCPAddDirectoryRequest& req) { return handleAddDirectory(req); },
        json{{"type", "object"},
             {"properties",
              {{"directory_path", {{"type", "string"}, {"description", "Directory path to index"}}},
               {"collection", {{"type", "string"}, {"description", "Collection name"}}},
               {"recursive",
                {{"type", "boolean"}, {"description", "Index recursively"}, {"default", true}}},
               {"include_patterns",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "File patterns to include"}}}}},
             {"required", json::array({"directory_path"})}},
        "Index all files from a directory into YAMS storage with optional filtering");

    toolRegistry_->registerTool<MCPGetByNameRequest, MCPGetByNameResponse>(
        "get_by_name", [this](const MCPGetByNameRequest& req) { return handleGetByName(req); },
        json{{"type", "object"},
             {"properties",
              {{"name", {{"type", "string"}, {"description", "Document name to retrieve"}}},
               {"raw_content",
                {{"type", "boolean"},
                 {"description", "Return raw content without text extraction"},
                 {"default", false}}},
               {"extract_text",
                {{"type", "boolean"},
                 {"description", "Extract text from HTML/PDF files"},
                 {"default", true}}}}},
             {"required", json::array({"name"})}},
        "Retrieve document content by name");

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
        "Delete documents by name, names array, or pattern");

    toolRegistry_->registerTool<MCPCatDocumentRequest, MCPCatDocumentResponse>(
        "cat", [this](const MCPCatDocumentRequest& req) { return handleCatDocument(req); },
        json{{"type", "object"},
             {"properties",
              {{"hash", {{"type", "string"}, {"description", "Document SHA-256 hash"}}},
               {"name", {{"type", "string"}, {"description", "Document name"}}},
               {"raw_content",
                {{"type", "boolean"},
                 {"description", "Return raw content without text extraction"},
                 {"default", false}}},
               {"extract_text",
                {{"type", "boolean"},
                 {"description", "Extract text from HTML/PDF files"},
                 {"default", true}}}}}},
        "Display document content by hash or name");

    toolRegistry_->registerTool<MCPUpdateMetadataRequest, MCPUpdateMetadataResponse>(
        "update", [this](const MCPUpdateMetadataRequest& req) { return handleUpdateMetadata(req); },
        json{{"type", "object"},
             {"properties",
              {{"hash", {{"type", "string"}, {"description", "Document hash"}}},
               {"name", {{"type", "string"}, {"description", "Document name"}}},
               {"metadata",
                {{"type", "object"}, {"description", "Metadata key-value pairs to update"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Tags to add or update"}}}}}},
        "Update document metadata and tags");

    toolRegistry_->registerTool<MCPRestoreCollectionRequest, MCPRestoreCollectionResponse>(
        "restore_collection",
        [this](const MCPRestoreCollectionRequest& req) { return handleRestoreCollection(req); },
        json{{"type", "object"},
             {"properties",
              {{"collection", {{"type", "string"}, {"description", "Collection name"}}},
               {"output_directory", {{"type", "string"}, {"description", "Output directory"}}},
               {"overwrite",
                {{"type", "boolean"},
                 {"description", "Overwrite existing files"},
                 {"default", false}}},
               {"dry_run",
                {{"type", "boolean"},
                 {"description", "Preview without writing"},
                 {"default", false}}}}},
             {"required", json::array({"collection", "output_directory"})}},
        "Restore all documents from a collection");

    toolRegistry_->registerTool<MCPRestoreSnapshotRequest, MCPRestoreSnapshotResponse>(
        "restore_snapshot",
        [this](const MCPRestoreSnapshotRequest& req) { return handleRestoreSnapshot(req); },
        json{{"type", "object"},
             {"properties",
              {{"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID"}}},
               {"output_directory", {{"type", "string"}, {"description", "Output directory"}}},
               {"overwrite",
                {{"type", "boolean"},
                 {"description", "Overwrite existing files"},
                 {"default", false}}},
               {"dry_run",
                {{"type", "boolean"},
                 {"description", "Preview without writing"},
                 {"default", false}}}}},
             {"required", json::array({"snapshot_id", "output_directory"})}},
        "Restore all documents from a snapshot");

    toolRegistry_->registerTool<MCPListCollectionsRequest, MCPListCollectionsResponse>(
        "list_collections",
        [this](const MCPListCollectionsRequest& req) { return handleListCollections(req); },
        json{{"type", "object"}}, "List available collections");

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
        "List available snapshots");
}

json MCPServer::createResponse(const json& id, const json& result) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
}

json MCPServer::createError(const json& id, int code, const std::string& message) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}};
}

Result<MCPGetByNameResponse> MCPServer::handleGetByName(const MCPGetByNameRequest& req) {
    // Streamed retrieval via GetInit/GetChunk/GetEnd to mirror CLI robustness
    MCPGetByNameResponse mcp_response;

    // Prepare pooled client for streaming calls
    yams::cli::AsioClientPool pool{};

    daemon::GetInitRequest init{};
    init.name = req.name;
    init.byName = true;
    // raw/extract flags removed from daemon::GetInitRequest; behavior determined server-side

    auto initRes = pool.call<daemon::GetInitRequest, daemon::GetInitResponse>(init);
    if (!initRes) {
        return initRes.error();
    }
    const auto& initVal = initRes.value();
    // Map GetInitResponse fields and metadata into MCP response
    mcp_response.size = initVal.totalSize;
    // Optional metadata keys: hash, path, fileName, mimeType
    if (auto it = initVal.metadata.find("hash"); it != initVal.metadata.end()) {
        mcp_response.hash = it->second;
    }
    if (auto it = initVal.metadata.find("fileName"); it != initVal.metadata.end()) {
        mcp_response.name = it->second;
    }
    if (auto it = initVal.metadata.find("path"); it != initVal.metadata.end()) {
        mcp_response.path = it->second;
    }
    if (auto it = initVal.metadata.find("mimeType"); it != initVal.metadata.end()) {
        mcp_response.mimeType = it->second;
    }

    // Chunked read with cap to avoid huge MCP payloads
    static constexpr std::size_t CHUNK = 64 * 1024;
    static constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024; // 1 MiB cap
    std::string buffer;
    buffer.reserve(std::min<std::size_t>(MAX_BYTES, static_cast<std::size_t>(initVal.totalSize)));

    std::uint64_t offset = 0;
    bool truncated = false;

    while (offset < initVal.totalSize) {
        daemon::GetChunkRequest c{};
        c.transferId = initVal.transferId;
        c.offset = offset;
        c.length =
            static_cast<std::uint32_t>(std::min<std::uint64_t>(CHUNK, initVal.totalSize - offset));
        auto cRes = pool.call<daemon::GetChunkRequest, daemon::GetChunkResponse>(c);
        if (!cRes) {
            // Attempt to close the transfer before returning error
            daemon::GetEndRequest e{};
            e.transferId = initVal.transferId;
            (void)pool.call<daemon::GetEndRequest, daemon::SuccessResponse>(e);
            return cRes.error();
        }
        const auto& chunk = cRes.value();
        if (!chunk.data.empty()) {
            if (buffer.size() + chunk.data.size() <= MAX_BYTES) {
                buffer.append(chunk.data.data(), chunk.data.size());
            } else {
                auto remaining = MAX_BYTES - buffer.size();
                buffer.append(chunk.data.data(), remaining);
                truncated = true;
                offset += chunk.data.size();
                break;
            }
        }
        offset += chunk.data.size();
    }

    // End transfer regardless
    daemon::GetEndRequest end{};
    end.transferId = initVal.transferId;
    (void)pool.call<daemon::GetEndRequest, daemon::SuccessResponse>(end);

    mcp_response.content = std::move(buffer);
    // If truncated, we simply return the capped content; no extra flags in response type.
    return mcp_response;
}

Result<MCPDeleteByNameResponse> MCPServer::handleDeleteByName(const MCPDeleteByNameRequest& req) {
    // Convert MCP request to daemon request
    daemon::DeleteRequest daemon_req;
    daemon_req.name = req.name;
    daemon_req.names = req.names;
    daemon_req.pattern = req.pattern;
    daemon_req.dryRun = req.dryRun;

    MCPDeleteByNameResponse mcp_response;
    std::optional<Error> execution_error;

    auto render = [&](const daemon::DeleteResponse& resp) -> Result<void> {
        // Convert daemon response to MCP response
        mcp_response.count = resp.results.size();
        mcp_response.dryRun = resp.dryRun;
        for (const auto& result : resp.results) {
            if (result.success) {
                mcp_response.deleted.push_back(result.name);
            }
        }
        return Result<void>();
    };

    auto fallback = [&]() -> Result<void> {
        execution_error =
            Error{ErrorCode::NetworkError, "Daemon not available or failed to respond"};
        return Result<void>();
    };

    auto result = delete_req_manager_->execute(daemon_req, fallback, render);

    if (execution_error) {
        return execution_error.value();
    }

    if (!result) {
        return result.error();
    }

    return mcp_response;
}

Result<MCPCatDocumentResponse> MCPServer::handleCatDocument(const MCPCatDocumentRequest& req) {
    // Streamed retrieval via GetInit/GetChunk/GetEnd to mirror CLI behavior for large content
    MCPCatDocumentResponse mcp_response;

    yams::cli::AsioClientPool pool{};

    daemon::GetInitRequest init{};
    init.hash = req.hash;
    init.name = req.name;
    init.byName = !req.name.empty();
    // GetInitRequest no longer supports raw/extract flags; server determines mode

    auto initRes = pool.call<daemon::GetInitRequest, daemon::GetInitResponse>(init);
    if (!initRes) {
        return initRes.error();
    }
    const auto& initVal = initRes.value();
    // Map GetInitResponse fields and metadata into MCP response
    mcp_response.size = initVal.totalSize;
    if (auto it = initVal.metadata.find("hash"); it != initVal.metadata.end()) {
        mcp_response.hash = it->second;
    }
    if (auto it = initVal.metadata.find("fileName"); it != initVal.metadata.end()) {
        mcp_response.name = it->second;
    }

    static constexpr std::size_t CHUNK = 64 * 1024;
    static constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024; // 1 MiB cap
    std::string buffer;
    buffer.reserve(std::min<std::size_t>(MAX_BYTES, static_cast<std::size_t>(initVal.totalSize)));

    std::uint64_t offset = 0;
    bool truncated = false;

    while (offset < initVal.totalSize) {
        daemon::GetChunkRequest c{};
        c.transferId = initVal.transferId;
        c.offset = offset;
        c.length =
            static_cast<std::uint32_t>(std::min<std::uint64_t>(CHUNK, initVal.totalSize - offset));
        auto cRes = pool.call<daemon::GetChunkRequest, daemon::GetChunkResponse>(c);
        if (!cRes) {
            daemon::GetEndRequest e{};
            e.transferId = initVal.transferId;
            (void)pool.call<daemon::GetEndRequest, daemon::SuccessResponse>(e);
            return cRes.error();
        }
        const auto& chunk = cRes.value();
        if (!chunk.data.empty()) {
            if (buffer.size() + chunk.data.size() <= MAX_BYTES) {
                buffer.append(chunk.data.data(), chunk.data.size());
            } else {
                auto remaining = MAX_BYTES - buffer.size();
                buffer.append(chunk.data.data(), remaining);
                truncated = true;
                offset += chunk.data.size();
                break;
            }
        }
        offset += chunk.data.size();
    }

    daemon::GetEndRequest end{};
    end.transferId = initVal.transferId;
    (void)pool.call<daemon::GetEndRequest, daemon::SuccessResponse>(end);

    mcp_response.content = std::move(buffer);
    // If truncated, we simply return the capped content; response type has no truncated flag.
    return mcp_response;
}

// Implementation of collection restore
Result<MCPRestoreCollectionResponse>
MCPServer::handleRestoreCollection(const MCPRestoreCollectionRequest& req) {
    try {
        if (!metadataRepo_) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        if (!store_) {
            return Error{ErrorCode::NotInitialized, "Content store not initialized"};
        }

        if (req.collection.empty()) {
            return Error{ErrorCode::InvalidArgument, "Collection name is required"};
        }

        spdlog::debug("MCP handleRestoreCollection: restoring collection '{}'", req.collection);

        // Get documents from collection
        auto docsResult = metadataRepo_->findDocumentsByCollection(req.collection);
        if (!docsResult) {
            return Error{ErrorCode::InternalError,
                         "Failed to find collection documents: " + docsResult.error().message};
        }

        const auto& documents = docsResult.value();
        if (documents.empty()) {
            MCPRestoreCollectionResponse response;
            response.filesRestored = 0;
            response.dryRun = req.dryRun;
            spdlog::info("MCP handleRestoreCollection: no documents found in collection '{}'",
                         req.collection);
            return response;
        }

        MCPRestoreCollectionResponse response;
        response.dryRun = req.dryRun;

        // Create output directory if needed
        std::filesystem::path outputDir(req.outputDirectory);
        if (!req.dryRun && req.createDirs) {
            std::error_code ec;
            std::filesystem::create_directories(outputDir, ec);
            if (ec) {
                return Error{ErrorCode::IOError,
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
                spdlog::info("MCP handleRestoreCollection: [DRY-RUN] would restore '{}' to '{}'",
                             doc.fileName, fullOutputPath.string());
            } else {
                // Retrieve content
                auto contentResult = store_->retrieveBytes(doc.sha256Hash);
                if (!contentResult) {
                    spdlog::error(
                        "MCP handleRestoreCollection: failed to retrieve content for '{}': {}",
                        doc.fileName, contentResult.error().message);
                    continue;
                }

                // Create parent directories
                std::error_code ec;
                std::filesystem::create_directories(fullOutputPath.parent_path(), ec);
                if (ec) {
                    spdlog::error(
                        "MCP handleRestoreCollection: failed to create directory for '{}': {}",
                        fullOutputPath.string(), ec.message());
                    continue;
                }

                // Write file
                std::ofstream outFile(fullOutputPath, std::ios::binary);
                if (!outFile) {
                    spdlog::error("MCP handleRestoreCollection: failed to open output file '{}'",
                                  fullOutputPath.string());
                    continue;
                }

                const auto& data = contentResult.value();
                outFile.write(reinterpret_cast<const char*>(data.data()), data.size());
                outFile.close();

                response.restoredPaths.push_back(fullOutputPath.string());
                response.filesRestored++;
                spdlog::info("MCP handleRestoreCollection: restored '{}' to '{}'", doc.fileName,
                             fullOutputPath.string());
            }
        }

        spdlog::info("MCP handleRestoreCollection: restored {} files from collection '{}'{}",
                     response.filesRestored, req.collection, req.dryRun ? " [DRY-RUN]" : "");

        return response;
    } catch (const std::exception& e) {
        spdlog::error("MCP handleRestoreCollection exception: {}", e.what());
        return Error{ErrorCode::InternalError,
                     std::string("Restore collection failed: ") + e.what()};
    }
}

Result<MCPRestoreSnapshotResponse>
MCPServer::handleRestoreSnapshot(const MCPRestoreSnapshotRequest& /*req*/) {
    return Error{ErrorCode::NotImplemented, "Restore snapshot not yet implemented"};
}

Result<MCPListCollectionsResponse>
MCPServer::handleListCollections(const MCPListCollectionsRequest& /*req*/) {
    MCPListCollectionsResponse response;
    // TODO: Implement collection listing
    return response;
}

Result<MCPListSnapshotsResponse>
MCPServer::handleListSnapshots(const MCPListSnapshotsRequest& /*req*/) {
    MCPListSnapshotsResponse response;
    // TODO: Implement snapshot listing
    return response;
}

} // namespace yams::mcp
