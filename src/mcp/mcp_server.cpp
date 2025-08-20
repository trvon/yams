#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <errno.h>
#include <iostream>
#include <memory>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/mcp/mcp_server.h>

// Platform-specific includes for non-blocking I/O
#ifdef _WIN32
#include <conio.h>
#include <windows.h>
#else
#include <poll.h>
#include <unistd.h>
#endif

namespace yams::mcp {

// StdioTransport implementation
StdioTransport::StdioTransport() {
    // Ensure unbuffered I/O for stdio communication
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
}

void StdioTransport::send(const json& message) {
    auto currentState = state_.load();
    if (currentState == TransportState::Connected) {
        const std::string payload = message.dump();
        std::cout << payload << "\n";
        std::cout.flush();
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
    while (state_.load() != TransportState::Closing) {
        // Check for input availability
        std::streamsize avail = std::cin.rdbuf() ? std::cin.rdbuf()->in_avail() : 0;

        // Check for EOF first
        if (std::cin.eof()) {
            state_.store(TransportState::Disconnected);
            return Error{ErrorCode::NetworkError, "End of file reached on stdin"};
        }

        if (isInputAvailable(100) || avail > 0) {
            std::string line;
            std::cin.clear();

            if (!std::getline(std::cin, line)) {
                if (std::cin.eof()) {
                    spdlog::debug("EOF on stdin, closing transport");
                    state_.store(TransportState::Disconnected);
                    return Error{ErrorCode::NetworkError, "End of file reached on stdin"};
                }
                // Clear error and retry if we should
                std::cin.clear();
                if (!shouldRetryAfterError()) {
                    state_.store(TransportState::Error);
                    return Error{ErrorCode::NetworkError, "Too many consecutive I/O errors"};
                }
                continue;
            }

            // Handle CRLF: strip trailing '\r' if present
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            // Skip empty lines but check for EOF after
            if (line.empty()) {
                // If we got an empty line and there's no more input, check for EOF
                if (std::cin.eof() || (!std::cin.rdbuf() || std::cin.rdbuf()->in_avail() == 0)) {
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
                if (!shouldRetryAfterError() || std::cin.eof()) {
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
MCPServer::MCPServer(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<search::SearchExecutor> searchExecutor,
                     std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                     std::shared_ptr<search::HybridSearchEngine> hybridEngine,
                     std::unique_ptr<ITransport> transport, std::atomic<bool>* externalShutdown)
    : store_(std::move(store)), searchExecutor_(std::move(searchExecutor)),
      metadataRepo_(std::move(metadataRepo)), hybridEngine_(std::move(hybridEngine)),
      transport_(std::move(transport)),
      appContext_{store_, searchExecutor_, metadataRepo_, hybridEngine_},
      externalShutdown_(externalShutdown) {
    // Set external shutdown flag on StdioTransport if applicable
    if (auto* stdioTransport = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdioTransport->setShutdownFlag(externalShutdown_);
    }

    // Initialize app services using cached context
    auto services = app::services::makeServices(appContext_);

    searchService_ = services.search;
    grepService_ = services.grep;
    documentService_ = services.document;
    downloadService_ = services.download;
    indexingService_ = services.indexing;
    statsService_ = services.stats;

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
    return {
        {"tools",
         json::array(
             {// Core document operations
              {{"name", "search_documents"},
               {"description",
                "Search for documents using keywords, fuzzy matching, or similarity"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"query",
                    {{"type", "string"},
                     {"description", "Search query (keywords, phrases, or hash)"}}},
                   {"limit",
                    {{"type", "integer"},
                     {"description", "Maximum number of results"},
                     {"default", 10}}},
                   {"fuzzy",
                    {{"type", "boolean"},
                     {"description", "Enable fuzzy matching"},
                     {"default", false}}},
                   {"similarity",
                    {{"type", "number"},
                     {"description", "Minimum similarity threshold (0-1)"},
                     {"default", 0.7}}},
                   {"hash",
                    {{"type", "string"},
                     {"description",
                      "Search by file hash (full or partial, minimum 8 characters)"}}},
                   {"verbose",
                    {{"type", "boolean"},
                     {"description", "Enable verbose output"},
                     {"default", false}}},
                   {"type",
                    {{"type", "string"},
                     {"description", "Search type: keyword, semantic, hybrid"},
                     {"default", "hybrid"}}},
                   {"paths_only",
                    {{"type", "boolean"},
                     {"description", "Return only file paths (LLM-friendly)"},
                     {"default", false}}},
                   {"line_numbers",
                    {{"type", "boolean"},
                     {"description", "Include line numbers in content"},
                     {"default", false}}},
                   {"after_context",
                    {{"type", "integer"},
                     {"description", "Lines of context after matches"},
                     {"default", 0}}},
                   {"before_context",
                    {{"type", "integer"},
                     {"description", "Lines of context before matches"},
                     {"default", 0}}},
                   {"context",
                    {{"type", "integer"},
                     {"description", "Lines of context around matches"},
                     {"default", 0}}},
                   {"color",
                    {{"type", "string"},
                     {"description",
                      "Color highlighting for matches (values: always, never, auto)"},
                     {"default", "auto"}}},
                   {"path_pattern",
                    {{"type", "string"},
                     {"description",
                      "Glob-like filename/path filter (e.g., **/*.md or substring)"}}},
                   {"path",
                    {{"type", "string"},
                     {"description", "Alias for path_pattern (substring or glob-like filter)"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Filter by tags (presence-based, matches any by default)"}}},
                   {"match_all_tags",
                    {{"type", "boolean"},
                     {"description", "Require all specified tags to be present"},
                     {"default", false}}}}},
                 {"required", {"query"}}}}},
              {{"name", "grep_documents"},
               {"description", "Search document contents using regular expressions"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"pattern", {{"type", "string"}, {"description", "Regular expression pattern"}}},
                   {"paths",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Specific paths to search (optional)"}}},
                   {"ignore_case",
                    {{"type", "boolean"},
                     {"description", "Case-insensitive search"},
                     {"default", false}}},
                   {"word",
                    {{"type", "boolean"},
                     {"description", "Match whole words only"},
                     {"default", false}}},
                   {"invert",
                    {{"type", "boolean"},
                     {"description", "Invert match (show non-matching lines)"},
                     {"default", false}}},
                   {"line_numbers",
                    {{"type", "boolean"},
                     {"description", "Show line numbers"},
                     {"default", false}}},
                   {"with_filename",
                    {{"type", "boolean"},
                     {"description", "Show filename with matches"},
                     {"default", true}}},
                   {"count",
                    {{"type", "boolean"},
                     {"description", "Count matches instead of showing them"},
                     {"default", false}}},
                   {"files_with_matches",
                    {{"type", "boolean"},
                     {"description", "Show only filenames with matches"},
                     {"default", false}}},
                   {"files_without_match",
                    {{"type", "boolean"},
                     {"description", "Show only filenames without matches"},
                     {"default", false}}},
                   {"after_context",
                    {{"type", "integer"}, {"description", "Lines after match"}, {"default", 0}}},
                   {"before_context",
                    {{"type", "integer"}, {"description", "Lines before match"}, {"default", 0}}},
                   {"context",
                    {{"type", "integer"}, {"description", "Lines around match"}, {"default", 0}}},
                   {"max_count",
                    {{"type", "integer"}, {"description", "Maximum matches per file"}}},
                   {"color",
                    {{"type", "string"},
                     {"description", "Color highlighting (values: always, never, auto)"},
                     {"default", "auto"}}}}},
                 {"required", {"pattern"}}}}},
              {{"name", "downloader.download"},
               {"description",
                "Robust downloader: store into CAS (store-only by default) with optional export"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"url", {{"type", "string"}, {"description", "Source URL"}}},
                   {"headers",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Custom headers, e.g., Authorization: Bearer <token>"}}},
                   {"checksum",
                    {{"type", "string"}, {"description", "Expected checksum '<algo>:<hex>'"}}},
                   {"concurrency",
                    {{"type", "integer"}, {"description", "Parallel connections"}, {"default", 4}}},
                   {"chunk_size_bytes",
                    {{"type", "integer"},
                     {"description", "Chunk size in bytes"},
                     {"default", 8388608}}},
                   {"timeout_ms",
                    {{"type", "integer"},
                     {"description", "Per-connection timeout (ms)"},
                     {"default", 60000}}},
                   {"retry",
                    {{"type", "object"},
                     {"properties",
                      {{"max_attempts", {{"type", "integer"}, {"default", 5}}},
                       {"backoff_ms", {{"type", "integer"}, {"default", 500}}},
                       {"backoff_multiplier", {{"type", "number"}, {"default", 2.0}}},
                       {"max_backoff_ms", {{"type", "integer"}, {"default", 15000}}}}}}},
                   {"rate_limit",
                    {{"type", "object"},
                     {"properties",
                      {{"global_bps", {{"type", "integer"}, {"default", 0}}},
                       {"per_conn_bps", {{"type", "integer"}, {"default", 0}}}}}}},
                   {"resume", {{"type", "boolean"}, {"default", true}}},
                   {"proxy", {{"type", "string"}}},
                   {"tls",
                    {{"type", "object"},
                     {"properties",
                      {{"insecure", {{"type", "boolean"}, {"default", false}}},
                       {"ca_path", {{"type", "string"}}}}}}},
                   {"follow_redirects", {{"type", "boolean"}, {"default", true}}},
                   {"store_only", {{"type", "boolean"}, {"default", true}}},
                   {"export_path", {{"type", "string"}, {"description", "Optional export path"}}},
                   {"overwrite",
                    {{"type", "string"},
                     {"description", "Overwrite policy: never|if-different-etag|always"},
                     {"default", "never"}}}}},
                 {"required", json::array({"url"})}}}},
              {{"name", "store_document"},
               {"description", "Store a document in YAMS"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"path", {{"type", "string"}, {"description", "File path to store"}}},
                   {"content", {{"type", "string"}, {"description", "Document content"}}},
                   {"name", {{"type", "string"}, {"description", "Document name/filename"}}},
                   {"mime_type", {{"type", "string"}, {"description", "MIME type of the content"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Tags for the document"}}},
                   {"metadata",
                    {{"type", "object"}, {"description", "Additional metadata key-value pairs"}}}}},
                 {"required", json::array()}}}}, // Note: either path OR (content+name) required
              {{"name", "retrieve_document"},
               {"description", "Retrieve a document by hash or name"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"hash", {{"type", "string"}, {"description", "Document SHA-256 hash"}}},
                   {"name", {{"type", "string"}, {"description", "Document name"}}},
                   {"graph",
                    {{"type", "boolean"},
                     {"description", "Include knowledge graph relationships"},
                     {"default", false}}},
                   {"depth",
                    {{"type", "integer"},
                     {"description", "Graph traversal depth (1-5)"},
                     {"default", 1},
                     {"minimum", 1},
                     {"maximum", 5}}},
                   {"include_content",
                    {{"type", "boolean"},
                     {"description", "Include full content in graph results"},
                     {"default", false}}}}}}}},
              {{"name", "delete_document"},
               {"description", "Delete a document by hash or name"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"hash", {{"type", "string"}, {"description", "Document SHA-256 hash"}}},
                   {"name", {{"type", "string"}, {"description", "Document name"}}}}}}}},
              {{"name", "update_metadata"},
               {"description", "Update document metadata"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"hash", {{"type", "string"}, {"description", "Document SHA-256 hash"}}},
                   {"name",
                    {{"type", "string"}, {"description", "Document name (alternative to hash)"}}},
                   {"metadata",
                    {{"type", "object"}, {"description", "Metadata key-value pairs to update"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Tags to add or update"}}}}}}}},

              // List and filter operations
              {{"name", "list_documents"},
               {"description", "List documents with optional filtering"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"limit",
                    {{"type", "integer"},
                     {"description", "Maximum number of results"},
                     {"default", 100}}},
                   {"offset",
                    {{"type", "integer"},
                     {"description", "Offset for pagination"},
                     {"default", 0}}},
                   {"pattern",
                    {{"type", "string"}, {"description", "Glob pattern for filtering names"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Filter by tags"}}},
                   {"type", {{"type", "string"}, {"description", "Filter by file type category"}}},
                   {"mime", {{"type", "string"}, {"description", "Filter by MIME type pattern"}}},
                   {"extension", {{"type", "string"}, {"description", "Filter by file extension"}}},
                   {"binary", {{"type", "boolean"}, {"description", "Filter binary files"}}},
                   {"text", {{"type", "boolean"}, {"description", "Filter text files"}}},
                   {"created_after",
                    {{"type", "string"}, {"description", "ISO 8601 timestamp or relative time"}}},
                   {"created_before",
                    {{"type", "string"}, {"description", "ISO 8601 timestamp or relative time"}}},
                   {"modified_after",
                    {{"type", "string"}, {"description", "ISO 8601 timestamp or relative time"}}},
                   {"modified_before",
                    {{"type", "string"}, {"description", "ISO 8601 timestamp or relative time"}}},
                   {"indexed_after",
                    {{"type", "string"}, {"description", "ISO 8601 timestamp or relative time"}}},
                   {"indexed_before",
                    {{"type", "string"}, {"description", "ISO 8601 timestamp or relative time"}}},
                   {"recent",
                    {{"type", "integer"}, {"description", "Get N most recent documents"}}},
                   {"sort_by",
                    {{"type", "string"},
                     {"description", "Sort field (values: name, size, created, modified, indexed)"},
                     {"default", "indexed"}}},
                   {"sort_order",
                    {{"type", "string"},
                     {"description", "Sort order (values: asc, desc)"},
                     {"default", "desc"}}}}}}}},

              // Statistics and maintenance
              {{"name", "get_stats"},
               {"description", "Get storage statistics and health status"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"detailed", {{"type", "boolean"}, {"default", false}}},
                   {"file_types",
                    {{"type", "boolean"},
                     {"description", "Include file type breakdown"},
                     {"default", false}}}}}}}},

              // CLI parity tools from v0.0.2
              {{"name", "delete_by_name"},
               {"description", "Delete documents by name with pattern support"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Document name"}}},
                   {"names",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Multiple document names"}}},
                   {"pattern",
                    {{"type", "string"}, {"description", "Glob pattern for matching names"}}},
                   {"dry_run",
                    {{"type", "boolean"},
                     {"description", "Preview what would be deleted"},
                     {"default", false}}}}}}}},
              {{"name", "get_by_name"},
               {"description", "Retrieve document content by name"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties", {{"name", {{"type", "string"}, {"description", "Document name"}}}}},
                 {"required", json::array({"name"})}}}},
              {{"name", "cat_document"},
               {"description", "Display document content (like cat command)"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"hash", {{"type", "string"}, {"description", "Document SHA-256 hash"}}},
                   {"name", {{"type", "string"}, {"description", "Document name"}}}}}}}},

              // Directory operations from v0.0.4
              {{"name", "add_directory"},
               {"description", "Add all files from a directory"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"directory_path", {{"type", "string"}, {"description", "Directory path"}}},
                   {"recursive",
                    {{"type", "boolean"},
                     {"description", "Recursively add subdirectories"},
                     {"default", false}}},
                   {"collection",
                    {{"type", "string"}, {"description", "Collection name for grouping"}}},
                   {"snapshot_id",
                    {{"type", "string"}, {"description", "Snapshot ID for versioning"}}},
                   {"snapshot_label",
                    {{"type", "string"}, {"description", "Human-readable snapshot label"}}},
                   {"include_patterns",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Include patterns (e.g., *.txt)"}}},
                   {"exclude_patterns",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Exclude patterns"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Tags to add to each stored document"}}},
                   {"metadata",
                    {{"type", "object"},
                     {"description",
                      "Additional metadata key-value pairs applied to each document"}}}}},
                 {"required", json::array({"directory_path"})}}}},
              {{"name", "restore_collection"},
               {"description", "Restore all documents from a collection"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"collection", {{"type", "string"}, {"description", "Collection name"}}},
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
                     {"description", "Overwrite files if they already exist"},
                     {"default", false}}},
                   {"create_dirs",
                    {{"type", "boolean"},
                     {"description", "Create parent directories if needed"},
                     {"default", true}}},
                   {"dry_run",
                    {{"type", "boolean"},
                     {"description", "Show what would be restored without writing files"},
                     {"default", false}}}}},
                 {"required", json::array({"collection", "output_directory"})}}}},
              {{"name", "restore_snapshot"},
               {"description", "Restore all documents from a snapshot"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID"}}},
                   {"snapshot_label",
                    {{"type", "string"},
                     {"description", "Snapshot label (alternative to snapshot_id)"}}},
                   {"output_directory", {{"type", "string"}, {"description", "Output directory"}}},
                   {"layout_template",
                    {{"type", "string"},
                     {"description", "Layout template"},
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
                     {"description", "Overwrite files if they already exist"},
                     {"default", false}}},
                   {"create_dirs",
                    {{"type", "boolean"},
                     {"description", "Create parent directories if needed"},
                     {"default", true}}},
                   {"dry_run",
                    {{"type", "boolean"},
                     {"description", "Show what would be restored without writing files"},
                     {"default", false}}}}},
                 {"required", json::array({"snapshot_id", "output_directory"})}}}},
              {{"name", "list_collections"},
               {"description", "List available collections"},
               {"inputSchema", {{"type", "object"}}}},
              {{"name", "list_snapshots"},
               {"description", "List available snapshots"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"collection", {{"type", "string"}, {"description", "Filter by collection"}}},
                   {"with_labels",
                    {{"type", "boolean"},
                     {"description", "Include snapshot labels"},
                     {"default", true}}}}}}}}})}};
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

    return toolRegistry_->callTool(name, arguments);
}

// Modern C++20 tool handler implementations
Result<MCPSearchResponse> MCPServer::handleSearchDocuments(const MCPSearchRequest& req) {
    try {
        if (!searchService_) {
            return Error{ErrorCode::NotInitialized, "Search service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::SearchRequest searchReq;
        searchReq.query = req.query;
        searchReq.limit = req.limit;
        searchReq.fuzzy = req.fuzzy;
        searchReq.similarity = req.similarity;
        searchReq.hash = req.hash;
        searchReq.type = req.type;
        searchReq.verbose = req.verbose;
        searchReq.pathsOnly = req.pathsOnly;
        searchReq.lineNumbers = req.lineNumbers;
        searchReq.beforeContext = req.beforeContext;
        searchReq.afterContext = req.afterContext;
        searchReq.context = req.context;
        searchReq.colorMode = req.colorMode;
        searchReq.pathPattern = req.pathPattern;
        searchReq.tags = req.tags;
        searchReq.matchAllTags = req.matchAllTags;

        auto result = searchService_->search(searchReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& searchRes = result.value();

        // Convert app services response to MCP response
        MCPSearchResponse response;
        response.total = searchRes.total;
        response.type = searchRes.type;
        response.executionTimeMs = searchRes.executionTimeMs;

        if (req.pathsOnly) {
            // Convert search results to paths
            for (const auto& item : searchRes.results) {
                response.paths.push_back(item.path);
            }
        } else {
            response.results.reserve(searchRes.results.size());
            for (const auto& item : searchRes.results) {
                MCPSearchResponse::Result mcpResult;
                mcpResult.id = std::to_string(item.id);
                mcpResult.hash = item.hash;
                mcpResult.title = item.title;
                mcpResult.path = item.path;
                mcpResult.score = static_cast<float>(item.score);
                mcpResult.snippet = item.snippet;
                mcpResult.vectorScore =
                    item.vectorScore ? std::optional<float>(static_cast<float>(*item.vectorScore))
                                     : std::nullopt;
                mcpResult.keywordScore =
                    item.keywordScore ? std::optional<float>(static_cast<float>(*item.keywordScore))
                                      : std::nullopt;
                mcpResult.kgEntityScore =
                    item.kgEntityScore
                        ? std::optional<float>(static_cast<float>(*item.kgEntityScore))
                        : std::nullopt;
                mcpResult.structuralScore =
                    item.structuralScore
                        ? std::optional<float>(static_cast<float>(*item.structuralScore))
                        : std::nullopt;
                response.results.push_back(std::move(mcpResult));
            }
        }

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Search failed: ") + e.what()};
    }
}

Result<MCPGrepResponse> MCPServer::handleGrepDocuments(const MCPGrepRequest& req) {
    try {
        if (!grepService_) {
            return Error{ErrorCode::NotInitialized, "Grep service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::GrepRequest grepReq;
        grepReq.pattern = req.pattern;
        grepReq.paths = req.paths;
        grepReq.ignoreCase = req.ignoreCase;
        grepReq.word = req.word;
        grepReq.invert = req.invert;
        grepReq.lineNumbers = req.lineNumbers;
        grepReq.withFilename = req.withFilename;
        grepReq.count = req.count;
        grepReq.filesWithMatches = req.filesWithMatches;
        grepReq.filesWithoutMatch = req.filesWithoutMatch;
        grepReq.afterContext = req.afterContext;
        grepReq.beforeContext = req.beforeContext;
        grepReq.context = req.context;
        if (req.maxCount) {
            grepReq.maxCount = *req.maxCount;
        }
        grepReq.colorMode = req.color;

        auto result = grepService_->grep(grepReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& grepRes = result.value();

        // Convert app services response to MCP response
        MCPGrepResponse response;
        // TODO: Format the structured grep results into output string
        // For now, just provide basic aggregated info
        response.output = "Grep completed";
        response.matchCount = grepRes.totalMatches;
        response.fileCount = grepRes.filesWith.size();

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Grep failed: ") + e.what()};
    }
}

Result<MCPDownloadResponse> MCPServer::handleDownload(const MCPDownloadRequest& req) {
    try {
        if (!downloadService_) {
            return Error{ErrorCode::NotInitialized, "Download service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::DownloadServiceRequest downloadReq;
        downloadReq.url = req.url;

        // Convert string headers to proper Header objects
        for (const auto& headerStr : req.headers) {
            auto pos = headerStr.find(':');
            if (pos != std::string::npos) {
                downloader::Header h;
                h.name = headerStr.substr(0, pos);
                std::string val = headerStr.substr(pos + 1);
                if (!val.empty() && val.front() == ' ')
                    val.erase(0, 1);
                h.value = val;
                downloadReq.headers.push_back(std::move(h));
            }
        }

        // Parse checksum if provided
        if (!req.checksum.empty()) {
            auto pos = req.checksum.find(':');
            if (pos != std::string::npos) {
                downloader::Checksum cs;
                auto algo = req.checksum.substr(0, pos);
                auto hex = req.checksum.substr(pos + 1);
                std::transform(algo.begin(), algo.end(), algo.begin(), ::tolower);
                if (algo == "sha256")
                    cs.algo = downloader::HashAlgo::Sha256;
                else if (algo == "sha512")
                    cs.algo = downloader::HashAlgo::Sha512;
                else if (algo == "md5")
                    cs.algo = downloader::HashAlgo::Md5;
                cs.hex = hex;
                downloadReq.checksum = cs;
            }
        }

        downloadReq.concurrency = req.concurrency;
        downloadReq.chunkSizeBytes = req.chunkSizeBytes;
        downloadReq.timeout = std::chrono::milliseconds(req.timeoutMs);
        downloadReq.resume = req.resume;
        downloadReq.followRedirects = req.followRedirects;
        downloadReq.storeOnly = req.storeOnly;

        if (!req.exportPath.empty()) {
            downloadReq.exportPath = req.exportPath;
        }

        if (req.overwrite == "always") {
            downloadReq.overwrite = downloader::OverwritePolicy::Always;
        } else if (req.overwrite == "if-different-etag") {
            downloadReq.overwrite = downloader::OverwritePolicy::IfDifferentEtag;
        } else {
            downloadReq.overwrite = downloader::OverwritePolicy::Never;
        }

        auto result = downloadService_->download(downloadReq);
        if (!result) {
            spdlog::error("MCP handleDownload failed: {}", result.error().message);
            return Error{ErrorCode::NetworkError, result.error().message};
        }

        const auto& downloadRes = result.value();

        // Convert app services response to MCP response
        MCPDownloadResponse response;
        response.url = downloadRes.url;
        response.hash = downloadRes.hash;
        response.storedPath = downloadRes.storedPath.string();
        response.sizeBytes = downloadRes.sizeBytes;
        response.success = downloadRes.success;
        response.httpStatus = downloadRes.httpStatus;
        response.etag = downloadRes.etag;
        response.lastModified = downloadRes.lastModified;
        response.checksumOk = downloadRes.checksumOk;

        spdlog::debug("MCP handleDownload returning response: url={}, hash={}, size={}",
                      response.url, response.hash, response.sizeBytes);

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Download failed: ") + e.what()};
    }
}

Result<MCPStoreDocumentResponse>
MCPServer::handleStoreDocument(const MCPStoreDocumentRequest& req) {
    try {
        if (!documentService_) {
            return Error{ErrorCode::NotInitialized, "Document service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::StoreDocumentRequest storeReq;
        storeReq.path = req.path;
        storeReq.content = req.content;
        storeReq.name = req.name;
        storeReq.mimeType = req.mimeType;
        storeReq.tags = req.tags;

        // Convert JSON metadata to string map
        if (!req.metadata.empty()) {
            for (const auto& [key, value] : req.metadata.items()) {
                if (value.is_string()) {
                    storeReq.metadata[key] = value.get<std::string>();
                } else {
                    storeReq.metadata[key] = value.dump();
                }
            }
        }

        auto result = documentService_->store(storeReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& storeRes = result.value();

        // Convert app services response to MCP response
        MCPStoreDocumentResponse response;
        response.hash = storeRes.hash;
        response.bytesStored = storeRes.bytesStored;
        response.bytesDeduped = storeRes.bytesDeduped;

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Store document failed: ") + e.what()};
    }
}

Result<MCPRetrieveDocumentResponse>
MCPServer::handleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
    try {
        if (!documentService_) {
            return Error{ErrorCode::NotInitialized, "Document service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::RetrieveDocumentRequest retrieveReq;
        retrieveReq.hash = req.hash;
        retrieveReq.outputPath = req.outputPath;
        retrieveReq.graph = req.graph;
        retrieveReq.depth = req.depth;
        retrieveReq.includeContent = req.includeContent;

        auto result = documentService_->retrieve(retrieveReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& retrieveRes = result.value();

        // Convert app services response to MCP response
        MCPRetrieveDocumentResponse response;
        response.graphEnabled = retrieveRes.graphEnabled;

        if (retrieveRes.document) {
            const auto& doc = *retrieveRes.document;
            response.hash = doc.hash;
            response.path = doc.path;
            response.name = doc.name;
            response.size = doc.size;
            response.mimeType = doc.mimeType;
            response.content = doc.content;
        }

        // Convert related documents to JSON
        for (const auto& related : retrieveRes.related) {
            json relatedJson = {
                {"hash", related.hash}, {"path", related.path}, {"distance", related.distance}};
            if (related.relationship) {
                relatedJson["relationship"] = *related.relationship;
            }
            response.related.push_back(std::move(relatedJson));
        }

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Retrieve document failed: ") + e.what()};
    }
}

Result<MCPListDocumentsResponse>
MCPServer::handleListDocuments(const MCPListDocumentsRequest& req) {
    try {
        if (!documentService_) {
            return Error{ErrorCode::NotInitialized, "Document service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::ListDocumentsRequest listReq;
        listReq.pattern = req.pattern;
        listReq.tags = req.tags;
        listReq.type = req.type;
        listReq.mime = req.mime;
        listReq.extension = req.extension;
        listReq.binary = req.binary;
        listReq.text = req.text;
        listReq.recent = req.recent;
        listReq.sortBy = req.sortBy;
        listReq.sortOrder = req.sortOrder;

        auto result = documentService_->list(listReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& listRes = result.value();

        // Convert app services response to MCP response
        MCPListDocumentsResponse response;
        response.total = listRes.totalFound;

        // Convert documents to JSON
        for (const auto& doc : listRes.documents) {
            json docJson = {{"hash", doc.hash},          {"path", doc.path},
                            {"name", doc.name},          {"size", doc.size},
                            {"mime_type", doc.mimeType}, {"created", doc.created},
                            {"modified", doc.modified},  {"indexed", doc.indexed}};
            if (!doc.tags.empty()) {
                docJson["tags"] = doc.tags;
            }
            // Note: DocumentEntry doesn't have metadata field, skipping
            response.documents.push_back(std::move(docJson));
        }

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("List documents failed: ") + e.what()};
    }
}

Result<MCPStatsResponse> MCPServer::handleGetStats(const MCPStatsRequest& req) {
    try {
        if (!statsService_) {
            return Error{ErrorCode::NotInitialized, "Stats service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::StatsRequest statsReq;
        statsReq.fileTypes = req.fileTypes;
        statsReq.verbose = req.verbose;

        auto result = statsService_->getStats(statsReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& statsRes = result.value();

        // Convert app services response to MCP response
        MCPStatsResponse response;
        response.totalObjects = statsRes.totalObjects;
        response.totalBytes = statsRes.totalBytes;
        response.uniqueHashes = statsRes.uniqueHashes;
        response.deduplicationSavings = statsRes.deduplicationSavings;

        // Convert file type stats to JSON
        for (const auto& ft : statsRes.fileTypes) {
            json ftJson = {
                {"extension", ft.extension}, {"count", ft.count}, {"total_bytes", ft.totalBytes}};
            response.fileTypes.push_back(std::move(ftJson));
        }

        // Convert additional stats
        if (!statsRes.additionalStats.empty()) {
            for (const auto& [key, value] : statsRes.additionalStats) {
                response.additionalStats[key] = value;
            }
        }

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Get stats failed: ") + e.what()};
    }
}

Result<MCPAddDirectoryResponse> MCPServer::handleAddDirectory(const MCPAddDirectoryRequest& req) {
    try {
        if (!indexingService_) {
            return Error{ErrorCode::NotInitialized, "Indexing service not initialized"};
        }

        // Convert MCP request to app services request
        app::services::AddDirectoryRequest addReq;
        addReq.directoryPath = req.directoryPath;
        addReq.collection = req.collection;
        addReq.includePatterns = req.includePatterns;
        addReq.excludePatterns = req.excludePatterns;
        addReq.recursive = req.recursive;
        addReq.followSymlinks = req.followSymlinks;

        // Convert JSON metadata to string map
        if (!req.metadata.empty()) {
            for (const auto& [key, value] : req.metadata.items()) {
                if (value.is_string()) {
                    addReq.metadata[key] = value.get<std::string>();
                } else {
                    addReq.metadata[key] = value.dump();
                }
            }
        }

        auto result = indexingService_->addDirectory(addReq);
        if (!result) {
            return Error{ErrorCode::InternalError, result.error().message};
        }

        const auto& addRes = result.value();

        // Convert app services response to MCP response
        MCPAddDirectoryResponse response;
        response.directoryPath = addRes.directoryPath;
        response.collection = addRes.collection;
        response.filesProcessed = addRes.filesProcessed;
        response.filesIndexed = addRes.filesIndexed;
        response.filesSkipped = addRes.filesSkipped;
        response.filesFailed = addRes.filesFailed;

        // Convert file results to JSON
        for (const auto& fileResult : addRes.results) {
            json resultJson = {{"path", fileResult.path},
                               {"hash", fileResult.hash},
                               {"size_bytes", fileResult.sizeBytes},
                               {"success", fileResult.success}};
            if (fileResult.error) {
                resultJson["error"] = *fileResult.error;
            }
            response.results.push_back(std::move(resultJson));
        }

        return response;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Add directory failed: ") + e.what()};
    }
}

void MCPServer::initializeToolRegistry() {
    toolRegistry_ = std::make_unique<ToolRegistry>();

    // Register modern type-safe handlers with descriptions
    toolRegistry_->registerTool<MCPSearchRequest, MCPSearchResponse>(
        "search_documents",
        [this](const MCPSearchRequest& req) { return handleSearchDocuments(req); },
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
        "grep_documents", [this](const MCPGrepRequest& req) { return handleGrepDocuments(req); },
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

    // Register download tool
    toolRegistry_->registerTool<MCPDownloadRequest, MCPDownloadResponse>(
        "downloader.download",
        [this](const MCPDownloadRequest& req) { return handleDownload(req); },
        json{{"type", "object"},
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
                 {"default", 4}}}}},
             {"required", json::array({"url"})}},
        "Download files from URLs and store them in YAMS content-addressed storage");

    // Register document management tools
    toolRegistry_->registerTool<MCPStoreDocumentRequest, MCPStoreDocumentResponse>(
        "store_document",
        [this](const MCPStoreDocumentRequest& req) { return handleStoreDocument(req); },
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
        "retrieve_document",
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
        "list_documents",
        [this](const MCPListDocumentsRequest& req) { return handleListDocuments(req); },
        json{
            {"type", "object"},
            {"properties",
             {{"pattern", {{"type", "string"}, {"description", "Name pattern filter"}}},
              {"tags",
               {{"type", "array"},
                {"items", {{"type", "string"}}},
                {"description", "Filter by tags"}}},
              {"recent", {{"type", "integer"}, {"description", "Show N most recent documents"}}}}}},
        "List documents with filtering by pattern, tags, type, or recency");

    // Register stats tool
    toolRegistry_->registerTool<MCPStatsRequest, MCPStatsResponse>(
        "get_stats", [this](const MCPStatsRequest& req) { return handleGetStats(req); },
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

    // Register indexing tool
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
}

json MCPServer::createResponse(const json& id, const json& result) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
}

json MCPServer::createError(const json& id, int code, const std::string& message) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}};
}

} // namespace yams::mcp
