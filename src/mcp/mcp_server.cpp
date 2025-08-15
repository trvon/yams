#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstring>
#include <errno.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <regex>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
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
    if (!closed_) {
        const std::string payload = message.dump();
        std::ostringstream oss;
        oss << "Content-Length: " << payload.size() << "\r\n"
            << "Content-Type: application/json\r\n"
            << "\r\n"
            << payload;
        std::cout << oss.str();
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

json StdioTransport::receive() {
    if (closed_) {
        return json{};
    }

    // Framed stdio protocol:
    //   Content-Length: N\r\n
    //   [other headers...]\r\n
    //   \r\n
    //   <N bytes JSON>
    while (!closed_) {
        // Also handle non-TTY test input where poll() may not reflect stringstream buffers
        std::streamsize avail = std::cin.rdbuf() ? std::cin.rdbuf()->in_avail() : 0;
        if (isInputAvailable(100) || avail > 0) {
            // Parse headers
            size_t contentLength = 0;

            for (;;) {
                std::string line;
                std::cin.clear();
                if (!std::getline(std::cin, line)) {
                    if (std::cin.eof()) {
                        spdlog::debug("EOF on stdin, closing transport");
                        closed_ = true;
                        break;
                    }
                    // Clear error and retry
                    std::cin.clear();
                    continue;
                }

                // Handle CRLF: strip trailing '\r' if present
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                // If the first non-space character looks like a JSON body, parse unframed
                {
                    auto first = line.find_first_not_of(" \t");
                    if (first != std::string::npos && (line[first] == '{' || line[first] == '[')) {
                        try {
                            return json::parse(line.substr(first));
                        } catch (const json::parse_error& e) {
                            spdlog::error("Failed to parse unframed JSON (first-line): {}",
                                          e.what());
                        }
                    }
                }

                // Blank line separates headers from body
                if (line.empty()) {
                    break;
                }

                // Parse Content-Length header (case-insensitive prefix match)
                const std::string cl1 = "Content-Length:";
                const std::string cl2 = "Content-length:";
                if (line.rfind(cl1, 0) == 0 || line.rfind(cl2, 0) == 0) {
                    const auto colon = line.find(':');
                    std::string value = (colon == std::string::npos) ? "" : line.substr(colon + 1);
                    // trim leading spaces/tabs
                    value.erase(0, value.find_first_not_of(" \t"));
                    try {
                        contentLength = static_cast<size_t>(std::stoull(value));
                    } catch (...) {
                        contentLength = 0;
                    }
                }
            }

            if (closed_) {
                break;
            }

            if (contentLength == 0) {
                // Back-compat: accept a single-line JSON if headers were missing
                std::string fallback;
                if (std::getline(std::cin, fallback)) {
                    if (!fallback.empty() && fallback.back() == '\r') {
                        fallback.pop_back();
                    }
                    if (!fallback.empty()) {
                        try {
                            return json::parse(fallback);
                        } catch (const json::parse_error& e) {
                            spdlog::error("Failed to parse unframed JSON: {}", e.what());
                        }
                    }
                } else if (std::cin.eof()) {
                    spdlog::debug("EOF on stdin, closing transport");
                    closed_ = true;
                    break;
                } else {
                    std::cin.clear();
                }
                continue;
            }

            // Read exactly contentLength bytes
            std::string body(contentLength, '\0');
            std::cin.read(&body[0], static_cast<std::streamsize>(contentLength));
            if (std::cin.gcount() < static_cast<std::streamsize>(contentLength)) {
                if (std::cin.eof()) {
                    spdlog::debug("EOF while reading body, closing transport");
                    closed_ = true;
                    break;
                }
                // Read error; clear and retry loop
                std::cin.clear();
                continue;
            }

            try {
                return json::parse(body);
            } catch (const json::parse_error& e) {
                spdlog::error("Failed to parse JSON body: {}", e.what());
                // Loop and wait for next message
            }
        }

        // Check if external shutdown was requested
        if (externalShutdown_ && *externalShutdown_) {
            spdlog::debug("External shutdown requested, closing transport");
            closed_ = true;
            break;
        }
    }

    return json{};
}

// MCPServer implementation
MCPServer::MCPServer(std::shared_ptr<api::IContentStore> store,
                     std::shared_ptr<search::SearchExecutor> searchExecutor,
                     std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                     std::shared_ptr<search::HybridSearchEngine> hybridEngine,
                     std::unique_ptr<ITransport> transport, std::atomic<bool>* externalShutdown)
    : store_(std::move(store)), searchExecutor_(std::move(searchExecutor)),
      metadataRepo_(std::move(metadataRepo)), hybridEngine_(std::move(hybridEngine)),
      transport_(std::move(transport)), externalShutdown_(externalShutdown) {
    // Set external shutdown flag on StdioTransport if applicable
    if (auto* stdioTransport = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdioTransport->setShutdownFlag(externalShutdown_);
    }
}

MCPServer::~MCPServer() {
    stop();
}

void MCPServer::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }

    spdlog::info("MCP server started");

    // Main message loop
    while (running_ && (!externalShutdown_ || !*externalShutdown_)) {
        try {
            auto message = transport_->receive();

            if (message.is_null() || message.empty()) {
                // Check if transport is still connected
                if (!transport_->isConnected()) {
                    spdlog::info("Transport disconnected, stopping server");
                    break;
                }
                continue;
            }

            auto response = handleRequest(message);
            if (!response.is_null()) {
                transport_->send(response);
            }

        } catch (const std::exception& e) {
            spdlog::error("Error in main loop: {}", e.what());

            // Send error response
            json errorResponse = {
                {"jsonrpc", "2.0"},
                {"error",
                 {{"code", -32603}, {"message", std::string("Internal error: ") + e.what()}}},
                {"id", nullptr}};

            try {
                transport_->send(errorResponse);
            } catch (...) {
                // Ignore send errors
            }
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

json MCPServer::handleRequest(const json& request) {
    const bool hasId = request.contains("id");
    json id = hasId ? request["id"] : json(nullptr);
    try {
        // Validate JSON-RPC request
        if (!request.contains("jsonrpc") || request["jsonrpc"] != "2.0") {
            if (!hasId)
                return json(nullptr);
            return {{"jsonrpc", "2.0"},
                    {"error",
                     {{"code", -32600},
                      {"message", "Invalid Request: Missing or invalid jsonrpc field"}}},
                    {"id", id}};
        }

        if (!request.contains("method")) {
            if (!hasId)
                return json(nullptr);
            return {
                {"jsonrpc", "2.0"},
                {"error", {{"code", -32600}, {"message", "Invalid Request: Missing method field"}}},
                {"id", id}};
        }

        std::string method = request["method"];
        json params = request.value("params", json::object());

        json result;

        // Notifications (no "id") must not receive responses. Handle known ones and ignore others.
        if (!hasId) {
            if (method == "initialized" || method == "notifications/initialized") {
                initialized_ = true;
                try {
                    json ready = {{"jsonrpc", "2.0"},
                                  {"method", "notifications/ready"},
                                  {"params", {{"protocolVersion", negotiatedProtocolVersion_}}}};
                    transport_->send(ready);
                } catch (...) {
                    // ignore notification send errors
                }
            } else if (method == "logging/setLevel" || method == "notifications/logging/setLevel") {
                // Optionally adjust log level here; intentionally no response for notifications
            }
            return json(nullptr);
        }

        // Route to appropriate handler
        if (method == "initialize") {
            result = initialize(params);
        } else if (method == "initialized") {
            // Client notification that initialization is complete
            initialized_ = true;
            // Send ready notification for legacy 'initialized'
            try {
                json ready = {{"jsonrpc", "2.0"},
                              {"method", "notifications/ready"},
                              {"params", {{"protocolVersion", negotiatedProtocolVersion_}}}};
                transport_->send(ready);
            } catch (...) {
                // ignore notification send errors
            }
            if (hasId) {
                return {{"jsonrpc", "2.0"}, {"result", json::object()}, {"id", id}};
            }
            return json(nullptr);
        } else if (method == "notifications/initialized") {
            // Namespaced notification; mark initialized and send ready
            initialized_ = true;
            try {
                json ready = {{"jsonrpc", "2.0"},
                              {"method", "notifications/ready"},
                              {"params", {{"protocolVersion", negotiatedProtocolVersion_}}}};
                transport_->send(ready);
            } catch (...) {
                // ignore notification send errors
            }
            if (hasId) {
                return {{"jsonrpc", "2.0"}, {"result", json::object()}, {"id", id}};
            }
            return json(nullptr);
        } else if (method == "resources/list") {
            result = listResources();
        } else if (method == "resources/read") {
            if (!params.contains("uri")) {
                throw std::runtime_error("Missing required parameter: uri");
            }
            result = readResource(params["uri"]);
        } else if (method == "tools/list") {
            result = listTools();
        } else if (method == "tools/call") {
            if (!params.contains("name") || !params.contains("arguments")) {
                throw std::runtime_error("Missing required parameters: name, arguments");
            }
            result = callTool(params["name"], params["arguments"]);
        } else if (method == "prompts/list") {
            result = listPrompts();
        } else if (method == "prompts/get") {
            if (!params.contains("name")) {
                throw std::runtime_error("Missing required parameter: name");
            }
            // Prompts not implemented yet
            return {{"jsonrpc", "2.0"},
                    {"id", id},
                    {"error", {{"code", -32601}, {"message", "Prompts not implemented"}}}};
        } else if (method == "completion/complete") {
            // Completion not implemented yet
            return {{"jsonrpc", "2.0"},
                    {"id", id},
                    {"error", {{"code", -32601}, {"message", "Completion not implemented"}}}};
        } else if (method == "ping" || method == "utilities/ping") {
            // Allow clients to ping before or after initialization
            result = json::object();
        } else if (method == "logging/setLevel") {
            // Logging level change not implemented
            result = json::object();
        } else if (method == "shutdown" || method == "exit") {
            running_ = false;
            result = json::object();
        } else {
            if (!hasId)
                return json(nullptr);
            return {{"jsonrpc", "2.0"},
                    {"error", {{"code", -32601}, {"message", "Method not found: " + method}}},
                    {"id", id}};
        }

        // Return successful response
        return {{"jsonrpc", "2.0"}, {"result", result}, {"id", id}};

    } catch (const std::exception& e) {
        if (!hasId)
            return json(nullptr);
        return {
            {"jsonrpc", "2.0"},
            {"error", {{"code", -32603}, {"message", std::string("Internal error: ") + e.what()}}},
            {"id", id}};
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
                     {"enum", {"always", "never", "auto"}},
                     {"description", "Color highlighting for matches"},
                     {"default", "auto"}}}}},
                 {"required", {"query"}}}}},
              {{"name", "grep_documents"},
               {"description", "Search document contents using regular expressions"},
               {"inputSchema",
                {{"type", "object"},
                 {"properties",
                  {{"pattern", {{"type", "string"}, {"description", "Regular expression pattern"}}},
                   {"paths",
                    {{"type", "array"},
                     {"items", {"type", "string"}},
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
                     {"enum", {"always", "never", "auto"}},
                     {"description", "Color highlighting"},
                     {"default", "auto"}}}}},
                 {"required", {"pattern"}}}}},
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
                     {"items", {"type", "string"}},
                     {"description", "Tags for the document"}}},
                   {"metadata",
                    {{"type", "object"}, {"description", "Additional metadata key-value pairs"}}}}},
                 {"oneOf", json::array({{{"required", json::array({"path"})}},
                                        {{"required", json::array({"content", "name"})}}})}}}},
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
                     {"items", {"type", "string"}},
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
                     {"items", {"type", "string"}},
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
                     {"enum", {"name", "size", "created", "modified", "indexed"}},
                     {"description", "Sort field"},
                     {"default", "indexed"}}},
                   {"sort_order",
                    {{"type", "string"},
                     {"enum", {"asc", "desc"}},
                     {"description", "Sort order"},
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
                     {"items", {"type", "string"}},
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
                 {"required", {"name"}}}}},
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
                     {"items", {"type", "string"}},
                     {"description", "Include patterns (e.g., *.txt)"}}},
                   {"exclude_patterns",
                    {{"type", "array"},
                     {"items", {"type", "string"}},
                     {"description", "Exclude patterns"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {"type", "string"}},
                     {"description", "Tags to add to each stored document"}}},
                   {"metadata",
                    {{"type", "object"},
                     {"description",
                      "Additional metadata key-value pairs applied to each document"}}}}},
                 {"required", {"directory_path"}}}}},
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
                     {"items", {"type", "string"}},
                     {"description", "Only restore files matching these patterns"}}},
                   {"exclude_patterns",
                    {{"type", "array"},
                     {"items", {"type", "string"}},
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
                 {"required", {"collection", "output_directory"}}}}},
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
                     {"items", {"type", "string"}},
                     {"description", "Only restore files matching these patterns"}}},
                   {"exclude_patterns",
                    {{"type", "array"},
                     {"items", {"type", "string"}},
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
                 {"required", {"snapshot_id", "output_directory"}}}}},
              {{"name", "list_collections"},
               {"description", "List available collections"},
               {"inputSchema", {{"type", "object"}, {"properties", {}}}}},
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

json MCPServer::listPrompts() {
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

json MCPServer::callTool(const std::string& name, const json& arguments) {
    // Route to appropriate tool implementation
    if (name == "search_documents") {
        return searchDocuments(arguments);
    } else if (name == "grep_documents") {
        return grepDocuments(arguments);
    } else if (name == "store_document") {
        return storeDocument(arguments);
    } else if (name == "retrieve_document") {
        return retrieveDocument(arguments);
    } else if (name == "delete_document") {
        return deleteDocument(arguments);
    } else if (name == "update_metadata") {
        return updateMetadata(arguments);
    } else if (name == "update_document_metadata") {
        return updateDocumentMetadata(arguments);
    } else if (name == "list_documents") {
        return listDocuments(arguments);
    } else if (name == "get_stats") {
        return getStats(arguments);
    } else if (name == "delete_by_name") {
        return deleteByName(arguments);
    } else if (name == "get_by_name") {
        return getByName(arguments);
    } else if (name == "cat_document") {
        return catDocument(arguments);
    } else if (name == "add_directory") {
        return addDirectory(arguments);
    } else if (name == "restore_collection") {
        return restoreCollection(arguments);
    } else if (name == "restore_snapshot") {
        return restoreSnapshot(arguments);
    } else if (name == "list_collections") {
        return listCollections(arguments);
    } else if (name == "list_snapshots") {
        return listSnapshots(arguments);
    } else {
        throw std::runtime_error("Unknown tool: " + name);
    }
}

// Tool implementations
json MCPServer::searchDocuments(const json& args) {
    try {
        using namespace yams::app::services;

        // Map MCP args to service request
        SearchRequest req;
        req.query = args.value("query", std::string{});
        req.limit = static_cast<std::size_t>(args.value("limit", 10));
        req.fuzzy = args.value("fuzzy", false);
        req.similarity = args.value("similarity", 0.7f);
        req.hash = args.value("hash", std::string{});
        req.type = args.value("type", std::string{"hybrid"});
        req.verbose = args.value("verbose", false);

        // LLM ergonomics and display shaping
        req.pathsOnly = args.value("paths_only", false);
        req.lineNumbers = args.value("line_numbers", false);
        const int context = args.value("context", 0);
        req.beforeContext = (context > 0) ? context : args.value("before_context", 0);
        req.afterContext = (context > 0) ? context : args.value("after_context", 0);
        req.context = context;
        req.colorMode = args.value("color", std::string{"never"});

        // Build shared context and service
        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto searchSvc = makeSearchService(ctx);

        auto result = searchSvc->search(req);
        if (!result) {
            return {{"error", result.error().message}};
        }

        const auto& resp = result.value();

        // paths_only fast path
        if (req.pathsOnly) {
            json out;
            out["paths"] = json::array();
            for (const auto& p : resp.paths) {
                out["paths"].push_back(p);
            }
            return out;
        }

        // Detailed response
        json out;
        out["total"] = resp.total;
        out["type"] = resp.type;
        out["execution_time_ms"] = resp.executionTimeMs;

        json results = json::array();
        for (const auto& it : resp.results) {
            json j;
            j["id"] = it.id;
            if (!it.hash.empty())
                j["hash"] = it.hash;
            if (!it.title.empty())
                j["title"] = it.title;
            if (!it.path.empty())
                j["path"] = it.path;
            j["score"] = it.score;
            if (!it.snippet.empty())
                j["snippet"] = it.snippet;

            if (req.verbose) {
                json breakdown;
                if (it.vectorScore)
                    breakdown["vector_score"] = *it.vectorScore;
                if (it.keywordScore)
                    breakdown["keyword_score"] = *it.keywordScore;
                if (it.kgEntityScore)
                    breakdown["kg_entity_score"] = *it.kgEntityScore;
                if (it.structuralScore)
                    breakdown["structural_score"] = *it.structuralScore;
                j["score_breakdown"] = breakdown;
            }
            results.push_back(std::move(j));
        }
        out["results"] = std::move(results);
        return out;
    } catch (const std::exception& e) {
        return {{"error", std::string("Search failed: ") + e.what()}};
    }
}

json MCPServer::storeDocument(const json& args) {
    try {
        using namespace yams::app::services;

        // Map MCP args to DocumentService::store DTO
        StoreDocumentRequest req;
        req.path = args.value("path", std::string{});
        req.content = args.value("content", std::string{});
        req.name = args.value("name", std::string{});
        req.mimeType = args.value("mime_type", std::string{});

        // Tags array -> vector
        if (args.contains("tags") && args["tags"].is_array()) {
            for (const auto& t : args["tags"]) {
                if (t.is_string())
                    req.tags.push_back(t.get<std::string>());
            }
        }

        // Metadata object -> map<string,string>
        if (args.contains("metadata") && args["metadata"].is_object()) {
            for (const auto& [k, v] : args["metadata"].items()) {
                if (v.is_string())
                    req.metadata[k] = v.get<std::string>();
                else if (v.is_number_integer())
                    req.metadata[k] = std::to_string(v.get<int64_t>());
                else if (v.is_number_float())
                    req.metadata[k] = std::to_string(v.get<double>());
                else if (v.is_boolean())
                    req.metadata[k] = v.get<bool>() ? "true" : "false";
                else
                    req.metadata[k] = v.dump();
            }
        }

        // Call shared document service
        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);
        auto res = docSvc->store(req);
        if (!res) {
            return {{"error", res.error().message}};
        }

        const auto& out = res.value();
        return {{"hash", out.hash},
                {"bytes_stored", out.bytesStored},
                {"bytes_deduped", out.bytesDeduped}};
    } catch (const std::exception& e) {
        return {{"error", std::string("Store failed: ") + e.what()}};
    }
}

json MCPServer::retrieveDocument(const nlohmann::json& args) {
    try {
        using namespace yams::app::services;

        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);

        // Resolve selector: hash or name
        std::string hash = args.value("hash", std::string{});
        if (hash.empty() && args.contains("name") && args["name"].is_string()) {
            auto h = docSvc->resolveNameToHash(args["name"].get<std::string>());
            if (!h) {
                return {{"error", h.error().message}};
            }
            hash = h.value();
        }
        if (hash.empty()) {
            return {{"error", "Provide 'hash' or 'name'"}};
        }

        RetrieveDocumentRequest req;
        req.hash = hash;
        // Accept both outputPath and output_path for compatibility
        if (args.contains("outputPath") && args["outputPath"].is_string()) {
            req.outputPath = args["outputPath"].get<std::string>();
        } else if (args.contains("output_path") && args["output_path"].is_string()) {
            req.outputPath = args["output_path"].get<std::string>();
        }
        req.graph = args.value("graph", false);
        req.depth = args.value("depth", 1);
        req.includeContent = args.value("include_content", false);

        auto res = docSvc->retrieve(req);
        if (!res) {
            return {{"error", res.error().message}};
        }
        const auto& r = res.value();

        json out;
        out["graph_enabled"] = r.graphEnabled;

        if (r.document) {
            const auto& d = *r.document;
            json jd;
            jd["hash"] = d.hash;
            jd["path"] = d.path;
            jd["name"] = d.name;
            jd["size"] = d.size;
            jd["mime_type"] = d.mimeType;
            if (d.content)
                jd["content"] = *d.content;
            out["document"] = std::move(jd);
        }

        if (!r.related.empty()) {
            json rel = json::array();
            for (const auto& rd : r.related) {
                json x;
                x["hash"] = rd.hash;
                x["path"] = rd.path;
                x["distance"] = rd.distance;
                rel.push_back(std::move(x));
            }
            out["related_documents"] = std::move(rel);
        }

        return out;
    } catch (const std::exception& e) {
        return {{"error", std::string("Retrieve failed: ") + e.what()}};
    }
}

json MCPServer::getStats(const json& args) {
    try {
        auto stats = store_->getStats();
        bool fileTypes = args.value("file_types", false);
        json resp = {{"total_objects", stats.totalObjects},
                     {"total_bytes", stats.totalBytes},
                     {"unique_blocks", stats.uniqueBlocks},
                     {"deduplicated_bytes", stats.deduplicatedBytes},
                     {"dedup_ratio", stats.dedupRatio()}};

        if (fileTypes && metadataRepo_) {
            auto docsRes = metadataRepo_->findDocumentsByPath("%");
            size_t textCount = 0, binaryCount = 0;
            if (docsRes) {
                for (const auto& d : docsRes.value()) {
                    if (!d.mimeType.empty() && d.mimeType.rfind("text/", 0) == 0) {
                        ++textCount;
                    } else {
                        ++binaryCount;
                    }
                }
            }
            resp["file_type_breakdown"] = {
                {"file_type_distribution",
                 json::array({json({{"type", "text"}, {"count", textCount}}),
                              json({{"type", "binary"}, {"count", binaryCount}})})}};
        }

        return resp;
    } catch (const std::exception& e) {
        return {{"error", std::string("Retrieve failed: ") + e.what()}};
    }
}

json MCPServer::deleteByName(const nlohmann::json& args) {
    try {
        using namespace yams::app::services;

        // Map MCP args to service DTO
        DeleteByNameRequest req;
        req.name = args.value("name", std::string{});
        if (args.contains("names") && args["names"].is_array()) {
            for (const auto& n : args["names"]) {
                if (n.is_string())
                    req.names.push_back(n.get<std::string>());
            }
        }
        req.pattern = args.value("pattern", std::string{});
        req.dryRun = args.value("dry_run", false);

        // Call shared service
        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);
        auto res = docSvc->deleteByName(req);
        if (!res) {
            return {{"error", res.error().message}};
        }

        const auto& r = res.value();
        json out;
        out["dry_run"] = r.dryRun;

        json items = json::array();
        for (const auto& it : r.deleted) {
            json ji;
            ji["name"] = it.name;
            ji["hash"] = it.hash;
            ji["deleted"] = it.deleted;
            if (it.error)
                ji["error"] = *it.error;
            items.push_back(std::move(ji));
        }
        out["deleted"] = std::move(items);
        out["count"] = r.count;

        if (!r.errors.empty()) {
            json errs = json::array();
            for (const auto& er : r.errors) {
                json je;
                je["name"] = er.name;
                je["hash"] = er.hash;
                if (er.error)
                    je["error"] = *er.error;
                errs.push_back(std::move(je));
            }
            out["errors"] = std::move(errs);
        }

        return out;
    } catch (const std::exception& e) {
        return {{"error", std::string("Delete failed: ") + e.what()}};
    }
}

json MCPServer::getByName(const nlohmann::json& args) {
    try {
        using namespace yams::app::services;

        std::string name = args["name"];
        std::string output_path = args.value("output_path", "");

        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);

        // Resolve via DocumentService for parity
        auto h = docSvc->resolveNameToHash(name);
        if (!h) {
            return {{"error", h.error().message}};
        }
        std::string hash = h.value();

        json result;
        result["name"] = name;
        result["hash"] = hash;

        if (!output_path.empty()) {
            // Retrieve to specified file path using the service
            RetrieveDocumentRequest rreq;
            rreq.hash = hash;
            rreq.outputPath = output_path;
            auto r = docSvc->retrieve(rreq);
            if (!r) {
                return {{"error", r.error().message}};
            }
            if (r.value().document) {
                result["size"] = r.value().document->size;
            }
            result["output_path"] = output_path;
            result["message"] = "Document retrieved to: " + output_path;
        } else {
            // Get content directly via service
            CatDocumentRequest creq;
            creq.hash = hash;
            auto c = docSvc->cat(creq);
            if (!c) {
                return {{"error", c.error().message}};
            }
            result["content"] = c.value().content;
            result["size"] = c.value().size;
            result["message"] = "Document content retrieved";
        }

        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Get by name failed: ") + e.what()}};
    }
}

json MCPServer::catDocument(const nlohmann::json& args) {
    try {
        using namespace yams::app::services;

        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);

        CatDocumentRequest req;
        req.hash = args.value("hash", std::string{});
        req.name = args.value("name", std::string{});

        // Resolve via service if only name was provided
        if (req.hash.empty()) {
            if (req.name.empty()) {
                return {{"error", "No document specified (hash or name required)"}};
            }
            auto h = docSvc->resolveNameToHash(req.name);
            if (!h) {
                return {{"error", h.error().message}};
            }
            req.hash = h.value();
        }

        auto res = docSvc->cat(req);
        if (!res) {
            return {{"error", res.error().message}};
        }

        const auto& out = res.value();
        json result = json::object();
        result["hash"] = out.hash;
        if (!out.name.empty())
            result["name"] = out.name;
        result["content"] = out.content;
        result["size"] = out.size;
        return result;
    } catch (const std::exception& e) {
        return {{"error", std::string("Cat document failed: ") + e.what()}};
    }
}

json MCPServer::listDocuments(const json& args) {
    try {
        using namespace yams::app::services;

        // Map MCP args to service DTO
        ListDocumentsRequest req;
        req.limit = args.value("limit", 100);
        req.offset = args.value("offset", 0);
        req.pattern = args.value("pattern", std::string{});
        req.tags = args.value("tags", std::vector<std::string>{});
        req.type = args.value("type", std::string{});
        req.mime = args.value("mime", std::string{});
        req.extension = args.value("extension", std::string{});
        req.binary = args.value("binary", false);
        req.text = args.value("text", false);
        req.createdAfter = args.value("created_after", std::string{});
        req.createdBefore = args.value("created_before", std::string{});
        req.modifiedAfter = args.value("modified_after", std::string{});
        req.modifiedBefore = args.value("modified_before", std::string{});
        req.indexedAfter = args.value("indexed_after", std::string{});
        req.indexedBefore = args.value("indexed_before", std::string{});
        if (args.contains("recent") && args["recent"].is_number_integer()) {
            req.recent = args["recent"].get<int>();
        }
        req.sortBy = args.value("sort_by", std::string{"indexed"});
        req.sortOrder = args.value("sort_order", std::string{"desc"});

        // Call shared service
        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);
        auto res = docSvc->list(req);
        if (!res) {
            return {{"error", res.error().message}};
        }

        const auto& out = res.value();
        json j;
        j["documents"] = json::array();
        for (const auto& d : out.documents) {
            json jd;
            jd["name"] = d.name;
            jd["hash"] = d.hash;
            jd["path"] = d.path;
            jd["extension"] = d.extension;
            jd["size"] = d.size;
            jd["mime_type"] = d.mimeType;
            jd["file_type"] = d.fileType;
            jd["created"] = d.created;
            jd["modified"] = d.modified;
            jd["indexed"] = d.indexed;
            if (!d.tags.empty())
                jd["tags"] = d.tags;
            j["documents"].push_back(std::move(jd));
        }
        j["count"] = out.count;
        j["total_found"] = out.totalFound;
        if (out.pattern)
            j["pattern"] = *out.pattern;
        if (!out.filteredByTags.empty())
            j["filtered_by_tags"] = out.filteredByTags;
        j["sort_by"] = out.sortBy;
        j["sort_order"] = out.sortOrder;

        return j;
    } catch (const std::exception& e) {
        return {{"error", std::string("List documents failed: ") + e.what()}};
    }
}

json MCPServer::createResponse(const json& id, const json& result) {
    json response;
    response["jsonrpc"] = "2.0";
    if (!id.is_null()) {
        response["id"] = id;
    }
    response["result"] = result;
    return response;
}

json MCPServer::createError(const json& id, int code, const std::string& message) {
    json response;
    response["jsonrpc"] = "2.0";
    if (!id.is_null()) {
        response["id"] = id;
    }
    response["error"] = {{"code", code}, {"message", message}};
    return response;
}

// Name resolution helper methods (similar to CLI commands)
Result<std::string> MCPServer::resolveNameToHash(const std::string& name) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
    }

    // Search for documents with matching fileName
    auto documentsResult = metadataRepo_->findDocumentsByPath("%/" + name);
    if (!documentsResult) {
        // Try exact match
        documentsResult = metadataRepo_->findDocumentsByPath(name);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound, "Document not found: " + name};
        }
    }

    const auto& documents = documentsResult.value();
    if (documents.empty()) {
        return Error{ErrorCode::NotFound, "Document not found: " + name};
    }

    if (documents.size() > 1) {
        return Error{ErrorCode::InvalidOperation,
                     "Ambiguous name: multiple documents match '" + name + "'"};
    }

    return documents[0].sha256Hash;
}

Result<std::vector<std::pair<std::string, std::string>>>
MCPServer::resolveNameToHashes(const std::string& name) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }

    // Search for documents with matching fileName
    auto documentsResult = metadataRepo_->findDocumentsByPath("%/" + name);
    if (!documentsResult) {
        // Try exact match
        documentsResult = metadataRepo_->findDocumentsByPath(name);
        if (!documentsResult) {
            return Error{ErrorCode::NotFound,
                         "Failed to query documents: " + documentsResult.error().message};
        }
    }

    const auto& documents = documentsResult.value();
    if (documents.empty()) {
        return Error{ErrorCode::NotFound, "No documents found with name: " + name};
    }

    std::vector<std::pair<std::string, std::string>> results;
    for (const auto& doc : documents) {
        results.emplace_back(doc.fileName, doc.sha256Hash);
    }

    return results;
}

Result<std::vector<std::pair<std::string, std::string>>>
MCPServer::resolveNamesToHashes(const std::vector<std::string>& names) {
    std::vector<std::pair<std::string, std::string>> allResults;

    // Resolve each name
    for (const auto& name : names) {
        auto result = resolveNameToHashes(name);
        if (result) {
            for (const auto& pair : result.value()) {
                allResults.push_back(pair);
            }
        } else if (result.error().code != ErrorCode::NotFound) {
            // Return early on non-NotFound errors
            return result;
        }
        // Skip NotFound errors for individual names
    }

    if (allResults.empty()) {
        return Error{ErrorCode::NotFound, "No documents found for the specified names"};
    }

    return allResults;
}

Result<std::vector<std::pair<std::string, std::string>>>
MCPServer::resolvePatternToHashes(const std::string& pattern) {
    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
    }

    // Convert glob pattern to SQL LIKE pattern
    std::string sqlPattern = pattern;
    // Replace glob wildcards with SQL wildcards
    std::replace(sqlPattern.begin(), sqlPattern.end(), '*', '%');
    std::replace(sqlPattern.begin(), sqlPattern.end(), '?', '_');

    // Ensure it matches the end of the path (filename)
    if (sqlPattern.front() != '%') {
        sqlPattern = "%/" + sqlPattern;
    }

    auto documentsResult = metadataRepo_->findDocumentsByPath(sqlPattern);
    if (!documentsResult) {
        return Error{ErrorCode::NotFound,
                     "Failed to query documents: " + documentsResult.error().message};
    }

    std::vector<std::pair<std::string, std::string>> results;
    for (const auto& doc : documentsResult.value()) {
        results.emplace_back(doc.fileName, doc.sha256Hash);
    }

    return results;
}

json MCPServer::addDirectory(const json& args) {
    try {
        std::string directoryPath = args.contains("directory_path")
                                        ? args["directory_path"].get<std::string>()
                                        : args.value("path", std::string{});
        std::string collection = args.value("collection", "");
        std::string snapshotId = args.value("snapshot_id", "");
        std::string snapshotLabel = args.value("snapshot_label", "");
        bool recursive = args.value("recursive", true);

        if (!std::filesystem::exists(directoryPath) ||
            !std::filesystem::is_directory(directoryPath)) {
            return {{"error", "Directory does not exist or is not a directory: " + directoryPath}};
        }

        if (!recursive) {
            return {{"error", "Non-recursive directory addition not supported via MCP"}};
        }

        // Generate snapshot ID if only label provided
        if (snapshotId.empty() && !snapshotLabel.empty()) {
            auto now = std::chrono::system_clock::now();
            auto timestamp =
                std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch())
                    .count();
            snapshotId = "snapshot_" + std::to_string(timestamp);
        }

        std::vector<std::filesystem::path> filesToAdd;

        // Collect files to add
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(directoryPath)) {
                if (!entry.is_regular_file())
                    continue;

                // TODO: Add include/exclude pattern filtering if needed
                filesToAdd.push_back(entry.path());
            }
        } catch (const std::filesystem::filesystem_error& e) {
            return {{"error", "Failed to traverse directory: " + std::string(e.what())}};
        }

        if (filesToAdd.empty()) {
            return {{"error", "No files found in directory"}};
        }

        // Process each file
        size_t successCount = 0;
        size_t failureCount = 0;
        json results = json::array();

        for (const auto& filePath : filesToAdd) {
            try {
                // Build metadata with collection/snapshot info
                api::ContentMetadata metadata;
                metadata.name = filePath.filename().string();

                // Add collection and snapshot metadata
                if (!collection.empty()) {
                    metadata.tags["collection"] = collection;
                }
                if (!snapshotId.empty()) {
                    metadata.tags["snapshot_id"] = snapshotId;
                }
                if (!snapshotLabel.empty()) {
                    metadata.tags["snapshot_label"] = snapshotLabel;
                }

                // Add relative path metadata
                auto relativePath = std::filesystem::relative(filePath, directoryPath);
                metadata.tags["path"] = relativePath.string();

                // Additional tags from arguments
                if (args.contains("tags")) {
                    for (const auto& tag : args["tags"]) {
                        if (tag.is_string()) {
                            metadata.tags[tag] = "";
                        }
                    }
                }

                // Additional metadata from arguments
                if (args.contains("metadata")) {
                    for (const auto& [key, value] : args["metadata"].items()) {
                        if (value.is_string()) {
                            metadata.tags[key] = value;
                        }
                    }
                }

                // Store the file
                auto result = store_->store(filePath, metadata);
                if (!result) {
                    failureCount++;
                    results.push_back({{"path", filePath.string()},
                                       {"status", "failed"},
                                       {"error", result.error().message}});
                    continue;
                }

                // Store metadata in database
                auto metadataRepo = metadataRepo_;
                if (metadataRepo) {
                    metadata::DocumentInfo docInfo;
                    docInfo.filePath = filePath.string();
                    docInfo.fileName = filePath.filename().string();
                    docInfo.fileExtension = filePath.extension().string();
                    docInfo.fileSize = std::filesystem::file_size(filePath);
                    docInfo.sha256Hash = result.value().contentHash;
                    docInfo.mimeType = "application/octet-stream";

                    auto now = std::chrono::system_clock::now();
                    docInfo.createdTime = now;
                    // Convert filesystem time to system_clock time
                    auto fsTime = std::filesystem::last_write_time(filePath);
                    auto systemTime =
                        std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                            fsTime - std::filesystem::file_time_type::clock::now() +
                            std::chrono::system_clock::now());
                    docInfo.modifiedTime = systemTime;
                    docInfo.indexedTime = now;

                    auto insertResult = metadataRepo->insertDocument(docInfo);
                    if (insertResult) {
                        int64_t docId = insertResult.value();

                        // Add all metadata
                        for (const auto& [key, value] : metadata.tags) {
                            metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
                        }
                    }
                }

                successCount++;
                results.push_back({{"path", filePath.string()},
                                   {"status", "success"},
                                   {"hash", result.value().contentHash},
                                   {"bytes_stored", result.value().bytesStored}});

            } catch (const std::exception& e) {
                failureCount++;
                results.push_back({{"path", filePath.string()},
                                   {"status", "failed"},
                                   {"error", std::string("Exception: ") + e.what()}});
            }
        }

        return {{"summary",
                 {{"files_processed", filesToAdd.size()},
                  {"files_added", successCount},
                  {"files_failed", failureCount}}},
                {"collection", collection},
                {"snapshot_id", snapshotId},
                {"snapshot_label", snapshotLabel},
                {"results", results}};

    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in addDirectory: ") + e.what()}};
    }
}

json MCPServer::restoreCollection(const json& args) {
    try {
        std::string collection = args["collection"];
        std::string outputDir = args.value("output_directory", ".");
        std::string layoutTemplate = args.value("layout_template", "{path}");
        bool overwrite = args.value("overwrite", false);
        bool createDirs = args.value("create_dirs", true);
        bool dryRun = args.value("dry_run", false);

        // Get documents from collection
        auto documentsResult = metadataRepo_->findDocumentsByCollection(collection);
        if (!documentsResult) {
            return {{"error",
                     "Failed to find documents in collection: " + documentsResult.error().message}};
        }

        const auto& documents = documentsResult.value();

        if (documents.empty()) {
            return {{"message", "No documents found in collection: " + collection}};
        }

        return performRestore(documents, outputDir, layoutTemplate, overwrite, createDirs, dryRun,
                              "collection: " + collection);

    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in restoreCollection: ") + e.what()}};
    }
}

json MCPServer::restoreSnapshot(const json& args) {
    try {
        std::string snapshotId = args.value("snapshot_id", "");
        std::string snapshotLabel = args.value("snapshot_label", "");
        std::string outputDir = args.value("output_directory", ".");
        std::string layoutTemplate = args.value("layout_template", "{path}");
        bool overwrite = args.value("overwrite", false);
        bool createDirs = args.value("create_dirs", true);
        bool dryRun = args.value("dry_run", false);

        // Get documents from snapshot
        Result<std::vector<metadata::DocumentInfo>> documentsResult =
            std::vector<metadata::DocumentInfo>();
        std::string scope;

        if (!snapshotId.empty()) {
            documentsResult = metadataRepo_->findDocumentsBySnapshot(snapshotId);
            scope = "snapshot ID: " + snapshotId;
        } else if (!snapshotLabel.empty()) {
            documentsResult = metadataRepo_->findDocumentsBySnapshotLabel(snapshotLabel);
            scope = "snapshot label: " + snapshotLabel;
        } else {
            return {{"error", "Either snapshot_id or snapshot_label must be provided"}};
        }

        if (!documentsResult) {
            return {{"error",
                     "Failed to find documents in snapshot: " + documentsResult.error().message}};
        }

        const auto& documents = documentsResult.value();

        if (documents.empty()) {
            return {{"message", "No documents found in snapshot"}};
        }

        return performRestore(documents, outputDir, layoutTemplate, overwrite, createDirs, dryRun,
                              scope);

    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in restoreSnapshot: ") + e.what()}};
    }
}

json MCPServer::listCollections(const json& /*args*/) {
    try {
        auto collectionsResult = metadataRepo_->getCollections();
        if (!collectionsResult) {
            return {{"error", "Failed to get collections: " + collectionsResult.error().message}};
        }

        return {{"collections", collectionsResult.value()}};

    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in listCollections: ") + e.what()}};
    }
}

json MCPServer::listSnapshots(const json& args) {
    try {
        bool withLabels = args.value("with_labels", true);

        auto snapshotsResult = metadataRepo_->getSnapshots();
        if (!snapshotsResult) {
            return {{"error", "Failed to get snapshots: " + snapshotsResult.error().message}};
        }

        json response;
        response["snapshot_ids"] = snapshotsResult.value();

        if (withLabels) {
            auto labelsResult = metadataRepo_->getSnapshotLabels();
            if (labelsResult) {
                response["snapshot_labels"] = labelsResult.value();
            }
        }

        return response;

    } catch (const std::exception& e) {
        return {{"error", std::string("Exception in listSnapshots: ") + e.what()}};
    }
}

json MCPServer::performRestore(const std::vector<metadata::DocumentInfo>& documents,
                               const std::string& outputDir, const std::string& layoutTemplate,
                               bool overwrite, bool createDirs, bool dryRun,
                               const std::string& scope) {
    size_t successCount = 0;
    size_t failureCount = 0;
    size_t skippedCount = 0;
    json results = json::array();

    for (const auto& doc : documents) {
        try {
            // Get metadata for layout expansion
            auto metadataResult = metadataRepo_->getAllMetadata(doc.id);
            if (!metadataResult) {
                failureCount++;
                results.push_back(
                    {{"document", doc.fileName},
                     {"status", "failed"},
                     {"error", "Failed to get metadata: " + metadataResult.error().message}});
                continue;
            }

            // Expand layout template
            std::string layoutPath =
                expandLayoutTemplate(layoutTemplate, doc, metadataResult.value());

            std::filesystem::path outputPath = std::filesystem::path(outputDir) / layoutPath;

            if (dryRun) {
                results.push_back({{"document", doc.fileName},
                                   {"status", "would_restore"},
                                   {"output_path", outputPath.string()},
                                   {"size", doc.fileSize}});
                successCount++;
                continue;
            }

            // Check if file already exists
            if (std::filesystem::exists(outputPath) && !overwrite) {
                skippedCount++;
                results.push_back({{"document", doc.fileName},
                                   {"status", "skipped"},
                                   {"reason", "File exists and overwrite=false"}});
                continue;
            }

            // Create parent directories if needed
            if (createDirs) {
                std::filesystem::create_directories(outputPath.parent_path());
            }

            // Retrieve document
            auto retrieveResult = store_->retrieve(doc.sha256Hash, outputPath);
            if (!retrieveResult) {
                failureCount++;
                results.push_back(
                    {{"document", doc.fileName},
                     {"status", "failed"},
                     {"error", "Failed to retrieve: " + retrieveResult.error().message}});
                continue;
            }

            successCount++;
            results.push_back({{"document", doc.fileName},
                               {"status", "restored"},
                               {"output_path", outputPath.string()},
                               {"size", doc.fileSize}});

        } catch (const std::exception& e) {
            failureCount++;
            results.push_back({{"document", doc.fileName},
                               {"status", "failed"},
                               {"error", std::string("Exception: ") + e.what()}});
        }
    }

    return {{"summary",
             {{"documents_found", documents.size()},
              {"documents_restored", successCount},
              {"documents_failed", failureCount},
              {"documents_skipped", skippedCount},
              {"dry_run", dryRun}}},
            {"scope", scope},
            {"output_directory", outputDir},
            {"layout_template", layoutTemplate},
            {"results", results}};
}

std::string MCPServer::expandLayoutTemplate(
    const std::string& layoutTemplate, const metadata::DocumentInfo& doc,
    const std::unordered_map<std::string, metadata::MetadataValue>& metadata) {
    std::string result = layoutTemplate;

    // Replace placeholders
    size_t pos = 0;
    while ((pos = result.find("{", pos)) != std::string::npos) {
        size_t endPos = result.find("}", pos);
        if (endPos == std::string::npos)
            break;

        std::string placeholder = result.substr(pos + 1, endPos - pos - 1);
        std::string replacement;

        if (placeholder == "collection") {
            auto it = metadata.find("collection");
            replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
        } else if (placeholder == "snapshot_id") {
            auto it = metadata.find("snapshot_id");
            replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
        } else if (placeholder == "snapshot_label") {
            auto it = metadata.find("snapshot_label");
            replacement = (it != metadata.end()) ? it->second.asString() : "unknown";
        } else if (placeholder == "path") {
            auto it = metadata.find("path");
            replacement = (it != metadata.end()) ? it->second.asString() : doc.fileName;
        } else if (placeholder == "name") {
            replacement = doc.fileName;
        } else if (placeholder == "hash") {
            replacement = doc.sha256Hash.substr(0, 12);
        } else {
            // Unknown placeholder, leave as is
            pos = endPos + 1;
            continue;
        }

        result.replace(pos, endPos - pos + 1, replacement);
        pos += replacement.length();
    }

    return result;
}

// Helper method implementations
bool MCPServer::isValidHash(const std::string& str) {
    if (str.length() != 64)
        return false;
    for (char c : str) {
        if (!std::isxdigit(c))
            return false;
    }
    return true;
}

json MCPServer::searchByHash(const std::string& hash, size_t limit) {
    // Search for documents by hash prefix - use path pattern as workaround
    auto docsResult = metadataRepo_->findDocumentsByPath("%");
    if (!docsResult) {
        return {{"error", "Failed to search by hash"}};
    }

    json results = json::array();
    size_t count = 0;
    for (const auto& doc : docsResult.value()) {
        // Filter by hash prefix
        if (doc.sha256Hash.substr(0, hash.length()) != hash)
            continue;
        if (count >= limit)
            break;
        results.push_back({{"hash", doc.sha256Hash},
                           {"name", doc.fileName},
                           {"path", doc.filePath},
                           {"size", doc.fileSize}});
        count++;
    }

    return results;
}

json MCPServer::grepDocuments(const json& args) {
    using namespace yams::app::services;
    try {
        GrepRequest req;
        req.pattern = args.value("pattern", std::string{});
        if (req.pattern.empty()) {
            return {{"error", "Pattern is required"}};
        }

        if (args.contains("paths") && args["paths"].is_array()) {
            for (const auto& p : args["paths"]) {
                if (p.is_string())
                    req.paths.push_back(p.get<std::string>());
            }
        }

        // Context
        req.beforeContext = args.value("before_context", 0);
        req.afterContext = args.value("after_context", 0);
        int context = args.value("context", 0);
        if (context > 0) {
            req.context = context;
            req.beforeContext = context;
            req.afterContext = context;
        }

        // Pattern options
        req.ignoreCase = args.value("ignore_case", false);
        req.word = args.value("word", false);
        req.invert = args.value("invert", false);

        // Output modes
        req.lineNumbers = args.value("line_numbers", false);
        req.withFilename = args.value("with_filename", true);
        req.count = args.value("count", false);
        req.filesWithMatches = args.value("files_with_matches", false);
        req.filesWithoutMatch = args.value("files_without_match", false);
        req.colorMode = args.value("color", std::string{"auto"});

        // Limits
        req.maxCount = args.value("max_count", 0);

        // Call shared service
        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto grepSvc = makeGrepService(ctx);
        auto res = grepSvc->grep(req);
        if (!res) {
            return {{"error", res.error().message}};
        }
        const auto& out = res.value();

        // files_with_matches mode
        if (req.filesWithMatches) {
            json j;
            j["files_with_matches"] = json::array();
            for (const auto& f : out.filesWith) {
                j["files_with_matches"].push_back(f);
            }
            j["count"] = out.filesWith.size();
            j["pattern"] = req.pattern;
            return j;
        }

        // files_without_match mode
        if (req.filesWithoutMatch) {
            json j;
            j["files_without_match"] = json::array();
            for (const auto& f : out.filesWithout) {
                j["files_without_match"].push_back(f);
            }
            j["count"] = out.filesWithout.size();
            j["pattern"] = req.pattern;
            return j;
        }

        // count mode
        if (req.count) {
            json j;
            j["total_matches"] = out.totalMatches;
            j["results"] = json::array();
            for (const auto& fileRes : out.results) {
                j["results"].push_back(
                    {{"file", fileRes.file}, {"match_count", fileRes.matchCount}});
            }
            j["pattern"] = req.pattern;
            return j;
        }

        // Detailed match results
        json j;
        json results = json::array();
        for (const auto& fileRes : out.results) {
            json jf;
            jf["file"] = fileRes.file;
            jf["matches"] = json::array();

            for (const auto& m : fileRes.matches) {
                json jm;
                if (req.lineNumbers && m.lineNumber > 0) {
                    jm["line_number"] = static_cast<int>(m.lineNumber);
                }
                jm["line"] = m.line;
                if (m.columnStart > 0) {
                    jm["column_start"] = static_cast<int>(m.columnStart);
                }
                if (m.columnEnd > 0) {
                    jm["column_end"] = static_cast<int>(m.columnEnd);
                }

                if (!m.before.empty()) {
                    json before = json::array();
                    for (const auto& b : m.before)
                        before.push_back(b);
                    jm["before"] = before;
                }
                if (!m.after.empty()) {
                    json after = json::array();
                    for (const auto& a : m.after)
                        after.push_back(a);
                    jm["after"] = after;
                }

                jf["matches"].push_back(std::move(jm));
            }

            results.push_back(std::move(jf));
        }

        j["results"] = std::move(results);
        j["pattern"] = req.pattern;
        return j;
    } catch (const std::exception& e) {
        return {{"error", std::string("Grep failed: ") + e.what()}};
    }
}

json MCPServer::deleteDocument(const json& args) {
    std::string hash = args.value("hash", "");
    if (hash.empty()) {
        return {{"error", "Hash is required"}};
    }

    auto result = store_->remove(hash);
    if (!result) {
        return {{"error", result.error().message}};
    }

    if (!result.value()) {
        return {{"error", "Document not found"}};
    }

    return {{"success", true}, {"hash", hash}};
}

json MCPServer::updateMetadata(const json& args) {
    // Deprecated - use updateDocumentMetadata instead
    return updateDocumentMetadata(args);
}

json MCPServer::updateDocumentMetadata(const nlohmann::json& args) {
    using namespace yams::app::services;
    try {
        AppContext ctx{store_, searchExecutor_, metadataRepo_, hybridEngine_};
        auto docSvc = makeDocumentService(ctx);

        UpdateMetadataRequest req;
        req.hash = args.value("hash", std::string{});
        req.name = args.value("name", std::string{});
        req.verbose = args.value("verbose", false);

        if (args.contains("metadata")) {
            const auto& md = args["metadata"];
            if (md.is_array()) {
                // Array of "key=value" pairs
                for (const auto& entry : md) {
                    if (entry.is_string()) {
                        req.pairs.push_back(entry.get<std::string>());
                    }
                }
            } else if (md.is_object()) {
                // Object of key -> value (stringify non-strings)
                for (const auto& [k, v] : md.items()) {
                    if (v.is_string())
                        req.keyValues[k] = v.get<std::string>();
                    else if (v.is_number_integer())
                        req.keyValues[k] = std::to_string(v.get<int64_t>());
                    else if (v.is_number_float())
                        req.keyValues[k] = std::to_string(v.get<double>());
                    else if (v.is_boolean())
                        req.keyValues[k] = v.get<bool>() ? "true" : "false";
                    else
                        req.keyValues[k] = v.dump();
                }
            } else {
                return {{"error", "Unsupported metadata format"}};
            }
        }

        // Optional compatibility: tags array -> key-only entries with empty value
        if (args.contains("tags") && args["tags"].is_array()) {
            for (const auto& t : args["tags"]) {
                if (t.is_string()) {
                    const auto tag = t.get<std::string>();
                    // store as a key with empty value to indicate presence
                    req.keyValues[tag] = "";
                }
            }
        }

        if (req.keyValues.empty() && req.pairs.empty()) {
            return {{"error", "Metadata is required"}};
        }

        auto res = docSvc->updateMetadata(req);
        if (!res) {
            return {{"error", res.error().message}};
        }

        const auto& out = res.value();
        nlohmann::json j;
        j["success"] = out.success;
        j["hash"] = out.hash;
        j["updates_applied"] = out.updatesApplied;
        if (out.documentId)
            j["document_id"] = *out.documentId;
        return j;
    } catch (const std::exception& e) {
        return {{"error", std::string("Update metadata failed: ") + e.what()}};
    }
}

} // namespace yams::mcp
