#include <yams/cli/async_bridge.h>
#include <yams/config/config_migration.h>
#include <yams/core/task.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/downloader/downloader.hpp>
#include <yams/mcp/error_handling.h>
#include <yams/mcp/mcp_server.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <mutex>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
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
namespace {
// Synchronous pooled_execute is deprecated and returns NotImplemented
// This is kept only for toolRegistry compatibility until it's migrated to async
template <typename Manager, typename TRequest, typename Render>
Result<void> pooled_execute(Manager& manager, const TRequest& req,
                            std::function<Result<void>()> fallback, Render&& render) {
    (void)manager;
    (void)req;
    (void)fallback;
    (void)render;
    return Error{ErrorCode::NotImplemented,
                 "Synchronous pooled_execute is deprecated. Use async handlers via direct method calls."};
}

// Async variant to be used once MCP handlers become coroutine-based.

// No-op async bridge here; MCP dispatch below uses a detached thread
// to run yams::Task<> to completion off the io thread.

} // namespace

// Define static mutex for StdioTransport
std::mutex StdioTransport::out_mutex_;

// Non-blocking send: enqueue message for writer thread
void StdioTransport::sendAsync(const json& message) {
    if (state_.load() != TransportState::Connected) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(queueMutex_);
        outQueue_.push_back(message);
    }
    queueCv_.notify_one();
}

// Dedicated writer thread: drains queue and writes framed messages to stdout
void StdioTransport::writerLoop() {
    while (true) {
        json message;
        {
            std::unique_lock<std::mutex> lk(queueMutex_);
            queueCv_.wait(lk, [&] {
                return !outQueue_.empty() || state_.load() == TransportState::Closing ||
                       (externalShutdown_ && *externalShutdown_);
            });
            if (!outQueue_.empty()) {
                message = std::move(outQueue_.front());
                outQueue_.pop_front();
            }
        }

        if (!message.is_null()) {
            auto currentState = state_.load();
            if (currentState == TransportState::Connected || currentState == TransportState::Closing) {
                std::lock_guard<std::mutex> lock(out_mutex_);
                const std::string payload = message.dump();

                if (outbuf_ && std::cout.rdbuf() != outbuf_) {
                    (void)std::cout.rdbuf(outbuf_);
                }

                // LSP/MCP framing
                std::cout << "Content-Length: " << payload.size() << "\r\n";
                std::cout << "Content-Type: application/json\r\n\r\n";
                std::cout << payload;
                std::cout << "\r\n";
                std::cout.flush();
            }
        }

        // Graceful exit: on close and queue drained, or external shutdown
        if (state_.load() == TransportState::Closing) {
            std::unique_lock<std::mutex> lk(queueMutex_);
            if (outQueue_.empty()) {
                break;
            }
        }
        if (externalShutdown_ && *externalShutdown_) {
            break;
        }
    }
    writerRunning_.store(false);
}

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
    state_.store(TransportState::Connected);
    // Start outbound writer thread for non-blocking sends
    writerRunning_.store(true);
    writerThread_ = std::thread(&StdioTransport::writerLoop, this);
    writerThread_.detach();
}

void StdioTransport::send(const json& message) {
    auto currentState = state_.load();
    if (currentState == TransportState::Connected) {
        std::lock_guard<std::mutex> lock(out_mutex_);
        const std::string payload = message.dump();

        if (outbuf_ && std::cout.rdbuf() != outbuf_) {
            (void)std::cout.rdbuf(outbuf_);
        }

        // Minimal LSP/MCP framing: only Content-Length header is required
        std::cout << "Content-Length: " << payload.size() << "\r\n";
        std::cout << "Content-Type: application/json\r\n\r\n";
        std::cout << payload;
        std::cout << "\r\n";
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
    fds.events = POLLIN | POLLHUP;
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

    return result > 0 && (fds.revents & (POLLIN | POLLHUP));
#endif
}

MessageResult StdioTransport::receive() {
    auto currentState = state_.load();
    if (currentState == TransportState::Closing || currentState == TransportState::Disconnected) {
        return Error{ErrorCode::NetworkError, "Transport is closed or disconnected"};
    }

    // MCP stdio transport: Support both LSP-style Content-Length framing and
    // newline-delimited JSON. Use captured input buffer to respect redirections.
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
            std::string firstLine;
            in.clear();

            // Lock mutex for thread-safe reads
            if (!std::getline(in, firstLine)) {
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

            // Handle CRLF: strip trailing '\r' if present
            if (!firstLine.empty() && firstLine.back() == '\r') {
                firstLine.pop_back();
            }

            // Check for LSP-style framing
            if (firstLine.rfind("Content-Length:", 0) == 0) {
                // Parse length
                std::size_t colon = firstLine.find(':');
                std::size_t len = 0;
                if (colon != std::string::npos) {
                    // Skip optional space
                    std::string lenStr = firstLine.substr(colon + 1);
                    if (!lenStr.empty() && lenStr.front() == ' ')
                        lenStr.erase(0, 1);
                    try {
                        len = static_cast<std::size_t>(std::stoul(lenStr));
                    } catch (...) {
                        recordError();
                        return Error{ErrorCode::InvalidData, "Invalid Content-Length header"};
                    }
                } else {
                    recordError();
                    return Error{ErrorCode::InvalidData, "Malformed Content-Length header"};
                }

                // Consume header lines until an empty line (CRLF) to support multiple headers
                std::string headerLine;
                while (std::getline(in, headerLine)) {
                    if (!headerLine.empty() && headerLine.back() == '\r')
                        headerLine.pop_back();
                    if (headerLine.empty())
                        break; // end of headers
                    // Ignore additional headers such as Content-Type
                }

                // Read exact number of bytes
                std::string payload;
                payload.resize(len);
                in.read(payload.data(), static_cast<std::streamsize>(len));
                if (in.gcount() != static_cast<std::streamsize>(len)) {
                    recordError();
                    return Error{ErrorCode::NetworkError, "Short read on Content-Length payload"};
                }
                // Compatibility: consume any trailing CR/LF after framed payload, if present
                if (in.rdbuf() && in.rdbuf()->in_avail() > 0) {
                    while (in.rdbuf()->in_avail() > 0) {
                        int c = in.peek();
                        if (c == '\r' || c == '\n') { (void)in.get(); }
                        else { break; }
                    }
                }

                // Parse JSON message using safe parser
                auto parseResult = json_utils::parse_json(payload);
                if (!parseResult) {
                    recordError();
                    spdlog::debug("JSON parse error (framed): {}", parseResult.error().message);
                    state_.store(TransportState::Error);
                    return parseResult.error();
                }

                resetErrorCount();
                return parseResult.value();
            }

            // Fallback: treat line as a full JSON message (newline-delimited mode)
            if (firstLine.empty()) {
                // If we got an empty line and there's no more input, check EOF/timeout
                if (in.eof() || (!in.rdbuf() || in.rdbuf()->in_avail() == 0)) {
                    if (!isInputAvailable(10)) {
                        state_.store(TransportState::Disconnected);
                        return Error{ErrorCode::NetworkError, "No more input after empty line"};
                    }
                }
                continue;
            }

            // Parse JSON message using safe parser
            auto parseResult = json_utils::parse_json(firstLine);
            if (!parseResult) {
                recordError();
                spdlog::debug("JSON parse error (line): {}", parseResult.error().message);

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
    // Ensure logging goes to stderr to keep stdout clean for MCP framing
    if (auto existing = spdlog::get("yams-mcp")) {
        spdlog::set_default_logger(existing);
    } else {
        auto logger = spdlog::stderr_color_mt("yams-mcp");
        spdlog::set_default_logger(logger);
    }
    // Set external shutdown flag on StdioTransport if applicable
    if (auto* stdioTransport = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdioTransport->setShutdownFlag(externalShutdown_);
    }

    // Initialize a single multiplexed daemon client (replaces legacy pool/managers)
    {
        yams::daemon::ClientConfig cfg;
        cfg.enableChunkedResponses = true;
        cfg.singleUseConnections = false;
        cfg.requestTimeout = std::chrono::seconds(30);
        cfg.headerTimeout = std::chrono::seconds(30);
        cfg.bodyTimeout = std::chrono::seconds(120);
        cfg.maxInflight = 128;
        daemon_client_ = std::make_shared<yams::daemon::DaemonClient>(cfg);
    }
    // Legacy pool config removed


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
    // Start a small fixed thread pool for request handling (avoid unbounded detached threads)
    {
        size_t hc = std::thread::hardware_concurrency();
        if (hc == 0) hc = 2;
        size_t threads = std::min<size_t>(4, std::max<size_t>(2, hc));
        startThreadPool(threads);
    }

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
        auto request = messageResult.value();
        // Detect JSON‑RPC notification (no "id" field per spec)
        const bool isNotification = !request.contains("id");

        // Async dispatch is now the default for all MCP handlers
        {
            std::string method = request.value("method", "");
            // Accept initialized notifications from clients (both MCP and legacy forms)
            if (method == "initialized" || method == "notifications/initialized") {
                // Client signals readiness; record and send server readiness notification
                initialized_.exchange(true);
                spdlog::info("MCP client sent notifications/initialized");
                transport_->send(createReadyNotification());
                continue;
            }
            if (method == "search") {
                auto id = request.value("id", json{});
                auto params = request.value("params", json::object());

                enqueueTask([this, id, params]() mutable {
                    auto task = [this, id, params]() -> yams::Task<void> {
                        auto mcpReq = MCPSearchRequest::fromJson(params);
                        auto result = co_await handleSearchDocuments(mcpReq);
                        if (result) {
                            auto resp = createResponse(id, result.value().toJson());
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        } else {
                            auto err = createError(id, protocol::INTERNAL_ERROR, result.error().message);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(err); } else { transport_->send(err); }
                        }
                        co_return;
                    }();
                    try { task.get(); } catch (...) {}
                });

                continue;
            } else if (method == "grep") {
                auto id = request.value("id", json{});
                auto params = request.value("params", json::object());

                enqueueTask([this, id, params]() mutable {
                    auto task = [this, id, params]() -> yams::Task<void> {
                        auto mcpReq = MCPGrepRequest::fromJson(params);
                        auto result = co_await handleGrepDocuments(mcpReq);
                        if (result) {
                            auto resp = createResponse(id, result.value().toJson());
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        } else {
                            auto err = createError(id, protocol::INTERNAL_ERROR, result.error().message);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(err); } else { transport_->send(err); }
                        }
                        co_return;
                    }();
                    try { task.get(); } catch (...) {}
                });

                continue;
            } else if (method == "list") {
                auto id = request.value("id", json{});
                auto params = request.value("params", json::object());

                enqueueTask([this, id, params]() mutable {
                    auto task = [this, id, params]() -> yams::Task<void> {
                        auto mcpReq = MCPListDocumentsRequest::fromJson(params);
                        auto result = co_await handleListDocuments(mcpReq);
                        if (result) {
                            auto resp = createResponse(id, result.value().toJson());
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        } else {
                            auto err = createError(id, protocol::INTERNAL_ERROR, result.error().message);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(err); } else { transport_->send(err); }
                        }
                        co_return;
                    }();
                    try { task.get(); } catch (...) {}
                });

                continue;
            } else if (method == "get") {
                auto id = request.value("id", json{});
                auto params = request.value("params", json::object());

                enqueueTask([this, id, params]() mutable {
                    auto task = [this, id, params]() -> yams::Task<void> {
                        auto mcpReq = MCPRetrieveDocumentRequest::fromJson(params);
                        auto result = co_await handleRetrieveDocument(mcpReq);
                        if (result) {
                            auto resp = createResponse(id, result.value().toJson());
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        } else {
                            auto err = createError(id, protocol::INTERNAL_ERROR, result.error().message);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(err); } else { transport_->send(err); }
                        }
                        co_return;
                    }();
                    try { task.get(); } catch (...) {}
                });

                continue;
            } else if (method == "add") {
                auto id = request.value("id", json{});
                auto params = request.value("params", json::object());

                enqueueTask([this, id, params]() mutable {
                    auto task = [this, id, params]() -> yams::Task<void> {
                        auto mcpReq = MCPStoreDocumentRequest::fromJson(params);
                        auto result = co_await handleStoreDocument(mcpReq);
                        if (result) {
                            auto resp = createResponse(id, result.value().toJson());
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        } else {
                            auto err = createError(id, protocol::INTERNAL_ERROR, result.error().message);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(err); } else { transport_->send(err); }
                        }
                        co_return;
                    }();
                    try { task.get(); } catch (...) {}
                });

                continue;
            } else if (method == "tools/call") {
                auto id = request.value("id", json{});
                auto params = request.value("params", json::object());
                auto toolName = params.value("name", "");
                auto toolArgs = params.value("arguments", json::object());

                enqueueTask([this, id, toolName, toolArgs]() mutable {
                    auto task = [this, id, toolName, toolArgs]() -> yams::Task<void> {
                        if (!toolRegistry_) {
                            auto err = createError(id, protocol::INTERNAL_ERROR, "Tool registry not initialized");
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(err); } else { transport_->send(err); }
                            co_return;
                        }

                        auto toolResult = co_await toolRegistry_->callTool(toolName, toolArgs);

                        // Wrap non-MCP-shaped results into MCP content format for compatibility with MCP clients
                        if (toolResult.contains("content") && toolResult["content"].is_array()) {
                            auto resp = createResponse(id, toolResult);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        } else {
                            json wrapped = {
                                {"content", json::array({json{{"type", "text"}, {"text", toolResult.dump(2)}}})}};
                            auto resp = createResponse(id, wrapped);
                            if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) { stdio->sendAsync(resp); } else { transport_->send(resp); }
                        }
                        co_return;
                    }();
                    try { task.get(); } catch (...) {}
                });

                continue;
            }
        }
        // Fall through to synchronous handling for any unhandled methods

        auto response = handleRequest(request);
        if (response) {
            // Skip responses for notifications (no id per JSON-RPC 2.0)
            if (!isNotification) {
                transport_->send(response.value());
            } else {
                spdlog::debug("Notification processed without response");
            }
        } else {
            // Send error response for protocol violations only for requests (not notifications)
            if (!isNotification) {
                const auto& error = response.error();
                json errorResponse = {
                    {"jsonrpc", protocol::JSONRPC_VERSION},
                    {"error", {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                    {"id", request.value("id", nullptr)}};
                transport_->send(errorResponse);
            } else {
                spdlog::debug("Notification error ignored (no response sent)");
            }
        }
    }

    stopThreadPool();
    running_ = false;
    spdlog::info("MCP server stopped");
}

void MCPServer::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }

    stopThreadPool();
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
            // Tool calls are handled asynchronously in the start() method
            return Error{ErrorCode::InvalidOperation, "Tool calls must be made through async message handling"};
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

    // Protocol version negotiation (latest supported: 2024-11-05)
    // If the requested version is unsupported, negotiate to the latest supported version
    // Compose capabilities and negotiate protocol version using latest MCP spec versions
    // Supported (preference order): 2025-06-18, 2025-03-26, 2024-11-05
    std::string requestedVersion = "2025-06-18";
    if (params.contains("protocolVersion") && params["protocolVersion"].is_string()) {
        requestedVersion = params["protocolVersion"].get<std::string>();
    }
    const char* supported[] = {"2025-06-18", "2025-03-26", "2024-11-05"};
    std::string negotiatedVersion = supported[0];
    bool supportedRequested = false;
    for (const auto& v : supported) {
        if (requestedVersion == v) {
            negotiatedVersion = v;
            supportedRequested = true;
            break;
        }
    }
    if (requestedVersion != negotiatedVersion) {
        spdlog::warn("Negotiated protocolVersion: requested='{}' -> using='{}'",
                     requestedVersion, negotiatedVersion);
    }

    // Compose capabilities (minimal, conservative defaults)
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
        if (!store_) {
            return {{"contents", {{{"uri", uri}, {"mimeType", "application/json"}, {"text", json({{"error", "Storage not initialized"}}).dump()}}}}};
        }
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
        if (!metadataRepo_) {
            return {{"contents", {{{"uri", uri}, {"mimeType", "application/json"}, {"text", json({{"error", "Metadata repository not initialized"}}).dump()}}}}};
        }
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
        props["limit"]["default"] = 20;
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
        props["limit"]["default"] = 20;
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

    // For now, return a simple response indicating the tool call was received
    // Full async implementation can be added later when async infrastructure is stable
    return {{"content", json::array({json{{"type", "text"}, {"text", "Tool call received: " + name + " with arguments: " + arguments.dump()}}})}};
}

// Modern C++20 tool handler implementations
yams::Task<Result<MCPSearchResponse>> MCPServer::handleSearchDocuments(const MCPSearchRequest& req) {
    yams::daemon::SearchRequest dreq;
    // Build query with optional path qualifier so daemon-side path filters work via qualifiers.
    std::string _query = req.query;
    if (!req.pathPattern.empty()) {
        if (!_query.empty()) _query += " ";
        _query += "name:" + req.pathPattern;
    }
    dreq.query = _query;
    dreq.limit = req.limit;
    dreq.fuzzy = req.fuzzy;
    dreq.similarity = static_cast<double>(req.similarity);
    // Pass-through hash when present to enable hash-first search
    dreq.hashQuery = req.hash;
    dreq.searchType = req.type;
    dreq.verbose = req.verbose;
    dreq.pathsOnly = req.pathsOnly;
    dreq.showLineNumbers = req.lineNumbers;
    dreq.beforeContext = req.beforeContext;
    dreq.afterContext = req.afterContext;
    dreq.context = req.context;

    MCPSearchResponse out;
    auto res = co_await daemon_client_->streamingSearch(dreq);
    if (!res) co_return res.error();
    const auto& r = res.value();
    out.total = r.totalCount;
    out.type = "daemon";
    out.executionTimeMs = r.elapsed.count();
    for (const auto& item : r.results) {
        MCPSearchResponse::Result m;
        m.id = item.id;
        m.hash = item.metadata.count("hash") ? item.metadata.at("hash") : "";
        m.title = item.title;
        m.path = item.path;
        m.score = item.score;
        m.snippet = item.snippet;
        out.results.push_back(std::move(m));
    }
    co_return out;
}


yams::Task<Result<MCPGrepResponse>> MCPServer::handleGrepDocuments(const MCPGrepRequest& req) {
    yams::daemon::GrepRequest dreq;
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
    if (req.maxCount) dreq.maxMatches = *req.maxCount;

    MCPGrepResponse out;
    auto res = co_await daemon_client_->streamingGrep(dreq);
    if (!res) co_return res.error();
    const auto& r = res.value();
    out.matchCount = r.totalMatches;
    out.fileCount = r.filesSearched;
    std::ostringstream oss;
    for (const auto& m : r.matches) {
        if (out.fileCount > 1 || req.withFilename) oss << m.file << ":";
        if (req.lineNumbers) oss << m.lineNumber << ":";
        oss << m.line << "\n";
    }
    out.output = oss.str();
    co_return out;
}


yams::Task<Result<MCPDownloadResponse>> MCPServer::handleDownload(const MCPDownloadRequest& req) {
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
                try { cfg.defaultConcurrency = std::stoi(f->second); } catch (...) {}
            }
            if (auto f = dl.find("default_chunk_size_bytes"); f != dl.end()) {
                try { cfg.defaultChunkSizeBytes = static_cast<std::size_t>(std::stoull(f->second)); } catch (...) {}
            }
            if (auto f = dl.find("default_timeout_ms"); f != dl.end()) {
                try { cfg.defaultTimeout = std::chrono::milliseconds(std::stoll(f->second)); } catch (...) {}
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
                try { cfg.maxFileBytes = static_cast<std::uint64_t>(std::stoull(f->second)); } catch (...) {}
            }
        }

        // Determine data root (env > core.data_dir > XDG_DATA_HOME > ~/.local/share/yams)
        fs::path dataRoot;
        if (const char* envStorage = std::getenv("YAMS_STORAGE")) {
            if (envStorage && *envStorage) dataRoot = fs::path(envStorage);
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
        if (objectsDir.empty()) objectsDir = dataRoot / "storage" / "objects";
        if (stagingDir.empty()) stagingDir = dataRoot / "storage" / "staging";
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
            co_return Error{ErrorCode::InternalError, std::string("Failed to create staging dir: ") +
                                                       storage.stagingDir.string()};
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
        // Resolve stored path to absolute under YAMS_STORAGE (or standard data dir) if relative
        std::filesystem::path __abs = std::filesystem::path(mcp_response.storedPath);
        if (__abs.is_relative()) {
            std::filesystem::path __base;
            if (const char* envStorage = std::getenv("YAMS_STORAGE"); envStorage && *envStorage) {
                __base = std::filesystem::path(envStorage);
            } else if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME"); xdgDataHome && *xdgDataHome) {
                __base = std::filesystem::path(xdgDataHome) / "yams";
            } else if (const char* homeEnv = std::getenv("HOME"); homeEnv && *homeEnv) {
                __base = std::filesystem::path(homeEnv) / ".local" / "share" / "yams";
            } else {
                __base = std::filesystem::current_path();
            }
            __abs = __base / __abs;
        }
        std::error_code __canon_ec;
        auto __canon = std::filesystem::weakly_canonical(__abs, __canon_ec);
        if (!__canon_ec && !__canon.empty()) {
            __abs = __canon;
        }
        if (verbose) {
            spdlog::debug("[MCP] post-index: resolved stored path: '{}' -> '{}'", mcp_response.storedPath, __abs.string());
        }
        addReq.path = __abs.string(); // normalized absolute path
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

        // Call daemon to add/index the downloaded document
        auto addres = co_await daemon_client_->streamingAddDocument(addReq);
        if (!addres) {
            spdlog::error("[MCP] post-index: daemon add failed for path='{}' error='{}'", addReq.path, addres.error().message);
            mcp_response.indexed = false;
        } else {
            mcp_response.indexed = true;
            const auto& addok = addres.value();
            spdlog::info("[MCP] post-index: indexed path='{}' hash='{}'", addReq.path, addok.hash);
        }
    }

    co_return mcp_response;
}







yams::Task<Result<MCPStoreDocumentResponse>>
MCPServer::handleStoreDocument(const MCPStoreDocumentRequest& req) {
    // Convert MCP request to daemon request
    daemon::AddDocumentRequest daemon_req;
    // Normalize path: expand '~' and make absolute using PWD for relative paths
    {
        std::string _p = req.path;
        if (!_p.empty() && _p.front() == '~') {
            if (const char* home = std::getenv("HOME")) {
                _p = std::string(home) + _p.substr(1);
            }
        }
        if (!_p.empty() && _p.front() != '/') {
            if (const char* pwd = std::getenv("PWD")) {
                if (pwd && *pwd) {
                    if (_p.rfind("./", 0) == 0) {
                        _p = std::string(pwd) + "/" + _p.substr(2);
                    } else {
                        _p = std::string(pwd) + "/" + _p;
                    }
                }
            }
        }
        daemon_req.path = _p;
    }
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
    // Increase daemon timeouts for potentially long single-file adds
    daemon_client_->setHeaderTimeout(std::chrono::seconds(60));
    daemon_client_->setBodyTimeout(std::chrono::seconds(300));
    // Validate that the target file path exists and is not a directory
    if (!daemon_req.path.empty()) {
        std::error_code __ec;
        if (!std::filesystem::exists(daemon_req.path, __ec)) {
            co_return Error{ErrorCode::InvalidArgument,
                            std::string("Path does not exist: ") + daemon_req.path};
        }
        if (std::filesystem::is_directory(daemon_req.path, __ec)) {
            co_return Error{ErrorCode::InvalidArgument,
                            std::string("Path is a directory (use add_directory): ") + daemon_req.path};
        }
    }
    auto dres = co_await daemon_client_->streamingAddDocument(daemon_req);
    if (!dres) co_return dres.error();
    MCPStoreDocumentResponse out;
    const auto& add = dres.value();
    out.hash = add.hash;
    out.bytesStored = 0;
    out.bytesDeduped = 0;
    co_return out;
}


yams::Task<Result<MCPRetrieveDocumentResponse>>
MCPServer::handleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
    // Convert MCP request to daemon request
    daemon::GetRequest daemon_req;
    daemon_req.hash = req.hash;
    daemon_req.outputPath = req.outputPath;
    daemon_req.showGraph = req.graph;
    daemon_req.graphDepth = req.depth;
    daemon_req.metadataOnly = !req.includeContent;

    auto dres = co_await daemon_client_->get(daemon_req);
    if (!dres) co_return dres.error();
    MCPRetrieveDocumentResponse mcp_response;
    const auto& resp = dres.value();
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
        mcp_response.related.push_back(relatedJson);
    }
    co_return mcp_response;
}



yams::Task<Result<MCPListDocumentsResponse>> MCPServer::handleListDocuments(const MCPListDocumentsRequest& req) {
    daemon::ListRequest daemon_req;
    // Map MCP filters to daemon ListRequest
    daemon_req.namePattern = req.pattern;
    daemon_req.tags = req.tags;
    daemon_req.fileType = req.type;
    daemon_req.mimeType = req.mime;
    daemon_req.extensions = req.extension;
    daemon_req.binaryOnly = req.binary;
    daemon_req.textOnly = req.text;
    daemon_req.recentCount = req.recent > 0 ? req.recent : 0;
    daemon_req.limit = req.limit > 0 ? static_cast<size_t>(req.limit) : daemon_req.limit;
    daemon_req.offset = req.offset > 0 ? req.offset : 0;
    daemon_req.sortBy = req.sortBy.empty() ? daemon_req.sortBy : req.sortBy;
    daemon_req.reverse = (req.sortOrder == "asc") ? true : false; // ascending means reverse order in server

    auto dres = co_await daemon_client_->list(daemon_req);
    if (!dres) co_return dres.error();
    MCPListDocumentsResponse out;
    const auto& lr = dres.value();
    out.total = lr.totalCount;
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
        out.documents.push_back(std::move(docJson));
    }
    co_return out;
}

yams::Task<Result<MCPStatsResponse>> MCPServer::handleGetStats(const MCPStatsRequest& req) {
    daemon::GetStatsRequest daemon_req;
    daemon_req.showFileTypes = req.fileTypes;
    daemon_req.detailed = req.verbose;
    auto dres = co_await daemon_client_->getStats(daemon_req);
    if (!dres) co_return dres.error();
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
}

yams::Task<Result<MCPAddDirectoryResponse>> MCPServer::handleAddDirectory(const MCPAddDirectoryRequest& req) {
    daemon::AddDocumentRequest daemon_req;
    // Normalize directory path: expand '~' and make absolute using PWD for relative paths
    {
        std::string _p = req.directoryPath;
        if (!_p.empty() && _p.front() == '~') {
            if (const char* home = std::getenv("HOME")) {
                _p = std::string(home) + _p.substr(1);
            }
        }
        if (!_p.empty() && _p.front() != '/') {
            if (const char* pwd = std::getenv("PWD")) {
                if (pwd && *pwd) {
                    if (_p.rfind("./", 0) == 0) {
                        _p = std::string(pwd) + "/" + _p.substr(2);
                    } else {
                        _p = std::string(pwd) + "/" + _p;
                    }
                }
            }
        }
        daemon_req.path = _p;
    }
    daemon_req.collection = req.collection;
    daemon_req.includePatterns = req.includePatterns;
    daemon_req.excludePatterns = req.excludePatterns;
    daemon_req.recursive = req.recursive;
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) daemon_req.metadata[key] = value.get<std::string>();
        else daemon_req.metadata[key] = value.dump();
    }
    // Increase daemon timeouts for long-running directory indexing
    daemon_client_->setHeaderTimeout(std::chrono::seconds(120));
    daemon_client_->setBodyTimeout(std::chrono::seconds(600));
    // Validate that the directory path exists and is a directory
    if (!daemon_req.path.empty()) {
        std::error_code __ec;
        if (!std::filesystem::exists(daemon_req.path, __ec)) {
            co_return Error{ErrorCode::InvalidArgument,
                            std::string("Directory does not exist: ") + daemon_req.path};
        }
        if (!std::filesystem::is_directory(daemon_req.path, __ec)) {
            co_return Error{ErrorCode::InvalidArgument,
                            std::string("Path is not a directory: ") + daemon_req.path};
        }
    }
    auto dres = co_await daemon_client_->streamingAddDocument(daemon_req);
    if (!dres) co_return dres.error();
    MCPAddDirectoryResponse out;
    const auto& add = dres.value();
    out.directoryPath = add.path;
    out.collection = daemon_req.collection;
    // Populate counts so clients can infer success
    out.filesIndexed = add.documentsAdded;
    out.filesProcessed = add.documentsAdded;
    out.filesSkipped = 0;
    out.filesFailed = 0;
    co_return out;
}

yams::Task<Result<MCPUpdateMetadataResponse>>
MCPServer::handleUpdateMetadata(const MCPUpdateMetadataRequest& req) {
    daemon::UpdateDocumentRequest daemon_req;
    daemon_req.hash = req.hash;
    daemon_req.name = req.name;
    daemon_req.addTags = req.tags;
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) daemon_req.metadata[key] = value.get<std::string>();
        else daemon_req.metadata[key] = value.dump();
    }
    auto dres = co_await daemon_client_->updateDocument(daemon_req);
    if (!dres) co_return dres.error();
    MCPUpdateMetadataResponse out;
    const auto& ur = dres.value();
    out.success = ur.metadataUpdated || ur.tagsUpdated || ur.contentUpdated;
    out.message = out.success ? "Update successful" : "No changes applied";
    co_return out;
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

json MCPServer::createReadyNotification() {
    // Compose a minimal ready notification per MCP 2.0
    json capabilities = {{"tools", json({{"listChanged", false}})},
                         {"prompts", json({{"listChanged", false}})},
                         {"resources", json({{"subscribe", false}, {"listChanged", false}})},
                         {"logging", json::object()}};
    json params = {
        {"protocolVersion", negotiatedProtocolVersion_},
        {"capabilities", capabilities},
        {"serverInfo", {{"name", serverInfo_.name}, {"version", serverInfo_.version}}}
    };
    return json{{"jsonrpc", "2.0"}, {"method", "notifications/ready"}, {"params", params}};
}

// Helper: create a structured MCP logging notification (optional, in-band logging)
static json createLogNotification(const std::string& level, const std::string& message) {
    json params = {{"level", level}, {"message", message}};
    return json{{"jsonrpc", "2.0"}, {"method", "notifications/log"}, {"params", params}};
}

yams::Task<Result<MCPGetByNameResponse>> MCPServer::handleGetByName(const MCPGetByNameRequest& req) {
    // Streamed retrieval via GetInit/GetChunk/GetEnd to mirror CLI robustness
    MCPGetByNameResponse mcp_response;

    daemon::GetInitRequest init{};
    init.name = req.name;
    init.byName = true;
    // raw/extract flags removed from daemon::GetInitRequest; behavior determined server-side

    auto initCall = co_await daemon_client_->getInit(init);
    if (!initCall) {
        co_return initCall.error();
    }
    const auto& initVal = initCall.value();
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

    while (offset < initVal.totalSize) {
        daemon::GetChunkRequest c{};
        c.transferId = initVal.transferId;
        c.offset = offset;
        c.length =
            static_cast<std::uint32_t>(std::min<std::uint64_t>(CHUNK, initVal.totalSize - offset));
        auto cRes = co_await daemon_client_->getChunk(c);
        if (!cRes) {
            // Attempt to close the transfer before returning error
            daemon::GetEndRequest e{};
            e.transferId = initVal.transferId;
            (void)co_await daemon_client_->getEnd(e);
            co_return cRes.error();
        }
        const auto& chunk = cRes.value();
        if (!chunk.data.empty()) {
            if (buffer.size() + chunk.data.size() <= MAX_BYTES) {
                buffer.append(chunk.data.data(), chunk.data.size());
            } else {
                auto remaining = MAX_BYTES - buffer.size();
                buffer.append(chunk.data.data(), remaining);
                offset += chunk.data.size();
                break;
            }
        }
        offset += chunk.data.size();
    }

    // End transfer regardless
    daemon::GetEndRequest end{};
    end.transferId = initVal.transferId;
    (void)co_await daemon_client_->getEnd(end);

    mcp_response.content = std::move(buffer);
    // If truncated, we simply return the capped content; no extra flags in response type.
    co_return mcp_response;
}

yams::Task<Result<MCPDeleteByNameResponse>> MCPServer::handleDeleteByName(const MCPDeleteByNameRequest& req) {
    daemon::DeleteRequest daemon_req;
    daemon_req.name = req.name;
    daemon_req.names = req.names;
    daemon_req.pattern = req.pattern;
    daemon_req.dryRun = req.dryRun;
    auto dres = co_await daemon_client_->remove(daemon_req);
    if (!dres) co_return dres.error();
    MCPDeleteByNameResponse out;
    out.count = 0; // Protocol returns SuccessResponse; detailed per-item results unavailable here
    out.dryRun = req.dryRun;
    co_return out;
}

yams::Task<yams::Result<yams::mcp::MCPCatDocumentResponse>> yams::mcp::MCPServer::handleCatDocument(const yams::mcp::MCPCatDocumentRequest& req) {
    MCPCatDocumentResponse out;
    yams::daemon::GetRequest dreq;
    dreq.hash = req.hash;
    dreq.name = req.name;
    dreq.byName = !req.name.empty();
    dreq.metadataOnly = false;
    auto dres = co_await daemon_client_->get(dreq);
    if (!dres) co_return dres.error();
    const auto& r = dres.value();
    out.size = r.size;
    out.hash = r.hash;
    out.name = r.name;
    if (!r.content.empty()) {
        constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
        out.content = r.content.size() <= MAX_BYTES ? r.content : r.content.substr(0, MAX_BYTES);
    }
    co_return out;
}

// Implementation of collection restore
yams::Task<yams::Result<yams::mcp::MCPRestoreCollectionResponse>>
yams::mcp::MCPServer::handleRestoreCollection(const yams::mcp::MCPRestoreCollectionRequest& req) {
    try {
        if (!metadataRepo_) {
            co_return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        if (!store_) {
            co_return Error{ErrorCode::NotInitialized, "Content store not initialized"};
        }

        if (req.collection.empty()) {
            co_return Error{ErrorCode::InvalidArgument, "Collection name is required"};
        }

        spdlog::debug("MCP handleRestoreCollection: restoring collection '{}'", req.collection);

        // Get documents from collection
        auto docsResult = metadataRepo_->findDocumentsByCollection(req.collection);
        if (!docsResult) {
            co_return Error{ErrorCode::InternalError,
                         "Failed to find collection documents: " + docsResult.error().message};
        }

        const auto& documents = docsResult.value();
        if (documents.empty()) {
            MCPRestoreCollectionResponse response;
            response.filesRestored = 0;
            response.dryRun = req.dryRun;
            spdlog::info("MCP handleRestoreCollection: no documents found in collection '{}'",
                         req.collection);
            co_return response;
        }

        MCPRestoreCollectionResponse response;
        response.dryRun = req.dryRun;

        // Create output directory if needed
        std::filesystem::path outputDir(req.outputDirectory);
        if (!req.dryRun && req.createDirs) {
            std::error_code ec;
            std::filesystem::create_directories(outputDir, ec);
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

        co_return response;
    } catch (const std::exception& e) {
        spdlog::error("MCP handleRestoreCollection exception: {}", e.what());
        co_return Error{ErrorCode::InternalError,
                     std::string("Restore collection failed: ") + e.what()};
    }
}

yams::Task<yams::Result<yams::mcp::MCPRestoreSnapshotResponse>>
yams::mcp::MCPServer::handleRestoreSnapshot(const yams::mcp::MCPRestoreSnapshotRequest& req) {
    co_return Error{ErrorCode::NotImplemented, "Restore snapshot not yet implemented"};
}

yams::Task<Result<MCPListCollectionsResponse>>
MCPServer::handleListCollections(const MCPListCollectionsRequest& req) {
    MCPListCollectionsResponse response;
    // TODO: Implement collection listing
    co_return response;
}

yams::Task<Result<MCPListSnapshotsResponse>>
MCPServer::handleListSnapshots(const MCPListSnapshotsRequest& req) {
    MCPListSnapshotsResponse response;
    // TODO: Implement snapshot listing
    co_return response;
}

// === Thread pool implementation for MCPServer ===
void MCPServer::startThreadPool(std::size_t threads) {
    stopWorkers_.store(false);
    for (std::size_t i = 0; i < threads; ++i) {
        workerPool_.emplace_back([this]() {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lk(taskMutex_);
                    taskCv_.wait(lk, [this]() {
                        return stopWorkers_.load() || !taskQueue_.empty();
                    });
                    if (stopWorkers_.load() && taskQueue_.empty()) {
                        return;
                    }
                    task = std::move(taskQueue_.front());
                    taskQueue_.pop_front();
                }
                try {
                    task();
                } catch (...) {
                    // Swallow to keep worker alive
                }
            }
        });
    }
}

void MCPServer::stopThreadPool() {
    {
        std::lock_guard<std::mutex> lk(taskMutex_);
        stopWorkers_.store(true);
    }
    taskCv_.notify_all();
    for (auto& t : workerPool_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workerPool_.clear();
    // Clear any remaining tasks
    {
        std::lock_guard<std::mutex> lk(taskMutex_);
        taskQueue_.clear();
    }
}

void MCPServer::enqueueTask(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lk(taskMutex_);
        taskQueue_.push_back(std::move(task));
    }
    taskCv_.notify_one();
}

} // namespace yams::mcp
