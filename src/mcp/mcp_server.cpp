#include <boost/asio/local/stream_protocol.hpp>
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/daemon_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/core/task.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/socket_utils.h>
#include <yams/downloader/downloader.hpp>
#include <yams/mcp/error_handling.h>
#include <yams/mcp/mcp_server.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/query_helpers.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>

#include <future>
#include <iomanip>
#include <mutex>
#include <thread>

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <csignal>
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
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <unordered_set>

// Platform-specific includes for non-blocking I/O
#ifdef _WIN32
#include <conio.h>
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <poll.h>
#include <unistd.h>
#endif

namespace yams::mcp {

// Stdio send helper: sends JSON-RPC messages via stdio transport
void MCPServer::sendResponse(const nlohmann::json& message) {
    spdlog::debug("MCP server sending response: {}", message.dump());

    // Serialize once for both telemetry and transport
    std::string payload;
    try {
        payload = message.dump();
    } catch (const std::exception& e) {
        spdlog::error("sendResponse: serialization failed: {}", e.what());
        return;
    }
    telemetrySentBytes_.fetch_add(static_cast<uint64_t>(payload.size()));
    if (payload.find("\"jsonrpc\":null") != std::string::npos ||
        payload.find("\"result\":null") != std::string::npos) {
        spdlog::error("MCP CORRUPTION SUSPECTED BEFORE SEND: {}", payload);
        telemetryIntegrityFailures_.fetch_add(1);
    }

    // MCP stdio spec: always output NDJSON (newline-delimited JSON)
    if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdio->send(message);
    } else {
        spdlog::error("sendResponse: transport is not stdio, cannot send");
    }
}

// StdioTransport implementation
StdioTransport::StdioTransport() {
    // Ensure predictable stdio behavior. In tests (env YAMS_TESTING=1), avoid changing
    // global iostream configuration so rdbuf redirection in unit tests works.
    bool testing_env = false;
    if (const char* t = std::getenv("YAMS_TESTING"))
        testing_env = (*t != '\0' && *t != '0');
#ifndef YAMS_TESTING
    if (!testing_env) {
        std::ios::sync_with_stdio(false);
        std::cin.tie(nullptr);
    }
#endif

    // Set stdin/stdout to binary mode on Windows to prevent CRLF translation
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif

    // Configure stdout buffering: line buffering for MCP (flush on newline) only outside tests
    if (!testing_env) {
        std::cout.setf(std::ios::unitbuf);
        std::cerr.setf(std::ios::unitbuf);
    }

    // Configure receive timeout from environment, enforce a sane minimum
    if (const char* env = std::getenv("YAMS_MCP_RECV_TIMEOUT_MS"); env && *env) {
        try {
            recvTimeoutMs_ = std::stoi(env);
            if (recvTimeoutMs_ < 50)
                recvTimeoutMs_ = 50;
        } catch (...) {
            // ignore and keep default
        }
    }
    state_.store(TransportState::Connected);
}

StdioTransport::~StdioTransport() {
    state_.store(TransportState::Closing);
}

void StdioTransport::send(const json& message) {
    if (state_.load() != TransportState::Connected) {
        return;
    }

    try {
        std::lock_guard<std::mutex> lock(outMutex_);
        // MCP stdio spec: newline-delimited JSON (NDJSON)
        // Messages are delimited by newlines and MUST NOT contain embedded newlines
        std::cout << message.dump() << '\n';
        std::cout.flush();
    } catch (const std::exception& e) {
        spdlog::error("StdioTransport::send failed: {}", e.what());
        recordError();
    }
}

void StdioTransport::sendNdjson(const json& message) {
    // For stdio transport, sendNdjson() is identical to send()
    // Both output MCP spec-compliant NDJSON
    send(message);
}

// Send helper for pre-serialized JSON payloads (outputs NDJSON per MCP spec)
void StdioTransport::sendFramedSerialized(const std::string& payload) {
    if (state_.load() != TransportState::Connected) {
        return;
    }

    try {
        std::lock_guard<std::mutex> lock(outMutex_);
        // MCP stdio spec: newline-delimited JSON
        std::cout << payload << '\n';
        std::cout.flush();
    } catch (const std::exception& e) {
        spdlog::error("StdioTransport::sendFramedSerialized failed: {}", e.what());
        recordError();
    }
}

bool StdioTransport::isInputAvailable(int timeoutMs) const {
#ifdef _WIN32
    // Windows: Prefer PeekNamedPipe for pipes, fallback to console wait
    HANDLE stdinHandle = GetStdHandle(STD_INPUT_HANDLE);
    if (stdinHandle == INVALID_HANDLE_VALUE || stdinHandle == nullptr) {
        return false;
    }
    DWORD fileType = GetFileType(stdinHandle);
    if (fileType == FILE_TYPE_PIPE) {
        DWORD bytesAvail = 0;
        if (PeekNamedPipe(stdinHandle, nullptr, 0, nullptr, &bytesAvail, nullptr)) {
            if (bytesAvail > 0)
                return true;
            // Simple timed wait: sleep for the timeout and check again once
            if (timeoutMs > 0) {
                Sleep(static_cast<DWORD>(timeoutMs));
                bytesAvail = 0;
                if (PeekNamedPipe(stdinHandle, nullptr, 0, nullptr, &bytesAvail, nullptr)) {
                    return bytesAvail > 0;
                }
            }
            return false;
        }
        // If PeekNamedPipe fails, fall back to a short sleep
        if (timeoutMs > 0)
            Sleep(static_cast<DWORD>(timeoutMs));
        return false;
    }
    // Console input or unknown: WaitForSingleObject is acceptable
    DWORD waitResult = WaitForSingleObject(stdinHandle, static_cast<DWORD>(timeoutMs));
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
    if (state_.load() != TransportState::Connected) {
        return Error{ErrorCode::NetworkError, "Transport not connected"};
    }

    if (externalShutdown_ && *externalShutdown_) {
        state_.store(TransportState::Closing);
        return Error{ErrorCode::NetworkError, "External shutdown requested"};
    }

    std::string line;
    // Loop to handle timeouts without exiting, which is required for clients like Jan
    // that keep the MCP server running idly.
    while (!readLineWithTimeout(line, recvTimeoutMs_)) {
        if (std::cin.eof()) {
            spdlog::info("StdioTransport: EOF on stdin; client disconnected");
            state_.store(TransportState::Disconnected);
            return Error{ErrorCode::NetworkError, "EOF on stdin"};
        }
        if (externalShutdown_ && *externalShutdown_) {
            state_.store(TransportState::Closing);
            return Error{ErrorCode::NetworkError, "External shutdown requested"};
        }
        // It was a timeout, so we loop again.
    }

    // Trim trailing CR for CRLF line endings
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }

    // Trim leading whitespace
    line.erase(line.begin(), std::find_if(line.begin(), line.end(),
                                          [](unsigned char ch) { return !std::isspace(ch); }));

    if (line.empty()) {
        // Ignore empty lines and wait for the next message
        return Error{ErrorCode::NetworkError, "Empty line received"};
    }

    spdlog::debug("StdioTransport: Received line: '{}'",
                  line.length() > 200 ? line.substr(0, 200) + "..." : line);

    // Support both NDJSON (MCP stdio) and LSP-style Content-Length framing on input.
    // If a Content-Length header is detected, consume headers and read the next line as body.
    {
        std::string lower = line;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (lower.rfind("content-length:", 0) == 0) {
            // Parse length after ':' (whitespace tolerated)
            std::size_t colon = line.find(':');
            int contentLength = 0;
            if (colon != std::string::npos) {
                try {
                    contentLength = std::stoi(line.substr(colon + 1));
                } catch (...) {
                    // ignore length parse errors; fall back to parsing next line
                }
            }
            // Read header lines until blank line
            std::string headerLine;
            while (readLineWithTimeout(headerLine, recvTimeoutMs_)) {
                if (!headerLine.empty() && headerLine.back() == '\r')
                    headerLine.pop_back();
                if (headerLine.empty())
                    break; // end of headers
            }
            // Read body line (most clients send compact single-line JSON)
            std::string bodyLine;
            if (readLineWithTimeout(bodyLine, recvTimeoutMs_)) {
                if (!bodyLine.empty() && bodyLine.back() == '\r')
                    bodyLine.pop_back();
                line = std::move(bodyLine);
                spdlog::debug("StdioTransport: Consumed LSP-framed body ({} bytes announced)",
                              contentLength);
            }
        }
    }

    // Parse JSON payload (expects a complete JSON value on a single line)
    auto parsed = json_utils::parse_json(line);
    if (parsed) {
        resetErrorCount();
        return parsed.value();
    }

    // JSON parsing failed.
    recordError();
    if (!shouldRetryAfterError()) {
        state_.store(TransportState::Error);
    }
    spdlog::error("Failed to parse MCP message as JSON: {}", parsed.error().message);
    return parsed.error();
}

// Helper: Read a line with timeout support
bool StdioTransport::readLineWithTimeout(std::string& line, int timeoutMs) const {
    // For testing with stringbuf-backed streams
    std::streambuf* inputBuffer = std::cin.rdbuf();
    auto* stringBuffer = dynamic_cast<std::stringbuf*>(inputBuffer);

    if (stringBuffer) {
        // Test mode: non-blocking check
        if (stringBuffer->in_avail() <= 0) {
            return false;
        }
        return static_cast<bool>(std::getline(std::cin, line));
    }

    // Production mode: wait for input with timeout
    if (!isInputAvailable(timeoutMs)) {
        return false;
    }

    return static_cast<bool>(std::getline(std::cin, line));
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
MCPServer::MCPServer(std::unique_ptr<ITransport> transport, std::atomic<bool>* externalShutdown,
                     std::filesystem::path overrideSocket,
                     std::optional<boost::asio::any_io_executor> executor)
    : transport_(std::move(transport)), externalShutdown_(externalShutdown),
      eagerReadyEnabled_(false), autoReadyEnabled_(false), strictProtocol_(false),
      limitToolResultDup_(false), daemonSocketOverride_(std::move(overrideSocket)) {
    (void)executor; // Unused in stdio-only mode
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

    // Initialize daemon client configuration (no pre-connection or pooling)
    // MCP stdio uses single-use connections for maximum reliability and simplicity
    {
        yams::daemon::ClientConfig cfg;
        if (!daemonSocketOverride_.empty()) {
            cfg.socketPath = daemonSocketOverride_;
        } else {
            cfg.socketPath = yams::daemon::socket_utils::resolve_socket_path_config_first();
        }
        cfg.enableChunkedResponses = true;
        cfg.singleUseConnections = true;
        cfg.requestTimeout = std::chrono::milliseconds(60000);
        // Use generous timeouts to handle large directory adds and busy daemon
        cfg.headerTimeout = std::chrono::milliseconds(120000); // 2 minutes
        cfg.bodyTimeout = std::chrono::milliseconds(120000);   // 2 minutes
        // Use GlobalIOContext executor for async operations
        cfg.executor = yams::daemon::GlobalIOContext::global_executor();

        if (const char* mi = std::getenv("YAMS_MCP_MAX_INFLIGHT")) {
            long v = std::strtol(mi, nullptr, 10);
            if (v > 0)
                cfg.maxInflight = static_cast<size_t>(v);
        } else {
            cfg.maxInflight = 128;
        }

        // Align dataDir resolution with CLI helpers if provided via env
        if (const char* ds = std::getenv("YAMS_STORAGE")) {
            if (*ds)
                cfg.dataDir = ds;
        }
        if (cfg.dataDir.empty()) {
            if (const char* dd = std::getenv("YAMS_DATA_DIR")) {
                if (*dd)
                    cfg.dataDir = dd;
            }
        }

        cfg.autoStart = false; // MCP server should not start the daemon
        daemon_client_config_ = cfg;

        // No pre-connection - clients created on-demand per request
    }
    // Legacy pool config removed

    // Initialize the tool registry with modern handlers
    initializeToolRegistry();

    // Set default handshake behavior from environment variables
    enableYamsExtensions_ = true;
    if (const char* env = std::getenv("YAMS_DISABLE_EXTENSIONS")) {
        enableYamsExtensions_ = !(std::string(env) == "1" || std::string(env) == "true");
    }
    // Environment variable support for handshake behavior
    if (const char* env = std::getenv("YAMS_MCP_EAGER_READY")) {
        eagerReadyEnabled_ = (std::string(env) == "1" || std::string(env) == "true");
    }
    if (const char* env = std::getenv("YAMS_MCP_AUTO_READY")) {
        autoReadyEnabled_ = !(std::string(env) == "0" || std::string(env) == "false");
    }
    if (const char* env = std::getenv("YAMS_MCP_STRICT_PROTOCOL")) {
        strictProtocol_ = (std::string(env) == "1" || std::string(env) == "true");
    }
    if (const char* env = std::getenv("YAMS_MCP_STRICT_LIFECYCLE")) {
        strictLifecycle_ = (std::string(env) == "1" || std::string(env) == "true");
    }
    if (const char* env = std::getenv("YAMS_MCP_HANDSHAKE_TRACE")) {
        handshakeTrace_ = (std::string(env) == "1" || std::string(env) == "true");
    }
    if (const char* env = std::getenv("YAMS_MCP_READY_DELAY_MS")) {
        try {
            autoReadyDelayMs_ = std::stoi(env);
            if (autoReadyDelayMs_ < 20)
                autoReadyDelayMs_ = 20;
        } catch (...) {
            autoReadyDelayMs_ = 100;
        }
    }

    if (const char* env = std::getenv("YAMS_MCP_LIMIT_DUP_CONTENT")) {
        limitToolResultDup_ = !(std::string(env) == "0" || std::string(env) == "false");
    }

    try {
        // Highest priority: explicit env override
        if (const char* env = std::getenv("YAMS_MCP_PROMPTS_DIR"); env && *env) {
            promptsDir_ = std::filesystem::path(env);
        }
        // Next: config.toml [mcp_server].prompts_dir
        if (promptsDir_.empty()) {
            std::map<std::string, std::map<std::string, std::string>> toml;
            std::filesystem::path configPath;
            if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
                configPath = std::filesystem::path(xdgConfigHome) / "yams" / "config.toml";
            } else if (const char* homeEnv = std::getenv("HOME")) {
                configPath = std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml";
            }
            if (!configPath.empty() && std::filesystem::exists(configPath)) {
                yams::config::ConfigMigrator migrator;
                if (auto parsed = migrator.parseTomlConfig(configPath)) {
                    toml = std::move(parsed.value());
                }
            }
            if (auto it = toml.find("mcp_server"); it != toml.end()) {
                const auto& mcp = it->second;
                if (auto f = mcp.find("prompts_dir"); f != mcp.end() && !f->second.empty()) {
                    std::string p = f->second;
                    if (!p.empty() && p.front() == '~') {
                        if (const char* home = std::getenv("HOME")) {
                            p = std::string(home) + p.substr(1);
                        }
                    }
                    promptsDir_ = std::filesystem::path(p);
                }
            }
        }
        // Next: XDG_DATA_HOME/yams/prompts or ~/.local/share/yams/prompts
        if (promptsDir_.empty()) {
            std::filesystem::path base;
            if (const char* xdgData = std::getenv("XDG_DATA_HOME")) {
                base = std::filesystem::path(xdgData);
            } else if (const char* home = std::getenv("HOME")) {
                base = std::filesystem::path(home) / ".local" / "share";
            }
            if (!base.empty()) {
                auto p = base / "yams" / "prompts";
                promptsDir_ = p;
            }
        }
        // Last: local docs/prompts (useful for dev runs from the repo root)
        if (!std::filesystem::exists(promptsDir_)) {
            auto localDocs = std::filesystem::current_path() / "docs" / "prompts";
            if (std::filesystem::exists(localDocs)) {
                promptsDir_ = localDocs;
            }
        }
        if (!promptsDir_.empty()) {
            spdlog::info("MCP prompts directory resolved to: {}", promptsDir_.string());
        }
    } catch (...) {
        // Ignore prompt dir resolution errors; built-ins remain available
    }

    // Size worker pool: prefer TuneAdvisor setting, else ~25% of cores clamped [2..8]
    try {
        using TA = yams::daemon::TuneAdvisor;
        uint32_t taThreads = 0;
        try {
            taThreads = TA::mcpWorkerThreads();
        } catch (...) {
        }
        unsigned hw = std::thread::hardware_concurrency();
        std::size_t threads = taThreads ? static_cast<std::size_t>(taThreads)
                                        : std::clamp<std::size_t>(hw ? (hw / 4u) : 2u, 2u, 8u);
        startThreadPool(threads);
    } catch (...) {
        startThreadPool(4);
    }
}

Result<void> MCPServer::ensureDaemonClient() {
    std::lock_guard<std::mutex> lock(daemon_client_mutex_);

    if (testEnsureDaemonClientHook_) {
        auto hookResult = testEnsureDaemonClientHook_(daemon_client_config_);
        if (!hookResult) {
            return hookResult;
        }
    }

    // Always create a fresh client for each request (single-use)
    // This is the simplest, most reliable approach for MCP stdio
    try {
        daemon_client_unique_ = std::make_unique<yams::daemon::DaemonClient>(daemon_client_config_);
        daemon_client_ = daemon_client_unique_.get();
        return Result<void>();
    } catch (const std::exception& e) {
        std::string hint = std::string("Failed to create daemon client: ") + e.what();
        try {
            hint += std::string(" (socket: '") + daemon_client_config_.socketPath.string() + "')";
        } catch (...) {
        }
        return Error{ErrorCode::NetworkError, hint};
    }
}

MCPServer::~MCPServer() {
    stop();
    shutdown();
}

void MCPServer::shutdown(std::chrono::milliseconds timeout) {
    // Stdio transport shutdown - nothing async to wait for
    (void)timeout; // Unused in stdio-only mode
}

void MCPServer::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }

    // Ensure stdout is not buffered for real-time communication (not in tests)
    bool testing_env = false;
    if (const char* t = std::getenv("YAMS_TESTING"))
        testing_env = (*t != '\0' && *t != '0');
    if (!testing_env) {
        std::cout.setf(std::ios::unitbuf);
    }
#ifndef _WIN32
    // Prevent abrupt termination on first write if the client side is not yet reading
    std::signal(SIGPIPE, SIG_IGN);
#endif

    spdlog::info("MCP server started");

    // Main message loop with modern error handling
    try {
        while (running_ && (!externalShutdown_ || !*externalShutdown_)) {
            auto messageResult = transport_->receive();

            if (!messageResult) {
                const auto& error = messageResult.error();

                // Handle different error types
                switch (error.code) {
                    case ErrorCode::NetworkError:
                        spdlog::debug("Transport closed: {}", error.message);
                        running_ = false;
                        break;

                    case ErrorCode::InvalidData:
                        spdlog::debug("Invalid JSON received: {}", error.message);
                        // Per JSON-RPC 2.0, respond with Parse error and id=null
                        sendResponse(
                            createError(json(nullptr), protocol::PARSE_ERROR, error.message));
                        // Continue processing - client may send valid messages
                        continue;

                    default:
                        spdlog::error("Unexpected transport error: {}", error.message);
                        continue;
                }
                continue;
            }

            // Process valid message (object or array batch)
            auto message = messageResult.value();
            auto processRequest = [this](const json& request) {
                if (!request.is_object()) {
                    spdlog::warn("MCP server received non-object entry in JSON-RPC batch");
                    this->sendResponse(createError(json(nullptr), protocol::INVALID_REQUEST,
                                                   "Batch entries must be JSON objects"));
                    return;
                }

                spdlog::debug("MCP server received message: {}", request.dump());
                if (this->handshakeTrace_) {
                    try {
                        std::string meth = request.value("method", "");
                        std::string id = request.contains("id") ? request["id"].dump() : "null";
                    } catch (...) {
                    }
                }

                const bool isNotification = !request.contains("id");
                auto id_val = request.value("id", json{});
                if (!isNotification && this->isCanceled(id_val)) {
                    spdlog::debug("Dropping response for cancelled request id={}", id_val.dump());
                    return;
                }

                std::string method = request.value("method", "");
                json params = request.value("params", json::object());

                if (isNotification) {
                    (void)this->handleRequest(request);
                    return;
                }

                // tools/call is handled by enqueueTask like other requests in stdio mode

                this->enqueueTask([this, req = request]() mutable {
                    if (auto response = this->handleRequest(req)) {
                        this->sendResponse(response.value());
                    } else {
                        const auto& error = response.error();
                        json errorResponse = {
                            {"jsonrpc", protocol::JSONRPC_VERSION},
                            {"error",
                             {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                            {"id", req.value("id", nullptr)}};
                        this->sendResponse(errorResponse);
                    }
                });
            };

            if (message.is_array()) {
                for (const auto& entry : message) {
                    processRequest(entry);
                }
            } else {
                processRequest(message);
            }
        }
    } catch (const std::exception& e) {
        spdlog::critical("Fatal exception in MCPServer::start: {}", e.what());
    } catch (...) {
        spdlog::critical("Fatal unknown exception in MCPServer::start");
    }

    running_ = false;
    spdlog::info("MCP server stopped");
}

// Async stdio-driven loop removed; startAsync delegates to start()
boost::asio::awaitable<void> MCPServer::startAsync() {
    start();
    co_return;
}

void MCPServer::stop() {
    running_.store(false);

    if (transport_) {
        // Always close the transport first so any in-flight worker tasks can
        // observe shutdown and finish promptly before we join the pool.
        transport_->close();
    }

    stopThreadPool();
}

MessageResult MCPServer::handleRequest(const json& request) {
    auto id = request.value("id", json{});
    registerCancelable(id);
    try {
        // Extract method and params
        std::string method = request.value("method", "");
        json params = request.value("params", json::object());
        auto id2 = request.value("id", json{});

        // Route to appropriate handler
        if (method == "initialize") {
            spdlog::debug("MCP handling initialize request with params: {}", params.dump());
            auto initResult = initialize(params);

            if (initResult.contains("_initialize_error")) {
                spdlog::error("MCP initialize failed with error");
                return json{{"jsonrpc", "2.0"},
                            {"id", id},
                            {"error",
                             {{"code", initResult["code"]},
                              {"message", initResult["message"]},
                              {"data", initResult["data"]}}}};
            }
            spdlog::debug("MCP initialize successful, protocol version: {}",
                          initResult.value("protocolVersion", "unknown"));

            auto response = createResponse(id2, initResult);

            return response;
        } else if (method == "notifications/cancelled") {
            // MCP cancellation notification; tolerate either 'id' or 'requestId'
            nlohmann::json cancelId;
            if (params.contains("id")) {
                cancelId = params["id"];
            } else if (params.contains("requestId")) {
                cancelId = params["requestId"];
            } else {
                spdlog::warn("notifications/cancelled missing id/requestId");
                return Error{ErrorCode::InvalidArgument, "Missing id/requestId"};
            }
            cancelRequest(cancelId);
            // Do not send a response for notifications
            return Error{ErrorCode::Success, "notification"}; // notification: no response sent
        } else if (method == "notifications/initialized") {
            // MCP client signals readiness; no response required
            markClientInitialized();
            return Error{ErrorCode::Success, "notification"};
        } else if (method == "ping") {
            return createResponse(id2, json::object());
        } else if (method == "shutdown") {
            // Per LSP spec: prepare for exit but don't actually exit yet
            // Just acknowledge the shutdown request
            spdlog::debug("Shutdown request received, preparing for exit");
            shutdownRequested_ = true;
            lifecycleState_.store(McpLifecycleState::ShuttingDown);
            return createResponse(id2, json::object());
        } else if (method == "exit") {
            // Per LSP spec: exit only after shutdown was requested
            spdlog::debug("Exit request received");
            if (externalShutdown_)
                *externalShutdown_ = true;
            running_ = false;
            lifecycleState_.store(McpLifecycleState::Disconnected);
            // Exit is a notification (no response expected)
            return Error{ErrorCode::Success, "notification"};
        } else if (method == "tools/list") {
            if (strictLifecycle_ && !initializedNotificationSeen_.load()) {
                spdlog::warn("MCP: tools/list before initialized notification (strict mode)");
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", id},
                    {"error",
                     {{"code", -32002},
                      {"message", "Server not ready. Send notifications/initialized first."},
                      {"data", {{"phase", "awaiting_initialized"}}}}}};
            }
            recordEarlyFeatureUse();
            return createResponse(id2, listTools());
        } else if (method == "tools/call") {
            if (strictLifecycle_ && !initializedNotificationSeen_.load()) {
                spdlog::warn("MCP: tools/call before initialized notification (strict mode)");
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", id},
                    {"error",
                     {{"code", -32002},
                      {"message", "Server not ready. Send notifications/initialized first."},
                      {"data", {{"phase", "awaiting_initialized"}}}}}};
            }
            recordEarlyFeatureUse();
            const auto toolName = params.value("name", "");
            const auto toolArgs = params.value("arguments", json::object());
            // Emit a coarse progress notification for visibility
            sendProgress("tool", 0.0, std::string("calling ") + toolName);
            json raw = callTool(toolName, toolArgs);

            // If the tool returned a JSON-RPC style error object, propagate as error response
            if (raw.is_object() && raw.contains("error")) {
                json err = raw["error"];
                return json{{"jsonrpc", protocol::JSONRPC_VERSION}, {"error", err}, {"id", id}};
            }
            // Otherwise, treat as successful tool result (typically content array)
            sendProgress("tool", 100.0, std::string("completed ") + toolName);
            return createResponse(id, raw);
        } else if (method == "resources/list") {
            if (strictLifecycle_ && !initializedNotificationSeen_.load()) {
                spdlog::warn("MCP: resources/list before initialized notification (strict mode)");
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", id},
                    {"error",
                     {{"code", -32002},
                      {"message", "Server not ready. Send notifications/initialized first."},
                      {"data", {{"phase", "awaiting_initialized"}}}}}};
            }
            return createResponse(id, listResources());
        } else if (method == "resources/read") {
            if (strictLifecycle_ && !initializedNotificationSeen_.load()) {
                spdlog::warn("MCP: resources/read before initialized notification (strict mode)");
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", id},
                    {"error",
                     {{"code", -32002},
                      {"message", "Server not ready. Send notifications/initialized first."},
                      {"data", {{"phase", "awaiting_initialized"}}}}}};
            }
            std::string uri = params.value("uri", "");
            return createResponse(id, readResource(uri));
        } else if (method == "prompts/list") {
            if (strictLifecycle_ && !initializedNotificationSeen_.load()) {
                spdlog::warn("MCP: prompts/list before initialized notification (strict mode)");
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", id},
                    {"error",
                     {{"code", -32002},
                      {"message", "Server not ready. Send notifications/initialized first."},
                      {"data", {{"phase", "awaiting_initialized"}}}}}};
            }
            return createResponse(id, listPrompts());
        } else if (method == "prompts/get") {
            if (strictLifecycle_ && !initializedNotificationSeen_.load()) {
                spdlog::warn("MCP: prompts/get before initialized notification (strict mode)");
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", id},
                    {"error",
                     {{"code", -32002},
                      {"message", "Server not ready. Send notifications/initialized first."},
                      {"data", {{"phase", "awaiting_initialized"}}}}}};
            }
            std::string name = params.value("name", "");
            json args = params.value("arguments", json::object());

            auto textContent = [](const std::string& t) {
                return json{{"type", "text"}, {"text", t}};
            };
            auto assistantMsg = [&](const std::string& t) {
                return json{{"role", "assistant"}, {"content", textContent(t)}};
            };
            auto userMsg = [&](const std::string& t) {
                return json{{"role", "user"}, {"content", textContent(t)}};
            };

            auto makeResponse = [&](std::vector<json> messages) {
                return createResponse(id, json{{"messages", std::move(messages)}});
            };

            if (name == "search_codebase") {
                const std::string pattern = args.value("pattern", "");
                const std::string fileType = args.value("file_type", "");
                std::string u =
                    "Goal: find occurrences in the codebase and propose next steps.\n"
                    "- Prefer tools/grep for regex/precise matches with contexts.\n"
                    "- Prefer tools/search for fuzzy/hybrid discovery across names+content.\n"
                    "- Respect session scoping by default (server may scope by session).\n"
                    "- When using grep, include line numbers and filenames.\n"
                    "- When using search, return names/paths and snippets.\n";
                if (!pattern.empty()) {
                    u += "\nRequested pattern: " + pattern + "\n";
                }
                if (!fileType.empty()) {
                    u += "File type filter hint: " + fileType + "\n";
                }
                return makeResponse(
                    {assistantMsg(
                         "You are a code navigator that selects between grep and hybrid search."),
                     userMsg(u)});
            }

            if (name == "summarize_document") {
                const std::string docName = args.value("document_name", "");
                const int maxLen = args.value("max_length", 200);
                std::string u =
                    "Task: summarize a single document retrieved from the knowledge store.\n"
                    "Steps:\n"
                    "1) Retrieve by name via tools/get_by_name (latest=true) or by hash via "
                    "tools/get.\n"
                    "2) Produce a concise, faithful summary within " +
                    std::to_string(maxLen) +
                    " words.\n"
                    "3) Include citation using the document name and/or hash.\n";
                if (!docName.empty()) {
                    u += "\nDocument name: " + docName + "\n";
                }
                return makeResponse(
                    {assistantMsg("You are a precise summarizer. Cite sources by name/hash."),
                     userMsg(u)});
            }

            if (name == "rag/rewrite_query") {
                const std::string query = args.value("query", "");
                const std::string intent = args.value("intent", "");
                std::string u = "Rewrite the user query to optimize retrieval (hybrid search: text "
                                "+ metadata).\n"
                                "Guidelines:\n"
                                "- Expand meaningful synonyms; preserve critical terms.\n"
                                "- Add lightweight qualifiers (e.g., name:*.md, tag:pinned) only "
                                "if clearly helpful.\n"
                                "- Keep it concise and robust to typos.\n";
                if (!query.empty()) {
                    u += "\nOriginal query: " + query + "\n";
                }
                if (!intent.empty()) {
                    u += "Intent hint: " + intent + "\n";
                }
                return makeResponse(
                    {assistantMsg("You are a retrieval query rewriter for hybrid search."),
                     userMsg(u)});
            }

            if (name == "rag/retrieve") {
                const std::string query = args.value("query", "");
                const int k = std::max(1, args.value("k", 10));
                const std::string session = args.value("session", "");
                std::string u =
                    "Retrieve top-" + std::to_string(k) +
                    " relevant items using tools/search.\n"
                    "Requirements:\n"
                    "- Use fuzzy=true and type=hybrid (server defaults may already do this).\n"
                    "- Prefer session scoping if available; include name/path/hash/score/snippet.\n"
                    "- Return a compact list suitable for follow-up fetches via tools/cat or "
                    "tools/get.\n";
                if (!query.empty()) {
                    u += "\nQuery: " + query + "\n";
                }
                if (!session.empty()) {
                    u += "Scope session: " + session + "\n";
                }
                return makeResponse(
                    {assistantMsg(
                         "You are a retrieval runner that returns compact, citeable candidates."),
                     userMsg(u)});
            }

            if (name == "rag/retrieve_summarize") {
                const std::string query = args.value("query", "");
                const int k = std::max(1, args.value("k", 5));
                const int maxWords = std::max(50, args.value("max_words", 250));
                std::string u = "RAG pipeline: retrieve then summarize.\n"
                                "Steps:\n"
                                "1) tools/search with query (hybrid/fuzzy), top-" +
                                std::to_string(k) +
                                ".\n"
                                "2) For each candidate, fetch content via tools/cat or tools/get "
                                "(by hash/name).\n"
                                "3) Synthesize a grounded summary in <= " +
                                std::to_string(maxWords) +
                                " words.\n"
                                "4) Include inline citations like (name | hash:abcd...).\n"
                                "5) Prefer diverse sources and de-dup near-identical results.\n";
                if (!query.empty()) {
                    u += "\nQuery: " + query + "\n";
                }
                return makeResponse(
                    {assistantMsg(
                         "You orchestrate retrieval and grounded summarization with citations."),
                     userMsg(u)});
            }

            if (name == "rag/extract_citations") {
                const std::string style = args.value("style", "inline");
                const int k = std::max(1, args.value("k", 10));
                std::string u =
                    "From prior retrieval results, produce " + std::to_string(k) +
                    " citations in style '" + style +
                    "'.\n"
                    "Include: name, hash (short), path (if available), and date/labels if known.\n";
                return makeResponse(
                    {assistantMsg("You format high-quality citations for retrieved artifacts."),
                     userMsg(u)});
            }

            if (name == "rag/code_navigation") {
                const std::string symbol = args.value("symbol", "");
                const std::string lang = args.value("language", "");
                std::string u =
                    "Plan code navigation using grep+search:\n"
                    "- Suggest regexes for tools/grep (e.g., function/class definitions; case "
                    "sensitivity).\n"
                    "- Suggest hybrid queries for tools/search (semantic context, filenames).\n"
                    "- Respect session include patterns when unspecified.\n";
                if (!symbol.empty()) {
                    u += "\nTarget symbol: " + symbol + "\n";
                }
                if (!lang.empty()) {
                    u += "Language hint: " + lang + "\n";
                }
                return makeResponse(
                    {assistantMsg("You guide symbol discovery and code navigation."), userMsg(u)});
            }

            // File-backed prompt fallback: if name matches a file-based template, return its
            // content
            if (!name.empty() && !promptsDir_.empty()) {
                auto fileFromName =
                    [&](const std::string& n) -> std::optional<std::filesystem::path> {
                    std::string stem = n;
                    std::replace(stem.begin(), stem.end(), '_', '-');
                    auto candidate = promptsDir_ / (std::string("PROMPT-") + stem + ".md");
                    if (std::filesystem::exists(candidate))
                        return candidate;
                    return std::nullopt;
                };
                if (auto p = fileFromName(name)) {
                    try {
                        std::ifstream in(*p);
                        std::stringstream buf;
                        buf << in.rdbuf();
                        std::string content = buf.str();
                        return makeResponse({assistantMsg(content)});
                    } catch (...) {
                        // fall through to default
                    }
                }
            }

            // Default fallback
            return makeResponse({assistantMsg(
                "You provide retrieval-augmented workflows over YAMS via MCP tools.")});
        } else if (method == "logging/setLevel") {
            if (!areYamsExtensionsEnabled()) {
                // logging/setLevel is a YAMS extension, and extensions are disabled
                return json{
                    {"jsonrpc", protocol::JSONRPC_VERSION},
                    {"error",
                     {{"code", -32601},
                      {"message", "Method not available (extensions disabled): " + method}}},
                    {"id", id}};
            }
            const auto level = params.value("level", "info");
            spdlog::level::level_enum lvl = spdlog::level::info;
            if (level == "trace")
                lvl = spdlog::level::trace;
            else if (level == "debug")
                lvl = spdlog::level::debug;
            else if (level == "info" || level == "notice")
                lvl = spdlog::level::info;
            else if (level == "warning" || level == "warn")
                lvl = spdlog::level::warn;
            else if (level == "error")
                lvl = spdlog::level::err;
            else if (level == "critical" || level == "alert" || level == "emergency")
                lvl = spdlog::level::critical;
            spdlog::set_level(lvl);
            return createResponse(id, json::object());
        } else {
            // Unknown method - return proper JSON-RPC error
            return json{{"jsonrpc", protocol::JSONRPC_VERSION},
                        {"error", {{"code", -32601}, {"message", "Method not found: " + method}}},
                        {"id", id}};
        }
    } catch (const json::exception& e) {
        return Error{ErrorCode::InvalidArgument, std::string("JSON error: ") + e.what()};
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, std::string("Internal error: ") + e.what()};
    }
}

json MCPServer::initialize(const json& params) {
    // Supported protocol versions (latest first)
    // MCP spec versions: https://spec.modelcontextprotocol.io/specification/2024-11-05/
    static const std::vector<std::string> kSupported = {
        "2025-06-18", // June 2025 update - elicitation, structured output, OAuth
        "2025-03-26", // March 2025 update
        "2025-01-15", // January 2025 update
        "2024-12-05", // December 2024 update
        "2024-11-05"  // Original MCP spec
    };
    const std::string latest = "2025-06-18"; // Latest published MCP spec

    // Extract requested version (optional)
    std::string requested = latest;
    if (params.contains("protocolVersion") && params["protocolVersion"].is_string()) {
        requested = params["protocolVersion"].get<std::string>();
    }
    spdlog::debug("MCP client requested protocol version: {}", requested);

    // Negotiate (fallback to latest if unsupported)
    std::string negotiated = latest;
    if (bool matched = std::ranges::find(kSupported, requested) != kSupported.end()) {
        negotiated = requested;
    } else if (strictProtocol_) {
        json error_data = {{"supportedVersions", kSupported}};
        return {{"_initialize_error", true},
                {{"code", kErrUnsupportedProtocolVersion}},
                {{"message", "Unsupported protocol version requested by client"}},
                {{"data", error_data}}};
    }

    // Capture client info if present (tolerant)
    if (params.contains("clientInfo") && params["clientInfo"].is_object()) {
        clientInfo_.name = params["clientInfo"].value("name", "unknown");
        clientInfo_.version = params["clientInfo"].value("version", "unknown");
    } else {
        clientInfo_.name = "unknown";
        clientInfo_.version = "unknown";
    }

    negotiatedProtocolVersion_ = negotiated;

    // Always build server capabilities (do NOT rely on client-supplied capabilities)
    json caps = buildServerCapabilities();

    json result = {{"protocolVersion", negotiated},
                   {"serverInfo", {{"name", serverInfo_.name}, {"version", serverInfo_.version}}},
                   {"capabilities", caps}};

    // Debug logging to diagnose empty result issues
    spdlog::debug("MCP initialize() result built:");
    spdlog::debug("  - protocolVersion: {}", negotiated);
    spdlog::debug("  - serverInfo.name: {}", serverInfo_.name);
    spdlog::debug("  - serverInfo.version: {}", serverInfo_.version);

    // Update lifecycle state
    lifecycleState_.store(McpLifecycleState::Initialized);

    return result;
}

json MCPServer::listResources() {
    json resources = json::array();

    // Add a resource for the YAMS storage statistics
    resources.push_back({{"uri", "yams://stats"},
                         {"name", "Storage Statistics"},
                         {"description", "Current YAMS storage statistics and health status"},
                         {"mimeType", "application/json"}});

    // Daemon status (symmetry with stats)
    resources.push_back({{"uri", "yams://status"},
                         {"name", "Daemon Status"},
                         {"description", "YAMS daemon status and readiness metrics"},
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
            return {{"contents",
                     {{{"uri", uri},
                       {"mimeType", "application/json"},
                       {"text", json({{"error", "Storage not initialized"}}).dump()}}}}};
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
    } else if (uri == "yams://status") {
        try {
            auto ensure = ensureDaemonClient();
            if (!ensure) {
                return {{"contents",
                         {{{"uri", uri},
                           {"mimeType", "application/json"},
                           {"text", json({{"error",
                                           std::string("status error: ") + ensure.error().message}})
                                        .dump()}}}}};
            }
            auto st = yams::cli::run_result(daemon_client_->status(), std::chrono::seconds(3));
            if (!st) {
                return {{"contents",
                         {{{"uri", uri},
                           {"mimeType", "application/json"},
                           {"text",
                            json({{"error", std::string("status error: ") + st.error().message}})
                                .dump()}}}}};
            }
            const auto& s = st.value();
            json j;
            j["running"] = s.running;
            j["ready"] = s.ready;
            j["uptimeSeconds"] = s.uptimeSeconds;
            j["requestsProcessed"] = s.requestsProcessed;
            j["activeConnections"] = s.activeConnections;
            j["memoryUsageMb"] = s.memoryUsageMb;
            j["cpuUsagePercent"] = s.cpuUsagePercent;
            j["version"] = s.version;
            j["overallStatus"] = s.overallStatus;
            j["lifecycleState"] = s.lifecycleState;
            j["lastError"] = s.lastError;
            j["readinessStates"] = s.readinessStates;
            j["initProgress"] = s.initProgress;
            j["counters"] = s.requestCounts;
            // Also include MCP worker counters
            try {
                size_t queued = 0;
                {
                    std::lock_guard<std::mutex> lk(taskMutex_);
                    queued = taskQueue_.size();
                }
                j["counters"]["mcp_worker_threads"] = workerPool_.size();
                j["counters"]["mcp_worker_active"] = mcpWorkerActive_.load();
                j["counters"]["mcp_worker_queued"] = queued;
                j["counters"]["mcp_worker_processed"] = mcpWorkerProcessed_.load();
                j["counters"]["mcp_worker_failed"] = mcpWorkerFailed_.load();
            } catch (...) {
            }
            return {{"contents",
                     {{{"uri", uri}, {"mimeType", "application/json"}, {"text", j.dump()}}}}};
        } catch (...) {
            return {{"contents",
                     {{{"uri", uri},
                       {"mimeType", "application/json"},
                       {"text", json({{"error", "status exception"}}).dump()}}}}};
        }
    } else if (uri == "yams://recent") {
        // Get recent documents
        if (!metadataRepo_) {
            return {
                {"contents",
                 {{{"uri", uri},
                   {"mimeType", "application/json"},
                   {"text", json({{"error", "Metadata repository not initialized"}}).dump()}}}}};
        }
        auto docsResult = metadata::queryDocumentsByPattern(*metadataRepo_, "%");
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
        props["verbose"] = makeProp("boolean", "Enable verbose output (YAMS extension)");
        props["verbose"]["default"] = false;
        props["type"] =
            makeProp("string", "Search type: keyword, semantic, hybrid (YAMS extension)");
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
        props["color"] = makeProp(
            "string",
            "Color highlighting for matches (values: always, never, auto) (YAMS extension)");
        props["color"]["default"] = "auto";
        props["path_pattern"] = makeProp(
            "string",
            "Glob-like filename/path filter (e.g., **/*.md or substring) (YAMS extension)");
        props["path"] = makeProp(
            "string", "Alias for path_pattern (substring or glob-like filter) (YAMS extension)");
        props["tags"] =
            json{{"type", "array"},
                 {"items", json{{"type", "string"}}},
                 {"description",
                  "Filter by tags (presence-based, matches any by default) (YAMS extension)"}};
        props["match_all_tags"] =
            makeProp("boolean", "Require all specified tags to be present (YAMS extension)");
        props["match_all_tags"]["default"] = false;
        // Session scoping for server-managed sessions
        props["use_session"] =
            makeProp("boolean", "Scope search to current session when available (YAMS extension)");
        props["use_session"]["default"] = true;
        props["session"] =
            makeProp("string", "Explicit session name to scope the search (YAMS extension)");
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
        props["after_context"] = makeProp("integer", "Lines after match (numeric; empty ignored)");
        props["after_context"]["default"] = 0;
        props["before_context"] =
            makeProp("integer", "Lines before match (numeric; empty ignored)");
        props["before_context"]["default"] = 0;
        props["context"] = makeProp("integer", "Lines around match (numeric; empty ignored)");
        props["context"]["default"] = 0;
        props["max_count"] =
            makeProp("integer", "Maximum matches per file (numeric; empty ignored)");
        props["color"] = makeProp("string", "Color highlighting (values: always, never, auto)");
        props["color"]["default"] = "auto";
        // Session scoping for name and path resolution
        props["use_session"] =
            makeProp("boolean", "Scope grep to current session when available (YAMS extension)");
        props["use_session"]["default"] = true;
        props["session"] =
            makeProp("string", "Explicit session name to scope the grep (YAMS extension)");
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
        props["store_only"] = makeProp("boolean", "Store file in CAS without exporting to a path");
        props["store_only"]["default"] = true;
        // When true (default), also ingest into YAMS and return the ingested hash
        props["post_index"] =
            makeProp("boolean",
                     "After download, ingest into YAMS (daemon add) and return the ingested hash");
        props["post_index"]["default"] = true;
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
        props["graph"] =
            makeProp("boolean", "Include knowledge graph relationships (YAMS extension)");
        props["graph"]["default"] = false;
        props["depth"] = makeProp("integer", "Graph traversal depth (1-5) (YAMS extension)");
        props["depth"]["default"] = 1;
        props["include_content"] =
            makeProp("boolean", "Include full content in graph results (YAMS extension)");
        props["include_content"]["default"] = true;
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // get_by_name
    {
        json tool;
        tool["name"] = "get_by_name";
        tool["description"] =
            "Retrieve document content by name or path, optionally returning raw bytes";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["name"] = makeProp("string", "Document name (basename or subpath)");
        props["path"] = makeProp("string", "Explicit path for exact match");
        props["subpath"] = makeProp("boolean", "Allow suffix match when exact path not found");
        props["subpath"]["default"] = true;
        props["raw_content"] =
            makeProp("boolean", "Return raw content without text extraction (binary friendly)");
        props["raw_content"]["default"] = false;
        props["extract_text"] =
            makeProp("boolean", "Extract text from rich formats (HTML/PDF) when available");
        props["extract_text"]["default"] = true;
        props["latest"] = makeProp("boolean", "Select newest match when multiple candidates exist");
        props["latest"]["default"] = true;
        props["oldest"] = makeProp("boolean", "Select oldest match when multiple candidates exist");
        props["oldest"]["default"] = false;
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
        // Session scoping
        props["session"] = makeProp("string", "Explicit session name to scope the list");
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
        props["latest"] = makeProp("boolean", "Select newest match when ambiguous");
        props["latest"]["default"] = true;
        props["oldest"] = makeProp("boolean", "Select oldest match when ambiguous");
        props["oldest"]["default"] = false;
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
        // Align default with CLI behavior: recursive by default for directory indexing
        props["recursive"]["default"] = true;
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

    // session_start
    {
        json tool;
        tool["name"] = "session_start";
        tool["description"] = "Start or switch to a named session for context scoping";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["name"] = makeProp("string", "Session name");
        schema["properties"] = props;
        schema["required"] = json::array({"name"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // session_stop
    {
        json tool;
        tool["name"] = "session_stop";
        tool["description"] = "Stop the current or specified session";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["name"] = makeProp("string", "Optional session name (defaults to current)");
        schema["properties"] = props;
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // session/pin
    {
        json tool;
        tool["name"] = "session_pin";
        tool["description"] = "Pin documents by path pattern (adds 'pinned' tag and updates repo)";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["path"] = makeProp("string", "Path glob pattern to pin");
        props["tags"] = json{{"type", "array"},
                             {"items", json{{"type", "string"}}},
                             {"description", "Additional tags"}};
        props["metadata"] = makeProp("object", "Metadata key/value pairs to add");
        schema["properties"] = props;
        schema["required"] = json::array({"path"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    // session/unpin
    {
        json tool;
        tool["name"] = "session_unpin";
        tool["description"] = "Unpin documents by path pattern (removes 'pinned' tag from repo)";
        json schema;
        schema["type"] = "object";
        json props = json::object();
        props["path"] = makeProp("string", "Path glob pattern to unpin");
        schema["properties"] = props;
        schema["required"] = json::array({"path"});
        tool["inputSchema"] = schema;
        tools.push_back(tool);
    }

    return json{{"tools", tools}};
}

json yams::mcp::MCPServer::listPrompts() {
    auto builtins = json::array(
        {{{"name", "search_codebase"},
          {"description", "Search for code patterns in the codebase"},
          {"arguments", json::array({{{"name", "pattern"},
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
                                      {"required", false}}})}},
         {{"name", "rag/rewrite_query"},
          {"description", "Rewrite a query to optimize hybrid retrieval"},
          {"arguments", json::array({{{"name", "query"},
                                      {"description", "Original user query text"},
                                      {"required", true}},
                                     {{"name", "intent"},
                                      {"description", "Optional intent hint to guide rewriting"},
                                      {"required", false}}})}},
         {{"name", "rag/retrieve"},
          {"description", "Retrieve top-k candidates via hybrid search"},
          {"arguments",
           json::array({{{"name", "query"},
                         {"description", "Search query for retrieval"},
                         {"required", true}},
                        {{"name", "k"},
                         {"description", "Number of candidates to return"},
                         {"required", false}},
                        {{"name", "session"},
                         {"description", "Session name to scope retrieval (optional)"},
                         {"required", false}},
                        {{"name", "tags"},
                         {"description", "Optional tag filters (comma-separated or array)"},
                         {"required", false}}})}},
         {{"name", "rag/retrieve_summarize"},
          {"description", "RAG pipeline: retrieve then summarize with citations"},
          {"arguments",
           json::array({{{"name", "query"},
                         {"description", "Search query for retrieval"},
                         {"required", true}},
                        {{"name", "k"},
                         {"description", "Number of items to retrieve before summarization"},
                         {"required", false}},
                        {{"name", "max_words"},
                         {"description", "Maximum words for the synthesized summary"},
                         {"required", false}}})}},
         {{"name", "rag/extract_citations"},
          {"description", "Format citations from retrieved artifacts"},
          {"arguments",
           json::array({{{"name", "style"},
                         {"description", "Citation style (e.g., inline, list)"},
                         {"required", false}},
                        {{"name", "k"},
                         {"description", "Number of citations to produce"},
                         {"required", false}},
                        {{"name", "include_hashes"},
                         {"description", "Whether to include content hashes in citations"},
                         {"required", false}}})}},
         {{"name", "rag/code_navigation"},
          {"description", "Suggest grep and hybrid search strategies for symbol discovery"},
          {"arguments", json::array({{{"name", "symbol"},
                                      {"description", "Target symbol or identifier to locate"},
                                      {"required", true}},
                                     {{"name", "language"},
                                      {"description", "Language hint (e.g., cpp, py, js)"},
                                      {"required", false}}})}}});

    // Merge file-backed prompts
    std::unordered_set<std::string> seen;
    for (const auto& t : builtins) {
        if (t.is_object() && t.contains("name")) {
            seen.insert(t.at("name").get<std::string>());
        }
    }

    auto sanitize = [](std::string s) {
        // PROMPT-foo-bar.md -> foo_bar
        if (s.rfind("PROMPT-", 0) == 0)
            s = s.substr(7);
        auto pos = s.rfind('.');
        if (pos != std::string::npos)
            s = s.substr(0, pos);
        std::replace(s.begin(), s.end(), '-', '_');
        return s;
    };

    if (!promptsDir_.empty() && std::filesystem::exists(promptsDir_)) {
        try {
            for (const auto& de : std::filesystem::directory_iterator(promptsDir_)) {
                if (!de.is_regular_file())
                    continue;
                auto fname = de.path().filename().string();
                if (fname.rfind("PROMPT-", 0) != 0)
                    continue;
                if (de.path().extension() != ".md")
                    continue;
                auto name = sanitize(fname);
                if (seen.count(name))
                    continue;
                // Description from first line if present
                std::ifstream in(de.path());
                std::string firstLine;
                if (in) {
                    std::getline(in, firstLine);
                }
                if (!firstLine.empty() && firstLine[0] == '#') {
                    // trim leading # and spaces
                    while (!firstLine.empty() && (firstLine[0] == '#' || firstLine[0] == ' '))
                        firstLine.erase(firstLine.begin());
                }
                json t = {{"name", name},
                          {"description", !firstLine.empty() ? firstLine
                                                             : std::string{"Template from "} +
                                                                   de.path().filename().string()},
                          {"arguments", json::array()}};
                builtins.push_back(std::move(t));
                seen.insert(name);
            }
        } catch (...) {
            // best effort
        }
    }

    return {{"prompts", builtins}};
}

json MCPServer::callTool(const std::string& name, const json& arguments) {
    if (!toolRegistry_) {
        return {{"error", {{"code", -32603}, {"message", "Tool registry not initialized"}}}};
    }

    // Stdio mode: run tool synchronously using the global io_context
    // The daemon client and all async operations use GlobalIOContext,
    // which already has worker threads running, so we just spawn and wait
    auto& global_io = yams::daemon::GlobalIOContext::instance().get_io_context();
    auto task = toolRegistry_->callTool(name, arguments);

    // Spawn on global io_context (which is already running) and wait for result
    json result;
    try {
        auto future = boost::asio::co_spawn(global_io, std::move(task), boost::asio::use_future);
        result = future.get();

        spdlog::debug("MCP tool '{}' returned: {}", name, result.dump());

        // Normalize errors: if registry returned content-based error, map to JSON-RPC error
        if (result.is_object() && result.value("isError", false)) {
            std::string msg;
            try {
                if (result.contains("content") && result["content"].is_array() &&
                    !result["content"].empty()) {
                    const auto& item = result["content"][0];
                    msg = item.value("text", std::string{"Tool error"});
                }
            } catch (...) {
                msg = "Tool error";
            }
            int code = -32602; // Invalid params by default
            if (msg.rfind("Unknown tool:", 0) == 0) {
                code = -32601; // Method not found
            }
            return json{{"error", json{{"code", code}, {"message", msg}}}};
        }

        // Ensure result is tool-result shaped (content array) when not error
        if (result.is_object() && result.contains("content")) {
            return result; // already wrapped
        }
        // Legacy/plain result: wrap into content per MCP spec
        return yams::mcp::wrapToolResult(result, /*isError=*/false);

    } catch (const std::exception& e) {
        return {{"error",
                 {{"code", -32603}, {"message", std::string("Tool call failed: ") + e.what()}}}};
    }
}

boost::asio::awaitable<json> MCPServer::callToolAsync(const std::string& name,
                                                      const json& arguments) {
    spdlog::debug("MCP callToolAsync invoked: '{}'", name);
    if (!toolRegistry_) {
        co_return json{{"error", {{"code", -32603}, {"message", "Tool registry not initialized"}}}};
    }
    auto result = co_await toolRegistry_->callTool(name, arguments);
    co_return result;
}

boost::asio::awaitable<Result<MCPSearchResponse>>
MCPServer::handleSearchDocuments(const MCPSearchRequest& req) {
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
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
    dreq.pathsOnly = req.pathsOnly;
    dreq.showLineNumbers = req.lineNumbers;
    dreq.beforeContext = req.beforeContext;
    dreq.afterContext = req.afterContext;
    dreq.context = req.context;

    // Send early progress notification
    sendProgress("search", 0.0, "search started");

    // If keyword search was explicitly requested, skip vector scoring entirely
    if (dreq.searchType == "keyword") {
        dreq.similarity = 0.0; // disable vector path
    } else {
        // Optional: degrade to keyword if provider/index unavailable
        try {
            if (auto st = co_await daemon_client_->status()) {
                const auto& s = st.value();
                bool provider_ready = false;
                for (const auto& p : s.providers) {
                    if (p.isProvider && p.ready && !p.degraded) {
                        provider_ready = true;
                        break;
                    }
                }
                bool vector_ready = true;
                if (auto it = s.readinessStates.find("vector_index"); it != s.readinessStates.end())
                    vector_ready = it->second;
                if (!provider_ready || !vector_ready) {
                    sendProgress("search", 10.0, "degraded to keyword");
                    dreq.searchType = "keyword";
                    dreq.similarity = 0.0;
                }
            }
        } catch (...) {
            // Best-effort: on status failure keep requested type
        }
    }

    MCPSearchResponse out;
    // Propagate session to services/daemon via environment for this handler
    std::string __session;
    if (!req.sessionName.empty()) {
        __session = req.sessionName;
    } else {
        auto __svc = app::services::makeSessionService(nullptr);
        __session = __svc->current().value_or("");
    }
    if (!__session.empty()) {
        ::setenv("YAMS_SESSION_CURRENT", __session.c_str(), 1);
        spdlog::debug("[MCP] search: using session '{}'", __session);
    }

    // Optional fast-first strategy: quick keyword preview before full hybrid
    if (dreq.searchType == "hybrid") {
        if (const char* ff = std::getenv("YAMS_MCP_SEARCH_FAST_FIRST"); ff && *ff && ff[0] != '0') {
            yams::daemon::SearchRequest kreq = dreq;
            kreq.searchType = "keyword";
            kreq.fuzzy = false;    // keep preview snappy
            kreq.similarity = 0.0; // skip vector
            kreq.limit = std::min<size_t>(dreq.limit > 0 ? dreq.limit : 10, 10);
            if (auto kres = co_await daemon_client_->search(kreq)) {
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

    // Execute search directly using co_await (already in coroutine context)
    auto res = co_await daemon_client_->search(dreq);
    // Clear after call
    if (!__session.empty()) {
        ::setenv("YAMS_SESSION_CURRENT", "", 1);
    }
    if (!res) {
        co_return res.error();
    }
    const auto& r = res.value();
    out.total = r.totalCount;
    out.type = "daemon";
    out.executionTimeMs = r.elapsed.count();
    // When pathsOnly was requested by the MCP client, populate the 'paths' field
    // to mirror CLI behavior and make it easy for clients to consume.
    if (req.pathsOnly) {
        out.paths.reserve(r.results.size());
        for (const auto& item : r.results) {
            std::string path =
                !item.path.empty()
                    ? item.path
                    : (item.metadata.contains("path") ? item.metadata.at("path") : std::string());
            if (path.empty())
                path = item.id; // last-resort fallback
            out.paths.push_back(std::move(path));
        }
        sendProgress("search", 100.0, "done");
        co_return out;
    }
    // Full result objects (with robust path fallback)
    for (const auto& item : r.results) {
        MCPSearchResponse::Result m;
        m.id = item.id;
        m.hash = item.metadata.contains("hash") ? item.metadata.at("hash") : "";
        m.title = item.title;
        // Fallback to metadata.path when daemon omitted direct path field
        m.path = !item.path.empty()
                     ? item.path
                     : (item.metadata.contains("path") ? item.metadata.at("path") : std::string());
        m.score = item.score;
        m.snippet = item.snippet;
        out.results.push_back(std::move(m));
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
                    yams::app::services::RetrievalService rsvc;
                    if (auto appc = app::services::makeSessionService(nullptr); appc) {
                        // no-op placeholder for future per-session retrieval options
                    }
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
                        yams::app::services::RetrievalOptions ropts;
                        yams::app::services::GetOptions greq;
                        greq.hash = hash;
                        greq.metadataOnly = false;
                        auto gr = rsvc.get(greq, ropts);
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

boost::asio::awaitable<Result<MCPGrepResponse>>
MCPServer::handleGrepDocuments(const MCPGrepRequest& req) {
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    yams::app::services::GrepOptions dreq;
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

    // Session scoping for grep: if no explicit paths, use session patterns
    if (req.useSession && dreq.paths.empty()) {
        auto sess = app::services::makeSessionService(nullptr);
        auto pats = sess->activeIncludePatterns(req.sessionName.empty()
                                                    ? std::optional<std::string>{}
                                                    : std::optional<std::string>{req.sessionName});
        if (!pats.empty()) {
            size_t added = 0;
            for (const auto& p : pats) {
                dreq.paths.push_back(p);
                if (++added >= 64)
                    break;
            }
        }
    }

    // Fast-first path: emit early semantic suggestions and return immediately if requested
    if (req.fastFirst) {
        yams::daemon::SearchRequest sreq;
        sreq.query = req.pattern;
        sreq.limit = 10;
        sreq.fuzzy = true;
        sreq.searchType = "hybrid";
        sreq.pathsOnly = false;
        if (auto sres = co_await daemon_client_->search(sreq)) {
            const auto& sr = sres.value();
            MCPGrepResponse early;
            std::ostringstream oss_;
            for (const auto& item : sr.results) {
                std::string p = !item.path.empty() ? item.path : item.title;
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
    // Propagate session to services/daemon via environment for this handler
    std::string __session;
    if (!req.sessionName.empty()) {
        __session = req.sessionName;
    } else {
        auto __svc = app::services::makeSessionService(nullptr);
        __session = __svc->current().value_or("");
    }
    if (!__session.empty()) {
        ::setenv("YAMS_SESSION_CURRENT", __session.c_str(), 1);
        spdlog::debug("[MCP] grep: using session '{}'", __session);
    }
    // Use service facade for grep (daemon-first)
    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.enableStreaming = true;
    ropts.requestTimeoutMs = 30000;
    ropts.headerTimeoutMs = 30000;
    ropts.bodyTimeoutMs = 120000;
    auto res = rsvc.grep(dreq, ropts);
    // Clear after call
    if (!__session.empty()) {
        ::setenv("YAMS_SESSION_CURRENT", "", 1);
    }
    if (!res)
        co_return res.error();
    const auto& r = res.value();
    out.matchCount = r.totalMatches;
    out.fileCount = r.filesSearched;
    std::ostringstream oss;
    std::unordered_set<std::string> seenFiles;
    for (const auto& m : r.matches) {
        if (!m.file.empty())
            seenFiles.insert(m.file);
        if (out.fileCount > 1 || req.withFilename) {
            if (!m.file.empty())
                oss << m.file << ":";
        }
        if (req.lineNumbers)
            oss << m.lineNumber << ":";
        oss << m.line << "\n";
    }
    if (out.fileCount == 0 && !seenFiles.empty()) {
        out.fileCount = seenFiles.size();
    }
    out.output = oss.str();
    co_return out;
}

boost::asio::awaitable<Result<MCPDownloadResponse>>
MCPServer::handleDownload(const MCPDownloadRequest& req) {
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
            co_return Error{ErrorCode::InternalError,
                            std::string("Failed to create staging dir: ") +
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

    auto manager = yams::downloader::makeDownloadManager(storage, cfg);
    auto dlRes = manager->download(dreq);
    if (!dlRes.ok()) {
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

    // Optionally post-index the artifact via daemon
    // Optionally post-index the artifact via daemon
    if (mcp_response.success && req.postIndex) {
        daemon::AddDocumentRequest addReq;
        // Resolve stored path to absolute under daemon-resolved content store root if relative
        std::filesystem::path __abs = std::filesystem::path(mcp_response.storedPath);
        if (__abs.is_relative()) {
            std::filesystem::path __base;
            try {
                auto sres = co_await daemon_client_->status();
                if (sres) {
                    const auto& s = sres.value();
                    if (!s.contentStoreRoot.empty()) {
                        __base = std::filesystem::path(s.contentStoreRoot).parent_path();
                    }
                }
            } catch (...) {
            }
            if (__base.empty()) {
                if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME");
                    xdgDataHome && *xdgDataHome) {
                    __base = std::filesystem::path(xdgDataHome) / "yams";
                } else if (const char* homeEnv = std::getenv("HOME"); homeEnv && *homeEnv) {
                    __base = std::filesystem::path(homeEnv) / ".local" / "share" / "yams";
                } else {
                    __base = std::filesystem::current_path();
                }
            }
            __abs = __base / __abs;
        }
        std::error_code __canon_ec;
        auto __canon = std::filesystem::weakly_canonical(__abs, __canon_ec);
        if (!__canon_ec && !__canon.empty()) {
            __abs = __canon;
        }
        addReq.path = __abs.string(); // normalized absolute path

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
                if (q != std::string::npos)
                    fname = fname.substr(0, q);
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
            const auto p = url.find("://");
            if (p == std::string::npos)
                return {};
            auto rest = url.substr(p + 3);
            auto slash = rest.find('/');
            return (slash == std::string::npos) ? rest : rest.substr(0, slash);
        };
        auto extract_scheme = [](const std::string& url) -> std::string {
            const auto p = url.find("://");
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

        // Preflight: require daemon content_store readiness
        try {
            auto sres = co_await daemon_client_->status();
            if (!sres)
                co_return sres.error();
            const auto& s = sres.value();
            bool csr = false;
            if (auto it = s.readinessStates.find("content_store"); it != s.readinessStates.end())
                csr = it->second;
            if (!csr) {
                std::string hint = "Content store not ready. Check daemon status and config.";
                if (!s.contentStoreError.empty())
                    hint += std::string(" Error: ") + s.contentStoreError;
                co_return Error{ErrorCode::InvalidState, hint};
            }
        } catch (...) {
            co_return Error{ErrorCode::Unknown, "Unable to fetch daemon status for preflight"};
        }
        // Call daemon to add/index the downloaded document
        auto addres = co_await daemon_client_->streamingAddDocument(addReq);
        if (!addres) {
            mcp_response.indexed = false;
        } else {
            mcp_response.indexed = true;
            const auto& addok = addres.value();
            spdlog::info("[MCP] post-index: indexed path='{}' hash='{}'", addReq.path, addok.hash);
            mcp_response.hash = addok.hash; // Update with the definitive hash from indexing
        }
    }

    co_return mcp_response;
}

boost::asio::awaitable<Result<MCPStoreDocumentResponse>>
MCPServer::handleStoreDocument(const MCPStoreDocumentRequest& req) {
    // Fast path: reject completely empty inputs before contacting the daemon
    if ((req.path.empty() || req.path == "") && (req.name.empty() || req.name == "") &&
        (req.content.empty() || req.content == "")) {
        co_return Error{ErrorCode::InvalidArgument,
                        "No content or path provided. Set 'path' to a file or provide 'content'."};
    }

    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }

    // Lightweight throttle: limit concurrent add/store operations to avoid stressing the IPC FSM
    {
        using namespace std::chrono_literals;
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer{exec};
        constexpr int maxConcurrent = 2;
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
    // Preflight: require daemon content_store readiness
    bool modelReadyFlag = true; // track model/embeddings readiness across function
    try {
        auto sres = co_await daemon_client_->status();
        if (!sres)
            co_return sres.error();
        const auto& s = sres.value();
        bool csr = false;
        if (auto it = s.readinessStates.find("content_store"); it != s.readinessStates.end())
            csr = it->second;
        // Detect embedding/model provider readiness; when unavailable, force noEmbeddings
        try {
            if (auto it = s.readinessStates.find("model_provider"); it != s.readinessStates.end())
                modelReadyFlag = it->second;
            // Some builds report 'embeddings' instead of 'model_provider'
            if (auto it2 = s.readinessStates.find("embeddings"); it2 != s.readinessStates.end())
                modelReadyFlag = modelReadyFlag && it2->second;
        } catch (...) {
            // default to ready if key missing
        }
        if (!csr) {
            // Graceful bounded wait for content_store readiness to avoid transient I/O failures
            using namespace std::chrono_literals;
            const auto ready_timeout = 2s;
            const auto poll_interval = 150ms;
            auto exec = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer timer{exec};
            auto start = std::chrono::steady_clock::now();
            for (;;) {
                auto s2 = co_await daemon_client_->status();
                if (s2) {
                    const auto& ss = s2.value();
                    if (auto it2 = ss.readinessStates.find("content_store");
                        it2 == ss.readinessStates.end() || it2->second) {
                        break; // proceed when ready or key missing
                    }
                }
                if (std::chrono::steady_clock::now() - start >= ready_timeout) {
                    break; // proceed and rely on retry/backoff below
                }
                timer.expires_after(poll_interval);
                co_await timer.async_wait(boost::asio::use_awaitable);
            }
        }
        // If model provider isn't ready, prefer graceful degradation by disabling embeddings.
        if (!modelReadyFlag) {
            spdlog::warn("[MCP] Model provider not ready — forcing noEmbeddings for add");
        }
        // We will apply the decision below once daemon_req/aopts are constructed.
    } catch (...) {
        co_return Error{ErrorCode::Unknown, "Unable to fetch daemon status for preflight"};
    }
    // Convert MCP request to daemon request
    daemon::AddDocumentRequest daemon_req;
    // Choose path source: prefer explicit path; else treat name as path if it points to a file
    std::string candidatePath = req.path;
    if (candidatePath.empty() && !req.name.empty()) {
        // Heuristic: if 'name' resolves to an existing file, treat it as path (CLI parity)
        std::string tmp = req.name;
        // Strip CR/LF and trim
        if (!tmp.empty()) {
            std::erase_if(tmp, [](unsigned char c) { return c == '\n' || c == '\r'; });
            auto ltrim = [](std::string& s) {
                s.erase(s.begin(),
                        std::ranges::find_if(s, [](int ch) { return !std::isspace(ch); }));
            };
            auto rtrim = [](std::string& s) {
                s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); })
                            .base(),
                        s.end());
            };
            ltrim(tmp);
            rtrim(tmp);
        }
        // Resolve relative against PWD/current and check existence
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
                // If 'name' appears to be a path, set document name to basename for UX
                try {
                    daemon_req.name = p.filename().string();
                } catch (...) {
                }
            }
        }
    }

    // Normalize path: expand '~' and make absolute using PWD for relative paths
    {
        std::string _p = candidatePath;
        // Sanitize control characters (CR/LF/NUL) and trim leading/trailing spaces/tabs
        if (!_p.empty()) {
            std::string cleaned;
            cleaned.reserve(_p.size());
            for (char c : _p) {
                if (c == '\0')
                    break; // stop at first NUL just in case
                if (c == '\r' || c == '\n')
                    continue; // drop CR/LF which can corrupt path resolution
                cleaned.push_back(c);
            }
            // Trim leading/trailing spaces and tabs (avoid accidental copy/paste whitespace)
            auto start = cleaned.find_first_not_of(" \t");
            if (start == std::string::npos) {
                cleaned.clear();
            } else {
                auto end = cleaned.find_last_not_of(" \t");
                cleaned = cleaned.substr(start, end - start + 1);
            }
            // Remove a trailing slash (except for root) to avoid treating intended file paths as
            // directories
            if (cleaned.size() > 1 && cleaned.back() == '/') {
                cleaned.pop_back();
            }
            _p = std::move(cleaned);
        }
        // Strip file:// scheme if present (basic normalization)
        if (_p.rfind("file://", 0) == 0) {
            _p = _p.substr(7);
        }
        if (!_p.empty() && _p.front() == '~') {
            if (const char* home = std::getenv("HOME")) {
                _p = std::string(home) + _p.substr(1);
            }
        }
        // Resolve relative paths against likely bases: PWD, then current_path()
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
                if (std::error_code ec; std::filesystem::exists(cand, ec)) {
                    chosen = cand;
                    resolved = true;
                    break;
                }
            }
            _p = resolved ? chosen.string() : (_p.rfind("./", 0) == 0 ? _p.substr(2) : _p);
        }
        // Best-effort canonicalization
        {
            std::error_code __canon_ec;
            auto __canon = std::filesystem::weakly_canonical(_p, __canon_ec);
            if (!__canon_ec && !__canon.empty()) {
                _p = __canon.string();
            }
        }
        daemon_req.path = _p;
    }
    daemon_req.content = req.content;
    if (daemon_req.name.empty())
        daemon_req.name = req.name;
    daemon_req.mimeType = req.mimeType;
    daemon_req.disableAutoMime = req.disableAutoMime;
    // Force noEmbeddings when model provider is not ready (from preflight snapshot)
    daemon_req.noEmbeddings = req.noEmbeddings || !modelReadyFlag;
    daemon_req.collection = req.collection;
    daemon_req.snapshotId = req.snapshotId;
    daemon_req.snapshotLabel = req.snapshotLabel;
    daemon_req.recursive = req.recursive;
    daemon_req.includePatterns = req.includePatterns;
    daemon_req.excludePatterns = req.excludePatterns;
    daemon_req.tags = req.tags;
    for (const auto& [key, value] : req.metadata.items()) {
        if (value.is_string()) {
            daemon_req.metadata[key] = value.get<std::string>();
        } else {
            daemon_req.metadata[key] = value.dump();
        }
    }
    // Validate request before we ever contact the daemon. The dispatcher/ingest pipeline now
    // expects either a resolved path or (content + name); enforcing it here avoids enqueueing
    // malformed tasks that previously triggered ingest crashes.
    if (daemon_req.path.empty()) {
        if (daemon_req.content.empty()) {
            co_return Error{ErrorCode::InvalidArgument,
                            "Provide either 'path' or 'content' + 'name'"};
        }
        if (daemon_req.name.empty()) {
            co_return Error{ErrorCode::InvalidArgument,
                            "Provide 'name' when sending inline 'content'"};
        }
    }

    // Single-path daemon call with bounded retries; always use streaming path
    {
        using namespace std::chrono_literals;
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer{exec};
        constexpr int maxAttempts = 3;
        bool hasContent = !daemon_req.content.empty();
        for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
            // Use streamingAddDocument for parity with CLI and to avoid unary-path edge cases
            Result<yams::daemon::AddDocumentResponse> addRes =
                co_await daemon_client_->streamingAddDocument(daemon_req);
            if (addRes) {
                MCPStoreDocumentResponse out;
                // For directory adds, return empty hash to signal multi-file op
                std::error_code ec;
                if (!hasContent && !daemon_req.path.empty() && daemon_req.recursive &&
                    std::filesystem::is_directory(daemon_req.path, ec)) {
                    co_return out;
                }
                out.hash = addRes.value().hash;
                out.bytesStored = 0;
                out.bytesDeduped = 0;
                co_return out;
            }
            const auto& err = addRes.error();
            bool retryable =
                (err.code == ErrorCode::NotInitialized || err.code == ErrorCode::Timeout ||
                 err.code == ErrorCode::NetworkError);
            if (!retryable || attempt == maxAttempts)
                co_return err;
            timer.expires_after(std::chrono::milliseconds(250 * attempt));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    // If neither content nor a valid path was provided, fail fast with a clear error
    if (daemon_req.path.empty() && daemon_req.content.empty()) {
        co_return Error{ErrorCode::InvalidArgument,
                        "No content or path provided. Set 'path' to a file or provide 'content'."};
    }

    // Should not reach here
    co_return Error{ErrorCode::Unknown, "Unexpected add failure"};
}

boost::asio::awaitable<Result<MCPRetrieveDocumentResponse>>
MCPServer::handleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    // Convert MCP request to daemon request
    yams::app::services::GetOptions daemon_req;
    daemon_req.hash = req.hash;
    daemon_req.name = req.name;
    daemon_req.byName = !req.name.empty();
    daemon_req.outputPath = req.outputPath;
    daemon_req.showGraph = req.graph;
    daemon_req.graphDepth = req.depth;
    daemon_req.metadataOnly = !req.includeContent;
    daemon_req.acceptCompressed = true;

    // Unified path: use RetrievalService name-smart get when name provided, else direct get
    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.requestTimeoutMs = 60000;
    ropts.headerTimeoutMs = 30000;
    ropts.bodyTimeoutMs = 120000;

    auto dres = rsvc.get(daemon_req, ropts);
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
        mcp_response.content = resp.content;
    }
    mcp_response.graphEnabled = resp.graphEnabled;
    for (const auto& rel : resp.related) {
        json relatedJson = {{"hash", rel.hash}, {"path", rel.path}, {"distance", rel.distance}};
        mcp_response.related.push_back(relatedJson);
    }
    co_return mcp_response;
}

boost::asio::awaitable<Result<MCPListDocumentsResponse>>
MCPServer::handleListDocuments(const MCPListDocumentsRequest& req) {
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    daemon::ListRequest daemon_req;
    // Map MCP filters to daemon ListRequest
    // Prioritize pattern, but fall back to name.
    if (!req.pattern.empty()) {
        if (auto normalized = yams::app::services::utils::normalizeLookupPath(req.pattern);
            normalized.changed && !normalized.hasWildcards) {
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
    std::string __session;
    if (!req.sessionName.empty()) {
        __session = req.sessionName;
    } else {
        auto __svc = app::services::makeSessionService(nullptr);
        __session = __svc->current().value_or("");
    }
    if (!__session.empty()) {
        ::setenv("YAMS_SESSION_CURRENT", __session.c_str(), 1);
        spdlog::debug("[MCP] list: using session '{}'", __session);
    }
    // Use service facade for list (daemon-first)
    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.enableStreaming = true;
    ropts.requestTimeoutMs = 30000;
    ropts.headerTimeoutMs = 30000;
    ropts.bodyTimeoutMs = 120000;
    yams::app::services::ListOptions list_opts;
    list_opts.limit = daemon_req.limit;
    list_opts.offset = daemon_req.offset;
    list_opts.namePattern = daemon_req.namePattern;
    list_opts.sortBy = daemon_req.sortBy;
    list_opts.reverse = daemon_req.reverse;
    auto dres = rsvc.list(list_opts, ropts);
    // Clear after call
    if (!__session.empty()) {
        ::setenv("YAMS_SESSION_CURRENT", "", 1);
    }
    if (!dres)
        co_return dres.error();
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
        // Synthetic tag when caller provided a concrete local file and it matches by suffix
        if (!req.name.empty()) {
            if (auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(req.name);
                resolved.isLocalFile && !item.path.empty()) {
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
                        // Get indexed content
                        yams::app::services::GetOptions greq;
                        greq.hash = item.hash;
                        greq.metadataOnly = false;
                        if (auto gr = rsvc.get(greq, ropts)) {
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
}

boost::asio::awaitable<Result<MCPStatsResponse>>
MCPServer::handleGetStats(const MCPStatsRequest& req) {
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    daemon::GetStatsRequest daemon_req;
    daemon_req.showFileTypes = req.fileTypes;
    daemon_req.detailed = req.verbose;
    auto dres = co_await daemon_client_->getStats(daemon_req);
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
}

boost::asio::awaitable<Result<MCPStatusResponse>>
MCPServer::handleGetStatus(const MCPStatusRequest& req) {
    (void)req;
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    auto sres = co_await daemon_client_->status();
    if (!sres)
        co_return sres.error();
    const auto& s = sres.value();
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
    // Merge MCP worker pool counters for observability
    try {
        // queued size snapshot under lock
        size_t queued = 0;
        {
            std::lock_guard<std::mutex> lk(taskMutex_);
            queued = taskQueue_.size();
        }
        out.counters["mcp_worker_threads"] = workerPool_.size();
        out.counters["mcp_worker_active"] = mcpWorkerActive_.load();
        out.counters["mcp_worker_queued"] = queued;
        out.counters["mcp_worker_processed"] = mcpWorkerProcessed_.load();
        out.counters["mcp_worker_failed"] = mcpWorkerFailed_.load();
    } catch (...) {
    }
    out.readinessStates = s.readinessStates;
    out.initProgress = s.initProgress;
    co_return out;
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
            std::erase_if(path_str, [](unsigned char c) { return c == '\n' || c == '\r'; });
            auto ltrim = [](std::string& s) {
                s.erase(s.begin(),
                        std::ranges::find_if(s, [](int ch) { return !std::isspace(ch); }));
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

    // Wait briefly for daemon readiness (content_store) to avoid transient startup errors
    {
        using namespace std::chrono_literals;
        const auto ready_timeout = 5s; // bounded wait
        const auto poll_interval = 150ms;
        auto exec = co_await boost::asio::this_coro::executor;
        boost::asio::steady_timer timer{exec};
        auto start = std::chrono::steady_clock::now();
        for (;;) {
            if (auto sres = co_await daemon_client_->status()) {
                const auto& s = sres.value();
                auto it = s.readinessStates.find("content_store");
                if (it == s.readinessStates.end() || it->second) {
                    break; // either not exposed or ready
                }
            }
            if (std::chrono::steady_clock::now() - start >= ready_timeout) {
                break; // proceed; server will return a retryable error if still not ready
            }
            timer.expires_after(poll_interval);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    // Build AddDocumentRequest targeting a directory ingestion (recursive)
    daemon::AddDocumentRequest dreq;
    dreq.path = dir_path.string();
    dreq.content.clear();
    dreq.name.clear();
    dreq.tags = req.tags;
    // metadata
    for (const auto& [k, v] : req.metadata.items()) {
        if (v.is_string())
            dreq.metadata[k] = v.get<std::string>();
        else
            dreq.metadata[k] = v.dump();
    }
    dreq.recursive = true; // force recursive for directories
    dreq.includeHidden = false;
    dreq.includePatterns = req.includePatterns;
    dreq.excludePatterns = req.excludePatterns;
    dreq.collection = req.collection;
    dreq.snapshotId.clear();
    dreq.snapshotLabel.clear();
    dreq.mimeType.clear();
    dreq.disableAutoMime = false;
    dreq.noEmbeddings = false;

    // Use streamingAddDocument; dispatcher will return a single AddDocumentResponse when done
    auto addRes = co_await daemon_client_->streamingAddDocument(dreq);
    if (!addRes) {
        // Map NotReady/Internal to a clearer message for clients; keep code as-is
        if (addRes.error().code == ErrorCode::NotInitialized) {
            co_return Error{ErrorCode::NotInitialized,
                            "Daemon: content store initializing; please retry shortly"};
        }
        co_return addRes.error();
    }

    // We do not have per-file detailed results over AddDocumentResponse; provide a summary
    MCPAddDirectoryResponse out;
    out.directoryPath = req.directoryPath;
    out.collection = req.collection;
    // Since dispatcher’s directory path produces multiple adds internally, we cannot accurately
    // report processed/indexed counts without a separate API. Provide a minimal summary.
    out.filesProcessed = 0;
    out.filesIndexed = 0;
    out.filesSkipped = 0;
    out.filesFailed = 0;
    out.results.clear();
    co_return out;
}

boost::asio::awaitable<Result<MCPDoctorResponse>>
MCPServer::handleDoctor(const MCPDoctorRequest& req) {
    (void)req;
    // Resolve socket and probe connectivity first so we can return structured info even if daemon
    // is unreachable.
    std::filesystem::path sock = daemon_client_config_.socketPath;
    if (sock.empty()) {
        try {
            sock = yams::daemon::socket_utils::resolve_socket_path_config_first();
            daemon_client_config_.socketPath = sock;
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
    if (auto ensure = ensureDaemonClient(); ensure) {
        if (auto sres = co_await daemon_client_->status()) {
            s = sres.value();
            haveStatus = true;
        }
    }

    MCPDoctorResponse out;
    std::vector<std::string> issues;
    json details;
    details["overallStatus"] = haveStatus ? s.overallStatus : std::string("unknown");
    details["lifecycleState"] = haveStatus ? s.lifecycleState : std::string("unknown");
    details["lastError"] = haveStatus ? s.lastError : std::string("unreachable");
    if (haveStatus)
        details["readiness"] = s.readinessStates;
    if (haveStatus)
        details["counters"] = s.requestCounts;
    details["socketPath"] = sock.empty() ? std::string("") : sock.string();
    details["socketExists"] = socketExists;
    details["connectable"] = connectable;

    if (!haveStatus || !s.running) {
        issues.emplace_back("daemon_not_running");
    }
    if (haveStatus && !s.ready) {
        issues.emplace_back("daemon_not_ready");
        for (const auto& [k, v] : s.readinessStates) {
            if (!v)
                issues.push_back(std::string("subsystem_not_ready:") + k);
        }
    }
    if (haveStatus && s.requestCounts.contains("post_ingest_queued") &&
        s.requestCounts.at("post_ingest_queued") > 1000) {
        issues.emplace_back("post_ingest_backlog_high");
    }
    if (haveStatus && s.requestCounts.contains("worker_queued") &&
        s.requestCounts.at("worker_queued") > 1000) {
        issues.emplace_back("worker_queue_high");
    }
    if (haveStatus && s.readinessStates.contains("vector_embeddings_available") &&
        !s.readinessStates.at("vector_embeddings_available")) {
        issues.emplace_back("vector_embeddings_unavailable");
    }

    // Suggestions
    std::vector<std::string> suggestions;
    for (const auto& iss : issues) {
        if (iss == "vector_embeddings_unavailable") {
            suggestions.emplace_back(
                "Load or initialize an embedding model; check plugins and model provider logs.");
        } else if (iss.rfind("subsystem_not_ready:", 0) == 0) {
            suggestions.emplace_back(
                "Review logs for the listed subsystem and ensure dependencies are initialized.");
        } else if (iss == "post_ingest_backlog_high") {
            suggestions.emplace_back("Increase post-ingest threads via TuneAdvisor or pause adds "
                                     "until the queue drains.");
        } else if (iss == "worker_queue_high") {
            suggestions.emplace_back(
                "Reduce parallel requests or increase worker threads via TuneAdvisor.");
        } else if (iss == "daemon_not_ready") {
            suggestions.emplace_back(
                "Wait for initialization to complete or investigate lifecycle lastError.");
        } else if (iss == "daemon_not_running") {
            suggestions.emplace_back("Start or restart the daemon.");
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
    out.summary = summary;
    out.issues = std::move(issues);
    out.details = std::move(details);
    co_return out;
}

boost::asio::awaitable<Result<MCPUpdateMetadataResponse>>
MCPServer::handleUpdateMetadata(const MCPUpdateMetadataRequest& req) {
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
        if (auto ensure = ensureDaemonClient(); !ensure) {
            co_return ensure.error();
        }
        auto dres = co_await daemon_client_->updateDocument(daemon_req);
        if (!dres)
            co_return dres.error();
        MCPUpdateMetadataResponse out;
        const auto& ur = dres.value();
        out.success = ur.metadataUpdated || ur.tagsUpdated || ur.contentUpdated;
        out.updated = out.success ? 1 : 0;
        out.matched = 1;
        if (!ur.hash.empty())
            out.updatedHashes.push_back(ur.hash);
        out.message = out.success ? "Update successful" : "No changes applied";
        co_return out;
    }

    // Name-first single-target path: reuse robust name resolution from get_by_name.
    // When a single name is provided (without other selectors), resolve to a single document
    // using RetrievalService::getByNameSmart and then perform a hash-based update via daemon.
    if (req.hash.empty() && !req.name.empty() && req.path.empty() && req.pattern.empty() &&
        req.names.empty()) {
        // Resolve name -> hash using the same strategy as handleGetByName (smart + fallback).
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
        yams::app::services::RetrievalService rsvc;
        yams::app::services::RetrievalOptions ropts;
        ropts.requestTimeoutMs = 60000;
        ropts.headerTimeoutMs = 30000;
        ropts.bodyTimeoutMs = 120000;
        auto resolver = [this](const std::string& nm) -> Result<std::string> {
            if (documentService_)
                return documentService_->resolveNameToHash(nm);
            return Error{ErrorCode::NotFound, "resolver unavailable"};
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
            // Fall back to simple basename-list matching similar to handleGetByName
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
            daemon_req.hash = cand->hash;
            daemon_req.addTags = req.tags;
            daemon_req.removeTags = req.removeTags;
            for (const auto& [key, value] : req.metadata.items()) {
                if (value.is_string())
                    daemon_req.metadata[key] = value.get<std::string>();
                else
                    daemon_req.metadata[key] = value.dump();
            }
            if (auto ensure = ensureDaemonClient(); !ensure) {
                co_return ensure.error();
            }
            auto dres = co_await daemon_client_->updateDocument(daemon_req);
            if (!dres)
                co_return dres.error();
            MCPUpdateMetadataResponse out;
            const auto& ur = dres.value();
            out.success = ur.metadataUpdated || ur.tagsUpdated || ur.contentUpdated;
            out.updated = out.success ? 1 : 0;
            out.matched = 1;
            if (!ur.hash.empty())
                out.updatedHashes.push_back(ur.hash);
            out.message = out.success ? "Update successful" : "No changes applied";
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
        if (auto ensure = ensureDaemonClient(); !ensure) {
            co_return ensure.error();
        }
        auto dres = co_await daemon_client_->updateDocument(daemon_req);
        if (!dres)
            co_return dres.error();
        MCPUpdateMetadataResponse out;
        const auto& ur = dres.value();
        out.success = ur.metadataUpdated || ur.tagsUpdated || ur.contentUpdated;
        out.updated = out.success ? 1 : 0;
        out.matched = 1;
        if (!ur.hash.empty())
            out.updatedHashes.push_back(ur.hash);
        out.message = out.success ? "Update successful" : "No changes applied";
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
        if (auto lr = docService->list(lreq); lr && !lr.value().documents.empty()) {
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
    std::ranges::sort(targets, [](const auto& a, const auto& b) { return a.hash < b.hash; });
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
        if (const auto ensure = ensureDaemonClient(); ensure) {
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
MCPServer::handleSessionPin(const MCPSessionPinRequest& req) const {
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
            u.addTags.emplace_back("pinned");
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

        u.removeTags.emplace_back("pinned");
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

void MCPServer::initializeToolRegistry() {
    toolRegistry_ = std::make_unique<ToolRegistry>();

    // Always register standard MCP tools
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
                {"description", "Filter by tags (YAMS extension)"}}}}},
            {"required", json::array({"query"})}},
        "Search documents using hybrid search (vector + full-text + knowledge graph)");

    toolRegistry_->registerTool<MCPGrepRequest, MCPGrepResponse>(
        "grep", [this](const MCPGrepRequest& req) { return handleGrepDocuments(req); },
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
                 {"default", false}}}}},
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
              {{"path", {{"type", "string"}, {"description", "File or directory path"}}},
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
        "Store documents (or directories) with deduplication; mirrors CLI add");

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
        "Retrieve documents from storage by hash with optional knowledge graph expansion");

    toolRegistry_->registerTool<MCPListDocumentsRequest, MCPListDocumentsResponse>(
        "list", [this](const MCPListDocumentsRequest& req) { return handleListDocuments(req); },
        json{{"type", "object"},
             {"properties",
              {{"pattern", {{"type", "string"}, {"description", "Name pattern filter"}}},
               {"name", {{"type", "string"}, {"description", "Exact name filter (optional)"}}},
               {"tags",
                {{"type", "array"},
                 {"items", {{"type", "string"}}},
                 {"description", "Filter by tags"}}},
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

    toolRegistry_->registerTool<MCPStatusRequest, MCPStatusResponse>(
        "status", [this](const MCPStatusRequest& req) { return handleGetStatus(req); },
        json{{"type", "object"},
             {"properties",
              {{"detailed",
                {{"type", "boolean"},
                 {"description", "Include verbose metrics"},
                 {"default", false}}}}}},
        "Get daemon status, readiness, and metrics");

    toolRegistry_->registerTool<MCPDoctorRequest, MCPDoctorResponse>(
        "doctor", [this](const MCPDoctorRequest& req) { return handleDoctor(req); },
        json{{"type", "object"},
             {"properties",
              {{"verbose",
                {{"type", "boolean"}, {"description", "Verbose output"}, {"default", true}}}}}},
        "Diagnose daemon readiness and provide actionable suggestions");

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
              {{"name",
                {{"type", "string"}, {"description", "Document name (basename or subpath)"}}},
               {"path", {{"type", "string"}, {"description", "Explicit path for exact match"}}},
               {"subpath",
                {{"type", "boolean"},
                 {"description", "Allow suffix match when exact path not found"},
                 {"default", true}}},
               {"raw_content",
                {{"type", "boolean"},
                 {"description", "Return raw content without text extraction"},
                 {"default", false}}},
               {"extract_text",
                {{"type", "boolean"},
                 {"description", "Extract text from HTML/PDF files"},
                 {"default", true}}},
               {"latest",
                {{"type", "boolean"},
                 {"description", "Select newest match when ambiguous"},
                 {"default", true}}},
               {"oldest",
                {{"type", "boolean"},
                 {"description", "Select oldest match when ambiguous"},
                 {"default", false}}}}}},
        "Retrieve document content by name or path");

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
                 {"default", true}}},
               {"latest",
                {{"type", "boolean"},
                 {"description", "Select newest match when ambiguous"},
                 {"default", true}}},
               {"oldest",
                {{"type", "boolean"},
                 {"description", "Select oldest match when ambiguous"},
                 {"default", false}}}}}},
        "Display document content by hash or name");

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
        "Update metadata/tags by hash, name, path, names[], or pattern");

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
            "Start (and optionally warm) a session with default budgets");

        // session_stop
        toolRegistry_->registerTool<MCPSessionStopRequest, MCPSessionStopResponse>(
            "session_stop",
            [this](const MCPSessionStopRequest& req) { return handleSessionStop(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Session name (optional)"}}},
                   {"clear", {{"type", "boolean"}, {"default", true}}}}}},
            "Stop session (clear materialized cache)");

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
            "Pin documents by path pattern (adds 'pinned' tag and updates repo)");

        // session_unpin
        toolRegistry_->registerTool<MCPSessionUnpinRequest, MCPSessionUnpinResponse>(
            "session_unpin",
            [this](const MCPSessionUnpinRequest& req) { return handleSessionUnpin(req); },
            json{{"type", "object"},
                 {"properties",
                  {{"path", {{"type", "string"}, {"description", "Path glob pattern to unpin"}}}}},
                 {"required", json::array({"path"})}},
            "Unpin documents by path pattern by removing 'pinned' tag");
    }

    // Collection/Snapshot tools (consider these as standard or extension based on use case)
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

    if (areYamsExtensionsEnabled()) {
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
}

json MCPServer::createResponse(const json& id, const json& result) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
}

json MCPServer::createError(const json& id, int code, const std::string& message) {
    return json{{"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}};
}

void MCPServer::recordEarlyFeatureUse() {
    if (!initializedNotificationSeen_.load()) {
        earlyFeatureUse_ = true;
    }
}

boost::asio::awaitable<Result<MCPGetByNameResponse>>
MCPServer::handleGetByName(const MCPGetByNameRequest& req) {
    // Path-first resolution: if explicit path provided or name includes a subpath,
    // use document service to resolve exact path or suffix, then retrieve by hash.
    if (!req.path.empty() ||
        (req.name.find('/') != std::string::npos || req.name.find('\\') != std::string::npos)) {
        auto docService = app::services::makeDocumentService(appContext_);
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
            // If a path-like query yields no results, fail explicitly instead of falling through.
            // This makes the behavior more predictable for callers.
            co_return Error{ErrorCode::NotFound, "document not found by path: " + wanted};
        } else {
            // Disambiguate
            if (req.latest || req.oldest) {
                std::ranges::sort(
                    matches, [](const auto& a, const auto& b) { return a.indexed < b.indexed; });
                const auto pick = req.latest ? matches.back() : matches.front();
                // Retrieve content by hash via RetrievalService
                yams::app::services::RetrievalService rsvc;
                yams::app::services::RetrievalOptions ropts;
                yams::app::services::GetOptions greq;
                greq.hash = pick.hash;
                greq.metadataOnly = false;
                auto grres = rsvc.get(greq, ropts);
                if (!grres)
                    co_return grres.error();
                const auto& gr = grres.value();
                MCPGetByNameResponse out;
                out.size = gr.size;
                out.hash = gr.hash;
                out.name = gr.name;
                out.path = gr.path;
                out.mimeType = gr.mimeType;
                if (!gr.content.empty()) {
                    constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
                    out.content = gr.content.size() <= MAX_BYTES ? gr.content
                                                                 : gr.content.substr(0, MAX_BYTES);
                }
                co_return out;
            }

            // If not choosing latest/oldest, prefer exact match by path equality
            auto exactIt = std::find_if(matches.begin(), matches.end(),
                                        [&](const auto& d) { return d.path == wanted; });
            const auto chosen = (exactIt != matches.end()) ? *exactIt : matches.front();

            yams::app::services::RetrievalService rsvc;
            yams::app::services::RetrievalOptions ropts;
            yams::app::services::GetOptions greq;
            greq.hash = chosen.hash;
            greq.metadataOnly = false;
            auto grres = rsvc.get(greq, ropts);
            if (!grres)
                co_return grres.error();
            const auto& gr = grres.value();
            MCPGetByNameResponse out;
            out.size = gr.size;
            out.hash = gr.hash;
            out.name = gr.name;
            out.path = gr.path;
            out.mimeType = gr.mimeType;
            if (!gr.content.empty()) {
                constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
                out.content =
                    gr.content.size() <= MAX_BYTES ? gr.content : gr.content.substr(0, MAX_BYTES);
            }
            co_return out;
        }
    }

    // Try smart retrieval first, then fallback to base-name list + fuzzy selection
    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.requestTimeoutMs = 60000;
    ropts.headerTimeoutMs = 30000;
    ropts.bodyTimeoutMs = 120000;
    auto resolver = [this](const std::string& nm) -> Result<std::string> {
        if (documentService_)
            return documentService_->resolveNameToHash(nm);
        return Error{ErrorCode::NotFound, "resolver unavailable"};
    };

    auto r = rsvc.getByNameSmart(req.name, req.oldest, true, /*useSession*/ false, std::string{},
                                 ropts, resolver);

    yams::daemon::GetResponse gr;
    if (r) {
        gr = r.value();
    } else {
        // Fallback path: search by base filename using list + simple fuzzy
        auto tryList = [&](const std::string& pat) -> std::optional<yams::daemon::ListResponse> {
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
                return chosen ? std::optional<yams::daemon::ListEntry>(*chosen) : std::nullopt;
            }
            auto scoreName = [&](const std::string& base) -> int {
                if (base == req.name)
                    return 1000;
                if (base.size() >= req.name.size() && base.rfind(req.name, 0) == 0)
                    return 800;
                if (base.find(req.name) != std::string::npos)
                    return 600;
                int dl =
                    static_cast<int>(std::abs(static_cast<long>(base.size() - req.name.size())));
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
        greq.hash = cand->hash;
        greq.metadataOnly = false;
        auto grres = rsvc.get(greq, ropts);
        if (!grres)
            co_return grres.error();
        gr = grres.value();
    }

    MCPGetByNameResponse out;
    out.size = gr.size;
    out.hash = gr.hash;
    out.name = gr.name;
    out.path = gr.path;
    out.mimeType = gr.mimeType;
    if (!gr.content.empty()) {
        constexpr std::size_t MAX_BYTES = 1 * 1024 * 1024;
        out.content = gr.content.size() <= MAX_BYTES ? gr.content : gr.content.substr(0, MAX_BYTES);
    }
    co_return out;
}

boost::asio::awaitable<Result<MCPDeleteByNameResponse>>
MCPServer::handleDeleteByName(const MCPDeleteByNameRequest& req) {
    daemon::DeleteRequest daemon_req;
    daemon_req.name = req.name;
    daemon_req.names = req.names;
    daemon_req.pattern = req.pattern;
    daemon_req.dryRun = req.dryRun;
    if (auto ensure = ensureDaemonClient(); !ensure) {
        co_return ensure.error();
    }
    auto dres = co_await daemon_client_->remove(daemon_req);
    if (!dres)
        co_return dres.error();
    MCPDeleteByNameResponse out;
    out.count = 0; // Protocol returns SuccessResponse; detailed per-item results unavailable here
    out.dryRun = req.dryRun;
    co_return out;
}

boost::asio::awaitable<yams::Result<yams::mcp::MCPCatDocumentResponse>>
yams::mcp::MCPServer::handleCatDocument(const yams::mcp::MCPCatDocumentRequest& req) {
    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.requestTimeoutMs = 60000;
    ropts.headerTimeoutMs = 30000;
    ropts.bodyTimeoutMs = 120000;

    yams::app::services::GetOptions dreq;
    dreq.hash = req.hash;
    dreq.name = req.name;
    dreq.byName = !req.name.empty();
    dreq.raw = req.rawContent;
    dreq.extract = req.extractText;
    dreq.latest = req.latest;
    dreq.oldest = req.oldest;
    dreq.metadataOnly = false; // cat always needs content

    auto gres = rsvc.get(dreq, ropts);
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
yams::mcp::MCPServer::handleRestoreCollection(const yams::mcp::MCPRestoreCollectionRequest& req) {
    try {
        if (!daemon_client_) {
            co_return Error{ErrorCode::NotInitialized, "Daemon client not initialized"};
        }

        if (req.collection.empty()) {
            co_return Error{ErrorCode::InvalidArgument, "Collection name is required"};
        }

        spdlog::debug("MCP handleRestoreCollection: restoring collection '{}' via daemon",
                      req.collection);

        daemon::RestoreCollectionRequest daemonReq;
        daemonReq.collection = req.collection;
        daemonReq.outputDirectory = req.outputDirectory;
        daemonReq.layoutTemplate = req.layoutTemplate;
        daemonReq.includePatterns = req.includePatterns;
        daemonReq.excludePatterns = req.excludePatterns;
        daemonReq.overwrite = req.overwrite;
        daemonReq.createDirs = req.createDirs;
        daemonReq.dryRun = req.dryRun;

        auto daemonResp = co_await daemon_client_->restoreCollection(daemonReq);

        if (!daemonResp) {
            co_return Error{daemonResp.error().code,
                            "Failed to restore collection: " + daemonResp.error().message};
        }

        MCPRestoreCollectionResponse response;
        response.filesRestored = daemonResp.value().filesRestored;
        response.dryRun = daemonResp.value().dryRun;

        for (const auto& file : daemonResp.value().files) {
            if (!file.skipped) {
                response.restoredPaths.push_back(file.path);
            }
        }

        spdlog::info("MCP handleRestoreCollection: restored {} files (dry_run={})",
                     response.filesRestored, response.dryRun);

        co_return response;
    } catch (const std::exception& e) {
        spdlog::error("MCP handleRestoreCollection exception: {}", e.what());
        co_return Error{ErrorCode::InternalError,
                        std::string("Restore collection failed: ") + e.what()};
    }
}

boost::asio::awaitable<yams::Result<yams::mcp::MCPRestoreSnapshotResponse>>
yams::mcp::MCPServer::handleRestoreSnapshot(const yams::mcp::MCPRestoreSnapshotRequest& req) {
    try {
        if (!daemon_client_) {
            co_return Error{ErrorCode::NotInitialized, "Daemon client not initialized"};
        }

        if (req.snapshotId.empty()) {
            co_return Error{ErrorCode::InvalidArgument, "Snapshot ID is required"};
        }

        spdlog::debug("MCP handleRestoreSnapshot: restoring snapshot '{}' via daemon",
                      req.snapshotId);

        daemon::RestoreSnapshotRequest daemonReq;
        daemonReq.snapshotId = req.snapshotId;
        daemonReq.outputDirectory = req.outputDirectory;
        daemonReq.layoutTemplate = req.layoutTemplate;
        daemonReq.includePatterns = req.includePatterns;
        daemonReq.excludePatterns = req.excludePatterns;
        daemonReq.overwrite = req.overwrite;
        daemonReq.createDirs = req.createDirs;
        daemonReq.dryRun = req.dryRun;

        auto daemonResp = co_await daemon_client_->restoreSnapshot(daemonReq);

        if (!daemonResp) {
            co_return Error{daemonResp.error().code,
                            "Failed to restore snapshot: " + daemonResp.error().message};
        }

        MCPRestoreSnapshotResponse response;
        response.filesRestored = daemonResp.value().filesRestored;
        response.dryRun = daemonResp.value().dryRun;

        for (const auto& file : daemonResp.value().files) {
            if (!file.skipped) {
                response.restoredPaths.push_back(file.path);
            }
        }

        spdlog::info("MCP handleRestoreSnapshot: restored {} files (dry_run={})",
                     response.filesRestored, response.dryRun);

        co_return response;
    } catch (const std::exception& e) {
        spdlog::error("MCP handleRestoreSnapshot exception: {}", e.what());
        co_return Error{ErrorCode::InternalError,
                        std::string("Restore snapshot failed: ") + e.what()};
    }
}

boost::asio::awaitable<Result<MCPListCollectionsResponse>>
MCPServer::handleListCollections([[maybe_unused]] const MCPListCollectionsRequest& req) {
    if (!daemon_client_) {
        co_return Error{ErrorCode::NotInitialized, "Daemon client not initialized"};
    }

    daemon::ListCollectionsRequest daemonReq;
    auto daemonResp = co_await daemon_client_->listCollections(daemonReq);

    if (!daemonResp) {
        co_return Error{daemonResp.error().code,
                        "Failed to list collections: " + daemonResp.error().message};
    }

    MCPListCollectionsResponse response;
    response.collections = daemonResp.value().collections;

    co_return response;
}

boost::asio::awaitable<Result<MCPListSnapshotsResponse>>
MCPServer::handleListSnapshots([[maybe_unused]] const MCPListSnapshotsRequest& req) {
    if (!daemon_client_) {
        co_return Error{ErrorCode::NotInitialized, "Daemon client not initialized"};
    }

    daemon::ListSnapshotsRequest daemonReq;
    auto daemonResp = co_await daemon_client_->listSnapshots(daemonReq);

    if (!daemonResp) {
        co_return Error{daemonResp.error().code,
                        "Failed to list snapshots: " + daemonResp.error().message};
    }

    MCPListSnapshotsResponse response;
    for (const auto& snap : daemonResp.value().snapshots) {
        json snapJson;
        snapJson["id"] = snap.id;
        if (!snap.label.empty()) {
            snapJson["label"] = snap.label;
        }
        if (!snap.createdAt.empty()) {
            snapJson["createdAt"] = snap.createdAt;
        }
        if (snap.documentCount > 0) {
            snapJson["documentCount"] = snap.documentCount;
        }
        response.snapshots.push_back(std::move(snapJson));
    }

    co_return response;
}

// === Thread pool implementation for MCPServer ===
void MCPServer::startThreadPool(std::size_t threads) {
    stopWorkers_.store(false);
    workerPool_.reserve(threads);
    try {
        for (std::size_t i = 0; i < threads; ++i) {
            workerPool_.emplace_back([this]() {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(taskMutex_);
                        taskCv_.wait(
                            lk, [this]() { return stopWorkers_.load() || !taskQueue_.empty(); });
                        if (stopWorkers_.load() && taskQueue_.empty()) {
                            return;
                        }
                        task = std::move(taskQueue_.front());
                        taskQueue_.pop_front();
                    }
                    mcpWorkerActive_.fetch_add(1, std::memory_order_relaxed);
                    try {
                        task();
                        mcpWorkerProcessed_.fetch_add(1, std::memory_order_relaxed);
                    } catch (...) {
                        mcpWorkerFailed_.fetch_add(1, std::memory_order_relaxed);
                        // Swallow to keep worker alive
                    }
                    mcpWorkerActive_.fetch_sub(1, std::memory_order_relaxed);
                }
            });
        }
    } catch (...) {
        spdlog::error("Failed to create MCP worker thread, cleaning up {} existing workers",
                      workerPool_.size());
        stopWorkers_.store(true);
        taskCv_.notify_all();
        for (auto& worker : workerPool_) {
            if (worker.joinable()) {
                try {
                    worker.join();
                } catch (...) {
                }
            }
        }
        workerPool_.clear();
        throw; // Rethrow to propagate error
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

// ---------------- Lifecycle helper implementations ----------------

bool MCPServer::isMethodAllowedBeforeInitialization(const std::string& method) {
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
    lifecycleState_.store(McpLifecycleState::Ready);
}

void MCPServer::handleExitRequest() {
    spdlog::info("MCP server received 'exit' request");
    exitRequested_.store(true);
    running_.store(false);
    if (transport_) {
        transport_->close();
    }
}

// --- Cancellation + capability helpers ---

void MCPServer::registerCancelable(const nlohmann::json& id) {
    if (id.is_null())
        return;
    std::lock_guard<std::mutex> lk(cancelMutex_);
    std::string key = id.is_string() ? id.get<std::string>() : id.dump();
    if (cancelTokens_.find(key) == cancelTokens_.end()) {
        cancelTokens_[key] = std::make_shared<std::atomic<bool>>(false);
    }
}

void MCPServer::cancelRequest(const nlohmann::json& id) {
    if (id.is_null())
        return;
    std::lock_guard<std::mutex> lk(cancelMutex_);
    std::string key = id.is_string() ? id.get<std::string>() : id.dump();
    auto it = cancelTokens_.find(key);
    if (it != cancelTokens_.end()) {
        it->second->store(true);
    }
}

bool MCPServer::isCanceled(const nlohmann::json& id) const {
    if (id.is_null())
        return false;
    std::lock_guard<std::mutex> lk(cancelMutex_);
    std::string key = id.is_string() ? id.get<std::string>() : id.dump();
    auto it = cancelTokens_.find(key);
    if (it == cancelTokens_.end())
        return false;
    return it->second->load();
}

json MCPServer::buildServerCapabilities() const {
    json caps = {{"tools", json({{"listChanged", false}})},
                 {"prompts", json({{"listChanged", false}})},
                 {"resources", json({{"subscribe", false}, {"listChanged", false}})},
                 {"logging", json::object()}};
    // Augment with extended flags
    caps["experimental"] = json::object();
    caps["experimental"]["cancellation"] = cancellationSupported_;
    caps["experimental"]["progress"] = progressSupported_;

    return caps;
}

// --- Cancel & Progress Helper Implementations ---
void MCPServer::handleCancelRequest(const nlohmann::json& params,
                                    [[maybe_unused]] const nlohmann::json& id) {
    // Expect params: { "id": <original request id> } but also accept { "requestId": ... }
    nlohmann::json target;
    if (params.contains("id")) {
        target = params["id"];
    } else if (params.contains("requestId")) {
        target = params["requestId"];
    } else {
        spdlog::warn("cancel: missing id/requestId field");
        return;
    }
    cancelRequest(target);
    spdlog::info("Cancel requested for original id '{}'",
                 target.is_string() ? target.get<std::string>() : target.dump());
    // Optionally emit a progress notification indicating cancellation acknowledged
    sendProgress("cancel", 100.0, "Cancellation acknowledged");
}

void MCPServer::sendProgress(const std::string& /*phase*/, double percent,
                             const std::string& message,
                             std::optional<nlohmann::json> progressToken) {
    // Per MCP spec, notifications/progress MUST include a progressToken from the request's
    // params._meta.progressToken. If absent, do not emit a progress notification.
    nlohmann::json token = nullptr;
    if (progressToken)
        token = *progressToken;

    if (token.is_null()) {
        return; // No valid token available; skip
    }

    double clamped = percent;
    if (clamped < 0.0)
        clamped = 0.0;
    if (clamped > 100.0)
        clamped = 100.0;

    json p = {{"progressToken", token}, {"progress", clamped}, {"total", 100.0}};
    if (!message.empty())
        p["message"] = message;
    sendResponse({{"jsonrpc", "2.0"}, {"method", "notifications/progress"}, {"params", p}});
}

bool MCPServer::shouldAutoInitialize() {
    return false;
}

} // namespace yams::mcp
