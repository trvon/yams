#include <boost/asio/local/stream_protocol.hpp>
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/daemon_helpers.h>
#include <yams/config/config_helpers.h>
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
#include <yams/metadata/query_helpers.h>
#include <yams/version.hpp>


#ifdef _WIN32
#include <cstdlib>
// Windows implementation of setenv
inline int setenv(const char *name, const char *value, int overwrite) {
    int errcode = 0;
    if (!overwrite) {
        size_t envsize = 0;
        errcode = getenv_s(&envsize, NULL, 0, name);
        if (errcode || envsize) return errcode;
    }
    return _putenv_s(name, value);
}
#endif

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

// Stdio send helper: sends JSON-RPC messages via stdio transport with buffering
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
    // Use buffered sending for large payloads to prevent "End of file" errors
    if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) {
        constexpr size_t kLargePayloadThreshold = 256 * 1024; // 256KB
        if (payload.size() > kLargePayloadThreshold) {
            spdlog::debug("sendResponse: using buffered send for large payload ({} bytes)",
                          payload.size());
            stdio->sendFramedSerialized(payload);
        } else {
            stdio->send(message);
        }
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

        constexpr size_t kChunkThreshold = 512 * 1024; // 512KB
        constexpr size_t kChunkSize = 64 * 1024;       // 64KB chunks

        // For large payloads, use chunked writing to prevent buffer overflow/"End of file" errors
        if (payload.size() > kChunkThreshold) {
            size_t offset = 0;
            const size_t totalSize = payload.size();

            while (offset < totalSize) {
                size_t remaining = totalSize - offset;
                size_t chunkLen = std::min(remaining, kChunkSize);

                // Write chunk with explicit flush
                std::cout.write(payload.data() + offset, static_cast<std::streamsize>(chunkLen));
                std::cout.flush();

                offset += chunkLen;
            }

            // Write final newline per NDJSON spec
            std::cout << '\n';
            std::cout.flush();
        } else {
            // Small payloads: send as-is (original behavior)
            std::cout << payload << '\n';
            std::cout.flush();
        }
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
            std::filesystem::path configPath = yams::config::get_config_path();
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
        // Next: platform-specific data dir + prompts
        if (promptsDir_.empty()) {
            promptsDir_ = yams::config::get_data_dir() / "prompts";
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

    return tools;
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

void MCPServer::initializeToolRegistry() {
    // Registry initialization if needed
}

json MCPServer::listPrompts() {
    return json{{"prompts", json::array()}};
}

json MCPServer::callTool(const std::string& name, const json& arguments) {
    std::promise<json> promise;
    auto future = promise.get_future();

    boost::asio::co_spawn(
        appContext_.workerExecutor,
        [this, name, arguments]() -> boost::asio::awaitable<json> {
            co_return co_await callToolAsync(name, arguments);
        },
        [&promise](std::exception_ptr e, json result) {
            if (e) {
                try {
                    std::rethrow_exception(e);
                } catch (const std::exception& ex) {
                    promise.set_value(json{{"content", json::array({json{{"type", "text"}, {"text", std::string("Error: ") + ex.what()}}})}, {"isError", true}});
                } catch (...) {
                    promise.set_value(json{{"content", json::array({json{{"type", "text"}, {"text", "Unknown error"}}})}, {"isError", true}});
                }
            } else {
                promise.set_value(result);
            }
        }
    );

    return future.get();
}

boost::asio::awaitable<json> MCPServer::callToolAsync(const std::string& name, const json& arguments) {
    try {
        if (name == "search") {
            MCPSearchRequest req;
            req.query = arguments.value("query", "");
            req.limit = arguments.value("limit", 20);
            // ... map other args
            auto res = co_await handleSearchDocuments(req);
            if (!res) co_return json{{"content", json::array({json{{"type", "text"}, {"text", res.error().message}}})}, {"isError", true}};
            // Convert response to json (simplified)
            json content = json::array();
            for (const auto& doc : res.value().results) {
                content.push_back(json{{"type", "text"}, {"text", doc.path}});
            }
            co_return json{{"content", content}, {"isError", false}};
        }
        // Add other tools here...
        
        co_return json{{"content", json::array({json{{"type", "text"}, {"text", "Tool not implemented: " + name}}})}, {"isError", true}};
    } catch (const std::exception& e) {
        co_return json{{"content", json::array({json{{"type", "text"}, {"text", std::string("Exception: ") + e.what()}}})}, {"isError", true}};
    }
}

boost::asio::awaitable<Result<MCPSearchResponse>>
MCPServer::handleSearchDocuments(const MCPSearchRequest& req) {
    // Stub implementation
    MCPSearchResponse res;
    co_return res;
}

boost::asio::awaitable<Result<MCPGrepResponse>>
MCPServer::handleGrepDocuments(const MCPGrepRequest& req) {
    // Stub implementation
    MCPGrepResponse res;
    co_return res;
}

boost::asio::awaitable<Result<MCPRetrieveDocumentResponse>>
MCPServer::handleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
    // Stub implementation
    co_return Error{ErrorCode::InternalError, "Not implemented"};
}

boost::asio::awaitable<Result<MCPListDocumentsResponse>>
MCPServer::handleListDocuments(const MCPListDocumentsRequest& req) {
    // Stub implementation
    MCPListDocumentsResponse res;
    co_return res;
}

boost::asio::awaitable<Result<MCPStatsResponse>>
MCPServer::handleGetStats(const MCPStatsRequest& req) {
    // Stub implementation
    MCPStatsResponse res;
    co_return res;
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
                    out.content = gr.content.size() <= MAX_BYTES ? gr.content : gr.content.substr(0, MAX_BYTES);
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
