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

// In-band logging helper (level + message variant) - YAMS extension, not standard MCP
static nlohmann::json createLogNotification(const std::string& level, const std::string& message) {
    // NOTE: This is a YAMS-specific extension. Standard MCP only supports notifications/log from
    // client->server Wrap simple textual message inside a data object for consistency with the
    // (level,data,logger) overload
    nlohmann::json params = {{"level", level}, {"data", nlohmann::json{{"message", message}}}};
    return {{"jsonrpc", "2.0"}, {"method", "notifications/message"}, {"params", params}};
}

void MCPServer::sendResponse(const nlohmann::json& message) {
    if (message.is_array()) {
        for (const auto& entry : message) {
            if (entry.is_object()) {
                sendResponse(entry);
            }
        }
        return;
    }

    // Serialize once for both logging, telemetry and transport
    std::string payload;
    try {
        payload = message.dump();
    } catch (const std::exception& e) {
        spdlog::error("sendResponse: serialization failed: {}", e.what());
        return;
    }

    size_t maxPayloadBytes = 64 * 1024;
    if (const char* env = std::getenv("YAMS_MCP_MAX_OUTPUT_BYTES")) {
        try {
            maxPayloadBytes = std::max<size_t>(4096, static_cast<size_t>(std::stoul(env)));
        } catch (...) {
            maxPayloadBytes = 64 * 1024;
        }
    }

    if (payload.size() > maxPayloadBytes && message.is_object()) {
        nlohmann::json adjusted = message;
        bool shrunk = false;

        try {
            if (adjusted.contains("result") && adjusted["result"].is_object()) {
                auto& result = adjusted["result"];
                if (result.contains("output") && result["output"].is_string()) {
                    const auto& output = result["output"].get_ref<const std::string&>();
                    const size_t chunkSize = std::max<size_t>(1024, maxPayloadBytes / 4);
                    const size_t total = (output.size() + chunkSize - 1) / chunkSize;

                    for (size_t i = 0; i < total; ++i) {
                        const size_t start = i * chunkSize;
                        const size_t len = std::min(chunkSize, output.size() - start);
                        nlohmann::json notif = createLogNotification(
                            "info", "mcp output chunk " + std::to_string(i + 1) + "/" +
                                        std::to_string(total));
                        notif["params"]["data"]["chunk_index"] = i + 1;
                        notif["params"]["data"]["chunk_total"] = total;
                        notif["params"]["data"]["output_chunk"] = output.substr(start, len);
                        try {
                            auto chunkPayload = notif.dump();
                            if (cachedStdioTransport_) {
                                cachedStdioTransport_->sendFramedSerialized(chunkPayload);
                            } else if (auto* stdio =
                                           dynamic_cast<StdioTransport*>(transport_.get())) {
                                cachedStdioTransport_ = stdio;
                                stdio->sendFramedSerialized(chunkPayload);
                            } else {
                                enqueueOutbound(std::move(chunkPayload));
                            }
                        } catch (...) {
                            break;
                        }
                    }

                    result["output"] = "[truncated: output sent via notifications/message chunks]";
                    result["truncated"] = true;
                    result["chunk_count"] = total;
                    shrunk = true;
                }
            }
        } catch (...) {
            // fall through
        }

        if (shrunk) {
            try {
                payload = adjusted.dump();
            } catch (...) {
            }
        }

        if (payload.size() > maxPayloadBytes) {
            nlohmann::json errorResp =
                createError(message.value("id", nlohmann::json(nullptr)), protocol::INTERNAL_ERROR,
                            "response too large; truncated");
            payload = errorResp.dump();
        }
    }

    spdlog::debug("MCP server sending response: {}", payload);

    // HTTP publish path (notifications). Do not short-circuit stdio delivery.
    if (!tlsSessionId_.empty() && httpPublisher_) {
        try {
            if (message.is_object() && message.contains("method")) {
                httpPublisher_(tlsSessionId_, message);
            }
        } catch (...) {
            // best effort; always continue to stdio
        }
    }

    telemetrySentBytes_.fetch_add(static_cast<uint64_t>(payload.size()));

    // Fast corruption check - look for null values in key fields
    // This is faster than string searching for specific patterns
    if (message.is_object()) {
        auto it = message.find("jsonrpc");
        if (it != message.end() && it->is_null()) {
            spdlog::error("MCP CORRUPTION SUSPECTED BEFORE SEND: {}", payload);
            telemetryIntegrityFailures_.fetch_add(1);
        }
    }

    // Prefer immediate synchronous flush for stdio transport; fallback to queue
    // Cache the stdio transport pointer to avoid repeated dynamic_cast
    if (cachedStdioTransport_) {
        cachedStdioTransport_->sendFramedSerialized(payload);
        return;
    }

    if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) {
        cachedStdioTransport_ = stdio;
        stdio->sendFramedSerialized(payload);
        return;
    }

    enqueueOutbound(std::move(payload));
}

// Enqueue payload and start drain coroutine if idle
void MCPServer::enqueueOutbound(std::string payload) {
    {
        std::lock_guard<std::mutex> lk(outboundMutex_);
        outboundQueue_.push_back(std::move(payload));
        // If not currently draining, start the drain coroutine
        bool expected = false;
        if (!outboundDraining_.compare_exchange_strong(expected, true)) {
            return; // drain already active
        }
    }
    if (outboundStrand_) {
        boost::asio::co_spawn(*outboundStrand_, outboundDrainAsync(), boost::asio::detached);
    } else {
        // Best-effort fallback: use a global/system executor.
#if defined(YAMS_WASI)
        boost::asio::co_spawn(boost::asio::system_executor(), outboundDrainAsync(),
                              boost::asio::detached);
#else
        boost::asio::co_spawn(yams::daemon::GlobalIOContext::global_executor(),
                              outboundDrainAsync(), boost::asio::detached);
#endif
    }
}

// Drain queue sequentially; choose best transport per message
boost::asio::awaitable<void> MCPServer::outboundDrainAsync() {
    for (;;) {
        std::string next;
        {
            std::lock_guard<std::mutex> lk(outboundMutex_);
            if (outboundQueue_.empty()) {
                // Mark not draining, but re-check in case a producer raced us
                outboundDraining_.store(false);
                if (outboundQueue_.empty()) {
                    break;
                }
                // New item arrived after store(false); claim draining again
                outboundDraining_.store(true);
            }
            next = std::move(outboundQueue_.front());
            outboundQueue_.pop_front();
        }

        if (auto* stdio = dynamic_cast<StdioTransport*>(transport_.get())) {
            // Synchronous path; next is a serialized JSON string. Use framed string sender
            stdio->sendFramedSerialized(next);
        } else {
            // No supported transport for raw framed write; drop with error
            spdlog::error("outboundDrainAsync: unsupported transport type for framed write; "
                          "dropping message");
        }
    }
    co_return;
}

// MCPServer implementation
MCPServer::MCPServer(std::unique_ptr<ITransport> transport, std::atomic<bool>* externalShutdown,
                     std::filesystem::path overrideSocket,
                     const std::optional<boost::asio::any_io_executor>& executor)
    : transport_(std::move(transport)), externalShutdown_(externalShutdown), exitRequested_{false},
      shutdownRequested_{false}, strictProtocol_(false), limitToolResultDup_(true),
      daemonSocketOverride_(std::move(overrideSocket)) {
    (void)executor; // Reserved for future use
    // Generate unique instance ID for this MCP server connection
    instanceId_ = yams::core::generateUUID();
    // Ensure logging goes to stderr to keep stdout clean for MCP framing.
    // Only rebind default logger for stdio transport; in-process test transports
    // should not mutate the global logger from arbitrary worker threads.
    if (dynamic_cast<StdioTransport*>(transport_.get()) != nullptr) {
        if (auto existing = spdlog::get("yams-mcp")) {
            spdlog::set_default_logger(std::move(existing));
        } else {
            auto logger = spdlog::stderr_color_mt("yams-mcp");
            spdlog::set_default_logger(std::move(logger));
        }
    }
    // Set external shutdown flag on StdioTransport if applicable
    if (auto* stdioTransport = dynamic_cast<StdioTransport*>(transport_.get())) {
        stdioTransport->setShutdownFlag(externalShutdown_);
    }

    // Initialize a single multiplexed daemon client; rely on DaemonClient defaults for dataDir
    {
#if !defined(YAMS_WASI)
        yams::daemon::ClientConfig cfg;
        // Use override socket if provided, otherwise resolve from config
        if (!daemonSocketOverride_.empty()) {
            cfg.socketPath = daemonSocketOverride_;
        } else {
            cfg.socketPath = yams::daemon::socket_utils::resolve_socket_path_config_first();
        }
        cfg.enableChunkedResponses = true;
        cfg.singleUseConnections = false;
        cfg.requestTimeout = std::chrono::milliseconds(15000);
        cfg.headerTimeout = std::chrono::milliseconds(10000);
        cfg.bodyTimeout = std::chrono::milliseconds(60000);
        cfg.maxInflight = 128;
        cfg.autoStart = false; // MCP server should not be responsible for starting the daemon
        daemon_client_config_ = cfg;
        daemon_client_shared_ = std::make_shared<yams::daemon::DaemonClient>(cfg);
        daemon_client_ = daemon_client_shared_.get();
        retrieval_svc_ = std::make_unique<app::services::RetrievalService>(daemon_client_shared_);
        ingestion_svc_ =
            std::make_unique<app::services::DocumentIngestionService>(daemon_client_shared_);
#endif
    }
    // Legacy pool config removed

    // Initialize the tool registry with modern handlers
    initializeToolRegistry();

    // Set default handshake behavior from environment variables
    enableYamsExtensions_ = true;
    if (const char* env = std::getenv("YAMS_DISABLE_EXTENSIONS")) {
        enableYamsExtensions_ = !(std::string(env) == "1" || std::string(env) == "true");
    }
    if (const char* env = std::getenv("YAMS_MCP_STRICT_PROTOCOL")) {
        strictProtocol_ = (std::string(env) == "1" || std::string(env) == "true");
    }

    if (const char* env = std::getenv("YAMS_MCP_LIMIT_DUP_CONTENT")) {
        limitToolResultDup_ = !(std::string(env) == "0" || std::string(env) == "false");
    }

    // Initialize outbound strand for serialized writes.
    // For WASI builds we don't have the daemon global IO context.
    {
#if defined(YAMS_WASI)
        outboundStrand_ = std::make_unique<boost::asio::strand<boost::asio::any_io_executor>>(
            boost::asio::system_executor());
#else
        outboundStrand_ = std::make_unique<boost::asio::strand<boost::asio::any_io_executor>>(
            yams::daemon::GlobalIOContext::global_executor());
#endif
    }

    // Resolve prompts directory (file-backed templates)
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
                promptsDir_ = std::move(p);
            }
        }
        // Last: local docs/prompts (useful for dev runs from the repo root)
        if (!std::filesystem::exists(promptsDir_)) {
            auto localDocs = std::filesystem::current_path() / "docs" / "prompts";
            if (std::filesystem::exists(localDocs)) {
                promptsDir_ = std::move(localDocs);
            }
        }
        if (!promptsDir_.empty()) {
            spdlog::info("MCP prompts directory resolved to: {}", promptsDir_.string());
        }
    } catch (...) {
        // Ignore prompt dir resolution errors; built-ins remain available
    }
}

Result<void> MCPServer::ensureDaemonClient() {
#if defined(YAMS_WASI)
    return Error{ErrorCode::NotSupported, "Daemon client not available on WASI"};
#else
    if (testEnsureDaemonClientHook_) {
        auto hookResult = testEnsureDaemonClientHook_(daemon_client_config_);
        if (!hookResult) {
            return hookResult;
        }
    }
    if (daemon_client_ && daemon_client_shared_)
        return Result<void>();
    daemon_client_shared_ = std::make_shared<yams::daemon::DaemonClient>(daemon_client_config_);
    daemon_client_ = daemon_client_shared_.get();

    // Initialize service facades sharing the daemon client
    if (!retrieval_svc_) {
        retrieval_svc_ = std::make_unique<app::services::RetrievalService>(daemon_client_shared_);
        ingestion_svc_ =
            std::make_unique<app::services::DocumentIngestionService>(daemon_client_shared_);
    }

    return Result<void>();
#endif
}

Result<yams::daemon::DaemonClient*> MCPServer::requireDaemonClient() {
    if (auto ensure = ensureDaemonClient(); !ensure) {
        return ensure.error();
    }
    return daemon_client_;
}

boost::asio::awaitable<Result<std::optional<yams::daemon::StatusResponse>>>
MCPServer::fetchDaemonStatus(DaemonStatusFetchMode mode) {
#if defined(YAMS_WASI)
    (void)mode;
    co_return Error{ErrorCode::NotSupported, "Daemon status not available on WASI"};
#else
    if (auto ensure = ensureDaemonClient(); !ensure) {
        if (mode == DaemonStatusFetchMode::BestEffort) {
            co_return std::optional<yams::daemon::StatusResponse>{};
        }
        co_return ensure.error();
    }

    auto sres = co_await daemon_client_->status();
    if (!sres) {
        if (mode == DaemonStatusFetchMode::BestEffort) {
            co_return std::optional<yams::daemon::StatusResponse>{};
        }
        co_return sres.error();
    }

    co_return std::optional<yams::daemon::StatusResponse>{std::move(sres.value())};
#endif
}

MCPServer::~MCPServer() {
    stop();
}

void MCPServer::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }

    // Ensure stdout is not buffered for real-time communication
    std::cout.setf(std::ios::unitbuf);

    spdlog::info("MCP server started");

    // Main message loop with modern error handling
    try {
        int loopCount = 0;
        while (running_ && (externalShutdown_ == nullptr || *externalShutdown_)) {
            loopCount++;
            if (loopCount <= 5 || loopCount % 100 == 0) {
                spdlog::debug("MCP server loop iteration {}, running={}", loopCount,
                              running_.load());
            }

            auto messageResult = transport_->receive();

            if (!messageResult) {
                const auto& error = messageResult.error();
                spdlog::debug("MCP server receive error: code={}, message='{}'",
                              static_cast<int>(error.code), error.message);

                // Handle different error types
                switch (error.code) {
                    case ErrorCode::NetworkError:
                        spdlog::debug("Transport network error (may be transient): {}",
                                      error.message);
                        // Only exit on persistent network errors, not initial "no data" states
                        if (error.message.find("EOF") != std::string::npos ||
                            error.message.find("Disconnected") != std::string::npos) {
                            spdlog::info("Transport closed permanently: {}", error.message);
                            running_ = false;
                        }
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
            auto message = std::move(messageResult).value();
            auto processRequest = [this](const json& request) {
                if (!request.is_object()) {
                    spdlog::warn("MCP server received non-object entry in JSON-RPC batch");
                    this->sendResponse(createError(json(nullptr), protocol::INVALID_REQUEST,
                                                   "Batch entries must be JSON objects"));
                    return;
                }

                spdlog::debug("MCP server received message: {}", request.dump());
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

                // Critical handshake methods must be handled synchronously to ensure
                // response is sent before the transport closes (fixes race condition
                // with MCP clients like OpenCode that may close stdin quickly).
                static const std::unordered_set<std::string> syncMethods = {
                    "initialize",
                    "tools/list",
                    "resources/list",
                    "prompts/list",
                    "notifications/initialized",
                    "ping"};

                if (syncMethods.count(method)) {
                    spdlog::debug("Handling '{}' synchronously on main thread", method);
                    auto response = this->handleRequest(request);
                    if (response) {
                        this->sendResponse(response.value());
                    } else {
                        const auto& error = response.error();
                        json errorResponse = {
                            {"jsonrpc", protocol::JSONRPC_VERSION},
                            {"error",
                             {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                            {"id", request.value("id", nullptr)}};
                        this->sendResponse(errorResponse);
                    }
                    return;
                }

                if (method == "tools/call") {
                    const auto toolName = params.value("name", "");
                    const auto toolArgs = params.value("arguments", json::object());
                    auto id_copy = request.value("id", json{});
                    std::optional<json> progressToken;
                    try {
                        if (params.contains("_meta") && params["_meta"].is_object()) {
                            const auto& meta = params["_meta"];
                            if (meta.contains("progressToken")) {
                                progressToken = meta["progressToken"];
                            }
                        }
                    } catch (...) {
                    }
                    this->sendProgress("tool", 0.0, std::string("calling ") + toolName,
                                       progressToken);

                    // Keep the server alive while the detached coroutine runs.
                    // Without this, a fast shutdown (or scope exit) can destroy MCPServer
                    // while a tool is still executing, leading to use-after-free.
                    auto self = this->shared_from_this();

                    // Shared flag: first coroutine to CAS false→true owns the response.
                    // This guards against double-response when the deadline timer fires
                    // concurrently with the tool result.
                    auto completed = std::make_shared<std::atomic<bool>>(false);

                    // Deadline timer: fires after 30s to prevent indefinite hangs
                    // when the daemon client is slow or unresponsive.
                    boost::asio::co_spawn(
#if defined(YAMS_WASI)
                        boost::asio::system_executor(),
#else
                        yams::daemon::GlobalIOContext::global_executor(),
#endif
                        [self, toolName, id_copy, completed]() -> boost::asio::awaitable<void> {
                            auto exec = co_await boost::asio::this_coro::executor;
                            boost::asio::steady_timer timer(exec);
                            timer.expires_after(std::chrono::seconds(30));
                            co_await timer.async_wait(boost::asio::use_awaitable);
                            bool expected = false;
                            if (completed->compare_exchange_strong(expected, true)) {
                                json err = {{"code", -32603},
                                            {"message", std::string("Tool '") + toolName +
                                                            "' timed out after 30s deadline"}};
                                self->sendResponse({{"jsonrpc", protocol::JSONRPC_VERSION},
                                                    {"error", err},
                                                    {"id", id_copy}});
                            }
                            co_return;
                        },
                        boost::asio::detached);

                    // Tool execution coroutine
                    boost::asio::co_spawn(
#if defined(YAMS_WASI)
                        boost::asio::system_executor(),
#else
                        yams::daemon::GlobalIOContext::global_executor(),
#endif
                        [self, toolName, toolArgs, id_copy, progressToken,
                         completed]() -> boost::asio::awaitable<void> {
                            if (progressToken)
                                MCPServer::tlsProgressToken_ = std::move(*progressToken);
                            try {
                                json raw = co_await self->callToolAsync(toolName, toolArgs);
                                bool expected = false;
                                if (completed->compare_exchange_strong(expected, true)) {
                                    if (raw.is_object() && raw.contains("error")) {
                                        const auto& err = raw["error"];
                                        self->sendResponse({{"jsonrpc", protocol::JSONRPC_VERSION},
                                                            {"error", err},
                                                            {"id", id_copy}});
                                    } else {
                                        self->sendResponse(self->createResponse(id_copy, raw));
                                    }
                                    self->sendProgress("tool", 100.0,
                                                       std::string("completed ") + toolName,
                                                       progressToken);
                                }
                            } catch (const std::exception& e) {
                                bool expected = false;
                                if (completed->compare_exchange_strong(expected, true)) {
                                    json err = {{"code", -32603}, {"message", e.what()}};
                                    self->sendResponse({{"jsonrpc", protocol::JSONRPC_VERSION},
                                                        {"error", std::move(err)},
                                                        {"id", id_copy}});
                                }
                            } catch (...) {
                                bool expected = false;
                                if (completed->compare_exchange_strong(expected, true)) {
                                    json err = {{"code", -32603}, {"message", "Tool call failed"}};
                                    self->sendResponse({{"jsonrpc", protocol::JSONRPC_VERSION},
                                                        {"error", std::move(err)},
                                                        {"id", id_copy}});
                                }
                            }
                            MCPServer::tlsProgressToken_ = nullptr;
                            co_return;
                        },
                        boost::asio::detached);
                    return;
                }

                // All other methods - handle directly (WASI: sync, non-WASI: async)
#if defined(YAMS_WASI)
                // WASI: Run synchronously on main thread
                auto response = this->handleRequest(request);
                if (response) {
                    this->sendResponse(response.value());
                } else {
                    const auto& error = response.error();
                    json errorResponse = {
                        {"jsonrpc", protocol::JSONRPC_VERSION},
                        {"error",
                         {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                        {"id", request.value("id", nullptr)}};
                    this->sendResponse(errorResponse);
                }
#else
                // Non-WASI: Use async coroutines
                // Keep the server alive while the detached coroutine runs.
                auto self = this->shared_from_this();

                boost::asio::co_spawn(
                    yams::daemon::GlobalIOContext::global_executor(),
                    [self, req = request]() -> boost::asio::awaitable<void> {
                        auto response = co_await self->handleRequestAsync(req);
                        if (response) {
                            self->sendResponse(response.value());
                        } else {
                            const auto& error = response.error();
                            json errorResponse = {
                                {"jsonrpc", protocol::JSONRPC_VERSION},
                                {"error",
                                 {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                                {"id", req.value("id", nullptr)}};
                            self->sendResponse(errorResponse);
                        }
                    },
                    boost::asio::detached);
#endif
            };

            if (message.is_array()) {
                spdlog::debug("MCP server received JSON-RPC batch with {} entries", message.size());

                // JSON-RPC batch requests should be answered with a single JSON array response.
                // Some MCP clients (including OpenCode) rely on strict batch semantics during
                // initialization/tool discovery.
                json batchResponses = json::array();

                for (const auto& entry : message) {
                    if (!entry.is_object()) {
                        batchResponses.push_back(createError(json(nullptr),
                                                             protocol::INVALID_REQUEST,
                                                             "Batch entries must be JSON objects"));
                        continue;
                    }

                    const bool isNotification = !entry.contains("id");
                    if (isNotification) {
                        (void)this->handleRequest(entry);
                        continue;
                    }

                    auto resp = this->handleRequest(entry);
                    // handleRequest returns MessageResult; if it errors, map to a JSON-RPC error
                    // response.
                    if (resp) {
                        batchResponses.push_back(resp.value());
                    } else {
                        const auto& error = resp.error();
                        batchResponses.push_back(json{
                            {"jsonrpc", protocol::JSONRPC_VERSION},
                            {"error",
                             {{"code", protocol::INVALID_REQUEST}, {"message", error.message}}},
                            {"id", entry.value("id", nullptr)}});
                    }
                }

                if (!batchResponses.empty()) {
                    sendResponse(batchResponses);
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
        transport_->close();
    }
}
} // namespace yams::mcp
