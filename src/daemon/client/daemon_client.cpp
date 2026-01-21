#define _CRT_SECURE_NO_WARNINGS
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/client/streaming_handlers.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/socket_utils.h>

#include <yams/config/config_helpers.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <spdlog/spdlog.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#else
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

namespace yams::daemon {

namespace {
inline std::string sanitize_for_terminal(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (unsigned char c : in) {
        if (c >= 0x20 && c <= 0x7E) {
            out.push_back(static_cast<char>(c));
        } else if (c == '\n' || c == '\r' || c == '\t') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('?');
        }
    }
    return out;
}

// Request timeout categories for snappier UI on fast operations
enum class TimeoutCategory { Fast, Medium, Slow };

// Returns appropriate timeout category based on request type
TimeoutCategory getTimeoutCategory(const Request& req) {
    return std::visit(
        [](const auto& r) -> TimeoutCategory {
            using T = std::decay_t<decltype(r)>;
            // Fast operations (5s) - simple round-trips
            if constexpr (std::is_same_v<T, PingRequest> || std::is_same_v<T, StatusRequest> ||
                          std::is_same_v<T, ShutdownRequest>) {
                return TimeoutCategory::Fast;
            }
            // Fast operations - AddDocument just pushes to queue and returns immediately
            else if constexpr (std::is_same_v<T, AddDocumentRequest>) {
                return TimeoutCategory::Fast;
            }
            // Slow operations (120s) - heavy or maintenance work
            else if constexpr (std::is_same_v<T, GenerateEmbeddingRequest> ||
                               std::is_same_v<T, UpdateDocumentRequest> ||
                               std::is_same_v<T, EmbedDocumentsRequest> ||
                               std::is_same_v<T, BatchEmbeddingRequest> ||
                               std::is_same_v<T, PruneRequest>) {
                return TimeoutCategory::Slow;
            }
            // Medium operations (30s) - queries and moderate work
            else {
                return TimeoutCategory::Medium;
            }
        },
        req);
}

// Requests that should use fresh, single-use connections (avoid pooled reuse)
bool requires_single_use_connection(const Request& req) {
    return std::holds_alternative<PruneRequest>(req);
}

// Get timeout milliseconds for a category
std::chrono::milliseconds getTimeoutForCategory(TimeoutCategory cat) {
    switch (cat) {
        case TimeoutCategory::Fast:
            return std::chrono::milliseconds(5000); // 5s
        case TimeoutCategory::Medium:
            return std::chrono::milliseconds(30000); // 30s
        case TimeoutCategory::Slow:
        default:
            return std::chrono::milliseconds(120000); // 120s
    }
}

} // namespace

// Implementation class
class DaemonClient::Impl {
public:
    explicit Impl(const ClientConfig& config)
        : config_(config), headerTimeout_(config.headerTimeout), bodyTimeout_(config.bodyTimeout) {}

    ~Impl() = default;

    // Mark as shutting down - checked by coroutines to exit early
    void markShuttingDown() { shutting_down_.store(true, std::memory_order_release); }
    bool isShuttingDown() const { return shutting_down_.load(std::memory_order_acquire); }

    ClientConfig config_;
    CircuitBreaker breaker_;
    std::chrono::milliseconds headerTimeout_{30000}; // 30s default
    std::chrono::milliseconds bodyTimeout_{60000};   // 60s default
    TransportOptions transportOptions_;
    std::shared_ptr<AsioConnectionPool> pool_;
    bool explicitly_disconnected_{false};    // Track explicit disconnect() calls
    std::atomic<bool> shutting_down_{false}; // Set when DaemonClient is being destroyed

    void refresh_transport() {
        transportOptions_.socketPath = config_.socketPath;
        transportOptions_.headerTimeout = headerTimeout_;
        transportOptions_.bodyTimeout = bodyTimeout_;
        transportOptions_.requestTimeout = config_.requestTimeout;
        transportOptions_.maxInflight = config_.maxInflight;
        transportOptions_.poolEnabled = !config_.singleUseConnections;
        transportOptions_.executor = config_.executor;
        pool_ = AsioConnectionPool::get_or_create(transportOptions_);
    }
};

// Cached data directory resolution - parsed once, reused across all DaemonClient instances
// This avoids repeated config file I/O on every CLI command
namespace {
std::once_flag g_dataDirOnce;
std::filesystem::path g_cachedDataDir;

std::filesystem::path resolveDataDirCached() {
    std::call_once(g_dataDirOnce, []() {
        namespace fs = std::filesystem;
        auto expand_tilde = [](const std::string& p) -> std::string {
            if (!p.empty() && p.front() == '~') {
                if (const char* home = std::getenv("HOME"))
                    return std::string(home) + p.substr(1);
            }
            return p;
        };
        try {
            // 1) Explicit environment override
            if (const char* envStorage = std::getenv("YAMS_STORAGE")) {
                if (*envStorage) {
                    g_cachedDataDir = fs::path(envStorage);
                    return;
                }
            }
            if (const char* envData = std::getenv("YAMS_DATA_DIR")) {
                if (*envData) {
                    g_cachedDataDir = fs::path(envData);
                    return;
                }
            }

            // 2) core.data_dir from config.toml
            fs::path cfgPath;
            if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
                cfgPath = fs::path(cfgEnv);
            } else {
                cfgPath = yams::config::get_config_path();
            }

            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                std::ifstream f(cfgPath);
                std::string line;
                bool in_core = false;
                auto ltrim = [](std::string& s) {
                    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                                return !std::isspace(ch);
                            }));
                };
                auto rtrim = [](std::string& s) {
                    s.erase(std::find_if(s.rbegin(), s.rend(),
                                         [](unsigned char ch) { return !std::isspace(ch); })
                                .base(),
                            s.end());
                };
                while (std::getline(f, line)) {
                    ltrim(line);
                    rtrim(line);
                    if (line.empty() || line[0] == '#')
                        continue;
                    if (line.front() == '[') {
                        in_core = (line == "[core]" || line == "[ core ]");
                        continue;
                    }
                    auto pos = line.find('=');
                    if (pos == std::string::npos)
                        continue;
                    std::string key = line.substr(0, pos);
                    std::string val = line.substr(pos + 1);
                    ltrim(key);
                    rtrim(key);
                    ltrim(val);
                    rtrim(val);
                    if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                                            (val.front() == '\'' && val.back() == '\''))) {
                        val = val.substr(1, val.size() - 2);
                    }
                    if (key == "core.data_dir" || (in_core && key == "data_dir")) {
                        val = expand_tilde(val);
                        if (!val.empty()) {
                            g_cachedDataDir = fs::path(val);
                            return;
                        }
                    }
                }
            }

            // 3) XDG/HOME defaults
            if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME")) {
                g_cachedDataDir = fs::path(xdgDataHome) / "yams";
            } else if (const char* homeEnv = std::getenv("HOME")) {
                g_cachedDataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
            } else {
                g_cachedDataDir = fs::current_path() / "yams_data";
            }
        } catch (const std::exception& e) {
            spdlog::debug("resolveDataDirCached: resolution failed: {}", e.what());
        } catch (...) {
            spdlog::debug("resolveDataDirCached: resolution failed with unknown exception");
        }
    });
    return g_cachedDataDir;
}
} // namespace

// DaemonClient implementation
DaemonClient::DaemonClient(const ClientConfig& config) : pImpl(std::make_shared<Impl>(config)) {
    if (pImpl->config_.socketPath.empty()) {
        pImpl->config_.socketPath = yams::daemon::ConnectionFsm::resolve_socket_path_config_first();
    }
    // Use cached data directory resolution for snappier startup
    if (pImpl->config_.dataDir.empty()) {
        pImpl->config_.dataDir = resolveDataDirCached();
    }
    pImpl->refresh_transport();

    // In test builds, default to no auto-start to avoid forking child daemons
#ifdef YAMS_TESTING
    pImpl->config_.autoStart = false;
#endif
    // Honor env toggle to disable auto-start (useful in tests/CI to avoid forking)
    if (const char* noauto = std::getenv("YAMS_DISABLE_AUTOSTART")) {
        std::string v(noauto);
        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
        if (v == "1" || v == "true" || v == "on" || v == "yes") {
            pImpl->config_.autoStart = false;
        }
    }
    // Optional env override for max inflight (load testing): YAMS_MAX_INFLIGHT
    if (const char* mi = std::getenv("YAMS_MAX_INFLIGHT")) {
        try {
            auto v = std::stoul(mi);
            if (v > 0)
                pImpl->config_.maxInflight = v;
        } catch (const std::exception& e) {
            spdlog::debug("DaemonClient init: failed to parse YAMS_MAX_INFLIGHT: {}", e.what());
        }
    }
    // Timeout overrides: prefer explicit env, else bump conservative defaults to 120s
    auto parse_ms = [](const char* v) -> std::optional<std::chrono::milliseconds> {
        if (!v)
            return std::nullopt;
        try {
            long ms = std::stol(std::string(v));
            if (ms > 0)
                return std::chrono::milliseconds(ms);
        } catch (const std::exception& e) {
            spdlog::debug("DaemonClient init: failed to parse timeout value '{}': {}", v, e.what());
        }
        return std::nullopt;
    };
    // Unified request timeout sets both header/body
    if (auto rt = parse_ms(std::getenv("YAMS_REQUEST_TIMEOUT_MS"))) {
        pImpl->headerTimeout_ = *rt;
        pImpl->bodyTimeout_ = *rt;
    }
    if (auto ht = parse_ms(std::getenv("YAMS_HEADER_TIMEOUT_MS"))) {
        pImpl->headerTimeout_ = *ht;
    }
    if (auto bt = parse_ms(std::getenv("YAMS_BODY_TIMEOUT_MS"))) {
        pImpl->bodyTimeout_ = *bt;
    }
    // Note: Request-type-aware timeouts are now applied per-request in
    // sendRequest/sendRequestStreaming Fast ops (ping/status): 5s, Medium ops (search/list): 30s,
    // Slow ops (add/embed): 120s
    spdlog::debug("DaemonClient init: resolved socket='{}'", pImpl->config_.socketPath.string());
    if (!pImpl->config_.dataDir.empty()) {
        spdlog::debug("DaemonClient init: resolved dataDir='{}'", pImpl->config_.dataDir.string());
    }
}

DaemonClient::~DaemonClient() {
    // Mark as shutting down FIRST - this allows any pending coroutines to exit early
    // when they resume, preventing use-after-free crashes
    if (pImpl) {
        pImpl->markShuttingDown();
    }
    // Only shut down non-shared pools. Shared pools (from the registry) may be
    // in use by other clients and will be cleaned up by shutdown_all() or when
    // the io_context is reset.
    if (pImpl && pImpl->pool_ && !pImpl->pool_->is_shared()) {
        pImpl->pool_->shutdown();
    }
}

DaemonClient::DaemonClient(DaemonClient&&) noexcept = default;
DaemonClient& DaemonClient::operator=(DaemonClient&&) noexcept = default;

void DaemonClient::setHeaderTimeout(std::chrono::milliseconds timeout) {
    if (pImpl) {
        pImpl->headerTimeout_ = timeout;
        pImpl->refresh_transport();
    }
}

void DaemonClient::setBodyTimeout(std::chrono::milliseconds timeout) {
    if (pImpl) {
        pImpl->bodyTimeout_ = timeout;
        pImpl->refresh_transport();
    }
}

// New: lightweight readiness probe that sends a real Ping and waits briefly
static bool pingDaemonSync(const std::filesystem::path& socketPath) {
    // Best-effort synchronous connectivity probe: try to synchronously connect to the UNIX socket.
    try {
        auto path = socketPath.empty() ? DaemonClient::resolveSocketPathConfigFirst() : socketPath;
        if (path.empty())
            return false;
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::socket sock(io);
        boost::system::error_code ec;
        sock.connect(boost::asio::local::stream_protocol::endpoint(path.string()), ec);
        if (!ec) {
            sock.close();
            return true;
        }
        return false;
    } catch (...) {
        return false;
    }
}

boost::asio::awaitable<Result<void>> DaemonClient::connect() {
    // Capture shared_ptr to extend Impl lifetime across co_await suspension points
    auto impl = pImpl;
    // Async variant using adapter's connect helper and timers; avoids blocking sleeps
    // If daemon is not reachable and autoStart is disabled, surface a failure.
    const auto socketPath = impl->config_.socketPath.empty()
                                ? DaemonClient::resolveSocketPathConfigFirst()
                                : impl->config_.socketPath;
    if (socketPath.empty()) {
        co_return Error{ErrorCode::NetworkError, "Socket path not resolved"};
    }

    using boost::asio::awaitable;
    using boost::asio::steady_timer;
    using boost::asio::use_awaitable;
    using boost::asio::this_coro::executor;

    auto try_connect =
        [&](std::chrono::milliseconds timeout) -> boost::asio::awaitable<Result<void>> {
        auto ex = co_await boost::asio::this_coro::executor;
        boost::asio::local::stream_protocol::socket sock(ex);
        boost::asio::local::stream_protocol::endpoint ep(socketPath.string());
        try {
            // Race connect against timeout using async_initiate (no experimental APIs)
            using ConnectResult = std::tuple<boost::system::error_code>;
            using RaceResult = std::variant<ConnectResult, bool>;

            auto connect_result = co_await boost::asio::async_initiate<
                decltype(use_awaitable), void(std::exception_ptr, RaceResult)>(
                [&sock, &ep, ex, timeout](auto handler) mutable {
                    auto completed = std::make_shared<std::atomic<bool>>(false);
                    auto timer = std::make_shared<boost::asio::steady_timer>(ex);
                    timer->expires_after(timeout);

                    using HandlerT = std::decay_t<decltype(handler)>;
                    auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                    auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, ex);

                    timer->async_wait([completed, handlerPtr, completion_exec](
                                          const boost::system::error_code& ec) mutable {
                        if (ec == boost::asio::error::operation_aborted)
                            return;
                        if (!completed->exchange(true, std::memory_order_acq_rel)) {
                            boost::asio::post(
                                completion_exec, [h = std::move(*handlerPtr)]() mutable {
                                    std::move(h)(std::exception_ptr{},
                                                 RaceResult(std::in_place_index<1>, true));
                                });
                        }
                    });

                    sock.async_connect(ep, [timer, completed, handlerPtr, completion_exec](
                                               const boost::system::error_code& ec) mutable {
                        if (!completed->exchange(true, std::memory_order_acq_rel)) {
                            timer->cancel();
                            boost::asio::post(completion_exec, [h = std::move(*handlerPtr),
                                                                ec]() mutable {
                                std::move(h)(std::exception_ptr{},
                                             RaceResult(std::in_place_index<0>, ConnectResult{ec}));
                            });
                        }
                    });
                },
                use_awaitable);

            if (connect_result.index() == 1) {
                co_return Error{ErrorCode::Timeout, "Connection timeout"};
            }

            auto& [ec] = std::get<0>(connect_result);
            if (ec) {
                co_return Error{ErrorCode::NetworkError, ec.message()};
            }

            boost::system::error_code close_ec;
            sock.close(close_ec);
            co_return Result<void>();
        } catch (const std::exception& e) {
            co_return Error{ErrorCode::NetworkError, e.what()};
        }
    };

    {
        // Try with a few retries even when autoStart=false
        // This handles race conditions where daemon reports ready but is still completing async
        // init
        const int quickRetries = impl->config_.autoStart ? 1 : 3;
        const auto retryDelay = std::chrono::milliseconds(100);
        Result<void> lastError;

        for (int i = 0; i < quickRetries; ++i) {
            auto r = co_await try_connect(impl->config_.connectTimeout);
            if (r) {
                if (!impl->config_.enableCircuitBreaker)
                    impl->breaker_.recordSuccess();
                // Clear explicitly disconnected flag on successful connection
                impl->explicitly_disconnected_ = false;
                // Refresh transport to get a fresh pool in case the old one was shut down
                // This handles reconnection after daemon restart
                if (impl->pool_ && impl->pool_->is_shutdown()) {
                    spdlog::debug(
                        "[DaemonClient::connect] pool was shutdown, refreshing transport");
                    impl->refresh_transport();
                }
                co_return Result<void>();
            }
            lastError = r;

            // If not autoStart and socket doesn't exist, don't retry
            // (daemon definitely isn't running)
            if (!impl->config_.autoStart && !std::filesystem::exists(socketPath)) {
                co_return r.error();
            }

            // Brief delay before retry
            if (i + 1 < quickRetries) {
                steady_timer t(co_await boost::asio::this_coro::executor);
                t.expires_after(retryDelay);
                co_await t.async_wait(boost::asio::use_awaitable);
            }
        }

        if (!impl->config_.autoStart) {
            co_return lastError.error();
        }
    }

    // Auto-start path
    if (auto result = startDaemon(impl->config_); !result) {
        spdlog::warn("Failed to auto-start daemon: {}",
                     sanitize_for_terminal(result.error().message));
        spdlog::info("Please manually start the daemon with: yams daemon start");
        co_return result.error();
    }

    // Retry with exponential backoff using steady_timer
    const int maxRetries = 10;
    const auto baseDelay = std::chrono::milliseconds(100);
    for (int i = 0; i < maxRetries; ++i) {
        auto r = co_await try_connect(impl->config_.connectTimeout);
        if (r) {
            spdlog::debug("Daemon started successfully after {} retries", i + 1);
            // Clear explicitly disconnected flag on successful connection
            impl->explicitly_disconnected_ = false;
            co_return Result<void>();
        }
        steady_timer t(co_await boost::asio::this_coro::executor);
        auto delay = baseDelay * (1 << std::min(i, 5));
        t.expires_after(delay);
        co_await t.async_wait(boost::asio::use_awaitable);
    }
    co_return Error{ErrorCode::Timeout, "Daemon failed to start after retries"};
}

void DaemonClient::disconnect() {
    // Only shut down non-shared pools. Shared pools may be in use by other clients.
    if (pImpl->pool_ && !pImpl->pool_->is_shared()) {
        pImpl->pool_->shutdown();
    }
    pImpl->explicitly_disconnected_ = true;
}

bool DaemonClient::isConnected() const {
    // If explicitly disconnected, return false until reconnect
    if (pImpl->explicitly_disconnected_) {
        return false;
    }
    // Treat connectivity as liveness of the daemon (socket + ping), not a persistent socket
    return pingDaemonSync(pImpl->config_.socketPath);
}

boost::asio::awaitable<Result<SearchResponse>> DaemonClient::search(const SearchRequest& req) {
    // Always use streaming search
    co_return co_await streamingSearch(req);
}

boost::asio::awaitable<Result<SearchResponse>>
DaemonClient::streamingSearch(const SearchRequest& req) {
    auto handler = std::make_shared<StreamingSearchHandler>(req.pathsOnly, req.limit);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

boost::asio::awaitable<Result<GetResponse>> DaemonClient::get(const GetRequest& req) {
    struct GetHandler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* res = std::get_if<GetResponse>(&r)) {
                response_ = *res;
            } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error_ = Error{err->code, err->message};
            }
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* res = std::get_if<GetResponse>(&r)) {
                response_.content.append(res->content);
            } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error_ = Error{err->code, err->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error_ = e; }
        void onComplete() override {}

        Result<GetResponse> getResults() {
            if (error_) {
                return *error_;
            }
            return response_;
        }

        GetResponse response_;
        std::optional<Error> error_;
    };

    auto handler = std::make_shared<GetHandler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }
    co_return handler->getResults();
}

boost::asio::awaitable<Result<ListResponse>> DaemonClient::list(const ListRequest& req) {
    spdlog::debug("DaemonClient::list entry: [{}] streaming=true", getRequestName(Request{req}));
    co_return co_await streamingList(req);
}

boost::asio::awaitable<Result<GrepResponse>> DaemonClient::grep(const GrepRequest& req) {
    spdlog::debug("DaemonClient::grep entry: [{}] streaming=true", getRequestName(Request{req}));
    co_return co_await streamingGrep(req);
}

boost::asio::awaitable<Result<FileHistoryResponse>>
DaemonClient::fileHistory(const FileHistoryRequest& req) {
    spdlog::debug("DaemonClient::fileHistory: filepath={}", req.filepath);
    co_return co_await call<FileHistoryRequest>(req);
}

boost::asio::awaitable<Result<StatusResponse>> DaemonClient::status() {
    StatusRequest req;
    req.detailed = false; // avoid heavy daemon-side scans by default

    // Transient-aware retry loop for early startup/socket closure races
    // Uses exponential backoff: 25ms, 50ms, 100ms (max 175ms total vs old 750ms)
    static constexpr int kMaxRetries = 3;
    static constexpr std::array<int, kMaxRetries> kRetryDelaysMs = {25, 50, 100};

    Error lastErr{ErrorCode::NetworkError, "Uninitialized"};
    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        auto response = co_await sendRequest(req);
        if (response) {
            if (auto* res = std::get_if<StatusResponse>(&response.value())) {
                co_return *res;
            }
            if (auto* er = std::get_if<ErrorResponse>(&response.value())) {
                co_return Error{er->code, er->message};
            }
            co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
        }

        lastErr = response.error();
        const std::string& msg = lastErr.message;
        bool transient = (lastErr.code == ErrorCode::NetworkError) &&
                         (msg.find("Connection closed") != std::string::npos ||
                          msg.find("Connection reset") != std::string::npos ||
                          msg.find("ECONNRESET") != std::string::npos ||
                          msg.find("EPIPE") != std::string::npos ||
                          msg.find("Broken pipe") != std::string::npos ||
                          msg.find("read header failed") != std::string::npos ||
                          msg.find("read payload failed") != std::string::npos ||
                          msg.find("Read failed") != std::string::npos);
        if (!transient) {
            co_return lastErr;
        }
        using namespace std::chrono_literals;
        boost::asio::steady_timer t(co_await boost::asio::this_coro::executor);
        t.expires_after(std::chrono::milliseconds(kRetryDelaysMs[attempt]));
        co_await t.async_wait(boost::asio::use_awaitable);
    }
    co_return lastErr;
}

boost::asio::awaitable<Result<Response>> DaemonClient::executeRequest(const Request& req) {
    co_return co_await sendRequest(req);
}

boost::asio::awaitable<Result<void>> DaemonClient::shutdown(bool graceful) {
    ShutdownRequest req;
    req.graceful = graceful;

    auto h = std::make_shared<daemon::client::ErrorOnlyHandler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    co_return Result<void>();
}

boost::asio::awaitable<Result<void>> DaemonClient::ping() {
    PingRequest req;

    auto h = std::make_shared<daemon::client::SuccessHandler<PongResponse>>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    if (h->success)
        co_return Result<void>();
    co_return Error{ErrorCode::InvalidData, "Unexpected response type for ping"};
}

boost::asio::awaitable<Result<Response>> DaemonClient::sendRequest(const Request& req) {
    // Capture shared_ptr to extend Impl lifetime across co_await suspension points
    auto impl = pImpl;
    if (!impl || impl->isShuttingDown()) {
        co_return Error{ErrorCode::InvalidState, "DaemonClient is shutting down"};
    }
    spdlog::debug("DaemonClient::sendRequest: [{}] streaming={} sock='{}'", getRequestName(req),
                  false, impl->config_.socketPath.string());
    // Use request-type-aware timeout without shortening configured limits
    auto opts = impl->transportOptions_;
    auto timeout = getTimeoutForCategory(getTimeoutCategory(req));
    opts.requestTimeout = std::max(opts.requestTimeout, timeout);
    opts.headerTimeout = std::max(opts.headerTimeout, timeout);
    opts.bodyTimeout = std::max(opts.bodyTimeout, timeout);
    // Prune and other long maintenance ops get fresh sockets to avoid stale pooled fds
    if (requires_single_use_connection(req)) {
        opts.poolEnabled = false;
    }
    // Force single-use connection for long maintenance ops (e.g., prune)
    if (requires_single_use_connection(req)) {
        opts.poolEnabled = false;
    }
    AsioTransportAdapter adapter(opts);
    auto r = co_await adapter.send_request(req);
    // After resumption, check if client was destroyed during suspension
    if (impl->isShuttingDown()) {
        co_return Error{ErrorCode::InvalidState, "DaemonClient destroyed during request"};
    }
    if (!r)
        co_return r.error();
    co_return r.value();
}

boost::asio::awaitable<Result<Response>> DaemonClient::sendRequest(Request&& req) {
    // Capture shared_ptr to extend Impl lifetime across co_await suspension points
    auto impl = pImpl;
    if (!impl || impl->isShuttingDown()) {
        co_return Error{ErrorCode::InvalidState, "DaemonClient is shutting down"};
    }
    const auto type = getRequestName(req);
    spdlog::debug("DaemonClient::sendRequest(move): [{}] streaming={} sock='{}'", type, false,
                  impl->config_.socketPath.string());
    // Use request-type-aware timeout without shortening configured limits
    auto opts = impl->transportOptions_;
    auto timeout = getTimeoutForCategory(getTimeoutCategory(req));
    opts.requestTimeout = std::max(opts.requestTimeout, timeout);
    opts.headerTimeout = std::max(opts.headerTimeout, timeout);
    opts.bodyTimeout = std::max(opts.bodyTimeout, timeout);
    if (requires_single_use_connection(req)) {
        opts.poolEnabled = false;
    }
    AsioTransportAdapter adapter(opts);
    auto r = co_await adapter.send_request(std::move(req));
    // After resumption, check if client was destroyed during suspension
    if (impl->isShuttingDown()) {
        co_return Error{ErrorCode::InvalidState, "DaemonClient destroyed during request"};
    }
    if (!r)
        co_return r.error();
    co_return r.value();
}

// StreamingListHandler implementation
void DaemonClient::StreamingListHandler::onHeaderReceived(const Response& headerResponse) {
    // Parse header information from response
    if (auto* listRes = std::get_if<ListResponse>(&headerResponse)) {
        // Store total count from header
        totalCount_ = listRes->totalCount;

        // Pre-allocate results vector if we know the size
        if (totalCount_ > 0 && limit_ > 0) {
            items_.reserve(static_cast<size_t>(
                std::min<uint64_t>(totalCount_, static_cast<uint64_t>(limit_))));
        } else if (totalCount_ > 0) {
            items_.reserve(totalCount_);
        } else {
            items_.reserve(100); // Default reservation
        }

        // Add any items that might be in the header
        for (const auto& item : listRes->items) {
            if (limit_ > 0 && count_ >= limit_) {
                break;
            }

            items_.push_back(item);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << item.path << '\n';
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
    }
}

// StreamingSearchHandler implementation
void DaemonClient::StreamingSearchHandler::onHeaderReceived(const Response& headerResponse) {
    // Parse header information from response
    if (auto* searchRes = std::get_if<SearchResponse>(&headerResponse)) {
        spdlog::debug("StreamingSearchHandler: header received (totalCount={}, elapsed={}ms)",
                      searchRes->totalCount, searchRes->elapsed.count());
        // Store total count and elapsed time from header
        totalCount_ = searchRes->totalCount;
        elapsed_ = searchRes->elapsed;

        // Pre-allocate results vector if we know the size
        if (totalCount_ > 0 && limit_ > 0) {
            results_.reserve(static_cast<size_t>(
                std::min<uint64_t>(totalCount_, static_cast<uint64_t>(limit_))));
        } else if (totalCount_ > 0) {
            results_.reserve(totalCount_);
        } else {
            results_.reserve(100); // Default reservation
        }

        // Add any results that might be in the header
        for (const auto& result : searchRes->results) {
            if (limit_ > 0 && count_ >= limit_) {
                break;
            }

            results_.push_back(result);
            count_++;
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
    }
}

bool DaemonClient::StreamingListHandler::onChunkReceived(const Response& chunkResponse,
                                                         bool isLastChunk) {
    (void)isLastChunk;
    // Process chunk data
    if (auto* listRes = std::get_if<ListResponse>(&chunkResponse)) {
        // Update totals if they changed
        if (listRes->totalCount > 0) {
            totalCount_ = listRes->totalCount;
        }

        // Process items in this chunk
        for (const auto& item : listRes->items) {
            if (limit_ > 0 && count_ >= limit_) {
                return false; // Stop processing if we reached the limit
            }

            items_.push_back(item);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << item.path << '\n';
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
        return false; // Stop processing on error
    }

    return true; // Continue processing next chunks
}

bool DaemonClient::StreamingSearchHandler::onChunkReceived(const Response& chunkResponse,
                                                           bool isLastChunk) {
    (void)isLastChunk;
    // Process chunk data
    if (auto* searchRes = std::get_if<SearchResponse>(&chunkResponse)) {
        // Update totals if they changed
        if (searchRes->totalCount > 0) {
            totalCount_ = searchRes->totalCount;
        }

        // Process results in this chunk
        for (const auto& result : searchRes->results) {
            if (limit_ > 0 && count_ >= limit_) {
                return false; // Stop processing if we reached the limit
            }

            results_.push_back(result);
            count_++;
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
        return false; // Stop processing on error
    }

    return true; // Continue processing next chunks
}

void DaemonClient::StreamingListHandler::onError(const Error& error) {
    error_ = error;

    // Log error for immediate feedback
    spdlog::error("List error: {}", error.message);
}

void DaemonClient::StreamingListHandler::onComplete() {
    // Final processing when all chunks have been received
    if (!error_ && !pathsOnly_) {
        spdlog::debug("List complete: found {} items (of {} total)", count_, totalCount_);
    }
    if (!error_ && pathsOnly_ && count_ == 0) {
        std::cout << "(no results)\n";
    }
}

Result<ListResponse> DaemonClient::StreamingListHandler::getResults() const {
    if (error_) {
        return *error_;
    }

    // Construct complete response
    ListResponse response;
    response.items = items_;
    response.totalCount = totalCount_;

    return response;
}

void DaemonClient::StreamingSearchHandler::onError(const Error& error) {
    error_ = error;

    // Log error for immediate feedback
    spdlog::error("Search error: {}", error.message);
}

void DaemonClient::StreamingSearchHandler::onComplete() {
    // Final processing when all chunks have been received
    // Note: All output rendering is handled by the CLI layer (search_command.cpp)
    // to avoid duplicate output and ensure consistent formatting
    if (!error_) {
        spdlog::debug("Search complete: found {} results (of {} total) in {}ms", count_,
                      totalCount_, elapsed_.count());
    }
}

Result<SearchResponse> DaemonClient::StreamingSearchHandler::getResults() const {
    if (error_) {
        return *error_;
    }

    // Construct complete response
    SearchResponse response;
    response.results = results_;
    response.totalCount = totalCount_;
    response.elapsed = elapsed_;

    return response;
}

// Static helper to set timeout environment variables
void DaemonClient::setTimeoutEnvVars(std::chrono::milliseconds headerTimeout,
                                     std::chrono::milliseconds bodyTimeout) {
    // Set environment variables
#ifdef _WIN32
    _putenv_s("YAMS_HEADER_TIMEOUT", std::to_string(headerTimeout.count()).c_str());
    _putenv_s("YAMS_BODY_TIMEOUT", std::to_string(bodyTimeout.count()).c_str());
#else
    setenv("YAMS_HEADER_TIMEOUT", std::to_string(headerTimeout.count()).c_str(), 1);
    setenv("YAMS_BODY_TIMEOUT", std::to_string(bodyTimeout.count()).c_str(), 1);
#endif
}

// Streaming list helper method
boost::asio::awaitable<Result<ListResponse>> DaemonClient::streamingList(const ListRequest& req) {
    auto handler = std::make_shared<StreamingListHandler>(req.pathsOnly, req.limit);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

// Streaming Grep handler methods
void DaemonClient::StreamingGrepHandler::onHeaderReceived(const Response& headerResponse) {
    if (auto* grepRes = std::get_if<GrepResponse>(&headerResponse)) {
        totalMatches_ = grepRes->totalMatches;
        filesSearched_ = grepRes->filesSearched;

        // Process any matches included in the header
        for (const auto& m : grepRes->matches) {
            // Enforce per-file cap if set
            if (perFileMax_ > 0) {
                auto& cnt = perFileCount_[m.file];
                if (cnt >= perFileMax_) {
                    continue;
                }
                cnt++;
            }
            matches_.push_back(m);
            if (pathsOnly_) {
                std::cout << m.file << '\n';
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        error_ = Error{errRes->code, errRes->message};
    }
}

bool DaemonClient::StreamingGrepHandler::onChunkReceived(const Response& chunkResponse,
                                                         bool /*isLastChunk*/) {
    if (auto* grepRes = std::get_if<GrepResponse>(&chunkResponse)) {
        // Update totals if present
        if (grepRes->totalMatches > 0) {
            totalMatches_ = grepRes->totalMatches;
        }
        if (grepRes->filesSearched > 0) {
            filesSearched_ = grepRes->filesSearched;
        }

        for (const auto& m : grepRes->matches) {
            if (perFileMax_ > 0) {
                auto& cnt = perFileCount_[m.file];
                if (cnt >= perFileMax_) {
                    continue;
                }
                cnt++;
            }
            matches_.push_back(m);
            if (pathsOnly_) {
                std::cout << m.file << '\n';
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        error_ = Error{errRes->code, errRes->message};
        return false;
    }
    return true;
}

void DaemonClient::StreamingGrepHandler::onError(const Error& error) {
    error_ = error;
    spdlog::error("Grep error: {}", error.message);
}

void DaemonClient::StreamingGrepHandler::onComplete() {
    if (!error_ && !pathsOnly_) {
        spdlog::debug("Grep complete: {} matches across {} files", totalMatches_, filesSearched_);
    }
    if (!error_ && pathsOnly_ && matches_.empty()) {
        std::cout << "(no results)\n";
    }
}

Result<GrepResponse> DaemonClient::StreamingGrepHandler::getResults() const {
    if (error_) {
        return *error_;
    }
    GrepResponse r;
    r.matches = matches_;
    r.totalMatches = totalMatches_;
    r.filesSearched = filesSearched_;
    return r;
}

// Streaming grep helper method
boost::asio::awaitable<Result<GrepResponse>> DaemonClient::streamingGrep(const GrepRequest& req) {
    // If pathsOnly or filesOnly/countOnly, we can progressively print in handler
    size_t perFileCap = 0;
    if (req.maxMatches > 0) {
        perFileCap = req.maxMatches;
    }
    auto handler =
        std::make_shared<StreamingGrepHandler>(req.pathsOnly || req.filesOnly, perFileCap);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

boost::asio::awaitable<Result<void>>
DaemonClient::sendRequestStreaming(const Request& req,
                                   std::shared_ptr<ChunkedResponseHandler> handler) {
    // Capture shared_ptr to extend Impl lifetime across co_await suspension points
    auto impl = pImpl;
    if (!impl || impl->isShuttingDown()) {
        co_return Error{ErrorCode::InvalidState, "DaemonClient is shutting down"};
    }
    // Skip the legacy connect() call when using AsioTransportAdapter
    // The adapter creates its own connection, and calling connect() here
    // causes a double connection issue where the POSIX socket immediately EOFs

    auto onHeader = [handler](const Response& r) { handler->onHeaderReceived(r); };
    auto onChunk = [handler](const Response& r, bool last) {
        // Pretty-print progress events if present, then forward to handler
        if (auto* ev = std::get_if<EmbeddingEvent>(&r)) {
            spdlog::info(
                "[embed] model={} processed={}/{} success={} failure={} inserted={} phase={} {}",
                ev->modelName, ev->processed, ev->total, ev->success, ev->failure, ev->inserted,
                ev->phase, ev->message);
            // Do not terminate stream on events
            (void)last;
            return true;
        }
        if (auto* ml = std::get_if<ModelLoadEvent>(&r)) {
            if (ml->bytesTotal > 0) {
                spdlog::info("[model] {} {} {}/{} bytes {}", ml->modelName, ml->phase,
                             ml->bytesLoaded, ml->bytesTotal, ml->message);
            } else {
                spdlog::info("[model] {} {} {}", ml->modelName, ml->phase, ml->message);
            }
            (void)last;
            return true;
        }
        return handler->onChunkReceived(r, last);
    };
    auto onError = [handler](const Error& e) { handler->onError(e); };
    auto onComplete = [handler]() { handler->onComplete(); };

    // Use request-type-aware timeout without shortening configured limits
    auto opts = impl->transportOptions_;
    auto timeout = getTimeoutForCategory(getTimeoutCategory(req));
    opts.requestTimeout = std::max(opts.requestTimeout, timeout);
    opts.headerTimeout = std::max(opts.headerTimeout, timeout);
    opts.bodyTimeout = std::max(opts.bodyTimeout, timeout);
    // Only disable pooling for long-running maintenance ops that might outlive connections
    if (requires_single_use_connection(req)) {
        opts.poolEnabled = false;
    }
    AsioTransportAdapter adapter(opts);
    auto res = co_await adapter.send_request_streaming(req, onHeader, onChunk, onError, onComplete);
    // After resumption, check if client was destroyed during suspension
    if (impl->isShuttingDown()) {
        co_return Error{ErrorCode::InvalidState, "DaemonClient destroyed during request"};
    }
    if (!res)
        co_return res.error();
    co_return Result<void>();
}

// Streaming AddDocument helper: header-first then final chunk contains full response
boost::asio::awaitable<Result<AddDocumentResponse>>
DaemonClient::streamingAddDocument(const AddDocumentRequest& req) {
    struct AddDocHandler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& headerResponse) override {
            // In unary (non-streaming) server paths, the full AddDocumentResponse may arrive here.
            if (auto* add = std::get_if<AddDocumentResponse>(&headerResponse)) {
                value = *add;
                return;
            }
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
                return;
            }
            if (auto* st = std::get_if<StatusResponse>(&headerResponse)) {
                std::string statusStr = st->overallStatus.empty()
                                            ? (st->ready ? "ready" : "initializing")
                                            : st->overallStatus;
                error = Error{ErrorCode::InvalidState,
                              std::string("Daemon not ready yet (status=") + statusStr + ")"};
                return;
            }
        }
        bool onChunkReceived(const Response& chunkResponse, bool isLastChunk) override {
            if (auto* err = std::get_if<ErrorResponse>(&chunkResponse)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* st = std::get_if<StatusResponse>(&chunkResponse)) {
                // When daemon is not ready, dispatcher returns StatusResponse instead of
                // processing the request. Surface a friendly message and stop.
                std::string statusStr = st->overallStatus.empty()
                                            ? (st->ready ? "ready" : "initializing")
                                            : st->overallStatus;
                error = Error{ErrorCode::InvalidState,
                              std::string("Daemon not ready yet (status=") + statusStr + ")"};
                return false;
            }
            if (auto* add = std::get_if<AddDocumentResponse>(&chunkResponse)) {
                if (!isLastChunk) {
                    // We expect the full response as the last chunk; ignore interim
                    return true;
                }
                value = *add;
                return true;
            }
            // Ignore other chunk types
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<AddDocumentResponse> value;
    };

    auto handler = std::make_shared<AddDocHandler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }
    if (handler->error.has_value()) {
        co_return *handler->error;
    }
    if (handler->value.has_value()) {
        co_return *handler->value;
    }
    co_return Error{ErrorCode::InvalidData, "Missing AddDocumentResponse in stream"};
}

boost::asio::awaitable<Result<EmbeddingResponse>>
DaemonClient::generateEmbedding(const GenerateEmbeddingRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* fin = std::get_if<EmbeddingResponse>(&r))
                value = *fin;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* ev = std::get_if<EmbeddingEvent>(&r)) {
                spdlog::info("[embed] model={} processed={} total={} success={} failure={} {}",
                             ev->modelName, ev->processed, ev->total, ev->success, ev->failure,
                             ev->message);
                (void)isLast;
                return true;
            }
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            if (auto* fin = std::get_if<EmbeddingResponse>(&r)) {
                if (isLast)
                    value = *fin;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<EmbeddingResponse> value;
    };
    auto h = std::make_shared<Handler>();
    spdlog::info("DaemonClient::generateEmbedding sending request model='{}'", req.modelName);
    auto r = co_await sendRequestStreaming(req, h);
    spdlog::info("DaemonClient::generateEmbedding sendRequestStreaming done ok={} error?{}",
                 r.has_value(), r ? 0 : 1);
    if (!r)
        co_return r.error();
    if (h->error.has_value())
        co_return *h->error;
    if (h->value.has_value())
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing EmbeddingResponse in stream"};
}

boost::asio::awaitable<Result<BatchEmbeddingResponse>>
DaemonClient::generateBatchEmbeddings(const BatchEmbeddingRequest& req) {
    // Route through streaming path even for batch for progress events
    co_return co_await streamingBatchEmbeddings(req);
}

boost::asio::awaitable<Result<BatchEmbeddingResponse>>
DaemonClient::streamingBatchEmbeddings(const BatchEmbeddingRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        Handler() {}
        void onHeaderReceived(const Response& headerResponse) override {
            // Support unary (non-chunked) server responses: final may arrive as header
            if (auto* fin = std::get_if<BatchEmbeddingResponse>(&headerResponse)) {
                // Treat as final; no partials were sent
                value = *fin;
                return;
            }
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
                return;
            }
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* e = std::get_if<EmbeddingEvent>(&r)) {
                // Progress only; continue
                (void)e;
                return true;
            }
            if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* fin = std::get_if<BatchEmbeddingResponse>(&r)) {
                if (!isLast) {
                    // Partial chunk – accumulate embeddings client-side
                    if (fin->dimensions > 0 && dim == 0)
                        dim = fin->dimensions;
                    if (!value)
                        value = BatchEmbeddingResponse{};
                    if (value->dimensions == 0 && fin->dimensions > 0)
                        value->dimensions = fin->dimensions;
                    value->embeddings.reserve(value->embeddings.size() + fin->embeddings.size());
                    for (auto& v : fin->embeddings)
                        value->embeddings.push_back(std::move(v));
                    value->successCount += static_cast<uint32_t>(fin->embeddings.size());
                    return true;
                }
                // Last chunk – may be summary only; ensure value is set
                if (!value)
                    value = *fin; // fallback if server sent full final
                else {
                    value->modelUsed = fin->modelUsed;
                    if (value->dimensions == 0)
                        value->dimensions = fin->dimensions;
                }
                return true;
            }
            if (auto* init = std::get_if<GetInitResponse>(&r)) {
                // Store handle to download after stream completes
                initResp = *init;
                return true;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<BatchEmbeddingResponse> value;
        size_t dim{0};
        std::optional<GetInitResponse> initResp;
    };

    auto handler = std::make_shared<Handler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result)
        co_return result.error();
    if (handler->error.has_value())
        co_return *handler->error;
    if (handler->value.has_value())
        co_return *handler->value;
    if (handler->initResp.has_value()) {
        // Download to memory, reconstruct BatchEmbeddingResponse
        auto ir = *handler->initResp;
        // Fetch bytes
        std::string data;
        data.reserve(static_cast<size_t>(ir.totalSize));
        uint64_t remaining = ir.totalSize;
        uint64_t offset = 0;
        const uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : 256 * 1024;
        while (remaining > 0) {
            GetChunkRequest creq{ir.transferId, offset,
                                 static_cast<uint32_t>(std::min<uint64_t>(remaining, step))};
            Request chunk_req{creq};
            auto cres = co_await sendRequest(std::move(chunk_req));
            if (!cres)
                co_return cres.error();
            auto* chunk = std::get_if<GetChunkResponse>(&cres.value());
            if (!chunk)
                co_return Error{ErrorCode::InvalidData, "Invalid chunk response"};
            if (!chunk->data.empty())
                data.append(chunk->data);
            const uint64_t wrote = chunk->data.size();
            if (wrote == 0 && chunk->bytesRemaining == remaining)
                break;
            offset += wrote;
            remaining = chunk->bytesRemaining;
        }
        if (ir.transferId != 0) {
            GetEndRequest ereq{ir.transferId};
            Request end_req{ereq};
            (void)co_await sendRequest(std::move(end_req));
        }
        // Parse metadata
        size_t dim = 0, count = 0;
        try {
            auto it = ir.metadata.find("dim");
            if (it != ir.metadata.end())
                dim = static_cast<size_t>(std::stoul(it->second));
            it = ir.metadata.find("count");
            if (it != ir.metadata.end())
                count = static_cast<size_t>(std::stoul(it->second));
        } catch (const std::exception& e) {
            spdlog::debug("streamingBatchEmbeddings: failed to parse metadata: {}", e.what());
            dim = 0;
            count = 0;
        }
        if (dim == 0 || count == 0)
            co_return Error{ErrorCode::InvalidData, "Missing embedding metadata"};
        // Reconstruct embeddings from little-endian f32
        const size_t bytes_needed = dim * count * sizeof(float);
        if (data.size() < bytes_needed)
            co_return Error{ErrorCode::InvalidData, "Downloaded size mismatch"};
        BatchEmbeddingResponse out;
        out.dimensions = dim;
        out.modelUsed = ir.metadata.count("model") ? ir.metadata.at("model") : std::string{};
        out.processingTimeMs = 0;
        out.successCount = count;
        out.failureCount = 0;
        out.embeddings.resize(count);
        const char* ptr = data.data();
        for (size_t i = 0; i < count; ++i) {
            out.embeddings[i].resize(dim);
            std::memcpy(out.embeddings[i].data(), ptr + i * dim * sizeof(float),
                        dim * sizeof(float));
        }
        co_return out;
    }
    co_return Error{ErrorCode::InvalidData, "Missing BatchEmbeddingResponse or handle in stream"};
}

boost::asio::awaitable<Result<EmbedDocumentsResponse>>
DaemonClient::streamingEmbedDocuments(const EmbedDocumentsRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& headerResponse) override {
            // Support unary (non-chunked) server responses: final may arrive as header
            if (auto* fin = std::get_if<EmbedDocumentsResponse>(&headerResponse)) {
                value = *fin;
                return;
            }
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
                return;
            }
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* e = std::get_if<EmbeddingEvent>(&r)) {
                // Emit a simple, user-visible progress line for repair UI and CLI flows
                // Format: [embed] model processed/total success failure inserted phase message
                try {
                    std::cout << "[embed] model=" << e->modelName << " " << e->processed << "/"
                              << e->total << " success=" << e->success << " failure=" << e->failure
                              << " inserted=" << e->inserted << " phase=" << e->phase
                              << (e->message.empty() ? "" : (" " + e->message)) << "\n";
                    std::cout.flush();
                } catch (const std::exception& ex) {
                    spdlog::debug("streamingEmbedDocuments: progress output failed: {}", ex.what());
                }
                return true;
            }
            if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* fin = std::get_if<EmbedDocumentsResponse>(&r)) {
                if (!isLast)
                    return true;
                value = *fin;
                return true;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<EmbedDocumentsResponse> value;
    };

    auto handler = std::make_shared<Handler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result)
        co_return result.error();
    if (handler->error.has_value())
        co_return *handler->error;
    if (handler->value.has_value())
        co_return *handler->value;
    co_return Error{ErrorCode::InvalidData, "Missing EmbedDocumentsResponse in stream"};
}

boost::asio::awaitable<Result<std::vector<EmbeddingEvent>>>
DaemonClient::callEvents(const EmbedDocumentsRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& headerResponse) override {
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
            }
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* e = std::get_if<EmbeddingEvent>(&r)) {
                try {
                    events.push_back(*e);
                } catch (const std::exception& ex) {
                    spdlog::debug("callEvents: failed to push event: {}", ex.what());
                }
                return true;
            }
            if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* fin = std::get_if<EmbedDocumentsResponse>(&r)) {
                (void)fin; // final stats are implied by events; keep API lean
                return true;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::vector<EmbeddingEvent> events;
    };

    auto handler = std::make_shared<Handler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result)
        co_return result.error();
    if (handler->error.has_value())
        co_return *handler->error;
    co_return handler->events;
}

boost::asio::awaitable<Result<ModelLoadResponse>>
DaemonClient::loadModel(const LoadModelRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* fin = std::get_if<ModelLoadResponse>(&r))
                value = *fin;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* ev = std::get_if<ModelLoadEvent>(&r)) {
                if (ev->bytesTotal > 0) {
                    spdlog::info("[model] {} {} {}/{} bytes {}", ev->modelName, ev->phase,
                                 ev->bytesLoaded, ev->bytesTotal, ev->message);
                } else {
                    spdlog::info("[model] {} {} {}", ev->modelName, ev->phase, ev->message);
                }
                (void)isLast;
                return true;
            }
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            if (auto* fin = std::get_if<ModelLoadResponse>(&r)) {
                if (isLast)
                    value = *fin;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<ModelLoadResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error.has_value())
        co_return *h->error;
    if (h->value.has_value())
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing ModelLoadResponse in stream"};
}

boost::asio::awaitable<Result<SuccessResponse>>
DaemonClient::unloadModel(const UnloadModelRequest& req) {
    auto h = std::make_shared<daemon::client::UnaryHandler<SuccessResponse>>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    if (h->value)
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing SuccessResponse in stream"};
}

boost::asio::awaitable<Result<ModelStatusResponse>>
DaemonClient::getModelStatus(const ModelStatusRequest& req) {
    auto h = std::make_shared<daemon::client::UnaryHandler<ModelStatusResponse>>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    if (h->value)
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing ModelStatusResponse in stream"};
}

boost::asio::awaitable<Result<GetStatsResponse>>
DaemonClient::getStats(const GetStatsRequest& req) {
    auto h = std::make_shared<daemon::client::UnaryHandler<GetStatsResponse>>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    if (h->value)
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing GetStatsResponse in stream"};
}

boost::asio::awaitable<Result<UpdateDocumentResponse>>
DaemonClient::updateDocument(const UpdateDocumentRequest& req) {
    auto h = std::make_shared<daemon::client::UnaryHandler<UpdateDocumentResponse>>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    if (h->value)
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing UpdateDocumentResponse in stream"};
}

std::filesystem::path DaemonClient::resolveSocketPath() {
    return yams::daemon::socket_utils::resolve_socket_path();
}

std::filesystem::path DaemonClient::resolveSocketPathConfigFirst() {
    return yams::daemon::socket_utils::resolve_socket_path_config_first();
}

bool DaemonClient::isDaemonRunning(const std::filesystem::path& socketPath) {
    auto path = socketPath.empty() ? resolveSocketPathConfigFirst() : socketPath;
    // Lightweight readiness probe using real Ping via Asio transport
    return pingDaemonSync(path);
}

Result<void> DaemonClient::startDaemon(const ClientConfig& config) {
    spdlog::info("Starting YAMS daemon...");

    // Resolve socket path with unified precedence: explicit config > config.toml > env > defaults
    auto socketPath = config.socketPath.empty() ? DaemonClient::resolveSocketPathConfigFirst()
                                                : config.socketPath;

    // Determine data dir from config or environment (YAMS_STORAGE)
    std::filesystem::path dataDir = config.dataDir;
    if (dataDir.empty()) {
        if (const char* env = std::getenv("YAMS_STORAGE")) {
            dataDir = std::filesystem::path(env);
        }
    }

#ifdef _WIN32
    // Windows implementation using CreateProcess

    // Determine config file path (env override > platform default)
    std::string configPath;
    if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
        configPath = cfgEnv;
    } else {
        auto cfgPath = yams::config::get_config_path();
        if (std::filesystem::exists(cfgPath)) {
            configPath = cfgPath.string();
        }
    }

    // Find yams-daemon.exe
    std::filesystem::path exePath;
    if (const char* daemonBin = std::getenv("YAMS_DAEMON_BIN"); daemonBin && *daemonBin) {
        exePath = daemonBin;
    } else {
        // Auto-detect relative to this process path
        wchar_t selfPath[MAX_PATH];
        DWORD len = GetModuleFileNameW(nullptr, selfPath, MAX_PATH);
        if (len > 0 && len < MAX_PATH) {
            std::filesystem::path selfExe(selfPath);
            auto cliDir = selfExe.parent_path();
            // Common build-tree locations
            std::vector<std::filesystem::path> candidates = {
                cliDir / "yams-daemon.exe",
                cliDir.parent_path() / "yams-daemon.exe",
                cliDir.parent_path() / "daemon" / "yams-daemon.exe",
                cliDir.parent_path().parent_path() / "daemon" / "yams-daemon.exe",
                cliDir.parent_path().parent_path() / "yams-daemon.exe",
                cliDir.parent_path().parent_path() / "src" / "daemon" / "yams-daemon.exe"};
            for (const auto& p : candidates) {
                if (std::filesystem::exists(p)) {
                    exePath = p;
                    break;
                }
            }
        }
        if (exePath.empty()) {
            // Fall back to searching PATH
            exePath = "yams-daemon.exe";
        }
    }

    // Build command line
    std::wstring cmdLine = L"\"" + exePath.wstring() + L"\"";
    cmdLine += L" --socket \"" + socketPath.wstring() + L"\"";

    if (!configPath.empty() && std::filesystem::exists(configPath)) {
        cmdLine += L" --config \"" + std::filesystem::path(configPath).wstring() + L"\"";
    }
    if (const char* ll = std::getenv("YAMS_LOG_LEVEL"); ll && *ll) {
        std::wstring logLevel(ll, ll + strlen(ll));
        cmdLine += L" --log-level " + logLevel;
    }
    if (!dataDir.empty()) {
        cmdLine += L" --data-dir \"" + dataDir.wstring() + L"\"";
    }

    // Set up environment variables for the child process
    if (!dataDir.empty()) {
        SetEnvironmentVariableW(L"YAMS_STORAGE", dataDir.wstring().c_str());
        SetEnvironmentVariableW(L"YAMS_DATA_DIR", dataDir.wstring().c_str());
    }
    if (!socketPath.empty()) {
        SetEnvironmentVariableW(L"YAMS_DAEMON_SOCKET", socketPath.wstring().c_str());
    }

    // Create process with DETACHED_PROCESS flag so it doesn't share console
    STARTUPINFOW si = {};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdInput = INVALID_HANDLE_VALUE;
    si.hStdOutput = INVALID_HANDLE_VALUE;
    si.hStdError = INVALID_HANDLE_VALUE;

    PROCESS_INFORMATION pi = {};

    // cmdLine must be mutable for CreateProcessW
    std::vector<wchar_t> cmdLineBuf(cmdLine.begin(), cmdLine.end());
    cmdLineBuf.push_back(L'\0');

    BOOL success = CreateProcessW(
        nullptr,           // lpApplicationName - use cmdLine instead
        cmdLineBuf.data(), // lpCommandLine
        nullptr,           // lpProcessAttributes
        nullptr,           // lpThreadAttributes
        FALSE,             // bInheritHandles
        DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW, // dwCreationFlags
        nullptr,                                                        // lpEnvironment - inherit
        nullptr, // lpCurrentDirectory - inherit
        &si,     // lpStartupInfo
        &pi      // lpProcessInformation
    );

    if (!success) {
        DWORD err = GetLastError();
        return Error{ErrorCode::InternalError,
                     "Failed to start daemon: CreateProcess error " + std::to_string(err)};
    }

    // Close handles - we don't need to wait for the daemon
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);

    spdlog::info("Daemon process spawned (PID: {}), client will now poll for readiness",
                 pi.dwProcessId);
    return Result<void>();

#else
    // Unix implementation using fork/exec

    // Fork and exec yams-daemon
    pid_t pid = fork();
    if (pid < 0) {
        return Error{ErrorCode::InternalError, "Failed to fork: " + std::string(strerror(errno))};
    }

    if (pid == 0) {
        // Child process - exec yams-daemon
        // Detach stdio from parent (MCP/CLI) to avoid corrupting parent's stdout framing
        int devnull = ::open("/dev/null", O_RDWR);
        if (devnull >= 0) {
            (void)::dup2(devnull, STDIN_FILENO);
            (void)::dup2(devnull, STDOUT_FILENO);
            (void)::dup2(devnull, STDERR_FILENO);
            if (devnull > 2)
                ::close(devnull);
        }
        // If a dataDir is provided, export it and pass as explicit CLI arg
        if (!dataDir.empty()) {
            setenv("YAMS_STORAGE", dataDir.c_str(), 1);
            // Also export legacy alias used by some components
            setenv("YAMS_DATA_DIR", dataDir.c_str(), 1);
        }

        // Determine config file path (env override > XDG/HOME)
        std::string configPath;
        if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
            configPath = cfgEnv;
        } else if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            configPath = (std::filesystem::path(xdgConfigHome) / "yams" / "config.toml").string();
        } else if (const char* homeEnv = std::getenv("HOME")) {
            configPath =
                (std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml").string();
        }

        // Allow overriding daemon path for development via YAMS_DAEMON_BIN
        std::string exePath;
        if (const char* daemonBin = std::getenv("YAMS_DAEMON_BIN"); daemonBin && *daemonBin) {
            exePath = daemonBin;
        } else {
            // Try to auto-detect relative to this process path
            // On Linux, read /proc/self/exe
            std::filesystem::path selfExe;
            char buf[4096];
            ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                selfExe = std::filesystem::path(buf);
            }
            if (!selfExe.empty()) {
                auto cliDir = selfExe.parent_path();
                // Common build-tree locations
                std::vector<std::filesystem::path> candidates = {
                    cliDir / "yams-daemon", cliDir.parent_path() / "yams-daemon",
                    cliDir.parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "yams-daemon"};
                for (const auto& p : candidates) {
                    if (std::filesystem::exists(p)) {
                        exePath = p.string();
                        break;
                    }
                }
            }
            if (exePath.empty()) {
                // Fall back to PATH lookup
                exePath = "yams-daemon";
            }
        }

        // Ensure child daemon inherits our resolved socket path for consistency
        if (!socketPath.empty()) {
            setenv("YAMS_DAEMON_SOCKET", socketPath.c_str(), 1);
        }

        // Use execlp to search PATH (or direct path if overridden) for yams-daemon
        // Pass socket and optional config/log-level arguments
        const char* ll = std::getenv("YAMS_LOG_LEVEL");
        // Build args vector conditionally
        std::vector<const char*> args;
        args.push_back(exePath.c_str());
        args.push_back("--socket");
        args.push_back(socketPath.c_str());

        bool haveCfg = !configPath.empty() && std::filesystem::exists(configPath);
        if (haveCfg) {
            args.push_back("--config");
            args.push_back(configPath.c_str());
        }
        if (ll && *ll) {
            args.push_back("--log-level");
            args.push_back(ll);
        }
        if (!dataDir.empty()) {
            args.push_back("--data-dir");
            args.push_back(dataDir.c_str());
        }
        args.push_back(nullptr);

        execvp(exePath.c_str(), const_cast<char* const*>(args.data()));

        // If we get here, exec failed
        spdlog::error("Failed to exec yams-daemon: {}", strerror(errno));
        spdlog::error("Make sure yams-daemon is installed and in your PATH");
        spdlog::error("You can manually start the daemon with: yams daemon start");
        exit(1);
    }

    // Parent process: we don't wait for the daemon to exit.

    spdlog::info("Daemon process spawned, client will now poll for readiness");
    return Result<void>();
#endif
}

// Circuit Breaker implementation
CircuitBreaker::CircuitBreaker(const Config& config)
    : config_(config), lastStateChange_(std::chrono::steady_clock::now()) {}

bool CircuitBreaker::shouldAllow() {
    if (state_ == State::Closed) {
        return true;
    }

    if (state_ == State::Open) {
        if (shouldTransitionToHalfOpen()) {
            transitionTo(State::HalfOpen);
            return true;
        }
        return false;
    }

    // HalfOpen - allow limited requests
    return true;
}

void CircuitBreaker::recordSuccess() {
    if (state_ == State::HalfOpen) {
        consecutiveSuccesses_++;
        if (consecutiveSuccesses_ >= config_.successThreshold) {
            transitionTo(State::Closed);
        }
    }
    consecutiveFailures_ = 0;
}

void CircuitBreaker::recordFailure() {
    consecutiveFailures_++;
    consecutiveSuccesses_ = 0;

    if (state_ == State::Closed && consecutiveFailures_ >= config_.failureThreshold) {
        transitionTo(State::Open);
    } else if (state_ == State::HalfOpen) {
        transitionTo(State::Open);
    }
}

void CircuitBreaker::transitionTo(State newState) {
    state_ = newState;
    lastStateChange_ = std::chrono::steady_clock::now();

    if (newState == State::Open) {
        openedAt_ = lastStateChange_;
    }

    consecutiveFailures_ = 0;
    consecutiveSuccesses_ = 0;
}

bool CircuitBreaker::shouldTransitionToHalfOpen() const {
    auto now = std::chrono::steady_clock::now();
    auto timeSinceOpen = std::chrono::duration_cast<std::chrono::seconds>(now - openedAt_);
    return timeSinceOpen >= config_.openTimeout;
}

void DaemonClient::setStreamingEnabled(bool enabled) {
    if (pImpl) {
        pImpl->config_.enableChunkedResponses = enabled;
    }
}

boost::asio::awaitable<Result<SuccessResponse>> DaemonClient::remove(const DeleteRequest& req) {
    auto h = std::make_shared<daemon::client::UnaryHandler<SuccessResponse>>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->hasError())
        co_return h->error();
    if (h->value)
        co_return *h->value;
    co_return Error{ErrorCode::InvalidData, "Missing SuccessResponse in stream"};
}

} // namespace yams::daemon
