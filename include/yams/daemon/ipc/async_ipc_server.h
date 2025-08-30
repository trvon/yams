#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/streaming_processor.h>
#include <yams/daemon/ipc/thread_pool.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <concepts>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>
#include <yams/profiling.h>

#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace yams::daemon {

// ============================================================================
// Concepts for Template Constraints
// ============================================================================

template <typename T>
concept IsRequestProcessor =
    // Type exposes .process(req)
    requires(T processor, const Request& req) {
        { processor.process(req) } -> std::same_as<Task<Response>>;
    } ||
    // Or is directly invocable with (req)
    requires(T callable, const Request& req) {
        { callable(req) } -> std::same_as<Task<Response>>;
    };

template <typename T>
concept IsStreamProcessor = IsRequestProcessor<T> && requires(T processor, const Request& req) {
    { processor.supports_streaming() } -> std::same_as<bool>;
};

// ============================================================================
// Modern Async IPC Server
//  - Implementations live in .cpp with explicit instantiation for default type
// ============================================================================

template <typename ProcessorT = std::function<Task<Response>(const Request&)>>
requires IsRequestProcessor<ProcessorT>
class AsyncIpcServer {
public:
    using RequestHandlerFunc = std::function<Response(const Request&)>;

    struct Config {
        std::filesystem::path socket_path;
        size_t worker_threads{4};
        size_t max_connections{100};
        size_t max_message_size{10 * 1024 * 1024};
        std::chrono::seconds connection_timeout{30};
        bool reuse_address{true};

        // Streaming response settings
        bool enable_streaming{true};
        size_t chunk_size{64 * 1024};
        bool flush_header_immediately{true};
        bool flush_chunks_immediately{true};
        std::chrono::milliseconds write_timeout{30000};
        std::chrono::milliseconds read_timeout{30000};
    };

    // Non-movable design to prevent lifetime issues
    AsyncIpcServer(const AsyncIpcServer&) = delete;
    AsyncIpcServer& operator=(const AsyncIpcServer&) = delete;
    AsyncIpcServer(AsyncIpcServer&&) = delete;
    AsyncIpcServer& operator=(AsyncIpcServer&&) = delete;

    explicit AsyncIpcServer(Config config = {}, ProcessorT processor = {})
        : config_(std::move(config)), processor_(std::move(processor)),
          io_context_(std::make_shared<AsyncIOContext>()),
          thread_pool_(std::make_shared<ThreadPool>(config_.worker_threads)) {
        setup_request_handler();
    }
    ~AsyncIpcServer() {
        if (running_.load())
            stop();
    }

    // Set the request processor (template-based)
    void set_processor(ProcessorT processor) {
        if (running_.load()) {
            throw std::runtime_error("Cannot change processor while server is running");
        }
        processor_ = std::move(processor);
        setup_request_handler();
    }

    // Convenience: allow a simple synchronous handler to be set
    void set_handler(std::function<Response(const Request&)> handler) {
        ProcessorT proc = [h = std::move(handler)](const Request& r) -> Task<Response> {
            co_return h(r);
        };
        set_processor(std::move(proc));
    }

    // Server lifecycle
    [[nodiscard]] Result<void> start() {
#ifndef _WIN32
        {
            struct sigaction sa{};
            sa.sa_handler = SIG_IGN;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = 0;
            sigaction(SIGPIPE, &sa, nullptr);
        }
#endif
        if (running_.exchange(true)) {
            return Error{ErrorCode::InvalidState, "Server already running"};
        }
        if (auto r = create_socket(); !r) {
            running_ = false;
            return r;
        }
        if (auto r = bind_socket(); !r) {
            cleanup_socket();
            running_ = false;
            return r;
        }
        if (::listen(listen_fd_, SOMAXCONN) < 0) {
            cleanup_socket();
            running_ = false;
            return Error{ErrorCode::NetworkError,
                         std::string("Failed to listen: ") + std::string(strerror(errno))};
        }
        set_socket_nonblocking();
        spdlog::info("AsyncIpcServer listening on {}", config_.socket_path.string());
        start_threads();
        stats_.total_connections = 0;
        stats_.active_connections = 0;
        return Result<void>();
    }
    void stop() {
        if (!running_.exchange(false))
            return;
        spdlog::info("Stopping AsyncIpcServer...");
        if (accept_thread_.joinable())
            accept_thread_.request_stop();
        if (io_thread_.joinable())
            io_thread_.request_stop();
        if (listen_fd_ >= 0) {
#ifdef _WIN32
            shutdown(listen_fd_, SHUT_RDWR);
#else
            ::shutdown(listen_fd_, SHUT_RDWR);
#endif
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (io_context_)
            io_context_->stop();
        if (thread_pool_)
            thread_pool_->stop();
        if (accept_thread_.joinable())
            accept_thread_.join();
        if (io_thread_.joinable())
            io_thread_.join();
        {
            std::lock_guard<std::mutex> lk(tasks_mutex_);
            for (auto& t : client_tasks_) {
                try {
                    t.get();
                } catch (...) {
                }
            }
            client_tasks_.clear();
        }
        cleanup_socket();
        spdlog::info("AsyncIpcServer stopped");
    }

    bool is_running() const { return running_.load(); }

    // Statistics
    struct Stats {
        size_t total_connections{0};
        size_t active_connections{0};
        size_t total_requests{0};
        size_t failed_requests{0};
        size_t bytes_received{0};
        size_t bytes_sent{0};
    };

    [[nodiscard]] Stats get_stats() const {
        Stats copy;
        copy.total_connections = stats_.total_connections.load();
        copy.active_connections = stats_.active_connections.load();
        copy.total_requests = stats_.total_requests.load();
        copy.failed_requests = stats_.failed_requests.load();
        copy.bytes_received = stats_.bytes_received.load();
        copy.bytes_sent = stats_.bytes_sent.load();
        return copy;
    }

    void reset_stats() {
        stats_.total_connections = 0;
        stats_.active_connections = 0;
        stats_.total_requests = 0;
        stats_.failed_requests = 0;
        stats_.bytes_received = 0;
        stats_.bytes_sent = 0;
    }

private:
    void setup_request_handler() {
        request_handler_ = std::make_unique<RequestHandler>();
        RequestHandler::Config rh_cfg;
        rh_cfg.enable_streaming = config_.enable_streaming;
        rh_cfg.chunk_size = config_.chunk_size;
        rh_cfg.write_timeout =
            std::chrono::duration_cast<std::chrono::seconds>(config_.write_timeout);
        rh_cfg.read_timeout =
            std::chrono::duration_cast<std::chrono::seconds>(config_.read_timeout);
        rh_cfg.close_after_response = false;
        rh_cfg.max_frame_size = config_.max_message_size;
        request_handler_->set_config(rh_cfg);

        class TemplateProcessorWrapper : public RequestProcessor {
        public:
            TemplateProcessorWrapper(ProcessorT& processor, ThreadPool* pool, AsyncIOContext& io)
                : processor_(processor), pool_(pool), io_(io) {}

            Task<Response> process(const Request& request) override {
                // Build a compute function and shared state
                auto compute = [this, request]() mutable -> Response {
                    if constexpr (requires(ProcessorT p, const Request& r) { p.process(r); }) {
                        auto t = processor_.process(request);
                        return t.get();
                    } else {
                        auto t = processor_(request);
                        return t.get();
                    }
                };

                struct PoolAwaiter {
                    ThreadPool* pool;
                    AsyncIOContext& io;
                    std::function<Response()> fn;
                    std::shared_ptr<Response> result = std::make_shared<Response>();
                    std::shared_ptr<std::exception_ptr> error =
                        std::make_shared<std::exception_ptr>();

                    // Liveness and single-resume state: avoid resuming a destroyed coroutine
                    struct State {
                        std::atomic<bool> scheduled{false};
                        std::weak_ptr<void> life;
                    };
                    std::shared_ptr<void> life_token = std::make_shared<int>(0);
                    std::shared_ptr<State> state = std::make_shared<State>();

                    PoolAwaiter(ThreadPool* p, AsyncIOContext& i, std::function<Response()> f)
                        : pool(p), io(i), fn(std::move(f)) {
                        state->life = life_token;
                    }

                    bool await_ready() const noexcept { return false; }
                    void await_suspend(std::coroutine_handle<> h) {
                        auto fn_local = fn; // copy callable
                        auto res = result;
                        auto err = error;
                        auto st = state;
                        std::weak_ptr<void> wk = st->life; // observe awaiter lifetime
                        AsyncIOContext* io_ptr = &io;
                        pool->enqueue_detached([fn_local = std::move(fn_local), res, err, io_ptr,
                                                st, wk, h]() mutable {
                            try {
                                *res = std::move(fn_local());
                            } catch (...) {
                                *err = std::current_exception();
                            }
                            io_ptr->post([res, err, st, wk, h]() mutable {
                                // Skip if the awaiting coroutine frame was destroyed
                                if (wk.expired())
                                    return;
                                bool already =
                                    st->scheduled.exchange(true, std::memory_order_acq_rel);
                                if (!already) {
                                    if (h)
                                        h.resume();
                                }
                            });
                        });
                    }
                    Response await_resume() {
                        // Mark awaiter as no longer alive for any late posts
                        life_token.reset();
                        if (error && *error) {
                            std::rethrow_exception(*error);
                        }
                        return std::move(*result);
                    }
                } awaiter{pool_, io_, std::move(compute)};

                Response resp = co_await awaiter;
                co_return resp;
            }

        private:
            ProcessorT& processor_;
            ThreadPool* pool_;
            AsyncIOContext& io_;
        };

        auto base = std::make_shared<TemplateProcessorWrapper>(processor_, thread_pool_.get(),
                                                               *io_context_);
        if (config_.enable_streaming) {
            request_handler_->set_processor(
                std::make_shared<StreamingRequestProcessor>(base, rh_cfg));
        } else {
            request_handler_->set_processor(base);
        }
    }

    [[nodiscard]] Result<void> create_socket();
    [[nodiscard]] Result<void> bind_socket();
    void set_socket_nonblocking();
    void start_threads();
    void stop_threads();
    void run_io_loop(std::stop_token token) {
        try {
            spdlog::debug("IO thread started");
            while (!token.stop_requested()) {
                {
                    YAMS_ZONE_SCOPED_N("IOContext::run");
                    io_context_->run();
                }
                if (token.stop_requested())
                    break;
            }
            spdlog::debug("IO thread exiting");
        } catch (const std::exception& e) {
            spdlog::error("IO thread crashed: {}", e.what());
        }
    }
    void run_accept_loop(std::stop_token token) {
        spdlog::debug("Accept loop started");
        YAMS_ZONE_SCOPED_N("AcceptLoop");
        while (!token.stop_requested() && running_.load()) {
            struct sockaddr_un client_addr{};
            socklen_t client_len = sizeof(client_addr);
            YAMS_ZONE_SCOPED_N("accept");
            int client_fd =
                ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
            if (client_fd < 0) {
                handle_accept_error();
                continue;
            }
            auto current_active = stats_.active_connections.load();
            if (current_active >= config_.max_connections) {
                spdlog::warn("Max connections reached, rejecting client fd={}", client_fd);
                ::close(client_fd);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            handle_new_connection(client_fd, token);
        }
        spdlog::debug("Accept loop exiting");
    }
    void handle_accept_error() {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else if (errno == EINTR) {
            // ignore
        } else {
            spdlog::error("Accept failed: {}", strerror(errno));
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    void handle_new_connection(int client_fd, std::stop_token token) {
        auto current_active = stats_.active_connections.load();
        auto current_total = stats_.total_connections.load();
        spdlog::debug("Accepted client fd={} active(next)={} total(next)={}", client_fd,
                      current_active + 1, current_total + 1);
        stats_.total_connections++;
        stats_.active_connections++;
        YAMS_PLOT("daemon_total_connections",
                  static_cast<int64_t>(stats_.total_connections.load()));
        YAMS_PLOT("daemon_active_connections",
                  static_cast<int64_t>(stats_.active_connections.load()));
        AsyncSocket<AsyncIOContext> client_socket(client_fd, *io_context_);
        auto client_task = handle_client(std::move(client_socket), token);
        {
            std::lock_guard<std::mutex> lk(tasks_mutex_);
            client_tasks_.push_back(std::move(client_task));
        }
    }
    Task<void> handle_client(AsyncSocket<AsyncIOContext> socket, std::stop_token token) {
        spdlog::debug("handle_client coroutine started (active={}, total={})",
                      stats_.active_connections.load(), stats_.total_connections.load());
        YAMS_ZONE_SCOPED_N("HandleClient");
        try {
            // Drive a per-connection FSM as the authoritative state source
            co_await request_handler_->handle_connection(std::move(socket), token);
        } catch (const std::exception& e) {
            spdlog::error("Client handler exception: {}", e.what());
        }
        auto remaining = --stats_.active_connections;
        spdlog::debug("Client connection closed (active now={}, total={})", remaining,
                      stats_.total_connections.load());
        YAMS_PLOT("daemon_total_connections",
                  static_cast<int64_t>(stats_.total_connections.load()));
        YAMS_PLOT("daemon_active_connections",
                  static_cast<int64_t>(stats_.active_connections.load()));
    }
    void cleanup_socket() {
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (std::filesystem::exists(config_.socket_path)) {
            std::filesystem::remove(config_.socket_path);
        }
    }

    // Member variables
    Config config_;
    ProcessorT processor_;
    int listen_fd_{-1};
    std::atomic<bool> running_{false};

    // Shared resources for thread-safe access
    std::shared_ptr<AsyncIOContext> io_context_;
    std::shared_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<RequestHandler> request_handler_;

    // Threads
    std::jthread accept_thread_;
    std::jthread io_thread_;

    // Keep client tasks alive until shutdown
    std::mutex tasks_mutex_;
    std::vector<Task<void>> client_tasks_;

    // Internal stats
    struct InternalStats {
        std::atomic<size_t> total_connections{0};
        std::atomic<size_t> active_connections{0};
        std::atomic<size_t> total_requests{0};
        std::atomic<size_t> failed_requests{0};
        std::atomic<size_t> bytes_received{0};
        std::atomic<size_t> bytes_sent{0};
    };
    mutable InternalStats stats_;
};

// ============================================================================
// Factory for Creating Servers
// ============================================================================

class IpcServerFactory {
public:
    template <typename P = std::function<Task<Response>(const Request&)>>
    requires IsRequestProcessor<P>
    static std::unique_ptr<AsyncIpcServer<P>>
    create_async_server(const std::filesystem::path& socket_path, size_t worker_threads = 4,
                        P processor = {}) {
        typename AsyncIpcServer<P>::Config cfg;
        cfg.socket_path = socket_path;
        cfg.worker_threads = worker_threads;
        return std::make_unique<AsyncIpcServer<P>>(std::move(cfg), std::move(processor));
    }

    template <typename P = std::function<Task<Response>(const Request&)>>
    requires IsRequestProcessor<P>
    static std::unique_ptr<AsyncIpcServer<P>>
    create_async_server(typename AsyncIpcServer<P>::Config config, P processor = {}) {
        return std::make_unique<AsyncIpcServer<P>>(std::move(config), std::move(processor));
    }
};

// ============================================================================
// Convenience Type Aliases
// ============================================================================

using SimpleAsyncIpcServer = AsyncIpcServer<std::function<Task<Response>(const Request&)>>;

} // namespace yams::daemon

// ============================================================================
// Implementation: Template method definitions (must be in header for templates)
// ============================================================================

namespace yams::daemon {

template <typename ProcessorT>
requires IsRequestProcessor<ProcessorT>
Result<void> AsyncIpcServer<ProcessorT>::create_socket() {
    listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return Error{ErrorCode::NetworkError,
                     std::string("Failed to create socket: ") + std::string(strerror(errno))};
    }

    if (config_.reuse_address) {
        int opt = 1;
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            spdlog::warn("Failed to set SO_REUSEADDR: {}", strerror(errno));
        }
    }
    return Result<void>();
}

template <typename ProcessorT>
requires IsRequestProcessor<ProcessorT>
Result<void> AsyncIpcServer<ProcessorT>::bind_socket() {
    // If socket path exists, check if active and remove if stale
    if (std::filesystem::exists(config_.socket_path)) {
        int test_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (test_fd >= 0) {
            struct sockaddr_un test_addr{};
            test_addr.sun_family = AF_UNIX;
            std::strncpy(test_addr.sun_path, config_.socket_path.c_str(),
                         sizeof(test_addr.sun_path) - 1);
            if (::connect(test_fd, reinterpret_cast<struct sockaddr*>(&test_addr),
                          sizeof(test_addr)) == 0) {
                ::close(test_fd);
                return Error{ErrorCode::InvalidState,
                             std::string("Socket already in use by another daemon: ") +
                                 config_.socket_path.string()};
            }
            ::close(test_fd);
        }
        spdlog::info("Removing stale socket file: {}", config_.socket_path.string());
        (void)std::filesystem::remove(config_.socket_path);
    }

    auto socket_dir = config_.socket_path.parent_path();
    if (!socket_dir.empty() && !std::filesystem::exists(socket_dir)) {
        std::error_code ec;
        std::filesystem::create_directories(socket_dir, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         std::string("Failed to create socket directory: ") + ec.message()};
        }
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (config_.socket_path.string().length() >= sizeof(addr.sun_path)) {
        return Error{ErrorCode::InvalidArgument,
                     std::string("Socket path too long: ") + config_.socket_path.string()};
    }
    std::strncpy(addr.sun_path, config_.socket_path.c_str(), sizeof(addr.sun_path) - 1);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        return Error{ErrorCode::NetworkError,
                     std::string("Failed to bind socket: ") + std::string(strerror(errno))};
    }

    std::filesystem::permissions(config_.socket_path,
                                 std::filesystem::perms::owner_read |
                                     std::filesystem::perms::owner_write,
                                 std::filesystem::perm_options::replace);

    return Result<void>();
}

template <typename ProcessorT>
requires IsRequestProcessor<ProcessorT>
void AsyncIpcServer<ProcessorT>::set_socket_nonblocking() {
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    if (flags >= 0) {
        (void)fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);
    }
}

template <typename ProcessorT>
requires IsRequestProcessor<ProcessorT>
void AsyncIpcServer<ProcessorT>::start_threads() {
    // Capture context by shared_ptr to avoid dereferencing 'this' from the IO thread
    auto ctx = io_context_;
    io_thread_ = std::jthread([ctx](std::stop_token token) {
        YAMS_SET_THREAD_NAME("yams-io");
        try {
            while (!token.stop_requested()) {
                {
                    YAMS_ZONE_SCOPED_N("IOContext::run");
                    if (ctx)
                        ctx->run();
                }
                if (token.stop_requested())
                    break;
            }
        } catch (const std::exception& e) {
            spdlog::error("IO thread crashed: {}", e.what());
        }
    });
    accept_thread_ = std::jthread([this](std::stop_token token) {
        YAMS_SET_THREAD_NAME("yams-accept");
        run_accept_loop(token);
    });
}

template <typename ProcessorT>
requires IsRequestProcessor<ProcessorT>
void AsyncIpcServer<ProcessorT>::stop_threads() {
    if (accept_thread_.joinable())
        accept_thread_.request_stop();
    if (io_thread_.joinable())
        io_thread_.request_stop();
    if (accept_thread_.joinable())
        accept_thread_.join();
    if (io_thread_.joinable())
        io_thread_.join();
}

} // namespace yams::daemon