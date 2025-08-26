#include <yams/daemon/ipc/async_ipc_server.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstring>
#include <future>
#include <optional>

#ifdef _WIN32
#include <afunix.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#ifndef SHUT_RDWR
#define SHUT_RDWR SD_BOTH
#endif
// Provide minimal fcntl-style nonblocking handling only within this TU
#ifndef F_GETFL
#define F_GETFL 0
#endif
#ifndef F_SETFL
#define F_SETFL 1
#endif
#ifndef O_NONBLOCK
#define O_NONBLOCK 0x800
#endif
namespace yams::daemon::detail {
inline int win32_fcntl(int fd, int cmd, int arg = 0) {
    if (cmd == F_SETFL) {
        u_long mode = (arg & O_NONBLOCK) ? 1 : 0;
        return ::ioctlsocket(fd, FIONBIO, &mode) == 0 ? 0 : -1;
    }
    // F_GETFL: return 0 (no flags)
    return 0;
}
} // namespace yams::daemon::detail
#define fcntl(fd, cmd, ...) yams::daemon::detail::win32_fcntl((fd), (cmd), ##__VA_ARGS__)
#define close(fd) ::closesocket(fd)
#else
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace yams::daemon {

// ============================================================================
// AsyncIpcServer Implementation
// ============================================================================

AsyncIpcServer::AsyncIpcServer(Config config)
    : config_(std::move(config)), io_context_(std::make_unique<AsyncIOContext>()),
      thread_pool_(std::make_unique<ThreadPool>(config_.worker_threads)) {
    request_handler_ = std::make_unique<RequestHandler>();
}

AsyncIpcServer::~AsyncIpcServer() {
    if (running_) {
        stop();
    }
}

AsyncIpcServer::AsyncIpcServer(AsyncIpcServer&& other) noexcept
    : config_(std::move(other.config_)), listen_fd_(std::exchange(other.listen_fd_, -1)),
      running_(other.running_.load()), io_context_(std::move(other.io_context_)),
      thread_pool_(std::move(other.thread_pool_)),
      request_handler_(std::move(other.request_handler_)), processor_(std::move(other.processor_)),
      handler_func_(std::move(other.handler_func_)),
      worker_threads_(std::move(other.worker_threads_)),
      accept_thread_(std::move(other.accept_thread_)), io_thread_(std::move(other.io_thread_)) {
    other.running_ = false;
}

AsyncIpcServer& AsyncIpcServer::operator=(AsyncIpcServer&& other) noexcept {
    if (this != &other) {
        if (running_) {
            stop();
        }

        config_ = std::move(other.config_);
        listen_fd_ = std::exchange(other.listen_fd_, -1);
        running_ = other.running_.load();
        io_context_ = std::move(other.io_context_);
        thread_pool_ = std::move(other.thread_pool_);
        request_handler_ = std::move(other.request_handler_);
        processor_ = std::move(other.processor_);
        handler_func_ = std::move(other.handler_func_);
        worker_threads_ = std::move(other.worker_threads_);
        accept_thread_ = std::move(other.accept_thread_);
        io_thread_ = std::move(other.io_thread_);

        other.running_ = false;
    }
    return *this;
}

void AsyncIpcServer::set_handler(RequestHandlerFunc handler) {
    handler_func_ = std::move(handler);

    // Create a simple processor that wraps the handler function
    class FunctionProcessor : public RequestProcessor {
    public:
        FunctionProcessor(RequestHandlerFunc func, ThreadPool* pool)
            : func_(std::move(func)), pool_(pool) {}

        // Use thread pool to handle requests safely with promise/future
        Task<Response> process(const Request& request) override {
            // Use promise/future for simpler thread safety
            auto promise = std::make_shared<std::promise<Response>>();
            auto future = promise->get_future();

            // Submit to thread pool with captured request
            pool_->enqueue_detached([this, req = request, promise]() mutable {
                try {
                    promise->set_value(func_(req));
                } catch (...) {
                    promise->set_exception(std::current_exception());
                }
            });

            // Wait for result and return it
            co_return future.get();
        }

    private:
        RequestHandlerFunc func_;
        ThreadPool* pool_;
    };

    processor_ = std::make_shared<FunctionProcessor>(handler_func_, thread_pool_.get());
    request_handler_->set_processor(processor_);
}

void AsyncIpcServer::set_processor(std::shared_ptr<RequestProcessor> processor) {
    processor_ = std::move(processor);
    request_handler_->set_processor(processor_);
}

Result<void> AsyncIpcServer::start() {
#ifndef _WIN32
    // Ignore SIGPIPE to prevent daemon termination on write to closed sockets
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

    // Create and bind socket
    if (auto result = create_socket(); !result) {
        spdlog::error("AsyncIpcServer: create_socket failed: {}", result.error().message);
        running_ = false;
        return result;
    }

    if (auto result = bind_socket(); !result) {
        spdlog::error("AsyncIpcServer: bind_socket failed: {}", result.error().message);
        cleanup_socket();
        running_ = false;
        return result;
    }

    // Start listening
    if (listen(listen_fd_, SOMAXCONN) < 0) {
        cleanup_socket();
        running_ = false;
        auto msg = std::string("Failed to listen on socket: ") + std::string(strerror(errno));
        spdlog::error("AsyncIpcServer: {} (fd={})", msg, listen_fd_);
        return Error{ErrorCode::NetworkError, msg};
    }

    // Set socket to non-blocking mode for the accept loop
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    if (flags == -1) {
        cleanup_socket();
        running_ = false;
        auto msg = std::string("Failed to get socket flags: ") + std::string(strerror(errno));
        spdlog::error("AsyncIpcServer: {}", msg);
        return Error{ErrorCode::NetworkError, msg};
    }
    if (fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK) == -1) {
        cleanup_socket();
        running_ = false;
        auto msg =
            std::string("Failed to set socket non-blocking: ") + std::string(strerror(errno));
        spdlog::error("AsyncIpcServer: {}", msg);
        return Error{ErrorCode::NetworkError, msg};
    }

    spdlog::info("AsyncIpcServer listening on {}", config_.socket_path.string());

    // Start a single IO thread for the event loop (kqueue doesn't support multiple threads)
    // The worker_threads will be used for processing requests, not for the IO loop
    io_thread_ = std::jthread([this](std::stop_token token) {
        try {
            spdlog::debug("AsyncIpcServer IO thread started");
            // Run until io_context_->stop() is called or stop requested
            while (!token.stop_requested()) {
                // Run with a timeout to check stop token periodically
                io_context_->run();
                if (token.stop_requested())
                    break;
            }
            spdlog::debug("AsyncIpcServer IO thread exiting");
        } catch (const std::exception& e) {
            spdlog::error("AsyncIpcServer IO thread crashed: {}", e.what());
        } catch (...) {
            spdlog::error("AsyncIpcServer IO thread crashed: unknown exception");
        }
    });

    // Start accept thread
    accept_thread_ = std::jthread([this](std::stop_token token) {
        try {
            spdlog::debug("AsyncIpcServer accept thread started");
            // Run accept loop synchronously
            accept_loop_sync(token);
            spdlog::debug("AsyncIpcServer accept thread exiting");
        } catch (const std::exception& e) {
            spdlog::error("AsyncIpcServer accept thread crashed: {}", e.what());
        } catch (...) {
            spdlog::error("AsyncIpcServer accept thread crashed: unknown exception");
        }
    });

    stats_.total_connections = 0;
    stats_.active_connections = 0;

    return Result<void>();
}

void AsyncIpcServer::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    spdlog::info("Stopping AsyncIpcServer...");

    // Request stop on all jthreads (they have built-in stop tokens)
    if (accept_thread_.joinable()) {
        accept_thread_.request_stop();
    }
    if (io_thread_.joinable()) {
        io_thread_.request_stop();
    }

    // Stop accepting new connections
    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
        close(listen_fd_);
        listen_fd_ = -1;
    }

    // Stop IO context (our instance)
    if (io_context_) {
        io_context_->stop();
    }

    // Stop the thread pool explicitly to ensure clean shutdown
    if (thread_pool_) {
        thread_pool_->stop();
    }

    // Wait for threads to finish
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    if (io_thread_.joinable()) {
        io_thread_.join();
    }

    // Ensure all retained client tasks have finished
    {
        std::lock_guard<std::mutex> lock(tasks_mutex_);
        for (auto& t : client_tasks_) {
            try {
                t.get();
            } catch (...) {
                // ignore errors during shutdown
            }
        }
    }

    // worker_threads_ is not used - ThreadPool manages its own threads
    worker_threads_.clear();

    // Cleanup socket
    cleanup_socket();

    spdlog::info("AsyncIpcServer stopped");
}

Result<void> AsyncIpcServer::create_socket() {
    listen_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        auto msg = std::string("Failed to create socket: ") + std::string(strerror(errno));
        return Error{ErrorCode::NetworkError, msg};
    }

    // Set socket options
    if (config_.reuse_address) {
        int opt = 1;
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            spdlog::warn("Failed to set SO_REUSEADDR: {}", strerror(errno));
        }
    }

    // Make socket non-blocking
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    if (flags < 0 || fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(listen_fd_);
        listen_fd_ = -1;
        return Error{ErrorCode::NetworkError,
                     "Failed to set non-blocking mode: " + std::string(strerror(errno))};
    }

    return Result<void>();
}

Result<void> AsyncIpcServer::bind_socket() {
    // Check if socket file exists and if it's in use
    if (std::filesystem::exists(config_.socket_path)) {
        // Try to connect to check if it's active
        int test_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (test_fd < 0) {
            return Error{ErrorCode::NetworkError,
                         "Failed to create test socket: " + std::string(strerror(errno))};
        }

        struct sockaddr_un test_addr;
        std::memset(&test_addr, 0, sizeof(test_addr));
        test_addr.sun_family = AF_UNIX;
        std::strncpy(test_addr.sun_path, config_.socket_path.c_str(),
                     sizeof(test_addr.sun_path) - 1);

        if (::connect(test_fd, reinterpret_cast<struct sockaddr*>(&test_addr), sizeof(test_addr)) ==
            0) {
            // Socket is active - another daemon is running
            close(test_fd);
            return Error{ErrorCode::InvalidState, "Socket already in use by another daemon: " +
                                                      config_.socket_path.string()};
        }
        close(test_fd);

        // Socket file exists but not connectable - stale, remove it
        spdlog::info("Removing stale socket file: {}", config_.socket_path.string());
        if (!std::filesystem::remove(config_.socket_path)) {
            spdlog::warn("Failed to remove stale socket file: {}", config_.socket_path.string());
        }
    }

    // Create socket directory if it doesn't exist
    auto socket_dir = config_.socket_path.parent_path();
    if (!socket_dir.empty() && !std::filesystem::exists(socket_dir)) {
        std::error_code ec;
        std::filesystem::create_directories(socket_dir, ec);
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to create socket directory: " + ec.message()};
        }
    }

    // Bind to socket path
    struct sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;

    if (config_.socket_path.string().length() >= sizeof(addr.sun_path)) {
        return Error{ErrorCode::InvalidArgument,
                     "Socket path too long: " + config_.socket_path.string()};
    }

    std::strncpy(addr.sun_path, config_.socket_path.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to bind socket: " + std::string(strerror(errno))};
    }

    // Set socket permissions (readable/writable by owner only)
    std::filesystem::permissions(config_.socket_path,
                                 std::filesystem::perms::owner_read |
                                     std::filesystem::perms::owner_write,
                                 std::filesystem::perm_options::replace);

    return Result<void>();
}

void AsyncIpcServer::accept_loop_sync(std::stop_token token) {
    spdlog::debug("Accept loop started (synchronous)");

    while (!token.stop_requested() && running_) {
        // Accept new connection
        struct sockaddr_un client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd =
            accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);

        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No pending connections, wait a bit and retry
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            if (errno == EINTR) {
                continue; // Interrupted, retry
            }

            spdlog::error("Accept failed: {}", strerror(errno));
            // Back off briefly to avoid tight error loop
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        // Enforce bounded active connections to apply backpressure
        auto current_active = stats_.active_connections.load();
        auto current_total = stats_.total_connections.load();
        if (current_active >= config_.max_connections) {
            spdlog::warn("Max connections reached (active={} max={}), rejecting client fd={}",
                         current_active, config_.max_connections, client_fd);
            close(client_fd);
            // Back off briefly to avoid tight accept loop
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        spdlog::debug("Accepted client fd={} active(next)={} total(next)={}", client_fd,
                      current_active + 1, current_total + 1);
        stats_.total_connections++;
        stats_.active_connections++;

        // Create async socket for the client
        AsyncSocket client_socket(client_fd, *io_context_);

        // Spawn coroutine to handle the client and retain task to keep coroutine alive
        auto client_task = handle_client(std::move(client_socket), token);
        {
            std::lock_guard<std::mutex> lock(tasks_mutex_);
            client_tasks_.push_back(std::move(client_task));
        }

        // The coroutine will run on the worker threads via io_context
        // It begins immediately (initial_suspend = never); resumes on I/O ready
    }

    spdlog::debug("Accept loop exiting");
}

Task<void> AsyncIpcServer::accept_loop([[maybe_unused]] std::stop_token token) {
    // Coroutine version kept for compatibility but not used
    co_return;
}

Task<void> AsyncIpcServer::handle_client(AsyncSocket socket, std::stop_token token) {
    spdlog::debug("handle_client coroutine started (active={}, total={})",
                  stats_.active_connections.load(), stats_.total_connections.load());

    // Use the request handler to process the connection
    co_await request_handler_->handle_connection(std::move(socket), token);

    auto remaining = --stats_.active_connections;
    spdlog::debug("Client connection closed (active now={}, total={})", remaining,
                  stats_.total_connections.load());
}

void AsyncIpcServer::cleanup_socket() {
    if (listen_fd_ >= 0) {
        close(listen_fd_);
        listen_fd_ = -1;
    }

    // Remove socket file
    if (std::filesystem::exists(config_.socket_path)) {
        std::filesystem::remove(config_.socket_path);
    }
}

// ============================================================================
// IpcServerFactory Implementation
// ============================================================================

std::unique_ptr<AsyncIpcServer>
IpcServerFactory::create_async_server(const std::filesystem::path& socket_path,
                                      size_t worker_threads) {
    AsyncIpcServer::Config config;
    config.socket_path = socket_path;
    config.worker_threads = worker_threads;

    return std::make_unique<AsyncIpcServer>(std::move(config));
}

std::unique_ptr<AsyncIpcServer>
IpcServerFactory::create_async_server(AsyncIpcServer::Config config) {
    return std::make_unique<AsyncIpcServer>(std::move(config));
}

} // namespace yams::daemon
