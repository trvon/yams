#include <yams/daemon/ipc/async_ipc_server.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <optional>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

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
      running_(other.running_.load()), stop_source_(std::move(other.stop_source_)),
      io_context_(std::move(other.io_context_)), thread_pool_(std::move(other.thread_pool_)),
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
        stop_source_ = std::move(other.stop_source_);
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

    spdlog::info("AsyncIpcServer listening on {}", config_.socket_path.string());

    // Start a single IO thread for the event loop (kqueue doesn't support multiple threads)
    // The worker_threads will be used for processing requests, not for the IO loop
    io_thread_ = std::thread([this]() {
        spdlog::debug("AsyncIpcServer IO thread started");
        // Run until io_context_->stop() is called
        io_context_->run();
        spdlog::debug("AsyncIpcServer IO thread exiting");
    });

    // Start accept thread
    accept_thread_ = std::thread([this]() {
        spdlog::debug("AsyncIpcServer accept thread started");
        auto token = stop_source_.get_token();
        // Drive the accept loop synchronously within this thread
        auto task = accept_loop(token);
        task.get();
        spdlog::debug("AsyncIpcServer accept thread exiting");
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

    // Signal all threads to stop
    stop_source_.request_stop();

    // Stop accepting new connections
    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
    }

    // Stop IO context (our instance)
    if (io_context_) {
        io_context_->stop();
    }

    // Wait for threads to finish
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    if (io_thread_.joinable()) {
        io_thread_.join();
    }

    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    worker_threads_.clear();

    // Cleanup socket
    cleanup_socket();

    spdlog::info("AsyncIpcServer stopped");
}

Result<void> AsyncIpcServer::create_socket() {
    listen_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to create socket: " + std::string(strerror(errno))};
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

Task<void> AsyncIpcServer::accept_loop(std::stop_token token) {
    spdlog::debug("Accept loop started");

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
    co_return;
}

Task<void> AsyncIpcServer::handle_client(AsyncSocket socket, std::stop_token token) {
    spdlog::debug("Handling new client connection (active={}, total={})",
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
