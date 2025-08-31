#include <yams/daemon/unix_ipc_server.h>

#include <spdlog/spdlog.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/ipc/streaming_processor.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

namespace yams::daemon {

// =============================
// DispatcherProcessor
// =============================
Task<Response> UnixIpcServer::DispatcherProcessor::process(const Request& request) {
    co_return dispatcher
        ? dispatcher->dispatch(request)
        : Response{ErrorResponse{ErrorCode::InternalError, "Dispatcher not available"}};
}

// =============================
// UnixIpcServer
// =============================
UnixIpcServer::UnixIpcServer(const Config& cfg, RequestDispatcher* dispatcher, StateComponent* st)
    : cfg_(cfg), dispatcher_(dispatcher), state_(st) {}

UnixIpcServer::~UnixIpcServer() {
    (void)stop();
}

Result<void> UnixIpcServer::start() {
    if (running_.exchange(true)) {
        return Error{ErrorCode::InvalidState, "IPC server already running"};
    }

    // Ensure socket directory exists and remove stale path
    namespace fs = std::filesystem;
    try {
        if (!cfg_.socketPath.empty()) {
            fs::create_directories(cfg_.socketPath.parent_path());
            std::error_code ec;
            if (fs::exists(cfg_.socketPath, ec)) {
                fs::remove(cfg_.socketPath, ec);
            }
        }
    } catch (const std::exception& e) {
        running_ = false;
        return Error{ErrorCode::InvalidArgument,
                     std::string{"Failed to prepare socket directory: "} + e.what()};
    }

    if (auto r = bind_and_listen(); !r) {
        running_ = false;
        return r;
    }

    // Start IO context thread
    io_ = std::make_unique<AsyncIOContext>();
    io_thread_ = std::jthread([this](std::stop_token tok) {
        spdlog::debug("AsyncIOContext thread started");
        while (!tok.stop_requested()) {
            io_->run();
        }
        spdlog::debug("AsyncIOContext thread exiting");
    });

    // Spawn accept loop thread
    accept_thread_ = std::jthread([this](std::stop_token tok) { accept_loop(tok); });

    if (state_) {
        state_->readiness.ipcServerReady = true;
    }
    spdlog::info("IPC server listening on {}", cfg_.socketPath.string());
    return Result<void>();
}

Result<void> UnixIpcServer::stop() {
    if (!running_.exchange(false)) {
        return Error{ErrorCode::InvalidState, "IPC server not running"};
    }

    if (state_) {
        state_->readiness.ipcServerReady = false;
    }

    // Stop accept loop
    if (accept_thread_.joinable()) {
        accept_thread_.request_stop();
        // Waking accept: close the listen fd to unblock
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        accept_thread_.join();
    }

    // Stop IO context
    if (io_thread_.joinable()) {
        io_thread_.request_stop();
        if (io_)
            io_->stop();
        io_thread_.join();
    }
    io_.reset();

    // Unlink socket path
    if (!cfg_.socketPath.empty()) {
        std::error_code ec;
        std::filesystem::remove(cfg_.socketPath, ec);
        if (!ec) {
            spdlog::debug("Removed IPC socket {}", cfg_.socketPath.string());
        }
    }

    return Result<void>();
}

Result<void> UnixIpcServer::bind_and_listen() {
    listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return Error{ErrorCode::NetworkError,
                     std::string{"socket(AF_UNIX) failed: "} + std::strerror(errno)};
    }

    // Make non-blocking
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    if (flags >= 0)
        fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);

    // Bind
    sockaddr_un addr{};
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::string spath = cfg_.socketPath.string();
    if (spath.size() >= sizeof(addr.sun_path)) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return Error{ErrorCode::InvalidArgument, "Socket path too long"};
    }
    std::strncpy(addr.sun_path, spath.c_str(), sizeof(addr.sun_path) - 1);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        int e = errno;
        ::close(listen_fd_);
        listen_fd_ = -1;
        return Error{ErrorCode::NetworkError, std::string{"bind failed: "} + std::strerror(e)};
    }

    // Set permissions (best effort; also governed by umask)
    ::chmod(spath.c_str(), static_cast<mode_t>(cfg_.permission_octal));

    if (::listen(listen_fd_, cfg_.backlog) < 0) {
        int e = errno;
        ::close(listen_fd_);
        listen_fd_ = -1;
        return Error{ErrorCode::NetworkError, std::string{"listen failed: "} + std::strerror(e)};
    }

    return Result<void>();
}

void UnixIpcServer::accept_loop(std::stop_token token) {
    spdlog::debug("IPC accept loop started");
    while (!token.stop_requested()) {
        sockaddr_un client_addr{};
        socklen_t len = sizeof(client_addr);
        int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(5ms);
                continue;
            }
            if (errno == EINTR)
                continue;
            // Likely listen_fd_ was closed during shutdown
            spdlog::debug("accept failed: {}", std::strerror(errno));
            break;
        }

        // Set close-on-exec on client fd
        int fl = fcntl(cfd, F_GETFD);
        if (fl >= 0) {
            (void)fcntl(cfd, F_SETFD, fl | FD_CLOEXEC);
        }

        // Increment active connection count
        active_.fetch_add(1);

        // Detach a handler thread per connection (actual IO runs on io_ thread)
        std::thread([this, cfd]() {
            std::stop_source s;
            handle_client(cfd, s.get_token());
            active_.fetch_sub(1);
        }).detach();
    }
    spdlog::debug("IPC accept loop exiting");
}

void UnixIpcServer::handle_client(int client_fd, std::stop_token token) {
    try {
        if (!io_) {
            ::close(client_fd);
            return;
        }
        // Wrap the raw fd in our AsyncSocket bound to shared IO context
        DefaultAsyncSocket sock(client_fd, *io_);

        // Build a processor that routes into RequestDispatcher, optionally wrapped
        auto base = std::make_shared<DispatcherProcessor>(dispatcher_);
        auto streaming = std::make_shared<StreamingRequestProcessor>(base, cfg_.handlerCfg);
        RequestHandler handler(streaming, cfg_.handlerCfg);

        // Fire-and-forget the coroutine; it runs on whatever scheduler resumes the awaiters
        (void)handler.handle_connection(std::move(sock), token);
    } catch (const std::exception& e) {
        spdlog::error("handle_client exception: {}", e.what());
        ::close(client_fd);
    } catch (...) {
        spdlog::error("handle_client unknown exception");
        ::close(client_fd);
    }
}

} // namespace yams::daemon
