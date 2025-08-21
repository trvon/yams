#include <yams/daemon/ipc/async_socket.h>

#include <spdlog/spdlog.h>
#include <atomic>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#ifdef HAVE_IO_URING
#include <liburing.h>
#endif

#ifdef HAVE_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace yams::daemon {

// ============================================================================
// AsyncSocket Implementation
// ============================================================================

AsyncSocket::AsyncSocket(int fd, AsyncIOContext& context) : fd_(fd), context_(&context) {
    if (fd_ >= 0) {
        make_nonblocking();
        context_->register_socket(fd_);
    }
}

AsyncSocket::AsyncSocket(AsyncSocket&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)), context_(std::exchange(other.context_, nullptr)) {}

AsyncSocket& AsyncSocket::operator=(AsyncSocket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0 && context_) {
            context_->unregister_socket(fd_);
            ::close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
        context_ = std::exchange(other.context_, nullptr);
    }
    return *this;
}

AsyncSocket::~AsyncSocket() {
    if (fd_ >= 0) {
        if (context_) {
            context_->unregister_socket(fd_);
        }
        ::close(fd_);
    }
}

Result<void> AsyncSocket::make_nonblocking() {
    int flags = fcntl(fd_, F_GETFL, 0);
    if (flags < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to get socket flags: " + std::string(strerror(errno))};
    }

    if (fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to set non-blocking mode: " + std::string(strerror(errno))};
    }

    return Result<void>();
}

Result<void> AsyncSocket::set_timeout(std::chrono::milliseconds timeout) {
    struct timeval tv;
    tv.tv_sec = timeout.count() / 1000;
    tv.tv_usec = (timeout.count() % 1000) * 1000;

    if (auto result = set_socket_option(SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); !result) {
        return result;
    }

    return set_socket_option(SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

Result<void> AsyncSocket::set_nodelay(bool enable) {
    int val = enable ? 1 : 0;
    return set_socket_option(IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
}

Result<void> AsyncSocket::set_keepalive(bool enable) {
    int val = enable ? 1 : 0;
    return set_socket_option(SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val));
}

Result<void> AsyncSocket::set_socket_option(int level, int optname, const void* optval,
                                            socklen_t optlen) {
    if (setsockopt(fd_, level, optname, optval, optlen) < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to set socket option: " + std::string(strerror(errno))};
    }
    return Result<void>();
}

AsyncSocket::ReadAwaiter AsyncSocket::async_read(void* buffer, size_t size) {
    return ReadAwaiter{this, buffer, size};
}

AsyncSocket::WriteAwaiter AsyncSocket::async_write(const void* buffer, size_t size) {
    return WriteAwaiter{this, buffer, size};
}

Task<Result<std::vector<uint8_t>>> AsyncSocket::async_read_exact(size_t size) {
    std::vector<uint8_t> buffer(size);
    size_t total_read = 0;

    while (total_read < size) {
        auto result = co_await async_read(buffer.data() + total_read, size - total_read);

        if (!result) {
            co_return result.error();
        }

        size_t bytes_read = result.value();
        if (bytes_read == 0) {
            co_return Error{ErrorCode::NetworkError, "Connection closed"};
        }

        total_read += bytes_read;
    }

    co_return buffer;
}

Task<Result<void>> AsyncSocket::async_write_all(std::span<const uint8_t> data) {
    size_t total_written = 0;

    while (total_written < data.size()) {
        auto result =
            co_await async_write(data.data() + total_written, data.size() - total_written);

        if (!result) {
            co_return result.error();
        }

        total_written += result.value();
    }

    co_return Result<void>();
}

// ============================================================================
// ReadAwaiter Implementation
// ============================================================================

bool AsyncSocket::ReadAwaiter::await_ready() const noexcept {
    // Try non-blocking read first
    result = recv(socket->fd_, buffer, size, MSG_DONTWAIT);
    if (result >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
        error_code = errno;
        return true; // Operation completed immediately
    }
    return false; // Need to suspend
}

void AsyncSocket::ReadAwaiter::await_suspend(std::coroutine_handle<> h) {
    socket->context_->submit_read(socket->fd_, buffer, size, h, this);
}

Result<size_t> AsyncSocket::ReadAwaiter::await_resume() {
    if (result < 0) {
        if (error_code == EAGAIN || error_code == EWOULDBLOCK) {
            // Should retry - this shouldn't happen after suspend
            return Error{ErrorCode::NetworkError, "Unexpected EAGAIN after async wait"};
        }
        return Error{ErrorCode::NetworkError, "Read failed: " + std::string(strerror(error_code))};
    }
    return static_cast<size_t>(result);
}

// ============================================================================
// WriteAwaiter Implementation
// ============================================================================

bool AsyncSocket::WriteAwaiter::await_ready() const noexcept {
    // Try non-blocking write first
    result = send(socket->fd_, buffer, size, MSG_DONTWAIT | MSG_NOSIGNAL);
    if (result >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
        error_code = errno;
        return true; // Operation completed immediately
    }
    return false; // Need to suspend
}

void AsyncSocket::WriteAwaiter::await_suspend(std::coroutine_handle<> h) {
    socket->context_->submit_write(socket->fd_, buffer, size, h, this);
}

Result<size_t> AsyncSocket::WriteAwaiter::await_resume() {
    if (result < 0) {
        if (error_code == EAGAIN || error_code == EWOULDBLOCK) {
            // Should retry - this shouldn't happen after suspend
            return Error{ErrorCode::NetworkError, "Unexpected EAGAIN after async wait"};
        }
        return Error{ErrorCode::NetworkError, "Write failed: " + std::string(strerror(error_code))};
    }
    return static_cast<size_t>(result);
}

// ============================================================================
// Platform-specific AsyncIOContext Implementation
// ============================================================================

#ifdef HAVE_IO_URING

// Linux implementation using io_uring
class AsyncIOContext::Impl {
public:
    Impl() {
        if (io_uring_queue_init(QUEUE_DEPTH, &ring_, 0) < 0) {
            throw std::runtime_error("Failed to initialize io_uring");
        }
    }

    ~Impl() { io_uring_queue_exit(&ring_); }

    Result<void> register_socket(int fd) {
        // io_uring doesn't require explicit registration for basic I/O
        return Result<void>();
    }

    Result<void> unregister_socket(int fd) {
        // io_uring doesn't require explicit unregistration
        return Result<void>();
    }

    struct UringOp {
        std::coroutine_handle<> handle;
        void* awaiter;
        bool isWrite;
        // Use atomic to ensure thread-safe access
        std::atomic<bool> completed{false};
        ssize_t result = -1;
        int error_code = 0;
    };

    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     void* awaiterPtr) {
        auto* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            auto* awaiter = reinterpret_cast<AsyncSocket::ReadAwaiter*>(awaiterPtr);
            awaiter->result = -1;
            awaiter->error_code = EAGAIN;
            h.resume();
            return;
        }

        io_uring_prep_recv(sqe, fd, buffer, size, 0);
        auto* op = new UringOp{h, awaiterPtr, false};
        io_uring_sqe_set_data(sqe, op);
        io_uring_submit(&ring_);
    }

    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      void* awaiterPtr) {
        auto* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            auto* awaiter = reinterpret_cast<AsyncSocket::WriteAwaiter*>(awaiterPtr);
            awaiter->result = -1;
            awaiter->error_code = EAGAIN;
            h.resume();
            return;
        }

        io_uring_prep_send(sqe, fd, buffer, size, MSG_NOSIGNAL);
        auto* op = new UringOp{h, awaiterPtr, true};
        io_uring_sqe_set_data(sqe, op);
        io_uring_submit(&ring_);
    }

    void run() {
        struct io_uring_cqe* cqe;

        while (!stop_requested_) {
            if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
                continue;
            }

            auto* op = reinterpret_cast<UringOp*>(io_uring_cqe_get_data(cqe));
            if (op) {
                // Store results first
                op->result = cqe->res;
                op->error_code = (cqe->res < 0) ? -cqe->res : 0;
                
                // Mark as completed atomically
                bool was_completed = op->completed.exchange(true);
                
                if (!was_completed && op->awaiter) {
                    // Copy results to the awaiter
                    if (op->isWrite) {
                        auto* awaiter = reinterpret_cast<AsyncSocket::WriteAwaiter*>(op->awaiter);
                        awaiter->result = op->result;
                        awaiter->error_code = op->error_code;
                    } else {
                        auto* awaiter = reinterpret_cast<AsyncSocket::ReadAwaiter*>(op->awaiter);
                        awaiter->result = op->result;
                        awaiter->error_code = op->error_code;
                    }
                    
                    // Resume the coroutine
                    auto h2 = op->handle;
                    delete op;
                    if (h2) {
                        h2.resume();
                    }
                } else {
                    // Already completed or awaiter invalid, just clean up
                    delete op;
                }
            }

            io_uring_cqe_seen(&ring_, cqe);
        }
    }

    void stop() { stop_requested_ = true; }

private:
    static constexpr unsigned QUEUE_DEPTH = 256;
    struct io_uring ring_;
    std::atomic<bool> stop_requested_{false};
};

#elif defined(HAVE_KQUEUE)

// macOS/BSD implementation using kqueue
class AsyncIOContext::Impl {
public:
    Impl() {
        kq_ = kqueue();
        if (kq_ < 0) {
            throw std::runtime_error("Failed to create kqueue");
        }

        // Create wake pipe for clean shutdown
        if (pipe(wake_pipe_) != 0) {
            ::close(kq_);
            throw std::runtime_error("Failed to create wake pipe");
        }

        // Make read end non-blocking
        int flags = fcntl(wake_pipe_[0], F_GETFL, 0);
        fcntl(wake_pipe_[0], F_SETFL, flags | O_NONBLOCK);

        // Register wake pipe with kqueue
        struct kevent ev;
        EV_SET(&ev, wake_pipe_[0], EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    }

    ~Impl() {
        if (wake_pipe_[0] >= 0) {
            ::close(wake_pipe_[0]);
        }
        if (wake_pipe_[1] >= 0) {
            ::close(wake_pipe_[1]);
        }
        if (kq_ >= 0) {
            ::close(kq_);
        }
    }

    Result<void> register_socket(int fd) {
        // kqueue doesn't require explicit registration
        return Result<void>();
    }

    Result<void> unregister_socket(int fd) {
        // Remove any pending events for this fd
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
        EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
        return Result<void>();
    }

    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     void* awaiterPtr) {
        PendingOp op{h, awaiterPtr, fd, buffer, size, OpType::Read};

        {
            std::lock_guard lock(mutex_);
            pending_ops_[fd].push_back(op);
        }

        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, reinterpret_cast<void*>(fd));
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    }

    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      void* awaiterPtr) {
        PendingOp op{h, awaiterPtr, fd, const_cast<void*>(buffer), size, OpType::Write};

        {
            std::lock_guard lock(mutex_);
            pending_ops_[fd].push_back(op);
        }

        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, reinterpret_cast<void*>(fd));
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    }

    void run() {
        struct kevent events[MAX_EVENTS];

        // Use 100ms timeout to allow checking stop_requested_ periodically
        struct timespec timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 100000000; // 100ms

        while (!stop_requested_) {
            int nev = kevent(kq_, nullptr, 0, events, MAX_EVENTS, &timeout);

            // Check if we should stop even if no events
            if (stop_requested_) {
                break;
            }

            for (int i = 0; i < nev; ++i) {
                int fd = static_cast<int>(events[i].ident);

                // Handle wake pipe - just drain it and continue
                if (fd == wake_pipe_[0]) {
                    char buffer[256];
                    while (read(wake_pipe_[0], buffer, sizeof(buffer)) > 0) {
                        // Drain the pipe
                    }
                    continue;
                }

                std::lock_guard lock(mutex_);
                auto it = pending_ops_.find(fd);
                if (it != pending_ops_.end() && !it->second.empty()) {
                    auto op = it->second.front();
                    it->second.pop_front();

                    // Perform the actual I/O operation
                    if (op.type == OpType::Read) {
                        auto* awaiter = reinterpret_cast<AsyncSocket::ReadAwaiter*>(op.awaiter);
                        awaiter->result = recv(fd, op.buffer, op.size, 0);
                        awaiter->error_code = errno;
                    } else {
                        auto* awaiter = reinterpret_cast<AsyncSocket::WriteAwaiter*>(op.awaiter);
                        awaiter->result = send(fd, op.buffer, op.size, MSG_NOSIGNAL);
                        awaiter->error_code = errno;
                    }

                    op.handle.resume();
                }
            }
        }
    }

    void stop() {
        stop_requested_ = true;
        // Wake up blocked kevent
        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            write(wake_pipe_[1], &byte, 1);
        }
    }

private:
    enum class OpType { Read, Write };

    struct PendingOp {
        std::coroutine_handle<> handle;
        void* awaiter;
        int fd;
        void* buffer;
        size_t size;
        OpType type;
    };

    static constexpr int MAX_EVENTS = 64;
    int kq_;
    int wake_pipe_[2] = {-1, -1};
    std::atomic<bool> stop_requested_{false};
    std::mutex mutex_;
    std::unordered_map<int, std::deque<PendingOp>> pending_ops_;
};

#else

// Fallback implementation using poll/select for other platforms
class AsyncIOContext::Impl {
public:
    Impl() {}
    ~Impl() {}

    Result<void> register_socket(int /*fd*/) { return Result<void>(); }

    Result<void> unregister_socket(int /*fd*/) { return Result<void>(); }

    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     void* awaiterPtr) {
        // Simple blocking read for fallback
        ssize_t result = recv(fd, buffer, size, 0);
        auto* awaiter = reinterpret_cast<AsyncSocket::ReadAwaiter*>(awaiterPtr);
        awaiter->result = result;
        awaiter->error_code = errno;
        h.resume();
    }

    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      void* awaiterPtr) {
        // Simple blocking write for fallback
        ssize_t result = send(fd, buffer, size, MSG_NOSIGNAL);
        auto* awaiter = reinterpret_cast<AsyncSocket::WriteAwaiter*>(awaiterPtr);
        awaiter->result = result;
        awaiter->error_code = errno;
        h.resume();
    }

    void run() {
        // Fallback: no event loop needed for blocking I/O
        while (!stop_requested_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    void stop() { stop_requested_ = true; }

private:
    std::atomic<bool> stop_requested_{false};
};

#endif

// ============================================================================
// AsyncIOContext Implementation
// ============================================================================

AsyncIOContext::AsyncIOContext() : pImpl(std::make_unique<Impl>()) {}
AsyncIOContext::~AsyncIOContext() = default;

AsyncIOContext::AsyncIOContext(AsyncIOContext&&) noexcept = default;
AsyncIOContext& AsyncIOContext::operator=(AsyncIOContext&&) noexcept = default;

Result<void> AsyncIOContext::register_socket(int fd) {
    return pImpl->register_socket(fd);
}

Result<void> AsyncIOContext::unregister_socket(int fd) {
    return pImpl->unregister_socket(fd);
}

void AsyncIOContext::submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                                 void* awaiterPtr) {
    pImpl->submit_read(fd, buffer, size, h, awaiterPtr);
}

void AsyncIOContext::submit_write(int fd, const void* buffer, size_t size,
                                  std::coroutine_handle<> h, void* awaiterPtr) {
    pImpl->submit_write(fd, buffer, size, h, awaiterPtr);
}

void AsyncIOContext::run() {
    pImpl->run();
}

void AsyncIOContext::stop() {
    pImpl->stop();
}

} // namespace yams::daemon
