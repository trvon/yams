#include <yams/daemon/ipc/async_socket.h>

#include <spdlog/spdlog.h>
#include <atomic>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#if (defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)) && \
    !defined(HAVE_KQUEUE)
#define HAVE_KQUEUE 1
#endif
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif
#ifndef _WIN32
#include <sys/select.h>
#endif

#ifdef HAVE_IO_URING
#include <liburing.h>
#endif

#ifdef HAVE_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace yams::daemon {

// Static member definition
std::atomic<uint64_t> AsyncSocket::next_generation_{0};

// ============================================================================
// AsyncSocket Implementation
// ============================================================================

AsyncSocket::AsyncSocket(int fd, AsyncIOContext& context)
    : fd_(fd), context_(&context), generation_(++next_generation_) {
    if (fd_ >= 0) {
        make_nonblocking();
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
        int on = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
        context_->register_socket(fd_);
    }
}

AsyncSocket::AsyncSocket(AsyncSocket&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)), context_(std::exchange(other.context_, nullptr)),
      generation_(other.generation_.exchange(0)) {}

AsyncSocket& AsyncSocket::operator=(AsyncSocket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0 && context_) {
            context_->unregister_socket(fd_);
            ::close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
        context_ = std::exchange(other.context_, nullptr);
        generation_ = other.generation_.exchange(0);
    }
    return *this;
}

AsyncSocket::~AsyncSocket() {
    if (fd_ >= 0) {
        int old_fd = fd_;
        fd_ = -1;

        if (context_) {
            context_->cancel_pending_operations(old_fd);
            context_->unregister_socket(old_fd);
        }
        ::close(old_fd);
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

Result<void> AsyncSocket::set_socket_option(int level, int optname, const void* optval,
                                            socklen_t optlen) {
    if (setsockopt(fd_, level, optname, optval, optlen) < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to set socket option: " + std::string(strerror(errno))};
    }
    return Result<void>();
}

AsyncSocket::AwaiterHolder<AsyncSocket::ReadAwaiter> AsyncSocket::async_read(void* buffer,
                                                                             size_t size) {
    auto awaiter = std::make_shared<ReadAwaiter>(this, buffer, size);
    return AwaiterHolder<ReadAwaiter>(awaiter);
}

AsyncSocket::AwaiterHolder<AsyncSocket::WriteAwaiter> AsyncSocket::async_write(const void* buffer,
                                                                               size_t size) {
    auto awaiter = std::make_shared<WriteAwaiter>(this, buffer, size);
    return AwaiterHolder<WriteAwaiter>(awaiter);
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

// ============================================================================
// Awaiter Implementations
// ============================================================================

bool AsyncSocket::ReadAwaiter::await_ready() noexcept {
    if (cancelled.load())
        return true;
    ssize_t res = recv(socket->fd_, buffer, size, MSG_DONTWAIT);
    result.store(res, std::memory_order_relaxed);
    if (res >= 0) {
        error_code.store(0, std::memory_order_relaxed);
        return true;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
        error_code.store(errno, std::memory_order_relaxed);
        return true;
    }
    return false;
}

void AsyncSocket::ReadAwaiter::await_suspend(std::coroutine_handle<> h) {
    auto shared_self = shared_from_this();
    if (socket->fd_ >= 0) {
        socket->context_->submit_read(socket->fd_, buffer, size, h,
                                      std::static_pointer_cast<AwaiterBase>(shared_self),
                                      socket->generation_.load());
    } else {
        result.store(-1);
        error_code.store(ECONNRESET);
        cancelled.store(true);
        h.resume();
    }
}

Result<size_t> AsyncSocket::ReadAwaiter::await_resume() {
    if (cancelled.load()) {
        return Error{ErrorCode::OperationCancelled, "Read operation cancelled"};
    }
    ssize_t res = result.load(std::memory_order_acquire);
    int err = error_code.load(std::memory_order_acquire);
    if (res < 0) {
        int ec = (err == 0) ? ECONNRESET : err;
        return Error{ErrorCode::NetworkError, "Read failed: " + std::string(strerror(ec))};
    }
    return static_cast<size_t>(res);
}

bool AsyncSocket::WriteAwaiter::await_ready() noexcept {
    if (cancelled.load())
        return true;
    ssize_t res = send(socket->fd_, buffer, size, MSG_DONTWAIT | MSG_NOSIGNAL);
    result.store(res, std::memory_order_relaxed);
    if (res >= 0) {
        error_code.store(0, std::memory_order_relaxed);
        return true;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
        error_code.store(errno, std::memory_order_relaxed);
        return true;
    }
    return false;
}

void AsyncSocket::WriteAwaiter::await_suspend(std::coroutine_handle<> h) {
    auto shared_self = shared_from_this();
    if (socket->fd_ >= 0) {
        socket->context_->submit_write(socket->fd_, buffer, size, h,
                                       std::static_pointer_cast<AwaiterBase>(shared_self),
                                       socket->generation_.load());
    } else {
        result.store(-1);
        error_code.store(ECONNRESET);
        cancelled.store(true);
        h.resume();
    }
}

Result<size_t> AsyncSocket::WriteAwaiter::await_resume() {
    if (cancelled.load()) {
        return Error{ErrorCode::OperationCancelled, "Write operation cancelled"};
    }
    ssize_t res = result.load(std::memory_order_acquire);
    int err = error_code.load(std::memory_order_acquire);
    if (res < 0) {
        int ec = (err == 0) ? ECONNRESET : err;
        return Error{ErrorCode::NetworkError, "Write failed: " + std::string(strerror(ec))};
    }
    return static_cast<size_t>(res);
}

// ============================================================================
// Platform-specific AsyncIOContext Implementation
// ============================================================================

#ifdef HAVE_KQUEUE

// macOS/BSD implementation using kqueue
class AsyncIOContext::Impl {
public:
    Impl() : stop_requested_(false) {
        kq_ = kqueue();
        if (kq_ < 0) {
            throw std::runtime_error("Failed to create kqueue");
        }

        if (pipe(wake_pipe_) != 0) {
            ::close(kq_);
            throw std::runtime_error("Failed to create wake pipe");
        }

        int flags = fcntl(wake_pipe_[0], F_GETFL, 0);
        fcntl(wake_pipe_[0], F_SETFL, flags | O_NONBLOCK);

        struct kevent ev;
        EV_SET(&ev, wake_pipe_[0], EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    }

    ~Impl() {
        if (wake_pipe_[0] >= 0)
            ::close(wake_pipe_[0]);
        if (wake_pipe_[1] >= 0)
            ::close(wake_pipe_[1]);
        if (kq_ >= 0)
            ::close(kq_);
    }

    Result<void> register_socket([[maybe_unused]] int fd) { return Result<void>(); }

    void cancel_pending_operations(int fd) {
        std::deque<PendingOp> ops_to_cancel;
        {
            std::lock_guard lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                ops_to_cancel = std::move(it->second);
                pending_ops_.erase(it);
            }
        }

        for (auto& op : ops_to_cancel) {
            cancel_operation(op);
        }
    }

    Result<void> unregister_socket(int fd) {
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
        EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);

        std::lock_guard lock(mutex_);
        socket_generations_.erase(fd);
        return Result<void>();
    }

    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     std::shared_ptr<AsyncSocket::AwaiterBase> awaiter, uint64_t generation) {
        if (!awaiter || !h)
            return;
        awaiter->generation_snapshot = generation;

        PendingOp op;
        op.handle = h;
        op.awaiter_shared = awaiter;
        op.fd = fd;
        op.buffer = buffer;
        op.size = size;
        op.type = OpType::Read;
        op.generation = generation;

        {
            std::lock_guard lock(mutex_);
            socket_generations_[fd] = generation;
            pending_ops_[fd].push_back(std::move(op));
        }

        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, reinterpret_cast<void*>(fd));
        if (kevent(kq_, &ev, 1, nullptr, 0, nullptr) == -1) {
            spdlog::error("kevent EVFILT_READ register failed for fd {}: {}", fd, strerror(errno));
            // If registration fails, immediately complete with error
            std::lock_guard lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                std::erase_if(it->second,
                              [&](const PendingOp& p) { return p.awaiter_shared == awaiter; });
            }
            awaiter->result.store(-1);
            awaiter->error_code.store(errno);
            awaiter->cancelled.store(true);
            if (!h.done())
                h.resume();
        }
    }

    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      std::shared_ptr<AsyncSocket::AwaiterBase> awaiter, uint64_t generation) {
        if (!awaiter || !h)
            return;
        awaiter->generation_snapshot = generation;

        PendingOp op;
        op.handle = h;
        op.awaiter_shared = awaiter;
        op.fd = fd;
        op.data.assign(static_cast<const uint8_t*>(buffer),
                       static_cast<const uint8_t*>(buffer) + size);
        op.size = size;
        op.type = OpType::Write;
        op.generation = generation;

        {
            std::lock_guard lock(mutex_);
            socket_generations_[fd] = generation;
            pending_ops_[fd].push_back(std::move(op));
        }

        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, reinterpret_cast<void*>(fd));
        if (kevent(kq_, &ev, 1, nullptr, 0, nullptr) == -1) {
            spdlog::error("kevent EVFILT_WRITE register failed for fd {}: {}", fd, strerror(errno));
            std::lock_guard lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                std::erase_if(it->second,
                              [&](const PendingOp& p) { return p.awaiter_shared == awaiter; });
            }
            awaiter->result.store(-1);
            awaiter->error_code.store(errno);
            awaiter->cancelled.store(true);
            if (!h.done())
                h.resume();
        }
    }

    void run() {
        struct kevent events[MAX_EVENTS];
        struct timespec timeout = {0, 100000000}; // 100ms

        while (!stop_requested_.load(std::memory_order_acquire)) {
            int nev = kevent(kq_, nullptr, 0, events, MAX_EVENTS, &timeout);
            if (stop_requested_.load(std::memory_order_acquire))
                break;

            if (nev < 0) {
                if (errno == EINTR) {
                    continue;
                }
                spdlog::warn("AsyncIOContext(kqueue): kevent() error: {}", strerror(errno));
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            for (int i = 0; i < nev; ++i) {
                int fd = static_cast<int>(events[i].ident);

                // Wake pipe
                if (fd == wake_pipe_[0]) {
                    char buffer[8];
                    while (read(wake_pipe_[0], buffer, sizeof(buffer)) > 0) {
                    }
                    continue;
                }

                // Defensive error/EOF handling
                if (events[i].flags & EV_ERROR) {
                    int err = static_cast<int>(events[i].data);
                    spdlog::debug("AsyncIOContext(kqueue): EV_ERROR on fd {}: {}", fd,
                                  strerror(err));
                    cancel_pending_operations(fd);
                    unregister_socket(fd);
                    continue;
                }
                if (events[i].flags & EV_EOF) {
                    spdlog::debug("AsyncIOContext(kqueue): EV_EOF (peer closed) on fd {}", fd);
                    cancel_pending_operations(fd);
                    continue;
                }

                // Extract one pending op for this fd/filter
                PendingOp op;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto it = pending_ops_.find(fd);
                    if (it != pending_ops_.end() && !it->second.empty()) {
                        OpType want =
                            (events[i].filter == EVFILT_READ) ? OpType::Read : OpType::Write;
                        auto& dq = it->second;
                        auto op_it = std::find_if(dq.begin(), dq.end(), [&](const PendingOp& p) {
                            return p.type == want;
                        });
                        if (op_it != dq.end()) {
                            op = std::move(*op_it);
                            dq.erase(op_it);
                        }
                    }
                }

                if (!op.awaiter_shared || !op.handle)
                    continue;

                // Validate generation and cancellation before I/O
                bool is_valid = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto gen_it = socket_generations_.find(fd);
                    is_valid =
                        (gen_it != socket_generations_.end() && gen_it->second == op.generation);
                }
                if (!is_valid || op.handle.done() || op.awaiter_shared->cancelled.load()) {
                    spdlog::debug("AsyncIOContext(kqueue): stale/cancelled op (fd={}, gen={})", fd,
                                  op.generation);
                    continue;
                }

                // Perform non-blocking I/O
                ssize_t io_result = -1;
                int io_errno = 0;
                if (op.type == OpType::Read) {
                    io_result = recv(fd, op.buffer, op.size, MSG_DONTWAIT);
                    io_errno = errno;
                } else {
                    io_result = send(fd, op.data.data(), op.size, MSG_DONTWAIT | MSG_NOSIGNAL);
                    io_errno = errno;
                }
                if (io_result < 0) {
                    if (io_errno == 0) {
#ifdef EPIPE
                        io_errno = EPIPE;
#else
                        io_errno = ECONNRESET;
#endif
                    }
                    op.awaiter_shared->error_code.store(io_errno, std::memory_order_release);
                }
                op.awaiter_shared->result.store(io_result, std::memory_order_release);

                // Resume safely (final check)
                if (!op.handle.done() && !op.awaiter_shared->cancelled.load()) {
                    op.handle.resume();
                } else {
                    spdlog::debug("AsyncIOContext(kqueue): skip resume (done/cancelled) for fd {}",
                                  fd);
                }
            } // for events
        } // while
    }

    void stop() {
        stop_requested_.store(true, std::memory_order_release);
        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            write(wake_pipe_[1], &byte, 1);
        }
    }

private:
    enum class OpType { Read, Write };

    struct PendingOp {
        std::coroutine_handle<> handle;
        std::shared_ptr<AsyncSocket::AwaiterBase> awaiter_shared;
        int fd;
        void* buffer;
        std::vector<uint8_t> data;
        size_t size;
        OpType type;
        uint64_t generation = 0;
    };

    void cancel_operation(PendingOp& op) {
        auto awaiter = op.awaiter_shared;
        if (!awaiter || !op.handle || op.handle.done())
            return;

        awaiter->result.store(-1, std::memory_order_release);
        awaiter->error_code.store(ECONNRESET, std::memory_order_release);
        awaiter->cancelled.store(true, std::memory_order_release);

        try {
            op.handle.resume();
        } catch (...) {
        }
    }

    static constexpr int MAX_EVENTS = 64;
    int kq_;
    int wake_pipe_[2] = {-1, -1};
    mutable std::mutex mutex_;
    std::unordered_map<int, std::deque<PendingOp>> pending_ops_;
    std::unordered_map<int, uint64_t> socket_generations_;
    std::atomic<bool> stop_requested_{false};
};

#else // Fallback implementation

class AsyncIOContext::Impl {
public:
    Impl() = default;
    ~Impl() = default;

    Result<void> register_socket(int) { return Result<void>(); }
    Result<void> unregister_socket(int) { return Result<void>(); }

    void cancel_pending_operations(int) {}

    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     std::shared_ptr<AsyncSocket::AwaiterBase> awaiter, uint64_t) {
        ssize_t bytes = recv(fd, buffer, size, MSG_DONTWAIT);
        if (awaiter) {
            awaiter->result = bytes;
            if (bytes < 0)
                awaiter->error_code = errno;
        }
        h.resume();
    }

    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      std::shared_ptr<AsyncSocket::AwaiterBase> awaiter, uint64_t) {
        ssize_t bytes = send(fd, buffer, size, MSG_NOSIGNAL);
        if (awaiter) {
            awaiter->result = bytes;
            if (bytes < 0)
                awaiter->error_code = errno;
        }
        h.resume();
    }

    void run() {
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
// AsyncIOContext Public Methods
// ============================================================================

AsyncIOContext::AsyncIOContext() : pImpl(std::make_unique<Impl>()) {}
AsyncIOContext::~AsyncIOContext() {
    if (pImpl) {
        pImpl->stop();
    }
}
AsyncIOContext::AsyncIOContext(AsyncIOContext&&) noexcept = default;
AsyncIOContext& AsyncIOContext::operator=(AsyncIOContext&&) noexcept = default;

Result<void> AsyncIOContext::register_socket(int fd) {
    return pImpl->register_socket(fd);
}
Result<void> AsyncIOContext::unregister_socket(int fd) {
    return pImpl->unregister_socket(fd);
}
void AsyncIOContext::cancel_pending_operations(int fd) {
    pImpl->cancel_pending_operations(fd);
}

void AsyncIOContext::submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                                 std::shared_ptr<AsyncSocket::AwaiterBase> awaiter,
                                 uint64_t generation) {
    pImpl->submit_read(fd, buffer, size, h, awaiter, generation);
}

void AsyncIOContext::submit_write(int fd, const void* buffer, size_t size,
                                  std::coroutine_handle<> h,
                                  std::shared_ptr<AsyncSocket::AwaiterBase> awaiter,
                                  uint64_t generation) {
    pImpl->submit_write(fd, buffer, size, h, awaiter, generation);
}

void AsyncIOContext::run() {
    pImpl->run();
}
void AsyncIOContext::stop() {
    pImpl->stop();
}

} // namespace yams::daemon
