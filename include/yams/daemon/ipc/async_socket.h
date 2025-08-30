#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>
#include <yams/core/task.h>
#include <yams/core/types.h>

// Platform socket includes/typedefs
#ifdef _WIN32
#include <BaseTsd.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#ifndef ssize_t
using ssize_t = SSIZE_T;
#endif
#ifndef socklen_t
using socklen_t = int;
#endif
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#else
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif
#endif

namespace yams::daemon {

// ============================================================================
// Concepts for Template Constraints
// ============================================================================

template <typename T>
concept IsIOContext = requires(T context, int fd, uint64_t generation) {
    // Registration methods
    { context.register_socket(fd, generation) } -> std::same_as<Result<void>>;
    { context.unregister_socket(fd) } -> std::same_as<Result<void>>;
    { context.cancel_pending_operations(fd) } -> std::same_as<void>;

    // Lifecycle methods
    { context.run() } -> std::same_as<void>;
    { context.stop() } -> std::same_as<void>;
};

// Forward declarations
template <typename IOContextT = class AsyncIOContext>
requires IsIOContext<IOContextT>
class AsyncSocket;

class AsyncIOContext;

// ============================================================================
// Non-templated awaiter base so AsyncIOContext pImpl can operate type-erased
// ============================================================================

struct SocketAwaiterBase : public std::enable_shared_from_this<SocketAwaiterBase> {
    std::atomic<ssize_t> result{0};
    std::atomic<int> error_code{0};
    std::atomic<bool> cancelled{false};
    // Single-resume guard: ensures a coroutine is resumed at most once across all paths
    std::atomic<bool> scheduled{false};
    // Alive flag: indicates if the awaiter is still valid
    std::atomic<bool> alive{true};
    virtual ~SocketAwaiterBase() = default;
};

// ============================================================================
// Templated AsyncSocket
// ============================================================================

template <typename IOContextT>
requires IsIOContext<IOContextT>
class AsyncSocket {
public:
    AsyncSocket(int fd, IOContextT& context);
    ~AsyncSocket();

    // Movable but not copyable
    AsyncSocket(const AsyncSocket&) = delete;
    AsyncSocket& operator=(const AsyncSocket&) = delete;
    AsyncSocket(AsyncSocket&&) noexcept;
    AsyncSocket& operator=(AsyncSocket&&) noexcept;

    // Awaitable operations
    struct ReadAwaiter;
    struct WriteAwaiter;

    template <typename Awaiter> class AwaiterHolder {
    public:
        explicit AwaiterHolder(std::shared_ptr<Awaiter> awaiter) : awaiter_(std::move(awaiter)) {}
        bool await_ready() noexcept { return awaiter_->await_ready(); }
        void await_suspend(std::coroutine_handle<> h) { awaiter_->await_suspend(h); }
        auto await_resume() {
            // Keep a strong reference to the awaiter object across await_resume()
            // to prevent premature destruction if the scheduler releases its ref
            // just before resumption.
            std::shared_ptr<Awaiter> keep_alive = awaiter_;
            (void)keep_alive;
            return awaiter_->await_resume();
        }

        // Synchronous helper: run the awaiter to completion and return the result
        auto get() {
            auto task_fn = [this]() -> Task<decltype(this->awaiter_->await_resume())> {
                co_return co_await *this;
            };
            auto t = task_fn();
            return t.get();
        }

    private:
        std::shared_ptr<Awaiter> awaiter_;
    };

    AwaiterHolder<ReadAwaiter> async_read(void* buffer, size_t size);
    AwaiterHolder<WriteAwaiter> async_write(const void* buffer, size_t size);

    // Higher-level operations
    Task<Result<std::vector<uint8_t>>> async_read_exact(size_t size);
    Task<Result<void>> async_write_all(std::span<const uint8_t> data);

    // Connection management
    void close();

    // Socket options
    Result<void> set_timeout(std::chrono::milliseconds timeout);
    Result<void> set_nodelay(bool enable);
    Result<void> set_keepalive(bool enable);

    bool is_valid() const { return fd_ >= 0; }
    int get_fd() const { return fd_; }

private:
    Result<void> make_nonblocking();
    Result<void> set_socket_option(int level, int optname, const void* optval, socklen_t optlen);

    int fd_ = -1;
    IOContextT* context_ = nullptr;
    std::atomic<uint64_t> generation_{0};
    static inline std::atomic<uint64_t> next_generation_{0};

public:
    // Awaiter definitions
    struct AwaiterBase : public SocketAwaiterBase {
        AsyncSocket<IOContextT>* socket;
        uint64_t generation_snapshot{0};

        explicit AwaiterBase(AsyncSocket<IOContextT>* s) : socket(s) {}
        ~AwaiterBase() override = default;
    };

    struct ReadAwaiter : public AwaiterBase {
        void* buffer;
        size_t size;

        ReadAwaiter(AsyncSocket<IOContextT>* s, void* b, size_t sz)
            : AwaiterBase(s), buffer(b), size(sz) {}

        bool await_ready() noexcept;
        void await_suspend(std::coroutine_handle<> h);
        Result<size_t> await_resume();
    };

    struct WriteAwaiter : public AwaiterBase {
        const void* buffer;
        size_t size;

        WriteAwaiter(AsyncSocket<IOContextT>* s, const void* b, size_t sz)
            : AwaiterBase(s), buffer(b), size(sz) {}

        bool await_ready() noexcept;
        void await_suspend(std::coroutine_handle<> h);
        Result<size_t> await_resume();
    };
};

// ============================================================================
// Default AsyncIOContext Implementation
// ============================================================================

class AsyncIOContext {
public:
    AsyncIOContext();
    ~AsyncIOContext();

    AsyncIOContext(const AsyncIOContext&) = delete;
    AsyncIOContext& operator=(const AsyncIOContext&) = delete;
    AsyncIOContext(AsyncIOContext&&) noexcept;
    AsyncIOContext& operator=(AsyncIOContext&&) noexcept;

    Result<void> register_socket(int fd, uint64_t generation);
    Result<void> unregister_socket(int fd);
    void cancel_pending_operations(int fd);

    template <typename IOContextT>
    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     std::shared_ptr<typename AsyncSocket<IOContextT>::AwaiterBase> awaiter,
                     uint64_t generation);

    template <typename IOContextT>
    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      std::shared_ptr<typename AsyncSocket<IOContextT>::AwaiterBase> awaiter,
                      uint64_t generation);

    // Type-erased submission used by templated overloads
    void submit_read_erased(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                            std::shared_ptr<SocketAwaiterBase> awaiter, uint64_t generation);
    void submit_write_erased(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                             std::shared_ptr<SocketAwaiterBase> awaiter, uint64_t generation);

    void run();
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

// ============================================================================
// Factory Functions
// ============================================================================

class AsyncSocketFactory {
public:
    template <typename IOContextT = AsyncIOContext>
    requires IsIOContext<IOContextT>
    static std::unique_ptr<AsyncSocket<IOContextT>> create_socket(int fd, IOContextT& context) {
        return std::make_unique<AsyncSocket<IOContextT>>(fd, context);
    }

    // Convenience factory for default configuration
    static std::unique_ptr<AsyncSocket<AsyncIOContext>>
    create_default_socket(int fd, AsyncIOContext& context) {
        return create_socket(fd, context);
    }
};

// ============================================================================
// Type Aliases
// ============================================================================

using DefaultAsyncSocket = AsyncSocket<AsyncIOContext>;

// ============================================================================
// Template Method Implementations (must be in header)
// ============================================================================

template <typename IOContextT>
requires IsIOContext<IOContextT>
AsyncSocket<IOContextT>::AsyncSocket(int fd, IOContextT& context)
    : fd_(fd), context_(&context), generation_(++next_generation_) {
    if (fd_ >= 0) {
        make_nonblocking();
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
        int on = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
        context_->register_socket(fd_, generation_.load());
    }
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
AsyncSocket<IOContextT>::AsyncSocket(AsyncSocket&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)), context_(std::exchange(other.context_, nullptr)),
      generation_(other.generation_.exchange(0)) {}

template <typename IOContextT>
requires IsIOContext<IOContextT>
AsyncSocket<IOContextT>& AsyncSocket<IOContextT>::operator=(AsyncSocket&& other) noexcept {
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

template <typename IOContextT>
requires IsIOContext<IOContextT>
AsyncSocket<IOContextT>::~AsyncSocket() {
    if (fd_ >= 0) {
        int old_fd = fd_;
        fd_ = -1;

        if (context_) {
            // Unregister will EV_DELETE and cancel any pending operations safely.
            context_->unregister_socket(old_fd);
        }

        // Mark all awaiters as not alive
        generation_.store(0, std::memory_order_release);
        ::close(old_fd);
    }
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
void AsyncSocket<IOContextT>::close() {
    if (fd_ >= 0) {
        int old_fd = fd_;
        fd_ = -1;

        if (context_) {
            // Unregister will EV_DELETE and cancel any pending operations safely.
            context_->unregister_socket(old_fd);
        }

        // Mark all awaiters as not alive
        generation_.store(0, std::memory_order_release);
        ::close(old_fd);
    }
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<void> AsyncSocket<IOContextT>::make_nonblocking() {
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

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<void> AsyncSocket<IOContextT>::set_socket_option(int level, int optname, const void* optval,
                                                        socklen_t optlen) {
    if (setsockopt(fd_, level, optname, optval, optlen) < 0) {
        return Error{ErrorCode::NetworkError,
                     "Failed to set socket option: " + std::string(strerror(errno))};
    }
    return Result<void>();
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
typename AsyncSocket<IOContextT>::template AwaiterHolder<
    typename AsyncSocket<IOContextT>::ReadAwaiter>
AsyncSocket<IOContextT>::async_read(void* buffer, size_t size) {
    auto awaiter = std::make_shared<ReadAwaiter>(this, buffer, size);
    return AwaiterHolder<ReadAwaiter>(awaiter);
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
typename AsyncSocket<IOContextT>::template AwaiterHolder<
    typename AsyncSocket<IOContextT>::WriteAwaiter>
AsyncSocket<IOContextT>::async_write(const void* buffer, size_t size) {
    auto awaiter = std::make_shared<WriteAwaiter>(this, buffer, size);
    return AwaiterHolder<WriteAwaiter>(awaiter);
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
Task<Result<std::vector<uint8_t>>> AsyncSocket<IOContextT>::async_read_exact(size_t size) {
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

template <typename IOContextT>
requires IsIOContext<IOContextT>
Task<Result<void>> AsyncSocket<IOContextT>::async_write_all(std::span<const uint8_t> data) {
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

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<void> AsyncSocket<IOContextT>::set_timeout(std::chrono::milliseconds timeout) {
    struct timeval tv;
    tv.tv_sec = timeout.count() / 1000;
    tv.tv_usec = (timeout.count() % 1000) * 1000;

    if (auto result = set_socket_option(SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); !result) {
        return result;
    }

    return set_socket_option(SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<void> AsyncSocket<IOContextT>::set_nodelay(bool enable) {
    int val = enable ? 1 : 0;
    return set_socket_option(IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<void> AsyncSocket<IOContextT>::set_keepalive(bool enable) {
    int val = enable ? 1 : 0;
    return set_socket_option(SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val));
}

// Awaiter implementations
template <typename IOContextT>
requires IsIOContext<IOContextT>
bool AsyncSocket<IOContextT>::ReadAwaiter::await_ready() noexcept {
    if (this->cancelled.load())
        return true;
    ssize_t res = recv(this->socket->fd_, buffer, size, MSG_DONTWAIT);
    this->result.store(res, std::memory_order_relaxed);
    if (res >= 0) {
        this->error_code.store(0, std::memory_order_relaxed);
        return true;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
        this->error_code.store(errno, std::memory_order_relaxed);
        return true;
    }
    return false;
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
void AsyncSocket<IOContextT>::ReadAwaiter::await_suspend(std::coroutine_handle<> h) {
    // Obtain a strong reference to this awaiter via shared_from_this()
    std::shared_ptr<SocketAwaiterBase> shared_self;
    try {
        shared_self = this->shared_from_this();
    } catch (const std::bad_weak_ptr&) {
        shared_self.reset();
    }
    if (this->socket->fd_ >= 0) {
        this->socket->context_->submit_read_erased(this->socket->get_fd(), buffer, size, h,
                                                   shared_self, this->socket->generation_.load());
    } else {
        this->result.store(-1, std::memory_order_relaxed);
        this->error_code.store(ECONNRESET, std::memory_order_relaxed);
        this->cancelled.store(true, std::memory_order_relaxed);
        try {
            if (h) {
                h.resume();
            }
        } catch (...) {
        }
    }
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<size_t> AsyncSocket<IOContextT>::ReadAwaiter::await_resume() {
    if (this->cancelled.load()) {
        return Error{ErrorCode::OperationCancelled, "Read operation cancelled"};
    }
    ssize_t res = this->result.load(std::memory_order_acquire);
    int err = this->error_code.load(std::memory_order_acquire);
    if (res < 0) {
        int ec = (err == 0) ? ECONNRESET : err;
        return Error{ErrorCode::NetworkError, "Read failed: " + std::string(strerror(ec))};
    }
    return static_cast<size_t>(res);
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
bool AsyncSocket<IOContextT>::WriteAwaiter::await_ready() noexcept {
    if (this->cancelled.load())
        return true;
    ssize_t res = send(this->socket->fd_, buffer, size, MSG_DONTWAIT | MSG_NOSIGNAL);
    this->result.store(res, std::memory_order_relaxed);
    if (res >= 0) {
        this->error_code.store(0, std::memory_order_relaxed);
        return true;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
        this->error_code.store(errno, std::memory_order_relaxed);
        return true;
    }
    return false;
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
void AsyncSocket<IOContextT>::WriteAwaiter::await_suspend(std::coroutine_handle<> h) {
    // Obtain a strong reference to this awaiter via shared_from_this()
    std::shared_ptr<SocketAwaiterBase> shared_self;
    try {
        shared_self = this->shared_from_this();
    } catch (const std::bad_weak_ptr&) {
        shared_self.reset();
    }
    if (this->socket->fd_ >= 0) {
        this->socket->context_->submit_write_erased(this->socket->get_fd(), buffer, size, h,
                                                    shared_self, this->socket->generation_.load());
    } else {
        this->result.store(-1, std::memory_order_relaxed);
        this->error_code.store(ECONNRESET, std::memory_order_relaxed);
        this->cancelled.store(true, std::memory_order_relaxed);
        try {
            if (h) {
                h.resume();
            }
        } catch (...) {
        }
    }
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
Result<size_t> AsyncSocket<IOContextT>::WriteAwaiter::await_resume() {
    if (this->cancelled.load()) {
        return Error{ErrorCode::OperationCancelled, "Write operation cancelled"};
    }
    ssize_t res = this->result.load(std::memory_order_acquire);
    int err = this->error_code.load(std::memory_order_acquire);
    if (res < 0) {
        int ec = (err == 0) ? ECONNRESET : err;
        return Error{ErrorCode::NetworkError, "Write failed: " + std::string(strerror(ec))};
    }
    if (res == 0) {
        // Non-blocking write that wrote zero bytes is not an error here; caller loops.
        return static_cast<size_t>(0);
    }
    return static_cast<size_t>(res);
}

// AsyncIOContext template method implementations (header-only)
template <typename IOContextT>
void AsyncIOContext::submit_read(
    int fd, void* buffer, size_t size, std::coroutine_handle<> h,
    std::shared_ptr<typename AsyncSocket<IOContextT>::AwaiterBase> awaiter, uint64_t generation) {
    // Forward via type-erased base to implementation
    submit_read_erased(fd, buffer, size, h, std::static_pointer_cast<SocketAwaiterBase>(awaiter),
                       generation);
}

template <typename IOContextT>
void AsyncIOContext::submit_write(
    int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
    std::shared_ptr<typename AsyncSocket<IOContextT>::AwaiterBase> awaiter, uint64_t generation) {
    // Forward via type-erased base to implementation
    submit_write_erased(fd, buffer, size, h, std::static_pointer_cast<SocketAwaiterBase>(awaiter),
                        generation);
}

} // namespace yams::daemon