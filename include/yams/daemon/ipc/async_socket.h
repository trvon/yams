#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
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

struct SocketAwaiterBase {
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
    struct ReadAwaiter {};
    struct WriteAwaiter {};

    template <typename Awaiter> class AwaiterHolder {
    public:
        // Read ctor
        AwaiterHolder(AsyncSocket<IOContextT>* s, void* buffer, size_t size)
            : socket_(s), rbuf_(buffer), size_(size), is_write_(false),
              state_(std::make_shared<SocketAwaiterBase>()) {}
        // Write ctor
        AwaiterHolder(AsyncSocket<IOContextT>* s, const void* buffer, size_t size, int)
            : socket_(s), wbuf_(buffer), size_(size), is_write_(true),
              state_(std::make_shared<SocketAwaiterBase>()) {}

        bool await_ready() noexcept {
            // Short-circuit only for pre-cancellation or immediate non-blocking success/failure
            if (!socket_ || socket_->fd_ < 0) {
                state_->result.store(-1, std::memory_order_relaxed);
                state_->error_code.store(ECONNRESET, std::memory_order_relaxed);
                state_->cancelled.store(true, std::memory_order_relaxed);
                return true;
            }

            // Fast non-blocking path on caller thread
            if constexpr (std::is_same<Awaiter, ReadAwaiter>::value) {
                ssize_t res = recv(socket_->fd_, rbuf_, size_, MSG_DONTWAIT);
                state_->result.store(res, std::memory_order_relaxed);
                if (res >= 0) {
                    state_->error_code.store(0, std::memory_order_relaxed);
                    return true;
                }
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    state_->error_code.store(errno, std::memory_order_relaxed);
                    return true;
                }
                return false;
            } else {
                ssize_t res = send(socket_->fd_, wbuf_, size_, MSG_DONTWAIT | MSG_NOSIGNAL);
                state_->result.store(res, std::memory_order_relaxed);
                if (res >= 0) {
                    state_->error_code.store(0, std::memory_order_relaxed);
                    return true;
                }
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    state_->error_code.store(errno, std::memory_order_relaxed);
                    return true;
                }
                return false;
            }
        }

        void await_suspend(std::coroutine_handle<> h) {
            if (!socket_ || socket_->fd_ < 0) {
                // Already closed; mark cancelled and resume synchronously
                state_->result.store(-1, std::memory_order_relaxed);
                state_->error_code.store(ECONNRESET, std::memory_order_relaxed);
                state_->cancelled.store(true, std::memory_order_relaxed);
                try {
                    if (h)
                        h.resume();
                } catch (...) {
                }
                return;
            }
            const int fd = socket_->get_fd();
            const uint64_t gen = socket_->generation_.load();
            if constexpr (std::is_same<Awaiter, ReadAwaiter>::value) {
                socket_->context_->submit_read_erased(fd, rbuf_, size_, h, state_, gen);
            } else {
                socket_->context_->submit_write_erased(fd, wbuf_, size_, h, state_, gen);
            }
        }

        Result<size_t> await_resume() {
            if (state_->cancelled.load(std::memory_order_acquire)) {
                return Result<size_t>{Error{ErrorCode::OperationCancelled, "Operation cancelled"}};
            }
            ssize_t res = state_->result.load(std::memory_order_acquire);
            int err = state_->error_code.load(std::memory_order_acquire);
            if (res < 0) {
                int ec = (err == 0) ? ECONNRESET : err;
                return Result<size_t>{Error{
                    ErrorCode::NetworkError,
                    (is_write_ ? std::string("Write failed: ") : std::string("Read failed: ")) +
                        std::string(strerror(ec))}};
            }
            return static_cast<size_t>(res);
        }

        // Synchronous helper: run to completion and return the result
        auto get() {
            auto task_fn = [this]() -> Task<decltype(this->await_resume())> {
                co_return co_await *this;
            };
            auto t = task_fn();
            return t.get();
        }

    private:
        AsyncSocket<IOContextT>* socket_ = nullptr;
        union {
            void* rbuf_ = nullptr;
            const void* wbuf_;
        };
        size_t size_ = 0;
        bool is_write_ = false;
        std::shared_ptr<SocketAwaiterBase> state_;
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
    // Legacy awaiter tags retained for API compatibility; all logic lives in AwaiterHolder.
    // No per-awaitable allocation occurs anymore.
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

    // Post a functor to be executed on the IO thread.
    // Thread-safe: can be called from any thread. The functor will run on the IO loop.
    void post(std::function<void()> fn);

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
    return AwaiterHolder<ReadAwaiter>(this, buffer, size);
}

template <typename IOContextT>
requires IsIOContext<IOContextT>
typename AsyncSocket<IOContextT>::template AwaiterHolder<
    typename AsyncSocket<IOContextT>::WriteAwaiter>
AsyncSocket<IOContextT>::async_write(const void* buffer, size_t size) {
    return AwaiterHolder<WriteAwaiter>(this, buffer, size, 0);
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

// Awaiter implementations removed; logic handled by AwaiterHolder only

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