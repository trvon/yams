#pragma once

#include <yams/core/types.h>

// Try to include shared Task; if unavailable, provide a local minimal Task
#if __has_include(<yams/core/task.h>)
#include <yams/core/task.h>
#else
#include <coroutine>
#include <exception>
#include <optional>
#include <utility>
namespace yams {
template <typename T> class Task {
public:
    struct promise_type {
        std::optional<T> value_;
        std::exception_ptr exception_;
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        template <typename U> void return_value(U&& value) { value_ = std::forward<U>(value); }
        void unhandled_exception() { exception_ = std::current_exception(); }
    };
    using handle_type = std::coroutine_handle<promise_type>;
    explicit Task(handle_type h) : handle_(h) {}
    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    ~Task() {
        if (handle_)
            handle_.destroy();
    }
    bool await_ready() const noexcept { return handle_.done(); }
    void await_suspend(std::coroutine_handle<>) { handle_.resume(); }
    T await_resume() {
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
        return std::move(handle_.promise().value_.value());
    }
    T get() {
        if (!handle_.done())
            handle_.resume();
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
        return std::move(handle_.promise().value_.value());
    }

private:
    handle_type handle_{};
};
template <> class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception_;
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { exception_ = std::current_exception(); }
    };
    using handle_type = std::coroutine_handle<promise_type>;
    explicit Task(handle_type h) : handle_(h) {}
    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(Task&) = delete;
    ~Task() {
        if (handle_)
            handle_.destroy();
    }
    bool await_ready() const noexcept { return handle_.done(); }
    void await_suspend(std::coroutine_handle<>) { handle_.resume(); }
    void await_resume() {
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
    }
    void get() {
        if (!handle_.done())
            handle_.resume();
        if (handle_.promise().exception_)
            std::rethrow_exception(handle_.promise().exception_);
    }

private:
    handle_type handle_{};
};
} // namespace yams
namespace yams::daemon {
template <typename T = void> using Task = ::yams::Task<T>;
}
#endif

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <span>
#include <vector>

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
#include <sys/socket.h>
#include <sys/types.h>
#endif

namespace yams::daemon {

class AsyncSocket;
class AsyncIOContext;

class AsyncSocket {
public:
    AsyncSocket(int fd, AsyncIOContext& context);
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
        auto await_resume() { return awaiter_->await_resume(); }

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
    AsyncIOContext* context_ = nullptr;
    std::atomic<uint64_t> generation_{0};
    static std::atomic<uint64_t> next_generation_;

public:
    // Awaiter definitions
    struct AwaiterBase {
        AsyncSocket* socket;
        std::atomic<ssize_t> result{0};
        std::atomic<int> error_code{0};
        std::atomic<bool> cancelled{false};
        uint64_t generation_snapshot{0};

        explicit AwaiterBase(AsyncSocket* s) : socket(s) {}
        virtual ~AwaiterBase() = default;
    };

    struct ReadAwaiter : public AwaiterBase, public std::enable_shared_from_this<ReadAwaiter> {
        void* buffer;
        size_t size;

        ReadAwaiter(AsyncSocket* s, void* b, size_t sz) : AwaiterBase(s), buffer(b), size(sz) {}

        bool await_ready() noexcept;
        void await_suspend(std::coroutine_handle<> h);
        Result<size_t> await_resume();
    };

    struct WriteAwaiter : public AwaiterBase, public std::enable_shared_from_this<WriteAwaiter> {
        const void* buffer;
        size_t size;

        WriteAwaiter(AsyncSocket* s, const void* b, size_t sz)
            : AwaiterBase(s), buffer(b), size(sz) {}

        bool await_ready() noexcept;
        void await_suspend(std::coroutine_handle<> h);
        Result<size_t> await_resume();
    };
};

class AsyncIOContext {
public:
    AsyncIOContext();
    ~AsyncIOContext();

    AsyncIOContext(const AsyncIOContext&) = delete;
    AsyncIOContext& operator=(const AsyncIOContext&) = delete;
    AsyncIOContext(AsyncIOContext&&) noexcept;
    AsyncIOContext& operator=(AsyncIOContext&&) noexcept;

    Result<void> register_socket(int fd);
    Result<void> unregister_socket(int fd);
    void cancel_pending_operations(int fd);

    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     std::shared_ptr<AsyncSocket::AwaiterBase> awaiter, uint64_t generation);
    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      std::shared_ptr<AsyncSocket::AwaiterBase> awaiter, uint64_t generation);

    void run();
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::daemon
