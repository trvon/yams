#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>
#include <sys/socket.h>

namespace yams::daemon {

// Forward declarations
class AsyncIOContext;

// Coroutine task type for async operations
template <typename T = void> class Task {
public:
    struct promise_type {
        std::optional<T> value_;
        std::exception_ptr exception_;

        promise_type() = default;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }

        void return_value(T value) { value_ = std::move(value); }
        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}
    ~Task() {
        if (handle_)
            handle_.destroy();
    }

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    T get() {
        if (!handle_.done()) {
            handle_.resume();
        }
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        return std::move(handle_.promise().value_.value());
    }

    // Make Task awaitable
    bool await_ready() const noexcept { return handle_.done(); }

    void await_suspend([[maybe_unused]] std::coroutine_handle<> h) { handle_.resume(); }

    T await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        return std::move(handle_.promise().value_.value());
    }

private:
    handle_type handle_;
};

// Specialization for void
template <> class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception_;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }

        void return_void() {}
        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}
    ~Task() {
        if (handle_)
            handle_.destroy();
    }

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    void get() {
        if (!handle_.done()) {
            handle_.resume();
        }
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
    }

    // Make Task<void> awaitable
    bool await_ready() const noexcept { return handle_.done(); }

    void await_suspend([[maybe_unused]] std::coroutine_handle<> h) { handle_.resume(); }

    void await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
    }

private:
    handle_type handle_;
};

// Async socket wrapper with coroutine support
class AsyncSocket {
public:
    explicit AsyncSocket(int fd, AsyncIOContext& context);
    AsyncSocket(AsyncSocket&& other) noexcept;
    AsyncSocket& operator=(AsyncSocket&& other) noexcept;
    ~AsyncSocket();

    // Delete copy operations
    AsyncSocket(const AsyncSocket&) = delete;
    AsyncSocket& operator=(const AsyncSocket&) = delete;

    // Awaitable for async read operation
    struct ReadAwaiter {
        AsyncSocket* socket;
        void* buffer;
        size_t size;
        mutable ssize_t result = -1;
        mutable int error_code = 0;

        bool await_ready() const noexcept;
        void await_suspend(std::coroutine_handle<> h);
        Result<size_t> await_resume();
    };

    // Awaitable for async write operation
    struct WriteAwaiter {
        AsyncSocket* socket;
        const void* buffer;
        size_t size;
        mutable ssize_t result = -1;
        mutable int error_code = 0;

        bool await_ready() const noexcept;
        void await_suspend(std::coroutine_handle<> h);
        Result<size_t> await_resume();
    };

    // Async operations
    [[nodiscard]] ReadAwaiter async_read(void* buffer, size_t size);
    [[nodiscard]] WriteAwaiter async_write(const void* buffer, size_t size);
    [[nodiscard]] Task<Result<std::vector<uint8_t>>> async_read_exact(size_t size);
    [[nodiscard]] Task<Result<void>> async_write_all(std::span<const uint8_t> data);

    // Socket configuration
    Result<void> set_timeout(std::chrono::milliseconds timeout);
    Result<void> set_nodelay(bool enable);
    Result<void> set_keepalive(bool enable);

    // Socket state
    bool is_valid() const { return fd_ >= 0; }
    int native_handle() const { return fd_; }

private:
    int fd_ = -1;
    AsyncIOContext* context_ = nullptr;

    Result<void> make_nonblocking();
    Result<void> set_socket_option(int level, int optname, const void* optval, socklen_t optlen);
};

// Platform-specific async I/O context
class AsyncIOContext {
public:
    AsyncIOContext();
    ~AsyncIOContext();

    // Delete copy operations to prevent accidental copies
    AsyncIOContext(const AsyncIOContext&) = delete;
    AsyncIOContext& operator=(const AsyncIOContext&) = delete;

    // Allow move operations for unique ownership
    AsyncIOContext(AsyncIOContext&&) noexcept;
    AsyncIOContext& operator=(AsyncIOContext&&) noexcept;

    // Register socket for async operations
    Result<void> register_socket(int fd);
    Result<void> unregister_socket(int fd);

    // Submit async operations
    // The awaiterPtr must point to the corresponding ReadAwaiter/WriteAwaiter instance
    void submit_read(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                     void* awaiterPtr);
    void submit_write(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                      void* awaiterPtr);

    // Process completions
    void run();
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::daemon
