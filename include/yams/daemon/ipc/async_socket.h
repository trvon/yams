#pragma once

#include <atomic>
#include <chrono>
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
        // Single-resume guard: ensures a coroutine is resumed at most once across all paths
        std::atomic<bool> scheduled{false};
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
