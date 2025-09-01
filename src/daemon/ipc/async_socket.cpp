#include <yams/daemon/ipc/async_socket.h>

#include <spdlog/spdlog.h>
#include <atomic>
#include <cassert>
#include <coroutine>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <mutex>
#include <queue>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <yams/core/types.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>

namespace yams::daemon {
// Timer compatibility: use deadline_timer for broad Boost.Asio compatibility
using IoTimer = boost::asio::deadline_timer;
static inline auto to_timer_duration(std::chrono::milliseconds d) {
    return boost::posix_time::milliseconds(d.count());
}

// ============================================================================
// AsyncIOContext Implementation
// ============================================================================

class AsyncIOContext::Impl {
public:
    Impl() : stop_requested_(false), io_(), work_guard_(boost::asio::make_work_guard(io_)) {}

    ~Impl() {
        try {
            stop();
        } catch (...) {
        }
    }

    yams::Result<void> register_socket(int fd, uint64_t generation) {
        std::lock_guard<std::mutex> lock(mutex_);
        try {
            auto desc = std::make_shared<boost::asio::posix::stream_descriptor>(io_, fd);
            descriptors_[fd] = desc;
            socket_generations_[fd] = generation;
            return yams::Result<void>();
        } catch (const std::exception& e) {
            return yams::Error{yams::ErrorCode::NetworkError,
                               std::string{"AsyncIOContext(Asio): register_socket failed: "} + e.what()};
        }
    }

    yams::Result<void> unregister_socket(int fd) {
        std::shared_ptr<boost::asio::posix::stream_descriptor> desc;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = descriptors_.find(fd);
            if (it != descriptors_.end()) {
                desc = it->second; // keep shared ownership during cancel/drain
                descriptors_.erase(it);
            }
            socket_generations_.erase(fd);
        }
        if (desc) {
            boost::system::error_code ec;
            desc->cancel(ec);
            // Release native handle so owner (::close) remains the single closer
            desc->release();
            // Ensure all canceled operations are processed before returning to reduce UAF risk
            try {
                io_.restart();
                io_.run_for(std::chrono::milliseconds(0));
            } catch (...) {
            }
        }
        return yams::Result<void>();
    }

    void cancel_pending_operations(int fd) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = descriptors_.find(fd);
        if (it != descriptors_.end() && it->second) {
            boost::system::error_code ec;
            it->second->cancel(ec);
        }
    }

    void post(std::function<void()> fn) {
        try {
            boost::asio::post(io_, std::move(fn));
        } catch (...) {
            // Swallow; best-effort
        }
    }

    void submit_read_erased(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                            std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter,
                            uint64_t generation) {
        if (!awaiter || !h)
            return;

        boost::asio::post(io_, [this, fd, buffer, size, h, awaiter, generation]() mutable {
            std::shared_ptr<boost::asio::posix::stream_descriptor> desc_shared;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto gen_it = socket_generations_.find(fd);
                if (gen_it == socket_generations_.end() || gen_it->second != generation) {
                    // cancel
                    awaiter->result.store(-1, std::memory_order_release);
                    awaiter->error_code.store(ECONNRESET, std::memory_order_release);
                    awaiter->cancelled.store(true, std::memory_order_release);
                    safe_resume_(awaiter, h);
                    return;
                }
                auto it = descriptors_.find(fd);
                if (it == descriptors_.end() || !it->second) {
                    awaiter->result.store(-1, std::memory_order_release);
                    awaiter->error_code.store(EBADF, std::memory_order_release);
                    awaiter->cancelled.store(true, std::memory_order_release);
                    safe_resume_(awaiter, h);
                    return;
                }
                // keep shared ownership for the duration of async op
                desc_shared = it->second;
            }

            auto buf = boost::asio::mutable_buffer(buffer, size);
            desc_shared->async_read_some(
                buf, [this, awaiter, h](const boost::system::error_code& ec, std::size_t n) {
                    if (ec) {
                        int err = ec.value() ? ec.value() : ECONNRESET;
                        awaiter->error_code.store(err, std::memory_order_release);
                        if (ec == boost::asio::error::operation_aborted) {
                            awaiter->cancelled.store(true, std::memory_order_release);
                        }
                        awaiter->result.store(-1, std::memory_order_release);
                    } else {
                        awaiter->error_code.store(0, std::memory_order_release);
                        awaiter->result.store(static_cast<ssize_t>(n), std::memory_order_release);
                    }
                    safe_resume_(awaiter, h);
                });
        });
    }

    void submit_write_erased(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                             std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter,
                             uint64_t generation) {
        if (!awaiter || !h)
            return;

        boost::asio::post(io_, [this, fd, buffer, size, h, awaiter, generation]() mutable {
            std::shared_ptr<boost::asio::posix::stream_descriptor> desc_shared;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto gen_it = socket_generations_.find(fd);
                if (gen_it == socket_generations_.end() || gen_it->second != generation) {
                    awaiter->result.store(-1, std::memory_order_release);
                    awaiter->error_code.store(ECONNRESET, std::memory_order_release);
                    awaiter->cancelled.store(true, std::memory_order_release);
                    safe_resume_(awaiter, h);
                    return;
                }
                auto it = descriptors_.find(fd);
                if (it == descriptors_.end() || !it->second) {
                    awaiter->result.store(-1, std::memory_order_release);
                    awaiter->error_code.store(EBADF, std::memory_order_release);
                    awaiter->cancelled.store(true, std::memory_order_release);
                    safe_resume_(awaiter, h);
                    return;
                }
                desc_shared = it->second;
            }

            auto buf = boost::asio::const_buffer(buffer, size);
            desc_shared->async_write_some(
                buf, [this, awaiter, h](const boost::system::error_code& ec, std::size_t n) {
                    if (ec) {
                        int err = ec.value() ? ec.value() : EPIPE;
                        awaiter->error_code.store(err, std::memory_order_release);
                        if (ec == boost::asio::error::operation_aborted) {
                            awaiter->cancelled.store(true, std::memory_order_release);
                        }
                        awaiter->result.store(-1, std::memory_order_release);
                    } else {
                        awaiter->error_code.store(0, std::memory_order_release);
                        awaiter->result.store(static_cast<ssize_t>(n), std::memory_order_release);
                    }
                    safe_resume_(awaiter, h);
                });
        });
    }

    void run() {
        // Run until stop requested; allow cooperative stop
        while (!stop_requested_.load(std::memory_order_acquire)) {
            try {
                io_.restart();
                io_.run_for(std::chrono::milliseconds(100));
            } catch (const std::exception& e) {
                spdlog::warn("AsyncIOContext(Asio): io.run_for exception: {}", e.what());
            }
        }
    }

    void stop() {
        stop_requested_.store(true, std::memory_order_release);
        try {
            io_.stop();
        } catch (...) {
        }
    }

    // Post a callback to run after a delay using a steady_timer (non-blocking)
        void post_after(std::chrono::milliseconds d, std::function<void()> cb) {
            try {
                boost::asio::post(io_, [this, d, cb = std::move(cb)]() mutable {
                    auto timer = std::make_shared<boost::asio::steady_timer>(io_);
                    timer->expires_after(d);
                    timer->async_wait([cb, timer](const boost::system::error_code& /*ec*/) mutable {
                        try {
                            if (cb)
                                cb();
                        } catch (...) {
                        }
                    });
                });
            } catch (...) {
                // Best-effort: if posting fails, run immediately to avoid hangs
                try {
                    if (cb)
                        cb();
                } catch (...) {
                }
            }
        }

        // Await writability on a file descriptor with an optional timeout using Asio.
        // Returns Timeout on expiry, NetworkError for other failures, or success.
        Task<yams::Result<void>> async_wait_writable(int fd, std::chrono::milliseconds timeout) {
    #ifndef _WIN32
            // Use a standalone descriptor to avoid interfering with registered maps
            auto desc = std::make_shared<boost::asio::posix::stream_descriptor>(io_, fd);
            std::shared_ptr<boost::asio::steady_timer> timer;
            if (timeout.count() > 0) {
                timer = std::make_shared<boost::asio::steady_timer>(io_);
                timer->expires_after(timeout);
                timer->expires_after(timeout);
            }

            struct Awaiter {
                std::shared_ptr<boost::asio::posix::stream_descriptor> d;
                std::shared_ptr<boost::asio::steady_timer> t;
                std::atomic<bool> resumed{false};
                bool timed_out = false;
                boost::system::error_code ec{};

                bool await_ready() const noexcept { return false; }
                void await_suspend(std::coroutine_handle<> h) {
                    d->async_wait(boost::asio::posix::stream_descriptor::wait_write,
                                  [this, h](const boost::system::error_code& e) mutable {
                                      ec = e;
                                      if (!resumed.exchange(true)) {
                                          if (t) {
                                              boost::system::error_code ignored;
                                              t->cancel(ignored);
                                          }
                                          try {
                                              if (h)
                                                  h.resume();
                                          } catch (...) {
                                          }
                                      }
                                  });
                    if (t) {
                        t->async_wait([this, h](const boost::system::error_code& e) mutable {
                            if (!resumed.exchange(true)) {
                                // If timer expired without error, mark timed_out
                                timed_out = !e;
                                boost::system::error_code ignored;
                                d->cancel(ignored);
                                try {
                                    if (h)
                                        h.resume();
                                } catch (...) {
                                }
                            }
                        });
                    }
                }
                void await_resume() const noexcept {}
            } aw{desc, timer};

            co_await aw;

            if (aw.timed_out) {
                co_return Error{ErrorCode::Timeout, "Operation timed out"};
            }
            if (aw.ec) {
                co_return Error{ErrorCode::NetworkError,
                                std::string("Wait failed: ") + aw.ec.message()};
            }
            co_return yams::Result<void>();
    #else
            (void)fd;
            (void)timeout;
            co_return Error{ErrorCode::InternalError, "Not supported on Windows"};
    #endif
        }

    private:
    void safe_resume_(const std::shared_ptr<yams::daemon::SocketAwaiterBase>& awaiter,
                      std::coroutine_handle<> h) {
        if (!awaiter)
            return;
        // Ensure single-resume
        bool already = awaiter->scheduled.exchange(true, std::memory_order_acq_rel);
        if (already)
            return;
        awaiter->alive.store(false, std::memory_order_release);
        auto keep_alive = awaiter; // keep shared state alive through resume
        try {
            // Avoid calling done() which may access freed coroutine state; resume defensively
            if (h)
                h.resume();
        } catch (...) {
        }
    }

    std::atomic<bool> stop_requested_{false};
    boost::asio::io_context io_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
    std::mutex mutex_;
    // Use shared_ptr to keep descriptors alive across in-flight async handlers safely.
    std::unordered_map<int, std::shared_ptr<boost::asio::posix::stream_descriptor>> descriptors_;
    std::unordered_map<int, uint64_t> socket_generations_;
};
} // namespace yams::daemon

// =============================================================================
// AsyncIOContext method definitions (forwarding to Impl)
// =============================================================================
namespace yams::daemon {

AsyncIOContext::AsyncIOContext() : pImpl(std::make_unique<Impl>()) {}

AsyncIOContext::~AsyncIOContext() = default;

AsyncIOContext::AsyncIOContext(AsyncIOContext&& other) noexcept = default;

AsyncIOContext& AsyncIOContext::operator=(AsyncIOContext&& other) noexcept = default;

yams::Result<void> AsyncIOContext::register_socket(int fd, uint64_t generation) {
    return pImpl->register_socket(fd, generation);
}

yams::Result<void> AsyncIOContext::unregister_socket(int fd) {
    return pImpl->unregister_socket(fd);
}

void AsyncIOContext::cancel_pending_operations(int fd) {
    pImpl->cancel_pending_operations(fd);
}

void AsyncIOContext::submit_read_erased(int fd, void* buffer, size_t size,
                                        std::coroutine_handle<> h,
                                        std::shared_ptr<SocketAwaiterBase> awaiter,
                                        uint64_t generation) {
    pImpl->submit_read_erased(fd, buffer, size, h, std::move(awaiter), generation);
}

void AsyncIOContext::submit_write_erased(int fd, const void* buffer, size_t size,
                                         std::coroutine_handle<> h,
                                         std::shared_ptr<SocketAwaiterBase> awaiter,
                                         uint64_t generation) {
    pImpl->submit_write_erased(fd, buffer, size, h, std::move(awaiter), generation);
}

void AsyncIOContext::post(std::function<void()> fn) {
    pImpl->post(std::move(fn));
}

void AsyncIOContext::run() {
    pImpl->run();
}

void AsyncIOContext::stop() {
    pImpl->stop();
}

Task<void> AsyncIOContext::sleep_for(std::chrono::milliseconds d) {
    struct Awaiter {
        AsyncIOContext::Impl* impl;
        std::chrono::milliseconds delay;
        bool await_ready() const noexcept { return delay.count() <= 0; }
        void await_suspend(std::coroutine_handle<> h) const {
            if (!impl) {
                // No impl; resume immediately
                try {
                    if (h)
                        h.resume();
                } catch (...) {
                }
                return;
            }
            impl->post_after(delay, [h]() mutable {
                try {
                    if (h)
                        h.resume();
                } catch (...) {
                }
            });
        }
        void await_resume() const noexcept {}
    };
    co_await Awaiter{pImpl.get(), d};
}

Task<Result<int>> AsyncIOContext::async_connect_unix(const std::filesystem::path& socketPath,
                                                     std::chrono::milliseconds timeout) {
#ifndef _WIN32
    // Create non-blocking Unix domain socket
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        co_return Error{ErrorCode::NetworkError, "Failed to create socket"};
    }
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    {
        int on = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
    }
#endif
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

    int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
        co_return fd; // Connected immediately
    }
    if (rc < 0 && errno != EINPROGRESS) {
        int err = errno;
        ::close(fd);
        co_return Error{ErrorCode::NetworkError,
                        std::string("Connect failed: ") + std::strerror(err)};
    }

    // Await writability using Impl helper to avoid accessing private io_ directly
    auto wait_res = co_await pImpl->async_wait_writable(fd, timeout);
    if (!wait_res) {
        ::close(fd);
        co_return wait_res.error();
    }

    // Check the connection status
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0) {
        so_error = errno;
    }

    if (so_error != 0) {
        ::close(fd);
        co_return Error{ErrorCode::NetworkError,
                        std::string("Connect failed: ") + std::strerror(so_error)};
    }

    co_return fd;
#else
    co_return Error{ErrorCode::InternalError, "Async unix connect not supported on Windows"};
#endif
}

} // namespace yams::daemon
