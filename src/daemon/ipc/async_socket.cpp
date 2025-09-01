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

#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>

namespace yams::daemon {

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

    Result<void> register_socket(int fd, uint64_t generation) {
        std::lock_guard<std::mutex> lock(mutex_);
        try {
            auto desc = std::make_shared<boost::asio::posix::stream_descriptor>(io_, fd);
            descriptors_[fd] = desc;
            socket_generations_[fd] = generation;
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::NetworkError,
                         std::string{"AsyncIOContext(Asio): register_socket failed: "} + e.what()};
        }
    }

    Result<void> unregister_socket(int fd) {
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
        return Result<void>();
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

Result<void> AsyncIOContext::register_socket(int fd, uint64_t generation) {
    return pImpl->register_socket(fd, generation);
}

Result<void> AsyncIOContext::unregister_socket(int fd) {
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

} // namespace yams::daemon