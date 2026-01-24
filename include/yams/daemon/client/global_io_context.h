#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace yams::daemon {

class GlobalIOContext {
public:
    static GlobalIOContext& instance();

    boost::asio::io_context& get_io_context();

    static boost::asio::any_io_executor global_executor() {
        return instance().get_io_context().get_executor();
    }

    static void reset();

    /// Fully restart the io_context threads.
    /// Use this after stopping a daemon to ensure the io_context is ready for new connections.
    /// Unlike reset() which only closes connections, restart() stops and recreates all threads.
    void restart();

    /// Safe version of restart() for use during teardown.
    /// Returns false if singleton is destroyed, true if restart was attempted.
    static bool safe_restart() noexcept;

    /// Returns true if the singleton is being/has been destroyed (static destruction in progress)
    static bool is_destroyed() noexcept;

private:
    friend class GlobalIOContextInitializer;
    GlobalIOContext();
    ~GlobalIOContext() noexcept;

    std::unique_ptr<boost::asio::io_context> io_context_;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;
    std::vector<std::thread> io_threads_;
    std::mutex restart_mutex_;
    std::once_flag init_flag_;
    std::atomic<bool> destroyed_{false};

    void ensure_initialized();
};

class GlobalIOContextInitializer {
public:
    GlobalIOContextInitializer();
    ~GlobalIOContextInitializer();
};

static GlobalIOContextInitializer g_global_io_context_initializer;

} // namespace yams::daemon
