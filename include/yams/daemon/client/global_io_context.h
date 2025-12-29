#pragma once

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

    void ensure_initialized();
    void restart();
};

class GlobalIOContextInitializer {
public:
    GlobalIOContextInitializer();
    ~GlobalIOContextInitializer();
};

static GlobalIOContextInitializer g_global_io_context_initializer;

} // namespace yams::daemon
