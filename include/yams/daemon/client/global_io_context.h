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

private:
    GlobalIOContext();
    ~GlobalIOContext();

    std::unique_ptr<boost::asio::io_context> io_context_;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        work_guard_;
    std::vector<std::thread> io_threads_;
    std::mutex restart_mutex_;
    std::once_flag init_flag_;  // For lazy initialization
    
    void ensure_initialized();  // Lazy init helper

    void restart();

public:
    static void reset();
};

} // namespace yams::daemon
