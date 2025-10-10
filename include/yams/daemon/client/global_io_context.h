#pragma once

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

    boost::asio::io_context io_context_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
    std::vector<std::thread> io_threads_;
};

} // namespace yams::daemon
