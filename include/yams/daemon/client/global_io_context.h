#pragma once

#include <thread>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>

namespace yams::daemon {

class GlobalIOContext {
public:
    static GlobalIOContext& instance();

    boost::asio::io_context& get_io_context();

private:
    GlobalIOContext();
    ~GlobalIOContext();

    boost::asio::io_context io_context_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
    std::thread io_thread_;
};

} // namespace yams::daemon