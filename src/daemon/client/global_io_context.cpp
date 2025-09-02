#include <yams/daemon/client/global_io_context.h>
#include <boost/asio/executor_work_guard.hpp>

namespace yams::daemon {

GlobalIOContext& GlobalIOContext::instance() {
    static GlobalIOContext instance;
    return instance;
}

boost::asio::io_context& GlobalIOContext::get_io_context() {
    return io_context_;
}

GlobalIOContext::GlobalIOContext() 
    : work_guard_(boost::asio::make_work_guard(io_context_)) {
    io_thread_ = std::thread([this]() { 
        io_context_.run(); 
    });
}

GlobalIOContext::~GlobalIOContext() {
    work_guard_.reset();
    io_context_.stop();
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
}

} // namespace yams::daemon