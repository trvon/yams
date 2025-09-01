#include <yams/daemon/client/global_io_context.h>

namespace yams::daemon {

GlobalIOContext& GlobalIOContext::instance() {
    static GlobalIOContext instance;
    return instance;
}

AsyncIOContext& GlobalIOContext::get_io_context() {
    return io_context_;
}

GlobalIOContext::GlobalIOContext() {
    io_thread_ = std::thread([this]() {
        io_context_.run();
    });
}

GlobalIOContext::~GlobalIOContext() {
    io_context_.stop();
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
}

} // namespace yams::daemon
