#include <boost/asio/executor_work_guard.hpp>
#include <yams/daemon/client/global_io_context.h>

namespace yams::daemon {

GlobalIOContext& GlobalIOContext::instance() {
    static GlobalIOContext* instance = new GlobalIOContext();
    return *instance;
}

void GlobalIOContext::reset() {
    instance().restart();
}

void GlobalIOContext::restart() {
    std::lock_guard<std::mutex> lock(restart_mutex_);

    // Stop existing io_context and join threads
    try {
        work_guard_.reset();
        io_context_.stop();
        for (auto& t : io_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
        io_threads_.clear();
    } catch (...) {
    }

    // Restart io_context (resets stopped state)
    io_context_.restart();
    work_guard_.emplace(boost::asio::make_work_guard(io_context_));

    // Spawn new worker threads
    unsigned int thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0)
        thread_count = 4;
    thread_count = std::min(thread_count, 16u);

    for (unsigned int i = 0; i < thread_count; ++i) {
        io_threads_.emplace_back([this]() { io_context_.run(); });
    }
}

boost::asio::io_context& GlobalIOContext::get_io_context() {
    return io_context_;
}

GlobalIOContext::GlobalIOContext() {
    work_guard_.emplace(boost::asio::make_work_guard(io_context_));

    unsigned int thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0)
        thread_count = 4;
    thread_count = std::min(thread_count, 16u);

    for (unsigned int i = 0; i < thread_count; ++i) {
        io_threads_.emplace_back([this]() { io_context_.run(); });
    }
}

GlobalIOContext::~GlobalIOContext() {
    try {
        work_guard_.reset();
    } catch (...) {
    }
    try {
        io_context_.stop();
    } catch (...) {
    }
    try {
        for (auto& t : io_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    } catch (...) {
    }
}

} // namespace yams::daemon
