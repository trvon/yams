#include <boost/asio/executor_work_guard.hpp>
#include <yams/daemon/client/global_io_context.h>

namespace yams::daemon {

GlobalIOContext& GlobalIOContext::instance() {
    // Intentionally allocate on the heap and never destroy to avoid
    // destructor-order races at process shutdown (tests may end while
    // other singletons still dispatch work). This trades a tiny leak
    // at exit for significantly more robust teardown.
    static GlobalIOContext* instance = new GlobalIOContext();
    return *instance;
}

boost::asio::io_context& GlobalIOContext::get_io_context() {
    return io_context_;
}

GlobalIOContext::GlobalIOContext() : work_guard_(boost::asio::make_work_guard(io_context_)) {
    // Use multiple threads for concurrent I/O operations (connection establishment, reads/writes)
    // This is critical for handling multiple concurrent clients without serialization
    unsigned int thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0)
        thread_count = 4; // Fallback if detection fails

    // Cap at reasonable maximum to avoid thread explosion
    thread_count = std::min(thread_count, 16u);

    for (unsigned int i = 0; i < thread_count; ++i) {
        io_threads_.emplace_back([this]() { io_context_.run(); });
    }
}

GlobalIOContext::~GlobalIOContext() {
    // Best-effort shutdown when explicitly destroyed (not expected in
    // normal runs due to the intentional leak above).
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
