#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <boost/asio/executor_work_guard.hpp>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>

namespace {
bool env_truthy(const char* value) {
    if (!value)
        return false;

    std::string normalized(value);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    return !(normalized.empty() || normalized == "0" || normalized == "false" ||
             normalized == "off" || normalized == "no");
}
} // namespace

namespace yams {
namespace daemon {

using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

GlobalIOContext& GlobalIOContext::instance() {
    static GlobalIOContext* instance = new GlobalIOContext();
    return *instance;
}

void GlobalIOContext::reset() {
    static constexpr std::array<const char*, 2> kSkipKeys = {
        "YAMS_TESTING",
        "YAMS_TEST_SAFE_SINGLE_INSTANCE",
    };

    for (const char* key : kSkipKeys) {
        if (env_truthy(std::getenv(key))) {
            return;
        }
    }

    AsioConnectionPool::shutdown_all();
    instance().restart();
}

void GlobalIOContext::restart() {
    std::lock_guard<std::mutex> lock(this->restart_mutex_);

    // Phase 1: Signal stop and release work guard
    if (this->work_guard_) {
        this->work_guard_->reset();
        this->work_guard_.reset();
    }

    // Phase 2: Stop io_context (will cause run() to return in worker threads)
    io_context_.stop();

    // Phase 3: Join all worker threads with timeout protection
    for (auto& t : this->io_threads_) {
        if (t.joinable()) {
            try {
                t.join();
            } catch (const std::exception&) {
                // Thread join failed - continue with cleanup
            } catch (...) {
                // Unknown exception during join
            }
        }
    }
    this->io_threads_.clear();

    // Phase 4: Allow brief settling time for any lingering async operations
    // This is critical on macOS to prevent resource corruption across restart cycles
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Phase 5: Restart io_context and recreate work guard
    io_context_.restart();
    this->work_guard_ = std::make_unique<WorkGuard>(io_context_.get_executor());

    // Phase 6: Calculate thread count
    unsigned int thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0)
        thread_count = 4;
    thread_count = std::min(thread_count, 16u);

    // Phase 7: Create new worker threads with proper error handling
    this->io_threads_.reserve(thread_count);
    std::vector<std::thread> new_threads;
    new_threads.reserve(thread_count);

    try {
        for (unsigned int i = 0; i < thread_count; ++i) {
            new_threads.emplace_back([this]() { io_context_.run(); });
        }
        // All threads created successfully - move them to member variable
        this->io_threads_ = std::move(new_threads);
    } catch (...) {
        // Thread creation failed - clean up partial state
        this->io_context_.stop();
        for (auto& worker : new_threads) {
            if (worker.joinable()) {
                try {
                    worker.join();
                } catch (...) {
                }
            }
        }
        if (this->work_guard_) {
            this->work_guard_->reset();
            this->work_guard_.reset();
        }
        this->io_threads_.clear();
        throw;
    }
}

boost::asio::io_context& GlobalIOContext::get_io_context() {
    return io_context_;
}

GlobalIOContext::GlobalIOContext() {
    this->work_guard_ = std::make_unique<WorkGuard>(io_context_.get_executor());

    unsigned int thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0)
        thread_count = 4;
    thread_count = std::min(thread_count, 16u);

    this->io_threads_.reserve(thread_count);
    try {
        for (unsigned int i = 0; i < thread_count; ++i) {
            this->io_threads_.emplace_back([this]() { io_context_.run(); });
        }
    } catch (...) {
        // Stop io_context first to wake any waiting threads
        this->io_context_.stop();
        // Join all successfully created threads before destroying work_guard
        for (auto& worker : this->io_threads_) {
            if (worker.joinable()) {
                try {
                    worker.join();
                } catch (...) {
                }
            }
        }
        // NOW it's safe to destroy work_guard (no threads are using it)
        if (this->work_guard_) {
            this->work_guard_->reset();
            this->work_guard_.reset();
        }
        this->io_threads_.clear();
        throw;
    }
}

GlobalIOContext::~GlobalIOContext() {
    if (this->work_guard_) {
        this->work_guard_->reset();
        this->work_guard_.reset();
    }

    io_context_.stop();
    for (auto& t : this->io_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

} // namespace daemon
} // namespace yams
