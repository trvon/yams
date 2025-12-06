#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <spdlog/spdlog.h>

#include <yams/daemon/client/asio_connection.h>
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
    // Meyer's singleton: Thread-safe in C++11+ (guaranteed by standard §6.7 [stmt.dcl] p4)
    // Constructed on first call, avoiding static initialization order fiasco
    static GlobalIOContext instance;
    return instance;
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
    io_context_->stop();

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
    io_context_->restart();
    this->work_guard_ = std::make_unique<WorkGuard>(io_context_->get_executor());

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
            new_threads.emplace_back([this]() {
                try {
                    io_context_->run();
                } catch (const std::exception& e) {
                    spdlog::error("GlobalIOContext worker exited with exception: {}", e.what());
                } catch (...) {
                    spdlog::error("GlobalIOContext worker exited with unknown exception");
                }
            });
        }
        // All threads created successfully - move them to member variable
        this->io_threads_ = std::move(new_threads);
    } catch (...) {
        // Thread creation failed - clean up partial state
        this->io_context_->stop();
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
    ensure_initialized();
    return *io_context_;
}

void GlobalIOContext::ensure_initialized() {
    std::call_once(init_flag_, [this]() {
        // Create io_context
        io_context_ = std::make_unique<boost::asio::io_context>();
        work_guard_ = std::make_unique<WorkGuard>(io_context_->get_executor());

        // Calculate thread count
        unsigned int thread_count = std::thread::hardware_concurrency();
        if (thread_count == 0)
            thread_count = 4;
        thread_count = std::min(thread_count, 16u);

        // Create worker threads
        io_threads_.reserve(thread_count);
        try {
            for (unsigned int i = 0; i < thread_count; ++i) {
                io_threads_.emplace_back([this]() {
                    try {
                        io_context_->run();
                    } catch (const std::exception& e) {
                        spdlog::error("GlobalIOContext worker exited with exception: {}", e.what());
                    } catch (...) {
                        spdlog::error("GlobalIOContext worker exited with unknown exception");
                    }
                });
            }
        } catch (...) {
            // Stop io_context first to wake any waiting threads
            io_context_->stop();
            // Join all successfully created threads
            for (auto& worker : io_threads_) {
                if (worker.joinable()) {
                    try {
                        worker.join();
                    } catch (...) {
                    }
                }
            }
            // Clean up
            if (work_guard_) {
                work_guard_->reset();
                work_guard_.reset();
            }
            io_threads_.clear();
            throw;
        }
    });
}

GlobalIOContext::GlobalIOContext() {
    // Trivial constructor - actual initialization deferred to ensure_initialized()
    // This avoids Windows static initialization order fiasco with boost::asio::io_context
}

GlobalIOContext::~GlobalIOContext() {
    ConnectionRegistry::instance().closeAll();
    AsioConnectionPool::shutdown_all(std::chrono::milliseconds(1000));

    if (this->work_guard_) {
        this->work_guard_->reset();
        this->work_guard_.reset();
    }

    io_context_->stop();

    for (auto& t : this->io_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

} // namespace daemon
} // namespace yams
