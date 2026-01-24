#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <spdlog/spdlog.h>

#include <yams/daemon/client/asio_connection.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/TuneAdvisor.h>

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

std::atomic<int> g_nifty_counter{0};
alignas(yams::daemon::GlobalIOContext) char g_global_io_context_storage[sizeof(
    yams::daemon::GlobalIOContext)];
yams::daemon::GlobalIOContext* g_global_io_context_ptr = nullptr;

} // namespace

namespace yams {
namespace daemon {

using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

GlobalIOContextInitializer::GlobalIOContextInitializer() {
    if (g_nifty_counter.fetch_add(1, std::memory_order_acq_rel) == 0) {
        g_global_io_context_ptr = new (g_global_io_context_storage) GlobalIOContext();
    }
}

GlobalIOContextInitializer::~GlobalIOContextInitializer() {
    if (g_nifty_counter.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (g_global_io_context_ptr) {
            g_global_io_context_ptr->~GlobalIOContext();
            g_global_io_context_ptr = nullptr;
        }
    }
}

GlobalIOContext& GlobalIOContext::instance() {
    return *g_global_io_context_ptr;
}

void GlobalIOContext::reset() {
    if (is_destroyed()) {
        return;
    }

    static constexpr std::array<const char*, 2> kSkipKeys = {
        "YAMS_TESTING",
        "YAMS_TEST_SAFE_SINGLE_INSTANCE",
    };

    for (const char* key : kSkipKeys) {
        if (env_truthy(std::getenv(key))) {
            return;
        }
    }

    // Close all connections and clear pools
    // Don't restart io_context - strands hold executor references that would become invalid
    ConnectionRegistry::instance().closeAll();
    AsioConnectionPool::shutdown_all();
}

void GlobalIOContext::restart() {
    if (destroyed_.load(std::memory_order_acquire)) {
        return;
    }

    std::lock_guard<std::mutex> lock(this->restart_mutex_);

    if (destroyed_.load(std::memory_order_acquire)) {
        return;
    }

    if (this->work_guard_) {
        this->work_guard_->reset();
        this->work_guard_.reset();
    }

    constexpr auto drain_timeout = std::chrono::milliseconds(2000);
    auto drain_deadline = std::chrono::steady_clock::now() + drain_timeout;

    while (std::chrono::steady_clock::now() < drain_deadline) {
        if (io_context_->stopped()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!io_context_->stopped()) {
        io_context_->stop();
    }

    for (auto& t : this->io_threads_) {
        if (t.joinable()) {
            try {
                t.join();
            } catch (const std::exception& e) {
                try {
                    spdlog::warn("[GlobalIOContext] restart join exception: {}", e.what());
                } catch (...) {
                }
            } catch (...) {
                try {
                    spdlog::warn("[GlobalIOContext] restart join unknown exception");
                } catch (...) {
                }
            }
        }
    }
    this->io_threads_.clear();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    io_context_->restart();
    this->work_guard_ = std::make_unique<WorkGuard>(io_context_->get_executor());

    unsigned int thread_count = static_cast<unsigned int>(TuneAdvisor::recommendedThreads());
    thread_count = std::max(thread_count, 2u);
    thread_count = std::min(thread_count, 16u);

    this->io_threads_.reserve(thread_count);
    std::vector<std::thread> new_threads;
    new_threads.reserve(thread_count);

    try {
        for (unsigned int i = 0; i < thread_count; ++i) {
            new_threads.emplace_back([this]() {
                try {
                    io_context_->run();
                } catch (const std::exception& e) {
                    try {
                        spdlog::error("GlobalIOContext worker exception: {}", e.what());
                    } catch (...) {
                    }
                } catch (...) {
                    try {
                        spdlog::error("GlobalIOContext worker unknown exception");
                    } catch (...) {
                    }
                }
            });
        }
        this->io_threads_ = std::move(new_threads);

        // Give threads time to start running io_context_->run() before returning
        // This ensures the executor is fully functional for subsequent operations
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    } catch (...) {
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
        io_context_ = std::make_unique<boost::asio::io_context>();
        work_guard_ = std::make_unique<WorkGuard>(io_context_->get_executor());

        unsigned int thread_count = static_cast<unsigned int>(TuneAdvisor::recommendedThreads());
        thread_count = std::max(thread_count, 2u);
        thread_count = std::min(thread_count, 16u);

        io_threads_.reserve(thread_count);
        try {
            for (unsigned int i = 0; i < thread_count; ++i) {
                io_threads_.emplace_back([this]() {
                    try {
                        io_context_->run();
                    } catch (const std::exception& e) {
                        try {
                            spdlog::error("GlobalIOContext worker exception: {}", e.what());
                        } catch (...) {
                        }
                    } catch (...) {
                        try {
                            spdlog::error("GlobalIOContext worker unknown exception");
                        } catch (...) {
                        }
                    }
                });
            }
        } catch (...) {
            io_context_->stop();
            for (auto& worker : io_threads_) {
                if (worker.joinable()) {
                    try {
                        worker.join();
                    } catch (...) {
                    }
                }
            }
            if (work_guard_) {
                work_guard_->reset();
                work_guard_.reset();
            }
            io_threads_.clear();
            throw;
        }
    });
}

GlobalIOContext::GlobalIOContext() {}

bool GlobalIOContext::is_destroyed() noexcept {
    if (!g_global_io_context_ptr) {
        return true;
    }
    return g_global_io_context_ptr->destroyed_.load(std::memory_order_acquire);
}

GlobalIOContext::~GlobalIOContext() noexcept {
    // Mark as destroyed FIRST to prevent restart() from trying to lock mutex
    destroyed_.store(true, std::memory_order_release);

    // IMPORTANT: Close connections BEFORE stopping io_context
    // Socket destructors need the reactor to properly deregister
    try {
        ConnectionRegistry::instance().closeAll();
    } catch (...) {
    }

    try {
        AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
    } catch (...) {
    }

    // Now safe to stop the io_context
    try {
        if (this->work_guard_) {
            this->work_guard_->reset();
            this->work_guard_.reset();
        }
    } catch (...) {
    }

    try {
        if (io_context_) {
            io_context_->stop();
        }
    } catch (...) {
    }

    for (auto& t : this->io_threads_) {
        if (t.joinable()) {
            try {
                t.join();
            } catch (...) {
            }
        }
    }
}

} // namespace daemon
} // namespace yams
