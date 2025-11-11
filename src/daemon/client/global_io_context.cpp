#include <boost/asio/executor_work_guard.hpp>
#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <yams/daemon/client/global_io_context.h>

namespace {
bool env_truthy(const char* value) {
    if (!value)
        return false;

    std::string normalized(value);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });

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

    instance().restart();
}

void GlobalIOContext::restart() {
    std::lock_guard<std::mutex> lock(this->restart_mutex_);

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
    this->io_threads_.clear();

    io_context_.restart();
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
        this->io_context_.stop();
        for (auto& worker : this->io_threads_) {
            if (worker.joinable()) {
                try { worker.join(); } catch (...) {}
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
                try { worker.join(); } catch (...) {}
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
