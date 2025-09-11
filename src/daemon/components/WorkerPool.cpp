#include <yams/daemon/components/WorkerPool.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <boost/asio/executor_work_guard.hpp>

namespace yams::daemon {

WorkerPool::WorkerPool(std::size_t threads) : io_(static_cast<int>(threads)) {
    if (threads == 0)
        threads = 1;
    guard_ = std::make_unique<WorkGuard>(boost::asio::make_work_guard(io_));
    threads_.reserve(threads);
    for (std::size_t i = 0; i < threads; ++i) {
        threads_.emplace_back([this](yams::compat::stop_token st) { run_thread(st); });
        active_.fetch_add(1, std::memory_order_relaxed);
    }
    desired_.store(threads, std::memory_order_relaxed);
    spdlog::info("WorkerPool started with {} threads", threads_.size());
}

WorkerPool::~WorkerPool() {
    stop();
}

void WorkerPool::stop() {
    try {
        if (guard_) {
            guard_->reset();
        }
        io_.stop();
        for (auto& t : threads_) {
            if (t.joinable())
                t.request_stop();
        }
        for (auto& t : threads_) {
            if (t.joinable())
                t.join();
        }
        threads_.clear();
        desired_.store(0, std::memory_order_relaxed);
        active_.store(0, std::memory_order_relaxed);
    } catch (...) {
    }
}

void WorkerPool::run_thread(yams::compat::stop_token st) {
    using namespace std::chrono_literals;
    try {
        for (;;) {
            if (st.stop_requested())
                break;
            auto target = desired_.load(std::memory_order_relaxed);
            auto act = active_.load(std::memory_order_relaxed);
            if (act > target && target > 0) {
                break; // shrink: allow this thread to exit
            }
#if defined(BOOST_ASIO_HAS_CO_AWAIT) || 1
            // Attempt timed run to periodically check exit conditions.
            // Use a longer wait to minimize idle CPU wakeups.
            io_.run_for(250ms);
#else
            io_.poll_one();
            std::this_thread::sleep_for(250ms);
#endif
        }
    } catch (const std::exception& e) {
        spdlog::warn("WorkerPool thread exited: {}", e.what());
    }
    active_.fetch_sub(1, std::memory_order_relaxed);
}

bool WorkerPool::resize(std::size_t target) noexcept {
    if (target == 0)
        target = 1;
    auto current = active_.load(std::memory_order_relaxed);
    if (target == current)
        return false;
    if (target > current) {
        // Grow: spawn additional threads
        std::size_t to_add = target - current;
        try {
            threads_.reserve(threads_.size() + to_add);
            for (std::size_t i = 0; i < to_add; ++i) {
                threads_.emplace_back([this](yams::compat::stop_token st) { run_thread(st); });
                active_.fetch_add(1, std::memory_order_relaxed);
            }
            desired_.store(target, std::memory_order_relaxed);
            spdlog::info("WorkerPool resized up to {} threads", target);
            return true;
        } catch (...) {
            return false;
        }
    } else {
        // Shrink: set desired_ lower; threads will exit after run_for cycles
        desired_.store(target, std::memory_order_relaxed);
        spdlog::info("WorkerPool resize requested down to {} threads", target);
        return true;
    }
}

} // namespace yams::daemon
