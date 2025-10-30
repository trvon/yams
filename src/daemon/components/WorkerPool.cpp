#include <yams/daemon/components/WorkerPool.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <future>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif
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
    spdlog::info("[WorkerPool] stop() called");
    try {
        // Order is critical: first reset guard, then stop io_context
        if (guard_) {
            spdlog::debug("[WorkerPool] Resetting work guard");
            guard_->reset();
            guard_.reset(); // Also destroy the guard
        }

        // Stop the io_context - this makes io_.run() return in all threads
        if (!io_.stopped()) {
            spdlog::debug("[WorkerPool] Stopping io_context");
            io_.stop();
        }

        // Request all threads to stop
        spdlog::debug("[WorkerPool] Requesting {} threads to stop", threads_.size());
        for (auto& t : threads_) {
            if (t.joinable())
                t.request_stop();
        }

        // Join all threads - they should exit quickly now since run_thread checks io_.stopped()
        spdlog::debug("[WorkerPool] Joining {} threads", threads_.size());
        for (size_t i = 0; i < threads_.size(); ++i) {
            auto& t = threads_[i];
            if (t.joinable()) {
                try {
                    t.join();
                    spdlog::debug("[WorkerPool] Thread {} joined", i);
                } catch (const std::system_error& e) {
                    spdlog::warn("WorkerPool::stop() thread {} join failed: {}", i, e.what());
                }
            }
        }

        spdlog::info("[WorkerPool] All threads joined, clearing");
        threads_.clear();
        desired_.store(0, std::memory_order_relaxed);
        active_.store(0, std::memory_order_relaxed);
        spdlog::info("[WorkerPool] stop() complete");
    } catch (...) {
        spdlog::error("[WorkerPool] stop() exception");
    }
}

void WorkerPool::run_thread(yams::compat::stop_token st) {
    using namespace std::chrono_literals;
    try {
#if defined(TRACY_ENABLE)
        ZoneScopedN("WorkerPool::run_thread");
#endif
        while (!st.stop_requested() && !io_.stopped()) {
            auto target = desired_.load(std::memory_order_relaxed);
            auto act = active_.load(std::memory_order_relaxed);
            if (act > target && target > 0) {
                break; // shrink: allow this thread to exit
            }

            // Process any ready handlers without blocking
            std::size_t count = io_.poll();

            // If no work was done, sleep briefly to avoid busy-wait
            if (count == 0) {
                std::this_thread::sleep_for(10ms);
            }
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
