#pragma once

#include <memory>
#include <vector>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/compat/thread_stop_compat.h>

namespace yams::daemon {

// Lightweight IO-based worker pool backed by compat::jthread threads.
class WorkerPool {
public:
    explicit WorkerPool(std::size_t threads = 1);
    ~WorkerPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    boost::asio::any_io_executor executor() const { return io_.get_executor(); }

    void stop();

    // Phase 6: prototype dynamic resize (grow/shrink with cooldown handled by caller)
    // Returns true if target differs from current and action taken.
    bool resize(std::size_t target) noexcept;
    std::size_t threads() const noexcept { return active_.load(std::memory_order_relaxed); }

private:
    void run_thread(yams::compat::stop_token st);

    mutable boost::asio::io_context io_;
    using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    std::unique_ptr<WorkGuard> guard_;
    std::vector<yams::compat::jthread> threads_;
    std::atomic<std::size_t> desired_{0};
    std::atomic<std::size_t> active_{0};
};

} // namespace yams::daemon
