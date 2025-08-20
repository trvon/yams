#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <vector>

namespace yams::daemon {

/**
 * Thread pool for executing tasks asynchronously.
 * Supports both traditional std::future and C++20 coroutines.
 *
 * Thread-safe and follows RAII principles.
 */
class ThreadPool {
public:
    /**
     * Create a thread pool with the specified number of threads.
     * @param num_threads Number of worker threads (defaults to hardware concurrency)
     */
    explicit ThreadPool(size_t num_threads = std::thread::hardware_concurrency());

    /**
     * Destructor - stops all threads and waits for them to finish.
     */
    ~ThreadPool();

    // Non-copyable, non-movable (singleton per server)
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    /**
     * Submit a task to the thread pool and get a future for the result.
     * @param f Function to execute
     * @param args Arguments to pass to the function
     * @return Future that will contain the result
     */
    template <typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args) -> std::future<typename std::invoke_result_t<F, Args...>>;

    /**
     * Submit a task without expecting a result (fire-and-forget).
     * @param f Function to execute
     */
    template <typename F> void enqueue_detached(F&& f);

    /**
     * Stop the thread pool. No new tasks will be accepted.
     * Existing tasks will complete.
     */
    void stop();

    /**
     * Get the number of pending tasks in the queue.
     * @return Number of tasks waiting to be executed
     */
    size_t queue_size() const;

    /**
     * Check if the thread pool is stopping.
     * @return true if stop() has been called
     */
    bool is_stopping() const { return stopping_.load(); }

private:
    void worker_thread();

    std::vector<std::jthread> workers_;
    std::queue<std::function<void()>> tasks_;
    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> stopping_{false};
};

// ThreadPoolAwaiter removed - using promise/future approach for better reliability
// The coroutine awaiter had complex lifetime issues with std::variant moves
// Promise/future provides cleaner thread synchronization

// Template implementations

template <typename F, typename... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::invoke_result_t<F, Args...>> {
    using return_type = typename std::invoke_result_t<F, Args...>;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        [f = std::forward<F>(f), ... args = std::forward<Args>(args)]() mutable {
            return std::invoke(std::move(f), std::move(args)...);
        });

    std::future<return_type> res = task->get_future();

    {
        std::unique_lock<std::mutex> lock(queue_mutex_);

        if (stopping_) {
            throw std::runtime_error("Cannot enqueue task: thread pool is stopping");
        }

        tasks_.emplace([task]() { (*task)(); });
    }

    condition_.notify_one();
    return res;
}

template <typename F> void ThreadPool::enqueue_detached(F&& f) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);

        if (stopping_) {
            return; // Silently drop task if stopping
        }

        tasks_.emplace(std::forward<F>(f));
    }

    condition_.notify_one();
}

} // namespace yams::daemon