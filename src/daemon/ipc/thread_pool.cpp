#include <yams/daemon/ipc/thread_pool.h>

namespace yams::daemon {

ThreadPool::ThreadPool(size_t num_threads) : state_(std::make_shared<ThreadPoolState>()) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) {
            num_threads = 4; // Fallback to 4 threads
        }
    }

    workers_.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i) {
        // Capture shared state by value so threads have their own shared_ptr
        workers_.emplace_back([this, state = state_](yams::compat::stop_token token) {
            worker_thread(state, token);
        });
    }
}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::stop() {
    {
        std::unique_lock<std::mutex> lock(state_->queue_mutex);
        if (state_->stopping) {
            return; // Already stopping
        }
        state_->stopping = true;
    }

    state_->condition.notify_all();

    // Request stop on all threads (no-op on fallback jthread)
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.request_stop();
        }
    }

    // Explicitly join all threads to ensure they finish before we return
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    // Now safe to clear the vector
    workers_.clear();
}

size_t ThreadPool::queue_size() const {
    std::unique_lock<std::mutex> lock(state_->queue_mutex);
    return state_->tasks.size();
}

void ThreadPool::worker_thread(std::shared_ptr<ThreadPoolState> state,
                               yams::compat::stop_token token) {
    // Work with the shared state - safe even if ThreadPool object is destroyed
    while (!token.stop_requested()) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(state->queue_mutex);

            // Wait for work or stop signal
            state->condition.wait(lock, [&state, &token] {
                return state->stopping || !state->tasks.empty() || token.stop_requested();
            });

            // Exit if stopping and no more tasks
            if ((state->stopping || token.stop_requested()) && state->tasks.empty()) {
                return;
            }

            // Get next task if available
            if (!state->tasks.empty()) {
                task = std::move(state->tasks.front());
                state->tasks.pop();
            }
        }

        // Execute task outside of lock
        if (task) {
            try {
                task();
            } catch (const std::exception& e) {
                // Log but don't crash on task exception
                // Note: Can't use spdlog here as it might not be thread-safe in all contexts
                // Errors in tasks should be handled by the task itself
            } catch (...) {
                // Catch all to prevent thread termination
            }
        }
    }
}

} // namespace yams::daemon
