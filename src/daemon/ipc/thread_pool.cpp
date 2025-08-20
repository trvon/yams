#include <yams/daemon/ipc/thread_pool.h>

namespace yams::daemon {

ThreadPool::ThreadPool(size_t num_threads) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) {
            num_threads = 4; // Fallback to 4 threads
        }
    }

    workers_.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this] { worker_thread(); });
    }
}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::stop() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (stopping_) {
            return; // Already stopping
        }
        stopping_ = true;
    }

    condition_.notify_all();

    // jthread automatically joins in destructor, but we can request stop
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.request_stop();
        }
    }

    // Wait for all threads to finish
    workers_.clear();
}

size_t ThreadPool::queue_size() const {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    return tasks_.size();
}

void ThreadPool::worker_thread() {
    while (true) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            condition_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });

            if (stopping_ && tasks_.empty()) {
                return;
            }

            if (!tasks_.empty()) {
                task = std::move(tasks_.front());
                tasks_.pop();
            }
        }

        if (task) {
            task();
        }
    }
}

} // namespace yams::daemon