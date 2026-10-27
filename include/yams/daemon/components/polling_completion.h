#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <utility>

namespace yams::daemon::detail {

// Owns the polling completion handshake, including its final notification. The observer
// overload lets a focused test hold that publication boundary without a live daemon.
class PollingCompletion {
public:
    void markStarted() {
        std::lock_guard lock(mutex_);
        stopped_ = false;
    }

    [[nodiscard]] bool isStopped() const {
        std::lock_guard lock(mutex_);
        return stopped_;
    }

    [[nodiscard]] std::optional<bool> tryStopped() const {
        std::unique_lock lock(mutex_, std::try_to_lock);
        if (!lock.owns_lock()) {
            return std::nullopt;
        }
        return stopped_;
    }

    void wait() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return stopped_; });
    }

    void complete() {
        complete([] {});
    }

    template <typename Observer> void complete(Observer&& beforeNotify) {
        // Observing completion must also observe the end of the notifier's access to cv_.
        // Otherwise an owner can return from stop and destroy cv_ during notify_one().
        std::lock_guard lock(mutex_);
        stopped_ = true;
        std::forward<Observer>(beforeNotify)();
        cv_.notify_one();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped_{true};
};

} // namespace yams::daemon::detail
