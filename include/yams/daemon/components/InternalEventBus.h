#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace yams::daemon {

// Simple lock-free SPSC ring buffer for intra-process events.
// Single-producer, single-consumer; capacity must be > 0 (not required to be power-of-two).
template <typename T> class SpscQueue {
public:
    explicit SpscQueue(std::size_t capacity)
        : buf_(capacity ? capacity : 1), cap_(capacity ? capacity : 1), head_(0), tail_(0) {}

    bool try_push(const T& v) noexcept {
        auto head = head_.load(std::memory_order_relaxed);
        auto next = inc(head);
        if (next == tail_.load(std::memory_order_acquire))
            return false; // full
        buf_[head] = v;
        head_.store(next, std::memory_order_release);
        return true;
    }
    bool try_push(T&& v) noexcept {
        auto head = head_.load(std::memory_order_relaxed);
        auto next = inc(head);
        if (next == tail_.load(std::memory_order_acquire))
            return false;
        buf_[head] = std::move(v);
        head_.store(next, std::memory_order_release);
        return true;
    }
    bool try_pop(T& out) noexcept {
        auto tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire))
            return false; // empty
        out = std::move(buf_[tail]);
        tail_.store(inc(tail), std::memory_order_release);
        return true;
    }
    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }
    bool full() const noexcept {
        return inc(head_.load(std::memory_order_acquire)) == tail_.load(std::memory_order_acquire);
    }
    std::size_t capacity() const noexcept { return cap_; }

private:
    std::size_t inc(std::size_t i) const noexcept { return (++i == cap_) ? 0 : i; }
    std::vector<T> buf_;
    const std::size_t cap_;
    std::atomic<std::size_t> head_;
    std::atomic<std::size_t> tail_;
};

// Minimal scaffolding for a typed internal event bus; channels will be registered ad-hoc by owners.
class InternalEventBus {
public:
    static InternalEventBus& instance() {
        static InternalEventBus b;
        return b;
    }

    template <typename T>
    std::shared_ptr<SpscQueue<T>> make_channel(const std::string& /*name*/, std::size_t capacity) {
        // For now, do not keep a global registry; return the channel to the owner.
        return std::make_shared<SpscQueue<T>>(capacity);
    }

private:
    InternalEventBus() = default;
};

} // namespace yams::daemon
