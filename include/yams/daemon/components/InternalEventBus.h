#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/integrity/repair_manager.h>

namespace yams::daemon {

// Simple ring buffer for intra-process events.
// Compile-time selectable mode:
//   - YAMS_INTERNAL_BUS_MPMC=1 (default): MPMC-safe using a small mutex
//   - YAMS_INTERNAL_BUS_MPMC=0: original lock-free SPSC (single producer/consumer only)
#ifndef YAMS_INTERNAL_BUS_MPMC
#define YAMS_INTERNAL_BUS_MPMC 1
#endif

// Capacity must be > 0 (not required to be power-of-two).
template <typename T> class SpscQueue {
public:
    explicit SpscQueue(std::size_t capacity)
        : buf_(capacity ? capacity : 1), cap_(capacity ? capacity : 1), head_(0), tail_(0) {}

#if YAMS_INTERNAL_BUS_MPMC
    // MPMC-safe variant guarded by a lightweight mutex.
    bool try_push(const T& v) noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        auto head = head_.load(std::memory_order_relaxed);
        auto next = inc(head);
        if (next == tail_.load(std::memory_order_acquire))
            return false; // full
        buf_[head] = v;
        head_.store(next, std::memory_order_release);
        return true;
    }
    bool try_push(T&& v) noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        auto head = head_.load(std::memory_order_relaxed);
        auto next = inc(head);
        if (next == tail_.load(std::memory_order_acquire))
            return false; // full
        buf_[head] = std::move(v);
        head_.store(next, std::memory_order_release);
        return true;
    }
    bool try_pop(T& out) noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        auto tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire))
            return false; // empty
        out = std::move(buf_[tail]);
        tail_.store(inc(tail), std::memory_order_release);
        return true;
    }
    bool empty() const noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }
    bool full() const noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        return inc(head_.load(std::memory_order_acquire)) == tail_.load(std::memory_order_acquire);
    }
#else
    // Original lock-free SPSC variant (unsafe for multiple producers/consumers).
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
            return false; // full
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
#endif
    std::size_t capacity() const noexcept { return cap_; }

private:
    std::size_t inc(std::size_t i) const noexcept { return (++i == cap_) ? 0 : i; }
    mutable std::mutex mu_;
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
    std::shared_ptr<SpscQueue<T>> make_channel(const std::string& name, std::size_t capacity) {
        return get_or_create_channel<T>(name, capacity);
    }

    template <typename T>
    std::shared_ptr<SpscQueue<T>> get_or_create_channel(const std::string& name,
                                                        std::size_t capacity) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = chans_.find(name);
        if (it != chans_.end()) {
            return std::static_pointer_cast<SpscQueue<T>>(it->second);
        }
        auto q = std::make_shared<SpscQueue<T>>(capacity ? capacity : 1000);
        chans_[name] = q;
        return q;
    }

    // Common event types
    struct EmbedJob {
        std::vector<std::string> hashes;
        uint32_t batchSize{0};
        bool skipExisting{true};
        std::string modelName;
    };

    enum class Fts5Operation {
        ExtractAndIndex, // Extract content + index in FTS5
        RemoveOrphans    // Remove FTS5 entries for non-existent documents
    };

    struct Fts5Job {
        std::vector<std::string> hashes;
        uint32_t batchSize{0};
        Fts5Operation operation{Fts5Operation::ExtractAndIndex};
    };
    struct PostIngestTask {
        std::string hash;
        std::string mime;
    };
    struct StoreDocumentTask {
        AddDocumentRequest request;
    };
    struct ModelReadyEvent {
        std::string modelId;
    };
    struct ModelLoadFailedEvent {
        std::string modelId;
        std::string error;
    };

    // PBI-062: Prune job
    struct PruneJob {
        uint64_t requestId; // To match response
        yams::integrity::PruneConfig config;
    };

private:
    InternalEventBus() = default;
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<void>> chans_;
    // Simple counters for doctor/status
    std::atomic<std::uint64_t> embedQueued_{0};
    std::atomic<std::uint64_t> embedDropped_{0};
    std::atomic<std::uint64_t> embedConsumed_{0};
    std::atomic<std::uint64_t> fts5Queued_{0};
    std::atomic<std::uint64_t> fts5Dropped_{0};
    std::atomic<std::uint64_t> fts5Consumed_{0};
    std::atomic<std::uint64_t> orphansDetected_{0};
    std::atomic<std::uint64_t> orphansRemoved_{0};
    std::atomic<std::uint64_t> lastOrphanScanEpochMs_{0}; // milliseconds since epoch, 0 = never
    std::atomic<std::uint64_t> fts5FailNoDoc_{0};         // Document not found in metadata
    std::atomic<std::uint64_t> fts5FailExtraction_{0};    // Text extraction failed or empty
    std::atomic<std::uint64_t> fts5FailIndex_{0};         // FTS5 indexing failed (DB error)
    std::atomic<std::uint64_t> fts5FailException_{0};     // Unexpected exceptions
    std::atomic<std::uint64_t> postQueued_{0};
    std::atomic<std::uint64_t> postDropped_{0};
    std::atomic<std::uint64_t> postConsumed_{0};

public:
    // Counter helpers
    void incEmbedQueued() { embedQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incEmbedDropped() { embedDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incEmbedConsumed() { embedConsumed_.fetch_add(1, std::memory_order_relaxed); }
    void incFts5Queued() { fts5Queued_.fetch_add(1, std::memory_order_relaxed); }
    void incFts5Dropped() { fts5Dropped_.fetch_add(1, std::memory_order_relaxed); }
    void incFts5Consumed() { fts5Consumed_.fetch_add(1, std::memory_order_relaxed); }
    void incOrphansDetected(uint64_t count = 1) {
        orphansDetected_.fetch_add(count, std::memory_order_relaxed);
    }
    void incOrphansRemoved(uint64_t count = 1) {
        orphansRemoved_.fetch_add(count, std::memory_order_relaxed);
    }
    void setLastOrphanScanTime(uint64_t epochMs) {
        lastOrphanScanEpochMs_.store(epochMs, std::memory_order_relaxed);
    }
    void incFts5FailNoDoc(uint64_t count = 1) {
        fts5FailNoDoc_.fetch_add(count, std::memory_order_relaxed);
    }
    void incFts5FailExtraction(uint64_t count = 1) {
        fts5FailExtraction_.fetch_add(count, std::memory_order_relaxed);
    }
    void incFts5FailIndex(uint64_t count = 1) {
        fts5FailIndex_.fetch_add(count, std::memory_order_relaxed);
    }
    void incFts5FailException(uint64_t count = 1) {
        fts5FailException_.fetch_add(count, std::memory_order_relaxed);
    }
    void incPostQueued() { postQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incPostDropped() { postDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incPostConsumed() { postConsumed_.fetch_add(1, std::memory_order_relaxed); }

    std::uint64_t embedQueued() const { return embedQueued_.load(std::memory_order_relaxed); }
    std::uint64_t embedDropped() const { return embedDropped_.load(std::memory_order_relaxed); }
    std::uint64_t embedConsumed() const { return embedConsumed_.load(std::memory_order_relaxed); }
    std::uint64_t fts5Queued() const { return fts5Queued_.load(std::memory_order_relaxed); }
    std::uint64_t fts5Dropped() const { return fts5Dropped_.load(std::memory_order_relaxed); }
    std::uint64_t fts5Consumed() const { return fts5Consumed_.load(std::memory_order_relaxed); }
    std::uint64_t orphansDetected() const {
        return orphansDetected_.load(std::memory_order_relaxed);
    }
    std::uint64_t orphansRemoved() const { return orphansRemoved_.load(std::memory_order_relaxed); }
    std::uint64_t lastOrphanScanEpochMs() const {
        return lastOrphanScanEpochMs_.load(std::memory_order_relaxed);
    }
    std::uint64_t fts5FailNoDoc() const { return fts5FailNoDoc_.load(std::memory_order_relaxed); }
    std::uint64_t fts5FailExtraction() const {
        return fts5FailExtraction_.load(std::memory_order_relaxed);
    }
    std::uint64_t fts5FailIndex() const { return fts5FailIndex_.load(std::memory_order_relaxed); }
    std::uint64_t fts5FailException() const {
        return fts5FailException_.load(std::memory_order_relaxed);
    }
    std::uint64_t postQueued() const { return postQueued_.load(std::memory_order_relaxed); }
    std::uint64_t postDropped() const { return postDropped_.load(std::memory_order_relaxed); }
    std::uint64_t postConsumed() const { return postConsumed_.load(std::memory_order_relaxed); }
};

} // namespace yams::daemon
