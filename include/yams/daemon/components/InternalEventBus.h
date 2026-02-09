#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
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
    // Batch push with a single lock acquisition.
    // Returns number of items successfully enqueued.
    std::size_t try_push_many(std::vector<T>& values, std::size_t start = 0) noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        if (start >= values.size()) {
            return 0;
        }
        std::size_t pushed = 0;
        auto head = head_.load(std::memory_order_relaxed);
        auto tail = tail_.load(std::memory_order_acquire);
        for (std::size_t i = start; i < values.size(); ++i) {
            auto next = inc(head);
            if (next == tail) {
                break; // full
            }
            buf_[head] = std::move(values[i]);
            head = next;
            ++pushed;
        }
        if (pushed > 0) {
            head_.store(head, std::memory_order_release);
        }
        return pushed;
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
    std::size_t size_approx() const noexcept {
        std::lock_guard<std::mutex> lk(mu_);
        auto h = head_.load(std::memory_order_acquire);
        auto t = tail_.load(std::memory_order_acquire);
        return (h >= t) ? (h - t) : (cap_ - t + h);
    }

    // Blocking push with exponential backoff. Returns true if pushed, false if timeout.
    // This provides backpressure for slow consumers (e.g., embedding service).
    // PBI-05b: Used by PostIngestQueue to prevent embed job drops during bulk ingest.
    template <typename Rep, typename Period>
    bool push_wait(T&& v, std::chrono::duration<Rep, Period> timeout) noexcept {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        auto delay = std::chrono::microseconds(100); // Start with 100us
        constexpr auto maxDelay = std::chrono::milliseconds(10);

        while (!try_push(std::move(v))) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false; // Timeout
            }
            std::this_thread::sleep_for(delay);
            delay = std::min(delay * 2,
                             std::chrono::duration_cast<std::chrono::microseconds>(maxDelay));
        }
        return true;
    }

    template <typename Rep, typename Period>
    bool push_wait(const T& v, std::chrono::duration<Rep, Period> timeout) noexcept {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        auto delay = std::chrono::microseconds(100);
        constexpr auto maxDelay = std::chrono::milliseconds(10);

        while (!try_push(v)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(delay);
            delay = std::min(delay * 2,
                             std::chrono::duration_cast<std::chrono::microseconds>(maxDelay));
        }
        return true;
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
    std::size_t try_push_many(std::vector<T>& values, std::size_t start = 0) noexcept {
        if (start >= values.size()) {
            return 0;
        }
        std::size_t pushed = 0;
        for (std::size_t i = start; i < values.size(); ++i) {
            if (!try_push(std::move(values[i]))) {
                break;
            }
            ++pushed;
        }
        return pushed;
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
    std::size_t size_approx() const noexcept {
        auto h = head_.load(std::memory_order_acquire);
        auto t = tail_.load(std::memory_order_acquire);
        return (h >= t) ? (h - t) : (cap_ - t + h);
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

    // Non-creating accessor: returns nullptr if channel doesn't exist.
    // Use for observability paths (status/metrics) to avoid side effects.
    template <typename T> std::shared_ptr<SpscQueue<T>> get_channel(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = chans_.find(name);
        if (it == chans_.end()) {
            return nullptr;
        }
        return std::static_pointer_cast<SpscQueue<T>>(it->second);
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
        std::vector<int64_t> ids; // For RemoveOrphans: document rowids to remove
        uint32_t batchSize{0};
        Fts5Operation operation{Fts5Operation::ExtractAndIndex};
    };
    struct PostIngestTask {
        std::string hash;
        std::string mime;
    };
    struct KgJob {
        std::string hash;
        int64_t documentId{-1};
        std::string filePath;
        std::vector<std::string> tags;
        std::shared_ptr<std::vector<std::byte>> contentBytes;
    };
    struct SymbolExtractionJob {
        std::string hash;
        int64_t documentId{-1};
        std::string filePath;
        std::string language;
        std::shared_ptr<std::vector<std::byte>> contentBytes;
    };
    struct EntityExtractionJob {
        std::string hash;
        int64_t documentId{-1};
        std::string filePath;
        std::string extension;
        std::shared_ptr<std::vector<std::byte>> contentBytes;
    };
    // Async GLiNER job for title + NL entity extraction (single call)
    struct TitleExtractionJob {
        std::string hash;
        int64_t documentId{-1};
        std::string textSnippet;   // First N chars for GLiNER inference
        std::string fallbackTitle; // Filename to use if GLiNER fails
        // Additional context for NL entity KG population
        std::string filePath; // For KG node creation
        std::string language; // Language hint for extraction
        std::string mimeType; // MIME type for content routing
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

    // PathTreeRepair job - creates missing path tree entries for documents
    struct PathTreeJob {
        uint64_t requestId{0}; // To match response
        bool runOnce{true};    // Run once at startup vs periodic
    };

    // Storage garbage collection job (routed through bus for centralized dispatch)
    struct StorageGcJob {
        bool dryRun{false};
    };

    // Entity graph extraction job (routes through bus instead of direct WorkCoordinator dispatch)
    struct EntityGraphJob {
        std::string documentHash;
        std::string filePath;
        std::string contentUtf8;
        std::string language;
        std::string mimeType;
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
    std::atomic<std::uint64_t> kgQueued_{0};
    std::atomic<std::uint64_t> kgDropped_{0};
    std::atomic<std::uint64_t> kgConsumed_{0};
    std::atomic<std::uint64_t> symbolQueued_{0};
    std::atomic<std::uint64_t> symbolDropped_{0};
    std::atomic<std::uint64_t> symbolConsumed_{0};
    std::atomic<std::uint64_t> entityQueued_{0};
    std::atomic<std::uint64_t> entityDropped_{0};
    std::atomic<std::uint64_t> entityConsumed_{0};
    std::atomic<std::uint64_t> titleQueued_{0};
    std::atomic<std::uint64_t> titleDropped_{0};
    std::atomic<std::uint64_t> titleConsumed_{0};
    std::atomic<std::uint64_t> gcQueued_{0};
    std::atomic<std::uint64_t> gcDropped_{0};
    std::atomic<std::uint64_t> gcConsumed_{0};
    std::atomic<std::uint64_t> entityGraphQueued_{0};
    std::atomic<std::uint64_t> entityGraphDropped_{0};
    std::atomic<std::uint64_t> entityGraphConsumed_{0};

public:
    // Counter helpers
    void incEmbedQueued(std::uint64_t n = 1) {
        embedQueued_.fetch_add(n, std::memory_order_relaxed);
    }
    void incEmbedDropped(std::uint64_t n = 1) {
        embedDropped_.fetch_add(n, std::memory_order_relaxed);
    }
    void incEmbedConsumed(std::uint64_t n = 1) {
        embedConsumed_.fetch_add(n, std::memory_order_relaxed);
    }
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
    void incKgQueued() { kgQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incKgDropped() { kgDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incKgConsumed() { kgConsumed_.fetch_add(1, std::memory_order_relaxed); }
    void incSymbolQueued() { symbolQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incSymbolDropped() { symbolDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incSymbolConsumed() { symbolConsumed_.fetch_add(1, std::memory_order_relaxed); }
    void incEntityQueued() { entityQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incEntityDropped() { entityDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incEntityConsumed() { entityConsumed_.fetch_add(1, std::memory_order_relaxed); }
    void incTitleQueued() { titleQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incTitleDropped() { titleDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incTitleConsumed() { titleConsumed_.fetch_add(1, std::memory_order_relaxed); }

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
    std::uint64_t kgQueued() const { return kgQueued_.load(std::memory_order_relaxed); }
    std::uint64_t kgDropped() const { return kgDropped_.load(std::memory_order_relaxed); }
    std::uint64_t kgConsumed() const { return kgConsumed_.load(std::memory_order_relaxed); }
    std::uint64_t symbolQueued() const { return symbolQueued_.load(std::memory_order_relaxed); }
    std::uint64_t symbolDropped() const { return symbolDropped_.load(std::memory_order_relaxed); }
    std::uint64_t symbolConsumed() const { return symbolConsumed_.load(std::memory_order_relaxed); }
    std::uint64_t entityQueued() const { return entityQueued_.load(std::memory_order_relaxed); }
    std::uint64_t entityDropped() const { return entityDropped_.load(std::memory_order_relaxed); }
    std::uint64_t entityConsumed() const { return entityConsumed_.load(std::memory_order_relaxed); }
    std::uint64_t titleQueued() const { return titleQueued_.load(std::memory_order_relaxed); }
    std::uint64_t titleDropped() const { return titleDropped_.load(std::memory_order_relaxed); }
    std::uint64_t titleConsumed() const { return titleConsumed_.load(std::memory_order_relaxed); }

    void incGcQueued() { gcQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incGcDropped() { gcDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incGcConsumed() { gcConsumed_.fetch_add(1, std::memory_order_relaxed); }
    void incEntityGraphQueued() { entityGraphQueued_.fetch_add(1, std::memory_order_relaxed); }
    void incEntityGraphDropped() { entityGraphDropped_.fetch_add(1, std::memory_order_relaxed); }
    void incEntityGraphConsumed() { entityGraphConsumed_.fetch_add(1, std::memory_order_relaxed); }

    std::uint64_t gcQueued() const { return gcQueued_.load(std::memory_order_relaxed); }
    std::uint64_t gcDropped() const { return gcDropped_.load(std::memory_order_relaxed); }
    std::uint64_t gcConsumed() const { return gcConsumed_.load(std::memory_order_relaxed); }
    std::uint64_t entityGraphQueued() const {
        return entityGraphQueued_.load(std::memory_order_relaxed);
    }
    std::uint64_t entityGraphDropped() const {
        return entityGraphDropped_.load(std::memory_order_relaxed);
    }
    std::uint64_t entityGraphConsumed() const {
        return entityGraphConsumed_.load(std::memory_order_relaxed);
    }
};

} // namespace yams::daemon
