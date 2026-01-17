#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>

namespace yams::daemon {

enum class SearchEngineState {
    NotBuilt,     // Initial state, no engine built yet
    Building,     // Engine build in progress
    Ready,        // Engine built and ready for queries
    Degraded,     // Engine available but with reduced functionality
    Failed,       // Engine build failed
    AwaitingDrain // Waiting for indexing to drain before rebuild
};

struct SearchEngineSnapshot {
    SearchEngineState state{SearchEngineState::NotBuilt};
    std::string lastError;
    std::string buildReason;
    bool vectorEnabled{false};
    bool hasEngine{false};
    bool rebuildPending{false}; // True when waiting for drain to complete
    std::chrono::system_clock::time_point lastBuildTime;
    uint64_t buildDurationMs{0};
};

struct SearchEngineRebuildStartedEvent {
    std::string reason;
    bool includeVectorSearch{false};
};

struct SearchEngineRebuildCompletedEvent {
    bool vectorEnabled{false};
    uint64_t durationMs{0};
};

struct SearchEngineRebuildFailedEvent {
    std::string error;
};

struct SearchEngineRebuildDegradedEvent {
    std::string reason;
};

// New events for drain-based rebuild triggering
struct SearchEngineRebuildRequestedEvent {
    std::string reason;
    bool includeVectorSearch{false};
    bool waitForDrain{true}; // If true, transition to AwaitingDrain; if false, start immediately
};

struct SearchEngineIndexingDrainedEvent {
    // Signaled when PostIngestQueue becomes empty after processing
};

class SearchEngineFsm {
public:
    using RebuildCallback = std::function<void(const std::string& reason, bool includeVector)>;

    SearchEngineSnapshot snapshot() const {
        std::lock_guard lock(mutex_);
        return snap_;
    }

    /// Set callback to be invoked when rebuild should start
    void setRebuildCallback(RebuildCallback cb) {
        std::lock_guard lock(mutex_);
        rebuildCallback_ = std::move(cb);
    }

    void dispatch(const SearchEngineRebuildStartedEvent& ev) {
        std::lock_guard lock(mutex_);
        transitionTo(SearchEngineState::Building);
        snap_.buildReason = ev.reason;
        snap_.vectorEnabled = ev.includeVectorSearch;
        snap_.buildDurationMs = 0;
        snap_.rebuildPending = false;
    }

    void dispatch(const SearchEngineRebuildCompletedEvent& ev) {
        std::lock_guard lock(mutex_);
        transitionTo(SearchEngineState::Ready);
        snap_.vectorEnabled = ev.vectorEnabled;
        snap_.hasEngine = true;
        snap_.lastBuildTime = std::chrono::system_clock::now();
        snap_.buildDurationMs = ev.durationMs;
        snap_.lastError.clear();
        snap_.rebuildPending = false;
    }

    void dispatch(const SearchEngineRebuildFailedEvent& ev) {
        std::lock_guard lock(mutex_);
        snap_.lastError = ev.error;
        snap_.hasEngine = false;
        snap_.rebuildPending = false;
        transitionTo(SearchEngineState::Failed);
    }

    void dispatch(const SearchEngineRebuildDegradedEvent& ev) {
        std::lock_guard lock(mutex_);
        snap_.lastError = ev.reason;
        transitionTo(SearchEngineState::Degraded);
    }

    /// Request a rebuild. If waitForDrain is true, transitions to AwaitingDrain.
    /// If already building/awaiting, the request is ignored.
    /// Returns true if the request was accepted.
    bool dispatch(const SearchEngineRebuildRequestedEvent& ev) {
        RebuildCallback cb;
        {
            std::lock_guard lock(mutex_);
            // Guard: don't accept if already building or awaiting drain
            if (snap_.state == SearchEngineState::Building ||
                snap_.state == SearchEngineState::AwaitingDrain) {
                return false;
            }

            pendingReason_ = ev.reason;
            pendingIncludeVector_ = ev.includeVectorSearch;

            if (ev.waitForDrain) {
                // Transition to awaiting drain - rebuild will start when drained
                transitionTo(SearchEngineState::AwaitingDrain);
                snap_.rebuildPending = true;
                snap_.buildReason = ev.reason + " (awaiting drain)";
                return true;
            }

            cb = rebuildCallback_;
        }

        // Start immediately (callback invoked outside lock to avoid deadlock)
        if (cb) {
            cb(ev.reason, ev.includeVectorSearch);
        }
        return true;
    }

    /// Called when indexing queue is drained. If in AwaitingDrain state, triggers rebuild.
    void dispatch(const SearchEngineIndexingDrainedEvent& /*ev*/) {
        RebuildCallback cb;
        std::string reason;
        bool includeVector = false;
        {
            std::lock_guard lock(mutex_);
            if (snap_.state != SearchEngineState::AwaitingDrain) {
                return; // Not waiting for drain, ignore
            }
            cb = rebuildCallback_;
            reason = pendingReason_;
            includeVector = pendingIncludeVector_;
        }

        // Trigger the actual rebuild (outside lock to avoid deadlock)
        if (cb) {
            cb(reason, includeVector);
        }
        // Note: The callback should dispatch SearchEngineRebuildStartedEvent
        // which will transition state to Building
    }

    // Query methods (use atomic for lock-free reads of state)
    bool isBuilding() const {
        return state_.load(std::memory_order_acquire) == SearchEngineState::Building;
    }
    bool isReady() const {
        return state_.load(std::memory_order_acquire) == SearchEngineState::Ready;
    }
    bool hasFailed() const {
        return state_.load(std::memory_order_acquire) == SearchEngineState::Failed;
    }
    bool isAwaitingDrain() const {
        return state_.load(std::memory_order_acquire) == SearchEngineState::AwaitingDrain;
    }
    bool hasRebuildPending() const {
        std::lock_guard lock(mutex_);
        return snap_.rebuildPending;
    }

private:
    void transitionTo(SearchEngineState next) {
        snap_.state = next;
        state_.store(next, std::memory_order_release);
    }

    mutable std::mutex mutex_;
    std::atomic<SearchEngineState> state_{SearchEngineState::NotBuilt};
    SearchEngineSnapshot snap_{};
    RebuildCallback rebuildCallback_;
    std::string pendingReason_;
    bool pendingIncludeVector_{false};
};

} // namespace yams::daemon
