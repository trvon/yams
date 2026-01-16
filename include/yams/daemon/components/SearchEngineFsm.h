#pragma once

#include <chrono>
#include <functional>
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

    SearchEngineSnapshot snapshot() const { return snap_; }

    /// Set callback to be invoked when rebuild should start
    void setRebuildCallback(RebuildCallback cb) { rebuildCallback_ = std::move(cb); }

    void dispatch(const SearchEngineRebuildStartedEvent& ev) {
        transitionTo(SearchEngineState::Building);
        snap_.buildReason = ev.reason;
        snap_.vectorEnabled = ev.includeVectorSearch;
        snap_.buildDurationMs = 0;
        snap_.rebuildPending = false;
    }

    void dispatch(const SearchEngineRebuildCompletedEvent& ev) {
        transitionTo(SearchEngineState::Ready);
        snap_.vectorEnabled = ev.vectorEnabled;
        snap_.hasEngine = true;
        snap_.lastBuildTime = std::chrono::system_clock::now();
        snap_.buildDurationMs = ev.durationMs;
        snap_.lastError.clear();
        snap_.rebuildPending = false;
    }

    void dispatch(const SearchEngineRebuildFailedEvent& ev) {
        snap_.lastError = ev.error;
        snap_.hasEngine = false;
        snap_.rebuildPending = false;
        transitionTo(SearchEngineState::Failed);
    }

    void dispatch(const SearchEngineRebuildDegradedEvent& ev) {
        snap_.lastError = ev.reason;
        transitionTo(SearchEngineState::Degraded);
    }

    /// Request a rebuild. If waitForDrain is true, transitions to AwaitingDrain.
    /// If already building/awaiting, the request is ignored.
    /// Returns true if the request was accepted.
    bool dispatch(const SearchEngineRebuildRequestedEvent& ev) {
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

        // Start immediately
        if (rebuildCallback_) {
            rebuildCallback_(ev.reason, ev.includeVectorSearch);
        }
        return true;
    }

    /// Called when indexing queue is drained. If in AwaitingDrain state, triggers rebuild.
    void dispatch(const SearchEngineIndexingDrainedEvent& /*ev*/) {
        if (snap_.state != SearchEngineState::AwaitingDrain) {
            return; // Not waiting for drain, ignore
        }

        // Trigger the actual rebuild
        if (rebuildCallback_) {
            rebuildCallback_(pendingReason_, pendingIncludeVector_);
        }
        // Note: The callback should dispatch SearchEngineRebuildStartedEvent
        // which will transition state to Building
    }

    // Query methods
    bool isBuilding() const { return snap_.state == SearchEngineState::Building; }
    bool isReady() const { return snap_.state == SearchEngineState::Ready; }
    bool hasFailed() const { return snap_.state == SearchEngineState::Failed; }
    bool isAwaitingDrain() const { return snap_.state == SearchEngineState::AwaitingDrain; }
    bool hasRebuildPending() const { return snap_.rebuildPending; }

private:
    void transitionTo(SearchEngineState next) { snap_.state = next; }

    SearchEngineSnapshot snap_{};
    RebuildCallback rebuildCallback_;
    std::string pendingReason_;
    bool pendingIncludeVector_{false};
};

} // namespace yams::daemon
