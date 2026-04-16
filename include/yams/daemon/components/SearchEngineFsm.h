#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>

namespace yams::daemon {

enum class SearchEngineState { NotBuilt, Building, Ready, Degraded, Failed, AwaitingDrain };

struct SearchEngineSnapshot {
    SearchEngineState state{SearchEngineState::NotBuilt};
    std::string lastError;
    std::string buildReason;
    bool vectorEnabled{false};
    bool hasEngine{false};
    bool rebuildPending{false};
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

struct SearchEngineRebuildRequestedEvent {
    std::string reason;
    bool includeVectorSearch{false};
    bool waitForDrain{true};
};

struct SearchEngineIndexingDrainedEvent {};

class SearchEngineFsm {
public:
    using RebuildCallback = std::function<void(const std::string& reason, bool includeVector)>;

    SearchEngineFsm();

    SearchEngineSnapshot snapshot() const;
    void setRebuildCallback(RebuildCallback cb);

    void dispatch(const SearchEngineRebuildStartedEvent& ev);
    void dispatch(const SearchEngineRebuildCompletedEvent& ev);
    void dispatch(const SearchEngineRebuildFailedEvent& ev);
    void dispatch(const SearchEngineRebuildDegradedEvent& ev);
    bool dispatch(const SearchEngineRebuildRequestedEvent& ev);
    void dispatch(const SearchEngineIndexingDrainedEvent& ev);

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
    bool hasRebuildPending() const;

private:
    void syncAtomicState();

    mutable std::mutex mutex_;
    std::atomic<SearchEngineState> state_{SearchEngineState::NotBuilt};
    RebuildCallback rebuildCallback_;
};

} // namespace yams::daemon
