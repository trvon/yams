#pragma once

#include <chrono>
#include <memory>
#include <string>

// Forward declaration to avoid circular dependency
namespace yams::search {
class HybridSearchEngine;
}

namespace yams::daemon {

enum class SearchEngineState { NotBuilt, Building, Ready, Degraded, Failed };

struct SearchEngineSnapshot {
    SearchEngineState state{SearchEngineState::NotBuilt};
    std::string lastError;
    std::string buildReason;
    bool vectorEnabled{false};
    bool hasEngine{false};
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

class SearchEngineFsm {
public:
    SearchEngineSnapshot snapshot() const { return snap_; }

    void dispatch(const SearchEngineRebuildStartedEvent& ev) {
        transitionTo(SearchEngineState::Building);
        snap_.buildReason = ev.reason;
        snap_.vectorEnabled = ev.includeVectorSearch;
        snap_.buildDurationMs = 0;
    }

    void dispatch(const SearchEngineRebuildCompletedEvent& ev) {
        transitionTo(SearchEngineState::Ready);
        snap_.vectorEnabled = ev.vectorEnabled;
        snap_.hasEngine = true;
        snap_.lastBuildTime = std::chrono::system_clock::now();
        snap_.buildDurationMs = ev.durationMs;
        snap_.lastError.clear();
    }

    void dispatch(const SearchEngineRebuildFailedEvent& ev) {
        snap_.lastError = ev.error;
        snap_.hasEngine = false;
        transitionTo(SearchEngineState::Failed);
    }

    void dispatch(const SearchEngineRebuildDegradedEvent& ev) {
        snap_.lastError = ev.reason;
        transitionTo(SearchEngineState::Degraded);
    }

    // Query methods
    bool isBuilding() const { return snap_.state == SearchEngineState::Building; }
    bool isReady() const { return snap_.state == SearchEngineState::Ready; }
    bool hasFailed() const { return snap_.state == SearchEngineState::Failed; }

private:
    void transitionTo(SearchEngineState next) { snap_.state = next; }

    SearchEngineSnapshot snap_{};
};

} // namespace yams::daemon
