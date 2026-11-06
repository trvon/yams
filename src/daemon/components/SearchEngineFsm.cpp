#include <yams/daemon/components/SearchEngineFsm.h>

#include <spdlog/spdlog.h>
#include <yams/core/assert.hpp>

namespace yams::daemon {

namespace {

bool canDispatch(SearchEngineState state, const SearchEngineRebuildStartedEvent&) {
    return state != SearchEngineState::Building;
}

bool canDispatch(SearchEngineState state, const SearchEngineRebuildCompletedEvent&) {
    return state == SearchEngineState::Building || state == SearchEngineState::NotBuilt ||
           state == SearchEngineState::Ready || state == SearchEngineState::Failed;
}

bool canDispatch(SearchEngineState state, const SearchEngineRebuildFailedEvent&) {
    return state == SearchEngineState::Building;
}

template <typename Event>
void validateDispatch(SearchEngineState state, const Event& ev, const char* message) {
    YAMS_DCHECK(canDispatch(state, ev), message);
}

} // namespace

// Transitions match the former tinyfsm machine:
// - RebuildStarted enters Building from any state.
// - RebuildCompleted and RebuildFailed apply only in Building (to Ready / Failed).
// - RebuildRequested is rejected in Building and AwaitingDrain. Otherwise a drain-gated request
//   enters AwaitingDrain and an immediate one hands its reason to the rebuild callback.
// - IndexingDrained in AwaitingDrain hands the stored reason to the rebuild callback.
SearchEngineFsm::SearchEngineFsm() = default;

void SearchEngineFsm::enterLocked(SearchEngineState next) {
    const auto prev = snap_.state;
    snap_.state = next;
    spdlog::info("[SearchEngineFSM] {} -> {}", static_cast<int>(prev), static_cast<int>(next));
}

SearchEngineSnapshot SearchEngineFsm::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snap_;
}

void SearchEngineFsm::setRebuildCallback(RebuildCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    rebuildCallback_ = std::move(cb);
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildStartedEvent& ev) {
    validateDispatch(
        snapshot().state, ev,
        "SearchEngineFsm SearchEngineRebuildStartedEvent must not arrive while already building");
    std::lock_guard<std::mutex> lock(mutex_);
    snap_.buildReason = ev.reason;
    snap_.vectorEnabled = ev.includeVectorSearch;
    snap_.buildDurationMs = 0;
    snap_.rebuildPending = false;
    enterLocked(SearchEngineState::Building);
    syncAtomicState();
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildCompletedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "SearchEngineFsm SearchEngineRebuildCompletedEvent requires an existing or "
                     "injected engine lifecycle state");
    std::lock_guard<std::mutex> lock(mutex_);
    if (snap_.state != SearchEngineState::Building) {
        return;
    }
    snap_.vectorEnabled = ev.vectorEnabled;
    snap_.hasEngine = true;
    snap_.lastBuildTime = std::chrono::system_clock::now();
    snap_.buildDurationMs = ev.durationMs;
    snap_.lastError.clear();
    snap_.rebuildPending = false;
    enterLocked(SearchEngineState::Ready);
    syncAtomicState();
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildFailedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "SearchEngineFsm SearchEngineRebuildFailedEvent requires Building state");
    std::lock_guard<std::mutex> lock(mutex_);
    if (snap_.state != SearchEngineState::Building) {
        return;
    }
    snap_.lastError = ev.error;
    snap_.hasEngine = false;
    snap_.rebuildPending = false;
    enterLocked(SearchEngineState::Failed);
    syncAtomicState();
}

bool SearchEngineFsm::dispatch(const SearchEngineRebuildRequestedEvent& ev) {
    RebuildCallback cb;
    std::string reason;
    bool includeVector = false;
    bool callbackConfigured = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (snap_.state == SearchEngineState::Building ||
            snap_.state == SearchEngineState::AwaitingDrain) {
            return false;
        }
        callbackReason_ = ev.reason;
        callbackIncludeVector_ = ev.includeVectorSearch;
        if (ev.waitForDrain) {
            snap_.rebuildPending = true;
            snap_.buildReason = ev.reason + " (awaiting drain)";
            enterLocked(SearchEngineState::AwaitingDrain);
            syncAtomicState();
            YAMS_POSTCONDITION(snap_.state == SearchEngineState::AwaitingDrain &&
                                   snap_.rebuildPending,
                               "SearchEngineFsm accepted drain-gated rebuilds must enter "
                               "AwaitingDrain with rebuildPending=true");
        } else {
            callbackConfigured = static_cast<bool>(rebuildCallback_);
            cb = rebuildCallback_;
            reason = callbackReason_;
            includeVector = callbackIncludeVector_;
        }
    }

    if (cb) {
        cb(reason, includeVector);
    }
    YAMS_POSTCONDITION(ev.waitForDrain || cb != nullptr || !callbackConfigured,
                       "SearchEngineFsm immediate rebuild requests must either hand off to the "
                       "configured callback or run without one explicitly configured");
    return true;
}

void SearchEngineFsm::dispatch(const SearchEngineIndexingDrainedEvent&) {
    RebuildCallback cb;
    std::string reason;
    bool includeVector = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (snap_.state != SearchEngineState::AwaitingDrain) {
            return;
        }
        cb = rebuildCallback_;
        reason = callbackReason_;
        includeVector = callbackIncludeVector_;
    }

    if (cb) {
        cb(reason, includeVector);
    }
}

void SearchEngineFsm::syncAtomicState() {
    state_.store(snap_.state, std::memory_order_release);
}

} // namespace yams::daemon
