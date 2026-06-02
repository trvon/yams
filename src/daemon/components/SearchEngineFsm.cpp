#include <yams/daemon/components/SearchEngineFsm.h>

#include <spdlog/spdlog.h>
#include <tinyfsm.hpp>
#include <yams/core/assert.hpp>

namespace yams::daemon {
namespace detail {

struct SENotBuilt;
struct SEBuilding;
struct SEReady;
struct SEDegraded;
struct SEFailed;
struct SEAwaitingDrain;

struct SearchEngineMachine : tinyfsm::MooreMachine<SearchEngineMachine> {
    inline static SearchEngineSnapshot snap{};
    inline static bool dispatchAccepted{false};
    inline static bool callbackPending{false};
    inline static std::string callbackReason;
    inline static bool callbackIncludeVector{false};

    virtual void react(const SearchEngineRebuildStartedEvent& ev);
    virtual void react(const SearchEngineRebuildCompletedEvent&) {}
    virtual void react(const SearchEngineRebuildFailedEvent&) {}
    virtual void react(const SearchEngineRebuildDegradedEvent& ev);
    virtual void react(const SearchEngineRebuildRequestedEvent& ev);
    virtual void react(const SearchEngineIndexingDrainedEvent&) {}

protected:
    void applyEntry(SearchEngineState next) {
        auto prev = snap.state;
        snap.state = next;
        spdlog::info("[SearchEngineFSM] {} -> {}", static_cast<int>(prev), static_cast<int>(next));
    }
};

void SearchEngineMachine::react(const SearchEngineRebuildStartedEvent& ev) {
    snap.buildReason = ev.reason;
    snap.vectorEnabled = ev.includeVectorSearch;
    snap.buildDurationMs = 0;
    snap.rebuildPending = false;
    transit<SEBuilding>();
}

void SearchEngineMachine::react(const SearchEngineRebuildDegradedEvent& ev) {
    snap.lastError = ev.reason;
    transit<SEDegraded>();
}

void SearchEngineMachine::react(const SearchEngineRebuildRequestedEvent& ev) {
    dispatchAccepted = true;

    if (ev.waitForDrain) {
        snap.rebuildPending = true;
        snap.buildReason = ev.reason + " (awaiting drain)";
        callbackReason = ev.reason;
        callbackIncludeVector = ev.includeVectorSearch;
        callbackPending = false;
        transit<SEAwaitingDrain>();
    } else {
        callbackPending = true;
        callbackReason = ev.reason;
        callbackIncludeVector = ev.includeVectorSearch;
    }
}

struct SENotBuilt : SearchEngineMachine {
    void entry() override { applyEntry(SearchEngineState::NotBuilt); }
};

struct SEBuilding : SearchEngineMachine {
    void entry() override { applyEntry(SearchEngineState::Building); }

    void react(const SearchEngineRebuildCompletedEvent& ev) override {
        snap.vectorEnabled = ev.vectorEnabled;
        snap.hasEngine = true;
        snap.lastBuildTime = std::chrono::system_clock::now();
        snap.buildDurationMs = ev.durationMs;
        snap.lastError.clear();
        snap.rebuildPending = false;
        transit<SEReady>();
    }

    void react(const SearchEngineRebuildFailedEvent& ev) override {
        snap.lastError = ev.error;
        snap.hasEngine = false;
        snap.rebuildPending = false;
        transit<SEFailed>();
    }

    void react(const SearchEngineRebuildRequestedEvent&) override {
        dispatchAccepted = false;
        callbackPending = false;
    }
};

struct SEReady : SearchEngineMachine {
    void entry() override { applyEntry(SearchEngineState::Ready); }
};

struct SEDegraded : SearchEngineMachine {
    void entry() override { applyEntry(SearchEngineState::Degraded); }
};

struct SEFailed : SearchEngineMachine {
    void entry() override { applyEntry(SearchEngineState::Failed); }
};

struct SEAwaitingDrain : SearchEngineMachine {
    void entry() override { applyEntry(SearchEngineState::AwaitingDrain); }

    void react(const SearchEngineRebuildRequestedEvent&) override {
        dispatchAccepted = false;
        callbackPending = false;
    }

    void react(const SearchEngineIndexingDrainedEvent&) override { callbackPending = true; }
};

} // namespace detail
} // namespace yams::daemon

FSM_INITIAL_STATE(yams::daemon::detail::SearchEngineMachine, yams::daemon::detail::SENotBuilt)

namespace yams::daemon {

namespace {

bool canDispatch(SearchEngineState state, const SearchEngineRebuildStartedEvent&) {
    return state != SearchEngineState::Building;
}

bool canDispatch(SearchEngineState state, const SearchEngineRebuildCompletedEvent&) {
    return state == SearchEngineState::Building || state == SearchEngineState::NotBuilt ||
           state == SearchEngineState::Ready || state == SearchEngineState::Failed ||
           state == SearchEngineState::Degraded;
}

bool canDispatch(SearchEngineState state, const SearchEngineRebuildFailedEvent&) {
    return state == SearchEngineState::Building;
}

bool canDispatch(SearchEngineState state, const SearchEngineRebuildDegradedEvent&) {
    return state != SearchEngineState::AwaitingDrain;
}

template <typename Event>
void validateDispatch(SearchEngineState state, const Event& ev, const char* message) {
    YAMS_DCHECK(canDispatch(state, ev), message);
}

} // namespace

SearchEngineFsm::SearchEngineFsm() {
    std::lock_guard<std::mutex> lock(sharedMutex());
    detail::SearchEngineMachine::snap = {};
    detail::SearchEngineMachine::dispatchAccepted = false;
    detail::SearchEngineMachine::callbackPending = false;
    detail::SearchEngineMachine::callbackReason.clear();
    detail::SearchEngineMachine::callbackIncludeVector = false;
    detail::SearchEngineMachine::start();
    syncAtomicState();
}

SearchEngineSnapshot SearchEngineFsm::snapshot() const {
    std::lock_guard<std::mutex> lock(sharedMutex());
    return detail::SearchEngineMachine::snap;
}

void SearchEngineFsm::setRebuildCallback(RebuildCallback cb) {
    std::lock_guard<std::mutex> lock(sharedMutex());
    rebuildCallback_ = std::move(cb);
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildStartedEvent& ev) {
    validateDispatch(
        snapshot().state, ev,
        "SearchEngineFsm SearchEngineRebuildStartedEvent must not arrive while already building");
    std::lock_guard<std::mutex> lock(sharedMutex());
    detail::SearchEngineMachine::dispatch(ev);
    syncAtomicState();
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildCompletedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "SearchEngineFsm SearchEngineRebuildCompletedEvent requires an existing or "
                     "injected engine lifecycle state");
    std::lock_guard<std::mutex> lock(sharedMutex());
    detail::SearchEngineMachine::dispatch(ev);
    syncAtomicState();
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildFailedEvent& ev) {
    validateDispatch(snapshot().state, ev,
                     "SearchEngineFsm SearchEngineRebuildFailedEvent requires Building state");
    std::lock_guard<std::mutex> lock(sharedMutex());
    detail::SearchEngineMachine::dispatch(ev);
    syncAtomicState();
}

void SearchEngineFsm::dispatch(const SearchEngineRebuildDegradedEvent& ev) {
    validateDispatch(
        snapshot().state, ev,
        "SearchEngineFsm SearchEngineRebuildDegradedEvent must not arrive while awaiting drain");
    std::lock_guard<std::mutex> lock(sharedMutex());
    detail::SearchEngineMachine::dispatch(ev);
    syncAtomicState();
}

bool SearchEngineFsm::dispatch(const SearchEngineRebuildRequestedEvent& ev) {
    RebuildCallback cb;
    std::string reason;
    bool includeVector = false;
    bool accepted = false;
    bool callbackConfigured = false;
    {
        std::lock_guard<std::mutex> lock(sharedMutex());
        detail::SearchEngineMachine::callbackPending = false;
        detail::SearchEngineMachine::dispatchAccepted = false;
        detail::SearchEngineMachine::dispatch(ev);
        syncAtomicState();

        accepted = detail::SearchEngineMachine::dispatchAccepted;
        if (!accepted) {
            return false;
        }

        if (detail::SearchEngineMachine::callbackPending) {
            callbackConfigured = static_cast<bool>(rebuildCallback_);
            cb = rebuildCallback_;
            reason = detail::SearchEngineMachine::callbackReason;
            includeVector = detail::SearchEngineMachine::callbackIncludeVector;
        }

        if (ev.waitForDrain) {
            YAMS_POSTCONDITION(detail::SearchEngineMachine::snap.state ==
                                       SearchEngineState::AwaitingDrain &&
                                   detail::SearchEngineMachine::snap.rebuildPending,
                               "SearchEngineFsm accepted drain-gated rebuilds must enter "
                               "AwaitingDrain with rebuildPending=true");
        }
    }

    if (cb) {
        cb(reason, includeVector);
    }
    YAMS_POSTCONDITION(!accepted || ev.waitForDrain || cb != nullptr || !callbackConfigured,
                       "SearchEngineFsm immediate rebuild requests must either hand off to the "
                       "configured callback or run without one explicitly configured");
    return true;
}

void SearchEngineFsm::dispatch(const SearchEngineIndexingDrainedEvent& ev) {
    RebuildCallback cb;
    std::string reason;
    bool includeVector = false;
    bool wasAwaitingDrain = false;
    {
        std::lock_guard<std::mutex> lock(sharedMutex());
        wasAwaitingDrain =
            detail::SearchEngineMachine::snap.state == SearchEngineState::AwaitingDrain;
        detail::SearchEngineMachine::callbackPending = false;
        detail::SearchEngineMachine::dispatch(ev);
        syncAtomicState();

        if (detail::SearchEngineMachine::callbackPending) {
            cb = rebuildCallback_;
            reason = detail::SearchEngineMachine::callbackReason;
            includeVector = detail::SearchEngineMachine::callbackIncludeVector;
        }

        YAMS_DCHECK(
            !wasAwaitingDrain || detail::SearchEngineMachine::callbackPending,
            "SearchEngineFsm AwaitingDrain state must arm rebuild callback when indexing drains");
    }

    if (cb) {
        cb(reason, includeVector);
    }
}

bool SearchEngineFsm::hasRebuildPending() const {
    std::lock_guard<std::mutex> lock(sharedMutex());
    return detail::SearchEngineMachine::snap.rebuildPending;
}

void SearchEngineFsm::syncAtomicState() {
    state_.store(detail::SearchEngineMachine::snap.state, std::memory_order_release);
}

} // namespace yams::daemon
