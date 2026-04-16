#pragma once

#include <mutex>
#include <string>

#include <tinyfsm.hpp>

namespace yams::daemon {

enum class EmbeddingProviderState {
    Unavailable,
    ProviderAdopted,
    ModelLoading,
    ModelReady,
    Degraded,
    Failed
};

struct ProviderSnapshot {
    EmbeddingProviderState state{EmbeddingProviderState::Unavailable};
    std::string modelName;
    std::string lastError;
    std::size_t embeddingDimension{0};
};

struct ProviderAdoptedEvent {
    std::string pluginName;
};
struct ModelLoadStartedEvent {
    std::string modelName;
};
struct ModelLoadedEvent {
    std::string modelName;
    std::size_t dimension;
};
struct LoadFailureEvent {
    std::string error;
};
struct ProviderDegradedEvent {
    std::string reason;
};
struct ModelReloadRequestedEvent {
    std::string modelName;
};
struct ProviderRecoveryEvent {
    std::string reason;
};

namespace detail {

struct EPUnavailable;
struct EPProviderAdopted;
struct EPModelLoading;
struct EPModelReady;
struct EPDegraded;
struct EPFailed;

struct EmbeddingProviderMachine : tinyfsm::MooreMachine<EmbeddingProviderMachine> {
    inline static ProviderSnapshot snap{};

    virtual void react(const ProviderAdoptedEvent&);
    virtual void react(const ModelLoadStartedEvent&);
    virtual void react(const ModelLoadedEvent&);
    virtual void react(const LoadFailureEvent&);
    virtual void react(const ProviderDegradedEvent&);
    virtual void react(const ModelReloadRequestedEvent&) {}
    virtual void react(const ProviderRecoveryEvent&) {}
};

struct EPUnavailable : EmbeddingProviderMachine {
    void entry() override { snap.state = EmbeddingProviderState::Unavailable; }
};

struct EPProviderAdopted : EmbeddingProviderMachine {
    void entry() override { snap.state = EmbeddingProviderState::ProviderAdopted; }
};

struct EPModelLoading : EmbeddingProviderMachine {
    void entry() override { snap.state = EmbeddingProviderState::ModelLoading; }
};

struct EPModelReady : EmbeddingProviderMachine {
    void entry() override { snap.state = EmbeddingProviderState::ModelReady; }
};

struct EPDegraded : EmbeddingProviderMachine {
    void entry() override { snap.state = EmbeddingProviderState::Degraded; }
    void react(const ModelReloadRequestedEvent& ev) override {
        snap.modelName = ev.modelName;
        snap.lastError.clear();
        transit<EPModelLoading>();
    }
    void react(const ProviderRecoveryEvent&) override {
        snap.lastError.clear();
        transit<EPProviderAdopted>();
    }
};

struct EPFailed : EmbeddingProviderMachine {
    void entry() override { snap.state = EmbeddingProviderState::Failed; }
    void react(const ModelReloadRequestedEvent& ev) override {
        snap.modelName = ev.modelName;
        snap.lastError.clear();
        transit<EPModelLoading>();
    }
    void react(const ProviderRecoveryEvent&) override {
        snap.lastError.clear();
        transit<EPProviderAdopted>();
    }
};

inline void EmbeddingProviderMachine::react(const ProviderAdoptedEvent&) {
    transit<EPProviderAdopted>();
}

inline void EmbeddingProviderMachine::react(const ModelLoadStartedEvent& ev) {
    snap.modelName = ev.modelName;
    transit<EPModelLoading>();
}

inline void EmbeddingProviderMachine::react(const ModelLoadedEvent& ev) {
    snap.modelName = ev.modelName;
    snap.embeddingDimension = ev.dimension;
    snap.lastError.clear();
    transit<EPModelReady>();
}

inline void EmbeddingProviderMachine::react(const LoadFailureEvent& ev) {
    snap.lastError = ev.error;
    transit<EPFailed>();
}

inline void EmbeddingProviderMachine::react(const ProviderDegradedEvent& ev) {
    snap.lastError = ev.reason;
    transit<EPDegraded>();
}

} // namespace detail
} // namespace yams::daemon

namespace tinyfsm {
template <> inline void Fsm<yams::daemon::detail::EmbeddingProviderMachine>::set_initial_state() {
    current_state_ptr = &_state_instance<yams::daemon::detail::EPUnavailable>::value;
}
} // namespace tinyfsm

namespace yams::daemon {

class EmbeddingProviderFsm {
public:
    EmbeddingProviderFsm() {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::EmbeddingProviderMachine::snap = {};
        detail::EmbeddingProviderMachine::start();
    }

    ProviderSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::EmbeddingProviderMachine::snap;
    }

    template <typename E> void dispatch(const E& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::EmbeddingProviderMachine::dispatch(ev);
    }

    bool isReady() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::EmbeddingProviderMachine::snap.state == EmbeddingProviderState::ModelReady;
    }

    bool isDegraded() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::EmbeddingProviderMachine::snap.state == EmbeddingProviderState::Degraded;
    }

    bool isLoadingOrReady() const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto s = detail::EmbeddingProviderMachine::snap.state;
        return s == EmbeddingProviderState::ModelLoading || s == EmbeddingProviderState::ModelReady;
    }

    std::size_t dimension() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::EmbeddingProviderMachine::snap.embeddingDimension;
    }

private:
    mutable std::mutex mutex_;
};

} // namespace yams::daemon
