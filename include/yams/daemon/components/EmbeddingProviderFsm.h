#pragma once

#include <cstddef>
#include <mutex>
#include <string>

namespace yams::daemon {

enum class EmbeddingProviderState {
    Unavailable,
    ProviderAdopted,
    ModelLoading,
    ModelReady,
    Degraded
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
struct ProviderDegradedEvent {
    std::string reason;
};

// Per-instance state. This was a tinyfsm machine, whose state is static, so PluginManager and
// EmbeddingLifecycleManager shared one provider state and each one's construction reset the other.
// Transitions are unchanged: every event applies from every state.
class EmbeddingProviderFsm {
public:
    EmbeddingProviderFsm() = default;

    ProviderSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_;
    }

    template <typename E> void dispatch(const E& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        apply(ev);
    }

    bool isReady() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == EmbeddingProviderState::ModelReady;
    }

    bool isDegraded() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == EmbeddingProviderState::Degraded;
    }

    bool isLoadingOrReady() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.state == EmbeddingProviderState::ModelLoading ||
               snap_.state == EmbeddingProviderState::ModelReady;
    }

    std::size_t dimension() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_.embeddingDimension;
    }

private:
    void apply(const ProviderAdoptedEvent&) {
        snap_.state = EmbeddingProviderState::ProviderAdopted;
    }
    void apply(const ModelLoadStartedEvent& ev) {
        snap_.modelName = ev.modelName;
        snap_.state = EmbeddingProviderState::ModelLoading;
    }
    void apply(const ModelLoadedEvent& ev) {
        snap_.modelName = ev.modelName;
        snap_.embeddingDimension = ev.dimension;
        snap_.lastError.clear();
        snap_.state = EmbeddingProviderState::ModelReady;
    }
    void apply(const ProviderDegradedEvent& ev) {
        snap_.lastError = ev.reason;
        snap_.state = EmbeddingProviderState::Degraded;
    }

    mutable std::mutex mutex_;
    ProviderSnapshot snap_{};
};

} // namespace yams::daemon
