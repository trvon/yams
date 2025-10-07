#pragma once

#include <string>

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

class EmbeddingProviderFsm {
public:
    ProviderSnapshot snapshot() const { return snap_; }

    void dispatch(const ProviderAdoptedEvent&) { /* plugin-level event */ }
    void dispatch(const ModelLoadStartedEvent& ev) {
        snap_.modelName = ev.modelName;
        transitionTo(EmbeddingProviderState::ModelLoading);
    }
    void dispatch(const ModelLoadedEvent& ev) {
        snap_.modelName = ev.modelName;
        snap_.embeddingDimension = ev.dimension;
        transitionTo(EmbeddingProviderState::ModelReady);
    }
    void dispatch(const LoadFailureEvent& ev) {
        snap_.lastError = ev.error;
        transitionTo(EmbeddingProviderState::Failed);
    }
    void dispatch(const ProviderDegradedEvent& ev) {
        snap_.lastError = ev.reason;
        transitionTo(EmbeddingProviderState::Degraded);
    }

    bool isReady() const { return snap_.state == EmbeddingProviderState::ModelReady; }
    bool isDegraded() const { return snap_.state == EmbeddingProviderState::Degraded; }
    std::size_t dimension() const { return snap_.embeddingDimension; }

private:
    void transitionTo(EmbeddingProviderState next) { snap_.state = next; }

    ProviderSnapshot snap_{};
};

} // namespace yams::daemon
