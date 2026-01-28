#pragma once

#include <mutex>
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
// Recovery events for production resilience
struct ModelReloadRequestedEvent {
    std::string modelName;
};
struct ProviderRecoveryEvent {
    std::string reason;
};

class EmbeddingProviderFsm {
public:
    ProviderSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snap_;
    }

    void dispatch(const ProviderAdoptedEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        transitionTo(EmbeddingProviderState::ProviderAdopted);
    }
    void dispatch(const ModelLoadStartedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.modelName = ev.modelName;
        transitionTo(EmbeddingProviderState::ModelLoading);
    }
    void dispatch(const ModelLoadedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.modelName = ev.modelName;
        snap_.embeddingDimension = ev.dimension;
        snap_.lastError.clear();
        transitionTo(EmbeddingProviderState::ModelReady);
    }
    void dispatch(const LoadFailureEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.lastError = ev.error;
        transitionTo(EmbeddingProviderState::Failed);
    }
    void dispatch(const ProviderDegradedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        snap_.lastError = ev.reason;
        transitionTo(EmbeddingProviderState::Degraded);
    }
    // Recovery events - allow transition from Failed/Degraded back to operational states
    void dispatch(const ModelReloadRequestedEvent& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        // Allow recovery from Failed or Degraded states
        if (snap_.state == EmbeddingProviderState::Failed ||
            snap_.state == EmbeddingProviderState::Degraded) {
            snap_.modelName = ev.modelName;
            snap_.lastError.clear();
            transitionTo(EmbeddingProviderState::ModelLoading);
        }
    }
    void dispatch(const ProviderRecoveryEvent&) {
        std::lock_guard<std::mutex> lock(mutex_);
        // Allow recovery from Failed or Degraded states
        if (snap_.state == EmbeddingProviderState::Failed ||
            snap_.state == EmbeddingProviderState::Degraded) {
            snap_.lastError.clear();
            transitionTo(EmbeddingProviderState::ProviderAdopted);
        }
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
    void transitionTo(EmbeddingProviderState next) {
        // Note: mutex_ already held by caller
        snap_.state = next;
    }

    ProviderSnapshot snap_{};
    mutable std::mutex mutex_;
};

} // namespace yams::daemon
