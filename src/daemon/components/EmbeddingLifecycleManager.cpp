#include <yams/daemon/components/EmbeddingLifecycleManager.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>

#include <spdlog/spdlog.h>

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/model_provider.h>

namespace {

int resolveEmbeddingLoadTimeoutMs(int requestedMs) {
    int timeoutMs = requestedMs > 0 ? requestedMs : 30000;
    if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
        try {
            timeoutMs = std::stoi(env);
        } catch (...) {
        }
    }
    return std::max(1000, timeoutMs);
}

std::string makeHotLoadOptions(bool hot) {
    return hot ? std::string{"{\"hot\":true}"} : std::string{"{\"hot\":false}"};
}

const std::vector<std::string>& embeddingWarmupTexts() {
    static const std::vector<std::string> texts = {"warmup text one", "warmup text two",
                                                   "warmup text three", "warmup text four"};
    return texts;
}

void emitModelLoadProgress(const std::function<void(const yams::daemon::ModelLoadEvent&)>& progress,
                           const std::string& modelName, std::string phase, std::string message) {
    if (!progress) {
        return;
    }
    yams::daemon::ModelLoadEvent ev;
    ev.modelName = modelName;
    ev.phase = std::move(phase);
    ev.message = std::move(message);
    progress(ev);
}

} // namespace

namespace yams::daemon {

EmbeddingLifecycleManager::EmbeddingLifecycleManager(Dependencies deps) : deps_(std::move(deps)) {}

Result<std::string>
EmbeddingLifecycleManager::ensureModelReadySync(const std::string& requestedModel,
                                                std::function<void(const ModelLoadEvent&)> progress,
                                                int timeoutMs, bool keepHot, bool warmup) {
    auto provider = deps_.getModelProvider();
    if (!provider || !provider->isAvailable()) {
        return Error{ErrorCode::InvalidState, "Model provider not available"};
    }

    std::string model = requestedModel;
    if (model.empty()) {
        try {
            model = resolvePreferredModel();
        } catch (...) {
        }
    }
    if (model.empty()) {
        model = modelName_;
    }
    if (model.empty()) {
        try {
            auto loaded = provider->getLoadedModels();
            if (!loaded.empty()) {
                model = loaded.front();
            }
        } catch (...) {
        }
    }
    if (model.empty()) {
        return Error{ErrorCode::NotFound, "No embedding model configured"};
    }

    const auto timeout = std::chrono::milliseconds(resolveEmbeddingLoadTimeoutMs(timeoutMs));
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    auto isReadyUnderLock = [&]() {
        bool loaded = false;
        try {
            loaded = provider->isModelLoaded(model);
        } catch (...) {
            loaded = false;
        }
        if (!loaded) {
            warmedModels_.erase(model);
            if (hotModel_ == model) {
                hotModel_.clear();
            }
            return false;
        }
        return !warmup || warmedModels_.contains(model);
    };

    {
        std::unique_lock lk(readyMutex_);
        while (readyActive_) {
            if (!readyCv_.wait_until(lk, deadline, [this]() { return !readyActive_; })) {
                return Error{ErrorCode::Timeout,
                             "Timed out waiting for another embedding model warmup"};
            }
        }

        if (isReadyUnderLock()) {
            if (keepHot) {
                hotModel_ = model;
            }
            return model;
        }

        readyActive_ = true;
        readyActiveModel_ = model;
    }

    struct ReadyGuard {
        EmbeddingLifecycleManager* self;
        ~ReadyGuard() {
            std::lock_guard lk(self->readyMutex_);
            self->readyActive_ = false;
            self->readyActiveModel_.clear();
            self->readyCv_.notify_all();
        }
    } readyGuard{this};

    const auto providerProgress = [progress, model](const ModelLoadEvent& ev) {
        if (!progress) {
            return;
        }
        if (!ev.modelName.empty() && ev.modelName != model) {
            return;
        }
        progress(ev);
    };
    provider->setProgressCallback(providerProgress);
    auto progressReset = std::unique_ptr<void, std::function<void(void*)>>(
        reinterpret_cast<void*>(1), [provider](void*) {
            try {
                provider->setProgressCallback({});
            } catch (...) {
            }
        });

    bool appliedHotDuringLoad = false;
    if (!provider->isModelLoaded(model)) {
        emitModelLoadProgress(progress, model, "loading", "Loading embedding model");
        Result<void> loadResult;
        if (keepHot) {
            loadResult = provider->loadModelWithOptions(model, makeHotLoadOptions(true));
            appliedHotDuringLoad = true;
        } else {
            loadResult = provider->loadModel(model);
        }
        if (!loadResult) {
            emitModelLoadProgress(progress, model, "failed",
                                  "Failed to load embedding model: " + loadResult.error().message);
            return loadResult.error();
        }
    }

    if (warmup) {
        bool needsWarm = false;
        {
            std::lock_guard lk(readyMutex_);
            needsWarm = !warmedModels_.contains(model);
        }
        if (needsWarm) {
            emitModelLoadProgress(progress, model, "warming", "Warming embedding model");
            auto warmupResult = provider->generateBatchEmbeddingsFor(model, embeddingWarmupTexts());
            if (!warmupResult) {
                emitModelLoadProgress(progress, model, "failed",
                                      "Embedding model warmup failed: " +
                                          warmupResult.error().message);
                return warmupResult.error();
            }
            std::lock_guard lk(readyMutex_);
            warmedModels_.insert(model);
        }
    }

    if (keepHot) {
        std::string previousHot;
        {
            std::lock_guard lk(readyMutex_);
            previousHot = hotModel_;
            hotModel_ = model;
        }
        if (!previousHot.empty() && previousHot != model && provider->isModelLoaded(previousHot)) {
            (void)provider->loadModelWithOptions(previousHot, makeHotLoadOptions(false));
        }
        if (!appliedHotDuringLoad && provider->isModelLoaded(model)) {
            (void)provider->loadModelWithOptions(model, makeHotLoadOptions(true));
        }
    }

    emitModelLoadProgress(progress, model, "completed", "Embedding model ready");
    return model;
}

std::string EmbeddingLifecycleManager::resolvePreferredModel() const {
    if (!deps_.config || !deps_.dataDir)
        return {};
    return ConfigResolver::resolvePreferredModel(*deps_.config, *deps_.dataDir);
}

std::size_t EmbeddingLifecycleManager::getEmbeddingDimension() const {
    auto modelProvider = deps_.getModelProvider();
    if (!modelProvider || !modelProvider->isAvailable())
        return 0;
    try {
        std::string mn = resolvePreferredModel();
        if (mn.empty())
            return 0;
        return modelProvider->getEmbeddingDim(mn);
    } catch (...) {
        return 0;
    }
}

bool EmbeddingLifecycleManager::detectPreloadFlag() const {
    if (!deps_.config)
        return false;
    return ConfigResolver::detectEmbeddingPreloadFlag(*deps_.config);
}

ProviderSnapshot EmbeddingLifecycleManager::fsmSnapshot() const {
    return fsm_.snapshot();
}

bool EmbeddingLifecycleManager::isDegraded() const {
    try {
        auto snap = fsm_.snapshot();
        return snap.state == EmbeddingProviderState::Degraded ||
               snap.state == EmbeddingProviderState::Failed;
    } catch (...) {
        return false;
    }
}

bool EmbeddingLifecycleManager::isLoadingOrReady() const {
    return fsm_.isLoadingOrReady();
}

std::size_t EmbeddingLifecycleManager::inFlightJobs() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inFlightJobs() : 0;
}

std::size_t EmbeddingLifecycleManager::queuedJobs() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->queuedJobs() : 0;
}

std::size_t EmbeddingLifecycleManager::activeInferSubBatches() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->activeInferSubBatches() : 0;
}

std::uint64_t EmbeddingLifecycleManager::inferOldestMs() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inferOldestActiveMs() : 0;
}

std::uint64_t EmbeddingLifecycleManager::inferStartedCount() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inferSubBatchStartedCount() : 0;
}

std::uint64_t EmbeddingLifecycleManager::inferCompletedCount() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inferSubBatchCompletedCount() : 0;
}

std::uint64_t EmbeddingLifecycleManager::inferLastMs() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inferSubBatchLastDurationMs() : 0;
}

std::uint64_t EmbeddingLifecycleManager::inferMaxMs() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inferSubBatchMaxDurationMs() : 0;
}

std::uint64_t EmbeddingLifecycleManager::inferWarnCount() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->inferSubBatchWarnCount() : 0;
}

std::uint64_t EmbeddingLifecycleManager::semanticEdgesCreated() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->semanticEdgesCreated() : 0;
}

std::uint64_t EmbeddingLifecycleManager::semanticDocsProcessed() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->semanticDocsProcessed() : 0;
}

std::uint64_t EmbeddingLifecycleManager::semanticUpdateErrors() const {
    auto svc = deps_.getEmbeddingService();
    return svc ? svc->semanticUpdateErrors() : 0;
}

void EmbeddingLifecycleManager::resetWarmupState() {
    std::lock_guard lk(readyMutex_);
    readyActive_ = false;
    readyActiveModel_.clear();
    warmedModels_.clear();
    hotModel_.clear();
    readyCv_.notify_all();
}

} // namespace yams::daemon
