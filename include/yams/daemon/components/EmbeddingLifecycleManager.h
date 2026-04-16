#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <yams/core/types.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::search {
class IReranker;
}
namespace yams::daemon {

class EmbeddingService;
class IModelProvider;
struct DaemonConfig;

class EmbeddingLifecycleManager {
public:
    struct Dependencies {
        std::function<std::shared_ptr<IModelProvider>()> getModelProvider;
        std::function<std::shared_ptr<EmbeddingService>()> getEmbeddingService;
        const DaemonConfig* config{nullptr};
        const std::filesystem::path* dataDir{nullptr};
    };

    explicit EmbeddingLifecycleManager(Dependencies deps);

    Result<std::string>
    ensureModelReadySync(const std::string& requestedModel,
                         std::function<void(const ModelLoadEvent&)> progress = {},
                         int timeoutMs = 0, bool keepHot = true, bool warmup = true);

    [[nodiscard]] std::string resolvePreferredModel() const;
    [[nodiscard]] std::size_t getEmbeddingDimension() const;
    [[nodiscard]] bool detectPreloadFlag() const;

    [[nodiscard]] ProviderSnapshot fsmSnapshot() const;
    [[nodiscard]] bool isDegraded() const;
    [[nodiscard]] bool isLoadingOrReady() const;
    EmbeddingProviderFsm& fsm() { return fsm_; }
    const EmbeddingProviderFsm& fsm() const { return fsm_; }

    [[nodiscard]] const std::string& modelName() const { return modelName_; }
    void setModelName(const std::string& name) { modelName_ = name; }
    [[nodiscard]] const std::string& adoptedPluginName() const { return adoptedPluginName_; }
    void setAdoptedPluginName(const std::string& name) { adoptedPluginName_ = name; }
    [[nodiscard]] bool isAutoOnAdd() const { return autoOnAdd_; }
    void setAutoOnAdd(bool val) { autoOnAdd_ = val; }
    [[nodiscard]] bool preloadOnStartup() const { return preloadOnStartup_; }
    void setPreloadOnStartup(bool val) { preloadOnStartup_ = val; }

    [[nodiscard]] std::shared_ptr<yams::search::IReranker> reranker() const {
        return rerankerAdapter_;
    }
    void setReranker(std::shared_ptr<yams::search::IReranker> reranker) {
        rerankerAdapter_ = std::move(reranker);
    }

    [[nodiscard]] std::size_t inFlightJobs() const;
    [[nodiscard]] std::size_t queuedJobs() const;
    [[nodiscard]] std::size_t activeInferSubBatches() const;
    [[nodiscard]] std::uint64_t inferOldestMs() const;
    [[nodiscard]] std::uint64_t inferStartedCount() const;
    [[nodiscard]] std::uint64_t inferCompletedCount() const;
    [[nodiscard]] std::uint64_t inferLastMs() const;
    [[nodiscard]] std::uint64_t inferMaxMs() const;
    [[nodiscard]] std::uint64_t inferWarnCount() const;
    [[nodiscard]] std::uint64_t semanticEdgesCreated() const;
    [[nodiscard]] std::uint64_t semanticDocsProcessed() const;
    [[nodiscard]] std::uint64_t semanticUpdateErrors() const;

    void resetWarmupState();
    void resetReranker() { rerankerAdapter_.reset(); }

private:
    Dependencies deps_;

    EmbeddingProviderFsm fsm_{};
    mutable std::mutex readyMutex_;
    std::condition_variable readyCv_;
    bool readyActive_{false};
    std::string readyActiveModel_;
    std::unordered_set<std::string> warmedModels_;
    std::string hotModel_;

    std::string modelName_;
    std::string adoptedPluginName_;
    bool preloadOnStartup_{false};
    bool autoOnAdd_{false};

    std::shared_ptr<yams::search::IReranker> rerankerAdapter_;
};

} // namespace yams::daemon
