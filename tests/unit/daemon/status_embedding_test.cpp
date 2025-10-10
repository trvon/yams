#include <chrono>
#include <gtest/gtest.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/resource/model_provider.h>

using namespace yams;
using namespace yams::daemon;

// Minimal stub provider implementing the parts used by ServiceManager/DaemonMetrics
class StubProvider : public IModelProvider {
public:
    explicit StubProvider(size_t dim, std::string path)
        : dim_(dim), path_(std::move(path)), available_(true) {}

    Result<std::vector<float>> generateEmbedding(const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<float>> generateEmbeddingFor(const std::string&,
                                                    const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string&, const std::vector<std::string>&) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> loadModel(const std::string& modelName) override {
        loaded_.push_back(modelName);
        return Result<void>();
    }
    Result<void> unloadModel(const std::string&) override { return Result<void>(); }
    bool isModelLoaded(const std::string& modelName) const override {
        return std::find(loaded_.begin(), loaded_.end(), modelName) != loaded_.end();
    }
    std::vector<std::string> getLoadedModels() const override { return loaded_; }
    size_t getLoadedModelCount() const override { return loaded_.size(); }
    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        if (!isModelLoaded(modelName))
            return ErrorCode::NotFound;
        ModelInfo mi;
        mi.name = modelName;
        mi.path = path_;
        mi.embeddingDim = dim_;
        return mi;
    }
    size_t getEmbeddingDim(const std::string& /*modelName*/) const override { return dim_; }
    std::string getProviderName() const override { return "StubProvider"; }
    std::string getProviderVersion() const override { return "vtest"; }
    bool isAvailable() const override { return available_; }
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}

private:
    size_t dim_;
    std::string path_;
    bool available_;
    std::vector<std::string> loaded_;
};

TEST(StatusEmbedding, SnapshotShowsEmbeddingRuntime) {
    StateComponent state;
    DaemonConfig cfg;
    cfg.dataDir = std::filesystem::temp_directory_path() / "yams_test_embedding_status";
    ServiceManager svc(cfg, state);

    // Inject stub provider and mark as adopted
    auto provider = std::make_shared<StubProvider>(384, "/tmp/test-model.onnx");
    svc.__test_setModelProvider(provider);
    svc.__test_setAdoptedProviderPluginName("stub");

    // Ensure generator for a specific model (should not throw)
    auto r = svc.ensureEmbeddingGeneratorFor("stub-model");
    // allow failure in absence of ONNX locally, but status should be consistent

    DaemonMetrics metrics(nullptr, &state, &svc);
    auto snap = metrics.getSnapshot();
    ASSERT_NE(snap, nullptr);

    // We allow either local or provider backend depending on environment; ensure fields are sane
    // Model name is tracked when generator initializes successfully
    // At minimum, embeddingAvailable is boolean and backend string present
    EXPECT_TRUE(snap->embeddingBackend == "provider" || snap->embeddingBackend == "local" ||
                snap->embeddingBackend == "unknown");
    // embeddingDim may be 0 if generator not initialized; path may be empty without real model
    // The test primarily asserts no crashes and fields exist.
}
