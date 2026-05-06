// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/onnx_model_pool.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#ifdef _WIN32
static int setenv(const char* name, const char* value, int /*overwrite*/) {
    return _putenv_s(name, value);
}
static void unsetenv(const char* name) {
    _putenv_s(name, "");
}
#endif

using yams::daemon::IModelProvider;
using yams::daemon::ModelPoolConfig;
using yams::daemon::ModelProviderFactoryRegistration;

namespace {

struct ScopedEmbedBackendAuto {
    std::string saved;
    bool hadValue{false};
    ScopedEmbedBackendAuto() {
        if (const char* p = std::getenv("YAMS_EMBED_BACKEND")) {
            saved = p;
            hadValue = true;
        }
        setenv("YAMS_EMBED_BACKEND", "auto", 1);
    }
    ~ScopedEmbedBackendAuto() {
        if (hadValue) {
            setenv("YAMS_EMBED_BACKEND", saved.c_str(), 1);
        } else {
            unsetenv("YAMS_EMBED_BACKEND");
        }
    }
};

class TaggedMockProvider : public IModelProvider {
public:
    explicit TaggedMockProvider(std::string tag) : tag_(std::move(tag)) {}
    const std::string& tag() const { return tag_; }

    yams::Result<std::vector<float>> generateEmbedding(const std::string&) override {
        return std::vector<float>{};
    }
    yams::Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>&) override {
        return std::vector<std::vector<float>>{};
    }
    yams::Result<std::vector<float>> generateEmbeddingFor(const std::string&,
                                                          const std::string&) override {
        return std::vector<float>{};
    }
    yams::Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string&, const std::vector<std::string>&) override {
        return std::vector<std::vector<float>>{};
    }

    yams::Result<void> loadModel(const std::string&) override { return {}; }
    yams::Result<void> unloadModel(const std::string&) override { return {}; }
    bool isModelLoaded(const std::string&) const override { return false; }
    std::vector<std::string> getLoadedModels() const override { return {}; }
    size_t getLoadedModelCount() const override { return 0; }

    yams::Result<yams::daemon::ModelInfo> getModelInfo(const std::string&) const override {
        return yams::daemon::ModelInfo{};
    }
    size_t getEmbeddingDim(const std::string&) const override { return 0; }
    std::shared_ptr<yams::vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string&) override {
        return nullptr;
    }

    std::string getProviderName() const override { return tag_; }
    std::string getProviderVersion() const override { return "0.0.0-test"; }
    bool isAvailable() const override { return true; }
    bool isTrainingFree() const override { return true; }

    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}

private:
    std::string tag_;
};

std::string makeUniqueName(const char* prefix) {
    static std::atomic<unsigned long> counter{0};
    return std::string(prefix) + "_" +
           std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

} // namespace

TEST_CASE("ModelProviderFactoryRegistration self-registers by name",
          "[daemon][model_provider][registry]") {
    const std::string name = makeUniqueName("registry_test_self_register");

    ModelProviderFactoryRegistration reg{name, [name]() -> std::unique_ptr<IModelProvider> {
                                             return std::make_unique<TaggedMockProvider>(name);
                                         }};

    auto providers = yams::daemon::getRegisteredProviders();
    REQUIRE(std::find(providers.begin(), providers.end(), name) != providers.end());
}

TEST_CASE("createModelProvider dispatches to preferred name when backend is auto",
          "[daemon][model_provider][registry]") {
    ScopedEmbedBackendAuto guard;
    const std::string name = makeUniqueName("registry_test_preferred");

    ModelProviderFactoryRegistration reg{name, [name]() -> std::unique_ptr<IModelProvider> {
                                             return std::make_unique<TaggedMockProvider>(name);
                                         }};

    ModelPoolConfig cfg;
    auto provider = yams::daemon::createModelProvider(cfg, name, /*forceMockProvider=*/false);
    REQUIRE(provider != nullptr);
    auto* tagged = dynamic_cast<TaggedMockProvider*>(provider.get());
    REQUIRE(tagged != nullptr);
    REQUIRE(tagged->tag() == name);
}

TEST_CASE("registerModelProvider: last registration wins for same name",
          "[daemon][model_provider][registry]") {
    ScopedEmbedBackendAuto guard;
    const std::string name = makeUniqueName("registry_test_reregister");

    yams::daemon::registerModelProvider(name, [name]() -> std::unique_ptr<IModelProvider> {
        return std::make_unique<TaggedMockProvider>(name + "/v1");
    });
    yams::daemon::registerModelProvider(name, [name]() -> std::unique_ptr<IModelProvider> {
        return std::make_unique<TaggedMockProvider>(name + "/v2");
    });

    ModelPoolConfig cfg;
    auto provider = yams::daemon::createModelProvider(cfg, name, /*forceMockProvider=*/false);
    REQUIRE(provider != nullptr);
    auto* tagged = dynamic_cast<TaggedMockProvider*>(provider.get());
    REQUIRE(tagged != nullptr);
    REQUIRE(tagged->tag() == name + "/v2");
}

TEST_CASE("createModelProvider with unknown name falls back to a provider",
          "[daemon][model_provider][registry]") {
    ScopedEmbedBackendAuto guard;
    const std::string name = makeUniqueName("registry_test_unknown_never_registered");

    ModelPoolConfig cfg;
    auto provider = yams::daemon::createModelProvider(cfg, name, /*forceMockProvider=*/false);
    REQUIRE(provider != nullptr);
    auto* tagged = dynamic_cast<TaggedMockProvider*>(provider.get());
    REQUIRE(tagged == nullptr);
}
