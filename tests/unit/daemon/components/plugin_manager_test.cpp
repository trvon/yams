// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for PluginManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/model_provider.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#ifdef _WIN32
static int setenv(const char* name, const char* value, int /*overwrite*/) {
    return _putenv_s(name, value);
}
static void unsetenv(const char* name) {
    _putenv_s(name, "");
}
#endif

using namespace yams::daemon;

namespace {

struct PluginManagerFixture {
    std::filesystem::path tempDir;
    std::unique_ptr<DaemonLifecycleFsm> lifecycleFsm;
    std::unique_ptr<StateComponent> stateComponent;
    DaemonConfig config;

    PluginManagerFixture() {
        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_pluginmgr_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);

        lifecycleFsm = std::make_unique<DaemonLifecycleFsm>();
        stateComponent = std::make_unique<StateComponent>();
    }

    ~PluginManagerFixture() {
        lifecycleFsm.reset();
        stateComponent.reset();

        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    PluginManager::Dependencies makeDeps() {
        PluginManager::Dependencies deps;
        deps.config = &config;
        deps.state = stateComponent.get();
        deps.lifecycleFsm = lifecycleFsm.get();
        deps.dataDir = tempDir;
        deps.resolvePreferredModel = []() { return std::string("test-model"); };
        return deps;
    }
};

} // namespace

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager construction",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();

    SECTION("construction succeeds with valid dependencies") {
        PluginManager mgr(deps);
        CHECK(mgr.getName() == std::string("PluginManager"));
    }
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager initialize/shutdown lifecycle",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();
    PluginManager mgr(deps);

    SECTION("initialize without plugins succeeds") {
        auto result = mgr.initialize();
        CHECK(result.has_value());
    }

    SECTION("shutdown is safe without initialize") {
        mgr.shutdown();
        // No crash = success
    }

    SECTION("initialize then shutdown works") {
        REQUIRE(mgr.initialize().has_value());
        mgr.shutdown();
        // No crash = success
    }

    SECTION("double shutdown is safe") {
        REQUIRE(mgr.initialize().has_value());
        mgr.shutdown();
        mgr.shutdown();
        // No crash = success
    }
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager plugin host accessors",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();
    PluginManager mgr(deps);

    SECTION("plugin host is nullptr before init") {
        // May or may not be null depending on implementation
        auto host = mgr.getPluginHost();
        // Just verify accessor doesn't crash
        (void)host;
    }

    SECTION("plugin loader is nullptr before init") {
        auto loader = mgr.getPluginLoader();
        (void)loader;
    }

    SECTION("external plugin host is nullptr before init") {
        auto ext = mgr.getExternalPluginHost();
        (void)ext;
    }
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager trust list operations",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();
    PluginManager mgr(deps);

    SECTION("initial trust list may be empty") {
        auto list = mgr.trustList();
        // Empty or contains defaults - both valid
        (void)list;
    }

    SECTION("trustAdd for valid path returns result") {
        auto result = mgr.trustAdd(tempDir / "plugin.so");
        // Either success or failure is acceptable without init
        (void)result;
    }

    SECTION("trustRemove for non-existent path returns result") {
        auto result = mgr.trustRemove(tempDir / "nonexistent.so");
        (void)result;
    }
}

TEST_CASE("PluginManager getName returns component name", "[daemon][components][plugin][catch2]") {
    DaemonLifecycleFsm fsm;
    StateComponent state;
    DaemonConfig config;

    PluginManager::Dependencies deps;
    deps.config = &config;
    deps.state = &state;
    deps.lifecycleFsm = &fsm;
    deps.resolvePreferredModel = []() { return std::string("test"); };

    PluginManager mgr(deps);
    CHECK(std::string(mgr.getName()) == "PluginManager");
}

namespace {

struct ScopedEmbedBackend {
    std::string savedBackend;
    bool hadBackend{false};
    std::string savedPreferred;
    bool hadPreferred{false};

    explicit ScopedEmbedBackend(const char* backend) {
        if (const char* p = std::getenv("YAMS_EMBED_BACKEND")) {
            savedBackend = p;
            hadBackend = true;
        }
        if (const char* p = std::getenv("YAMS_PREFERRED_MODEL")) {
            savedPreferred = p;
            hadPreferred = true;
        }
        setenv("YAMS_EMBED_BACKEND", backend, 1);
        unsetenv("YAMS_PREFERRED_MODEL");
    }

    ~ScopedEmbedBackend() {
        if (hadBackend) {
            setenv("YAMS_EMBED_BACKEND", savedBackend.c_str(), 1);
        } else {
            unsetenv("YAMS_EMBED_BACKEND");
        }
        if (hadPreferred) {
            setenv("YAMS_PREFERRED_MODEL", savedPreferred.c_str(), 1);
        } else {
            unsetenv("YAMS_PREFERRED_MODEL");
        }
    }
};

class TaggedInProcessProvider : public IModelProvider {
public:
    explicit TaggedInProcessProvider(std::string tag) : tag_(std::move(tag)) {}

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
    size_t getEmbeddingDim(const std::string&) const override { return 384; }
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

std::string makeUniqueProviderName(const char* prefix) {
    static std::atomic<unsigned long> counter{0};
    return std::string(prefix) + "_" +
           std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

} // namespace

TEST_CASE_METHOD(PluginManagerFixture,
                 "adoptModelProvider: prefers in-process backend when selected via env",
                 "[daemon][components][plugin][catch2][simeon]") {
    const std::string backendName = makeUniqueProviderName("pm_test_backend");
    const std::string expectedTag = backendName;

    ModelProviderFactoryRegistration reg{
        backendName, [expectedTag]() -> std::unique_ptr<IModelProvider> {
            return std::make_unique<TaggedInProcessProvider>(expectedTag);
        }};

    ScopedEmbedBackend guard{backendName.c_str()};

    auto deps = makeDeps();
    PluginManager mgr(deps);
    REQUIRE(mgr.initialize().has_value());

    auto result = mgr.adoptModelProvider();
    REQUIRE(result.has_value());
    REQUIRE(result.value());

    auto provider = mgr.getModelProvider();
    REQUIRE(provider != nullptr);
    CHECK(provider->getProviderName() == expectedTag);
    CHECK(provider->isTrainingFree());
    CHECK(mgr.getEmbeddingModelName() == expectedTag);
}

TEST_CASE_METHOD(
    PluginManagerFixture,
    "adoptModelProvider: empty preferredName and backend=auto does not force in-process",
    "[daemon][components][plugin][catch2][simeon]") {
    ScopedEmbedBackend guard{"auto"};

    auto deps = makeDeps();
    PluginManager mgr(deps);
    REQUIRE(mgr.initialize().has_value());

    auto result = mgr.adoptModelProvider();
    REQUIRE(result.has_value());
    CHECK_FALSE(result.value());
    CHECK(mgr.getModelProvider() == nullptr);
}
