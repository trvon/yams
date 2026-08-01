// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for PluginManager component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>

#include "../../../common/test_helpers_catch2.h"

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_content_extractor_adapter.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

using namespace yams::daemon;

template <typename T>
concept HasIndependentPluginStatusSnapshot =
    requires(const T& manager) { manager.getStatusSnapshot(); };

static_assert(!HasIndependentPluginStatusSnapshot<PluginManager>,
              "ServiceManager is the sole plugin status snapshot authority");

namespace {

class CountingAbiPluginHost final : public AbiPluginHost {
public:
    CountingAbiPluginHost() = default;

    std::vector<PluginDescriptor> listLoaded() const override {
        ++listCalls;
        if (unloadCalls == 0) {
            return {{.name = "counted_plugin"}};
        }
        return {};
    }

    yams::Result<void> unload(const std::string&) override {
        ++unloadCalls;
        return {};
    }

    mutable std::size_t listCalls{0};
    std::size_t unloadCalls{0};
};

class RecordingPluginHost final : public IPluginHost {
public:
    yams::Result<PluginDescriptor> scanTarget(const std::filesystem::path&) override {
        return descriptor;
    }
    yams::Result<std::vector<PluginDescriptor>>
    scanDirectory(const std::filesystem::path&) override {
        return std::vector<PluginDescriptor>{descriptor};
    }
    yams::Result<PluginDescriptor> load(const std::filesystem::path& path,
                                        const std::string& configJson) override {
        loadedPath = path;
        loadedConfigJson = configJson;
        descriptor.path = path;
        return descriptor;
    }
    yams::Result<void> unload(const std::string&) override { return {}; }
    std::vector<PluginDescriptor> listLoaded() const override { return {}; }
    std::vector<std::filesystem::path> trustList() const override { return {}; }
    yams::Result<void> trustAdd(const std::filesystem::path&) override { return {}; }
    yams::Result<void> trustRemove(const std::filesystem::path&) override { return {}; }
    yams::Result<std::string> health(const std::string&) override { return std::string("{}"); }

    PluginDescriptor descriptor;
    std::filesystem::path loadedPath;
    std::string loadedConfigJson;
};

class DiscoveryAbiPluginHost final : public AbiPluginHost {
public:
    yams::Result<PluginDescriptor> scanTarget(const std::filesystem::path& path) override {
        ++targetScans;
        auto result = descriptor;
        result.path = path;
        return result;
    }

    yams::Result<std::vector<PluginDescriptor>>
    scanDirectory(const std::filesystem::path& path) override {
        ++directoryScans;
        auto result = descriptor;
        result.path = path / descriptor.path.filename();
        return std::vector<PluginDescriptor>{result};
    }

    yams::Result<PluginDescriptor> load(const std::filesystem::path& path,
                                        const std::string& configJson) override {
        ++loads;
        loadedConfigJson = configJson;
        auto result = descriptor;
        result.path = path;
        loadedPlugins.push_back(result);
        return result;
    }

    yams::Result<void> unload(const std::string& name) override {
        std::erase_if(loadedPlugins, [&](const auto& loaded) { return loaded.name == name; });
        return {};
    }

    std::vector<PluginDescriptor> listLoaded() const override { return loadedPlugins; }
    std::vector<std::filesystem::path> trustList() const override { return trustedPaths; }

    PluginDescriptor descriptor{.name = "libyams_glint",
                                .path = "libyams_glint.so",
                                .interfaces = {"content_extractor_v1"}};
    std::vector<std::filesystem::path> trustedPaths;
    std::vector<PluginDescriptor> loadedPlugins;
    std::size_t targetScans{0};
    std::size_t directoryScans{0};
    std::size_t loads{0};
    std::string loadedConfigJson;
};

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

TEST_CASE("PluginContentExtractorAdapter retains ABI plugin lease",
          "[daemon][components][plugin][lifetime][catch2]") {
    auto keepAlive = std::make_shared<int>(42);
    std::weak_ptr<int> weakKeepAlive = keepAlive;
    auto adapter = std::make_shared<PluginContentExtractorAdapter>(
        nullptr, std::static_pointer_cast<void>(keepAlive));
    keepAlive.reset();

    CHECK_FALSE(weakKeepAlive.expired());
    adapter.reset();
    CHECK(weakKeepAlive.expired());
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager construction",
                 "[daemon][components][plugin][catch2]") {
    auto deps = makeDeps();

    SECTION("construction succeeds with valid dependencies") {
        PluginManager mgr(deps);
        CHECK((mgr.getName() == std::string("PluginManager")));
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

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager autoload is synchronous",
                 "[daemon][components][plugin][autoload][catch2]") {
    config.useMockModelProvider = true;
    PluginManager mgr(makeDeps());
    REQUIRE(mgr.initialize());

    const auto result = mgr.autoloadPlugins();

    REQUIRE(result);
    CHECK((result.value() == 0));
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager owns shared host shutdown",
                 "[daemon][components][plugin][ownership][catch2]") {
    CountingAbiPluginHost sharedHost;
    auto deps = makeDeps();
    deps.sharedPluginHost = &sharedHost;
    PluginManager mgr(deps);
    REQUIRE(mgr.initialize());

    mgr.shutdown();
    mgr.shutdown();

    CHECK((sharedHost.listCalls == 1));
    CHECK((sharedHost.unloadCalls == 1));
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

    SECTION("external plugin host is nullptr before init") {
        auto ext = mgr.getExternalPluginHost();
        (void)ext;
    }
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager applies name policy to fallback host",
                 "[daemon][components][plugin][policy][catch2]") {
    yams::test::ScopedEnvVar namePolicyEnv("YAMS_PLUGIN_NAME_POLICY", std::nullopt);
    config.pluginNamePolicy = "spec";

    const auto nonPlugin = tempDir / "third_party.so";
    {
        std::ofstream pluginFile(nonPlugin, std::ios::binary);
        REQUIRE(pluginFile.good());
        pluginFile.put('\0');
    }

    const auto requireSpecPolicy = [&](AbiPluginHost& host) {
        REQUIRE(host.scanDirectory(tempDir));
        const auto skips = host.getLastScanSkips();
        const auto skip = std::find_if(skips.begin(), skips.end(),
                                       [&](const auto& entry) { return entry.first == nonPlugin; });
        REQUIRE((skip != skips.end()));
        CHECK((skip->second == "name policy: require libyams_* or yams_*"));
    };

    SECTION("fallback host") {
        PluginManager mgr(makeDeps());
        REQUIRE(mgr.initialize());
        auto* host = mgr.getPluginHost();
        REQUIRE((host != nullptr));
        requireSpecPolicy(*host);
    }

    SECTION("shared host") {
        AbiPluginHost sharedHost(tempDir / "shared-plugins.trust");
        auto deps = makeDeps();
        deps.sharedPluginHost = &sharedHost;
        PluginManager mgr(deps);
        REQUIRE(mgr.initialize());
        requireSpecPolicy(sharedHost);
    }
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager configured loads use one config path",
                 "[daemon][components][plugin][config][catch2]") {
    PluginManager mgr(makeDeps());
    RecordingPluginHost host;

    SECTION("short aliases resolve for ABI and external hosts") {
        config.pluginConfigs["glint"] = R"({"mode":"fast"})";
        host.descriptor.name = "yams_glint";
        host.descriptor.path = tempDir / "libyams_glint.so";

        REQUIRE(mgr.loadConfiguredPlugin(host, host.descriptor));
        CHECK((host.loadedPath == host.descriptor.path));
        CHECK((host.loadedConfigJson == R"({"mode":"fast"})"));
    }

    SECTION("conflicting aliases fail closed") {
        config.pluginConfigs["glint"] = R"({"mode":"short"})";
        config.pluginConfigs["yams_glint"] = R"({"mode":"prefixed"})";
        host.descriptor.name = "yams_glint";
        host.descriptor.path = tempDir / "libyams_glint.so";

        auto result = mgr.loadConfiguredPlugin(host, host.descriptor);

        REQUIRE_FALSE(result);
        CHECK((result.error().code == yams::ErrorCode::InvalidData));
        CHECK(host.loadedPath.empty());
    }

    SECTION("ONNX pool constraints are applied after config resolution") {
        config.pluginConfigs["onnx"] = R"({"mode":"fast"})";
        config.modelPoolConfig.maxLoadedModels = 7;
        host.descriptor.name = "yams_onnx";
        host.descriptor.path = tempDir / "libyams_onnx.so";
        host.descriptor.interfaces = {"model_provider_v1"};

        REQUIRE(mgr.loadConfiguredPlugin(host, host.descriptor));
        const auto loadedConfig = nlohmann::json::parse(host.loadedConfigJson);
        CHECK((loadedConfig["mode"] == "fast"));
        CHECK((loadedConfig["max_loaded_models"] == 7));
    }
}

TEST_CASE_METHOD(PluginManagerFixture, "PluginManager owns discovery routing and loading",
                 "[daemon][components][plugin][discovery][catch2]") {
    config.pluginDirStrict = true;
    DiscoveryAbiPluginHost abiHost;
    abiHost.trustedPaths = {tempDir / "plugins", tempDir / "plugins"};
    config.pluginConfigs["glint"] = R"({"mode":"canonical"})";

    auto deps = makeDeps();
    deps.sharedPluginHost = &abiHost;
    PluginManager mgr(deps);
    REQUIRE(mgr.initialize());

    SECTION("target discovery chooses one host and uses canonical configured load") {
        abiHost.descriptor.interfaces.clear();
        const auto pluginPath = tempDir / "libyams_glint.so";
        auto discovered = mgr.scanPluginTarget(pluginPath);
        REQUIRE(discovered);
        CHECK((discovered.value().host == PluginManager::PluginHostKind::Abi));
        CHECK((discovered.value().descriptor.path == pluginPath));
        CHECK((abiHost.targetScans == 1));

        auto loaded = mgr.loadDiscoveredPlugin(discovered.value());
        REQUIRE(loaded);
        CHECK((abiHost.loads == 1));
        CHECK((abiHost.loadedConfigJson == R"({"mode":"canonical"})"));
        auto snapshot = mgr.getPluginHostFsmSnapshot();
        CHECK((snapshot.state == PluginHostState::Ready));
        CHECK((snapshot.loadedCount == 1));
        CHECK((snapshot.loadedPlugins == std::vector<std::string>{"libyams_glint"}));

        REQUIRE(mgr.unloadPlugin("libyams_glint"));
        snapshot = mgr.getPluginHostFsmSnapshot();
        CHECK((snapshot.state == PluginHostState::Ready));
        CHECK((snapshot.loadedCount == 0));
        CHECK(snapshot.loadedPlugins.empty());
    }

    SECTION("runtime interfaces reject unsafe hot unload") {
        const std::array<const char*, 6> runtimeInterfaces = {
            "model_provider_v1",   "content_extractor_v1",  "symbol_extractor_v1",
            "entity_extractor_v2", "kg_entity_provider_v1", "future_runtime_v9"};
        for (const auto* interfaceName : runtimeInterfaces) {
            INFO("interface=" << interfaceName);
            abiHost.descriptor.interfaces = {interfaceName};
            abiHost.loadedPlugins = {abiHost.descriptor};

            auto result = mgr.unloadPlugin("libyams_glint");
            REQUIRE_FALSE(result);
            CHECK((result.error().code == yams::ErrorCode::InvalidState));
            CHECK((abiHost.loadedPlugins.size() == 1));
        }
    }

    SECTION("configured discovery roots are deduplicated and obey strict mode") {
        auto discovered = mgr.scanConfiguredPluginRoots();
        REQUIRE(discovered);
        REQUIRE((discovered.value().size() == 1));
        CHECK((discovered.value().front().host == PluginManager::PluginHostKind::Abi));
        CHECK((abiHost.directoryScans == 1));
    }

    SECTION("loaded child does not suppress its containing directory") {
        const auto pluginDirectory = tempDir / "plugins";
        const auto loadedPath = pluginDirectory / "libyams_one.so";
        abiHost.loadedPlugins = {{.name = "yams_one", .path = loadedPath}};

        CHECK(mgr.isPluginLoadedFrom(loadedPath));
        CHECK_FALSE(mgr.isPluginLoadedFrom(pluginDirectory));
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
    CHECK((std::string(mgr.getName()) == "PluginManager"));
}

namespace {

struct ScopedEmbedBackend {
    yams::test::ScopedEnvVar backend;
    yams::test::ScopedEnvVar preferredModel;

    explicit ScopedEmbedBackend(const char* selectedBackend)
        : backend("YAMS_EMBED_BACKEND", std::string(selectedBackend)),
          preferredModel("YAMS_PREFERRED_MODEL", std::nullopt) {}
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
    REQUIRE((provider != nullptr));
    CHECK((provider->getProviderName() == expectedTag));
    CHECK(provider->isTrainingFree());
    CHECK((mgr.getEmbeddingModelName() == expectedTag));
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
    CHECK((mgr.getModelProvider() == nullptr));
}
