// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/config/config_helpers.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#define YAMS_DAEMON_TEST_HOOKS_IMPL 1
#include <yams/daemon/components/PluginManager.h>
#undef YAMS_DAEMON_TEST_HOOKS_IMPL
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/daemon/resource/external_entity_provider_adapter.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_content_extractor_adapter.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/plugins/content_extractor_v1.h>
#include <yams/plugins/entity_extractor_v2.h>
#include <yams/plugins/model_provider_v1.h>
#include <yams/plugins/symbol_extractor_v1.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <type_traits>
#include <unordered_set>
#include <utility>

namespace yams::daemon {
namespace {

std::optional<std::string> getenvCopy(const char* name) {
    auto value = yams::config::getenv_copy(name);
    if (value.empty()) {
        return std::nullopt;
    }
    return value;
}

void writeShutdownFailure(const char* operation, const char* name = nullptr,
                          const char* message = nullptr) noexcept {
    std::fputs("PluginManager ", stderr);
    std::fputs(operation, stderr);
    if (name) {
        std::fputs(" for ", stderr);
        std::fputs(name, stderr);
    }
    if (message) {
        std::fputs(": ", stderr);
        std::fputs(message, stderr);
    }
    std::fputc('\n', stderr);
}

void appendPluginNameVariant(std::vector<std::string>& variants, const std::string& variant) {
    if (!variant.empty() &&
        std::find(variants.begin(), variants.end(), variant) == variants.end()) {
        variants.push_back(variant);
    }
}

std::vector<std::string> pluginNameVariants(const std::string& name) {
    std::vector<std::string> variants;
    appendPluginNameVariant(variants, name);
    if (name.rfind("libyams_", 0) == 0 && name.size() > 8) {
        appendPluginNameVariant(variants, name.substr(8));
        appendPluginNameVariant(variants, name.substr(3));
    } else if (name.rfind("yams_", 0) == 0 && name.size() > 5) {
        appendPluginNameVariant(variants, name.substr(5));
    }
    return variants;
}

std::string pluginDiscoveryKey(const PluginDescriptor& descriptor) {
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(descriptor.path, ec);
    if (ec) {
        canonical = descriptor.path.lexically_normal();
    }
    return canonical.string() + "\n" + descriptor.name;
}

std::string normalizedExtension(const std::filesystem::path& target) {
    auto extension = target.extension().string();
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return extension;
}

bool isExternalPluginTarget(const std::filesystem::path& target) {
    const auto extension = normalizedExtension(target);
    if (extension == ".py" || extension == ".js") {
        return true;
    }
    std::error_code ec;
    if (std::filesystem::is_directory(target, ec)) {
        return std::filesystem::exists(target / "yams-plugin.json", ec);
    }
    return target.has_parent_path() &&
           std::filesystem::exists(target.parent_path() / "yams-plugin.json", ec);
}

bool isAbiPluginTarget(const std::filesystem::path& target) {
    const auto extension = normalizedExtension(target);
    return extension == ".so" || extension == ".dylib" || extension == ".dll";
}

bool exposesRuntimeInterface(const PluginDescriptor& descriptor) {
    // PluginManager cannot prove that an unknown interface has no live consumer. Fail closed for
    // every declared interface so future ABI additions cannot bypass unload safety.
    return !descriptor.interfaces.empty();
}

} // namespace

std::string adjustOnnxConfigJson(const std::string& configJson, std::size_t defaultMax) {
    nlohmann::json cfgJson = nlohmann::json::object();
    if (!configJson.empty()) {
        auto parsed = nlohmann::json::parse(configJson, nullptr, false);
        if (!parsed.is_discarded() && parsed.is_object()) {
            cfgJson = std::exchange(parsed, nlohmann::json{});
        }
    }

    std::size_t configuredMax = 0;
    if (cfgJson.contains("max_loaded_models") && cfgJson["max_loaded_models"].is_number_integer()) {
        configuredMax = static_cast<std::size_t>(cfgJson["max_loaded_models"].get<int>());
    }

    constexpr std::size_t kMinRequiredModels = 3; // embedding + GLiNER + reranker
    std::size_t desiredMax = std::max({configuredMax, defaultMax, kMinRequiredModels});
    cfgJson["max_loaded_models"] = desiredMax;

    return cfgJson.dump();
}

// Template-based plugin adoption helper
template <typename AbiTableType, typename AdapterType, typename ContainerValueType>
static size_t
adoptPluginInterfaceImpl(AbiPluginHost* host, const std::string& interfaceName,
                         uint32_t interfaceVersion,
                         std::vector<std::shared_ptr<ContainerValueType>>& targetContainer,
                         const std::function<bool(const AbiTableType*)>& validateTable = nullptr) {
    size_t adopted = 0;
    if (!host)
        return adopted;

    for (const auto& descriptor : host->listLoaded()) {
        bool hasInterface = false;
        for (const auto& id : descriptor.interfaces) {
            if (id == interfaceName) {
                hasInterface = true;
                break;
            }
        }
        if (!hasInterface)
            continue;

        auto lease = host->acquireInterface(descriptor.name, interfaceName, interfaceVersion);
        if (!lease)
            continue;

        auto* table = reinterpret_cast<AbiTableType*>(lease.value().interface);
        if (!table)
            continue;

        if (validateTable && !validateTable(table))
            continue;

        try {
            std::shared_ptr<AdapterType> adapter;
            if constexpr (std::is_constructible_v<AdapterType, AbiTableType*,
                                                  std::shared_ptr<void>>) {
                adapter = std::make_shared<AdapterType>(table, lease.value().keepAlive);
            } else {
                adapter = std::make_shared<AdapterType>(table);
            }
            targetContainer.push_back(adapter);
            ++adopted;
            spdlog::info("Adopted {} from plugin: {}", interfaceName, descriptor.name);
        } catch (const std::exception& e) {
            spdlog::warn("Failed to create adapter for {} from plugin {}: {}", interfaceName,
                         descriptor.name, e.what());
        }
    }
    return adopted;
}

PluginManager::PluginManager(Dependencies deps) {
    std::swap(deps_, deps);
}

PluginManager::~PluginManager() {
    shutdown();
}

Result<void> PluginManager::initialize() {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    spdlog::debug("[PluginManager] Initializing");

    // Use shared plugin host if provided, otherwise create our own with retry
    if (deps_.sharedPluginHost) {
        sharedPluginHost_ = deps_.sharedPluginHost;
        spdlog::info("[PluginManager] Using shared plugin host from ServiceManager");
    } else {
        std::filesystem::path trustFile = deps_.dataDir.empty()
                                              ? yams::config::get_daemon_plugin_trust_file()
                                              : (deps_.dataDir / "plugins.trust");
        // Retry trust verification up to 3 times with exponential backoff
        bool trustOk = false;
        for (int attempt = 0; attempt < 3; ++attempt) {
            try {
                pluginHost_ = std::make_unique<AbiPluginHost>(trustFile);
                spdlog::info("[PluginManager] AbiPluginHost initialized with trust file: {}",
                             trustFile.string());
                trustOk = true;
                break;
            } catch (const std::exception& e) {
                spdlog::warn("[PluginManager] Trust verification attempt {} failed: {}",
                             attempt + 1, e.what());
                if (attempt == 2) {
                    spdlog::error("[PluginManager] Trust verification failed after 3 attempts");
                    // Continue without plugins - graceful degradation
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
            }
        }
        if (!trustOk) {
            spdlog::warn("[PluginManager] Continuing without ABI plugin host due to trust failure");
        }
    }

    // Initialize ExternalPluginHost for Python/JS plugins with retry
    bool externalTrustOk = false;
    for (int attempt = 0; attempt < 3; ++attempt) {
        try {
            std::filesystem::path externalTrustFile =
                deps_.dataDir.empty() ? yams::config::get_daemon_plugin_trust_file()
                                      : (deps_.dataDir / "plugins.trust");
            ExternalPluginHostConfig externalConfig;
            externalHost_ =
                std::make_unique<ExternalPluginHost>(nullptr, externalTrustFile, externalConfig);
            spdlog::info("[PluginManager] ExternalPluginHost initialized (unified trust file: {})",
                         externalTrustFile.string());
            externalTrustOk = true;
            break;
        } catch (const std::exception& e) {
            spdlog::warn("[PluginManager] External trust verification attempt {} failed: {}",
                         attempt + 1, e.what());
            if (attempt == 2) {
                spdlog::warn("[PluginManager] External trust verification failed after 3 attempts");
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
        }
    }
    if (!externalTrustOk) {
        spdlog::warn("[PluginManager] External plugins not available due to trust failure");
    }

    // Configure name policy
    if (deps_.config) {
        std::string policy = deps_.config->pluginNamePolicy;
        if (const auto env = getenvCopy("YAMS_PLUGIN_NAME_POLICY"))
            policy = *env;
        for (auto& c : policy)
            c = static_cast<char>(std::tolower(c));
        if (auto* host = getActivePluginHost()) {
            host->setNamePolicy(policy == "spec" ? AbiPluginHost::NamePolicy::Spec
                                                 : AbiPluginHost::NamePolicy::Relaxed);
        }
    }

    // Treat a host with zero plugins as a valid steady state. Autoload can still
    // transition Ready -> Scanning/Loading -> Ready when invoked.
    pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});

    return Result<void>{};
}

void PluginManager::shutdown() noexcept {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    if (shutdownInvoked_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    try {
        spdlog::debug("[PluginManager] Shutting down");
    } catch (...) {
        std::fputs("PluginManager shutdown logging failed\n", stderr);
    }

    // Release adopted interfaces before destroying or unloading either host. Runtime hot-unload
    // rejects interface-bearing plugins because other daemon components can hold shared adapter
    // copies; shutdown is the quiesced path that safely releases them.
    modelProvider_.reset();
    contentExtractors_.clear();
    symbolExtractors_.clear();
    entityExtractors_.clear();
    entityProviders_.clear();
    externalHost_.reset();

    try {
        if (auto* host = getActivePluginHost()) {
            for (const auto& descriptor : host->listLoaded()) {
                try {
                    auto result = host->unload(descriptor.name);
                    if (!result) {
                        writeShutdownFailure("unload failed", descriptor.name.c_str(),
                                             result.error().message.c_str());
                    }
                } catch (const std::exception& e) {
                    writeShutdownFailure("unload failed", descriptor.name.c_str(), e.what());
                } catch (...) {
                    writeShutdownFailure("unload failed", descriptor.name.c_str());
                }
            }
        }
    } catch (const std::exception& e) {
        writeShutdownFailure("could not enumerate plugins during shutdown", nullptr, e.what());
    } catch (...) {
        std::fputs("PluginManager could not enumerate plugins during shutdown\n", stderr);
    }

    pluginHost_.reset();
}

std::vector<std::filesystem::path> PluginManager::trustList() const {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    std::vector<std::filesystem::path> paths;
    if (getActivePluginHost()) {
        auto abi = getActivePluginHost()->trustList();
        paths.insert(paths.end(), abi.begin(), abi.end());
    }
    if (externalHost_) {
        auto ext = externalHost_->trustList();
        paths.insert(paths.end(), ext.begin(), ext.end());
    }
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
    return paths;
}

Result<void> PluginManager::trustAdd(const std::filesystem::path& path) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    Result<void> lastErr{Error{ErrorCode::InvalidState, "Plugin host not initialized"}};
    bool any = false;

    if (getActivePluginHost()) {
        auto r = getActivePluginHost()->trustAdd(path);
        if (r) {
            any = true;
        } else {
            lastErr = r;
        }
    }

    if (externalHost_) {
        auto r = externalHost_->trustAdd(path);
        if (r) {
            any = true;
        } else {
            lastErr = r;
        }
    }

    if (any)
        return Result<void>();
    return lastErr;
}

Result<void> PluginManager::trustRemove(const std::filesystem::path& path) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    Result<void> lastErr{Error{ErrorCode::InvalidState, "Plugin host not initialized"}};
    bool any = false;

    if (getActivePluginHost()) {
        auto r = getActivePluginHost()->trustRemove(path);
        if (r) {
            any = true;
        } else {
            lastErr = r;
        }
    }

    if (externalHost_) {
        auto r = externalHost_->trustRemove(path);
        if (r) {
            any = true;
        } else {
            lastErr = r;
        }
    }

    if (any)
        return Result<void>();
    return lastErr;
}

Result<PluginDescriptor> PluginManager::loadConfiguredPlugin(IPluginHost& host,
                                                             const PluginDescriptor& descriptor,
                                                             std::string explicitConfigJson) const {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    std::string configJson = std::exchange(explicitConfigJson, {});
    std::string configKey;

    if (configJson.empty() && deps_.config) {
        for (const auto& variant : pluginNameVariants(descriptor.name)) {
            const auto it = deps_.config->pluginConfigs.find(variant);
            if (it == deps_.config->pluginConfigs.end()) {
                continue;
            }
            if (configKey.empty()) {
                configJson = it->second;
                configKey = variant;
                continue;
            }
            if (it->second != configJson) {
                return Error{ErrorCode::InvalidData,
                             "Conflicting plugin configuration aliases for: " + descriptor.name};
            }
        }
    }

    if (!configKey.empty()) {
        spdlog::info("[PluginManager] applying configured settings to plugin '{}' (key '{}')",
                     descriptor.name, configKey);
    }

    const bool isModelProvider =
        std::find(descriptor.interfaces.begin(), descriptor.interfaces.end(),
                  "model_provider_v1") != descriptor.interfaces.end();
    const bool isOnnxPlugin = descriptor.name.find("onnx") != std::string::npos;
    if (isModelProvider && isOnnxPlugin) {
        const std::size_t defaultMax =
            deps_.config ? deps_.config->modelPoolConfig.maxLoadedModels : 0;
        configJson = adjustOnnxConfigJson(configJson, defaultMax);
        spdlog::info("[PluginManager] enforcing ONNX model pool max_loaded_models");
    }

    return host.load(descriptor.path, configJson);
}

IPluginHost* PluginManager::getHost(PluginHostKind kind) const {
    if (kind == PluginHostKind::Abi) {
        return getActivePluginHost();
    }
    return externalHost_.get();
}

std::vector<std::pair<PluginManager::PluginHostKind, std::filesystem::path>>
PluginManager::pluginScanRoots() const {
    using Root = std::pair<PluginHostKind, std::filesystem::path>;
    auto appendRoot = [](std::vector<Root>& roots, PluginHostKind kind,
                         const std::filesystem::path& path) {
        if (path.empty()) {
            return;
        }
        const auto duplicate = std::find_if(roots.begin(), roots.end(), [&](const Root& root) {
            return root.first == kind && root.second == path;
        });
        if (duplicate == roots.end()) {
            roots.emplace_back(kind, path);
        }
    };

    std::vector<Root> trustedRoots;
    for (const auto kind : {PluginHostKind::Abi, PluginHostKind::External}) {
        if (const auto* host = getHost(kind)) {
            for (const auto& path : host->trustList()) {
                appendRoot(trustedRoots, kind, path);
            }
        }
    }

    const bool strictMode = deps_.config ? deps_.config->pluginDirStrict : false;
    if (strictMode) {
        return trustedRoots;
    }

    std::vector<Root> roots;

    namespace fs = std::filesystem;
#ifdef _WIN32
    std::optional<fs::path> userBase;
    if (const auto localAppData = getenvCopy("LOCALAPPDATA")) {
        userBase = fs::path(*localAppData) / "yams";
    } else if (const auto userProfile = getenvCopy("USERPROFILE")) {
        userBase = fs::path(*userProfile) / "AppData" / "Local" / "yams";
    }
    if (userBase) {
        appendRoot(roots, PluginHostKind::Abi, *userBase / "plugins");
        appendRoot(roots, PluginHostKind::External, *userBase / "external-plugins");
    }
    if (const auto programFiles = getenvCopy("ProgramFiles")) {
        const auto installBase = fs::path(*programFiles) / "YAMS" / "lib" / "yams";
        appendRoot(roots, PluginHostKind::Abi, installBase / "plugins");
        appendRoot(roots, PluginHostKind::External, installBase / "external-plugins");
    }
#else
    if (const auto home = getenvCopy("HOME")) {
        const auto userBase = fs::path(*home) / ".local" / "lib" / "yams";
        appendRoot(roots, PluginHostKind::Abi, userBase / "plugins");
        appendRoot(roots, PluginHostKind::External, userBase / "external-plugins");
    }
#ifdef __APPLE__
    appendRoot(roots, PluginHostKind::Abi, "/opt/homebrew/lib/yams/plugins");
    appendRoot(roots, PluginHostKind::External, "/opt/homebrew/lib/yams/external-plugins");
#endif
    appendRoot(roots, PluginHostKind::Abi, "/usr/local/lib/yams/plugins");
    appendRoot(roots, PluginHostKind::External, "/usr/local/lib/yams/external-plugins");
    appendRoot(roots, PluginHostKind::Abi, "/usr/lib/yams/plugins");
    appendRoot(roots, PluginHostKind::External, "/usr/lib/yams/external-plugins");
#endif
#ifdef YAMS_INSTALL_PREFIX
    const auto installBase = fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams";
    appendRoot(roots, PluginHostKind::Abi, installBase / "plugins");
    appendRoot(roots, PluginHostKind::External, installBase / "external-plugins");
#endif
    for (const auto& [kind, path] : trustedRoots) {
        appendRoot(roots, kind, path);
    }
    return roots;
}

Result<PluginManager::DiscoveredPlugin>
PluginManager::scanPluginTarget(const std::filesystem::path& target) const {
    if (!getHost(PluginHostKind::Abi) && !getHost(PluginHostKind::External)) {
        return Error{ErrorCode::InvalidState, "No plugin host available"};
    }

    const bool externalTarget = isExternalPluginTarget(target);
    const bool abiTarget = isAbiPluginTarget(target);
    const auto preferred = externalTarget ? PluginHostKind::External : PluginHostKind::Abi;
    if (auto* host = getHost(preferred)) {
        if (auto result = host->scanTarget(target)) {
            return DiscoveredPlugin{preferred, result.value()};
        }
    }
    if (!externalTarget && !abiTarget) {
        const auto fallback =
            preferred == PluginHostKind::Abi ? PluginHostKind::External : PluginHostKind::Abi;
        if (auto* host = getHost(fallback)) {
            if (auto result = host->scanTarget(target)) {
                return DiscoveredPlugin{fallback, result.value()};
            }
        }
    }
    return Error{ErrorCode::NotFound, "No plugin found at target: " + target.string()};
}

Result<std::vector<PluginManager::DiscoveredPlugin>>
PluginManager::scanPluginDirectory(const std::filesystem::path& directory) const {
    std::vector<DiscoveredPlugin> discovered;
    std::unordered_set<std::string> seen;
    bool hasHost = false;

    for (const auto kind : {PluginHostKind::Abi, PluginHostKind::External}) {
        auto* host = getHost(kind);
        if (!host) {
            continue;
        }
        hasHost = true;
        auto result = host->scanDirectory(directory);
        if (!result) {
            spdlog::debug("[PluginManager] scan failed for {}: {}", directory.string(),
                          result.error().message);
            continue;
        }
        for (auto& descriptor : result.value()) {
            const auto key = pluginDiscoveryKey(descriptor);
            if (seen.insert(key).second) {
                discovered.push_back(DiscoveredPlugin{kind, descriptor});
            }
        }
    }

    if (!hasHost) {
        return Error{ErrorCode::InvalidState, "No plugin host available"};
    }
    return discovered;
}

Result<std::vector<PluginManager::DiscoveredPlugin>>
PluginManager::scanConfiguredPluginRoots() const {
    std::vector<DiscoveredPlugin> discovered;
    std::unordered_set<std::string> seen;

    for (const auto& [kind, root] : pluginScanRoots()) {
        auto* host = getHost(kind);
        if (!host) {
            continue;
        }

        std::error_code ec;
        Result<std::vector<PluginDescriptor>> result{
            Error{ErrorCode::InvalidState, "Plugin root was not scanned"}};
        if (std::filesystem::is_regular_file(root, ec)) {
            auto targetResult = host->scanTarget(root);
            if (targetResult) {
                result = std::vector<PluginDescriptor>{targetResult.value()};
            } else {
                result = targetResult.error();
            }
        } else {
            result = host->scanDirectory(root);
        }
        if (!result) {
            spdlog::debug("[PluginManager] scan failed for {}: {}", root.string(),
                          result.error().message);
            continue;
        }
        for (auto& descriptor : result.value()) {
            const auto key = pluginDiscoveryKey(descriptor);
            if (seen.insert(key).second) {
                discovered.push_back(DiscoveredPlugin{kind, descriptor});
            }
        }
    }
    return discovered;
}

Result<std::filesystem::path>
PluginManager::resolvePluginTarget(const std::filesystem::path& target) const {
    std::error_code ec;
    if (std::filesystem::exists(target, ec)) {
        return target;
    }
    for (const auto& [kind, root] : pluginScanRoots()) {
        (void)kind;
        if (!std::filesystem::is_directory(root, ec)) {
            ec.clear();
            continue;
        }
        auto candidate = root / target;
        if (std::filesystem::exists(candidate, ec)) {
            return candidate;
        }
        ec.clear();
    }
    return Error{ErrorCode::NotFound, "Plugin not found: " + target.string()};
}

Result<PluginDescriptor> PluginManager::loadDiscoveredPlugin(const DiscoveredPlugin& discovered,
                                                             std::string explicitConfigJson) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    auto* host = getHost(discovered.host);
    if (!host) {
        return Error{ErrorCode::InvalidState, "Plugin host not initialized"};
    }

    auto result =
        loadConfiguredPlugin(*host, discovered.descriptor, std::exchange(explicitConfigJson, {}));
    if (!result) {
        pluginHostFsm_.dispatch(PluginLoadRejectedEvent{result.error().message});
        return result.error();
    }

    pluginHostFsm_.dispatch(PluginLoadedEvent{result.value().name});
    std::size_t loadedCount = 0;
    for (const auto kind : {PluginHostKind::Abi, PluginHostKind::External}) {
        if (const auto* candidateHost = getHost(kind)) {
            loadedCount += candidateHost->listLoaded().size();
        }
    }
    pluginHostFsm_.dispatch(AllPluginsLoadedEvent{loadedCount});
    return result.value();
}

Result<PluginDescriptor> PluginManager::loadPluginTarget(const std::filesystem::path& target,
                                                         std::string explicitConfigJson) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    auto resolved = resolvePluginTarget(target);
    if (!resolved) {
        return resolved.error();
    }
    auto discovered = scanPluginTarget(resolved.value());
    if (!discovered) {
        return discovered.error();
    }
    return loadDiscoveredPlugin(discovered.value(), std::exchange(explicitConfigJson, {}));
}

Result<std::vector<PluginDescriptor>>
PluginManager::loadPluginDirectory(const std::filesystem::path& directory) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    auto discovered = scanPluginDirectory(directory);
    if (!discovered) {
        return discovered.error();
    }

    std::vector<PluginDescriptor> loaded;
    std::optional<Error> lastError;
    for (const auto& candidate : discovered.value()) {
        auto result = loadDiscoveredPlugin(candidate);
        if (result) {
            loaded.push_back(result.value());
        } else {
            lastError = result.error();
        }
    }
    if (loaded.empty() && lastError) {
        return *lastError;
    }
    return loaded;
}

Result<void> PluginManager::unloadPlugin(const std::string& name) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    for (const auto kind : {PluginHostKind::Abi, PluginHostKind::External}) {
        auto* host = getHost(kind);
        if (!host) {
            continue;
        }
        const auto loaded = host->listLoaded();
        const auto match = std::find_if(loaded.begin(), loaded.end(), [&](const auto& descriptor) {
            return descriptor.name == name;
        });
        if (match == loaded.end()) {
            continue;
        }
        if (exposesRuntimeInterface(*match)) {
            const std::string message =
                "Hot unload is unsafe for a plugin with active runtime interfaces; restart the "
                "daemon to unload: " +
                name;
            pluginHostFsm_.dispatch(PluginLoadRejectedEvent{message});
            return Error{ErrorCode::InvalidState, message};
        }
        auto result = host->unload(name);
        if (!result) {
            pluginHostFsm_.dispatch(PluginLoadRejectedEvent{result.error().message});
            return result.error();
        }
        if (adoptedProviderPluginName_ == name) {
            modelProvider_.reset();
            adoptedProviderPluginName_.clear();
            embeddingModelName_.clear();
            if (deps_.state) {
                deps_.state->readiness.modelProviderReady = false;
            }
        }
        (void)adoptModelProvider();
        (void)adoptContentExtractors();
        (void)adoptSymbolExtractors();
        (void)adoptEntityExtractors();
        (void)adoptEntityProviders();
        pluginHostFsm_.dispatch(PluginUnloadedEvent{name});
        return {};
    }
    return Error{ErrorCode::NotFound, "Plugin not loaded: " + name};
}

bool PluginManager::isPluginLoadedFrom(const std::filesystem::path& path) const {
    std::error_code ec;
    auto requested = std::filesystem::weakly_canonical(path, ec);
    if (ec) {
        requested = path.lexically_normal();
        ec.clear();
    }

    for (const auto kind : {PluginHostKind::Abi, PluginHostKind::External}) {
        const auto* host = getHost(kind);
        if (!host) {
            continue;
        }
        for (const auto& descriptor : host->listLoaded()) {
            auto loaded = std::filesystem::weakly_canonical(descriptor.path, ec);
            if (ec) {
                loaded = descriptor.path.lexically_normal();
                ec.clear();
            }
            if (loaded == requested) {
                return true;
            }
        }
    }
    return false;
}

Result<size_t> PluginManager::autoloadPlugins() {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    size_t loadedCount = 0;

    try {
        // FSM guard: avoid concurrent autoload scans
        auto ps = pluginHostFsm_.snapshot().state;
        if (ps == PluginHostState::ScanningDirectories || ps == PluginHostState::LoadingPlugins) {
            spdlog::warn("[PluginManager] autoload skipped: scan already in progress");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            return Result<size_t>(0);
        }

        // Check for mock provider or disabled plugins
        if (deps_.config && deps_.config->useMockModelProvider) {
            spdlog::info("[PluginManager] autoload skipped (mock provider in use)");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            return Result<size_t>(0);
        }
#if defined(YAMS_TESTING) || defined(YAMS_TEST_PROVIDER_CONTROLS)
        if (const auto mockProvider = getenvCopy("YAMS_USE_MOCK_PROVIDER");
            mockProvider && ConfigResolver::envTruthy(mockProvider->c_str())) {
            spdlog::info("[PluginManager] autoload skipped (mock provider via env)");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            return Result<size_t>(0);
        }
#endif
        if (const auto disabled = getenvCopy("YAMS_DISABLE_ABI_PLUGINS");
            disabled && !disabled->empty()) {
            spdlog::info("[PluginManager] autoload disabled by YAMS_DISABLE_ABI_PLUGINS");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            return Result<size_t>(0);
        }

        bool skipAbiModelProviders = false;
        std::string inProcessBackend;
        {
            auto backend = ConfigResolver::resolveEmbeddingBackend("auto");
            if (backend != "auto" && backend != "daemon" && backend != "onnxruntime") {
                const auto known = getRegisteredProviders();
                if (std::find(known.begin(), known.end(), backend) != known.end()) {
                    skipAbiModelProviders = true;
                    inProcessBackend = backend;
                    spdlog::info("[PluginManager] model_provider plugins will be filtered: "
                                 "backend='{}' is satisfied in-process",
                                 inProcessBackend);
                }
            }
        }

        const auto roots = pluginScanRoots();
        spdlog::info("[PluginManager] autoload: {} resolved roots to scan", roots.size());
        for (const auto& [host, root] : roots) {
            spdlog::debug("[PluginManager] scan root: host={} path='{}'",
                          host == PluginHostKind::Abi ? "abi" : "external", root.string());
        }
        pluginHostFsm_.dispatch(PluginScanStartedEvent{roots.size()});

        auto scanResult = scanConfiguredPluginRoots();
        if (!scanResult) {
            pluginHostFsm_.dispatch(PluginLoadFailedEvent{scanResult.error().message});
            return scanResult.error();
        }

        std::unordered_set<std::string> scheduledPluginNames;
        for (const auto& discovered : scanResult.value()) {
            const auto& descriptor = discovered.descriptor;
            if (!scheduledPluginNames.insert(descriptor.name).second) {
                spdlog::info("[PluginManager] skipping duplicate plugin candidate '{}' from '{}'",
                             descriptor.name, descriptor.path.string());
                continue;
            }

            const bool isModelProvider =
                std::find(descriptor.interfaces.begin(), descriptor.interfaces.end(),
                          "model_provider_v1") != descriptor.interfaces.end();
            if (discovered.host == PluginHostKind::Abi && isModelProvider &&
                skipAbiModelProviders) {
                spdlog::info("[PluginManager] skipping model_provider plugin '{}' (path='{}'): "
                             "backend='{}' is satisfied in-process",
                             descriptor.name, descriptor.path.string(), inProcessBackend);
                continue;
            }

            try {
                auto loadResult = loadDiscoveredPlugin(discovered);
                if (!loadResult) {
                    spdlog::warn("[PluginManager] load failed for '{}': {}", descriptor.name,
                                 loadResult.error().message);
                    pluginHostFsm_.dispatch(PluginLoadFailedEvent{loadResult.error().message});
                    continue;
                }
                ++loadedCount;
                spdlog::info("[PluginManager] loaded: '{}'", loadResult.value().name);
            } catch (const std::exception& e) {
                spdlog::warn("[PluginManager] load exception for '{}': {}", descriptor.name,
                             e.what());
                pluginHostFsm_.dispatch(PluginLoadFailedEvent{e.what()});
            }
        }

        spdlog::info("[PluginManager] autoload complete: {} plugin(s) loaded", loadedCount);
        pluginHostFsm_.dispatch(AllPluginsLoadedEvent{loadedCount});

        // Adopt interfaces
        auto providerResult = adoptModelProvider();
        if (providerResult && providerResult.value()) {
            spdlog::info("[PluginManager] model provider adopted");
        }

        adoptContentExtractors();
        adoptSymbolExtractors();
        adoptEntityExtractors();
        adoptEntityProviders();

        return Result<size_t>(loadedCount);

    } catch (const std::exception& e) {
        spdlog::error("[PluginManager] autoload exception: {}", e.what());
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<bool> PluginManager::adoptModelProvider(const std::string& preferredName) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    if (!getActivePluginHost()) {
        return Error{ErrorCode::InvalidState, "Plugin host not initialized"};
    }

    try {
        if (modelProvider_ && modelProvider_->isAvailable()) {
            const bool preferredMatches = preferredName.empty() ||
                                          preferredName == adoptedProviderPluginName_ ||
                                          preferredName == modelProvider_->getProviderName();
            if (preferredMatches) {
                spdlog::info("[PluginManager] Reusing adopted model provider: {}",
                             adoptedProviderPluginName_);
                return Result<bool>(true);
            }
        }

        auto loaded = getActivePluginHost()->listLoaded();

        auto pathFor = [&](const std::string& name) -> std::string {
            for (const auto& d : loaded) {
                if (d.name == name)
                    return d.path.string();
                try {
                    auto stem = std::filesystem::path(d.path).stem().string();
                    if (stem == name)
                        return d.path.string();
                } catch (const std::exception& e) {
                    spdlog::debug("[PluginManager] failed to inspect path '{}': {}",
                                  d.path.string(), e.what());
                }
            }
            return {};
        };

        auto tryAdopt = [&](const std::string& pluginName) -> bool {
            // Prefer latest interface version, but fall back to v3 for older plugins.
            // NOTE: Host-side code supports v3+ (v4 adds optional evict_under_pressure).
            auto lease = getActivePluginHost()->acquireInterface(
                pluginName, YAMS_IFACE_MODEL_PROVIDER_V1, YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);
            if (!lease) {
                lease = getActivePluginHost()->acquireInterface(
                    pluginName, YAMS_IFACE_MODEL_PROVIDER_V1, /*version*/ 3u);
            }
            if (!lease) {
                spdlog::warn("[PluginManager] No model_provider_v1 interface for '{}': {}",
                             pluginName, lease.error().message);
                return false;
            }

            auto* table = reinterpret_cast<yams_model_provider_v1*>(lease.value().interface);
            if (!table) {
                spdlog::debug("[PluginManager] Null provider table for '{}'", pluginName);
                return false;
            }

            // Accept v3+ for backward compatibility; v4 adds optional evict_under_pressure
            constexpr uint32_t kMinSupportedVersion = 3u;
            if (table->abi_version < kMinSupportedVersion) {
                spdlog::debug("[PluginManager] ABI too old for '{}': got v{}, need v{}+",
                              pluginName, table->abi_version, kMinSupportedVersion);
                embeddingFsm_.dispatch(ProviderDegradedEvent{"ABI too old: " + pluginName});
                return false;
            }
            if (table->abi_version < YAMS_IFACE_MODEL_PROVIDER_V1_VERSION) {
                spdlog::info("[PluginManager] Plugin '{}' uses ABI v{} (current v{}); "
                             "evict_under_pressure not available",
                             pluginName, table->abi_version, YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);
            }

            modelProvider_ =
                std::make_shared<AbiModelProviderAdapter>(table, lease.value().keepAlive);
            adoptedProviderPluginName_ = pluginName;

            if (deps_.state) {
                deps_.state->readiness.modelProviderReady = true;
            }

            spdlog::info("[PluginManager] Adopted model provider from: {} (path='{}')", pluginName,
                         pathFor(pluginName));

            // Update FSM
            embeddingFsm_.dispatch(ProviderAdoptedEvent{pluginName});

            // Check provider availability and get model info
            if (modelProvider_ && modelProvider_->isAvailable()) {
                std::string modelName =
                    deps_.resolvePreferredModel ? deps_.resolvePreferredModel() : "";
                if (!modelName.empty()) {
                    size_t dimension = modelProvider_->getEmbeddingDim(modelName);
                    spdlog::info("[PluginManager] Provider ready: model='{}', dim={}", modelName,
                                 dimension);
                    embeddingFsm_.dispatch(ModelLoadedEvent{modelName, dimension});
                    embeddingModelName_ = modelName;
                }

                if (deps_.lifecycleFsm) {
                    deps_.lifecycleFsm->setSubsystemDegraded("embeddings", false);
                }
            }

            return true;
        };

        auto tryAdoptInProcess = [&](const std::string& backend) -> bool {
            auto provider = createModelProvider(ModelPoolConfig{}, backend);
            if (!provider || !provider->isAvailable()) {
                spdlog::warn("[PluginManager] In-process provider '{}' registered but factory "
                             "returned unavailable instance",
                             backend);
                return false;
            }
            modelProvider_.reset(provider.release());
            adoptedProviderPluginName_ = backend;
            if (deps_.state) {
                deps_.state->readiness.modelProviderReady = true;
            }
            const auto providerName = modelProvider_->getProviderName();
            const auto trainingFree = modelProvider_->isTrainingFree();
            embeddingModelName_ = trainingFree ? providerName : (backend + "-default");
            const size_t dim = modelProvider_->getEmbeddingDim(embeddingModelName_);
            spdlog::info("[PluginManager] Adopted in-process model provider: {} "
                         "(training_free={}, dim={})",
                         providerName, trainingFree, dim);
            embeddingFsm_.dispatch(ProviderAdoptedEvent{backend});
            if (dim > 0) {
                embeddingFsm_.dispatch(ModelLoadedEvent{embeddingModelName_, dim});
            }
            if (deps_.lifecycleFsm) {
                deps_.lifecycleFsm->setSubsystemDegraded("embeddings", false);
            }
            return true;
        };

        const auto configuredBackend = ConfigResolver::resolveEmbeddingBackend("auto");
        const bool onnxRuntimeBackendSelected = configuredBackend == "onnxruntime";
        const bool inProcessBackendSelected =
            configuredBackend != "auto" && configuredBackend != "daemon" &&
            !onnxRuntimeBackendSelected && [&]() {
                const auto known = getRegisteredProviders();
                return std::find(known.begin(), known.end(), configuredBackend) != known.end();
            }();

        // Try preferred name first
        if (!preferredName.empty() && tryAdopt(preferredName)) {
            return Result<bool>(true);
        }

        auto tryAdoptOnnxRuntime = [&]() -> bool {
            spdlog::info("[PluginManager] adoptModelProvider: preferring ONNX Runtime plugin "
                         "because embeddings.backend='onnxruntime'");
            for (const auto& d : loaded) {
                std::string name = d.name;
                std::transform(name.begin(), name.end(), name.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                if (name.find("onnx") != std::string::npos && tryAdopt(d.name)) {
                    return true;
                }
                try {
                    auto stem = std::filesystem::path(d.path).stem().string();
                    std::string lowerStem = stem;
                    std::transform(
                        lowerStem.begin(), lowerStem.end(), lowerStem.begin(),
                        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                    if (!stem.empty() && stem != d.name &&
                        lowerStem.find("onnx") != std::string::npos && tryAdopt(stem)) {
                        return true;
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("[PluginManager] failed to inspect ONNX path '{}': {}",
                                  d.path.string(), e.what());
                }
            }
            spdlog::warn("[PluginManager] ONNX Runtime embeddings requested, but no usable ONNX "
                         "model_provider plugin was loaded");
            return false;
        };

        if (preferredName.empty() && onnxRuntimeBackendSelected) {
            return Result<bool>(tryAdoptOnnxRuntime());
        }

        // When the user has selected a specific in-process backend and did not name a
        // specific plugin, honor that selection before iterating ABI plugins.
        if (preferredName.empty() && inProcessBackendSelected) {
            spdlog::info("[PluginManager] adoptModelProvider: preferring in-process backend '{}' "
                         "over ABI plugins",
                         configuredBackend);
            if (tryAdoptInProcess(configuredBackend)) {
                return Result<bool>(true);
            }
        }

        // Try all loaded plugins
        spdlog::info("[PluginManager] adoptModelProvider: {} loaded plugins to try", loaded.size());
        for (const auto& d : loaded) {
            spdlog::info("[PluginManager] Trying plugin name='{}' path='{}'", d.name,
                         d.path.string());
            if (tryAdopt(d.name))
                return Result<bool>(true);

            // Try path stem as alternate name
            try {
                std::string alt = std::filesystem::path(d.path).stem().string();
                if (!alt.empty() && alt != d.name) {
                    spdlog::info("[PluginManager] Trying alternate name='{}'", alt);
                    if (tryAdopt(alt))
                        return Result<bool>(true);
                }
            } catch (const std::exception& e) {
                spdlog::debug("[PluginManager] failed to inspect alternate path '{}': {}",
                              d.path.string(), e.what());
            }
        }

        // No ABI plugin adopted; try in-process registry if a specific backend is selected.
        if (inProcessBackendSelected && tryAdoptInProcess(configuredBackend)) {
            return Result<bool>(true);
        }

        // No provider found
        spdlog::warn("[PluginManager] No model provider adopted from any plugin");
        embeddingFsm_.dispatch(ProviderDegradedEvent{"no provider adopted"});
        return Result<bool>(false);

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptContentExtractors() {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    try {
        size_t adopted = 0;

        // Clear existing extractors to avoid duplicates on re-adoption
        contentExtractors_.clear();

        // Adopt from ABI (native) plugins
        // Use getActivePluginHost() to get the correct host (shared or owned)
        adopted +=
            adoptPluginInterfaceImpl<yams_content_extractor_v1, PluginContentExtractorAdapter,
                                     extraction::IContentExtractor>(
                getActivePluginHost(), "content_extractor_v1",
                YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION, contentExtractors_,
                [](const yams_content_extractor_v1* table) {
                    return table->abi_version == YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION;
                });

        // PBI-096: Adopt from external (Python/JS) plugins
        if (externalHost_) {
            for (const auto& desc : externalHost_->listLoaded()) {
                // Check if plugin implements content_extractor_v1
                bool hasInterface = std::find(desc.interfaces.begin(), desc.interfaces.end(),
                                              "content_extractor_v1") != desc.interfaces.end();
                if (!hasInterface)
                    continue;

                // Parse capabilities from manifest to get supported formats
                std::vector<std::string> mimes;
                std::vector<std::string> extensions;
                try {
                    if (!desc.manifestJson.empty()) {
                        auto manifest = nlohmann::json::parse(desc.manifestJson);
                        if (manifest.contains("capabilities") &&
                            manifest["capabilities"].contains("content_extraction")) {
                            auto& ce = manifest["capabilities"]["content_extraction"];
                            if (ce.contains("formats") && ce["formats"].is_array()) {
                                for (const auto& f : ce["formats"]) {
                                    if (f.is_string())
                                        mimes.push_back(f.get<std::string>());
                                }
                            }
                            if (ce.contains("extensions") && ce["extensions"].is_array()) {
                                for (const auto& e : ce["extensions"]) {
                                    if (e.is_string())
                                        extensions.push_back(e.get<std::string>());
                                }
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Failed to parse manifest for {}: {}", desc.name, e.what());
                }

                try {
                    auto adapter = std::make_shared<PluginContentExtractorAdapter>(
                        externalHost_.get(), desc.name, mimes, extensions);
                    contentExtractors_.push_back(adapter);
                    ++adopted;
                    spdlog::info("Adopted content_extractor_v1 from external plugin: {}",
                                 desc.name);
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to create adapter for external plugin {}: {}", desc.name,
                                 e.what());
                }
            }
        }

        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptSymbolExtractors() {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    try {
        // Clear existing extractors to avoid duplicates on re-adoption
        symbolExtractors_.clear();

        // Use getActivePluginHost() to get the correct host (shared or owned)
        auto* host = getActivePluginHost();
        if (!host) {
            spdlog::warn("[PluginManager] No active plugin host for symbol extractor adoption");
            return Result<size_t>(0);
        }

        size_t adopted =
            adoptPluginInterfaceImpl<yams_symbol_extractor_v1, AbiSymbolExtractorAdapter,
                                     AbiSymbolExtractorAdapter>(
                host, "symbol_extractor_v1", YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION,
                symbolExtractors_, [](const yams_symbol_extractor_v1* table) {
                    return table->abi_version == YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION;
                });
        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptEntityExtractors() {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    try {
        // Clear existing extractors to avoid duplicates on re-adoption
        entityExtractors_.clear();

        // Use getActivePluginHost() to get the correct host (shared or owned)
        auto* host = getActivePluginHost();
        if (!host) {
            spdlog::info("[PluginManager] No active plugin host for entity extractor adoption");
            return Result<size_t>(0);
        }

        size_t adopted =
            adoptPluginInterfaceImpl<yams_entity_extractor_v2, AbiEntityExtractorAdapter,
                                     AbiEntityExtractorAdapter>(
                host, YAMS_IFACE_ENTITY_EXTRACTOR_V2, YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION,
                entityExtractors_, [](const yams_entity_extractor_v2* table) {
                    return table->abi_version == YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION;
                });

        if (adopted > 0) {
            spdlog::info("[PluginManager] Adopted {} entity extractor(s) (e.g., Glint)", adopted);
        }
        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptEntityProviders() {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    try {
        size_t adopted = 0;

        // Clear existing providers to avoid duplicates on re-adoption
        entityProviders_.clear();

        // Entity providers are only available from external plugins
        if (!externalHost_) {
            spdlog::info("[PluginManager] adoptEntityProviders: No external plugin host");
            return Result<size_t>(0);
        }

        spdlog::info("[PluginManager] adoptEntityProviders: Scanning {} external plugins",
                     externalHost_->listLoaded().size());

        for (const auto& desc : externalHost_->listLoaded()) {
            spdlog::info(
                "[PluginManager] adoptEntityProviders: Checking plugin '{}' with {} interfaces",
                desc.name, desc.interfaces.size());
            for (const auto& iface : desc.interfaces) {
                spdlog::debug("[PluginManager]   - interface: {}", iface);
            }

            // Check if plugin implements kg_entity_provider_v1
            bool hasInterface = std::find(desc.interfaces.begin(), desc.interfaces.end(),
                                          "kg_entity_provider_v1") != desc.interfaces.end();
            if (!hasInterface) {
                spdlog::debug("[PluginManager] Plugin '{}' does not have kg_entity_provider_v1",
                              desc.name);
                continue;
            }

            // Parse capabilities from manifest to get supported extensions and RPC method
            std::vector<std::string> extensions;
            std::string rpcMethod = "ghidra.getEntities"; // Default for Ghidra plugin

            try {
                if (!desc.manifestJson.empty()) {
                    auto manifest = nlohmann::json::parse(desc.manifestJson);
                    if (manifest.contains("capabilities") &&
                        manifest["capabilities"].contains("kg_entities")) {
                        auto& ke = manifest["capabilities"]["kg_entities"];
                        if (ke.contains("extensions") && ke["extensions"].is_array()) {
                            for (const auto& ext : ke["extensions"]) {
                                if (ext.is_string())
                                    extensions.push_back(ext.get<std::string>());
                            }
                        }
                        if (ke.contains("rpc_method") && ke["rpc_method"].is_string()) {
                            rpcMethod = ke["rpc_method"].get<std::string>();
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("Failed to parse manifest for {}: {}", desc.name, e.what());
            }

            // If no extensions specified, skip this provider
            if (extensions.empty()) {
                spdlog::debug("[PluginManager] kg_entity_provider_v1 from {} has no extensions",
                              desc.name);
                continue;
            }

            try {
                auto adapter = std::make_shared<ExternalEntityProviderAdapter>(
                    externalHost_.get(), desc.name, rpcMethod, extensions);
                entityProviders_.push_back(adapter);
                ++adopted;
                spdlog::info("Adopted kg_entity_provider_v1 from external plugin: {} (method={})",
                             desc.name, rpcMethod);
            } catch (const std::exception& e) {
                spdlog::warn("Failed to create entity provider adapter for {}: {}", desc.name,
                             e.what());
            }
        }

        // Always reconcile the queue, including clearing providers after plugin unload.
        if (postIngestQueue_) {
            postIngestQueue_->setEntityProviders(entityProviders_);
            spdlog::info("[PluginManager] Wired {} entity providers to PostIngestQueue", adopted);
        } else if (adopted > 0) {
            spdlog::debug(
                "[PluginManager] Entity providers adopted but PostIngestQueue not yet available");
        }

        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

std::shared_ptr<IModelProvider> PluginManager::getModelProvider() const {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    return modelProvider_;
}

std::string PluginManager::getAdoptedProviderPluginName() const {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    return adoptedProviderPluginName_;
}

std::string PluginManager::getEmbeddingModelName() const {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    return embeddingModelName_;
}

void PluginManager::setEmbeddingModelName(const std::string& name) {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    embeddingModelName_ = name;
}

bool PluginManager::isModelProviderDegraded() const {
    try {
        auto snap = embeddingFsm_.snapshot();
        return snap.state == EmbeddingProviderState::Degraded;
    } catch (...) {
        return false;
    }
}

std::string PluginManager::lastModelError() const {
    if (deps_.lifecycleFsm) {
        return deps_.lifecycleFsm->degradationReason("embeddings");
    }
    return {};
}

void PluginManager::clearModelProviderError() {
    if (deps_.lifecycleFsm) {
        deps_.lifecycleFsm->setSubsystemDegraded("embeddings", false);
    }
}

size_t PluginManager::getEmbeddingDimension() const {
    std::lock_guard lifecycleLock(pluginLifecycleMutex_);
    if (!modelProvider_ || !modelProvider_->isAvailable())
        return 0;

    try {
        std::string preferredModel;
        if (deps_.resolvePreferredModel) {
            preferredModel = deps_.resolvePreferredModel();
        }
        const std::string& modelName =
            preferredModel.empty() ? embeddingModelName_ : preferredModel;
        if (modelName.empty())
            return 0;
        return modelProvider_->getEmbeddingDim(modelName);
    } catch (...) {
        return 0;
    }
}

void PluginManager::testingSetEmbeddingDegraded(bool degraded, const std::string& error) {
    if (degraded) {
        embeddingFsm_.dispatch(ProviderDegradedEvent{error.empty() ? std::string{"test"} : error});
    } else {
        embeddingFsm_.dispatch(ModelLoadedEvent{"", 0});
    }
}

} // namespace yams::daemon
