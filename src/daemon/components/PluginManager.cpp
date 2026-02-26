// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/config/config_helpers.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
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
#include <fstream>
#include <sstream>

namespace yams::daemon {

std::string adjustOnnxConfigJson(const std::string& configJson, std::size_t defaultMax) {
    nlohmann::json cfgJson = nlohmann::json::object();
    if (!configJson.empty()) {
        auto parsed = nlohmann::json::parse(configJson, nullptr, false);
        if (!parsed.is_discarded() && parsed.is_object()) {
            cfgJson = parsed;
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
                         int interfaceVersion,
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

        auto ifaceRes = host->getInterface(descriptor.name, interfaceName, interfaceVersion);
        if (!ifaceRes)
            continue;

        auto* table = reinterpret_cast<AbiTableType*>(ifaceRes.value());
        if (!table)
            continue;

        if (validateTable && !validateTable(table))
            continue;

        try {
            auto adapter = std::make_shared<AdapterType>(table);
            targetContainer.push_back(std::move(adapter));
            ++adopted;
            spdlog::info("Adopted {} from plugin: {}", interfaceName, descriptor.name);
        } catch (const std::exception& e) {
            spdlog::warn("Failed to create adapter for {} from plugin {}: {}", interfaceName,
                         descriptor.name, e.what());
        }
    }
    return adopted;
}

PluginManager::PluginManager(Dependencies deps) : deps_(std::move(deps)) {}

PluginManager::~PluginManager() {
    shutdown();
}

Result<void> PluginManager::initialize() {
    spdlog::debug("[PluginManager] Initializing");

    // Create plugin loader
    pluginLoader_ = std::make_unique<AbiPluginLoader>();

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
                pluginHost_ = std::make_unique<AbiPluginHost>(nullptr, trustFile);
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
        if (const char* env = std::getenv("YAMS_PLUGIN_NAME_POLICY"))
            policy = env;
        for (auto& c : policy)
            c = static_cast<char>(std::tolower(c));
        if (policy == "spec")
            pluginLoader_->setNamePolicy(AbiPluginLoader::NamePolicy::Spec);
        else
            pluginLoader_->setNamePolicy(AbiPluginLoader::NamePolicy::Relaxed);
    }

    // Treat a host with zero plugins as a valid steady state. Autoload can still
    // transition Ready -> Scanning/Loading -> Ready when invoked.
    pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});

    return Result<void>{};
}

void PluginManager::shutdown() {
    spdlog::debug("[PluginManager] Shutting down");

    // Shutdown external plugin host first (terminates child processes)
    externalHost_.reset();

    // Clear adopted interfaces
    modelProvider_.reset();
    contentExtractors_.clear();
    symbolExtractors_.clear();
    entityProviders_.clear();

    // Unload plugins
    if (getActivePluginHost()) {
        for (const auto& d : getActivePluginHost()->listLoaded()) {
            try {
                getActivePluginHost()->unload(d.name);
            } catch (...) {
            }
        }
    }

    pluginHost_.reset();
    pluginLoader_.reset();
}

std::vector<std::filesystem::path> PluginManager::trustList() const {
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

boost::asio::awaitable<Result<size_t>>
PluginManager::autoloadPlugins(boost::asio::any_io_executor executor) {
    size_t loadedCount = 0;

    try {
        // FSM guard: avoid concurrent autoload scans
        auto ps = pluginHostFsm_.snapshot().state;
        if (ps == PluginHostState::ScanningDirectories || ps == PluginHostState::LoadingPlugins) {
            spdlog::warn("[PluginManager] autoload skipped: scan already in progress");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            co_return Result<size_t>(0);
        }

        // Check for mock provider or disabled plugins
        if (deps_.config && deps_.config->useMockModelProvider) {
            spdlog::info("[PluginManager] autoload skipped (mock provider in use)");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            co_return Result<size_t>(0);
        }
        if (ConfigResolver::envTruthy(std::getenv("YAMS_USE_MOCK_PROVIDER"))) {
            spdlog::info("[PluginManager] autoload skipped (mock provider via env)");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            co_return Result<size_t>(0);
        }
        if (const char* d = std::getenv("YAMS_DISABLE_ABI_PLUGINS"); d && *d) {
            spdlog::info("[PluginManager] autoload disabled by YAMS_DISABLE_ABI_PLUGINS");
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{0});
            co_return Result<size_t>(0);
        }

        // Build list of plugin directories to scan
        std::vector<std::filesystem::path> roots;
        std::vector<std::filesystem::path> trustedRoots;
        std::vector<std::filesystem::path> defaultRoots;

        // Add trust list paths
        if (getActivePluginHost()) {
            for (const auto& p : getActivePluginHost()->trustList()) {
                trustedRoots.push_back(p);
                roots.push_back(p);
            }
        }

        // Add platform-specific default directories unless strict plugin-dir mode is enabled.
        // Strict mode is useful for benchmark/test isolation where only explicitly trusted roots
        // should be scanned.
        namespace fs = std::filesystem;
        bool strictPluginDirMode = deps_.config ? deps_.config->pluginDirStrict : false;
        if (const char* envStrict = std::getenv("YAMS_PLUGIN_DIR_STRICT")) {
            strictPluginDirMode = ConfigResolver::envTruthy(envStrict);
        }
        if (!strictPluginDirMode) {
#ifdef _WIN32
            defaultRoots.push_back(yams::config::get_data_dir() / "plugins");
#else
            if (const char* home = std::getenv("HOME")) {
                defaultRoots.push_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
            }
#ifdef __APPLE__
            // macOS: Homebrew default install location
            defaultRoots.push_back(fs::path("/opt/homebrew/lib/yams/plugins"));
#endif
            defaultRoots.push_back(fs::path("/usr/local/lib/yams/plugins"));
            defaultRoots.push_back(fs::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
            defaultRoots.push_back(fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins");
#endif
        } else {
            spdlog::info("[PluginManager] strict plugin-dir mode enabled; skipping default plugin "
                         "roots");
        }

        roots.insert(roots.end(), defaultRoots.begin(), defaultRoots.end());

        // Deduplicate
        std::sort(roots.begin(), roots.end());
        roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

        auto joinPaths = [](const std::vector<std::filesystem::path>& paths) {
            std::string out;
            for (const auto& p : paths) {
                if (!out.empty())
                    out += ";";
                out += p.string();
            }
            return out;
        };

        if (!trustedRoots.empty()) {
            std::sort(trustedRoots.begin(), trustedRoots.end());
            trustedRoots.erase(std::unique(trustedRoots.begin(), trustedRoots.end()),
                               trustedRoots.end());
        }
        if (!defaultRoots.empty()) {
            std::sort(defaultRoots.begin(), defaultRoots.end());
            defaultRoots.erase(std::unique(defaultRoots.begin(), defaultRoots.end()),
                               defaultRoots.end());
        }

        spdlog::info("[PluginManager] trust roots ({}): {}", trustedRoots.size(),
                     joinPaths(trustedRoots));
        spdlog::info("[PluginManager] default roots (strict={} count={}): {}", strictPluginDirMode,
                     defaultRoots.size(), joinPaths(defaultRoots));

        spdlog::info("[PluginManager] effective scan roots: {}", joinPaths(roots));
        spdlog::info("[PluginManager] autoload: {} roots to scan", roots.size());
        pluginHostFsm_.dispatch(PluginScanStartedEvent{roots.size()});

        // Collect load tasks
        std::vector<boost::asio::awaitable<Result<PluginDescriptor>>> loadTasks;

        for (const auto& root : roots) {
            spdlog::info("[PluginManager] scanning: {}", root.string());

            if (!getActivePluginHost())
                continue;

            auto scanResult = getActivePluginHost()->scanDirectory(root);
            if (!scanResult) {
                spdlog::debug("[PluginManager] scan failed for {}: {}", root.string(),
                              scanResult.error().message);
                pluginHostFsm_.dispatch(PluginLoadFailedEvent{scanResult.error().message});
                continue;
            }

            for (const auto& desc : scanResult.value()) {
                spdlog::info("[PluginManager] candidate: '{}' path='{}' ifaces=[{}]", desc.name,
                             desc.path.string(), [&]() {
                                 std::string s;
                                 for (size_t i = 0; i < desc.interfaces.size(); i++) {
                                     if (i)
                                         s += ",";
                                     s += desc.interfaces[i];
                                 }
                                 return s;
                             }());

                // Create awaitable load task
                // Use getActivePluginHost() to get the correct host (shared or owned)
                auto host = getActivePluginHost();
                if (!host) {
                    spdlog::warn("[PluginManager] no active plugin host for loading");
                    continue;
                }
                auto path = desc.path;
                auto pluginName = desc.name;

                // Look up plugin-specific config from DaemonConfig
                // Try multiple name variants: the config may use a short name (e.g., "glint")
                // while the plugin descriptor uses the full name (e.g., "yams_glint")
                std::string configJson;
                if (deps_.config) {
                    // Build list of name variants to try
                    std::vector<std::string> nameVariants = {pluginName};

                    // If name starts with "yams_", also try without the prefix
                    if (pluginName.rfind("yams_", 0) == 0 && pluginName.size() > 5) {
                        nameVariants.push_back(pluginName.substr(5));
                    }
                    // If name starts with "libyams_", try without the "lib" and "libyams_" prefixes
                    if (pluginName.rfind("libyams_", 0) == 0 && pluginName.size() > 8) {
                        nameVariants.push_back(pluginName.substr(8)); // without "libyams_"
                        nameVariants.push_back(pluginName.substr(3)); // without "lib" (yams_*)
                    }

                    for (const auto& variant : nameVariants) {
                        auto it = deps_.config->pluginConfigs.find(variant);
                        if (it != deps_.config->pluginConfigs.end()) {
                            configJson = it->second;
                            spdlog::info(
                                "[PluginManager] passing config to plugin '{}' (key '{}'): {}",
                                pluginName, variant, configJson);
                            break;
                        }
                    }
                }

                const bool isModelProvider =
                    std::find(desc.interfaces.begin(), desc.interfaces.end(),
                              "model_provider_v1") != desc.interfaces.end();
                const bool isOnnxPlugin = (pluginName.find("onnx") != std::string::npos);
                if (isModelProvider && isOnnxPlugin) {
                    std::size_t defaultMax =
                        deps_.config ? deps_.config->modelPoolConfig.maxLoadedModels : 0;
                    configJson = adjustOnnxConfigJson(configJson, defaultMax);
                    spdlog::info("[PluginManager] enforcing ONNX model pool max_loaded_models");
                }

                loadTasks.push_back(boost::asio::co_spawn(
                    executor,
                    [host, path, configJson]() -> boost::asio::awaitable<Result<PluginDescriptor>> {
                        co_return host->load(path, configJson);
                    },
                    boost::asio::use_awaitable));
            }
        }

        // Execute load tasks
        for (auto& task : loadTasks) {
            try {
                auto res = co_await std::move(task);
                if (res) {
                    ++loadedCount;
                    spdlog::info("[PluginManager] loaded: '{}'", res.value().name);
                    pluginHostFsm_.dispatch(PluginLoadedEvent{res.value().name});
                } else {
                    spdlog::warn("[PluginManager] load failed: {}", res.error().message);
                    pluginHostFsm_.dispatch(PluginLoadFailedEvent{res.error().message});
                }
            } catch (const std::exception& e) {
                spdlog::warn("[PluginManager] load exception: {}", e.what());
            }
        }

        // External plugins autoload: scan external plugin directories
        if (externalHost_) {
            std::vector<std::filesystem::path> externalRoots;

            // Add external trust list paths
            for (const auto& p : externalHost_->trustList()) {
                externalRoots.push_back(p);
            }

            // Add platform-specific default external plugin directories
#ifdef _WIN32
            externalRoots.push_back(yams::config::get_data_dir() / "external-plugins");
#else
            if (const char* home = std::getenv("HOME")) {
                externalRoots.push_back(fs::path(home) / ".local" / "lib" / "yams" /
                                        "external-plugins");
            }
#ifdef __APPLE__
            // macOS: Homebrew default install location
            externalRoots.push_back(fs::path("/opt/homebrew/lib/yams/external-plugins"));
#endif
            externalRoots.push_back(fs::path("/usr/local/lib/yams/external-plugins"));
            externalRoots.push_back(fs::path("/usr/lib/yams/external-plugins"));
#endif

            // Deduplicate
            std::sort(externalRoots.begin(), externalRoots.end());
            externalRoots.erase(std::unique(externalRoots.begin(), externalRoots.end()),
                                externalRoots.end());

            for (const auto& root : externalRoots) {
                if (!std::filesystem::exists(root))
                    continue;

                try {
                    spdlog::info("[PluginManager] scanning external root: {}", root.string());
                    auto scanResult = externalHost_->scanDirectory(root);
                    if (scanResult) {
                        for (const auto& desc : scanResult.value()) {
                            spdlog::info("[PluginManager] loading external plugin '{}' from {}",
                                         desc.name, desc.path.string());

                            // Look up plugin-specific config from DaemonConfig
                            // Try multiple name variants (same logic as ABI plugins)
                            std::string configJson;
                            if (deps_.config) {
                                std::vector<std::string> nameVariants = {desc.name};
                                if (desc.name.rfind("yams_", 0) == 0 && desc.name.size() > 5) {
                                    nameVariants.push_back(desc.name.substr(5));
                                }
                                if (desc.name.rfind("libyams_", 0) == 0 && desc.name.size() > 8) {
                                    nameVariants.push_back(desc.name.substr(8));
                                    nameVariants.push_back(desc.name.substr(3));
                                }

                                for (const auto& variant : nameVariants) {
                                    auto it = deps_.config->pluginConfigs.find(variant);
                                    if (it != deps_.config->pluginConfigs.end()) {
                                        configJson = it->second;
                                        spdlog::info("[PluginManager] passing config to external "
                                                     "plugin '{}' (key '{}'): {}",
                                                     desc.name, variant, configJson);
                                        break;
                                    }
                                }
                            }

                            auto loadResult = externalHost_->load(desc.path, configJson);
                            if (loadResult) {
                                ++loadedCount;
                                spdlog::info("[PluginManager] loaded external: '{}' (ifaces=[{}])",
                                             loadResult.value().name, [&]() {
                                                 std::string s;
                                                 for (size_t i = 0;
                                                      i < loadResult.value().interfaces.size();
                                                      ++i) {
                                                     if (i)
                                                         s += ",";
                                                     s += loadResult.value().interfaces[i];
                                                 }
                                                 return s;
                                             }());
                                pluginHostFsm_.dispatch(PluginLoadedEvent{loadResult.value().name});
                            } else {
                                spdlog::warn("[PluginManager] external plugin load failed: {}",
                                             loadResult.error().message);
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("[PluginManager] external scan error at {}: {}", root.string(),
                                 e.what());
                }
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

        refreshStatusSnapshot();
        co_return Result<size_t>(loadedCount);

    } catch (const std::exception& e) {
        spdlog::error("[PluginManager] autoload exception: {}", e.what());
        co_return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<bool> PluginManager::adoptModelProvider(const std::string& preferredName) {
    if (!getActivePluginHost()) {
        return Error{ErrorCode::InvalidState, "Plugin host not initialized"};
    }

    try {
        auto loaded = getActivePluginHost()->listLoaded();

        auto pathFor = [&](const std::string& name) -> std::string {
            for (const auto& d : loaded) {
                if (d.name == name)
                    return d.path.string();
                try {
                    auto stem = std::filesystem::path(d.path).stem().string();
                    if (stem == name)
                        return d.path.string();
                } catch (...) {
                }
            }
            return {};
        };

        auto tryAdopt = [&](const std::string& pluginName) -> bool {
            // Prefer latest interface version, but fall back to v3 for older plugins.
            // NOTE: Host-side code supports v3+ (v4 adds optional evict_under_pressure).
            auto ifaceRes = getActivePluginHost()->getInterface(
                pluginName, YAMS_IFACE_MODEL_PROVIDER_V1, YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);
            if (!ifaceRes) {
                ifaceRes =
                    getActivePluginHost()->getInterface(pluginName, YAMS_IFACE_MODEL_PROVIDER_V1,
                                                        /*version*/ 3u);
            }
            if (!ifaceRes) {
                spdlog::warn("[PluginManager] No model_provider_v1 interface for '{}': {}",
                             pluginName, ifaceRes.error().message);
                return false;
            }

            auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
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

            modelProvider_ = std::make_shared<AbiModelProviderAdapter>(table);
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

            // Update model count cache
            try {
                auto count = static_cast<std::uint32_t>(modelProvider_->getLoadedModelCount());
                cachedModelCount_.store(count, std::memory_order_relaxed);
            } catch (...) {
                cachedModelCount_.store(0, std::memory_order_relaxed);
            }

            refreshStatusSnapshot();
            return true;
        };

        // Try preferred name first
        if (!preferredName.empty() && tryAdopt(preferredName)) {
            return Result<bool>(true);
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
            } catch (...) {
            }
        }

        // No provider found
        spdlog::warn("[PluginManager] No model provider adopted from any plugin");
        embeddingFsm_.dispatch(ProviderDegradedEvent{"no provider adopted"});
        refreshStatusSnapshot();
        return Result<bool>(false);

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptContentExtractors() {
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
                        externalHost_.get(), desc.name, std::move(mimes), std::move(extensions));
                    contentExtractors_.push_back(std::move(adapter));
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
                    externalHost_.get(), desc.name, rpcMethod, std::move(extensions));
                entityProviders_.push_back(std::move(adapter));
                ++adopted;
                spdlog::info("Adopted kg_entity_provider_v1 from external plugin: {} (method={})",
                             desc.name, rpcMethod);
            } catch (const std::exception& e) {
                spdlog::warn("Failed to create entity provider adapter for {}: {}", desc.name,
                             e.what());
            }
        }

        // Wire entity providers to PostIngestQueue if available
        if (adopted > 0 && postIngestQueue_) {
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

bool PluginManager::isModelProviderDegraded() const {
    try {
        auto snap = embeddingFsm_.snapshot();
        return snap.state == EmbeddingProviderState::Degraded ||
               snap.state == EmbeddingProviderState::Failed;
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
    if (!modelProvider_ || !modelProvider_->isAvailable())
        return 0;

    try {
        std::string modelName =
            deps_.resolvePreferredModel ? deps_.resolvePreferredModel() : embeddingModelName_;
        if (modelName.empty())
            return 0;
        return modelProvider_->getEmbeddingDim(modelName);
    } catch (...) {
        return 0;
    }
}

PluginManager::StatusSnapshot PluginManager::getStatusSnapshot() const {
    std::shared_lock lock(statusMutex_);
    return statusSnapshot_;
}

void PluginManager::refreshStatusSnapshot() {
    StatusSnapshot snap;

    try {
        snap.hostState = pluginHostFsm_.snapshot();
    } catch (...) {
    }

    try {
        snap.providerState = embeddingFsm_.snapshot();
    } catch (...) {
    }

    // Helper lambda to add plugin records
    auto addPluginRecord = [&](const PluginDescriptor& d) {
        PluginStatusRecord rec;
        rec.name = d.name;
        rec.isProvider = (d.name == adoptedProviderPluginName_);
        rec.ready = rec.isProvider && modelProvider_ && modelProvider_->isAvailable();
        rec.degraded = rec.isProvider && isModelProviderDegraded();
        if (rec.isProvider) {
            rec.modelsLoaded = cachedModelCount_.load(std::memory_order_relaxed);
        }
        snap.plugins.push_back(std::move(rec));
    };

    // Build plugin records from ABI (native) plugins
    if (getActivePluginHost()) {
        for (const auto& d : getActivePluginHost()->listLoaded()) {
            addPluginRecord(d);
        }
    }

    // PBI-096: Include external (Python/JS) plugins
    if (externalHost_) {
        for (const auto& d : externalHost_->listLoaded()) {
            addPluginRecord(d);
        }
    }

    std::lock_guard lock(statusMutex_);
    statusSnapshot_ = std::move(snap);
}

void PluginManager::__test_setEmbeddingDegraded(bool degraded, const std::string& error) {
    if (degraded) {
        embeddingFsm_.dispatch(ProviderDegradedEvent{error.empty() ? std::string{"test"} : error});
    } else {
        embeddingFsm_.dispatch(ModelLoadedEvent{"", 0});
    }
}

#ifdef YAMS_TESTING
void PluginManager::__test_pluginLoadFailed(const std::string& error) {
    pluginHostFsm_.dispatch(PluginLoadFailedEvent{error});
}

void PluginManager::__test_pluginScanComplete(std::size_t count) {
    pluginHostFsm_.dispatch(AllPluginsLoadedEvent{count});
}
#endif

} // namespace yams::daemon
