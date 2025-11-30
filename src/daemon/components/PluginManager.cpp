// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/PluginManager.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/resource/abi_content_extractor_adapter.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/config/config_helpers.h>
#include <yams/plugins/model_provider_v1.h>
#include <yams/plugins/content_extractor_v1.h>
#include <yams/plugins/symbol_extractor_v1.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <fstream>
#include <sstream>

namespace yams::daemon {

// Template-based plugin adoption helper
template <typename AbiTableType, typename AdapterType, typename ContainerValueType>
static size_t
adoptPluginInterfaceImpl(AbiPluginHost* host, const std::string& interfaceName, int interfaceVersion,
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

PluginManager::~PluginManager() { shutdown(); }

Result<void> PluginManager::initialize() {
    spdlog::debug("[PluginManager] Initializing");

    // Create plugin loader
    pluginLoader_ = std::make_unique<AbiPluginLoader>();

    // Determine trust file path (default to dataDir/plugins.trust)
    std::filesystem::path trustFile = deps_.dataDir / "plugins.trust";

    // Create plugin host (pass nullptr for ServiceManager - we don't need circular dep)
    // The host only uses ServiceManager for config, which we pass via deps_
    pluginHost_ = std::make_unique<AbiPluginHost>(nullptr, trustFile);

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

    spdlog::info("[PluginManager] Initialized with trust file: {}", trustFile.string());
    return Result<void>{};
}

void PluginManager::shutdown() {
    spdlog::debug("[PluginManager] Shutting down");

    // Clear adopted interfaces
    modelProvider_.reset();
    contentExtractors_.clear();
    symbolExtractors_.clear();

    // Unload plugins
    if (pluginHost_) {
        for (const auto& d : pluginHost_->listLoaded()) {
            try {
                pluginHost_->unload(d.name);
            } catch (...) {
            }
        }
    }

    pluginHost_.reset();
    pluginLoader_.reset();
}

std::vector<std::filesystem::path> PluginManager::trustList() const {
    if (!pluginHost_)
        return {};
    return pluginHost_->trustList();
}

Result<void> PluginManager::trustAdd(const std::filesystem::path& path) {
    if (!pluginHost_)
        return Error{ErrorCode::InvalidState, "Plugin host not initialized"};
    return pluginHost_->trustAdd(path);
}

Result<void> PluginManager::trustRemove(const std::filesystem::path& path) {
    if (!pluginHost_)
        return Error{ErrorCode::InvalidState, "Plugin host not initialized"};
    return pluginHost_->trustRemove(path);
}

boost::asio::awaitable<Result<size_t>>
PluginManager::autoloadPlugins(boost::asio::any_io_executor executor) {
    size_t loadedCount = 0;

    try {
        // FSM guard: avoid concurrent autoload scans
        auto ps = pluginHostFsm_.snapshot().state;
        if (ps == PluginHostState::ScanningDirectories || ps == PluginHostState::LoadingPlugins) {
            spdlog::warn("[PluginManager] autoload skipped: scan already in progress");
            co_return Result<size_t>(0);
        }

        // Check for mock provider or disabled plugins
        if (deps_.config && deps_.config->useMockModelProvider) {
            spdlog::info("[PluginManager] autoload skipped (mock provider in use)");
            co_return Result<size_t>(0);
        }
        if (ConfigResolver::envTruthy(std::getenv("YAMS_USE_MOCK_PROVIDER"))) {
            spdlog::info("[PluginManager] autoload skipped (mock provider via env)");
            co_return Result<size_t>(0);
        }
        if (const char* d = std::getenv("YAMS_DISABLE_ABI_PLUGINS"); d && *d) {
            spdlog::info("[PluginManager] autoload disabled by YAMS_DISABLE_ABI_PLUGINS");
            co_return Result<size_t>(0);
        }

        // Build list of plugin directories to scan
        std::vector<std::filesystem::path> roots;

        // Add trust list paths
        if (pluginHost_) {
            for (const auto& p : pluginHost_->trustList())
                roots.push_back(p);
        }

        // Add platform-specific default directories
        namespace fs = std::filesystem;
#ifdef _WIN32
        roots.push_back(yams::config::get_data_dir() / "plugins");
#else
        if (const char* home = std::getenv("HOME")) {
            roots.push_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
        }
        roots.push_back(fs::path("/usr/local/lib/yams/plugins"));
        roots.push_back(fs::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
        roots.push_back(fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins");
#endif

        // Deduplicate
        std::sort(roots.begin(), roots.end());
        roots.erase(std::unique(roots.begin(), roots.end()), roots.end());

        spdlog::info("[PluginManager] autoload: {} roots to scan", roots.size());
        pluginHostFsm_.dispatch(PluginScanStartedEvent{roots.size()});

        // Collect load tasks
        std::vector<boost::asio::awaitable<Result<PluginDescriptor>>> loadTasks;

        for (const auto& root : roots) {
            spdlog::info("[PluginManager] scanning: {}", root.string());

            if (!pluginHost_)
                continue;

            auto scanResult = pluginHost_->scanDirectory(root);
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
                auto host = pluginHost_.get();
                auto path = desc.path;
                loadTasks.push_back(boost::asio::co_spawn(
                    executor,
                    [host, path]() -> boost::asio::awaitable<Result<PluginDescriptor>> {
                        co_return host->load(path, "");
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

        spdlog::info("[PluginManager] autoload complete: {} plugin(s) loaded", loadedCount);
        pluginHostFsm_.dispatch(AllPluginsLoadedEvent{loadedCount});

        // Adopt interfaces
        auto providerResult = adoptModelProvider();
        if (providerResult && providerResult.value()) {
            spdlog::info("[PluginManager] model provider adopted");
        }

        adoptContentExtractors();
        adoptSymbolExtractors();

        refreshStatusSnapshot();
        co_return Result<size_t>(loadedCount);

    } catch (const std::exception& e) {
        spdlog::error("[PluginManager] autoload exception: {}", e.what());
        co_return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<bool> PluginManager::adoptModelProvider(const std::string& preferredName) {
    if (!pluginHost_) {
        return Error{ErrorCode::InvalidState, "Plugin host not initialized"};
    }

    try {
        auto loaded = pluginHost_->listLoaded();

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
            auto ifaceRes = pluginHost_->getInterface(pluginName, "model_provider_v1", 2);
            if (!ifaceRes) {
                spdlog::debug("[PluginManager] No model_provider_v1 interface for '{}'", pluginName);
                return false;
            }

            auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
            if (!table) {
                spdlog::debug("[PluginManager] Null provider table for '{}'", pluginName);
                return false;
            }

            if (table->abi_version != YAMS_IFACE_MODEL_PROVIDER_V1_VERSION) {
                spdlog::debug("[PluginManager] ABI mismatch for '{}': got v{}, expected v{}",
                              pluginName, table->abi_version, YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);
                embeddingFsm_.dispatch(ProviderDegradedEvent{"ABI mismatch: " + pluginName});
                return false;
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
        for (const auto& d : loaded) {
            if (tryAdopt(d.name))
                return Result<bool>(true);

            // Try path stem as alternate name
            try {
                std::string alt = std::filesystem::path(d.path).stem().string();
                if (!alt.empty() && alt != d.name && tryAdopt(alt))
                    return Result<bool>(true);
            } catch (...) {
            }
        }

        // No provider found
        embeddingFsm_.dispatch(ProviderDegradedEvent{"no provider adopted"});
        refreshStatusSnapshot();
        return Result<bool>(false);

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptContentExtractors() {
    try {
        size_t adopted = adoptPluginInterfaceImpl<yams_content_extractor_v1,
                                                   AbiContentExtractorAdapter,
                                                   extraction::IContentExtractor>(
            pluginHost_.get(), "content_extractor_v1", YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION,
            contentExtractors_, [](const yams_content_extractor_v1* table) {
                return table->abi_version == YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION;
            });
        return Result<size_t>(adopted);
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<size_t> PluginManager::adoptSymbolExtractors() {
    try {
        size_t adopted =
            adoptPluginInterfaceImpl<yams_symbol_extractor_v1, AbiSymbolExtractorAdapter,
                                     AbiSymbolExtractorAdapter>(
                pluginHost_.get(), "symbol_extractor_v1", YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION,
                symbolExtractors_, [](const yams_symbol_extractor_v1* table) {
                    return table->abi_version == YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION;
                });
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

    // Build plugin records
    if (pluginHost_) {
        for (const auto& d : pluginHost_->listLoaded()) {
            PluginStatusRecord rec;
            rec.name = d.name;
            rec.isProvider = (d.name == adoptedProviderPluginName_);
            rec.ready = rec.isProvider && modelProvider_ && modelProvider_->isAvailable();
            rec.degraded = rec.isProvider && isModelProviderDegraded();
            if (rec.isProvider) {
                rec.modelsLoaded = cachedModelCount_.load(std::memory_order_relaxed);
            }
            snap.plugins.push_back(std::move(rec));
        }
    }

    std::lock_guard lock(statusMutex_);
    statusSnapshot_ = std::move(snap);
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
