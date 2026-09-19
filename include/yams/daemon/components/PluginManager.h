// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/IComponent.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/test_hooks.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/plugin_host.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace yams::extraction {
class IContentExtractor;
}

namespace yams::daemon {
class ExternalEntityProviderAdapter;
}

namespace yams::daemon {
class AbiPluginHost;
class AbiEntityExtractorAdapter;
class AbiSymbolExtractorAdapter;
class DaemonLifecycleFsm;
class IModelProvider;
class PostIngestQueue;
struct StateComponent;
struct DaemonConfig;
struct ExternalPluginHostConfig;

std::string adjustOnnxConfigJson(const std::string& configJson, std::size_t defaultMax);

/**
 * @brief Manages plugin lifecycle, loading, and interface adoption.
 *
 * Extracted from ServiceManager (PBI-088) to centralize plugin concerns.
 *
 * ## Responsibilities
 * - Plugin host and loader ownership
 * - Trust management
 * - Plugin autoloading from configured directories
 * - Interface adoption (model provider, content extractors, symbol extractors)
 * - FSM tracking (PluginHostFsm, EmbeddingProviderFsm)
 *
 * ## Thread Safety
 * - Autoload uses FSM guards to prevent concurrent scans
 */
class PluginManager : public IComponent {
public:
    /**
     * @brief Dependency injection for PluginManager.
     */
    struct Dependencies {
        /// Daemon configuration
        const DaemonConfig* config{nullptr};

        /// State component for readiness tracking
        StateComponent* state{nullptr};

        /// Lifecycle FSM for degradation reporting
        DaemonLifecycleFsm* lifecycleFsm{nullptr};

        /// Resolved data directory
        std::filesystem::path dataDir;

        /// Functions backed by the immutable daemon embedding snapshot.
        std::function<std::string()> resolvePreferredModel;
        std::function<std::string()> resolveEmbeddingBackend;

        /// Optional: shared plugin host (if null, PluginManager creates its own)
        AbiPluginHost* sharedPluginHost{nullptr};

        /// PostIngestQueue for wiring entity providers
        PostIngestQueue* postIngestQueue{nullptr};
    };

    explicit PluginManager(Dependencies deps);
    ~PluginManager() override;

    // IComponent interface
    const char* getName() const override { return "PluginManager"; }
    Result<void> initialize() override;
    void shutdown() noexcept override;

    // --- Plugin Host Operations ---

    enum class PluginHostKind { Abi, External };

    struct DiscoveredPlugin {
        PluginHostKind host;
        PluginDescriptor descriptor;
    };

    /**
     * @brief Get the active ABI plugin host for native C++ plugins.
     */
    AbiPluginHost* getPluginHost() const { return getActivePluginHost(); }

    /**
     * @brief Get the external plugin host for Python/JS plugins.
     */
    ExternalPluginHost* getExternalPluginHost() const { return externalHost_.get(); }

    /**
     * @brief Set the PostIngestQueue for wiring entity providers.
     * Called after PostIngestQueue is created during async initialization.
     */
    void setPostIngestQueue(PostIngestQueue* queue) { postIngestQueue_ = queue; }

    /**
     * @brief Get trust list paths.
     */
    std::vector<std::filesystem::path> trustList() const;

    /**
     * @brief Add a path to the trust list.
     */
    Result<void> trustAdd(const std::filesystem::path& path);

    /**
     * @brief Remove a path from the trust list.
     */
    Result<void> trustRemove(const std::filesystem::path& path);

    /**
     * @brief Load a scanned plugin with daemon configuration applied.
     *
     * Resolves short-name aliases from DaemonConfig::pluginConfigs and applies
     * model-provider constraints such as the ONNX model-pool minimum. A non-empty
     * explicitConfigJson overrides pluginConfigs before constraints are applied.
     */
    Result<PluginDescriptor> loadConfiguredPlugin(IPluginHost& host,
                                                  const PluginDescriptor& descriptor,
                                                  std::string explicitConfigJson = {}) const;

    /**
     * @brief Discover one plugin target using canonical ABI/external routing.
     */
    Result<DiscoveredPlugin> scanPluginTarget(const std::filesystem::path& target) const;

    /**
     * @brief Discover all ABI and external plugins in an explicit directory.
     */
    Result<std::vector<DiscoveredPlugin>>
    scanPluginDirectory(const std::filesystem::path& directory) const;

    /**
     * @brief Discover plugins from trusted and configured default roots.
     */
    Result<std::vector<DiscoveredPlugin>> scanConfiguredPluginRoots() const;

    /**
     * @brief Resolve a direct path or plugin name against canonical scan roots.
     */
    Result<std::filesystem::path> resolvePluginTarget(const std::filesystem::path& target) const;

    /**
     * @brief Load a discovered plugin through its owning host with configuration applied.
     */
    Result<PluginDescriptor> loadDiscoveredPlugin(const DiscoveredPlugin& discovered,
                                                  std::string explicitConfigJson = {});

    /**
     * @brief Discover and load one direct path or plugin name.
     */
    Result<PluginDescriptor> loadPluginTarget(const std::filesystem::path& target,
                                              std::string explicitConfigJson = {});

    /**
     * @brief Discover and load every plugin in an explicit directory.
     */
    Result<std::vector<PluginDescriptor>>
    loadPluginDirectory(const std::filesystem::path& directory);

    /**
     * @brief Unload a plugin through its owning host and reconcile FSM state.
     */
    Result<void> unloadPlugin(const std::string& name);

    /**
     * @brief Check whether a file or directory already owns a loaded plugin.
     */
    bool isPluginLoadedFrom(const std::filesystem::path& path) const;

    // --- Autoload & Adoption ---

    /**
     * @brief Autoload plugins from configured directories.
     *
     * Scans trust list and default plugin directories, loads discovered plugins,
     * and adopts interfaces (model provider, extractors).
     *
     * @return Number of plugins loaded
     */
    Result<size_t> autoloadPlugins();

    /**
     * @brief Adopt model provider from loaded plugins.
     *
     * Scans loaded plugins for model_provider_v1 interface and adopts first valid one.
     *
     * @param preferredName Optional preferred plugin name to try first
     * @return true if a provider was adopted
     */
    Result<bool> adoptModelProvider(const std::string& preferredName = "");

    /**
     * @brief Adopt content extractors from loaded plugins.
     *
     * @return Number of extractors adopted
     */
    Result<size_t> adoptContentExtractors();

    /**
     * @brief Adopt symbol extractors from loaded plugins.
     *
     * @return Number of extractors adopted
     */
    Result<size_t> adoptSymbolExtractors();

    /**
     * @brief Adopt entity providers from loaded external plugins.
     *
     * Entity providers extract KG entities (nodes, edges, aliases) from binary files.
     * Currently supports external plugins implementing kg_entity_provider_v1.
     *
     * @return Number of providers adopted
     */
    Result<size_t> adoptEntityProviders();

    /**
     * @brief Adopt NL entity extractors from loaded plugins (e.g., Glint).
     *
     * Entity extractors implement entity_extractor_v2 for extracting named entities
     * (person, organization, location, etc.) from natural language text.
     *
     * @return Number of extractors adopted
     */
    Result<size_t> adoptEntityExtractors();

    // --- Adopted Interface Accessors ---

    std::shared_ptr<IModelProvider> getModelProvider() const;
    std::string getAdoptedProviderPluginName() const;

    const std::vector<std::shared_ptr<extraction::IContentExtractor>>&
    getContentExtractors() const {
        return contentExtractors_;
    }

    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& getSymbolExtractors() const {
        return symbolExtractors_;
    }

    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& getEntityProviders() const {
        return entityProviders_;
    }

    const std::vector<std::shared_ptr<AbiEntityExtractorAdapter>>& getEntityExtractors() const {
        return entityExtractors_;
    }

    // --- Model Provider State ---

    /**
     * @brief Check if model provider is in degraded state.
     */
    bool isModelProviderDegraded() const;

    /**
     * @brief Get last model provider error.
     */
    std::string lastModelError() const;

    /**
     * @brief Clear model provider degradation state.
     */
    void clearModelProviderError();

    /**
     * @brief Get embedding model name (set after successful adoption).
     */
    std::string getEmbeddingModelName() const;

    /**
     * @brief Set embedding model name.
     */
    void setEmbeddingModelName(const std::string& name);

    /**
     * @brief Get embedding dimension from model provider.
     */
    size_t getEmbeddingDimension() const;

    // --- Status & Diagnostics ---

    /**
     * @brief Get plugin host FSM snapshot.
     */
    PluginHostSnapshot getPluginHostFsmSnapshot() const { return pluginHostFsm_.snapshot(); }

    /**
     * @brief Get embedding provider FSM snapshot.
     */
    ProviderSnapshot getEmbeddingProviderFsmSnapshot() const { return embeddingFsm_.snapshot(); }

    /**
     * @brief Dispatch plugin load failed event to FSM.
     * Used by ServiceManager test helpers.
     */
    void dispatchPluginLoadFailed(const std::string& error) {
        try {
            pluginHostFsm_.dispatch(PluginLoadFailedEvent{error});
        } catch (const std::exception&) {
            ignoreFsmDispatchFailure();
        } catch (...) {
            ignoreFsmDispatchFailure();
        }
    }

    /**
     * @brief Dispatch all plugins loaded event to FSM.
     * Used by ServiceManager test helpers.
     */
    void dispatchAllPluginsLoaded(std::size_t count) {
        try {
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{count});
        } catch (const std::exception&) {
            ignoreFsmDispatchFailure();
        } catch (...) {
            ignoreFsmDispatchFailure();
        }
    }

    void dispatchPluginScanStarted(std::size_t directoryCount) {
        try {
            pluginHostFsm_.dispatch(PluginScanStartedEvent{directoryCount});
        } catch (const std::exception&) {
            ignoreFsmDispatchFailure();
        } catch (...) {
            ignoreFsmDispatchFailure();
        }
    }

    void dispatchPluginLoaded(const std::string& name) {
        try {
            pluginHostFsm_.dispatch(PluginLoadedEvent{name});
        } catch (const std::exception&) {
            ignoreFsmDispatchFailure();
        } catch (...) {
            ignoreFsmDispatchFailure();
        }
    }

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
    YAMS_DAEMON_TEST_HOOK void testingSetEmbeddingDegraded(bool degraded, const std::string& error);
#endif
#ifdef YAMS_TESTING
    void testingSetExternalPluginHost(std::unique_ptr<ExternalPluginHost> host) {
        externalHost_ = std::move(host);
    }
    void testingSetSharedPluginHost(AbiPluginHost* host) { sharedPluginHost_ = host; }
#endif

private:
    static void ignoreFsmDispatchFailure() noexcept {}

    AbiPluginHost* getActivePluginHost() const {
        return sharedPluginHost_ ? sharedPluginHost_ : pluginHost_.get();
    }
    IPluginHost* getHost(PluginHostKind kind) const;
    std::vector<std::pair<PluginHostKind, std::filesystem::path>> pluginScanRoots() const;

    Dependencies deps_;
    mutable std::recursive_mutex pluginLifecycleMutex_;

    // Plugin infrastructure
    std::unique_ptr<AbiPluginHost> pluginHost_;        // Owned when created internally
    std::unique_ptr<ExternalPluginHost> externalHost_; // For Python/JS plugins
    AbiPluginHost* sharedPluginHost_{nullptr};         // Non-owning when shared from ServiceManager
    std::atomic<bool> shutdownInvoked_{false};

    // FSMs for state tracking
    PluginHostFsm pluginHostFsm_;
    EmbeddingProviderFsm embeddingFsm_;

    // Adopted interfaces
    std::shared_ptr<IModelProvider> modelProvider_;
    std::string adoptedProviderPluginName_;
    std::string embeddingModelName_;
    std::vector<std::shared_ptr<extraction::IContentExtractor>> contentExtractors_;
    std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> symbolExtractors_;
    std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> entityExtractors_;
    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders_;
    PostIngestQueue* postIngestQueue_{nullptr};
};

} // namespace yams::daemon
