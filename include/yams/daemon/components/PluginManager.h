// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/IComponent.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/resource/plugin_host.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

namespace yams::extraction {
class IContentExtractor;
}

namespace yams::daemon {
class ExternalEntityProviderAdapter;
}

namespace yams::daemon {

class AbiPluginLoader;
class AbiPluginHost;
class AbiEntityExtractorAdapter;
class AbiSymbolExtractorAdapter;
class DaemonLifecycleFsm;
class ExternalPluginHost;
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
 * - Plugin status snapshots for diagnostics
 * - FSM tracking (PluginHostFsm, EmbeddingProviderFsm)
 *
 * ## Thread Safety
 * - Status snapshots use shared_mutex for concurrent reads
 * - Autoload uses FSM guards to prevent concurrent scans
 */
class PluginManager : public IComponent {
public:
    /**
     * @brief Plugin status record for a single plugin.
     */
    struct PluginStatusRecord {
        std::string name;
        bool isProvider{false};
        bool ready{false};
        bool degraded{false};
        std::string error;
        std::uint32_t modelsLoaded{0};
    };

    /**
     * @brief Snapshot of all plugin status for diagnostics.
     */
    struct StatusSnapshot {
        PluginHostSnapshot hostState;
        ProviderSnapshot providerState;
        std::vector<PluginStatusRecord> plugins;
    };

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

        /// Function to resolve preferred model name
        std::function<std::string()> resolvePreferredModel;

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
    void shutdown() override;

    // --- Plugin Host Operations ---

    /**
     * @brief Get the ABI plugin host for native C++ plugins.
     */
    AbiPluginHost* getPluginHost() const { return pluginHost_.get(); }

    /**
     * @brief Get the external plugin host for Python/JS plugins.
     */
    ExternalPluginHost* getExternalPluginHost() const { return externalHost_.get(); }

    /**
     * @brief Get the plugin loader.
     */
    AbiPluginLoader* getPluginLoader() const { return pluginLoader_.get(); }

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

    // --- Autoload & Adoption ---

    /**
     * @brief Autoload plugins from configured directories.
     *
     * Scans trust list and default plugin directories, loads discovered plugins,
     * and adopts interfaces (model provider, extractors).
     *
     * @param executor Executor for async operations
     * @return Number of plugins loaded
     */
    boost::asio::awaitable<Result<size_t>> autoloadPlugins(boost::asio::any_io_executor executor);

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

    std::shared_ptr<IModelProvider> getModelProvider() const { return modelProvider_; }
    const std::string& getAdoptedProviderPluginName() const { return adoptedProviderPluginName_; }

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
    const std::string& getEmbeddingModelName() const { return embeddingModelName_; }

    /**
     * @brief Set embedding model name.
     */
    void setEmbeddingModelName(const std::string& name) { embeddingModelName_ = name; }

    /**
     * @brief Get embedding dimension from model provider.
     */
    size_t getEmbeddingDimension() const;

    // --- Status & Diagnostics ---

    /**
     * @brief Get current status snapshot (non-blocking).
     */
    StatusSnapshot getStatusSnapshot() const;

    /**
     * @brief Refresh the cached status snapshot.
     */
    void refreshStatusSnapshot();

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
        } catch (...) {
        }
    }

    /**
     * @brief Dispatch all plugins loaded event to FSM.
     * Used by ServiceManager test helpers.
     */
    void dispatchAllPluginsLoaded(std::size_t count) {
        try {
            pluginHostFsm_.dispatch(AllPluginsLoadedEvent{count});
        } catch (...) {
        }
    }

    /**
     * @brief Set cached model count (for status snapshot).
     */
    void setCachedModelCount(std::uint32_t count) {
        cachedModelCount_.store(count, std::memory_order_relaxed);
    }

    // --- Test Helpers ---
    void __test_setEmbeddingDegraded(bool degraded, const std::string& error);
#ifdef YAMS_TESTING
    void __test_setModelProvider(std::shared_ptr<IModelProvider> provider) {
        modelProvider_ = std::move(provider);
    }
    void __test_pluginLoadFailed(const std::string& error);
    void __test_pluginScanComplete(std::size_t count);
#endif

private:
    AbiPluginHost* getActivePluginHost() const {
        return sharedPluginHost_ ? sharedPluginHost_ : pluginHost_.get();
    }

    Dependencies deps_;

    // Plugin infrastructure
    std::unique_ptr<AbiPluginLoader> pluginLoader_;
    std::unique_ptr<AbiPluginHost> pluginHost_;        // Owned when created internally
    std::unique_ptr<ExternalPluginHost> externalHost_; // For Python/JS plugins
    AbiPluginHost* sharedPluginHost_{nullptr};         // Non-owning when shared from ServiceManager

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

    // Status snapshot cache
    mutable std::shared_mutex statusMutex_;
    StatusSnapshot statusSnapshot_;
    std::atomic<std::uint32_t> cachedModelCount_{0};
};

} // namespace yams::daemon
