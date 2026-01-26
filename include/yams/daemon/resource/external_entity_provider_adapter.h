// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0
//
// Adapter for external plugin kg_entity_provider_v1 interface.
// Enables ingestion of KG entities from binary analysis plugins like Ghidra.

#pragma once

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace yams::daemon {

class ExternalPluginHost;

/**
 * @brief Adapter for external plugins implementing kg_entity_provider_v1.
 *
 * This adapter wraps JSON-RPC calls to external plugins (like yams-ghidra-plugin)
 * that implement the kg_entity_provider_v1 interface. It translates the JSON
 * response into internal KGNode/KGEdge/KGAlias types for ingestion.
 *
 * The plugin's RPC method (e.g., "ghidra.getEntities") returns:
 * - nodes: Array of {node_key, label, type, properties}
 * - edges: Array of {src_key, dst_key, relation, weight, properties}
 * - aliases: Array of {node_key, alias, source, confidence}
 * - binary_sha: SHA256 hash of the analyzed binary
 * - has_more: Whether more results are available (pagination)
 * - next_offset: Offset for next batch
 */
class ExternalEntityProviderAdapter {
public:
    /**
     * @brief Result of entity extraction.
     */
    struct EntityResult {
        std::vector<metadata::KGNode> nodes;
        std::vector<metadata::KGEdge> edges;
        std::vector<metadata::KGAlias> aliases;
        std::string binarySha;
        size_t totalFunctions{0};
        size_t nextOffset{0};
        bool hasMore{false};
    };

    /**
     * @brief Progress information for streaming extraction.
     */
    struct ExtractionProgress {
        size_t batchNumber{0};
        size_t totalBatchesEstimate{0};
        size_t functionsProcessed{0};
        size_t totalFunctions{0};
        size_t nodesExtracted{0};
        size_t edgesExtracted{0};
        size_t aliasesExtracted{0};
        double elapsedSeconds{0.0};
        size_t binarySize{0};
    };

    /**
     * @brief Callback for streaming entity extraction.
     *
     * Called after each batch is extracted. Return false to abort extraction.
     */
    using BatchCallback =
        std::function<bool(EntityResult batch, const ExtractionProgress& progress)>;

    /**
     * @brief Construct adapter for an external plugin.
     *
     * @param host External plugin host for RPC communication
     * @param pluginName Name of the plugin to call
     * @param rpcMethod RPC method name (e.g., "ghidra.getEntities")
     * @param supportedExtensions File extensions this provider supports (e.g., ".exe", ".dll")
     * @param timeout RPC timeout per batch (default 15 minutes for heavy analysis)
     */
    ExternalEntityProviderAdapter(ExternalPluginHost* host, std::string pluginName,
                                  std::string rpcMethod,
                                  std::vector<std::string> supportedExtensions,
                                  std::chrono::milliseconds timeout = std::chrono::minutes{15});

    ~ExternalEntityProviderAdapter() = default;

    // Non-copyable, non-movable (due to mutex)
    ExternalEntityProviderAdapter(const ExternalEntityProviderAdapter&) = delete;
    ExternalEntityProviderAdapter& operator=(const ExternalEntityProviderAdapter&) = delete;
    ExternalEntityProviderAdapter(ExternalEntityProviderAdapter&&) = delete;
    ExternalEntityProviderAdapter& operator=(ExternalEntityProviderAdapter&&) = delete;

    /**
     * @brief Check if this provider supports the given file extension.
     */
    bool supports(const std::string& extension) const;

    /**
     * @brief Extract KG entities from binary data.
     *
     * Calls the plugin's entity extraction RPC and translates the response.
     * Supports pagination for large binaries via offset/limit.
     *
     * @param bytes Binary file content
     * @param filePath Path to the file (for metadata)
     * @param offset Starting function index for pagination
     * @param limit Maximum entities to return
     * @return EntityResult on success, Error on failure
     */
    Result<EntityResult> extractEntities(const std::vector<std::byte>& bytes,
                                         const std::string& filePath, size_t offset = 0,
                                         size_t limit = 500);

    /**
     * @brief Extract all entities with automatic pagination.
     *
     * Repeatedly calls extractEntities until hasMore is false, accumulating results.
     *
     * @param bytes Binary file content
     * @param filePath Path to the file (for metadata)
     * @param batchSize Entities per batch (0 = auto-size based on binary)
     * @return EntityResult containing all entities
     */
    Result<EntityResult> extractAllEntities(const std::vector<std::byte>& bytes,
                                            const std::string& filePath, size_t batchSize = 0);

    /**
     * @brief Extract entities with streaming callback for per-batch processing.
     *
     * Calls the callback after each batch, allowing immediate KG insertion.
     * Dynamic batch sizing based on binary size when batchSize=0.
     *
     * @param bytes Binary file content
     * @param filePath Path to the file (for metadata)
     * @param callback Called after each batch; return false to abort
     * @param batchSize Entities per batch (0 = auto-size based on binary)
     * @return Total extraction stats on success
     */
    Result<ExtractionProgress> extractEntitiesStreaming(const std::vector<std::byte>& bytes,
                                                        const std::string& filePath,
                                                        BatchCallback callback,
                                                        size_t batchSize = 0);

    /**
     * @brief Calculate optimal batch size based on binary size.
     *
     * Smaller binaries get larger batches (faster), larger binaries get
     * smaller batches (avoid timeouts).
     */
    static size_t calculateBatchSize(size_t binarySize);

    /// Get the plugin name
    const std::string& name() const { return pluginName_; }

    /// Get the RPC method name
    const std::string& rpcMethod() const { return rpcMethod_; }

    /// Get supported extensions
    const std::vector<std::string>& supportedExtensions() const { return supportedExtensions_; }

    /// Check if currently busy (for backpressure)
    bool isBusy() const;

private:
    Result<EntityResult> extractEntitiesWithBase64(const std::string& base64,
                                                   const std::string& filePath, size_t offset,
                                                   size_t limit);
    ExternalPluginHost* host_;
    std::string pluginName_;
    std::string rpcMethod_;
    std::vector<std::string> supportedExtensions_;
    std::chrono::milliseconds timeout_;
    mutable std::mutex mutex_;
};

} // namespace yams::daemon
