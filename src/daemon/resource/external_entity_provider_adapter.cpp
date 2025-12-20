// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/resource/external_entity_provider_adapter.h>
#include <yams/daemon/resource/external_plugin_host.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>

namespace yams::daemon {

// Dynamic batch sizing thresholds
// Smaller binaries can use larger batches; larger binaries need smaller batches to avoid timeouts
constexpr size_t BATCH_SIZE_TINY = 1000;   // < 1MB: fast analysis
constexpr size_t BATCH_SIZE_SMALL = 500;   // 1-5MB: moderate
constexpr size_t BATCH_SIZE_MEDIUM = 200;  // 5-20MB: careful
constexpr size_t BATCH_SIZE_LARGE = 100;   // 20-50MB: slow
constexpr size_t BATCH_SIZE_HUGE = 50;     // > 50MB: very slow

using json = nlohmann::json;

ExternalEntityProviderAdapter::ExternalEntityProviderAdapter(
    ExternalPluginHost* host, std::string pluginName, std::string rpcMethod,
    std::vector<std::string> supportedExtensions, std::chrono::milliseconds timeout)
    : host_(host), pluginName_(std::move(pluginName)), rpcMethod_(std::move(rpcMethod)),
      supportedExtensions_(std::move(supportedExtensions)), timeout_(timeout) {}

bool ExternalEntityProviderAdapter::supports(const std::string& extension) const {
    if (extension.empty())
        return false;

    // Normalize extension to include leading dot
    std::string extNormalized = extension;
    if (extNormalized[0] != '.')
        extNormalized = "." + extNormalized;

    // Case-insensitive comparison
    std::string extLower = extNormalized;
    std::transform(extLower.begin(), extLower.end(), extLower.begin(), ::tolower);

    for (const auto& supported : supportedExtensions_) {
        std::string supportedLower = supported;
        std::transform(supportedLower.begin(), supportedLower.end(), supportedLower.begin(),
                       ::tolower);
        if (extLower == supportedLower)
            return true;
    }

    return false;
}

Result<ExternalEntityProviderAdapter::EntityResult>
ExternalEntityProviderAdapter::extractEntities(const std::vector<std::byte>& bytes,
                                               const std::string& filePath, size_t offset,
                                               size_t limit) {
    if (!host_) {
        return Error{ErrorCode::InvalidState, "No external plugin host"};
    }

    // Serialize access (external plugin can only handle one request at a time)
    std::lock_guard<std::mutex> lock(mutex_);

    try {
        // Encode bytes as base64 for JSON transport
        static constexpr char base64_chars[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        std::string base64;
        base64.reserve(((bytes.size() + 2) / 3) * 4);

        const uint8_t* data = reinterpret_cast<const uint8_t*>(bytes.data());
        size_t len = bytes.size();

        for (size_t i = 0; i < len; i += 3) {
            uint32_t n = static_cast<uint32_t>(data[i]) << 16;
            if (i + 1 < len)
                n |= static_cast<uint32_t>(data[i + 1]) << 8;
            if (i + 2 < len)
                n |= static_cast<uint32_t>(data[i + 2]);

            base64 += base64_chars[(n >> 18) & 0x3F];
            base64 += base64_chars[(n >> 12) & 0x3F];
            base64 += (i + 1 < len) ? base64_chars[(n >> 6) & 0x3F] : '=';
            base64 += (i + 2 < len) ? base64_chars[n & 0x3F] : '=';
        }

        // Build RPC params
        json params = {{"source", {{"type", "bytes"}, {"data", base64}}},
                       {"opts",
                        {{"offset", offset},
                         {"limit", limit},
                         {"include_decompiled", false}, // Don't need decompiled code for KG
                         {"include_call_graph", true}}}};

        spdlog::info("ExternalEntityProviderAdapter[{}]: calling {} ({} bytes, offset={}, limit={})",
                     pluginName_, rpcMethod_, bytes.size(), offset, limit);

        auto result = host_->callRpc(pluginName_, rpcMethod_, params, timeout_);
        if (!result) {
            spdlog::warn("ExternalEntityProviderAdapter[{}]: {} RPC failed", pluginName_, rpcMethod_);
            return Error{ErrorCode::IOError, "Entity extraction RPC failed"};
        }

        const auto& resp = result.value();

        // Parse response into EntityResult
        EntityResult entityResult;

        // Parse binary SHA
        if (resp.contains("binary_sha") && resp["binary_sha"].is_string()) {
            entityResult.binarySha = resp["binary_sha"].get<std::string>();
        }

        // Parse pagination info
        if (resp.contains("total_functions") && resp["total_functions"].is_number()) {
            entityResult.totalFunctions = resp["total_functions"].get<size_t>();
        }
        if (resp.contains("next_offset") && resp["next_offset"].is_number()) {
            entityResult.nextOffset = resp["next_offset"].get<size_t>();
        }
        if (resp.contains("has_more") && resp["has_more"].is_boolean()) {
            entityResult.hasMore = resp["has_more"].get<bool>();
        }

        // Parse nodes
        if (resp.contains("nodes") && resp["nodes"].is_array()) {
            for (const auto& nodeJson : resp["nodes"]) {
                metadata::KGNode node;
                if (nodeJson.contains("node_key") && nodeJson["node_key"].is_string()) {
                    node.nodeKey = nodeJson["node_key"].get<std::string>();
                }
                if (nodeJson.contains("label") && nodeJson["label"].is_string()) {
                    node.label = nodeJson["label"].get<std::string>();
                }
                if (nodeJson.contains("type") && nodeJson["type"].is_string()) {
                    node.type = nodeJson["type"].get<std::string>();
                }
                if (nodeJson.contains("properties")) {
                    node.properties = nodeJson["properties"].dump();
                }
                entityResult.nodes.push_back(std::move(node));
            }
        }

        // Parse edges - note: these use node_key references, not IDs
        // We'll need to resolve keys to IDs during ingestion
        if (resp.contains("edges") && resp["edges"].is_array()) {
            for (const auto& edgeJson : resp["edges"]) {
                metadata::KGEdge edge;
                // Store src_key and dst_key in properties temporarily
                // They'll be resolved to srcNodeId/dstNodeId during ingestion
                json edgeProps;
                if (edgeJson.contains("src_key") && edgeJson["src_key"].is_string()) {
                    edgeProps["_src_key"] = edgeJson["src_key"].get<std::string>();
                }
                if (edgeJson.contains("dst_key") && edgeJson["dst_key"].is_string()) {
                    edgeProps["_dst_key"] = edgeJson["dst_key"].get<std::string>();
                }
                if (edgeJson.contains("relation") && edgeJson["relation"].is_string()) {
                    edge.relation = edgeJson["relation"].get<std::string>();
                }
                if (edgeJson.contains("weight") && edgeJson["weight"].is_number()) {
                    edge.weight = edgeJson["weight"].get<float>();
                } else {
                    edge.weight = 1.0f;
                }
                if (edgeJson.contains("properties") && edgeJson["properties"].is_object()) {
                    for (auto& [k, v] : edgeJson["properties"].items()) {
                        edgeProps[k] = v;
                    }
                }
                edge.properties = edgeProps.dump();
                entityResult.edges.push_back(std::move(edge));
            }
        }

        // Parse aliases - note: these use node_key references
        if (resp.contains("aliases") && resp["aliases"].is_array()) {
            for (const auto& aliasJson : resp["aliases"]) {
                metadata::KGAlias alias;
                // Store node_key in source field temporarily for resolution
                if (aliasJson.contains("node_key") && aliasJson["node_key"].is_string()) {
                    alias.source = "_node_key:" + aliasJson["node_key"].get<std::string>();
                }
                if (aliasJson.contains("alias") && aliasJson["alias"].is_string()) {
                    alias.alias = aliasJson["alias"].get<std::string>();
                }
                if (aliasJson.contains("confidence") && aliasJson["confidence"].is_number()) {
                    alias.confidence = aliasJson["confidence"].get<float>();
                } else {
                    alias.confidence = 1.0f;
                }
                entityResult.aliases.push_back(std::move(alias));
            }
        }

        spdlog::info(
            "ExternalEntityProviderAdapter[{}]: extracted {} nodes, {} edges, {} aliases "
            "(hasMore={}, nextOffset={})",
            pluginName_, entityResult.nodes.size(), entityResult.edges.size(),
            entityResult.aliases.size(), entityResult.hasMore, entityResult.nextOffset);

        return entityResult;

    } catch (const std::exception& e) {
        spdlog::warn("ExternalEntityProviderAdapter[{}]: extraction failed: {}", pluginName_,
                     e.what());
        return Error{ErrorCode::InternalError, std::string("Entity extraction failed: ") + e.what()};
    }
}

size_t ExternalEntityProviderAdapter::calculateBatchSize(size_t binarySize) {
    constexpr size_t MB = 1024 * 1024;
    if (binarySize < 1 * MB) {
        return BATCH_SIZE_TINY;
    } else if (binarySize < 5 * MB) {
        return BATCH_SIZE_SMALL;
    } else if (binarySize < 20 * MB) {
        return BATCH_SIZE_MEDIUM;
    } else if (binarySize < 50 * MB) {
        return BATCH_SIZE_LARGE;
    } else {
        return BATCH_SIZE_HUGE;
    }
}

Result<ExternalEntityProviderAdapter::EntityResult>
ExternalEntityProviderAdapter::extractAllEntities(const std::vector<std::byte>& bytes,
                                                  const std::string& filePath, size_t batchSize) {
    // Use dynamic batch sizing if not specified
    size_t effectiveBatchSize = batchSize > 0 ? batchSize : calculateBatchSize(bytes.size());

    EntityResult combined;
    size_t offset = 0;
    bool firstBatch = true;

    spdlog::info("ExternalEntityProviderAdapter[{}]: extractAllEntities starting "
                 "(size={} bytes, batchSize={})",
                 pluginName_, bytes.size(), effectiveBatchSize);

    while (true) {
        auto batchResult = extractEntities(bytes, filePath, offset, effectiveBatchSize);
        if (!batchResult) {
            return batchResult.error();
        }

        auto& batch = batchResult.value();

        // Copy metadata from first batch
        if (firstBatch) {
            combined.binarySha = batch.binarySha;
            combined.totalFunctions = batch.totalFunctions;
            firstBatch = false;
        }

        // Append entities
        combined.nodes.insert(combined.nodes.end(), std::make_move_iterator(batch.nodes.begin()),
                              std::make_move_iterator(batch.nodes.end()));
        combined.edges.insert(combined.edges.end(), std::make_move_iterator(batch.edges.begin()),
                              std::make_move_iterator(batch.edges.end()));
        combined.aliases.insert(combined.aliases.end(),
                                std::make_move_iterator(batch.aliases.begin()),
                                std::make_move_iterator(batch.aliases.end()));

        if (!batch.hasMore) {
            break;
        }

        offset = batch.nextOffset;
    }

    combined.hasMore = false;
    combined.nextOffset = 0;

    spdlog::info(
        "ExternalEntityProviderAdapter[{}]: extracted all entities: {} nodes, {} edges, {} aliases",
        pluginName_, combined.nodes.size(), combined.edges.size(), combined.aliases.size());

    return combined;
}

Result<ExternalEntityProviderAdapter::ExtractionProgress>
ExternalEntityProviderAdapter::extractEntitiesStreaming(
    const std::vector<std::byte>& bytes, const std::string& filePath,
    BatchCallback callback, size_t batchSize) {

    // Use dynamic batch sizing if not specified
    size_t effectiveBatchSize = batchSize > 0 ? batchSize : calculateBatchSize(bytes.size());

    auto startTime = std::chrono::steady_clock::now();
    ExtractionProgress progress;
    progress.binarySize = bytes.size();

    size_t offset = 0;
    size_t batchNumber = 0;

    spdlog::info("ExternalEntityProviderAdapter[{}]: streaming extraction starting "
                 "(size={} bytes, batchSize={})",
                 pluginName_, bytes.size(), effectiveBatchSize);

    while (true) {
        auto batchResult = extractEntities(bytes, filePath, offset, effectiveBatchSize);
        if (!batchResult) {
            spdlog::warn("ExternalEntityProviderAdapter[{}]: batch {} failed: {}",
                         pluginName_, batchNumber, batchResult.error().message);
            // Return progress so far - partial success is better than total failure
            if (batchNumber > 0) {
                spdlog::info("ExternalEntityProviderAdapter[{}]: returning partial progress "
                             "(batches={}, nodes={}, edges={}, aliases={})",
                             pluginName_, batchNumber, progress.nodesExtracted,
                             progress.edgesExtracted, progress.aliasesExtracted);
            }
            return batchResult.error();
        }

        auto& batch = batchResult.value();
        ++batchNumber;

        // Capture pagination info before potential move
        bool hasMore = batch.hasMore;
        size_t nextOffset = batch.nextOffset;

        // Update progress
        progress.batchNumber = batchNumber;
        progress.functionsProcessed = nextOffset > 0 ? nextOffset : offset + effectiveBatchSize;
        progress.totalFunctions = batch.totalFunctions;
        progress.nodesExtracted += batch.nodes.size();
        progress.edgesExtracted += batch.edges.size();
        progress.aliasesExtracted += batch.aliases.size();

        auto elapsed = std::chrono::steady_clock::now() - startTime;
        progress.elapsedSeconds = std::chrono::duration<double>(elapsed).count();

        // Estimate total batches
        if (batch.totalFunctions > 0 && effectiveBatchSize > 0) {
            progress.totalBatchesEstimate = (batch.totalFunctions + effectiveBatchSize - 1) / effectiveBatchSize;
        }

        spdlog::info("ExternalEntityProviderAdapter[{}]: batch {}/{} complete "
                     "(funcs={}/{}, nodes={}, edges={}, aliases={}, elapsed={:.1f}s)",
                     pluginName_, batchNumber, progress.totalBatchesEstimate,
                     progress.functionsProcessed, progress.totalFunctions,
                     batch.nodes.size(), batch.edges.size(), batch.aliases.size(),
                     progress.elapsedSeconds);

        // Call the callback with this batch - it can ingest immediately
        if (!callback(std::move(batch), progress)) {
            spdlog::info("ExternalEntityProviderAdapter[{}]: extraction aborted by callback",
                         pluginName_);
            break;
        }

        if (!hasMore) {
            break;
        }

        offset = nextOffset;

        // Safety check: prevent infinite loop if nextOffset doesn't advance
        if (offset == 0 && batchNumber > 1) {
            spdlog::warn("ExternalEntityProviderAdapter[{}]: nextOffset=0 after batch {}, stopping",
                         pluginName_, batchNumber);
            break;
        }
    }

    spdlog::info("ExternalEntityProviderAdapter[{}]: streaming extraction complete "
                 "(batches={}, nodes={}, edges={}, aliases={}, elapsed={:.1f}s)",
                 pluginName_, progress.batchNumber, progress.nodesExtracted,
                 progress.edgesExtracted, progress.aliasesExtracted, progress.elapsedSeconds);

    return progress;
}

bool ExternalEntityProviderAdapter::isBusy() const {
    if (mutex_.try_lock()) {
        mutex_.unlock();
        return false;
    }
    return true;
}

} // namespace yams::daemon
