// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/GraphComponent.h>

#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>

#include <unordered_set>

namespace yams::daemon {

GraphComponent::GraphComponent(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                               std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)) {}

GraphComponent::~GraphComponent() {
    shutdown();
}

Result<void> GraphComponent::initialize() {
    if (initialized_) {
        return Result<void>();
    }

    if (!metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::InvalidArgument, "GraphComponent: missing required dependencies"};
    }

    queryService_ = app::services::makeGraphQueryService(kgStore_, metadataRepo_);
    if (!queryService_) {
        return Error{ErrorCode::InternalError, "GraphComponent: failed to create query service"};
    }

    initialized_ = true;
    spdlog::info("[GraphComponent] Initialized successfully");
    return Result<void>();
}

void GraphComponent::shutdown() {
    if (!initialized_) {
        return;
    }

    if (entityService_) {
        entityService_->stop();
        entityService_.reset();
    }

    queryService_.reset();
    initialized_ = false;
    spdlog::info("[GraphComponent] Shutdown complete");
}

bool GraphComponent::isReady() const {
    return initialized_ && queryService_ != nullptr;
}

Result<void> GraphComponent::onDocumentIngested(const DocumentGraphContext& ctx) {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    try {
        metadata::KGNode docNode;
        docNode.nodeKey = "doc:" + ctx.documentHash;
        docNode.type = "document";
        docNode.label =
            !ctx.filePath.empty() ? std::optional<std::string>(ctx.filePath) : std::nullopt;

        auto docNodeResult = kgStore_->upsertNode(docNode);
        if (!docNodeResult) {
            return Error{ErrorCode::InternalError,
                         "Failed to create document node: " + docNodeResult.error().message};
        }

        int64_t docNodeId = docNodeResult.value();

        for (const auto& tag : ctx.tags) {
            metadata::KGNode tagNode;
            tagNode.nodeKey = "tag:" + tag;
            tagNode.type = "tag";

            auto tagResult = kgStore_->upsertNode(tagNode);
            if (tagResult) {
                metadata::KGEdge edge;
                edge.srcNodeId = docNodeId;
                edge.dstNodeId = tagResult.value();
                edge.relation = "HAS_TAG";
                (void)kgStore_->addEdge(edge);
            }
        }

        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     "Document ingestion failed: " + std::string(e.what())};
    }
}

Result<void>
GraphComponent::onTreeDiffApplied(int64_t diffId,
                                  const std::vector<metadata::TreeChangeRecord>& changes) {
    if (!initialized_ || !kgStore_ || !metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    try {
        for (const auto& change : changes) {
            if (!change.oldHash.empty()) {
                auto oldBlobRes = kgStore_->ensureBlobNode(change.oldHash);
                if (!oldBlobRes) {
                    spdlog::warn("Failed to create KG blob node for hash={}: {}", change.oldHash,
                                 oldBlobRes.error().message);
                }
            }
            if (!change.newHash.empty()) {
                auto newBlobRes = kgStore_->ensureBlobNode(change.newHash);
                if (!newBlobRes) {
                    spdlog::warn("Failed to create KG blob node for hash={}: {}", change.newHash,
                                 newBlobRes.error().message);
                }
            }

            std::optional<int64_t> oldPathNodeId;
            std::optional<int64_t> newPathNodeId;

            if (!change.oldPath.empty()) {
                metadata::PathNodeDescriptor oldDesc{.snapshotId = "",
                                                     .path = change.oldPath,
                                                     .rootTreeHash = "",
                                                     .isDirectory = change.isDirectory};
                auto oldPathRes = kgStore_->ensurePathNode(oldDesc);
                if (oldPathRes) {
                    oldPathNodeId = oldPathRes.value();
                } else {
                    spdlog::warn("Failed to create KG path node for path={}: {}", change.oldPath,
                                 oldPathRes.error().message);
                }
            }

            if (!change.newPath.empty()) {
                metadata::PathNodeDescriptor newDesc{.snapshotId = "",
                                                     .path = change.newPath,
                                                     .rootTreeHash = "",
                                                     .isDirectory = change.isDirectory};
                auto newPathRes = kgStore_->ensurePathNode(newDesc);
                if (newPathRes) {
                    newPathNodeId = newPathRes.value();
                } else {
                    spdlog::warn("Failed to create KG path node for path={}: {}", change.newPath,
                                 newPathRes.error().message);
                }
            }

            if (oldPathNodeId && !change.oldHash.empty()) {
                auto oldBlobNodeRes = kgStore_->ensureBlobNode(change.oldHash);
                if (oldBlobNodeRes) {
                    auto linkRes =
                        kgStore_->linkPathVersion(*oldPathNodeId, oldBlobNodeRes.value(), diffId);
                    if (!linkRes) {
                        spdlog::warn("Failed to link path to blob in KG: path={}, hash={}",
                                     change.oldPath, change.oldHash);
                    }
                }
            }

            if (newPathNodeId && !change.newHash.empty()) {
                auto newBlobNodeRes = kgStore_->ensureBlobNode(change.newHash);
                if (newBlobNodeRes) {
                    auto linkRes =
                        kgStore_->linkPathVersion(*newPathNodeId, newBlobNodeRes.value(), diffId);
                    if (!linkRes) {
                        spdlog::warn("Failed to link path to blob in KG: path={}, hash={}",
                                     change.newPath, change.newHash);
                    }
                }
            }

            if ((change.type == metadata::TreeChangeType::Renamed ||
                 change.type == metadata::TreeChangeType::Moved) &&
                oldPathNodeId && newPathNodeId) {
                auto renameRes = kgStore_->recordRenameEdge(*oldPathNodeId, *newPathNodeId, diffId);
                if (!renameRes) {
                    spdlog::warn("Failed to record rename edge in KG: {} -> {}", change.oldPath,
                                 change.newPath);
                }
            }
        }

        spdlog::debug("[GraphComponent] Populated KG with {} tree changes for diff_id={}",
                      changes.size(), diffId);
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     "Tree diff KG update failed: " + std::string(e.what())};
    }
}

Result<void> GraphComponent::submitEntityExtraction(EntityExtractionJob job) {
    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    if (!entityService_) {
        return Error{ErrorCode::NotSupported, "EntityGraphService not available"};
    }

    EntityGraphService::Job entityJob{
        .documentHash = std::move(job.documentHash),
        .filePath = std::move(job.filePath),
        .contentUtf8 = std::move(job.contentUtf8),
        .language = std::move(job.language),
    };

    return entityService_->submitExtraction(std::move(entityJob));
}

Result<GraphComponent::RepairStats> GraphComponent::repairGraph(bool dryRun) {
    if (!initialized_ || !metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    try {
        RepairStats stats;

        spdlog::info("[GraphComponent] Starting graph repair (dryRun={})", dryRun);

        auto docsResult = metadata::queryDocumentsByPattern(*metadataRepo_, "%");
        if (!docsResult) {
            return Error{ErrorCode::InternalError,
                         "Failed to query documents: " + docsResult.error().message};
        }

        const auto& docs = docsResult.value();
        uint64_t processed = 0;

        auto upsertNode =
            [&](const std::string& key, const std::string& type,
                const std::optional<std::string>& label,
                const std::optional<std::string>& propsJson) -> std::optional<int64_t> {
            if (dryRun) {
                stats.nodesCreated++;
                return 1;
            }

            metadata::KGNode node;
            node.nodeKey = key;
            node.type = type;
            node.label = label;
            node.properties = propsJson;
            auto result = kgStore_->upsertNode(node);
            if (!result) {
                stats.errors++;
                stats.issues.push_back("Failed to upsert node: " + key);
                return std::nullopt;
            }
            stats.nodesCreated++;
            return result.value();
        };

        for (const auto& doc : docs) {
            processed++;

            std::unordered_set<int64_t> existingNodes;
            if (!dryRun) {
                auto entitiesResult = kgStore_->getDocEntitiesForDocument(doc.id, 5000, 0);
                if (entitiesResult) {
                    for (const auto& entity : entitiesResult.value()) {
                        if (entity.nodeId) {
                            existingNodes.insert(*entity.nodeId);
                        }
                    }
                }
            }

            std::vector<metadata::DocEntity> batch;
            batch.reserve(32);

            auto tagsResult = metadataRepo_->getDocumentTags(doc.id);
            if (tagsResult) {
                for (const auto& tag : tagsResult.value()) {
                    std::string key = "tag:" + tag;
                    auto nodeId = upsertNode(key, "tag", tag, std::nullopt);
                    if (nodeId && existingNodes.find(*nodeId) == existingNodes.end()) {
                        if (!dryRun) {
                            metadata::DocEntity entity;
                            entity.documentId = doc.id;
                            entity.nodeId = *nodeId;
                            entity.entityText = tag;
                            entity.confidence = 1.0f;
                            entity.extractor = "repair";
                            batch.push_back(std::move(entity));
                        }
                        stats.edgesCreated++;
                    }
                }
            }

            if (!doc.mimeType.empty()) {
                std::string key = "mime:" + doc.mimeType;
                auto nodeId = upsertNode(key, "mime", doc.mimeType, std::nullopt);
                if (nodeId && existingNodes.find(*nodeId) == existingNodes.end()) {
                    if (!dryRun) {
                        metadata::DocEntity entity;
                        entity.documentId = doc.id;
                        entity.nodeId = *nodeId;
                        entity.entityText = doc.mimeType;
                        entity.confidence = 1.0f;
                        entity.extractor = "repair";
                        batch.push_back(std::move(entity));
                    }
                    stats.edgesCreated++;
                }
            }

            if (!doc.fileExtension.empty()) {
                std::string ext =
                    doc.fileExtension[0] == '.' ? doc.fileExtension.substr(1) : doc.fileExtension;
                std::string key = "ext:" + ext;
                auto nodeId = upsertNode(key, "ext", ext, std::nullopt);
                if (nodeId && existingNodes.find(*nodeId) == existingNodes.end()) {
                    if (!dryRun) {
                        metadata::DocEntity entity;
                        entity.documentId = doc.id;
                        entity.nodeId = *nodeId;
                        entity.entityText = ext;
                        entity.confidence = 1.0f;
                        entity.extractor = "repair";
                        batch.push_back(std::move(entity));
                    }
                    stats.edgesCreated++;
                }
            }

            auto metadataResult = metadataRepo_->getAllMetadata(doc.id);
            if (metadataResult) {
                for (const auto& [key, value] : metadataResult.value()) {
                    std::string val = value.asString();
                    if (val.empty())
                        continue;

                    upsertNode("meta_key:" + key, "meta_key", key, std::nullopt);

                    std::string nodeKey = "meta_val:" + key + ":" + val;
                    std::string props = "{\"key\":\"" + key + "\"}";
                    auto nodeId = upsertNode(nodeKey, "meta_val", val, props);
                    if (nodeId && existingNodes.find(*nodeId) == existingNodes.end()) {
                        if (!dryRun) {
                            metadata::DocEntity entity;
                            entity.documentId = doc.id;
                            entity.nodeId = *nodeId;
                            entity.entityText = val;
                            entity.confidence = 1.0f;
                            entity.extractor = "repair";
                            batch.push_back(std::move(entity));
                        }
                        stats.edgesCreated++;
                    }
                }
            }

            if (!batch.empty() && !dryRun) {
                auto addResult = kgStore_->addDocEntities(batch);
                if (addResult) {
                    stats.nodesUpdated++;
                } else {
                    stats.errors++;
                    stats.issues.push_back("Failed to add entities for doc: " +
                                           std::to_string(doc.id));
                }
            }

            if (processed % 100 == 0) {
                spdlog::debug("[GraphComponent] Repair progress: processed={}, nodes={}, edges={}",
                              processed, stats.nodesCreated, stats.edgesCreated);
            }
        }

        spdlog::info(
            "[GraphComponent] Graph repair complete: processed={}, nodes={}, edges={}, errors={}",
            processed, stats.nodesCreated, stats.edgesCreated, stats.errors);

        return stats;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, "Graph repair failed: " + std::string(e.what())};
    }
}

Result<GraphComponent::GraphHealthReport> GraphComponent::validateGraph() {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    try {
        GraphHealthReport report;

        spdlog::info("[GraphComponent] Starting graph validation");

        // Run health check first
        auto healthResult = kgStore_->healthCheck();
        if (!healthResult) {
            report.issues.push_back("Health check failed: " + healthResult.error().message);
            spdlog::error("[GraphComponent] Health check failed: {}", healthResult.error().message);
        }

        // For now, return basic report
        // TODO: Add full validation in future iteration:
        // - Count total nodes and edges
        // - Detect orphaned nodes (no edges)
        // - Detect broken edges (pointing to non-existent nodes)
        // - Check reachability from root nodes

        spdlog::info("[GraphComponent] Graph validation complete: {} issues found",
                     report.issues.size());

        return report;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, "Graph validation failed: " + std::string(e.what())};
    }
}

Result<void> GraphComponent::recomputeStats() {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    try {
        spdlog::info("[GraphComponent] Starting stats recomputation");

        // TODO: Implement full stats recomputation in future iteration:
        // - Query all nodes by type
        // - For each node, call kgStore_->recomputeNodeStats(nodeId)
        // - Track progress and errors
        // - Consider batching for large graphs

        spdlog::info("[GraphComponent] Stats recomputation complete");
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     "Stats recomputation failed: " + std::string(e.what())};
    }
}

std::shared_ptr<app::services::IGraphQueryService> GraphComponent::getQueryService() const {
    if (!initialized_) {
        return nullptr;
    }
    return queryService_;
}

GraphComponent::EntityStats GraphComponent::getEntityStats() const {
    if (!entityService_) {
        return EntityStats{};
    }

    auto stats = entityService_->getStats();
    return EntityStats{
        .jobsAccepted = stats.accepted,
        .jobsProcessed = stats.processed,
        .jobsFailed = stats.failed,
    };
}

} // namespace yams::daemon
