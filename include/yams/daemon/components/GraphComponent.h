// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/core/types.h>

namespace yams::app::services {
class IGraphQueryService;
}

namespace yams::metadata {
class MetadataRepository;
class KnowledgeGraphStore;
struct TreeChangeRecord;
} // namespace yams::metadata

namespace yams::daemon {

class EntityGraphService;

class GraphComponent {
public:
    GraphComponent(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                   std::shared_ptr<metadata::KnowledgeGraphStore> kgStore);

    ~GraphComponent();

    Result<void> initialize();
    void shutdown();
    bool isReady() const;

    struct DocumentGraphContext {
        std::string documentHash;
        std::string filePath;
        std::optional<std::string> snapshotId;
        std::optional<std::string> rootTreeHash;
        std::vector<std::string> tags;
        int64_t documentDbId;
    };
    Result<void> onDocumentIngested(const DocumentGraphContext& ctx);

    Result<void> onTreeDiffApplied(int64_t diffId,
                                   const std::vector<metadata::TreeChangeRecord>& changes);

    struct EntityExtractionJob {
        std::string documentHash;
        std::string filePath;
        std::string contentUtf8;
        std::string language;
    };
    Result<void> submitEntityExtraction(EntityExtractionJob job);

    struct RepairStats {
        uint64_t nodesCreated{0};
        uint64_t nodesUpdated{0};
        uint64_t edgesCreated{0};
        uint64_t errors{0};
        std::vector<std::string> issues;
    };
    Result<RepairStats> repairGraph(bool dryRun = false);

    struct GraphHealthReport {
        uint64_t totalNodes{0};
        uint64_t totalEdges{0};
        uint64_t orphanedNodes{0};
        uint64_t unreachableNodes{0};
        std::vector<std::string> issues;
    };
    Result<GraphHealthReport> validateGraph();

    Result<void> recomputeStats();

    std::shared_ptr<app::services::IGraphQueryService> getQueryService() const;

    struct EntityStats {
        uint64_t jobsAccepted{0};
        uint64_t jobsProcessed{0};
        uint64_t jobsFailed{0};
    };
    EntityStats getEntityStats() const;

private:
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<app::services::IGraphQueryService> queryService_;
    std::shared_ptr<EntityGraphService> entityService_;
    bool initialized_{false};
};

} // namespace yams::daemon
