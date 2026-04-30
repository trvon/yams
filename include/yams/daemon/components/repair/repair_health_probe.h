#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace yams::metadata {
class IMetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::vector {
class VectorDatabase;
}

namespace yams::daemon {
class GraphComponent;
}

namespace yams::daemon::repair {

struct RepairHealthSnapshot {
    uint64_t documentsScanned{0};
    uint64_t fts5EligibleDocs{0};
    uint64_t missingFts5{0};
    uint64_t embeddableDocs{0};
    uint64_t missingEmbeddings{0};
    uint64_t graphDocNodes{0};
    uint64_t graphDocNodeGap{0};
    // Bounded probe of doc-nodes that have no outbound semantic_neighbor
    // edges. Capped by RepairHealthOptions::semanticNeighborGapProbeLimit so a
    // probe never scans the full corpus. Saturates at the limit (i.e.
    // gap == limit means "at least this many").
    uint64_t semanticNeighborEdgeGap{0};
    bool graphIntegrityOk{true};
    std::vector<std::string> issues;
};

struct RepairHealthOptions {
    bool checkFts5{true};
    bool checkEmbeddings{true};
    bool checkGraph{true};
    bool scanDocuments{true};
    // Upper bound on the bounded semantic_neighbor gap probe. 0 disables it.
    std::size_t semanticNeighborGapProbeLimit{256};
};

class RepairHealthProbe {
public:
    RepairHealthProbe(std::shared_ptr<metadata::IMetadataRepository> meta,
                      std::shared_ptr<vector::VectorDatabase> vectorDb,
                      std::shared_ptr<GraphComponent> graphComponent,
                      std::shared_ptr<metadata::KnowledgeGraphStore> kgStore);

    RepairHealthSnapshot probe(const RepairHealthOptions& options) const;

private:
    static bool vectorsDisabledByEnv();

    std::shared_ptr<metadata::IMetadataRepository> meta_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
    std::shared_ptr<GraphComponent> graphComponent_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
};

} // namespace yams::daemon::repair
