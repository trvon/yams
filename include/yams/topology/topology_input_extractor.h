#pragma once

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/vector/vector_database.h>

#include <memory>

namespace yams::topology {

struct TopologyExtractionConfig {
    std::vector<std::string> documentHashes;
    int limit{0};
    std::size_t maxNeighborsPerDocument{32};
    bool includeEmbeddings{true};
    bool includeMetadata{true};
    bool requireEmbeddings{false};
    bool requireGraphNode{false};
};

struct TopologyExtractionStats {
    std::size_t seedDocuments{0};
    std::size_t documentsRequested{0};
    std::size_t documentsLoaded{0};
    std::size_t documentsReturned{0};
    std::size_t documentsMissingEmbeddings{0};
    std::size_t documentsMissingGraphNodes{0};
    std::size_t metadataMapsLoaded{0};
    std::size_t neighborEdgesScanned{0};
    std::size_t neighborsReturned{0};
    std::size_t regionDocuments{0};
};

class TopologyInputExtractor {
public:
    TopologyInputExtractor(std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                           std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                           std::shared_ptr<vector::VectorDatabase> vectorDb);

    Result<std::vector<TopologyDocumentInput>>
    extract(const TopologyExtractionConfig& config, TopologyExtractionStats* stats = nullptr) const;

private:
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
};

} // namespace yams::topology
