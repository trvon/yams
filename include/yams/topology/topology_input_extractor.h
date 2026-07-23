#pragma once

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_artifacts.h>

#include <memory>

namespace yams::vector {
class VectorDatabase;
}

namespace yams::topology {

// Phase V: per-doc feature composition for clustering input.
// Composer concatenates: w_dense * dense_view + w_entity * entity_signature + w_minhash *
// minhash_sketch. Each branch is a no-op when its enable flag is false; V0 baseline (all off)
// reproduces the pre-Phase-V dense-only feature exactly (modulo L2 normalization, which matches
// the prior embedding-only input).
struct FeatureComposition {
    // V-A: GLiNER entity-type histogram fused with dense embedding.
    bool enableEntityFusion{false};
    std::size_t entitySignatureK{16}; // top-K most frequent canonical entity types in corpus
    float entityFusionAlpha{0.25F};   // weight on the entity signature; dense gets (1 - α_e - α_m)
    float entityMinConfidence{0.45F}; // drop GLiNER tags below this confidence

    // V-B: Simeon matryoshka coarse-view replacement of the dense embedding.
    bool enableMatryoshkaCoarseView{false};
    std::size_t matryoshkaTargetDim{1024}; // 0 or >= dense dim = no-op; 64/128/256 = coarsen

    // V-C: per-doc MinHash signature sketch (requires Phase V-C ingest persistence).
    bool enableMinHashSketch{false};
    std::size_t minhashSketchDim{16}; // bucket-count sketch dim
    float minhashAlpha{0.10F};        // weight on the MinHash sketch
};

struct TopologyExtractionConfig {
    std::vector<std::string> documentHashes;
    int limit{0};
    std::size_t maxNeighborsPerDocument{32};
    bool includeEmbeddings{true};
    bool includeMetadata{true};
    bool requireEmbeddings{false};
    bool requireGraphNode{false};
    FeatureComposition featureComposition{};
};

struct TopologyExtractionStats {
    std::size_t seedDocuments{0};
    std::size_t documentsRequested{0};
    std::size_t documentsLoaded{0};
    std::size_t documentsReturned{0};
    // Common vector-record model identity. Empty when absent or mixed.
    std::string embeddingSpaceIdentity;
    bool mixedEmbeddingSpaces{false};
    std::size_t documentsMissingEmbeddings{0};
    std::size_t documentsMissingGraphNodes{0};
    std::size_t metadataMapsLoaded{0};
    std::size_t neighborEdgesScanned{0};
    std::size_t neighborsReturned{0};
    std::size_t regionDocuments{0};
    // Phase V: feature composer telemetry.
    std::size_t composedFeatureDim{0}; // final per-doc vector length
    std::size_t composedDocsWithEntitySignature{
        0};                                       // docs that produced a non-zero entity signature
    std::size_t composedDocsWithMinHashSketch{0}; // docs that produced a non-zero minhash sketch
    std::size_t composedFilteredLowConfidenceCount{0};
    std::size_t composedTopKEntityTypeCount{0}; // size of the corpus top-K entity-type cache
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
