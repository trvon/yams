#pragma once

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_artifacts.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>

namespace yams::vector {
class VectorDatabase;
}

namespace yams::topology {

/// Document metadata keys the extractor lifts into typed `TopologyDocumentInput` fields.
inline constexpr std::string_view kTopologyEventTimeMetadataKey = "event_time";
inline constexpr std::string_view kTopologySessionIdMetadataKey = "session_id";

/// Parse an `event_time` metadata value to epoch seconds (UTC). Accepts, after trimming
/// whitespace, an optionally signed integer (epoch seconds), an ISO-8601 date `YYYY-MM-DD`
/// (midnight UTC), or an ISO-8601 datetime `YYYY-MM-DD[T ]HH:MM[:SS[.frac]]` followed by an
/// optional `Z` or `±HH[:]MM` offset; a datetime without a zone is UTC. Calendar fields are
/// range-checked (leap days included). Anything else yields nullopt.
[[nodiscard]] std::optional<std::int64_t> parseTopologyEventTime(std::string_view value) noexcept;

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
    std::vector<std::string> documentHashes{};
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
    // Typed event time / session provenance (see TopologyDocumentInput::eventTime).
    std::size_t eventTimesFromMetadata{0};    // `event_time` parsed from metadata
    std::size_t eventTimesFromCreatedTime{0}; // fell back to the document createdTime
    std::size_t eventTimesInvalid{0};         // `event_time` present but unparsable (ignored)
    std::size_t sessionIdsLoaded{0};          // non-empty `session_id` metadata
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
