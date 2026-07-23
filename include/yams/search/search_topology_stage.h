#pragma once

#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/search/topology_routing_session.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace yams::metadata

namespace yams::search {

/// Inputs for the tiered-search topology assist stage (after Tier-1 text).
struct TopologyAssistStageRequest {
    std::string query;
    SearchEngineConfig config;
    SearchEngineConfig::TopologyRoutingMode routingMode{
        SearchEngineConfig::TopologyRoutingMode::Disabled};
    bool weakTier1Query{false};
    std::vector<std::string> tier1SeedHashes;
    std::vector<yams::topology::WeightedDocumentSeed> tier1SeedEvidence;
    std::unordered_set<std::string> existingCandidateHashes;
    std::optional<std::vector<float>> queryEmbedding;
    std::string queryEmbeddingSpaceIdentity;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore;
    std::shared_ptr<TopologyRoutingSnapshotCache> snapshotCache;
    std::uint64_t expectedTopologyEpoch{0};
    /// Extra vector hits to mix into graph-neighbor seeds (already ranked preferred).
    std::vector<std::string> vectorSeedHashes;
    /// Cap on newly added vector seeds. 0 = add none (Tier-1 only).
    std::size_t maxVectorSeeds{0};
    /// Existing stage-trace switch; enables diagnostic reads without changing routing output.
    bool collectGraphDiagnostics{false};
};

struct TopologyAssistStageResult {
    TopologyRoutingSessionResult session;
    std::string skipReason;
    std::vector<std::string> enrichedSeedHashes;
    std::size_t vectorSeedsAdded{0};
};

/// Merge Tier-1 seeds with optional vector seeds (deduped, Tier-1 first).
/// maxVectorSeeds=0 adds no vector seeds; otherwise caps newly-added vector hashes.
[[nodiscard]] std::vector<std::string>
mergeTopologySeedHashes(const std::vector<std::string>& tier1Seeds,
                        const std::vector<std::string>& vectorSeeds, std::size_t maxVectorSeeds);

/// Preserve lexical rank/score evidence before Tier-1 candidates become an
/// unordered set. Vector/metadata legs are deliberately excluded.
[[nodiscard]] std::vector<yams::topology::WeightedDocumentSeed>
rankTopologySeedEvidence(const std::vector<ComponentResult>& components, std::size_t maxSeeds);

/// Fill skipReason when the session did not apply expansion (stable product diagnostics).
void fillTopologySkipReason(std::string& skipReason, const TopologyRoutingOptions& options,
                            bool hasStores, const TopologyRoutingSessionResult& session);

/// Run topology routing for tiered search and return session + diagnostics.
[[nodiscard]] TopologyAssistStageResult
runTopologyAssistStage(const TopologyAssistStageRequest& request);

} // namespace yams::search
