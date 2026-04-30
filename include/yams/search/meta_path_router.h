#pragma once

#include <yams/search/search_engine_config.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::metadata {
class KnowledgeGraphStore;
}
namespace yams::vector {
class VectorDatabase;
}

namespace yams::search {

struct MetaPathRoutingResult {
    std::unordered_map<std::string, float> docBoost;
    std::size_t seedDocCount{0};
    std::size_t semHits{0};
    std::size_t callHits{0};
    std::size_t defHits{0};
    std::size_t entityHits{0};
    std::size_t blobHits{0};
};

// Phase Y router: walks M_sem + M_call + M_def + M_entity + M_blob meta-paths
// from the query's top-K seeds, returns per-doc additive boost factors. The
// caller applies them post-fusion: result.score *= (1 + α * boost[doc]).
//
// Empty queryEmbedding → empty result (no-op).
// Missing kgStore/vectorDb → empty result (no-op).
// All weights zero → empty result (no-op).
MetaPathRoutingResult
computeMetaPathBoosts(const std::vector<float>& queryEmbedding,
                      const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                      const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                      const SearchEngineConfig& config);

} // namespace yams::search
