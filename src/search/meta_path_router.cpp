#include <yams/search/meta_path_router.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <string_view>
#include <unordered_set>

namespace yams::search {

namespace {

constexpr std::string_view kDocPrefix = "doc:";

inline std::string docKey(const std::string& hash) {
    return std::string{kDocPrefix} + hash;
}

inline std::optional<std::string> docHashFromNodeKey(const std::string& nodeKey) {
    if (nodeKey.size() <= kDocPrefix.size() ||
        nodeKey.compare(0, kDocPrefix.size(), kDocPrefix) != 0) {
        return std::nullopt;
    }
    return nodeKey.substr(kDocPrefix.size());
}

// Walk one meta-path of shape: seed_doc → (relation_out) → intermediate
//                              → (relation_in via getEdgesTo) → other_doc.
// Returns map<doc_hash, score> (binary 1.0 per reachable candidate).
// `relationOut` and `relationIn` may be the same (e.g., "calls" both ways for symmetric)
// or different (e.g., "contains" forward, "defined_in" inverse).
std::unordered_map<std::string, float>
walkTwoHopMetaPath(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                   const std::vector<std::int64_t>& seedNodeIds, std::string_view relationOut,
                   std::string_view relationInverse, std::size_t hopLimit) {
    std::unordered_map<std::string, float> hits;
    if (!kgStore || seedNodeIds.empty() || hopLimit == 0) {
        return hits;
    }

    auto outEdgesRes = kgStore->getEdgesFromBatch(seedNodeIds, relationOut, hopLimit);
    if (!outEdgesRes) {
        return hits;
    }

    std::unordered_set<std::int64_t> intermediateIds;
    for (const auto& [_, edges] : outEdgesRes.value()) {
        for (const auto& e : edges) {
            intermediateIds.insert(e.dstNodeId);
        }
    }
    if (intermediateIds.empty()) {
        return hits;
    }

    std::vector<std::int64_t> intermediateVec(intermediateIds.begin(), intermediateIds.end());
    auto inEdgesRes = kgStore->getEdgesToBatch(intermediateVec, relationInverse, hopLimit);
    if (!inEdgesRes) {
        return hits;
    }

    std::unordered_set<std::int64_t> dstIds;
    for (const auto& [_, edges] : inEdgesRes.value()) {
        for (const auto& e : edges) {
            dstIds.insert(e.srcNodeId);
        }
    }
    if (dstIds.empty()) {
        return hits;
    }

    std::vector<std::int64_t> dstVec(dstIds.begin(), dstIds.end());
    auto nodesRes = kgStore->getNodesByIds(dstVec);
    if (!nodesRes) {
        return hits;
    }
    const std::unordered_set<std::int64_t> seedSet(seedNodeIds.begin(), seedNodeIds.end());
    for (const auto& node : nodesRes.value()) {
        if (seedSet.contains(node.id)) {
            continue; // skip self
        }
        if (auto h = docHashFromNodeKey(node.nodeKey)) {
            hits[*h] += 1.0F;
        }
    }
    return hits;
}

// One-hop meta-path: seed → (relation) → other_doc (when other_doc is the dst).
std::unordered_map<std::string, float>
walkOneHopMetaPath(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                   const std::vector<std::int64_t>& seedNodeIds, std::string_view relation,
                   std::size_t hopLimit,
                   const std::unordered_map<std::int64_t, float>* seedSims = nullptr,
                   bool reciprocalOnly = false) {
    std::unordered_map<std::string, float> hits;
    if (!kgStore || seedNodeIds.empty() || hopLimit == 0) {
        return hits;
    }
    auto edgesRes = kgStore->getEdgesFromBatch(seedNodeIds, relation, hopLimit);
    if (!edgesRes) {
        return hits;
    }
    std::unordered_map<std::int64_t, float> dstScore;
    for (const auto& [src, edges] : edgesRes.value()) {
        const float seedSim = [&] {
            if (!seedSims) {
                return 1.0F;
            }
            auto it = seedSims->find(src);
            return it != seedSims->end() ? it->second : 0.0F;
        }();
        for (const auto& e : edges) {
            if (seedSims) {
                dstScore[e.dstNodeId] += e.weight * seedSim;
            } else {
                dstScore[e.dstNodeId] += 1.0F;
            }
        }
    }
    if (dstScore.empty()) {
        return hits;
    }
    const std::unordered_set<std::int64_t> seedSet(seedNodeIds.begin(), seedNodeIds.end());
    std::vector<std::int64_t> dstVec;
    dstVec.reserve(dstScore.size());
    for (const auto& [id, _] : dstScore) {
        dstVec.push_back(id);
    }
    if (reciprocalOnly) {
        std::unordered_set<std::int64_t> reciprocal;
        if (auto backRes = kgStore->getEdgesFromBatch(dstVec, relation, hopLimit)) {
            for (const auto& [src, edges] : backRes.value()) {
                for (const auto& e : edges) {
                    if (seedSet.contains(e.dstNodeId)) {
                        reciprocal.insert(src);
                        break;
                    }
                }
            }
        }
        std::erase_if(dstVec, [&](std::int64_t id) { return !reciprocal.contains(id); });
        if (dstVec.empty()) {
            return hits;
        }
    }
    auto nodesRes = kgStore->getNodesByIds(dstVec);
    if (!nodesRes) {
        return hits;
    }
    for (const auto& node : nodesRes.value()) {
        if (seedSet.contains(node.id)) {
            continue;
        }
        if (auto h = docHashFromNodeKey(node.nodeKey)) {
            hits[*h] += dstScore[node.id];
        }
    }
    return hits;
}

void mergeHitsAsBoost(MetaPathRoutingResult& result,
                      const std::unordered_map<std::string, float>& hits, float weight,
                      std::size_t seedCount, std::size_t* hitCounter) {
    if (weight <= 0.0F || seedCount == 0 || hits.empty()) {
        return;
    }
    if (hitCounter) {
        *hitCounter += hits.size();
    }
    const float invSeeds = 1.0F / static_cast<float>(seedCount);
    for (const auto& [hash, score] : hits) {
        // sqrt-damped cover-fraction: a doc covered by all seeds gets weight√1=1,
        // a doc covered by 1 of K seeds gets √(1/K). Damping prevents single-seed
        // noise from dominating.
        const float frac = score * invSeeds;
        const float damped = std::sqrt(std::clamp(frac, 0.0F, 1.0F));
        result.docBoost[hash] += weight * damped;
    }
}

} // namespace

MetaPathRoutingResult
computeMetaPathBoosts(const std::vector<float>& queryEmbedding,
                      const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                      const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                      const SearchEngineConfig& config) {
    MetaPathRoutingResult result;
    if (!config.enableMetaPathRouting) {
        return result;
    }
    if (queryEmbedding.empty() || !vectorDb || !kgStore || config.metaPathSeedK == 0) {
        return result;
    }

    // Over-fetch: only DOCUMENT-level rows survive the filter below.
    vector::VectorSearchParams params;
    params.k = config.metaPathSeedK * 4;
    params.similarity_threshold = config.metaPathSeedSimilarity;
    auto seedRecs = vectorDb->search(queryEmbedding, params);
    if (seedRecs.empty()) {
        return result;
    }

    std::vector<std::int64_t> seedNodeIds;
    seedNodeIds.reserve(config.metaPathSeedK);
    std::unordered_set<std::string> seedHashes;
    std::unordered_map<std::int64_t, float> seedSims;
    float maxSeedSim = 0.0F;
    for (const auto& rec : seedRecs) {
        if (rec.level != vector::EmbeddingLevel::DOCUMENT || rec.document_hash.empty()) {
            continue;
        }
        if (!seedHashes.insert(rec.document_hash).second) {
            continue;
        }
        auto nodeRes = kgStore->getNodeByKey(docKey(rec.document_hash));
        if (!nodeRes || !nodeRes.value().has_value()) {
            continue;
        }
        const auto nodeId = nodeRes.value()->id;
        seedNodeIds.push_back(nodeId);
        seedSims[nodeId] = rec.relevance_score;
        maxSeedSim = std::max(maxSeedSim, rec.relevance_score);
        if (seedNodeIds.size() >= config.metaPathSeedK) {
            break;
        }
    }
    result.seedDocCount = seedNodeIds.size();
    if (seedNodeIds.empty()) {
        return result;
    }
    if (config.metaPathMinSeedSimilarity > 0.0F && maxSeedSim < config.metaPathMinSeedSimilarity) {
        return result;
    }

    const std::size_t hopK = std::max<std::size_t>(1, config.metaPathHopLimit);

    // M_sem: doc — semantic_neighbor — doc (1-hop, dst is a doc node)
    if (config.metaPathWeightSem > 0.0F) {
        auto hits = walkOneHopMetaPath(kgStore, seedNodeIds, "semantic_neighbor", hopK,
                                       config.metaPathUseEdgeWeights ? &seedSims : nullptr,
                                       config.metaPathReciprocalOnly);
        mergeHitsAsBoost(result, hits, config.metaPathWeightSem, seedNodeIds.size(),
                         &result.semHits);
    }
    // M_def: doc — contains → symbol → defined_in⁻¹ → other doc (2-hop with relation flip)
    if (config.metaPathWeightDef > 0.0F) {
        auto hits = walkTwoHopMetaPath(kgStore, seedNodeIds, "contains", "defined_in", hopK);
        mergeHitsAsBoost(result, hits, config.metaPathWeightDef, seedNodeIds.size(),
                         &result.defHits);
    }
    // M_call: doc — contains → function → calls → function → defined_in⁻¹ → doc
    // (3-hop; collapsed to 2-hop walk via "contains" + "calls" then resolve via getEdgesTo
    //  on the called functions to find their defining doc)
    if (config.metaPathWeightCall > 0.0F) {
        // Step 1: get all functions contained by seed docs
        auto containsEdges = kgStore->getEdgesFromBatch(seedNodeIds, "contains", hopK);
        if (containsEdges) {
            std::vector<std::int64_t> containedFns;
            for (const auto& [_, edges] : containsEdges.value()) {
                for (const auto& e : edges) {
                    containedFns.push_back(e.dstNodeId);
                }
            }
            if (!containedFns.empty()) {
                // Step 2: get callees of those functions
                auto callsEdges = kgStore->getEdgesFromBatch(containedFns, "calls", hopK);
                if (callsEdges) {
                    std::unordered_set<std::int64_t> callees;
                    for (const auto& [_, edges] : callsEdges.value()) {
                        for (const auto& e : edges) {
                            callees.insert(e.dstNodeId);
                        }
                    }
                    if (!callees.empty()) {
                        // Step 3: resolve each callee's defining doc via defined_in
                        std::vector<std::int64_t> calleeVec(callees.begin(), callees.end());
                        auto definedIn = kgStore->getEdgesFromBatch(calleeVec, "defined_in", hopK);
                        if (definedIn) {
                            std::unordered_map<std::string, float> callHits;
                            std::unordered_set<std::int64_t> dstDocIds;
                            for (const auto& [_, edges] : definedIn.value()) {
                                for (const auto& e : edges) {
                                    dstDocIds.insert(e.dstNodeId);
                                }
                            }
                            if (!dstDocIds.empty()) {
                                std::vector<std::int64_t> dstVec(dstDocIds.begin(),
                                                                 dstDocIds.end());
                                auto nodesRes = kgStore->getNodesByIds(dstVec);
                                if (nodesRes) {
                                    const std::unordered_set<std::int64_t> seedSet(
                                        seedNodeIds.begin(), seedNodeIds.end());
                                    for (const auto& node : nodesRes.value()) {
                                        if (seedSet.contains(node.id)) {
                                            continue;
                                        }
                                        if (auto h = docHashFromNodeKey(node.nodeKey)) {
                                            callHits[*h] += 1.0F;
                                        }
                                    }
                                }
                            }
                            mergeHitsAsBoost(result, callHits, config.metaPathWeightCall,
                                             seedNodeIds.size(), &result.callHits);
                        }
                    }
                }
            }
        }
    }
    // M_blob: doc — blob — doc (dedup; 2-hop through a blob node, getEdgesTo on the blob)
    // Skipped on this pass: blob edges in YAMS are doc→blob (out) and blob has no
    // back-references to multiple docs unless content is duplicated. Add later if
    // dedup signal is needed.

    return result;
}

} // namespace yams::search
