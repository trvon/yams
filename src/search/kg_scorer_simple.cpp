#include <yams/app/services/graph_query_service.hpp>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/kg_scorer.h>
#include <yams/search/kg_scorer_simple.h>
#include <yams/search/query_text_utils.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::search {

using yams::app::services::GraphQueryRequest;
using yams::app::services::GraphRelationType;
using yams::app::services::IGraphQueryService;
using yams::metadata::AliasResolution;
using yams::metadata::DocEntity;
using yams::metadata::KnowledgeGraphStore;

// A very simple local-first KG scorer.
// - Extracts alias-like tokens from the query
// - Resolves tokens to KG node ids via KnowledgeGraphStore (exact, then FTS if enabled)
// - For each candidate (interpreting its id as a document id if numeric):
//     entity score = Jaccard(query_nodes, candidate_doc_nodes)
//     structural score = overlap of candidate_doc_nodes with 1-hop neighbors of query_nodes,
//                        normalized by candidate size (bounded to [0,1])
class SimpleKGScorer final : public KGScorer {
public:
    explicit SimpleKGScorer(std::shared_ptr<KnowledgeGraphStore> store)
        : store_(std::move(store)) {}

    // Set graph query service for advanced traversal (optional)
    void setGraphQueryService(std::shared_ptr<IGraphQueryService> graphService) {
        graphService_ = std::move(graphService);
    }

    void setConfig(const KGScoringConfig& cfg) override { cfg_ = cfg; }
    const KGScoringConfig& getConfig() const override { return cfg_; }

    Result<std::unordered_map<std::string, KGScore>>
    score(const std::string& query_text, const std::vector<std::string>& candidate_ids) override {
        last_expl_.clear();

        if (!store_) {
            return Error{ErrorCode::InvalidState, "SimpleKGScorer: no KnowledgeGraphStore set"};
        }

        const auto t0 = std::chrono::steady_clock::now();

        // 1) Build query node set from query text
        auto query_aliases = tokenize(query_text);
        std::unordered_set<std::int64_t> query_nodes;
        std::unordered_map<std::int64_t, metadata::KGNode> nodeCache;

        const auto getNodeCached = [&](std::int64_t nodeId) -> std::optional<metadata::KGNode> {
            auto it = nodeCache.find(nodeId);
            if (it != nodeCache.end()) {
                return it->second;
            }
            auto nodeRes = store_->getNodeById(nodeId);
            if (!nodeRes || !nodeRes.value().has_value()) {
                return std::nullopt;
            }
            nodeCache.emplace(nodeId, *nodeRes.value());
            return *nodeRes.value();
        };

        // Resolve aliases/phrases to nodes (budget-aware)
        for (const auto& a : query_aliases) {
            if (timedOut(t0))
                break;

            // Try exact first
            auto exact = store_->resolveAliasExact(a, 16);
            if (!exact)
                return exact.error();

            if (exact.value().empty()) {
                // For claim-style biomedical queries, labels are often more reliable than
                // alias FTS. Prefer label lookup before fuzzy alias expansion.
                auto labelMatches = store_->searchNodesByLabel(a, 16, 0);
                if (!labelMatches) {
                    return labelMatches.error();
                }
                if (!labelMatches.value().empty()) {
                    for (const auto& node : labelMatches.value()) {
                        if (nodeTypeWeight(node.type, node.label.value_or("")) > 0.10f) {
                            query_nodes.insert(node.id);
                        }
                    }
                } else {
                    // Fallback to fuzzy/FTS if enabled by store config
                    auto fuzzy = store_->resolveAliasFuzzy(a, 16);
                    if (!fuzzy)
                        return fuzzy.error();
                    for (const auto& ar : fuzzy.value()) {
                        const auto node = getNodeCached(ar.nodeId);
                        const float weight =
                            node.has_value() ? nodeTypeWeight(node->type, node->label.value_or(""))
                                             : 0.60f;
                        if (weight > 0.10f) {
                            query_nodes.insert(ar.nodeId);
                        }
                    }
                }
            } else {
                for (const auto& ar : exact.value()) {
                    const auto node = getNodeCached(ar.nodeId);
                    const float weight = node.has_value()
                                             ? nodeTypeWeight(node->type, node->label.value_or(""))
                                             : 0.60f;
                    if (weight > 0.10f) {
                        query_nodes.insert(ar.nodeId);
                    }
                }
            }
        }

        // Pre-compute 1-hop neighbor set for structural scoring (budget-aware)
        std::unordered_set<std::int64_t> query_neighbor_union;
        for (auto nid : query_nodes) {
            if (timedOut(t0))
                break;
            auto nb = store_->neighbors(nid, cfg_.max_neighbors);
            if (!nb)
                return nb.error();
            for (auto v : nb.value()) {
                query_neighbor_union.insert(v);
            }
        }

        // 2) Score each candidate
        std::unordered_map<std::string, KGScore> out;
        out.reserve(candidate_ids.size());
        last_expl_.reserve(candidate_ids.size());

        for (const auto& cid : candidate_ids) {
            if (timedOut(t0))
                break;

            KGScore s{};
            KGExplain expl;
            expl.id = cid;
            expl.components["query_node_count"] = static_cast<double>(query_nodes.size());
            expl.components["query_neighbor_count"] =
                static_cast<double>(query_neighbor_union.size());

            // Resolve candidate as document id (numeric id, hash, or path)
            auto docIdOpt = resolveDocumentId(cid);
            if (!docIdOpt.has_value()) {
                // If not numeric, emit zeros (missing id implies zero by contract)
                expl.reasons.emplace_back("Candidate could not be resolved to a document id");
                last_expl_.push_back(std::move(expl));
                out.emplace(cid, s);
                continue;
            }
            const auto docId = docIdOpt.value();
            expl.components["resolved_document_id"] = static_cast<double>(docId);

            // Fetch document entities
            auto entsR = store_->getDocEntitiesForDocument(docId, 2000, 0);
            if (!entsR) {
                // If doc lookup fails, keep zero scores (do not hard-fail the whole batch)
                expl.reasons.emplace_back("Document entity lookup failed");
                last_expl_.push_back(std::move(expl));
                out.emplace(cid, s);
                continue;
            }
            const auto& ents = entsR.value();
            expl.components["candidate_doc_entity_count"] = static_cast<double>(ents.size());

            // Build candidate node set
            std::unordered_set<std::int64_t> cand_nodes;
            cand_nodes.reserve(ents.size());
            float bestSurfaceSignal = 0.0f;
            float bestQueryCoverage = 0.0f;
            float bestTypeWeight = 0.0f;
            std::string bestSurfaceLabel;
            for (const auto& de : ents) {
                std::optional<metadata::KGNode> node;
                if (de.nodeId.has_value()) {
                    node = getNodeCached(de.nodeId.value());
                    const float weight = node.has_value()
                                             ? nodeTypeWeight(node->type, node->label.value_or(""))
                                             : 0.60f;
                    if (weight > 0.10f) {
                        cand_nodes.insert(de.nodeId.value());
                    }
                }

                const std::string label =
                    node.has_value() ? node->label.value_or(de.entityText) : de.entityText;
                const float typeWeight = nodeTypeWeight(
                    node.has_value() ? node->type : std::optional<std::string>{}, label);
                const float confidence = de.confidence.value_or(1.0f);
                const float surfaceScore = std::max(surfaceMatchScore(query_aliases, de.entityText),
                                                    surfaceMatchScore(query_aliases, label));
                if (surfaceScore > 0.0f) {
                    const float weighted = clamp01(surfaceScore * typeWeight * confidence);
                    if (weighted > bestSurfaceSignal) {
                        bestSurfaceSignal = weighted;
                        bestTypeWeight = typeWeight;
                        bestSurfaceLabel = label;
                    }
                    bestQueryCoverage = std::max(bestQueryCoverage, clamp01(surfaceScore));
                }
            }
            expl.components["candidate_node_count"] = static_cast<double>(cand_nodes.size());

            // Entity score: Jaccard between query_nodes and cand_nodes
            const float entity = jaccard(query_nodes, cand_nodes);

            // Structural score: neighbor overlap normalized by candidate size
            const float structural = structuralOverlap(query_neighbor_union, cand_nodes);

            s.entity = std::max(clamp01(entity), bestSurfaceSignal);
            s.structural = clamp01(structural);
            // Auxiliary, normalized features
            // Overlap ratios relative to candidate and query sets (when non-zero)
            const float cand_den =
                cand_nodes.empty() ? 0.0f : static_cast<float>(cand_nodes.size());
            const float qry_den =
                query_nodes.empty() ? 0.0f : static_cast<float>(query_nodes.size());
            if (cand_den > 0.0f) {
                s.features["feature_entity_overlap_ratio"] = std::min(
                    1.0f, static_cast<float>(intersectionSize(query_nodes, cand_nodes)) / cand_den);
                s.features["feature_neighbor_overlap_ratio"] = s.structural; // same normalization
            }
            if (qry_den > 0.0f) {
                s.features["feature_query_coverage_ratio"] = std::min(
                    1.0f, static_cast<float>(intersectionSize(query_nodes, cand_nodes)) / qry_den);
            } else if (bestQueryCoverage > 0.0f) {
                s.features["feature_query_coverage_ratio"] = bestQueryCoverage;
            }
            if (bestSurfaceSignal > 0.0f) {
                s.features["feature_surface_match_ratio"] = bestSurfaceSignal;
            }

            // Relation diversity: count distinct relations from candidate nodes that touch the
            // query neighborhood (or query nodes directly), honoring basic allow/block filters.
            if (!cand_nodes.empty() && (!query_neighbor_union.empty() || !query_nodes.empty())) {
                std::unordered_set<std::string> rels;
                auto accepts = [&](std::string_view rel) {
                    if (!cfg_.relation_allow.empty()) {
                        bool ok = false;
                        for (const auto& a : cfg_.relation_allow) {
                            if (rel == a) {
                                ok = true;
                                break;
                            }
                        }
                        if (!ok)
                            return false;
                    }
                    for (const auto& b : cfg_.relation_block) {
                        if (rel == b)
                            return false;
                    }
                    return true;
                };

                for (auto nid : cand_nodes) {
                    if (timedOut(t0))
                        break;
                    auto edgesR = store_->getEdgesFrom(nid, std::nullopt, cfg_.max_neighbors, 0);
                    if (!edgesR) {
                        continue;
                    }
                    for (const auto& e : edgesR.value()) {
                        if (!accepts(e.relation))
                            continue;
                        if (query_neighbor_union.find(e.dstNodeId) != query_neighbor_union.end() ||
                            query_nodes.find(e.dstNodeId) != query_nodes.end()) {
                            rels.insert(e.relation);
                        }
                    }
                }
                if (!rels.empty()) {
                    // Normalize by allowed relation universe when provided, else by a soft cap.
                    const float denom = !cfg_.relation_allow.empty()
                                            ? static_cast<float>(cfg_.relation_allow.size())
                                            : 8.0f;
                    s.features["feature_relation_diversity_ratio"] =
                        std::min(1.0f, static_cast<float>(rels.size()) / std::max(1.0f, denom));
                }

                // Path support score (budget-aware; optional)
                if (cfg_.enable_path_enumeration && cfg_.max_hops > 0) {
                    s.features["feature_path_support_score"] =
                        pathSupportScore(query_nodes, cand_nodes, t0);
                }
            }
            out.emplace(cid, s);

            // Explanations (best-effort)
            if (!query_nodes.empty() && !cand_nodes.empty()) {
                expl.components["entity_jaccard"] = static_cast<double>(s.entity);
                expl.components["neighbor_overlap"] = static_cast<double>(s.structural);
                if (auto it = s.features.find("feature_entity_overlap_ratio");
                    it != s.features.end())
                    expl.components["entity_overlap_ratio"] = it->second;
                if (auto it = s.features.find("feature_query_coverage_ratio");
                    it != s.features.end())
                    expl.components["query_coverage_ratio"] = it->second;
                if (s.entity > 0.0f) {
                    expl.reasons.emplace_back("Shares linked entities with query");
                }
                if (s.structural > 0.0f) {
                    expl.reasons.emplace_back(
                        "Candidate entities are neighbors of query-linked entities");
                }
                if (bestSurfaceSignal > 0.0f) {
                    expl.components["surface_match_score"] = bestSurfaceSignal;
                    expl.components["surface_type_weight"] = bestTypeWeight;
                    expl.reasons.emplace_back("Candidate entity labels overlap query phrases: " +
                                              bestSurfaceLabel);
                }
            } else {
                if (query_nodes.empty()) {
                    expl.reasons.emplace_back("No query entities resolved from KG aliases/labels");
                }
                if (cand_nodes.empty()) {
                    expl.reasons.emplace_back("Candidate has no linked KG entities");
                }
                if (bestSurfaceSignal > 0.0f) {
                    expl.components["surface_match_score"] = bestSurfaceSignal;
                    expl.components["surface_type_weight"] = bestTypeWeight;
                    expl.reasons.emplace_back("Candidate entity labels overlap query phrases: " +
                                              bestSurfaceLabel);
                }
            }
            last_expl_.push_back(std::move(expl));
        }

        return out;
    }

    std::vector<KGExplain> getLastExplanations() const override { return last_expl_; }

private:
    // Helpers
    static std::vector<std::string> tokenizeSurface(std::string_view text) {
        std::vector<std::string> out;
        std::string cur;
        for (unsigned char uc : text) {
            if (std::isalnum(uc)) {
                cur.push_back(static_cast<char>(std::tolower(uc)));
            } else if (!cur.empty()) {
                out.push_back(cur);
                cur.clear();
            }
        }
        if (!cur.empty()) {
            out.push_back(cur);
        }
        return out;
    }

    static std::string normalizeSurface(std::string_view text) {
        return normalizeGraphSurface(text);
    }

    static float nodeTypeWeight(const std::optional<std::string>& typeOpt,
                                std::string_view labelView) {
        std::string type;
        if (typeOpt.has_value()) {
            type.reserve(typeOpt->size());
            for (unsigned char uc : *typeOpt) {
                type.push_back(static_cast<char>(std::tolower(uc)));
            }
        }
        const std::string label = normalizeSurface(labelView);

        if (type == "date" || type == "time" || type == "duration" || type == "number" ||
            type == "percentage" || type == "money" || type == "ordinal") {
            return 0.0f;
        }
        if (label.find("january") != std::string::npos ||
            label.find("february") != std::string::npos ||
            label.find("march") != std::string::npos || label.find("april") != std::string::npos ||
            label.find("may ") != std::string::npos || label.find("june") != std::string::npos ||
            label.find("july") != std::string::npos || label.find("august") != std::string::npos ||
            label.find("september") != std::string::npos ||
            label.find("october") != std::string::npos ||
            label.find("november") != std::string::npos ||
            label.find("december") != std::string::npos) {
            return 0.0f;
        }
        if (type == "gene" || type == "protein" || type == "receptor" || type == "cell" ||
            type == "cell_type" || type == "disease" || type == "chemical" || type == "drug" ||
            type == "pathway" || type == "biological_process" || type == "mutation" ||
            type == "anatomy" || type == "organism" || type == "species" || type == "biomarker") {
            return 1.0f;
        }
        if (type == "product") {
            return 0.45f;
        }
        if (type == "location") {
            return 0.25f;
        }
        if (type == "person" || type == "organization") {
            return 0.15f;
        }
        if (label == "encoded protein product" || label == "plasma membrane" ||
            label == "cell membrane" || label == "crossover products") {
            return 0.10f;
        }
        return 0.60f;
    }

    static float surfaceMatchScore(const std::vector<std::string>& queryAliases,
                                   std::string_view surface) {
        const std::string normSurface = normalizeSurface(surface);
        if (normSurface.empty()) {
            return 0.0f;
        }
        const auto surfaceTokens = tokenizeSurface(normSurface);
        const std::unordered_set<std::string> surfaceSet(surfaceTokens.begin(),
                                                         surfaceTokens.end());

        float best = 0.0f;
        for (const auto& alias : queryAliases) {
            if (alias.empty()) {
                continue;
            }
            if (alias == normSurface) {
                return 1.0f;
            }
            if (alias.size() >= 3 && (normSurface.find(alias) != std::string::npos ||
                                      alias.find(normSurface) != std::string::npos)) {
                best = std::max(best, 0.85f);
            }
            const auto aliasTokens = tokenizeSurface(alias);
            if (aliasTokens.empty()) {
                continue;
            }
            size_t overlap = 0;
            for (const auto& tok : aliasTokens) {
                if (surfaceSet.find(tok) != surfaceSet.end()) {
                    ++overlap;
                }
            }
            if (overlap > 0) {
                const float coverage = static_cast<float>(overlap) /
                                       static_cast<float>(std::max<size_t>(1, aliasTokens.size()));
                best = std::max(best, coverage * 0.75f);
            }
        }
        return best;
    }

    static float clamp01(float v) {
        if (v < 0.0f)
            return 0.0f;
        if (v > 1.0f)
            return 1.0f;
        return v;
    }

    static std::optional<std::int64_t> parseInt64(std::string_view s) {
        // Accept plain decimal, optionally with "doc:" prefix
        if (s.rfind("doc:", 0) == 0) {
            s.remove_prefix(4);
        }
        // Reject empty
        if (s.empty())
            return std::nullopt;
        bool neg = false;
        if (s[0] == '-') {
            neg = true;
            s.remove_prefix(1);
        }
        if (s.empty())
            return std::nullopt;

        std::int64_t val = 0;
        for (char c : s) {
            if (c < '0' || c > '9')
                return std::nullopt;
            int d = c - '0';
            // Basic overflow check
            if (val > (std::numeric_limits<std::int64_t>::max() - d) / 10) {
                return std::nullopt;
            }
            val = val * 10 + d;
        }
        return neg ? -val : val;
    }

    std::optional<std::int64_t> resolveDocumentId(const std::string& candidateId) const {
        if (auto parsed = parseInt64(candidateId); parsed.has_value()) {
            return parsed;
        }

        if (!store_) {
            return std::nullopt;
        }

        auto byHash = store_->getDocumentIdByHash(candidateId);
        if (byHash) {
            const auto hashDocId = byHash.value();
            if (hashDocId.has_value()) {
                return hashDocId;
            }
        }

        auto byPath = store_->getDocumentIdByPath(candidateId);
        if (byPath) {
            const auto pathDocId = byPath.value();
            if (pathDocId.has_value()) {
                return pathDocId;
            }
        }

        return std::nullopt;
    }

    static float jaccard(const std::unordered_set<std::int64_t>& a,
                         const std::unordered_set<std::int64_t>& b) {
        if (a.empty() || b.empty())
            return 0.0f;

        // Iterate over smaller set
        const auto* small = &a;
        const auto* large = &b;
        if (b.size() < a.size()) {
            small = &b;
            large = &a;
        }
        std::size_t inter = 0;
        for (auto x : *small) {
            if (large->find(x) != large->end()) {
                ++inter;
            }
        }
        const std::size_t uni = a.size() + b.size() - inter;
        if (uni == 0)
            return 0.0f;
        return static_cast<float>(inter) / static_cast<float>(uni);
    }

    static std::size_t intersectionSize(const std::unordered_set<std::int64_t>& a,
                                        const std::unordered_set<std::int64_t>& b) {
        if (a.empty() || b.empty())
            return 0;
        const auto* small = &a;
        const auto* large = &b;
        if (b.size() < a.size()) {
            small = &b;
            large = &a;
        }
        std::size_t inter = 0;
        for (auto x : *small) {
            if (large->find(x) != large->end())
                ++inter;
        }
        return inter;
    }

    static float structuralOverlap(const std::unordered_set<std::int64_t>& neighbor_union,
                                   const std::unordered_set<std::int64_t>& cand_nodes) {
        if (neighbor_union.empty() || cand_nodes.empty())
            return 0.0f;

        std::size_t overlap = 0;
        for (auto x : cand_nodes) {
            if (neighbor_union.find(x) != neighbor_union.end()) {
                ++overlap;
            }
        }
        // Normalize by candidate cardinality (bounded to [0,1])
        return cand_nodes.empty() ? 0.0f
                                  : std::min(1.0f, static_cast<float>(overlap) /
                                                       static_cast<float>(cand_nodes.size()));
    }

    // Budget-aware, small-path support score. Enumerate paths up to cfg_.max_hops from query
    // nodes toward candidate nodes. Each discovered path contributes hop_decay^length. The final
    // score is normalized by max_paths (soft cap) and clamped to [0,1]. Relation filters are
    // honored when present by expanding via getEdgesFrom; otherwise we use fast neighbors().
    float pathSupportScore(const std::unordered_set<std::int64_t>& query_nodes,
                           const std::unordered_set<std::int64_t>& cand_nodes,
                           const std::chrono::steady_clock::time_point& t0) {
        if (!cfg_.enable_path_enumeration || cfg_.max_hops == 0 || query_nodes.empty() ||
            cand_nodes.empty()) {
            return 0.0f;
        }
        const float decay = std::max(0.0f, std::min(1.0f, cfg_.hop_decay));
        const std::size_t max_paths = std::max<std::size_t>(1, cfg_.max_paths);
        std::size_t paths_found = 0;
        double score_acc = 0.0;

        auto accepts = [&](std::string_view rel) {
            if (!cfg_.relation_allow.empty()) {
                bool ok = false;
                for (const auto& a : cfg_.relation_allow) {
                    if (rel == a) {
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    return false;
            }
            for (const auto& b : cfg_.relation_block) {
                if (rel == b)
                    return false;
            }
            return true;
        };

        for (auto q : query_nodes) {
            if (timedOut(t0))
                break;
            // BFS up to max_hops
            std::queue<std::pair<std::int64_t, std::size_t>> qq;
            std::unordered_set<std::int64_t> visited;
            qq.emplace(q, 0);
            visited.insert(q);
            while (!qq.empty()) {
                if (timedOut(t0))
                    break;
                auto [node, depth] = qq.front();
                qq.pop();
                if (depth > cfg_.max_hops)
                    continue;
                if (depth > 0 && cand_nodes.find(node) != cand_nodes.end()) {
                    // Found a path of length=depth
                    score_acc += std::pow(decay, static_cast<double>(depth));
                    if (++paths_found >= max_paths)
                        goto end_enum;
                    // Continue search to possibly find different candidates; do not early return
                }
                if (depth == cfg_.max_hops)
                    continue;

                if (!cfg_.relation_allow.empty() || !cfg_.relation_block.empty()) {
                    auto edgesR = store_->getEdgesFrom(node, std::nullopt, cfg_.max_neighbors, 0);
                    if (!edgesR)
                        continue;
                    for (const auto& e : edgesR.value()) {
                        if (!accepts(e.relation))
                            continue;
                        if (!visited.insert(e.dstNodeId).second)
                            continue;
                        qq.emplace(e.dstNodeId, depth + 1);
                    }
                } else {
                    auto nbR = store_->neighbors(node, cfg_.max_neighbors);
                    if (!nbR)
                        continue;
                    for (auto nxt : nbR.value()) {
                        if (!visited.insert(nxt).second)
                            continue;
                        qq.emplace(nxt, depth + 1);
                    }
                }
            }
        }

    end_enum:
        if (paths_found == 0)
            return 0.0f;
        double norm = static_cast<double>(max_paths);
        double out = std::min(1.0, score_acc / norm);
        return static_cast<float>(out);
    }

    // Tokenize query into candidate aliases:
    // - lowercase alnum sequences (minus stopwords/noise)
    // - phrase n-grams (2..4) to capture multi-token entities in scientific claims
    static std::vector<std::string> tokenize(const std::string& text) {
        static const std::unordered_set<std::string> kStopwords = {
            "a",    "an",   "and",   "are",  "as", "at",  "be",   "by",  "for", "from",
            "has",  "have", "in",    "into", "is", "it",  "its",  "of",  "on",  "or",
            "that", "the",  "their", "this", "to", "was", "were", "with"};

        const auto flushTokenVariants = [](const std::string& raw, std::vector<std::string>& dest) {
            if (raw.empty()) {
                return;
            }

            std::string lowered;
            lowered.reserve(raw.size());
            for (unsigned char uc : raw) {
                lowered.push_back(static_cast<char>(std::tolower(uc)));
            }
            dest.push_back(lowered);

            // Also split mixed biomedical tokens on alpha<->digit boundaries so forms like
            // p16INK4A can contribute p16 / ink4a / p16 ink4a candidates.
            std::vector<std::string> parts;
            std::string current = lowered;
            if (!current.empty()) {
                std::string part;
                part.push_back(current[0]);
                for (size_t i = 1; i < current.size(); ++i) {
                    const bool prevDigit = std::isdigit(static_cast<unsigned char>(current[i - 1]));
                    const bool curDigit = std::isdigit(static_cast<unsigned char>(current[i]));
                    if (prevDigit != curDigit && !part.empty()) {
                        parts.push_back(part);
                        part.clear();
                    }
                    part.push_back(current[i]);
                }
                if (!part.empty()) {
                    parts.push_back(std::move(part));
                }
            }

            if (parts.size() >= 2) {
                for (const auto& p : parts) {
                    if (p.size() >= 2) {
                        dest.push_back(p);
                    }
                }
                std::string joined = parts[0];
                for (size_t i = 1; i < parts.size(); ++i) {
                    joined.push_back(' ');
                    joined += parts[i];
                }
                dest.push_back(std::move(joined));
            }
        };

        std::vector<std::string> tokens;
        tokens.reserve(text.size() / 4 + 1);
        std::string cur;
        for (unsigned char uc : text) {
            if (std::isalnum(uc)) {
                cur.push_back(static_cast<char>(uc));
            } else if (uc == '-' || uc == '/' || uc == '_') {
                // Preserve biomedical compounds as token boundaries that still contribute a
                // combined phrase via variant expansion.
                flushTokenVariants(cur, tokens);
                cur.clear();
            } else {
                flushTokenVariants(cur, tokens);
                cur.clear();
            }
        }
        flushTokenVariants(cur, tokens);

        // Prune obvious noise tokens before generating phrase aliases.
        std::vector<std::string> filtered;
        filtered.reserve(tokens.size());
        std::unordered_set<std::string> seenFiltered;
        for (const auto& tok : tokens) {
            if (tok.size() < 2) {
                continue;
            }
            if (kStopwords.find(tok) != kStopwords.end()) {
                continue;
            }
            if (seenFiltered.insert(tok).second) {
                filtered.push_back(tok);
            }
        }

        std::vector<std::string> out;
        out.reserve(filtered.size() * 3);
        std::unordered_set<std::string> seenOut;

        // N-grams (4..2) first so phrase/entity labels are attempted before noisy unigrams.
        constexpr std::size_t kMaxN = 4;
        constexpr std::size_t kMaxAliases = 96;
        for (std::size_t n = kMaxN; n >= 2; --n) {
            if (filtered.size() < n) {
                continue;
            }
            for (std::size_t i = 0; i + n <= filtered.size(); ++i) {
                std::string phrase = filtered[i];
                for (std::size_t j = 1; j < n; ++j) {
                    phrase.push_back(' ');
                    phrase.append(filtered[i + j]);
                }
                if (seenOut.insert(phrase).second) {
                    out.push_back(std::move(phrase));
                }
                if (out.size() >= kMaxAliases) {
                    return out;
                }
            }
            if (n == 2) {
                break;
            }
        }

        // Unigrams after phrases.
        for (const auto& tok : filtered) {
            if (seenOut.insert(tok).second) {
                out.push_back(tok);
            }
            if (out.size() >= kMaxAliases) {
                break;
            }
        }

        return out;
    }

    bool timedOut(const std::chrono::steady_clock::time_point& t0) const {
        if (cfg_.budget.count() <= 0)
            return false;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0);
        return elapsed > cfg_.budget;
    }

    KGScoringConfig cfg_{};
    std::shared_ptr<KnowledgeGraphStore> store_;
    std::shared_ptr<IGraphQueryService> graphService_; // Optional: enhanced traversal
    std::vector<KGExplain> last_expl_;
};

// Factory helper (optional): create a SimpleKGScorer bound to a KG store
std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store,
                   std::shared_ptr<IGraphQueryService> graphService) {
    auto scorer = std::make_shared<SimpleKGScorer>(std::move(store));
    if (graphService) {
        scorer->setGraphQueryService(std::move(graphService));
    }
    return scorer;
}

} // namespace yams::search
