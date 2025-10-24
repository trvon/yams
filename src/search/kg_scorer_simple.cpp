#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/kg_scorer.h>

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

        // Resolve aliases to nodes (budget-aware)
        for (const auto& a : query_aliases) {
            if (timedOut(t0))
                break;

            // Try exact first
            auto exact = store_->resolveAliasExact(a, 16);
            if (!exact)
                return exact.error();

            if (exact.value().empty()) {
                // Fallback to fuzzy/FTS if enabled by store config
                auto fuzzy = store_->resolveAliasFuzzy(a, 16);
                if (!fuzzy)
                    return fuzzy.error();
                for (const auto& ar : fuzzy.value()) {
                    query_nodes.insert(ar.nodeId);
                }
            } else {
                for (const auto& ar : exact.value()) {
                    query_nodes.insert(ar.nodeId);
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

            // Parse candidate id as document id (best-effort)
            auto docIdOpt = parseInt64(cid);
            if (!docIdOpt.has_value()) {
                // If not numeric, emit zeros (missing id implies zero by contract)
                out.emplace(cid, s);
                continue;
            }
            const auto docId = docIdOpt.value();

            // Fetch document entities
            auto entsR = store_->getDocEntitiesForDocument(docId, 2000, 0);
            if (!entsR) {
                // If doc lookup fails, keep zero scores (do not hard-fail the whole batch)
                out.emplace(cid, s);
                continue;
            }
            const auto& ents = entsR.value();

            // Build candidate node set
            std::unordered_set<std::int64_t> cand_nodes;
            cand_nodes.reserve(ents.size());
            for (const auto& de : ents) {
                if (de.nodeId.has_value()) {
                    cand_nodes.insert(de.nodeId.value());
                }
            }

            // Entity score: Jaccard between query_nodes and cand_nodes
            const float entity = jaccard(query_nodes, cand_nodes);

            // Structural score: neighbor overlap normalized by candidate size
            const float structural = structuralOverlap(query_neighbor_union, cand_nodes);

            s.entity = clamp01(entity);
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
            }
            last_expl_.push_back(std::move(expl));
        }

        return out;
    }

    std::vector<KGExplain> getLastExplanations() const override { return last_expl_; }

private:
    // Helpers
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

    // Very simple tokenizer: lowercase alnum sequences as aliases + join bigrams
    static std::vector<std::string> tokenize(const std::string& text) {
        std::vector<std::string> tokens;
        tokens.reserve(text.size() / 5 + 1);
        std::string cur;
        for (unsigned char uc : text) {
            if (std::isalnum(uc)) {
                cur.push_back(static_cast<char>(std::tolower(uc)));
            } else {
                if (!cur.empty()) {
                    tokens.push_back(cur);
                    cur.clear();
                }
            }
        }
        if (!cur.empty())
            tokens.push_back(cur);

        // Merge in bigrams to capture simple phrases like "new york"
        std::vector<std::string> out = tokens;
        for (std::size_t i = 0; i + 1 < tokens.size(); ++i) {
            std::string bi;
            bi.reserve(tokens[i].size() + 1 + tokens[i + 1].size());
            bi.append(tokens[i]).push_back(' ');
            bi.append(tokens[i + 1]);
            out.push_back(std::move(bi));
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

    // Symbol query detection (PBI-059): Just check if KG resolves query to symbol nodes
    SymbolQueryHints detectSymbolQuery(const std::string& query_text) const override {
        SymbolQueryHints hints;

        if (!store_) {
            return hints;
        }

        // Tokenize and try resolving
        auto tokens = tokenize(query_text);
        hints.symbolTokens.reserve(tokens.size());

        for (const auto& token : tokens) {
            auto exact = store_->resolveAliasExact(token, 5);
            if (!exact || exact.value().empty()) {
                continue;
            }

            // Check if any resolved nodes are symbol types
            for (const auto& ar : exact.value()) {
                auto nodeRes = store_->getNodeById(ar.nodeId);
                if (!nodeRes || !nodeRes.value().has_value()) {
                    continue;
                }

                const auto& node = nodeRes.value().value();

                // Check if node type is a code symbol
                if (node.type &&
                    (node.type == "function" || node.type == "class" || node.type == "method" ||
                     node.type == "variable" || node.type == "typedef" || node.type == "macro")) {
                    hints.looksLikeSymbol = true;
                    hints.symbolTokens.push_back(token);

                    // Check for qualifiers in node key
                    if (node.nodeKey.find("::") != std::string::npos) {
                        hints.hasQualifiers = true;
                        hints.detectedLanguage = "cpp";
                    } else if (node.nodeKey.find('.') != std::string::npos) {
                        hints.hasQualifiers = true;
                        hints.detectedLanguage = "python";
                    }
                    break; // Found symbol node for this token
                }
            }
        }

        return hints;
    }

private:
    KGScoringConfig cfg_{};
    std::shared_ptr<KnowledgeGraphStore> store_;
    std::vector<KGExplain> last_expl_;
};

// Factory helper (optional): create a SimpleKGScorer bound to a KG store
std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store) {
    return std::make_shared<SimpleKGScorer>(std::move(store));
}

} // namespace yams::search
