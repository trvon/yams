#include <yams/search/kg_scorer.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::search {

using yams::metadata::KnowledgeGraphStore;
using yams::metadata::AliasResolution;
using yams::metadata::DocEntity;

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

    Result<std::unordered_map<std::string, KGScore>> score(
        const std::string& query_text,
        const std::vector<std::string>& candidate_ids) override
    {
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
            if (timedOut(t0)) break;

            // Try exact first
            auto exact = store_->resolveAliasExact(a, 16);
            if (!exact) return exact.error();

            if (exact.value().empty()) {
                // Fallback to fuzzy/FTS if enabled by store config
                auto fuzzy = store_->resolveAliasFuzzy(a, 16);
                if (!fuzzy) return fuzzy.error();
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
            if (timedOut(t0)) break;
            auto nb = store_->neighbors(nid, cfg_.max_neighbors);
            if (!nb) return nb.error();
            for (auto v : nb.value()) {
                query_neighbor_union.insert(v);
            }
        }

        // 2) Score each candidate
        std::unordered_map<std::string, KGScore> out;
        out.reserve(candidate_ids.size());
        last_expl_.reserve(candidate_ids.size());

        for (const auto& cid : candidate_ids) {
            if (timedOut(t0)) break;

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
            out.emplace(cid, s);

            // Explanations (best-effort)
            if (!query_nodes.empty() && !cand_nodes.empty()) {
                expl.components["entity_jaccard"] = static_cast<double>(s.entity);
                expl.components["neighbor_overlap"] = static_cast<double>(s.structural);
                if (s.entity > 0.0f) {
                    expl.reasons.emplace_back("Shares linked entities with query");
                }
                if (s.structural > 0.0f) {
                    expl.reasons.emplace_back("Candidate entities are neighbors of query-linked entities");
                }
            }
            last_expl_.push_back(std::move(expl));
        }

        return out;
    }

    std::vector<KGExplain> getLastExplanations() const override {
        return last_expl_;
    }

private:
    // Helpers
    static float clamp01(float v) {
        if (v < 0.0f) return 0.0f;
        if (v > 1.0f) return 1.0f;
        return v;
    }

    static std::optional<std::int64_t> parseInt64(std::string_view s) {
        // Accept plain decimal, optionally with "doc:" prefix
        if (s.rfind("doc:", 0) == 0) {
            s.remove_prefix(4);
        }
        // Reject empty
        if (s.empty()) return std::nullopt;
        bool neg = false;
        if (s[0] == '-') {
            neg = true;
            s.remove_prefix(1);
        }
        if (s.empty()) return std::nullopt;

        std::int64_t val = 0;
        for (char c : s) {
            if (c < '0' || c > '9') return std::nullopt;
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
        if (a.empty() || b.empty()) return 0.0f;

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
        if (uni == 0) return 0.0f;
        return static_cast<float>(inter) / static_cast<float>(uni);
    }

    static float structuralOverlap(const std::unordered_set<std::int64_t>& neighbor_union,
                                   const std::unordered_set<std::int64_t>& cand_nodes) {
        if (neighbor_union.empty() || cand_nodes.empty()) return 0.0f;

        std::size_t overlap = 0;
        for (auto x : cand_nodes) {
            if (neighbor_union.find(x) != neighbor_union.end()) {
                ++overlap;
            }
        }
        // Normalize by candidate cardinality (bounded to [0,1])
        return cand_nodes.empty()
                   ? 0.0f
                   : std::min(1.0f, static_cast<float>(overlap) / static_cast<float>(cand_nodes.size()));
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
        if (!cur.empty()) tokens.push_back(cur);

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
        if (cfg_.budget.count() <= 0) return false;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0);
        return elapsed > cfg_.budget;
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