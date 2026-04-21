#pragma once

#include <yams/core/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace simeon {
class Bm25Index;
class QueryRouter;
struct RouterConfig;
enum class Recipe : std::uint8_t;
} // namespace simeon

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::search {

// In-memory SAB-smooth BM25 reranker over the FTS5 candidate pool. Wraps
// simeon::Bm25Index (third_party/simeon/include/simeon/bm25.hpp) so the
// SearchEngine can rescore FTS5 hits with simeon's SubwordAwareBackoff
// variant, which matches MiniLM-L6 on NFCorpus nDCG@10 and ties the best
// pool-rerank baselines on scifact / FiQA (see
// third_party/simeon/docs/benchmarks.md).
//
// Activation is bundled with the existing simeon embedding opt-in:
// SearchEngine instantiates this only when
// ConfigResolver::resolveEmbeddingBackend() == "simeon". When simeon is off
// or build() has not completed, score() is a no-op and SearchEngine falls
// back to FTS5 BM25, byte-identical to pre-integration behavior.
//
// When `Config::router_enabled` is true, the backend additionally builds
// the Atire variant and exposes scoreRouted(), which picks between Atire
// and SabSmooth per-query via simeon::QueryRouter using the passE preset
// by default (see third_party/simeon/docs/router_design.md).
class SimeonLexicalBackend {
public:
    enum class Variant : std::uint8_t {
        Atire,     // Robertson BM25 — matches FTS5 semantics, debug/diff aid
        SabSmooth, // SubwordAwareBackoff γ=5 — "best of simeon" default
    };

    struct RouterPreset {
        // passE_scq0_clar3 — the shippable router config from simeon's BEIR
        // bench. NFCorpus +0.009 nDCG@10 vs single-variant SAB; ties MiniLM.
        // See third_party/simeon/docs/router_design.md and docs/benchmarks.md.
        float oov_threshold = 0.0f;
        float high_idf_threshold = 3.0f;
        std::uint32_t cascade_min_terms = 4u;
        float cascade_max_idf = 5.0f;
        float atire_min_scq = 0.0f;
        float atire_max_clarity = 3.0f;
    };

    struct Config {
        Variant variant = Variant::SabSmooth;
        float subword_gamma = 5.0f;
        std::size_t max_corpus_docs = 200'000;
        bool router_enabled = false;
        RouterPreset router_preset{};
    };

    explicit SimeonLexicalBackend(Config cfg);
    ~SimeonLexicalBackend();

    SimeonLexicalBackend(const SimeonLexicalBackend&) = delete;
    SimeonLexicalBackend& operator=(const SimeonLexicalBackend&) = delete;

    // Kick off a background rebuild from the current document corpus.
    // Returns immediately; ready() flips to true when the thread finishes.
    // Safe to call once per backend instance; a second call while the
    // first is in flight is a no-op.
    Result<void> buildAsync(std::shared_ptr<metadata::MetadataRepository> repo);

    bool ready() const noexcept { return ready_.load(std::memory_order_acquire); }
    std::size_t doc_count() const noexcept { return doc_count_; }
    const Config& config() const noexcept { return cfg_; }

    // Rescore an FTS5 candidate pool. Runs the full-corpus
    // simeon::Bm25Index::score() internally and projects to the requested
    // subset. Returns one float per candidate_doc_id, same order; missing
    // doc_ids (not present in the simeon index) get 0.0f.
    Result<std::vector<float>> score(std::string_view query,
                                     std::span<const std::int64_t> candidate_doc_ids) const;

    struct RescoreDecision {
        // Short recipe label: "Bm25Atire" | "Bm25SabSmooth" | "CascadeLinearAlpha".
        // Debug-only; stash into response.debugStats["simeon_route"].
        const char* recipe_name = "Bm25SabSmooth";
        std::vector<float> scores;
    };

    // Router-driven rescore: computes pre-retrieval QueryFeatures, picks a
    // recipe via simeon::QueryRouter (passE preset by default), scores
    // against the selected BM25 variant. When router_enabled is false this
    // behaves exactly like score() + fixed recipe_name = config variant.
    //
    // CascadeLinearAlpha maps to the SabSmooth index at this layer — yams's
    // ResultFusion already implements the α=0.75 linear-z hybrid that
    // CascadeLinearAlpha describes, so the routing decision just picks which
    // BM25 variant feeds into that fusion.
    Result<RescoreDecision> scoreRouted(std::string_view query,
                                        std::span<const std::int64_t> candidate_doc_ids) const;

private:
    Config cfg_;
    std::atomic<bool> ready_{false};
    std::atomic<bool> building_{false};

    // Populated by the background build thread; published once ready_ is
    // stored with release order.
    std::unique_ptr<simeon::Bm25Index>
        index_; // variant per cfg_.variant (or SabSmooth if router on)
    std::unique_ptr<simeon::Bm25Index> atire_index_; // built only when router_enabled
    std::unique_ptr<simeon::QueryRouter> router_;    // built only when router_enabled
    std::unordered_map<std::int64_t, std::uint32_t> doc_id_to_index_;
    std::size_t doc_count_ = 0;
};

} // namespace yams::search
