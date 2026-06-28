#pragma once

#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>

#include <simeon/concept_mining.hpp>
#include <simeon/fragment_geometry.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace simeon {
class Bm25Index;
class Encoder;
class PmiEmbeddings;
class QueryRouter;
struct RouterConfig;
enum class Recipe : std::uint8_t;
class RetrievalStrategy;
class StrategyRouter;
class TextAdapter;
struct QueryProfile;
} // namespace simeon

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::search {

// In-memory simeon lexical / fragment-geometry reranker over the FTS5 candidate
// pool. Wraps simeon::Bm25Index plus optional PMI fragment geometry so the
// SearchEngine can rescore FTS5 hits with simeon's strongest training-free
// sparse retrieval stack.
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
//
// Default is router_enabled=false. Rationale: simeon's own three-corpus
// evaluation (docs/research/benchmarks.md, training_free_saturation.md)
// shows SAB-smooth γ=5 alone is within ~1.8 nDCG@10 points of the
// dual-build router on scifact, ties it on NFCorpus, and trails by 0.006
// on FiQA — for roughly 2× the BM25 steady-state memory. The dual-build
// is kept as an opt-in for callers who want the marginal lift.
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
        // Hard cap on raw corpus text bytes scanned into the in-memory simeon
        // lexical build. When exceeded, yams stays on FTS5-only instead of
        // attempting an unbounded in-memory lexical index build.
        // Real default is computed by ResourceGovernor::recommendLexicalCorpusBytes()
        // which scales with the daemon memory budget; 256 MiB is the floor used
        // when the governor is disabled or unavailable.
        std::size_t max_corpus_bytes = 256ULL * 1024ULL * 1024ULL;
        // Chunk very large documents into a small number of evenly spaced
        // windows before feeding them into the in-memory simeon lexical
        // enhancement stack. This bounds char-ngram / fragment build pressure
        // without affecting the FTS5 full-text index.
        std::size_t build_doc_chunk_bytes = 16ULL * 1024ULL;
        std::size_t build_doc_max_chunks = 4;
        bool router_enabled = false;
        RouterPreset router_preset{};

        // Enable the new simeon strategy-routing framework (retrieval_strategy.hpp).
        // When true, replaces per-query QueryRouter selection with an EntropyRouter
        // that chooses among BM25, Keyphrase, and LeadField strategies based on
        // query BM25-score entropy. Requires router_enabled=false (they conflict).
        bool strategy_router_enabled = false;

        // When true, build both SabSmooth and Atire BM25 variants and RRF-fuse
        // their rankings instead of routing to a single variant. Shown to
        // improve nDCG@10 by +0.009 on NFCorpus in simeon benchmarks.
        bool bm25_variants_rrf = false;

        // Fragment-geometry (PHSS topology) reranker. OFF by default: a per-arm
        // retrieval bench showed it loses to plain SAB and lead_field even after
        // its scale-mixing bug was fixed (nDCG@10 0.243 vs 0.35 baseline) while
        // costing the bulk of index-build time (PMI learn + fragment build +
        // PHSS). Opt-in for prose corpora via this flag; the scoreRouted path
        // still consumes it (quality-router gated) when enabled. The simeon
        // fragment_geometry/PHSS code remains as the research asset.
        bool fragment_geometry_enabled = false;
        std::size_t fragment_geometry_min_corpus_docs = 1000;
        // Fragment geometry is bounded separately from the lexical build. The
        // builder keeps only a bounded PMI sample in memory, then streams a
        // second pass to build fragment vectors/doc signatures for at most the
        // first `fragment_geometry_max_docs` docs and `fragment_geometry_max_corpus_bytes`
        // bytes. Remaining docs stay lexical-only.
        std::size_t fragment_geometry_max_docs = 20'000;
        std::size_t fragment_geometry_max_corpus_bytes = 64ULL * 1024ULL * 1024ULL;
        std::size_t fragment_geometry_pmi_sample_docs = 8192;
        std::size_t fragment_geometry_pmi_sample_bytes = 32ULL * 1024ULL * 1024ULL;
        std::uint32_t fragment_build_top_sentences = 6;
        std::uint32_t fragment_build_signature_terms = 8;
        simeon::FragmentGeometryConfig fragment_geometry_config{};

        // PMI-based concept mining: discovers word-bigram concepts from the
        // corpus at finalize time and blends them into BM25 scores at query
        // time. Training-free; enabled by default for prose corpora.
        bool concept_mining_enabled = false;
        simeon::ConceptConfig concept_config{};

        // When concept_mining_enabled is true and this callback is set,
        // the build thread calls fn(docId, entityText, confidence) for
        // every bigram concept matched in every document. Intended for
        // populating kg_doc_entities so the graph reranker can match
        // query concepts against document entities.
        using EntityCallback =
            std::function<void(std::int64_t docId, std::string entityText, float confidence)>;
        EntityCallback entity_callback;

        // Reserved for backend-owned bandit flows. Normal SearchEngine bandit
        // routing is controlled per request by SearchEngineConfig::simeonBanditArm.
        // Direct scoreBanditRouted() calls are available whenever the backend is
        // ready and receive an explicit arm name from the caller.
        bool bandit_arm_enabled = false;

        // Hot-term score cache: memoizes full-corpus BM25 score vectors for
        // frequently repeated queries. When set >0, up to this many queries
        // are cached. Default 0 (disabled). Typical: 64 for interactive use,
        // 0 for bulk evaluation.
        std::size_t score_cache_entries = 0;
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
    bool building() const noexcept { return building_.load(std::memory_order_acquire); }
    bool hasStrategyRouter() const noexcept { return strategy_router_ != nullptr; }
    std::size_t doc_count() const noexcept { return doc_count_; }
    bool concept_mining_enabled() const noexcept { return cfg_.concept_mining_enabled; }
    std::uint32_t concept_count() const noexcept {
        if (concept_index_)
            return concept_index_->size();
        return 0;
    }
    const Config& config() const noexcept { return cfg_; }
    bool fragmentGeometryReady() const noexcept {
        return fragment_encoder_ != nullptr && !doc_frags_.empty();
    }

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

    struct TopCandidate {
        std::int64_t document_id = 0;
        float score = 0.0f;
    };

    struct TopCandidateDecision {
        const char* recipe_name = "Bm25SabSmooth";
        std::vector<TopCandidate> candidates;
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

    // Strategy-router driven rescore (new retrieval_strategy.hpp framework).
    // When strategy_router_enabled is true, uses an EntropyRouter to choose
    // among BM25, Keyphrase, and LeadField strategies per query. Falls back
    // to scoreRouted() when the strategy router is not configured.
    Result<RescoreDecision>
    scoreStrategyRouted(std::string_view query,
                        std::span<const std::int64_t> candidate_doc_ids) const;

    // Bandit-driven rescore: selects the simeon scoring recipe for `arm_name`.
    // The arm names are preset keys that map to tested (R_q, R_d, S) combos
    // from the simeon Omega search. Recognized presets include:
    //   "sab_smooth"              - plain SAB-smooth gamma=5
    //   "bm25_variants_rrf"       - RRF over SAB-smooth + ATIRE when available
    //   "atire"                   - ATIRE BM25 when available
    //   "keyphrase"               - keyphrase strategy when available
    //   "lead_field"              - lead-field strategy when available
    // Falls back to plain SAB when the arm name is unrecognized.
    // Training-free at inference: the arm name is selected by TunerMAB from
    // qrel-free proxy rewards.
    Result<RescoreDecision>
    scoreBanditRouted(std::string_view query, std::string_view arm_name,
                      std::span<const std::int64_t> candidate_doc_ids) const;

    // Generate top documents directly from the Simeon corpus. This is used for weak lexical
    // queries where FTS5 does not provide enough candidates to rescore.
    Result<TopCandidateDecision> searchTop(std::string_view query, std::size_t limit,
                                           std::string_view arm_name = {}) const;

    /// Number of score cache hits since last reset.
    [[nodiscard]] std::uint64_t scoreCacheHits() const noexcept {
        return score_cache_hits_.load(std::memory_order_relaxed);
    }

    /// Reset the score cache hit counter.
    void resetScoreCacheHits() { score_cache_hits_.store(0, std::memory_order_relaxed); }

    /// Clear the score cache (e.g., after corpus rebuild).
    void clearScoreCache();

private:
    Config cfg_;
    std::atomic<bool> ready_{false};
    std::atomic<bool> building_{false};

    // Owning handle for the background build. compat::jthread uses native
    // std::jthread where available and falls back to an auto-joining
    // std::thread on libc++/SDK combinations that do not expose jthread.
    yams::compat::jthread build_thread_;

    // Populated by the background build thread; published once ready_ is
    // stored with release order.
    std::unique_ptr<simeon::Bm25Index>
        index_; // variant per cfg_.variant (or SabSmooth if router on)
    std::unique_ptr<simeon::Bm25Index> atire_index_; // built only when router_enabled
    std::unique_ptr<simeon::QueryRouter> router_;    // built only when router_enabled
    std::unique_ptr<simeon::PmiEmbeddings> pmi_;     // built only when fragment geometry is on
    std::unique_ptr<simeon::Encoder> fragment_encoder_;
    std::unique_ptr<simeon::ConceptIndex> concept_index_;
    std::vector<std::vector<simeon::SemanticFragment>> doc_frags_;

    // Strategy router (new retrieval_strategy.hpp framework)
    std::unique_ptr<simeon::TextAdapter> text_adapter_;
    std::vector<std::unique_ptr<simeon::RetrievalStrategy>> strategies_;
    std::unique_ptr<simeon::StrategyRouter> strategy_router_;
    std::unordered_map<std::int64_t, std::uint32_t> doc_id_to_index_;
    std::vector<std::int64_t> index_to_doc_id_;
    std::size_t doc_count_ = 0;

    // Hot-query score cache: memoizes full-corpus BM25 score vectors.
    // Keyed by query string and evicted LRU when full. Access is serialized
    // with score_cache_mutex_ because score() is const but mutates cache state.
    struct ScoreCacheEntry {
        std::vector<float> scores;
        std::uint32_t concept_count = 0; // invalidated when concept index changes
    };
    mutable std::mutex score_cache_mutex_;
    mutable std::list<std::pair<std::string, ScoreCacheEntry>> score_cache_list_;
    mutable std::unordered_map<std::string,
                               std::list<std::pair<std::string, ScoreCacheEntry>>::iterator>
        score_cache_map_;
    mutable std::atomic<std::uint64_t> score_cache_hits_{0};
};

} // namespace yams::search
