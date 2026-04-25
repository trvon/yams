#pragma once

#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>

#include <simeon/fragment_geometry.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
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

        // Default-on fragment geometry reranker. Uses the primary BM25 variant
        // as the lexical leg and PHSS-driven fragment propagation as the
        // semantic leg. To keep local/test corpora stable, auto-activation is
        // gated by fragment_geometry_min_corpus_docs and a vocabulary-richness
        // preflight; below those thresholds yams falls back to plain BM25.
        bool fragment_geometry_enabled = true;
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
    std::size_t doc_count() const noexcept { return doc_count_; }
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
    std::vector<std::vector<simeon::SemanticFragment>> doc_frags_;
    std::unordered_map<std::int64_t, std::uint32_t> doc_id_to_index_;
    std::size_t doc_count_ = 0;
};

} // namespace yams::search
