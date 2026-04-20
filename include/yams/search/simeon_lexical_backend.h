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
}

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
class SimeonLexicalBackend {
public:
    enum class Variant : std::uint8_t {
        Atire,     // Robertson BM25 — matches FTS5 semantics, debug/diff aid
        SabSmooth, // SubwordAwareBackoff γ=5 — "best of simeon" default
    };

    struct Config {
        Variant variant = Variant::SabSmooth;
        float subword_gamma = 5.0f;
        std::size_t max_corpus_docs = 200'000;
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

private:
    Config cfg_;
    std::atomic<bool> ready_{false};
    std::atomic<bool> building_{false};

    // Populated by the background build thread; published once ready_ is
    // stored with release order.
    std::unique_ptr<simeon::Bm25Index> index_;
    std::unordered_map<std::int64_t, std::uint32_t> doc_id_to_index_;
    std::size_t doc_count_ = 0;
};

} // namespace yams::search
