#include <yams/search/simeon_lexical_backend.h>

#include <yams/metadata/metadata_repository.h>

#include <simeon/bm25.hpp>
#include <simeon/query_router.hpp>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace yams::search {

namespace {

simeon::Bm25Variant toSimeonVariant(SimeonLexicalBackend::Variant v) noexcept {
    switch (v) {
        case SimeonLexicalBackend::Variant::Atire:
            return simeon::Bm25Variant::Atire;
        case SimeonLexicalBackend::Variant::SabSmooth:
            return simeon::Bm25Variant::SubwordAwareBackoff;
    }
    return simeon::Bm25Variant::SubwordAwareBackoff;
}

const char* variantLabel(SimeonLexicalBackend::Variant v) noexcept {
    switch (v) {
        case SimeonLexicalBackend::Variant::Atire:
            return "Atire";
        case SimeonLexicalBackend::Variant::SabSmooth:
            return "SabSmooth";
    }
    return "?";
}

simeon::RouterConfig toRouterConfig(const SimeonLexicalBackend::RouterPreset& p) noexcept {
    simeon::RouterConfig rc;
    rc.oov_threshold = p.oov_threshold;
    rc.high_idf_threshold = p.high_idf_threshold;
    rc.cascade_min_terms = p.cascade_min_terms;
    rc.cascade_max_idf = p.cascade_max_idf;
    rc.atire_min_scq = p.atire_min_scq;
    rc.atire_max_clarity = p.atire_max_clarity;
    return rc;
}

} // namespace

SimeonLexicalBackend::SimeonLexicalBackend(Config cfg) : cfg_(cfg) {}
SimeonLexicalBackend::~SimeonLexicalBackend() {
    // Ensure the detached build thread cannot outlive this instance. jthread's
    // destructor requests stop and joins; the build thread is expected to honor
    // stop_token at its cancellation checkpoints and return without publishing.
    if (build_thread_.joinable()) {
        build_thread_.request_stop();
        build_thread_.join();
    }
}

Result<void> SimeonLexicalBackend::buildAsync(std::shared_ptr<metadata::MetadataRepository> repo) {
    if (!repo) {
        return Error{ErrorCode::InvalidArgument, "SimeonLexicalBackend: null metadata repo"};
    }
    bool expected = false;
    if (!building_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return Result<void>{};
    }

    // If a previous build thread finished and was never reaped, join it now to
    // keep jthread's invariants clean before we start a new one.
    if (build_thread_.joinable()) {
        build_thread_.join();
    }

    build_thread_ = std::jthread([this, repo = std::move(repo)](std::stop_token stop) mutable {
        const auto t0 = std::chrono::steady_clock::now();

        auto idsResult = repo->getAllFts5IndexedDocumentIds();
        if (!idsResult) {
            spdlog::warn("[simeon-lexical] enumerate doc ids failed: {}",
                         idsResult.error().message);
            building_.store(false, std::memory_order_release);
            return;
        }
        const auto& ids = idsResult.value();
        if (ids.size() > cfg_.max_corpus_docs) {
            spdlog::info("[simeon-lexical] corpus {} docs > cap {} — staying on FTS5 only",
                         ids.size(), cfg_.max_corpus_docs);
            building_.store(false, std::memory_order_release);
            return;
        }
        if (stop.stop_requested()) {
            building_.store(false, std::memory_order_release);
            return;
        }

        // Primary index: matches cfg_.variant (used by score()).
        simeon::Bm25Config bcfg;
        bcfg.variant = toSimeonVariant(cfg_.variant);
        bcfg.subword_gamma = cfg_.subword_gamma;
        auto primary = std::make_unique<simeon::Bm25Index>(bcfg);

        // Secondary index: Atire, only when routing is enabled AND the
        // primary isn't already Atire. Router dispatches between
        // {Atire, SabSmooth}, so the pair is always (Atire, SabSmooth).
        std::unique_ptr<simeon::Bm25Index> atire;
        const bool build_atire =
            cfg_.router_enabled && cfg_.variant != SimeonLexicalBackend::Variant::Atire;
        if (build_atire) {
            simeon::Bm25Config acfg;
            acfg.variant = simeon::Bm25Variant::Atire;
            atire = std::make_unique<simeon::Bm25Index>(acfg);
        }

        spdlog::info("[simeon-lexical] bm25_config: variant={} subword_gamma={} "
                     "router={} build_atire={} max_corpus_docs={} corpus_docs={}",
                     variantLabel(cfg_.variant), cfg_.subword_gamma,
                     cfg_.router_enabled ? "on" : "off", build_atire ? "yes" : "no",
                     cfg_.max_corpus_docs, ids.size());

        std::unordered_map<std::int64_t, std::uint32_t> mapping;
        mapping.reserve(ids.size());

        std::uint32_t dense = 0;
        std::size_t missing = 0;
        for (auto docId : ids) {
            // Check stop every ~1k docs so shutdown doesn't wait the full build.
            if ((dense & 0x3ffu) == 0u && stop.stop_requested()) {
                building_.store(false, std::memory_order_release);
                return;
            }
            auto contentResult = repo->getContent(docId);
            if (!contentResult || !contentResult.value()) {
                ++missing;
                continue;
            }
            const auto& content = contentResult.value().value();
            primary->add_doc(content.contentText);
            if (atire) {
                atire->add_doc(content.contentText);
            }
            mapping.emplace(docId, dense++);
        }
        if (stop.stop_requested()) {
            // Owner is going away — drop locally owned indexes; do not publish
            // into member state (would be UAF on a destructed `this`).
            building_.store(false, std::memory_order_release);
            return;
        }
        primary->finalize();
        if (atire) {
            atire->finalize();
        }

        // Router uses the Atire index when available (matches simeon bench
        // convention — df/idf values are BM25-variant-independent, so the
        // choice is cosmetic). If router is enabled but the primary IS
        // Atire, use the primary directly; no secondary index exists.
        std::unique_ptr<simeon::QueryRouter> router;
        if (cfg_.router_enabled) {
            const simeon::Bm25Index& rindex = atire ? *atire : *primary;
            router =
                std::make_unique<simeon::QueryRouter>(rindex, toRouterConfig(cfg_.router_preset));
        }

        if (stop.stop_requested()) {
            building_.store(false, std::memory_order_release);
            return;
        }

        const auto t1 = std::chrono::steady_clock::now();
        const auto buildMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        index_ = std::move(primary);
        atire_index_ = std::move(atire);
        router_ = std::move(router);
        doc_id_to_index_ = std::move(mapping);
        doc_count_ = dense;
        ready_.store(true, std::memory_order_release);
        building_.store(false, std::memory_order_release);

        spdlog::info("[simeon-lexical] ready: variant={} router={} docs={} missing={} build_ms={}",
                     variantLabel(cfg_.variant), cfg_.router_enabled ? "on" : "off", dense, missing,
                     buildMs);
    });

    return Result<void>{};
}

Result<std::vector<float>>
SimeonLexicalBackend::score(std::string_view query,
                            std::span<const std::int64_t> candidate_doc_ids) const {
    if (!ready_.load(std::memory_order_acquire) || !index_) {
        return Error{ErrorCode::NotInitialized, "SimeonLexicalBackend: not ready"};
    }

    std::vector<float> full(doc_count_, 0.0f);
    index_->score(query, std::span<float>{full});

    std::vector<float> out;
    out.reserve(candidate_doc_ids.size());
    for (auto id : candidate_doc_ids) {
        auto it = doc_id_to_index_.find(id);
        out.push_back(it == doc_id_to_index_.end() ? 0.0f : full[it->second]);
    }
    return out;
}

Result<SimeonLexicalBackend::RescoreDecision>
SimeonLexicalBackend::scoreRouted(std::string_view query,
                                  std::span<const std::int64_t> candidate_doc_ids) const {
    if (!ready_.load(std::memory_order_acquire) || !index_) {
        return Error{ErrorCode::NotInitialized, "SimeonLexicalBackend: not ready"};
    }

    // Routing disabled → single-variant rescore; label from cfg_.variant.
    if (!router_) {
        auto scores = score(query, candidate_doc_ids);
        if (!scores) {
            return Error{scores.error()};
        }
        RescoreDecision decision;
        decision.recipe_name = variantLabel(cfg_.variant);
        decision.scores = std::move(scores.value());
        return decision;
    }

    const auto recipe = router_->choose(query);
    // CascadeLinearAlpha at this layer folds to SabSmooth rescoring — the
    // α=0.75 hybrid is realized by yams's fuseWeightedLinearZScore above
    // this call site, not by a separate BM25 variant.
    const simeon::Bm25Index* chosen = index_.get();
    const char* recipe_label = simeon::recipe_name(recipe);
    if (recipe == simeon::Recipe::Bm25Atire) {
        chosen = atire_index_ ? atire_index_.get() : index_.get();
    }
    // else (Bm25SabSmooth, CascadeLinearAlpha) → primary index, which is
    // SabSmooth by convention when routing is on.

    std::vector<float> full(doc_count_, 0.0f);
    chosen->score(query, std::span<float>{full});

    RescoreDecision decision;
    decision.recipe_name = recipe_label;
    decision.scores.reserve(candidate_doc_ids.size());
    for (auto id : candidate_doc_ids) {
        auto it = doc_id_to_index_.find(id);
        decision.scores.push_back(it == doc_id_to_index_.end() ? 0.0f : full[it->second]);
    }
    return decision;
}

} // namespace yams::search
