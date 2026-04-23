#include <yams/search/simeon_lexical_backend.h>

#include <yams/metadata/metadata_repository.h>

#include <spdlog/spdlog.h>
#include <simeon/bm25.hpp>
#include <simeon/fragment_geometry.hpp>
#include <simeon/pmi.hpp>
#include <simeon/query_router.hpp>
#include <simeon/simeon.hpp>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__GLIBC__)
#include <malloc.h>
#endif

#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>
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

std::size_t estimateUniqueWordCount(std::span<const std::string> docs) {
    std::unordered_set<std::string> uniq;
    std::string tok;
    for (const auto& doc : docs) {
        tok.clear();
        for (unsigned char c : doc) {
            if (std::isalnum(c)) {
                tok.push_back(static_cast<char>(std::tolower(c)));
            } else if (!tok.empty()) {
                if (tok.size() >= 3) {
                    uniq.insert(tok);
                }
                tok.clear();
            }
        }
        if (!tok.empty() && tok.size() >= 3) {
            uniq.insert(tok);
        }
    }
    return uniq.size();
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
        std::vector<std::string> corpus_texts;
        corpus_texts.reserve(ids.size());

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
            corpus_texts.push_back(content.contentText);
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

        std::unique_ptr<simeon::PmiEmbeddings> pmi;
        std::unique_ptr<simeon::Encoder> fragmentEncoder;
        std::vector<std::vector<simeon::SemanticFragment>> docFrags;
        const std::size_t uniqueWordCount = estimateUniqueWordCount(corpus_texts);
        if (cfg_.fragment_geometry_enabled &&
            corpus_texts.size() >= cfg_.fragment_geometry_min_corpus_docs &&
            uniqueWordCount >= 64) {
            try {
                std::vector<std::string_view> seedViews;
                seedViews.reserve(corpus_texts.size());
                for (const auto& doc : corpus_texts) {
                    seedViews.emplace_back(doc);
                }

                simeon::PmiConfig pcfg;
                pcfg.target_rank = 32;
                pcfg.min_token_count = 5;
                pcfg.max_vocab_size = 20'000;
                auto learned = simeon::PmiEmbeddings::learn(
                    std::span<const std::string_view>(seedViews), pcfg);

                simeon::EncoderConfig ecfg;
                ecfg.ngram_mode = simeon::NGramMode::WordOnly;
                ecfg.ngram_min = 1;
                ecfg.ngram_max = 1;
                ecfg.sketch_dim = 0;
                ecfg.output_dim = learned.dim();
                ecfg.projection = simeon::ProjectionMode::None;
                ecfg.l2_normalize = true;

                pmi = std::make_unique<simeon::PmiEmbeddings>(std::move(learned));
                ecfg.pmi_rows = pmi.get();
                fragmentEncoder = std::make_unique<simeon::Encoder>(ecfg);

                docFrags.reserve(corpus_texts.size());
                for (const auto& doc : corpus_texts) {
                    if ((docFrags.size() & 0x3ffu) == 0u && stop.stop_requested()) {
                        building_.store(false, std::memory_order_release);
                        return;
                    }
                    docFrags.push_back(simeon::build_doc_semantic_fragments(
                        *fragmentEncoder, doc, *primary, cfg_.fragment_build_top_sentences,
                        cfg_.fragment_build_signature_terms));
                }
            } catch (const std::exception& e) {
                spdlog::warn("[simeon-lexical] fragment geometry disabled: {}", e.what());
                pmi.reset();
                fragmentEncoder.reset();
                docFrags.clear();
            }
        } else if (cfg_.fragment_geometry_enabled && !corpus_texts.empty()) {
            spdlog::info("[simeon-lexical] fragment geometry skipped: corpus {} docs / {} unique "
                         "words below thresholds (min_docs={}, min_unique_words=64)",
                         corpus_texts.size(), uniqueWordCount,
                         cfg_.fragment_geometry_min_corpus_docs);
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
        pmi_ = std::move(pmi);
        fragment_encoder_ = std::move(fragmentEncoder);
        doc_frags_ = std::move(docFrags);
        doc_id_to_index_ = std::move(mapping);
        doc_count_ = dense;
        ready_.store(true, std::memory_order_release);
        building_.store(false, std::memory_order_release);

        spdlog::info("[simeon-lexical] ready: variant={} router={} fragment_geometry={} docs={} "
                     "missing={} build_ms={}",
                     variantLabel(cfg_.variant), cfg_.router_enabled ? "on" : "off",
                     fragment_encoder_ ? "on" : "off", dense, missing, buildMs);
        // Release the transient tf / posting scratch pages back to the OS.
        // BM25 index construction churns hundreds of MB of short-lived
        // allocations (per-doc tf maps, rehashes in FlatHashMapU64); macOS
        // otherwise keeps them parked in the DefaultMallocZone.
#if defined(__APPLE__)
        ::malloc_zone_pressure_relief(nullptr, 0);
#elif defined(__GLIBC__)
        ::malloc_trim(0);
#endif
    });

    return Result<void>{};
}

Result<std::vector<float>>
SimeonLexicalBackend::score(std::string_view query,
                            std::span<const std::int64_t> candidate_doc_ids) const {
    if (!ready_.load(std::memory_order_acquire) || !index_) {
        return Error{ErrorCode::NotInitialized, "SimeonLexicalBackend: not ready"};
    }

    std::vector<float> full;
    std::vector<float> lexical;
    if (cfg_.fragment_geometry_enabled && fragment_encoder_ && !doc_frags_.empty()) {
        full = simeon::score_fragment_geometry(query, *index_, *fragment_encoder_, doc_frags_,
                                               cfg_.fragment_geometry_config);
        lexical.assign(doc_count_, 0.0f);
        index_->score(query, std::span<float>{lexical});
    } else {
        full.assign(doc_count_, 0.0f);
        index_->score(query, std::span<float>{full});
    }

    std::vector<float> out;
    out.reserve(candidate_doc_ids.size());
    for (auto id : candidate_doc_ids) {
        auto it = doc_id_to_index_.find(id);
        if (it == doc_id_to_index_.end()) {
            out.push_back(0.0f);
            continue;
        }
        const auto di = it->second;
        const float score = std::isfinite(full[di]) ? full[di] : lexical[di];
        out.push_back(score);
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

    std::vector<float> full;
    std::vector<float> lexical;
    if (cfg_.fragment_geometry_enabled && fragment_encoder_ && !doc_frags_.empty()) {
        full = simeon::score_fragment_geometry(query, *chosen, *fragment_encoder_, doc_frags_,
                                               cfg_.fragment_geometry_config);
        lexical.assign(doc_count_, 0.0f);
        chosen->score(query, std::span<float>{lexical});
    } else {
        full.assign(doc_count_, 0.0f);
        chosen->score(query, std::span<float>{full});
    }

    RescoreDecision decision;
    decision.recipe_name = recipe_label;
    decision.scores.reserve(candidate_doc_ids.size());
    for (auto id : candidate_doc_ids) {
        auto it = doc_id_to_index_.find(id);
        if (it == doc_id_to_index_.end()) {
            decision.scores.push_back(0.0f);
            continue;
        }
        const auto di = it->second;
        const float score = std::isfinite(full[di]) ? full[di] : lexical[di];
        decision.scores.push_back(score);
    }
    return decision;
}

} // namespace yams::search
