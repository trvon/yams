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
#include <mach/mach.h>
#include <mach/mach_init.h>
#include <mach/task_info.h>
#elif defined(__GLIBC__)
#include <malloc.h>
#include <sys/resource.h>
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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::search {

namespace {

std::uint64_t rssBytesCurrent() noexcept {
#if defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &infoCount) == KERN_SUCCESS) {
        return info.resident_size;
    }
    return 0;
#elif defined(__GLIBC__)
    struct rusage ru{};
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        return static_cast<std::uint64_t>(ru.ru_maxrss) * 1024ull;
    }
    return 0;
#else
    return 0;
#endif
}

void releaseTransientPages(const char* phase) {
    const auto before = rssBytesCurrent();
#if defined(__APPLE__)
    vm_address_t* zoneAddrs = nullptr;
    unsigned zoneCount = 0;
    if (malloc_get_all_zones(mach_task_self(), nullptr, &zoneAddrs, &zoneCount) == KERN_SUCCESS &&
        zoneAddrs != nullptr) {
        for (unsigned i = 0; i < zoneCount; ++i) {
            auto* zone = reinterpret_cast<malloc_zone_t*>(zoneAddrs[i]);
            if (zone == nullptr)
                continue;
            if (zone->pressure_relief != nullptr) {
                zone->pressure_relief(zone, 0);
            }
        }
    }
#elif defined(__GLIBC__)
    ::malloc_trim(0);
#endif
    const auto after = rssBytesCurrent();
    if (before > 0 && after > 0) {
        spdlog::info("[simeon-lexical] {} rss {} MiB -> {} MiB (released {} MiB)", phase,
                     before / (1024ull * 1024ull), after / (1024ull * 1024ull),
                     (before > after ? before - after : 0ull) / (1024ull * 1024ull));
    }
}

} // namespace

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

bool exceedsBudget(std::size_t used, std::size_t add, std::size_t limit) noexcept {
    return limit > 0 && add > 0 && (add > limit || used > limit - add);
}

std::size_t clampChunkStart(std::string_view text, std::size_t start) {
    if (start == 0 || start >= text.size()) {
        return std::min(start, text.size());
    }
    const std::size_t search_floor = (start > 128) ? start - 128 : 0;
    for (std::size_t i = start; i > search_floor; --i) {
        const unsigned char c = static_cast<unsigned char>(text[i - 1]);
        if (std::isspace(c)) {
            return i;
        }
    }
    return start;
}

std::size_t clampChunkEnd(std::string_view text, std::size_t end) {
    if (end >= text.size()) {
        return text.size();
    }
    const std::size_t search_ceiling = std::min(text.size(), end + 128);
    for (std::size_t i = end; i < search_ceiling; ++i) {
        const unsigned char c = static_cast<unsigned char>(text[i]);
        if (std::isspace(c)) {
            return i;
        }
    }
    return end;
}

struct ChunkedEnhancementText {
    std::string owned;
    std::string_view view;
    bool chunked{false};
};

ChunkedEnhancementText buildEnhancementText(std::string_view text,
                                            const SimeonLexicalBackend::Config& cfg) {
    const auto chunkBytes = cfg.build_doc_chunk_bytes;
    const auto maxChunks = cfg.build_doc_max_chunks;
    if (text.empty() || chunkBytes == 0 || maxChunks == 0) {
        return {.owned = {}, .view = text, .chunked = false};
    }
    const std::size_t totalBudget = chunkBytes * maxChunks;
    if (text.size() <= totalBudget) {
        return {.owned = {}, .view = text, .chunked = false};
    }

    const std::size_t maxStart = text.size() > chunkBytes ? text.size() - chunkBytes : 0;
    std::string out;
    out.reserve(std::min<std::size_t>(text.size(), totalBudget + maxChunks));
    std::size_t lastEnd = 0;
    for (std::size_t i = 0; i < maxChunks; ++i) {
        const std::size_t startBase =
            (maxChunks == 1) ? 0 : (maxStart * i) / static_cast<std::size_t>(maxChunks - 1);
        const std::size_t start = clampChunkStart(text, startBase);
        const std::size_t end = clampChunkEnd(text, std::min(text.size(), start + chunkBytes));
        if (end <= start || (i > 0 && end <= lastEnd)) {
            continue;
        }
        if (!out.empty()) {
            out.push_back('\n');
        }
        const std::size_t sliceStart = (start < lastEnd) ? lastEnd : start;
        out.append(text.substr(sliceStart, end - sliceStart));
        lastEnd = end;
    }
    if (out.empty()) {
        out.assign(text.substr(0, std::min(text.size(), chunkBytes)));
    }
    ChunkedEnhancementText result;
    result.owned = std::move(out);
    result.view = result.owned;
    result.chunked = true;
    return result;
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
        primary->reserve_docs(ids.size());

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
            atire->reserve_docs(ids.size());
        }

        spdlog::info("[simeon-lexical] bm25_config: variant={} subword_gamma={} "
                     "router={} build_atire={} max_corpus_docs={} corpus_docs={}",
                     variantLabel(cfg_.variant), cfg_.subword_gamma,
                     cfg_.router_enabled ? "on" : "off", build_atire ? "yes" : "no",
                     cfg_.max_corpus_docs, ids.size());

        std::unordered_map<std::int64_t, std::uint32_t> mapping;
        mapping.reserve(ids.size());
        const bool considerFragmentGeometry =
            cfg_.fragment_geometry_enabled && ids.size() >= cfg_.fragment_geometry_min_corpus_docs;
        std::vector<std::int64_t> dense_doc_ids;
        dense_doc_ids.reserve(ids.size());
        std::vector<std::string> pmi_sample_texts;
        if (considerFragmentGeometry) {
            const auto reserveDocs =
                cfg_.fragment_geometry_pmi_sample_docs == 0
                    ? ids.size()
                    : std::min<std::size_t>(ids.size(), cfg_.fragment_geometry_pmi_sample_docs);
            pmi_sample_texts.reserve(reserveDocs);
        }

        std::uint32_t dense = 0;
        std::size_t missing = 0;
        std::size_t rawCorpusBytes = 0;
        std::size_t processedCorpusBytes = 0;
        std::size_t pmiSampleBytes = 0;
        std::size_t chunkedDocs = 0;
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
            const auto rawDocBytes = content.contentText.size();
            auto buildText = buildEnhancementText(content.contentText, cfg_);
            const auto buildDocBytes = buildText.view.size();
            if (buildText.chunked) {
                ++chunkedDocs;
            }
            if (exceedsBudget(processedCorpusBytes, buildDocBytes, cfg_.max_corpus_bytes)) {
                spdlog::warn("[simeon-lexical] corpus text budget exceeded (docs={} raw_bytes={} "
                             "processed_bytes={} cap={}) "
                             "- staying on FTS5 only",
                             dense, rawCorpusBytes + rawDocBytes,
                             processedCorpusBytes + buildDocBytes, cfg_.max_corpus_bytes);
                building_.store(false, std::memory_order_release);
                return;
            }
            rawCorpusBytes += rawDocBytes;
            processedCorpusBytes += buildDocBytes;
            primary->add_doc(buildText.view);
            if (atire) {
                atire->add_doc(buildText.view);
            }
            if (considerFragmentGeometry) {
                const bool sampleDocsOk =
                    cfg_.fragment_geometry_pmi_sample_docs == 0 ||
                    pmi_sample_texts.size() < cfg_.fragment_geometry_pmi_sample_docs;
                const bool sampleBytesOk = !exceedsBudget(pmiSampleBytes, buildDocBytes,
                                                          cfg_.fragment_geometry_pmi_sample_bytes);
                if (sampleDocsOk && sampleBytesOk) {
                    pmi_sample_texts.emplace_back(buildText.view);
                    pmiSampleBytes += buildDocBytes;
                }
            }
            mapping.emplace(docId, dense++);
            dense_doc_ids.push_back(docId);
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
        releaseTransientPages("post-bm25-finalize");

        // Router uses the Atire index when available (matches simeon bench
        // convention — df/idf values are BM25-variant-independent, so the
        // choice is cosmetic). Build it whenever the lexical BM25 router or
        // the fragment-quality router may need query features.
        std::unique_ptr<simeon::QueryRouter> router;
        if (cfg_.router_enabled || cfg_.fragment_geometry_enabled) {
            const simeon::Bm25Index& rindex = atire ? *atire : *primary;
            router =
                std::make_unique<simeon::QueryRouter>(rindex, toRouterConfig(cfg_.router_preset));
        }

        std::unique_ptr<simeon::PmiEmbeddings> pmi;
        std::unique_ptr<simeon::Encoder> fragmentEncoder;
        std::vector<std::vector<simeon::SemanticFragment>> docFrags;
        const std::size_t uniqueWordCount =
            considerFragmentGeometry ? estimateUniqueWordCount(pmi_sample_texts) : 0;
        if (considerFragmentGeometry && uniqueWordCount >= 64 && !pmi_sample_texts.empty()) {
            try {
                std::vector<std::string_view> seedViews;
                seedViews.reserve(pmi_sample_texts.size());
                for (const auto& doc : pmi_sample_texts) {
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

                {
                    std::vector<std::string>().swap(pmi_sample_texts);
                    std::vector<std::string_view>().swap(seedViews);
                }
                releaseTransientPages("post-pmi-learn");

                docFrags.resize(dense);
                const auto fragmentDocCap =
                    cfg_.fragment_geometry_max_docs == 0
                        ? dense_doc_ids.size()
                        : std::min<std::size_t>(dense_doc_ids.size(),
                                                cfg_.fragment_geometry_max_docs);
                std::size_t fragmentBytes = 0;
                std::size_t fragmentDocsBuilt = 0;
                std::size_t fragmentChunkedDocs = 0;
                for (std::size_t i = 0; i < dense_doc_ids.size() && i < fragmentDocCap; ++i) {
                    if ((i & 0x3ffu) == 0u && stop.stop_requested()) {
                        building_.store(false, std::memory_order_release);
                        return;
                    }
                    auto docResult = repo->getContent(dense_doc_ids[i]);
                    if (!docResult || !docResult.value()) {
                        continue;
                    }
                    const auto& doc = docResult.value().value().contentText;
                    auto buildText = buildEnhancementText(doc, cfg_);
                    const auto docBytes = buildText.view.size();
                    if (buildText.chunked) {
                        ++fragmentChunkedDocs;
                    }
                    if (exceedsBudget(fragmentBytes, docBytes,
                                      cfg_.fragment_geometry_max_corpus_bytes)) {
                        break;
                    }
                    fragmentBytes += docBytes;
                    docFrags[i] = simeon::build_doc_semantic_fragments_rich_covered(
                        *fragmentEncoder, buildText.view, *primary,
                        cfg_.fragment_build_top_sentences, cfg_.fragment_build_signature_terms,
                        0.60f, 0.80f);
                    std::span<std::vector<simeon::SemanticFragment>> oneDoc(&docFrags[i], 1);
                    simeon::compress_fragments_to_bf16(oneDoc, fragmentEncoder->output_dim());
                    ++fragmentDocsBuilt;
                }
                spdlog::info(
                    "[simeon-lexical] fragment geometry built: docs={} bytes={} chunked_docs={} "
                    "sample_docs={} sample_bytes={} unique_words={}",
                    fragmentDocsBuilt, fragmentBytes, fragmentChunkedDocs, pmi_sample_texts.size(),
                    pmiSampleBytes, uniqueWordCount);
                if (fragmentDocsBuilt == 0) {
                    pmi.reset();
                    fragmentEncoder.reset();
                    docFrags.clear();
                }
            } catch (const std::exception& e) {
                spdlog::warn("[simeon-lexical] fragment geometry disabled: {}", e.what());
                pmi.reset();
                fragmentEncoder.reset();
                docFrags.clear();
            }
        } else if (considerFragmentGeometry && !pmi_sample_texts.empty()) {
            spdlog::info("[simeon-lexical] fragment geometry skipped: sample {} docs / {} unique "
                         "words below thresholds (min_docs={}, min_unique_words=64)",
                         pmi_sample_texts.size(), uniqueWordCount,
                         cfg_.fragment_geometry_min_corpus_docs);
        }
        if (!pmi_sample_texts.empty()) {
            std::vector<std::string>().swap(pmi_sample_texts);
        }
        releaseTransientPages("post-fragment-build");

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
                     "missing={} raw_bytes={} processed_bytes={} chunked_docs={} build_ms={}",
                     variantLabel(cfg_.variant), cfg_.router_enabled ? "on" : "off",
                     fragment_encoder_ ? "on" : "off", dense, missing, rawCorpusBytes,
                     processedCorpusBytes, chunkedDocs, buildMs);
        releaseTransientPages("post-build");
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

    const auto* lexicalRouter = router_.get();
    const bool fragmentGeometryReady =
        cfg_.fragment_geometry_enabled && fragment_encoder_ && !doc_frags_.empty();

    const auto score_bm25 = [&](const simeon::Bm25Index& chosen) {
        std::vector<float> scores(doc_count_, 0.0f);
        chosen.score(query, std::span<float>{scores});
        return scores;
    };

    const auto choose_bm25_recipe = [&](const simeon::QueryFeatures& features)
        -> std::pair<simeon::Recipe, const simeon::Bm25Index*> {
        if (!cfg_.router_enabled || lexicalRouter == nullptr) {
            return {simeon::Recipe::Bm25SabSmooth, index_.get()};
        }
        const auto recipe = lexicalRouter->choose(features);
        const simeon::Bm25Index* chosen = index_.get();
        if (recipe == simeon::Recipe::Bm25Atire) {
            chosen = atire_index_ ? atire_index_.get() : index_.get();
        }
        return {recipe, chosen};
    };

    std::vector<float> full;
    std::vector<float> lexical;
    const char* recipe_label = variantLabel(cfg_.variant);

    if (fragmentGeometryReady && lexicalRouter != nullptr) {
        const auto features = lexicalRouter->features(query);
        const auto qualityRecipe = lexicalRouter->choose_quality(features);
        if (qualityRecipe == simeon::QualityRecipe::Bm25Only) {
            const auto [recipe, chosen] = choose_bm25_recipe(features);
            recipe_label =
                cfg_.router_enabled ? simeon::recipe_name(recipe) : variantLabel(cfg_.variant);
            full = score_bm25(*chosen);
        } else {
            auto geomCfg = cfg_.fragment_geometry_config;
            geomCfg.use_phss = true;
            geomCfg.phss_config.criterion = simeon::PhssConfig::Criterion::LargestGapApprox;
            geomCfg.top_fragments_per_doc =
                std::max<std::uint32_t>(geomCfg.top_fragments_per_doc, 8u);
            if (qualityRecipe == simeon::QualityRecipe::FragmentRichCovPhssApproxMax) {
                geomCfg.doc_aggregator = simeon::FragmentGeometryConfig::DocAggregator::Max;
            }
            recipe_label = simeon::quality_recipe_name(qualityRecipe);
            full = simeon::score_fragment_geometry(query, *index_, *fragment_encoder_, doc_frags_,
                                                   geomCfg);
            lexical = score_bm25(*index_);
        }
    } else if (cfg_.router_enabled && lexicalRouter != nullptr) {
        const auto features = lexicalRouter->features(query);
        const auto [recipe, chosen] = choose_bm25_recipe(features);
        recipe_label = simeon::recipe_name(recipe);
        full = score_bm25(*chosen);
    } else {
        full = score_bm25(*index_);
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
