#include <yams/search/simeon_lexical_backend.h>

#include <yams/metadata/metadata_repository.h>

#include <simeon/bm25.hpp>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
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

} // namespace

SimeonLexicalBackend::SimeonLexicalBackend(Config cfg) : cfg_(cfg) {}
SimeonLexicalBackend::~SimeonLexicalBackend() = default;

Result<void> SimeonLexicalBackend::buildAsync(std::shared_ptr<metadata::MetadataRepository> repo) {
    if (!repo) {
        return Error{ErrorCode::InvalidArgument, "SimeonLexicalBackend: null metadata repo"};
    }
    bool expected = false;
    if (!building_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return Result<void>{};
    }

    std::thread([this, repo = std::move(repo)]() mutable {
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

        simeon::Bm25Config bcfg;
        bcfg.variant = toSimeonVariant(cfg_.variant);
        bcfg.subword_gamma = cfg_.subword_gamma;
        auto index = std::make_unique<simeon::Bm25Index>(bcfg);

        std::unordered_map<std::int64_t, std::uint32_t> mapping;
        mapping.reserve(ids.size());

        std::uint32_t dense = 0;
        std::size_t missing = 0;
        for (auto docId : ids) {
            auto contentResult = repo->getContent(docId);
            if (!contentResult || !contentResult.value()) {
                ++missing;
                continue;
            }
            const auto& content = contentResult.value().value();
            index->add_doc(content.contentText);
            mapping.emplace(docId, dense++);
        }
        index->finalize();

        const auto t1 = std::chrono::steady_clock::now();
        const auto buildMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        index_ = std::move(index);
        doc_id_to_index_ = std::move(mapping);
        doc_count_ = dense;
        ready_.store(true, std::memory_order_release);
        building_.store(false, std::memory_order_release);

        spdlog::info("[simeon-lexical] ready: variant={} docs={} missing={} build_ms={}",
                     variantLabel(cfg_.variant), dense, missing, buildMs);
    }).detach();

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

} // namespace yams::search
