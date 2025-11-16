#include <yams/search/search_engine_builder.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/hybrid_search_factory.h>
#include <yams/search/kg_scorer.h>
#include <yams/vector/vector_index_manager.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <map>
#include <regex>
#include <sstream>
#include <unordered_set>

namespace yams::search {

// Forward declaration for the simple scorer factory implemented in the KG scorer TU.
std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store,
                   std::shared_ptr<yams::app::services::IGraphQueryService> graphService = nullptr);

// ------------------------------
// MetadataKeywordAdapter
// ------------------------------

MetadataKeywordAdapter::MetadataKeywordAdapter(
    std::shared_ptr<yams::metadata::MetadataRepository> repo)
    : repo_(std::move(repo)) {}

Result<std::vector<KeywordSearchResult>>
MetadataKeywordAdapter::search(const std::string& query, size_t k,
                               const yams::vector::SearchFilter* filter) {
    if (!repo_) {
        return Error{ErrorCode::InvalidState, "MetadataKeywordAdapter: repository not set"};
    }

    // Delegate to MetadataRepository's FTS-backed search
    auto res = repo_->search(query, static_cast<int>(k), 0);
    if (!res) {
        return Error{res.error().code, res.error().message};
    }

    const auto& r = res.value();

    // Build a quick lookup for exclude ids if provided
    std::unordered_set<std::string> exclude;
    if (filter) {
        for (const auto& id : filter->exclude_ids) {
            exclude.insert(id);
        }
    }

    // Optional filter for document hashes
    std::unordered_set<std::string> include_hashes;
    if (filter && !filter->document_hashes.empty()) {
        include_hashes.insert(filter->document_hashes.begin(), filter->document_hashes.end());
    }

    std::vector<KeywordSearchResult> out;
    out.reserve(r.results.size());

    for (const auto& item : r.results) {
        // Map document id to adapter id string
        std::string id = std::to_string(item.document.id);

        // Apply basic filters
        if (!exclude.empty() && exclude.count(id) > 0) {
            continue;
        }
        if (!include_hashes.empty() && include_hashes.count(item.document.sha256Hash) == 0) {
            continue;
        }
        if (filter && filter->min_similarity.has_value()) {
            // Metadata repo score is an arbitrary relevance score; apply threshold directly.
            if (static_cast<float>(item.score) < filter->min_similarity.value()) {
                continue;
            }
        }
        if (filter && filter->custom_filter) {
            // Provide minimal metadata map into custom filter
            std::map<std::string, std::string> meta;
            meta["path"] = item.document.filePath;
            meta["title"] = item.document.fileName;
            meta["hash"] = item.document.sha256Hash;
            if (!filter->custom_filter(id, meta)) {
                continue;
            }
        }

        KeywordSearchResult kr;
        kr.id = std::move(id);
        kr.content = item.snippet; // Use snippet as the 'content' payload for ranking context
        kr.score = static_cast<float>(item.score);

        // Populate lightweight metadata for downstream fusion/explanations
        kr.metadata["title"] = item.document.fileName;
        kr.metadata["name"] = item.document.fileName;
        kr.metadata["path"] = item.document.filePath;
        kr.metadata["hash"] = item.document.sha256Hash;
        kr.metadata["mime_type"] = item.document.mimeType;
        if (!item.document.fileExtension.empty()) {
            std::string ext = item.document.fileExtension;
            std::transform(ext.begin(), ext.end(), ext.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (!ext.empty() && ext.front() == '.')
                ext.erase(ext.begin());
            if (!ext.empty())
                kr.metadata["extension"] = std::move(ext);
        }

        out.push_back(std::move(kr));
        if (out.size() >= k) {
            break;
        }
    }

    return out;
}

Result<std::vector<std::vector<KeywordSearchResult>>>
MetadataKeywordAdapter::batchSearch(const std::vector<std::string>& queries, size_t k,
                                    const yams::vector::SearchFilter* filter) {
    std::vector<std::vector<KeywordSearchResult>> results;
    results.reserve(queries.size());
    for (const auto& q : queries) {
        auto r = search(q, k, filter);
        if (!r) {
            return Error{r.error().code, r.error().message};
        }
        results.push_back(std::move(r.value()));
    }
    return results;
}

// Index management is delegated to the DB FTS index; treat as no-ops.
Result<void>
MetadataKeywordAdapter::addDocument(const std::string& /*id*/, const std::string& /*content*/,
                                    const std::map<std::string, std::string>& /*metadata*/) {
    return Result<void>(); // no-op
}

Result<void> MetadataKeywordAdapter::removeDocument(const std::string& /*id*/) {
    return Result<void>(); // no-op
}

Result<void>
MetadataKeywordAdapter::updateDocument(const std::string& /*id*/, const std::string& /*content*/,
                                       const std::map<std::string, std::string>& /*metadata*/) {
    return Result<void>(); // no-op
}

Result<void> MetadataKeywordAdapter::addDocuments(
    const std::vector<std::string>& /*ids*/, const std::vector<std::string>& /*contents*/,
    const std::vector<std::map<std::string, std::string>>& /*metadata*/) {
    return Result<void>(); // no-op
}

Result<void> MetadataKeywordAdapter::buildIndex() {
    return Result<void>();
} // no-op
Result<void> MetadataKeywordAdapter::optimizeIndex() {
    return Result<void>();
} // no-op
Result<void> MetadataKeywordAdapter::clearIndex() {
    return Result<void>();
} // no-op

Result<void> MetadataKeywordAdapter::saveIndex(const std::string& /*path*/) {
    return Error{ErrorCode::InvalidOperation, "MetadataKeywordAdapter: saveIndex not supported"};
}

Result<void> MetadataKeywordAdapter::loadIndex(const std::string& /*path*/) {
    return Error{ErrorCode::InvalidOperation, "MetadataKeywordAdapter: loadIndex not supported"};
}

size_t MetadataKeywordAdapter::getDocumentCount() const {
    if (!repo_)
        return 0;
    auto rc = repo_->getDocumentCount();
    if (!rc)
        return 0;
    return static_cast<size_t>(std::max<int64_t>(0, rc.value()));
}

size_t MetadataKeywordAdapter::getTermCount() const {
    return 0;
} // Not exposed by repo
size_t MetadataKeywordAdapter::getIndexSize() const {
    return 0;
} // Not exposed by repo

std::vector<std::string> MetadataKeywordAdapter::analyzeQuery(const std::string& query) const {
    // Very simple tokenization to mirror other keyword engines
    std::vector<std::string> tokens;
    std::regex word_regex(R"(\b[a-zA-Z0-9]+\b)");
    auto words_begin = std::sregex_iterator(query.begin(), query.end(), word_regex);
    auto words_end = std::sregex_iterator();
    for (auto it = words_begin; it != words_end; ++it) {
        std::string t = it->str();
        std::transform(t.begin(), t.end(), t.begin(), ::tolower);
        tokens.push_back(std::move(t));
    }
    return tokens;
}

std::vector<std::string> MetadataKeywordAdapter::extractKeywords(const std::string& text) const {
    auto tokens = analyzeQuery(text);
    std::unordered_set<std::string> uniq(tokens.begin(), tokens.end());
    return std::vector<std::string>(uniq.begin(), uniq.end());
}

// ------------------------------
// SearchEngineBuilder
// ------------------------------

Result<std::shared_ptr<KeywordSearchEngine>> SearchEngineBuilder::makeKeywordEngine() {
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: MetadataRepository not provided"};
    }
    std::shared_ptr<KeywordSearchEngine> engine =
        std::make_shared<MetadataKeywordAdapter>(metadataRepo_);
    return engine;
}

Result<std::shared_ptr<KGScorer>>
SearchEngineBuilder::makeKGScorerIfEnabled(const HybridSearchConfig& cfg) {
    if (!cfg.enable_kg) {
        return std::shared_ptr<KGScorer>{}; // KG disabled => null scorer is fine
    }
    if (!kgStore_) {
        // KG requested but store not available; return null and let caller decide how to proceed
        spdlog::warn("KG enabled in config, but no KnowledgeGraphStore provided; proceeding "
                     "without KG scorer");
        return std::shared_ptr<KGScorer>{};
    }
    auto scorer = makeSimpleKGScorer(kgStore_, graphQueryService_);
    return scorer;
}

Result<std::shared_ptr<HybridSearchEngine>>
SearchEngineBuilder::buildEmbedded(const BuildOptions& options) {
    spdlog::info("Building embedded HybridSearchEngine");
    if (!vectorIndex_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: VectorIndexManager not provided"};
    }
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: MetadataRepository not provided"};
    }

    // Prepare hybrid config
    HybridSearchConfig cfg = options.hybrid;
    if (!cfg.isValid()) {
        spdlog::warn("Invalid HybridSearchConfig provided; falling back to defaults");
        cfg = BuildOptions::makeDefault().hybrid;
    }
    cfg.normalizeWeights();

    // Create keyword engine (adapter to MetadataRepository FTS)
    auto kwRes = makeKeywordEngine();
    if (!kwRes) {
        return Error{kwRes.error().code, kwRes.error().message};
    }
    auto keywordEngine = kwRes.value();

    // Pass embedding generator to factory (will create default if null)
    if (cfg.enable_kg && kgStore_) {
        // Create engine with KG store; factory will attach a default KG scorer
        auto engRes = HybridSearchFactory::createWithKGStore(
            vectorIndex_, keywordEngine, cfg, kgStore_, embeddingGenerator_, graphQueryService_);
        if (!engRes) {
            // Fail open: attempt without KG
            spdlog::warn("HybridSearchFactory::createWithKGStore failed: {}. Retrying without KG.",
                         engRes.error().message);
            HybridSearchConfig fallback = cfg;
            fallback.enable_kg = false;
            return HybridSearchFactory::create(vectorIndex_, keywordEngine, fallback, nullptr,
                                               embeddingGenerator_);
        }
        return engRes;
    } else {
        // Either KG disabled or no store; build without KG
        return HybridSearchFactory::create(vectorIndex_, keywordEngine, cfg, nullptr,
                                           embeddingGenerator_);
    }
}

} // namespace yams::search
