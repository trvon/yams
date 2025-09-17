#include <yams/search/hybrid_search_engine.h>
#include <yams/search/kg_scorer.h>
#include <yams/search/query_qualifiers.hpp>
#include <yams/vector/embedding_generator.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <condition_variable>
#include <future>
#include <mutex>
#include <numeric>
#include <queue>
#include <regex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::search {

// =============================================================================
// Simple Keyword Search Engine Implementation (BM25-based)
// =============================================================================

class SimpleKeywordSearchEngine : public KeywordSearchEngine {
public:
    SimpleKeywordSearchEngine() {
        // Initialize with default BM25 parameters
        k1_ = 1.2f;
        b_ = 0.75f;
    }

    Result<std::vector<KeywordSearchResult>> search(const std::string& query, size_t k,
                                                    const vector::SearchFilter* filter) override {
        auto terms = analyzeQuery(query);
        if (terms.empty()) {
            return Result<std::vector<KeywordSearchResult>>(std::vector<KeywordSearchResult>{});
        }

        // Calculate BM25 scores for all documents
        std::vector<std::pair<float, std::string>> scores;

        for (const auto& [doc_id, doc_content] : documents_) {
            // Apply filter if provided
            if (filter && filter->hasFilters()) {
                // Exclude by id
                if (!filter->exclude_ids.empty()) {
                    if (std::find(filter->exclude_ids.begin(), filter->exclude_ids.end(), doc_id) !=
                        filter->exclude_ids.end()) {
                        continue;
                    }
                }
                // Enforce metadata key/value matches (e.g., file_name, extension, mime_type)
                if (!filter->metadata_filters.empty()) {
                    auto itMeta = metadata_.find(doc_id);
                    const std::map<std::string, std::string>* docMeta =
                        (itMeta != metadata_.end()) ? &itMeta->second : nullptr;
                    bool metaOk = true;
                    for (const auto& kv : filter->metadata_filters) {
                        const auto& key = kv.first;
                        const auto& val = kv.second;
                        if (!docMeta) {
                            metaOk = false;
                            break;
                        }
                        auto it = docMeta->find(key);
                        if (it == docMeta->end() || it->second != val) {
                            metaOk = false;
                            break;
                        }
                    }
                    if (!metaOk) {
                        continue;
                    }
                }
            }

            float score = calculateBM25Score(terms, doc_content);
            if (score > 0) {
                scores.emplace_back(score, doc_id);
            }
        }

        // Sort by score and take top k
        std::sort(scores.begin(), scores.end(), std::greater<>());

        size_t result_count = std::min(k, scores.size());
        std::vector<KeywordSearchResult> results;
        results.reserve(result_count);

        for (size_t i = 0; i < result_count; ++i) {
            KeywordSearchResult result;
            result.id = scores[i].second;
            result.content = documents_[result.id];
            result.score = scores[i].first;
            result.matched_terms = findMatchedTerms(terms, result.content);

            auto meta_it = metadata_.find(result.id);
            if (meta_it != metadata_.end()) {
                result.metadata = meta_it->second;
            }

            results.push_back(std::move(result));
        }

        return Result<std::vector<KeywordSearchResult>>(results);
    }

    Result<std::vector<std::vector<KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>& queries, size_t k,
                const vector::SearchFilter* filter) override {
        std::vector<std::vector<KeywordSearchResult>> results;
        results.reserve(queries.size());

        for (const auto& query : queries) {
            auto result = search(query, k, filter);
            if (!result.has_value()) {
                return Result<std::vector<std::vector<KeywordSearchResult>>>(result.error());
            }
            results.push_back(result.value());
        }

        return Result<std::vector<std::vector<KeywordSearchResult>>>(results);
    }

    Result<void> addDocument(const std::string& id, const std::string& content,
                             const std::map<std::string, std::string>& metadata) override {
        documents_[id] = content;
        if (!metadata.empty()) {
            metadata_[id] = metadata;
        }

        // Update term frequencies
        auto terms = extractKeywords(content);
        for (const auto& term : terms) {
            document_frequencies_[term]++;
        }

        // Update average document length
        updateAverageDocLength();

        return Result<void>();
    }

    Result<void> removeDocument(const std::string& id) override {
        auto it = documents_.find(id);
        if (it == documents_.end()) {
            return Result<void>(Error{ErrorCode::NotFound, "Document not found"});
        }

        // Update term frequencies
        auto terms = extractKeywords(it->second);
        for (const auto& term : terms) {
            if (document_frequencies_[term] > 0) {
                document_frequencies_[term]--;
            }
        }

        documents_.erase(it);
        metadata_.erase(id);

        updateAverageDocLength();

        return Result<void>();
    }

    Result<void> updateDocument(const std::string& id, const std::string& content,
                                const std::map<std::string, std::string>& metadata) override {
        // Remove old document
        auto remove_result = removeDocument(id);
        if (!remove_result.has_value() && remove_result.error().code != ErrorCode::NotFound) {
            return remove_result;
        }

        // Add new document
        return addDocument(id, content, metadata);
    }

    Result<void>
    addDocuments(const std::vector<std::string>& ids, const std::vector<std::string>& contents,
                 const std::vector<std::map<std::string, std::string>>& metadata) override {
        if (ids.size() != contents.size()) {
            return Result<void>(
                Error{ErrorCode::InvalidArgument, "IDs and contents size mismatch"});
        }

        for (size_t i = 0; i < ids.size(); ++i) {
            std::map<std::string, std::string> meta;
            if (i < metadata.size()) {
                meta = metadata[i];
            }

            auto result = addDocument(ids[i], contents[i], meta);
            if (!result.has_value()) {
                return result;
            }
        }

        return Result<void>();
    }

    Result<void> buildIndex() override {
        // For simple implementation, index is built incrementally
        return Result<void>();
    }

    Result<void> optimizeIndex() override {
        // Clean up zero-frequency terms
        for (auto it = document_frequencies_.begin(); it != document_frequencies_.end();) {
            if (it->second == 0) {
                it = document_frequencies_.erase(it);
            } else {
                ++it;
            }
        }
        return Result<void>();
    }

    Result<void> clearIndex() override {
        documents_.clear();
        metadata_.clear();
        document_frequencies_.clear();
        avg_doc_length_ = 0;
        return Result<void>();
    }

    Result<void> saveIndex(const std::string& path) override {
        // TODO: Implement persistence
        return Result<void>(Error{ErrorCode::InvalidOperation, "Save not implemented"});
    }

    Result<void> loadIndex(const std::string& path) override {
        // TODO: Implement persistence
        return Result<void>(Error{ErrorCode::InvalidOperation, "Load not implemented"});
    }

    size_t getDocumentCount() const override { return documents_.size(); }

    size_t getTermCount() const override { return document_frequencies_.size(); }

    size_t getIndexSize() const override {
        size_t size = 0;
        for (const auto& [id, content] : documents_) {
            size += content.size();
        }
        return size;
    }

    std::vector<std::string> analyzeQuery(const std::string& query) const override {
        return tokenize(toLowerCase(query));
    }

    std::vector<std::string> extractKeywords(const std::string& text) const override {
        auto tokens = tokenize(toLowerCase(text));

        // Remove duplicates
        std::unordered_set<std::string> unique_tokens(tokens.begin(), tokens.end());
        return std::vector<std::string>(unique_tokens.begin(), unique_tokens.end());
    }

private:
    // BM25 parameters
    float k1_;
    float b_;

    // Document storage
    std::unordered_map<std::string, std::string> documents_;
    std::unordered_map<std::string, std::map<std::string, std::string>> metadata_;

    // Index structures
    std::unordered_map<std::string, size_t> document_frequencies_;
    float avg_doc_length_ = 0;

    // Helper methods
    std::string toLowerCase(const std::string& text) const {
        std::string lower = text;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        return lower;
    }

    std::vector<std::string> tokenize(const std::string& text) const {
        std::vector<std::string> tokens;
        std::regex word_regex(R"(\b[a-z0-9]+\b)");
        auto words_begin = std::sregex_iterator(text.begin(), text.end(), word_regex);
        auto words_end = std::sregex_iterator();

        for (auto it = words_begin; it != words_end; ++it) {
            tokens.push_back(it->str());
        }

        return tokens;
    }

    float calculateBM25Score(const std::vector<std::string>& query_terms,
                             const std::string& document) const {
        auto doc_terms = tokenize(toLowerCase(document));
        std::unordered_map<std::string, size_t> term_freq;

        for (const auto& term : doc_terms) {
            term_freq[term]++;
        }

        float score = 0.0f;
        float doc_length = static_cast<float>(doc_terms.size());
        size_t N = documents_.size();

        for (const auto& query_term : query_terms) {
            auto tf_it = term_freq.find(query_term);
            if (tf_it == term_freq.end()) {
                continue;
            }

            float tf = static_cast<float>(tf_it->second);

            auto df_it = document_frequencies_.find(query_term);
            float df =
                df_it != document_frequencies_.end() ? static_cast<float>(df_it->second) : 0.5f;

            float idf = std::log((N - df + 0.5f) / (df + 0.5f));

            float normalized_tf =
                (tf * (k1_ + 1)) / (tf + k1_ * (1 - b_ + b_ * doc_length / avg_doc_length_));

            score += idf * normalized_tf;
        }

        return score;
    }

    std::vector<std::string> findMatchedTerms(const std::vector<std::string>& query_terms,
                                              const std::string& document) const {
        auto doc_lower = toLowerCase(document);
        std::vector<std::string> matched;

        for (const auto& term : query_terms) {
            if (doc_lower.find(term) != std::string::npos) {
                matched.push_back(term);
            }
        }

        return matched;
    }

    void updateAverageDocLength() {
        if (documents_.empty()) {
            avg_doc_length_ = 0;
            return;
        }

        size_t total_length = 0;
        for (const auto& [id, content] : documents_) {
            total_length += tokenize(toLowerCase(content)).size();
        }

        avg_doc_length_ = static_cast<float>(total_length) / documents_.size();
    }
};

// =============================================================================
// Hybrid Search Engine Implementation
// =============================================================================

class HybridSearchEngine::Impl {
public:
    Impl(std::shared_ptr<vector::VectorIndexManager> vector_index,
         std::shared_ptr<KeywordSearchEngine> keyword_engine, const HybridSearchConfig& config,
         std::shared_ptr<vector::EmbeddingGenerator> embedding_generator = nullptr)
        : vector_index_(std::move(vector_index)), keyword_engine_(std::move(keyword_engine)),
          embedding_generator_(std::move(embedding_generator)), config_(config) {
        config_.normalizeWeights();
        initThreadPool();
    }

    Result<void> initialize() {
        if (initialized_) {
            return Result<void>();
        }

        if (!vector_index_ || !keyword_engine_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Invalid search engines"});
        }

        // Initialize vector index if needed; do not block engine if it fails.
        // We can still operate in keyword/KG-only mode.
        vector_index_ready_ = vector_index_->isInitialized();
        if (!vector_index_ready_) {
            auto result = vector_index_->initialize();
            if (!result.has_value()) {
                spdlog::warn(
                    "VectorIndex initialization failed; continuing with keyword-only mode: {}",
                    result.error().message);
                vector_index_ready_ = false;
            } else {
                vector_index_ready_ = true;
            }
        }

        // Initialize embedding generator if provided
        if (embedding_generator_ && !embedding_generator_->isInitialized()) {
            if (!embedding_generator_->initialize()) {
                // Log warning but don't fail - fallback to zero vectors
                spdlog::warn("Failed to initialize embedding generator, vector search will use "
                             "placeholders");
            }
        }

        initialized_ = true;
        return Result<void>();
    }

    std::vector<float> generateQueryEmbedding(const std::string& query) {
        // Generate embedding with a tight timeout to avoid stalling queries.
        if (embedding_generator_ && embedding_generator_->isInitialized()) {
            try {
                auto fut = embedding_generator_->generateEmbeddingAsync(query);
                if (fut.wait_for(config_.embed_timeout_ms) == std::future_status::ready) {
                    auto embedding = fut.get();
                    if (!embedding.empty())
                        return embedding;
                } else {
                    spdlog::debug("Query embedding timed out after {} ms; skipping vector path",
                                  config_.embed_timeout_ms.count());
                }
            } catch (const std::exception& e) {
                spdlog::debug("Failed to generate query embedding: {}", e.what());
            }
        }
        return {};
    }

    bool isInitialized() const { return initialized_; }

    void shutdown() {
        if (vector_index_) {
            vector_index_->shutdown();
        }
        cache_.clear();
        initialized_ = false;
    }

    Result<std::vector<HybridSearchResult>> search(const std::string& query, size_t k,
                                                   const vector::SearchFilter& filter) {
        if (!initialized_) {
            return Result<std::vector<HybridSearchResult>>(
                Error{ErrorCode::InvalidState, "Engine not initialized"});
        }

        auto start_time = std::chrono::high_resolution_clock::now();

        // Parse inline qualifiers and normalize query text
        auto parsed = yams::search::parseQueryQualifiers(query);
        const std::string& normalizedQuery = parsed.normalizedQuery;

        // Lightweight query analysis for heuristics
        auto tokens = keyword_engine_->analyzeQuery(normalizedQuery);
        const bool is_single_term = (tokens.size() == 1);
        auto lower = normalizedQuery;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        const bool looks_structured =
            (lower.find('"') != std::string::npos) || (lower.find(" and ") != std::string::npos) ||
            (lower.find(" or ") != std::string::npos) ||
            (lower.find(" not ") != std::string::npos) || (lower.find('(') != std::string::npos) ||
            (lower.find(')') != std::string::npos);

        // Check cache
        if (config_.enable_cache) {
            auto cache_key = generateCacheKey(normalizedQuery, k, filter);
            auto cached = getCachedResult(cache_key);
            if (cached.has_value()) {
                metrics_.cache_hits++;
                return Result<std::vector<HybridSearchResult>>(cached.value());
            }
            metrics_.cache_misses++;
        }

        // Expand query if enabled (use normalized query). Skip expansion when vector is disabled
        // or query appears structured to preserve semantics and reduce latency.
        std::string expanded_query = normalizedQuery;
        // Whether vector search is possible (embedding generator ready)
        const bool embeddings_ready =
            (embedding_generator_ && embedding_generator_->isInitialized());
        if (config_.enable_query_expansion && embeddings_ready && !looks_structured) {
            auto expanded_terms = expandQuery(normalizedQuery);
            if (!expanded_terms.empty()) {
                expanded_query += " " + joinStrings(expanded_terms, " ");
            }
        }

        // Perform parallel or sequential search (with runtime gates via env)
        const bool env_disable_keyword = (std::getenv("YAMS_DISABLE_KEYWORD") != nullptr);
        const bool env_disable_vector = (std::getenv("YAMS_DISABLE_VECTOR") != nullptr);
        const bool env_disable_onnx = (std::getenv("YAMS_DISABLE_ONNX") != nullptr);
        const bool env_disable_kg = (std::getenv("YAMS_DISABLE_KG") != nullptr);

        // Effective config reflects runtime gates
        HybridSearchConfig effective = config_;
        if (env_disable_keyword) {
            effective.keyword_weight = 0.0f;
        }
        if (env_disable_vector) {
            effective.vector_weight = 0.0f;
        }
        if (env_disable_kg) {
            effective.kg_entity_weight = 0.0f;
            effective.structural_weight = 0.0f;
            effective.enable_kg = false;
        }
        // Heuristics: prefer speed and exactness for single-term and structured expressions
        if (is_single_term || looks_structured) {
            effective.enable_reranking = false;
            // Keep KG disabled for these simple/structured cases to reduce overhead
            effective.enable_kg = false;
            effective.kg_entity_weight = 0.0f;
            effective.structural_weight = 0.0f;
        }
        effective.normalizeWeights();

        const size_t request_limit = std::max<size_t>(1, k);
        const size_t baseline_final = std::max<size_t>(effective.final_top_k, request_limit);
        const size_t desired_final = std::max(request_limit, baseline_final);
        auto computeTopK = [&](size_t base) {
            size_t scaled = std::max(base, desired_final * 3);
            const size_t cap = desired_final + 500;
            if (scaled > cap)
                scaled = cap;
            return std::max(scaled, desired_final);
        };
        const size_t vector_limit = computeTopK(effective.vector_top_k);
        const size_t keyword_limit = computeTopK(effective.keyword_top_k);

        std::future<Result<std::vector<vector::SearchResult>>> vector_future;
        std::future<Result<std::vector<KeywordSearchResult>>> keyword_future;

        // Determine if vector path should be attempted at all
        const bool vector_enabled =
            !env_disable_vector && !env_disable_onnx && embeddings_ready && vector_index_ready_;

        if (config_.parallel_search) {
            // Launch searches in parallel honoring gates
            if (vector_enabled) {
                auto nq = std::string(normalizedQuery);
                auto flt = filter;         // copy
                auto limit = vector_limit; // copy for lambda capture
                vector_future = pool_.submit([this, nq, flt, limit]() {
                    auto query_vector = generateQueryEmbedding(nq);
                    if (query_vector.empty()) {
                        return Result<std::vector<vector::SearchResult>>(
                            std::vector<vector::SearchResult>{});
                    }
                    return vector_index_->search(query_vector, limit, flt);
                });
            }
            if (!env_disable_keyword) {
                auto eq = std::string(expanded_query);
                auto flt = filter;          // copy
                auto limit = keyword_limit; // copy for lambda capture
                keyword_future = pool_.submit(
                    [this, eq, flt, limit]() { return keyword_engine_->search(eq, limit, &flt); });
            }
        }

        // Get results
        std::vector<vector::SearchResult> vector_results;
        std::vector<KeywordSearchResult> keyword_results;

        if (config_.parallel_search) {
            if (vector_future.valid()) {
                auto vector_result = vector_future.get();
                if (vector_result.has_value()) {
                    vector_results = vector_result.value();
                }
            }
            if (keyword_future.valid()) {
                auto keyword_result = keyword_future.get();
                if (keyword_result.has_value()) {
                    keyword_results = keyword_result.value();
                }
            }
        } else {
            // Sequential search honoring gates
            if (vector_enabled) {
                // Generate embedding for normalized query
                auto query_vector = generateQueryEmbedding(normalizedQuery);
                if (!query_vector.empty()) {
                    auto vector_result = vector_index_->search(query_vector, vector_limit, filter);
                    if (vector_result.has_value()) {
                        vector_results = vector_result.value();
                    }
                }
            }
            if (!env_disable_keyword) {
                auto keyword_result =
                    keyword_engine_->search(expanded_query, keyword_limit, &filter);
                if (keyword_result.has_value()) {
                    keyword_results = keyword_result.value();
                }
            }
        }

        // Optional KG scoring (best-effort within budget)
        if (effective.enable_kg && kg_scorer_) {
            KGScoringConfig kgcfg;
            kgcfg.max_neighbors = config_.kg_max_neighbors;
            kgcfg.max_hops = config_.kg_max_hops;
            kgcfg.budget = config_.kg_budget_ms;
            kg_scorer_->setConfig(kgcfg);

            // Collect candidate ids from both engines and de-duplicate
            std::vector<std::string> cids;
            cids.reserve(vector_results.size() + keyword_results.size());
            for (const auto& vr : vector_results) {
                cids.push_back(vr.id);
            }
            for (const auto& kr : keyword_results) {
                cids.push_back(kr.id);
            }
            std::sort(cids.begin(), cids.end());
            cids.erase(std::unique(cids.begin(), cids.end()), cids.end());

            auto kg_res = kg_scorer_->score(query, cids);
            last_kg_scores_.clear();
            if (kg_res.has_value()) {
                last_kg_scores_ = std::move(kg_res.value());
            }
        }

        // Record search times
        auto search_end = std::chrono::high_resolution_clock::now();
        auto search_duration =
            std::chrono::duration_cast<std::chrono::microseconds>(search_end - start_time);

        // If vector is disabled/unavailable, zero-out its weight to avoid rank dilution
        if (!vector_enabled) {
            effective.vector_weight = 0.0f;
            effective.normalizeWeights();
        }

        // Fuse results
        auto fusion_start = std::chrono::high_resolution_clock::now();
        auto fused_results = fuseResults(vector_results, keyword_results, effective);
        auto fusion_end = std::chrono::high_resolution_clock::now();
        auto fusion_duration =
            std::chrono::duration_cast<std::chrono::microseconds>(fusion_end - fusion_start);

        // Re-rank if enabled (skip when vector is disabled to reduce latency)
        if (config_.enable_reranking && !fused_results.empty() && vector_enabled) {
            auto rerank_start = std::chrono::high_resolution_clock::now();
            fused_results = rerankResults(fused_results, normalizedQuery);
            auto rerank_end = std::chrono::high_resolution_clock::now();
            auto rerank_duration =
                std::chrono::duration_cast<std::chrono::microseconds>(rerank_end - rerank_start);
            metrics_.rerank_time_ms = rerank_duration.count() / 1000.0;
        }

        // Scoped snippet shaping for lines:<range>
        if (parsed.scope.type == yams::search::ExtractScopeType::Lines &&
            !parsed.scope.range.empty()) {
            auto parseRanges = [](const std::string& expr) {
                std::vector<std::pair<int, int>> out;
                std::stringstream ss(expr);
                std::string token;
                while (std::getline(ss, token, ',')) {
                    if (token.empty())
                        continue;
                    // trim
                    token.erase(token.begin(),
                                std::find_if(token.begin(), token.end(),
                                             [](unsigned char c) { return !std::isspace(c); }));
                    token.erase(std::find_if(token.rbegin(), token.rend(),
                                             [](unsigned char c) { return !std::isspace(c); })
                                    .base(),
                                token.end());
                    auto pos = token.find('-');
                    if (pos == std::string::npos) {
                        try {
                            int v = std::stoi(token);
                            if (v > 0)
                                out.emplace_back(v, v);
                        } catch (...) {
                            // ignore malformed token
                        }
                    } else {
                        try {
                            int a = std::stoi(token.substr(0, pos));
                            int b = std::stoi(token.substr(pos + 1));
                            if (a > 0 && b >= a)
                                out.emplace_back(a, b);
                        } catch (...) {
                            // ignore malformed range
                        }
                    }
                }
                std::sort(out.begin(), out.end());
                std::vector<std::pair<int, int>> merged;
                for (const auto& r : out) {
                    if (merged.empty() || r.first > merged.back().second + 1) {
                        merged.push_back(r);
                    } else {
                        merged.back().second = std::max(merged.back().second, r.second);
                    }
                }
                return merged;
            };

            const auto ranges = parseRanges(parsed.scope.range);
            if (!ranges.empty()) {
                for (auto& r : fused_results) {
                    if (r.content.empty())
                        continue;

                    // Split into lines
                    std::vector<std::string> lines;
                    lines.reserve(256);
                    {
                        std::stringstream ss(r.content);
                        std::string line;
                        while (std::getline(ss, line)) {
                            if (!line.empty() && line.back() == '\r')
                                line.pop_back();
                            lines.push_back(std::move(line));
                        }
                    }
                    if (lines.empty())
                        continue;

                    // Rebuild content keeping only requested line ranges (1-based indices)
                    std::string scoped;
                    bool firstOut = true;
                    for (const auto& [a, b] : ranges) {
                        const int start = std::max(1, a);
                        const int end = std::min(static_cast<int>(lines.size()), b);
                        for (int idx = start; idx <= end; ++idx) {
                            if (!firstOut)
                                scoped.push_back('\n');
                            firstOut = false;
                            scoped += lines[static_cast<size_t>(idx - 1)];
                        }
                    }
                    if (!scoped.empty()) {
                        r.content = std::move(scoped);
                    }
                }
            }
        }

        // Take top k results
        if (fused_results.size() > k) {
            fused_results.resize(k);
        }

        // Update final ranks
        for (size_t i = 0; i < fused_results.size(); ++i) {
            fused_results[i].final_rank = i;
        }

        // Generate explanations if enabled
        if (config_.generate_explanations) {
            generateExplanations(fused_results, normalizedQuery);
        }

        // Update metrics
        updateMetrics(search_duration.count() / 1000.0, fusion_duration.count() / 1000.0,
                      fused_results);

        // Cache result if enabled
        if (config_.enable_cache) {
            auto cache_key = generateCacheKey(normalizedQuery, k, filter);
            cacheResult(cache_key, fused_results);
        }

        return Result<std::vector<HybridSearchResult>>(fused_results);
    }

    std::vector<HybridSearchResult>
    fuseResults(const std::vector<vector::SearchResult>& vector_results,
                const std::vector<KeywordSearchResult>& keyword_results,
                const HybridSearchConfig& config) {
        // Fast path: if vector is effectively disabled and no vector results, compute
        // keyword/KG-only fusion without extra allocation work.
        if ((config.vector_weight <= 1e-6f || vector_results.empty()) && !keyword_results.empty()) {
            std::vector<HybridSearchResult> out;
            out.reserve(keyword_results.size());
            for (size_t i = 0; i < keyword_results.size(); ++i) {
                const auto& kr = keyword_results[i];
                HybridSearchResult r;
                r.id = kr.id;
                r.content = kr.content;
                r.keyword_score = normalizeKeywordScore(kr.score, keyword_results);
                r.keyword_rank = i;
                r.found_by_keyword = true;
                r.matched_keywords = kr.matched_terms;
                // Attach KG signals if available
                if (config.enable_kg) {
                    auto it = last_kg_scores_.find(r.id);
                    if (it != last_kg_scores_.end()) {
                        r.kg_entity_score = std::clamp(it->second.entity, 0.0f, 1.0f);
                        r.structural_score = std::clamp(it->second.structural, 0.0f, 1.0f);
                    }
                }
                // Merge metadata
                r.metadata = kr.metadata;
                // Compute hybrid score without vector contribution
                float base =
                    fusion::linearCombination(0.0f, r.keyword_score, 0.0f, config.keyword_weight);
                if (config.enable_kg) {
                    base += r.kg_entity_score * config.kg_entity_weight;
                    base += r.structural_score * config.structural_weight;
                }
                r.hybrid_score = base;
                if (config.normalize_scores) {
                    if (r.hybrid_score < 0.0f)
                        r.hybrid_score = 0.0f;
                    else if (r.hybrid_score > 1.0f)
                        r.hybrid_score = 1.0f;
                }
                out.push_back(std::move(r));
            }
            std::sort(out.begin(), out.end());
            return out;
        }

        std::unordered_map<std::string, HybridSearchResult> result_map;
        result_map.reserve(vector_results.size() + keyword_results.size());

        std::unordered_map<std::string, float> normalized_vector_scores;
        if (!vector_results.empty()) {
            std::vector<float> raw_scores;
            raw_scores.reserve(vector_results.size());
            for (const auto& vr : vector_results) {
                raw_scores.push_back(vr.similarity);
            }
            auto normalized = fusion::normalizeScores(raw_scores);
            normalized_vector_scores.reserve(vector_results.size());
            for (size_t i = 0; i < vector_results.size(); ++i) {
                normalized_vector_scores.emplace(vector_results[i].id, normalized[i]);
            }
        }

        // Process vector results
        for (size_t i = 0; i < vector_results.size(); ++i) {
            const auto& vr = vector_results[i];
            auto& result = result_map[vr.id];
            result.id = vr.id;
            if (!normalized_vector_scores.empty()) {
                auto itNorm = normalized_vector_scores.find(vr.id);
                result.vector_score =
                    itNorm != normalized_vector_scores.end() ? itNorm->second : vr.similarity;
            } else {
                result.vector_score = vr.similarity;
            }
            result.vector_rank = i;
            result.found_by_vector = true;
            result.metadata = vr.metadata;
        }

        // Process keyword results
        for (size_t i = 0; i < keyword_results.size(); ++i) {
            const auto& kr = keyword_results[i];
            auto& result = result_map[kr.id];
            result.id = kr.id;
            result.content = kr.content;
            result.keyword_score = normalizeKeywordScore(kr.score, keyword_results);
            result.keyword_rank = i;
            result.found_by_keyword = true;
            result.matched_keywords = kr.matched_terms;

            // Merge metadata
            for (const auto& [key, value] : kr.metadata) {
                result.metadata[key] = value;
            }
        }

        // Calculate hybrid scores based on fusion strategy
        std::vector<HybridSearchResult> fused_results;
        fused_results.reserve(result_map.size());

        for (auto& [id, result] : result_map) {
            // Attach KG scores if available
            if (config.enable_kg) {
                auto it = last_kg_scores_.find(id);
                if (it != last_kg_scores_.end()) {
                    result.kg_entity_score = std::clamp(it->second.entity, 0.0f, 1.0f);
                    result.structural_score = std::clamp(it->second.structural, 0.0f, 1.0f);
                }
            }

            switch (config.fusion_strategy) {
                case HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION: {
                    float base =
                        fusion::linearCombination(result.vector_score, result.keyword_score,
                                                  config.vector_weight, config.keyword_weight);
                    if (config.enable_kg) {
                        base += result.kg_entity_score * config.kg_entity_weight;
                        base += result.structural_score * config.structural_weight;
                    }
                    result.hybrid_score = base;
                    if (config.normalize_scores) {
                        if (result.hybrid_score < 0.0f)
                            result.hybrid_score = 0.0f;
                        else if (result.hybrid_score > 1.0f)
                            result.hybrid_score = 1.0f;
                    }
                    break;
                }

                case HybridSearchConfig::FusionStrategy::RECIPROCAL_RANK:
                    result.hybrid_score = fusion::reciprocalRankFusion(
                        result.vector_rank, result.keyword_rank, config.rrf_k);
                    if (config.normalize_scores) {
                        if (result.hybrid_score < 0.0f)
                            result.hybrid_score = 0.0f;
                        else if (result.hybrid_score > 1.0f)
                            result.hybrid_score = 1.0f;
                    }
                    break;

                default: {
                    // Default to linear combination with optional KG weights
                    float base =
                        fusion::linearCombination(result.vector_score, result.keyword_score,
                                                  config.vector_weight, config.keyword_weight);
                    if (config.enable_kg) {
                        base += result.kg_entity_score * config.kg_entity_weight;
                        base += result.structural_score * config.structural_weight;
                    }
                    result.hybrid_score = base;
                    if (config.normalize_scores) {
                        if (result.hybrid_score < 0.0f)
                            result.hybrid_score = 0.0f;
                        else if (result.hybrid_score > 1.0f)
                            result.hybrid_score = 1.0f;
                    }
                    break;
                }
            }

            fused_results.push_back(std::move(result));
        }

        // Sort by hybrid score
        std::sort(fused_results.begin(), fused_results.end());

        return fused_results;
    }

    std::vector<HybridSearchResult> rerankResults(const std::vector<HybridSearchResult>& results,
                                                  const std::string& query) {
        // Simple re-ranking based on matched keywords and position
        std::vector<HybridSearchResult> reranked = results;

        // Take only top k for reranking if specified
        size_t rerank_count = std::min(config_.rerank_top_k, reranked.size());

        // Apply boost for exact matches and keyword density
        for (size_t i = 0; i < rerank_count; ++i) {
            auto& result = reranked[i];

            // Boost for exact query match
            if (result.content.find(query) != std::string::npos) {
                result.hybrid_score *= 1.2f;
            }

            // Boost based on number of matched keywords
            if (!result.matched_keywords.empty()) {
                float keyword_boost = 1.0f + (0.05f * result.matched_keywords.size());
                result.hybrid_score *= keyword_boost;
            }
        }

        // Re-sort after boosting
        std::sort(reranked.begin(), reranked.begin() + rerank_count);

        return reranked;
    }

    std::vector<std::string> expandQuery(const std::string& query) {
        // Simple query expansion - in real implementation would use synonyms, etc.
        std::vector<std::string> expanded;

        // Add some common variations
        auto tokens = keyword_engine_->analyzeQuery(query);
        for (const auto& token : tokens) {
            // Add plural/singular forms (simplified)
            if (token.back() == 's') {
                expanded.push_back(token.substr(0, token.size() - 1));
            } else {
                expanded.push_back(token + "s");
            }
        }

        // Limit expansion terms
        if (expanded.size() > config_.expansion_terms) {
            expanded.resize(config_.expansion_terms);
        }

        return expanded;
    }

    void setKGScorer(std::shared_ptr<KGScorer> kg_scorer) { kg_scorer_ = std::move(kg_scorer); }
    void setConfig(const HybridSearchConfig& cfg) {
        config_ = cfg;
        config_.normalizeWeights();
    }
    const HybridSearchConfig& getConfig() const { return config_; }
    void updateWeights(float vector_weight, float keyword_weight) {
        config_.vector_weight = vector_weight;
        config_.keyword_weight = keyword_weight;
        config_.normalizeWeights();
    }

private:
    // Minimal reusable thread pool to avoid oversubscription with std::async bursts
    class ThreadPool {
    public:
        ThreadPool() = default;
        ~ThreadPool() { stop(); }
        void start(size_t threads) {
            stop();
            if (threads == 0)
                return;
            done_ = false;
            for (size_t i = 0; i < threads; ++i) {
                workers_.emplace_back([this]() { run(); });
            }
        }
        void stop() {
            {
                std::lock_guard<std::mutex> lk(m_);
                done_ = true;
            }
            cv_.notify_all();
            for (auto& t : workers_)
                if (t.joinable())
                    t.join();
            workers_.clear();
            while (!q_.empty())
                q_.pop();
        }
        template <typename F, typename R = std::invoke_result_t<F&>> std::future<R> submit(F&& f) {
            // Use shared_ptr to hold move-only packaged_task inside copyable std::function
            auto pt = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));
            auto fut = pt->get_future();
            {
                std::lock_guard<std::mutex> lk(m_);
                q_.emplace([pt]() mutable { (*pt)(); });
            }
            cv_.notify_one();
            return fut;
        }

    private:
        void run() {
            for (;;) {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lk(m_);
                    cv_.wait(lk, [this]() { return done_ || !q_.empty(); });
                    if (done_ && q_.empty())
                        return;
                    job = std::move(q_.front());
                    q_.pop();
                }
                try {
                    job();
                } catch (...) {
                }
            }
        }
        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> q_;
        std::mutex m_;
        std::condition_variable cv_;
        bool done_{true};
    };

    void initThreadPool() {
        size_t n = config_.num_threads;
        if (n == 0) {
            size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
            n = std::clamp<size_t>(hw / 2, 2, 4);
        }
        pool_.start(n);
    }

    std::shared_ptr<vector::VectorIndexManager> vector_index_;
    std::shared_ptr<KeywordSearchEngine> keyword_engine_;
    std::shared_ptr<vector::EmbeddingGenerator> embedding_generator_;
    bool vector_index_ready_ = false;
    HybridSearchConfig config_;
    std::shared_ptr<KGScorer> kg_scorer_;
    std::unordered_map<std::string, KGScore> last_kg_scores_;
    bool initialized_ = false;

    // Cache
    struct CacheEntry {
        std::vector<HybridSearchResult> results;
        std::chrono::steady_clock::time_point timestamp;
    };
    std::unordered_map<std::string, CacheEntry> cache_;
    mutable std::mutex cache_mutex_;

    // Metrics
    SearchMetrics metrics_;
    mutable std::mutex metrics_mutex_;

    ThreadPool pool_;

    // Helper methods
    float normalizeKeywordScore(float score, const std::vector<KeywordSearchResult>& all_results) {
        if (all_results.empty()) {
            return 0.0f;
        }

        float max_score = all_results[0].score; // Assumes sorted
        if (max_score == 0) {
            return 0.0f;
        }

        return score / max_score;
    }

    std::string generateCacheKey(const std::string& query, size_t k,
                                 [[maybe_unused]] const vector::SearchFilter& filter) {
        // Simple cache key generation
        return query + "_" + std::to_string(k);
    }

    std::optional<std::vector<HybridSearchResult>> getCachedResult(const std::string& key) {
        std::lock_guard<std::mutex> lock(cache_mutex_);

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            auto age = std::chrono::steady_clock::now() - it->second.timestamp;
            if (age <= config_.cache_ttl) {
                return it->second.results;
            } else {
                cache_.erase(it);
            }
        }

        return std::nullopt;
    }

    void cacheResult(const std::string& key, const std::vector<HybridSearchResult>& results) {
        std::lock_guard<std::mutex> lock(cache_mutex_);

        // Enforce cache size limit
        if (cache_.size() >= config_.cache_size) {
            // Remove oldest entry (simple LRU)
            auto oldest = cache_.begin();
            for (auto it = cache_.begin(); it != cache_.end(); ++it) {
                if (it->second.timestamp < oldest->second.timestamp) {
                    oldest = it;
                }
            }
            cache_.erase(oldest);
        }

        cache_[key] = {results, std::chrono::steady_clock::now()};
    }

    void generateExplanations(std::vector<HybridSearchResult>& results,
                              [[maybe_unused]] const std::string& query) {
        for (auto& result : results) {
            HybridSearchResult::Explanation explanation;

            // Determine primary method
            if (result.found_by_vector && result.found_by_keyword) {
                explanation.primary_method = "Both vector and keyword search";
            } else if (result.found_by_vector) {
                explanation.primary_method = "Vector similarity search";
            } else {
                explanation.primary_method = "Keyword search";
            }

            // Add score breakdown
            explanation.score_breakdown["vector_score"] = result.vector_score;
            explanation.score_breakdown["keyword_score"] = result.keyword_score;
            explanation.score_breakdown["kg_entity_score"] = result.kg_entity_score;
            explanation.score_breakdown["structural_score"] = result.structural_score;
            explanation.score_breakdown["hybrid_score"] = result.hybrid_score;

            // Add fusion method
            switch (config_.fusion_strategy) {
                case HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION:
                    explanation.fusion_method = "Linear combination";
                    break;
                case HybridSearchConfig::FusionStrategy::RECIPROCAL_RANK:
                    explanation.fusion_method = "Reciprocal rank fusion";
                    break;
                default:
                    explanation.fusion_method = "Unknown";
            }

            // Add reasons
            if (result.found_by_vector) {
                explanation.reasons.push_back("High semantic similarity to query");
            }
            if (!result.matched_keywords.empty()) {
                explanation.reasons.push_back("Contains keywords: " +
                                              joinStrings(result.matched_keywords, ", "));
            }
            if (config_.enable_kg &&
                (result.kg_entity_score > 0.0f || result.structural_score > 0.0f)) {
                explanation.reasons.push_back("KG signals contributed to ranking");
            }

            result.explanation = explanation;
        }
    }

    void updateMetrics(double search_time, double fusion_time,
                       const std::vector<HybridSearchResult>& results) {
        std::lock_guard<std::mutex> lock(metrics_mutex_);

        metrics_.total_searches++;
        metrics_.total_results_returned += results.size();

        // Update timing metrics (moving average)
        metrics_.avg_latency_ms =
            (metrics_.avg_latency_ms * (metrics_.total_searches - 1) + search_time) /
            metrics_.total_searches;
        metrics_.fusion_time_ms = fusion_time;

        // Update method distribution
        for (const auto& result : results) {
            if (result.found_by_vector && result.found_by_keyword) {
                metrics_.both_methods_results++;
            } else if (result.found_by_vector) {
                metrics_.vector_only_results++;
            } else {
                metrics_.keyword_only_results++;
            }
        }

        // Update cache hit rate
        if (metrics_.cache_hits + metrics_.cache_misses > 0) {
            metrics_.cache_hit_rate = static_cast<double>(metrics_.cache_hits) /
                                      (metrics_.cache_hits + metrics_.cache_misses);
        }
    }

    std::string joinStrings(const std::vector<std::string>& strings, const std::string& delimiter) {
        if (strings.empty()) {
            return "";
        }

        std::ostringstream oss;
        oss << strings[0];
        for (size_t i = 1; i < strings.size(); ++i) {
            oss << delimiter << strings[i];
        }
        return oss.str();
    }
};

// =============================================================================
// HybridSearchEngine Public Interface
// =============================================================================

HybridSearchEngine::HybridSearchEngine(
    std::shared_ptr<vector::VectorIndexManager> vector_index,
    std::shared_ptr<KeywordSearchEngine> keyword_engine, const HybridSearchConfig& config,
    std::shared_ptr<vector::EmbeddingGenerator> embedding_generator)
    : pImpl(std::make_unique<Impl>(std::move(vector_index), std::move(keyword_engine), config,
                                   std::move(embedding_generator))) {}

HybridSearchEngine::~HybridSearchEngine() = default;
HybridSearchEngine::HybridSearchEngine(HybridSearchEngine&&) noexcept = default;
HybridSearchEngine& HybridSearchEngine::operator=(HybridSearchEngine&&) noexcept = default;

Result<void> HybridSearchEngine::initialize() {
    return pImpl->initialize();
}

bool HybridSearchEngine::isInitialized() const {
    return pImpl->isInitialized();
}

void HybridSearchEngine::shutdown() {
    pImpl->shutdown();
}

Result<std::vector<HybridSearchResult>>
HybridSearchEngine::search(const std::string& query, size_t k, const vector::SearchFilter& filter) {
    return pImpl->search(query, k, filter);
}

std::vector<HybridSearchResult>
HybridSearchEngine::fuseResults(const std::vector<vector::SearchResult>& vector_results,
                                const std::vector<KeywordSearchResult>& keyword_results,
                                const HybridSearchConfig& config) {
    return pImpl->fuseResults(vector_results, keyword_results, config);
}

std::vector<HybridSearchResult>
HybridSearchEngine::rerankResults(const std::vector<HybridSearchResult>& results,
                                  const std::string& query) {
    return pImpl->rerankResults(results, query);
}

std::vector<std::string> HybridSearchEngine::expandQuery(const std::string& query) {
    return pImpl->expandQuery(query);
}

void HybridSearchEngine::setKGScorer(std::shared_ptr<KGScorer> kg_scorer) {
    pImpl->setKGScorer(std::move(kg_scorer));
}

void HybridSearchEngine::setConfig(const HybridSearchConfig& config) {
    pImpl->setConfig(config);
}

const HybridSearchConfig& HybridSearchEngine::getConfig() const {
    return pImpl->getConfig();
}

void HybridSearchEngine::updateWeights(float vector_weight, float keyword_weight) {
    pImpl->updateWeights(vector_weight, keyword_weight);
}

// =============================================================================
// Fusion Utilities
// =============================================================================

namespace fusion {

float linearCombination(float vector_score, float keyword_score, float vector_weight,
                        float keyword_weight) {
    return (vector_score * vector_weight) + (keyword_score * keyword_weight);
}

float reciprocalRankFusion(size_t vector_rank, size_t keyword_rank, float k) {
    float score = 0.0f;

    if (vector_rank != SIZE_MAX) {
        score += 1.0f / (k + vector_rank + 1); // +1 because ranks are 0-based
    }

    if (keyword_rank != SIZE_MAX) {
        score += 1.0f / (k + keyword_rank + 1);
    }

    return score;
}

std::vector<float> normalizeScores(const std::vector<float>& scores) {
    if (scores.empty()) {
        return {};
    }

    auto [min_it, max_it] = std::minmax_element(scores.begin(), scores.end());
    float min_score = *min_it;
    float max_score = *max_it;

    if (max_score == min_score) {
        return std::vector<float>(scores.size(), 1.0f);
    }

    std::vector<float> normalized;
    normalized.reserve(scores.size());

    for (float score : scores) {
        normalized.push_back((score - min_score) / (max_score - min_score));
    }

    return normalized;
}

float minMaxNormalize(float value, float min, float max) {
    if (max == min) {
        return 0.5f;
    }
    return (value - min) / (max - min);
}

float zScoreNormalize(float value, float mean, float stddev) {
    if (stddev == 0) {
        return 0.0f;
    }
    return (value - mean) / stddev;
}

ScoreStats computeScoreStats(const std::vector<float>& scores) {
    ScoreStats stats;

    if (scores.empty()) {
        return stats;
    }

    // Min and max
    auto [min_it, max_it] = std::minmax_element(scores.begin(), scores.end());
    stats.min = *min_it;
    stats.max = *max_it;

    // Mean
    float sum = std::accumulate(scores.begin(), scores.end(), 0.0f);
    stats.mean = sum / scores.size();

    // Standard deviation
    float sq_sum = 0.0f;
    for (float score : scores) {
        float diff = score - stats.mean;
        sq_sum += diff * diff;
    }
    stats.stddev = std::sqrt(sq_sum / scores.size());

    // Median
    std::vector<float> sorted_scores = scores;
    std::sort(sorted_scores.begin(), sorted_scores.end());
    size_t mid = sorted_scores.size() / 2;
    if (sorted_scores.size() % 2 == 0) {
        stats.median = (sorted_scores[mid - 1] + sorted_scores[mid]) / 2.0f;
    } else {
        stats.median = sorted_scores[mid];
    }

    return stats;
}

} // namespace fusion

} // namespace yams::search
