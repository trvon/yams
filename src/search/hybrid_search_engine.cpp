#include <yams/search/hybrid_search_engine.h>

#include <algorithm>
#include <numeric>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <thread>
#include <future>
#include <mutex>
#include <sstream>
#include <regex>

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
    
    Result<std::vector<KeywordSearchResult>> search(
        const std::string& query,
        size_t k,
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
                if (!filter->exclude_ids.empty()) {
                    if (std::find(filter->exclude_ids.begin(), 
                                 filter->exclude_ids.end(), 
                                 doc_id) != filter->exclude_ids.end()) {
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
    
    Result<std::vector<std::vector<KeywordSearchResult>>> batchSearch(
        const std::vector<std::string>& queries,
        size_t k,
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
    
    Result<void> addDocument(
        const std::string& id,
        const std::string& content,
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
    
    Result<void> updateDocument(
        const std::string& id,
        const std::string& content,
        const std::map<std::string, std::string>& metadata) override {
        
        // Remove old document
        auto remove_result = removeDocument(id);
        if (!remove_result.has_value() && 
            remove_result.error().code != ErrorCode::NotFound) {
            return remove_result;
        }
        
        // Add new document
        return addDocument(id, content, metadata);
    }
    
    Result<void> addDocuments(
        const std::vector<std::string>& ids,
        const std::vector<std::string>& contents,
        const std::vector<std::map<std::string, std::string>>& metadata) override {
        
        if (ids.size() != contents.size()) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "IDs and contents size mismatch"});
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
    
    size_t getDocumentCount() const override {
        return documents_.size();
    }
    
    size_t getTermCount() const override {
        return document_frequencies_.size();
    }
    
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
            float df = df_it != document_frequencies_.end() ? 
                      static_cast<float>(df_it->second) : 0.5f;
            
            float idf = std::log((N - df + 0.5f) / (df + 0.5f));
            
            float normalized_tf = (tf * (k1_ + 1)) / 
                                 (tf + k1_ * (1 - b_ + b_ * doc_length / avg_doc_length_));
            
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
         std::shared_ptr<KeywordSearchEngine> keyword_engine,
         const HybridSearchConfig& config)
        : vector_index_(std::move(vector_index))
        , keyword_engine_(std::move(keyword_engine))
        , config_(config) {
        
        config_.normalizeWeights();
    }
    
    Result<void> initialize() {
        if (initialized_) {
            return Result<void>();
        }
        
        if (!vector_index_ || !keyword_engine_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Invalid search engines"});
        }
        
        // Initialize vector index if needed
        if (!vector_index_->isInitialized()) {
            auto result = vector_index_->initialize();
            if (!result.has_value()) {
                return result;
            }
        }
        
        initialized_ = true;
        return Result<void>();
    }
    
    bool isInitialized() const {
        return initialized_;
    }
    
    void shutdown() {
        if (vector_index_) {
            vector_index_->shutdown();
        }
        cache_.clear();
        initialized_ = false;
    }
    
    Result<std::vector<HybridSearchResult>> search(
        const std::string& query,
        size_t k,
        const vector::SearchFilter& filter) {
        
        if (!initialized_) {
            return Result<std::vector<HybridSearchResult>>(
                Error{ErrorCode::InvalidState, "Engine not initialized"});
        }
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Check cache
        if (config_.enable_cache) {
            auto cache_key = generateCacheKey(query, k, filter);
            auto cached = getCachedResult(cache_key);
            if (cached.has_value()) {
                metrics_.cache_hits++;
                return Result<std::vector<HybridSearchResult>>(cached.value());
            }
            metrics_.cache_misses++;
        }
        
        // Expand query if enabled
        std::string expanded_query = query;
        if (config_.enable_query_expansion) {
            auto expanded_terms = expandQuery(query);
            if (!expanded_terms.empty()) {
                expanded_query += " " + joinStrings(expanded_terms, " ");
            }
        }
        
        // Perform parallel or sequential search
        std::future<Result<std::vector<vector::SearchResult>>> vector_future;
        std::future<Result<std::vector<KeywordSearchResult>>> keyword_future;
        
        if (config_.parallel_search) {
            // Launch searches in parallel
            vector_future = std::async(std::launch::async, [this, &query, &filter]() {
                // TODO: Generate embedding for query
                std::vector<float> query_vector(384, 0.0f); // Placeholder
                return vector_index_->search(query_vector, config_.vector_top_k, filter);
            });
            
            keyword_future = std::async(std::launch::async, [this, &expanded_query, &filter]() {
                return keyword_engine_->search(expanded_query, config_.keyword_top_k, &filter);
            });
        }
        
        // Get results
        std::vector<vector::SearchResult> vector_results;
        std::vector<KeywordSearchResult> keyword_results;
        
        if (config_.parallel_search) {
            auto vector_result = vector_future.get();
            auto keyword_result = keyword_future.get();
            
            if (vector_result.has_value()) {
                vector_results = vector_result.value();
            }
            if (keyword_result.has_value()) {
                keyword_results = keyword_result.value();
            }
        } else {
            // Sequential search
            // TODO: Generate embedding for query
            std::vector<float> query_vector(384, 0.0f); // Placeholder
            auto vector_result = vector_index_->search(query_vector, config_.vector_top_k, filter);
            if (vector_result.has_value()) {
                vector_results = vector_result.value();
            }
            
            auto keyword_result = keyword_engine_->search(expanded_query, config_.keyword_top_k, &filter);
            if (keyword_result.has_value()) {
                keyword_results = keyword_result.value();
            }
        }
        
        // Record search times
        auto search_end = std::chrono::high_resolution_clock::now();
        auto search_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            search_end - start_time);
        
        // Fuse results
        auto fusion_start = std::chrono::high_resolution_clock::now();
        auto fused_results = fuseResults(vector_results, keyword_results, config_);
        auto fusion_end = std::chrono::high_resolution_clock::now();
        auto fusion_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            fusion_end - fusion_start);
        
        // Re-rank if enabled
        if (config_.enable_reranking && !fused_results.empty()) {
            auto rerank_start = std::chrono::high_resolution_clock::now();
            fused_results = rerankResults(fused_results, query);
            auto rerank_end = std::chrono::high_resolution_clock::now();
            auto rerank_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                rerank_end - rerank_start);
            metrics_.rerank_time_ms = rerank_duration.count() / 1000.0;
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
            generateExplanations(fused_results, query);
        }
        
        // Update metrics
        updateMetrics(search_duration.count() / 1000.0,
                     fusion_duration.count() / 1000.0,
                     fused_results);
        
        // Cache result if enabled
        if (config_.enable_cache) {
            auto cache_key = generateCacheKey(query, k, filter);
            cacheResult(cache_key, fused_results);
        }
        
        return Result<std::vector<HybridSearchResult>>(fused_results);
    }
    
    std::vector<HybridSearchResult> fuseResults(
        const std::vector<vector::SearchResult>& vector_results,
        const std::vector<KeywordSearchResult>& keyword_results,
        const HybridSearchConfig& config) {
        
        std::unordered_map<std::string, HybridSearchResult> result_map;
        
        // Process vector results
        for (size_t i = 0; i < vector_results.size(); ++i) {
            const auto& vr = vector_results[i];
            auto& result = result_map[vr.id];
            result.id = vr.id;
            result.vector_score = vr.similarity;
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
            switch (config.fusion_strategy) {
                case HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION:
                    result.hybrid_score = fusion::linearCombination(
                        result.vector_score,
                        result.keyword_score,
                        config.vector_weight,
                        config.keyword_weight);
                    break;
                    
                case HybridSearchConfig::FusionStrategy::RECIPROCAL_RANK:
                    result.hybrid_score = fusion::reciprocalRankFusion(
                        result.vector_rank,
                        result.keyword_rank,
                        config.rrf_k);
                    break;
                    
                default:
                    // Default to linear combination
                    result.hybrid_score = fusion::linearCombination(
                        result.vector_score,
                        result.keyword_score,
                        config.vector_weight,
                        config.keyword_weight);
                    break;
            }
            
            fused_results.push_back(std::move(result));
        }
        
        // Sort by hybrid score
        std::sort(fused_results.begin(), fused_results.end());
        
        return fused_results;
    }
    
    std::vector<HybridSearchResult> rerankResults(
        const std::vector<HybridSearchResult>& results,
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
    
private:
    std::shared_ptr<vector::VectorIndexManager> vector_index_;
    std::shared_ptr<KeywordSearchEngine> keyword_engine_;
    HybridSearchConfig config_;
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
    
    // Helper methods
    float normalizeKeywordScore(float score, 
                                const std::vector<KeywordSearchResult>& all_results) {
        if (all_results.empty()) {
            return 0.0f;
        }
        
        float max_score = all_results[0].score; // Assumes sorted
        if (max_score == 0) {
            return 0.0f;
        }
        
        return score / max_score;
    }
    
    std::string generateCacheKey(const std::string& query, 
                                 size_t k,
                                 const vector::SearchFilter& filter) {
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
                              const std::string& query) {
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
            
            result.explanation = explanation;
        }
    }
    
    void updateMetrics(double search_time, double fusion_time,
                      const std::vector<HybridSearchResult>& results) {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        
        metrics_.total_searches++;
        metrics_.total_results_returned += results.size();
        
        // Update timing metrics (moving average)
        metrics_.avg_latency_ms = (metrics_.avg_latency_ms * (metrics_.total_searches - 1) + 
                                   search_time) / metrics_.total_searches;
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
    std::shared_ptr<KeywordSearchEngine> keyword_engine,
    const HybridSearchConfig& config)
    : pImpl(std::make_unique<Impl>(std::move(vector_index), 
                                   std::move(keyword_engine), 
                                   config)) {}

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

Result<std::vector<HybridSearchResult>> HybridSearchEngine::search(
    const std::string& query,
    size_t k,
    const vector::SearchFilter& filter) {
    return pImpl->search(query, k, filter);
}

std::vector<HybridSearchResult> HybridSearchEngine::fuseResults(
    const std::vector<vector::SearchResult>& vector_results,
    const std::vector<KeywordSearchResult>& keyword_results,
    const HybridSearchConfig& config) {
    return pImpl->fuseResults(vector_results, keyword_results, config);
}

std::vector<HybridSearchResult> HybridSearchEngine::rerankResults(
    const std::vector<HybridSearchResult>& results,
    const std::string& query) {
    return pImpl->rerankResults(results, query);
}

std::vector<std::string> HybridSearchEngine::expandQuery(const std::string& query) {
    return pImpl->expandQuery(query);
}

// =============================================================================
// Fusion Utilities
// =============================================================================

namespace fusion {

float linearCombination(float vector_score, float keyword_score,
                        float vector_weight, float keyword_weight) {
    return (vector_score * vector_weight) + (keyword_score * keyword_weight);
}

float reciprocalRankFusion(size_t vector_rank, size_t keyword_rank, float k) {
    float score = 0.0f;
    
    if (vector_rank != SIZE_MAX) {
        score += 1.0f / (k + vector_rank + 1);  // +1 because ranks are 0-based
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