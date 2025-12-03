#include <yams/api/semantic_search_api.h>
/*
 TODO(yams): Composition point for KG-enabled SearchEngine:
 - Build SearchEngine with SearchEngineBuilder
 - Configure SearchEngineConfig (weights, kg settings, etc.)
 - Create a KnowledgeGraphStore (SQLite) using the metadata DB path
 - Pass KG store into SearchEngine constructor
 - Initialize and pass into SemanticSearchAPI
*/

#include <algorithm>
#include <future>
#include <mutex>
#include <numeric>
#include <random>
#include <regex>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::api {

// =============================================================================
// Document Processing Pipeline
// =============================================================================

class DocumentProcessor {
public:
    DocumentProcessor(std::shared_ptr<vector::DocumentChunker> chunker,
                      std::shared_ptr<vector::EmbeddingGenerator> embedder)
        : chunker_(std::move(chunker)), embedder_(std::move(embedder)) {}

    struct ProcessingResult {
        std::string document_id;
        std::vector<std::string> chunk_ids;
        std::vector<std::vector<float>> embeddings;
        std::vector<std::string> extracted_keywords;
        DocumentMetadata enriched_metadata;

        struct {
            size_t chunks_created = 0;
            double processing_time_ms = 0.0;
            size_t total_tokens = 0;
        } stats;
    };

    Result<ProcessingResult> processDocument(const std::string& content,
                                             const DocumentMetadata& metadata) {
        auto start_time = std::chrono::high_resolution_clock::now();

        ProcessingResult result;
        result.document_id = generateDocumentId();

        try {
            // 1. Preprocess text
            std::string processed_content = preprocessText(content);

            // 2. Chunk document
            auto chunks = chunker_->chunkDocument(processed_content, result.document_id);
            if (chunks.empty()) {
                return Result<ProcessingResult>(
                    Error{ErrorCode::InvalidData, "Failed to chunk document"});
            }

            result.stats.chunks_created = chunks.size();

            // 3. Generate embeddings for chunks
            std::vector<std::string> chunk_contents;
            for (const auto& chunk : chunks) {
                chunk_contents.push_back(chunk.content);
                result.chunk_ids.push_back(chunk.chunk_id);
            }

            if (!embedder_->isInitialized()) {
                bool init_result = embedder_->initialize();
                if (!init_result) {
                    return Result<ProcessingResult>(Error{
                        ErrorCode::InternalError, "Failed to initialize embedding generator"});
                }
            }

            auto embeddings = embedder_->generateEmbeddings(chunk_contents);
            result.embeddings = embeddings;

            // 4. Extract keywords
            result.extracted_keywords = extractKeywords(processed_content);

            // 5. Enrich metadata
            result.enriched_metadata = enrichMetadata(processed_content, metadata);

            // 6. Calculate statistics
            for (const auto& chunk_content : chunk_contents) {
                result.stats.total_tokens += embedder_->estimateTokenCount(chunk_content);
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            result.stats.processing_time_ms = duration.count() / 1000.0;

            return Result<ProcessingResult>(result);

        } catch (const std::exception& e) {
            return Result<ProcessingResult>(Error{
                ErrorCode::InternalError, "Document processing failed: " + std::string(e.what())});
        }
    }

private:
    std::shared_ptr<vector::DocumentChunker> chunker_;
    std::shared_ptr<vector::EmbeddingGenerator> embedder_;

    std::string preprocessText(const std::string& text) {
        std::string processed = text;

        // Normalize whitespace
        std::regex multiple_spaces(R"(\s+)");
        processed = std::regex_replace(processed, multiple_spaces, " ");

        // Trim
        processed.erase(0, processed.find_first_not_of(" \t\n\r"));
        processed.erase(processed.find_last_not_of(" \t\n\r") + 1);

        return processed;
    }

    std::vector<std::string> extractKeywords(const std::string& text) {
        // Simple keyword extraction - in real implementation would use NLP library
        std::vector<std::string> keywords;
        std::regex word_regex(R"(\b[a-zA-Z]{3,}\b)"); // Words with 3+ letters
        auto words_begin = std::sregex_iterator(text.begin(), text.end(), word_regex);
        auto words_end = std::sregex_iterator();

        std::unordered_map<std::string, size_t> word_freq;
        for (auto it = words_begin; it != words_end; ++it) {
            std::string word = it->str();
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            word_freq[word]++;
        }

        // Get most frequent words
        std::vector<std::pair<size_t, std::string>> freq_words;
        for (const auto& [word, freq] : word_freq) {
            if (freq >= 2) { // Must appear at least twice
                freq_words.emplace_back(freq, word);
            }
        }

        std::sort(freq_words.rbegin(), freq_words.rend());

        size_t max_keywords = std::min(size_t(10), freq_words.size());
        for (size_t i = 0; i < max_keywords; ++i) {
            keywords.push_back(freq_words[i].second);
        }

        return keywords;
    }

    DocumentMetadata enrichMetadata(const std::string& text, const DocumentMetadata& original) {
        DocumentMetadata enriched = original;

        // Calculate word count
        std::regex word_regex(R"(\b\w+\b)");
        enriched.word_count = std::distance(
            std::sregex_iterator(text.begin(), text.end(), word_regex), std::sregex_iterator());

        // Set timestamps if not provided
        auto now = std::chrono::system_clock::now();
        if (enriched.created_at == std::chrono::system_clock::time_point{}) {
            enriched.created_at = now;
        }
        if (enriched.updated_at == std::chrono::system_clock::time_point{}) {
            enriched.updated_at = now;
        }

        // Estimate content quality (simple heuristic)
        enriched.content_quality = std::min(1.0f, static_cast<float>(enriched.word_count) / 100.0f);

        return enriched;
    }

    std::string generateDocumentId() {
        // Simple UUID-like ID generation
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 15);

        std::string id = "doc_";
        const char hex_chars[] = "0123456789abcdef";
        for (int i = 0; i < 16; ++i) {
            id += hex_chars[dis(gen)];
        }
        return id;
    }
};

// =============================================================================
// Query Processor
// =============================================================================

class QueryProcessor {
public:
    struct ProcessedQuery {
        std::string original_query;
        std::string normalized_query;
        std::vector<std::string> tokens;
        std::vector<std::string> keywords;

        // Analysis results
        std::string detected_language = "en";
        QueryIntent intent = QueryIntent::INFORMATIONAL;
        std::vector<Entity> entities;

        // Query enhancements
        std::vector<std::string> synonyms;
        std::vector<std::string> related_terms;
        std::optional<std::string> corrected_spelling;

        // Extracted filters
        SearchFilters extracted_filters;
    };

    ProcessedQuery processQuery(const std::string& query) {
        ProcessedQuery result;
        result.original_query = query;
        result.normalized_query = query_utils::normalizeQuery(query);
        result.tokens = tokenize(result.normalized_query);
        result.keywords = query_utils::extractKeywords(result.normalized_query);
        result.detected_language = query_utils::detectLanguage(query);
        result.intent = query_utils::classifyIntent(query);
        result.entities = query_utils::extractEntities(query);
        result.extracted_filters = extractFiltersFromQuery(query);

        return result;
    }

private:
    std::vector<std::string> tokenize(const std::string& text) {
        std::vector<std::string> tokens;
        std::regex word_regex(R"(\b\w+\b)");
        auto words_begin = std::sregex_iterator(text.begin(), text.end(), word_regex);
        auto words_end = std::sregex_iterator();

        for (auto it = words_begin; it != words_end; ++it) {
            tokens.push_back(it->str());
        }

        return tokens;
    }

    SearchFilters extractFiltersFromQuery(const std::string& query) {
        SearchFilters filters;

        // Simple filter extraction (in real implementation would be more sophisticated)
        // Look for patterns like "author:name", "type:pdf", etc.
        std::regex filter_regex(R"(\b(\w+):(\w+)\b)");
        auto matches_begin = std::sregex_iterator(query.begin(), query.end(), filter_regex);
        auto matches_end = std::sregex_iterator();

        for (auto it = matches_begin; it != matches_end; ++it) {
            std::string key = it->str(1);
            std::string value = it->str(2);

            if (key == "author") {
                filters.authors.push_back(value);
            } else if (key == "type") {
                filters.content_types.push_back(value);
            } else if (key == "lang" || key == "language") {
                filters.languages.push_back(value);
            }
        }

        return filters;
    }
};

// =============================================================================
// Session Manager
// =============================================================================

class SessionManager {
public:
    struct SessionInfo {
        std::string session_id;
        SessionConfig config;
        std::chrono::system_clock::time_point created_at;
        std::chrono::system_clock::time_point last_accessed;

        std::vector<std::string> query_history;
        std::unordered_map<std::string, size_t> click_history;
        std::unordered_map<std::string, size_t> conversion_history;

        // User preferences (simplified)
        std::map<std::string, float> preferences;
    };

    std::string createSession(const SessionConfig& config = {}) {
        std::string session_id = generateSessionId();

        std::lock_guard<std::mutex> lock(mutex_);
        SessionInfo info;
        info.session_id = session_id;
        info.config = config;
        info.created_at = std::chrono::system_clock::now();
        info.last_accessed = info.created_at;

        sessions_[session_id] = info;
        return session_id;
    }

    bool hasSession(const std::string& session_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return sessions_.find(session_id) != sessions_.end();
    }

    std::optional<SessionInfo> getSession(const std::string& session_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = sessions_.find(session_id);
        if (it != sessions_.end()) {
            it->second.last_accessed = std::chrono::system_clock::now();
            return it->second;
        }
        return std::nullopt;
    }

    void addQuery(const std::string& session_id, const std::string& query) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = sessions_.find(session_id);
        if (it != sessions_.end()) {
            auto& info = it->second;
            info.query_history.push_back(query);
            if (info.query_history.size() > info.config.max_history_size) {
                info.query_history.erase(info.query_history.begin());
            }
            info.last_accessed = std::chrono::system_clock::now();
        }
    }

    void endSession(const std::string& session_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        sessions_.erase(session_id);
    }

    void cleanupExpiredSessions() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = std::chrono::system_clock::now();

        for (auto it = sessions_.begin(); it != sessions_.end();) {
            auto age = now - it->second.last_accessed;
            if (age > it->second.config.timeout) {
                it = sessions_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, SessionInfo> sessions_;

    std::string generateSessionId() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 15);

        std::string id = "sess_";
        const char hex_chars[] = "0123456789abcdef";
        for (int i = 0; i < 24; ++i) {
            id += hex_chars[dis(gen)];
        }
        return id;
    }
};

// =============================================================================
// Semantic Search API Implementation
// =============================================================================

class SemanticSearchAPI::Impl {
public:
    Impl(std::shared_ptr<search::SearchEngine> search_engine,
         std::shared_ptr<vector::DocumentChunker> chunker,
         std::shared_ptr<vector::EmbeddingGenerator> embedder, const SearchAPIConfig& config)
        : search_engine_(std::move(search_engine)),
          document_processor_(std::make_unique<DocumentProcessor>(chunker, embedder)),
          query_processor_(std::make_unique<QueryProcessor>()),
          session_manager_(std::make_unique<SessionManager>()), config_(config) {}

    Result<void> initialize() {
        if (initialized_) {
            return Result<void>();
        }

        if (!search_engine_ || !document_processor_) {
            return Result<void>(Error{ErrorCode::InvalidArgument, "Missing required components"});
        }

        // SearchEngine doesn't require explicit initialization
        // (components are initialized via constructor injection)

        // Start cleanup thread for sessions
        cleanup_thread_ = std::thread([this]() {
            while (initialized_) {
                std::this_thread::sleep_for(std::chrono::minutes(5));
                if (session_manager_) {
                    session_manager_->cleanupExpiredSessions();
                }
            }
        });

        initialized_ = true;
        return Result<void>();
    }

    bool isInitialized() const { return initialized_; }

    void shutdown() {
        initialized_ = false;

        if (cleanup_thread_.joinable()) {
            cleanup_thread_.join();
        }

        // SearchEngine doesn't have explicit shutdown (RAII cleanup)
    }

    Result<std::string> ingestDocument(const std::string& content,
                                       const DocumentMetadata& metadata) {
        if (!initialized_) {
            return Result<std::string>(Error{ErrorCode::InvalidState, "API not initialized"});
        }

        if (content.empty()) {
            return Result<std::string>(Error{ErrorCode::InvalidArgument, "Empty content"});
        }

        // Process document
        auto processing_result = document_processor_->processDocument(content, metadata);
        if (!processing_result.has_value()) {
            return Result<std::string>(processing_result.error());
        }

        auto& result = processing_result.value();

        // TODO: Document ingestion now goes through the IndexingService, not SearchEngine
        // This API needs refactoring to work with the new architecture.
        // For now, just store document metadata locally.
        // Proper integration would use:
        //   - IndexingService for document ingestion
        //   - MetadataRepository for document storage
        //   - VectorDatabase for embedding storage

        // Store document metadata
        {
            std::lock_guard<std::mutex> lock(documents_mutex_);
            documents_[result.document_id] = {content, result.enriched_metadata};
        }

        return Result<std::string>(result.document_id);
    }

    Result<SearchResponse> search(const SearchRequest& request) {
        if (!initialized_) {
            return Result<SearchResponse>(Error{ErrorCode::InvalidState, "API not initialized"});
        }

        if (!request.isValid()) {
            return Result<SearchResponse>(Error{ErrorCode::InvalidArgument, "Invalid request"});
        }

        auto start_time = std::chrono::high_resolution_clock::now();

        try {
            // Process query
            auto processed_query = query_processor_->processQuery(request.query);

            // Convert to SearchParams
            search::SearchParams searchParams;
            searchParams.limit = static_cast<int>(request.results_per_page);
            searchParams.offset = static_cast<int>((request.page - 1) * request.results_per_page);
            // TODO: Convert API filters to SearchParams filters

            // Perform search
            auto search_result =
                search_engine_->search(processed_query.normalized_query, searchParams);

            if (!search_result.has_value()) {
                return Result<SearchResponse>(search_result.error());
            }

            // Convert results
            SearchResponse response;
            response.success = true;
            response.search_id = generateSearchId();
            response.timestamp = std::chrono::system_clock::now();
            response.processed_query = processed_query.normalized_query;
            response.detected_intent = processed_query.intent;
            response.entities = processed_query.entities;
            response.page = request.page;
            response.results_per_page = request.results_per_page;
            response.mode_used = request.options.mode;

            // Convert search engine results to API results
            for (const auto& result : search_result.value()) {
                SearchResult api_result;
                api_result.id = result.document.sha256Hash;
                api_result.title = result.document.fileName;
                api_result.url = result.document.filePath;
                api_result.content = result.snippet;
                api_result.score = static_cast<float>(result.score);
                api_result.highlighted_terms = result.matchedTerms;
                // Note: SearchEngine provides unified score, not separate component scores

                // Generate snippets if requested
                if (request.options.include_snippets && !result.snippet.empty()) {
                    TextSnippet snippet;
                    snippet.text = result.snippet;
                    snippet.relevance_score = static_cast<float>(result.score);
                    api_result.snippets.push_back(std::move(snippet));
                }

                response.results.push_back(std::move(api_result));
            }

            response.total_results = response.results.size();
            response.total_pages =
                (response.total_results + request.results_per_page - 1) / request.results_per_page;
            response.has_more_results = response.page < response.total_pages;

            // Calculate search time
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            response.search_time_ms = duration.count() / 1000.0;

            // Add session context if provided
            if (!request.session_id.empty()) {
                session_manager_->addQuery(request.session_id, request.query);
            }

            return Result<SearchResponse>(response);

        } catch (const std::exception& e) {
            SearchResponse error_response;
            error_response.success = false;
            error_response.error_message = e.what();
            return Result<SearchResponse>(error_response);
        }
    }

    std::string createSession(const SessionConfig& config) {
        return session_manager_->createSession(config);
    }

    Result<std::vector<std::string>> getSessionHistory(const std::string& session_id) {
        auto session_info = session_manager_->getSession(session_id);
        if (session_info.has_value()) {
            return Result<std::vector<std::string>>(session_info.value().query_history);
        }
        return Result<std::vector<std::string>>(Error{ErrorCode::NotFound, "Session not found"});
    }

    bool isHealthy() const { return initialized_ && search_engine_ != nullptr; }

private:
    std::shared_ptr<search::SearchEngine> search_engine_;
    std::unique_ptr<DocumentProcessor> document_processor_;
    std::unique_ptr<QueryProcessor> query_processor_;
    std::unique_ptr<SessionManager> session_manager_;

    SearchAPIConfig config_;
    bool initialized_ = false;
    std::thread cleanup_thread_;

    // Document storage
    struct DocumentInfo {
        std::string content;
        DocumentMetadata metadata;
    };
    std::unordered_map<std::string, DocumentInfo> documents_;
    std::mutex documents_mutex_;

    std::string generateSearchId() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 15);

        std::string id = "search_";
        const char hex_chars[] = "0123456789abcdef";
        for (int i = 0; i < 16; ++i) {
            id += hex_chars[dis(gen)];
        }
        return id;
    }
};

// =============================================================================
// SemanticSearchAPI Public Interface
// =============================================================================

SemanticSearchAPI::SemanticSearchAPI(std::shared_ptr<search::SearchEngine> search_engine,
                                     std::shared_ptr<vector::DocumentChunker> chunker,
                                     std::shared_ptr<vector::EmbeddingGenerator> embedder,
                                     const SearchAPIConfig& config)
    : pImpl(std::make_unique<Impl>(std::move(search_engine), std::move(chunker),
                                   std::move(embedder), config)) {}

SemanticSearchAPI::~SemanticSearchAPI() = default;
SemanticSearchAPI::SemanticSearchAPI(SemanticSearchAPI&&) noexcept = default;
SemanticSearchAPI& SemanticSearchAPI::operator=(SemanticSearchAPI&&) noexcept = default;

Result<void> SemanticSearchAPI::initialize() {
    return pImpl->initialize();
}

bool SemanticSearchAPI::isInitialized() const {
    return pImpl->isInitialized();
}

void SemanticSearchAPI::shutdown() {
    pImpl->shutdown();
}

Result<std::string> SemanticSearchAPI::ingestDocument(const std::string& content,
                                                      const DocumentMetadata& metadata) {
    return pImpl->ingestDocument(content, metadata);
}

Result<SearchResponse> SemanticSearchAPI::search(const SearchRequest& request) {
    return pImpl->search(request);
}

std::string SemanticSearchAPI::createSession(const SessionConfig& config) {
    return pImpl->createSession(config);
}

Result<std::vector<std::string>>
SemanticSearchAPI::getSessionHistory(const std::string& session_id) {
    return pImpl->getSessionHistory(session_id);
}

bool SemanticSearchAPI::isHealthy() const {
    return pImpl->isHealthy();
}

// =============================================================================
// Utility Functions
// =============================================================================

namespace query_utils {

std::string normalizeQuery(const std::string& query) {
    std::string normalized = query;

    // Convert to lowercase
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);

    // Normalize whitespace
    std::regex multiple_spaces(R"(\s+)");
    normalized = std::regex_replace(normalized, multiple_spaces, " ");

    // Trim
    normalized.erase(0, normalized.find_first_not_of(" \t\n\r"));
    normalized.erase(normalized.find_last_not_of(" \t\n\r") + 1);

    return normalized;
}

std::vector<std::string> extractKeywords(const std::string& query) {
    std::vector<std::string> keywords;
    std::regex word_regex(R"(\b[a-zA-Z]{2,}\b)");
    auto words_begin = std::sregex_iterator(query.begin(), query.end(), word_regex);
    auto words_end = std::sregex_iterator();

    for (auto it = words_begin; it != words_end; ++it) {
        keywords.push_back(it->str());
    }

    return keywords;
}

std::string detectLanguage(const std::string& query) {
    // Simple language detection - in real implementation would use proper detection library
    if (query.empty()) {
        return "unknown";
    }

    // Count ASCII vs non-ASCII characters
    size_t ascii_count = 0;
    for (char c : query) {
        if (static_cast<unsigned char>(c) < 128) {
            ascii_count++;
        }
    }

    double ascii_ratio = static_cast<double>(ascii_count) / query.size();
    return ascii_ratio > 0.8 ? "en" : "unknown";
}

std::vector<Entity> extractEntities(const std::string& query) {
    // Placeholder entity extraction - in real implementation would use NLP library
    std::vector<Entity> entities;

    // Simple pattern matching for basic entity types
    std::regex email_regex(R"(\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b)");
    std::regex url_regex(R"(https?://[^\s]+)");

    auto emails_begin = std::sregex_iterator(query.begin(), query.end(), email_regex);
    auto emails_end = std::sregex_iterator();
    for (auto it = emails_begin; it != emails_end; ++it) {
        Entity entity;
        entity.text = it->str();
        entity.type = "EMAIL";
        entity.confidence = 0.9f;
        entity.start_pos = it->position();
        entity.end_pos = entity.start_pos + entity.text.length();
        entities.push_back(entity);
    }

    return entities;
}

QueryIntent classifyIntent(const std::string& query) {
    std::string lower_query = query;
    std::transform(lower_query.begin(), lower_query.end(), lower_query.begin(), ::tolower);

    // Simple intent classification based on keywords
    if (lower_query.find("how to") != std::string::npos ||
        lower_query.find("what is") != std::string::npos ||
        lower_query.find("why") != std::string::npos) {
        return QueryIntent::INFORMATIONAL;
    }

    if (lower_query.find("find") != std::string::npos ||
        lower_query.find("locate") != std::string::npos ||
        lower_query.find("where") != std::string::npos) {
        return QueryIntent::NAVIGATIONAL;
    }

    if (lower_query.find("buy") != std::string::npos ||
        lower_query.find("download") != std::string::npos ||
        lower_query.find("purchase") != std::string::npos) {
        return QueryIntent::TRANSACTIONAL;
    }

    if (lower_query.find("vs") != std::string::npos ||
        lower_query.find("versus") != std::string::npos ||
        lower_query.find("compare") != std::string::npos) {
        return QueryIntent::COMPARATIVE;
    }

    return QueryIntent::INFORMATIONAL; // Default
}

bool isValidQuery(const std::string& query) {
    if (query.empty() || query.length() > 1000) {
        return false;
    }

    // Check for only whitespace
    if (query.find_first_not_of(" \t\n\r") == std::string::npos) {
        return false;
    }

    return true;
}

float calculateComplexity(const std::string& query) {
    float complexity = 0.0f;

    // Base complexity from length
    complexity += std::min(1.0f, static_cast<float>(query.length()) / 100.0f);

    // Add complexity for special characters
    for (char c : query) {
        if (!std::isalnum(c) && !std::isspace(c)) {
            complexity += 0.1f;
        }
    }

    // Add complexity for multiple words
    std::regex word_regex(R"(\b\w+\b)");
    auto word_count = std::distance(std::sregex_iterator(query.begin(), query.end(), word_regex),
                                    std::sregex_iterator());

    complexity += std::min(1.0f, static_cast<float>(word_count) / 10.0f);

    return std::min(1.0f, complexity);
}

} // namespace query_utils

namespace result_utils {

std::vector<TextSnippet> generateSnippets(const std::string& content,
                                          const std::vector<std::string>& query_terms,
                                          size_t max_snippets, size_t snippet_length) {
    std::vector<TextSnippet> snippets;

    if (content.empty() || query_terms.empty()) {
        return snippets;
    }

    // Find all occurrences of query terms
    std::vector<size_t> match_positions;
    for (const auto& term : query_terms) {
        size_t pos = 0;
        while ((pos = content.find(term, pos)) != std::string::npos) {
            match_positions.push_back(pos);
            pos += term.length();
        }
    }

    if (match_positions.empty()) {
        // No matches found, return snippet from beginning
        TextSnippet snippet;
        snippet.text = content.substr(0, std::min(snippet_length, content.length()));
        snippet.start_position = 0;
        snippet.end_position = snippet.text.length();
        snippet.relevance_score = 0.1f;
        snippets.push_back(snippet);
        return snippets;
    }

    // Sort match positions
    std::sort(match_positions.begin(), match_positions.end());

    // Generate snippets around matches
    std::set<size_t> used_positions;

    for (size_t match_pos : match_positions) {
        if (snippets.size() >= max_snippets) {
            break;
        }

        // Skip if this position is too close to an existing snippet
        bool too_close = false;
        for (size_t used_pos : used_positions) {
            if (std::abs(static_cast<int>(match_pos) - static_cast<int>(used_pos)) <
                static_cast<int>(snippet_length / 2)) {
                too_close = true;
                break;
            }
        }

        if (too_close) {
            continue;
        }

        // Create snippet centered on match
        size_t half_length = snippet_length / 2;
        size_t start = match_pos > half_length ? match_pos - half_length : 0;
        size_t end = std::min(start + snippet_length, content.length());

        // Adjust to word boundaries
        while (start > 0 && !std::isspace(content[start - 1])) {
            start--;
        }
        while (end < content.length() && !std::isspace(content[end])) {
            end++;
        }

        TextSnippet snippet;
        snippet.text = content.substr(start, end - start);
        snippet.start_position = start;
        snippet.end_position = end;
        snippet.relevance_score = 1.0f;

        // Mark highlighted ranges
        for (const auto& term : query_terms) {
            size_t term_pos = 0;
            while ((term_pos = snippet.text.find(term, term_pos)) != std::string::npos) {
                snippet.highlighted_ranges.emplace_back(term_pos, term_pos + term.length());
                term_pos += term.length();
            }
        }

        snippets.push_back(snippet);
        used_positions.insert(match_pos);
    }

    return snippets;
}

std::string highlightTerms(const std::string& text, const std::vector<std::string>& terms,
                           const std::string& start_tag, const std::string& end_tag) {
    if (text.empty() || terms.empty()) {
        return text;
    }

    std::string result = text;

    // Sort terms by length (longest first) to avoid partial replacements
    std::vector<std::string> sorted_terms = terms;
    std::sort(sorted_terms.begin(), sorted_terms.end(),
              [](const std::string& a, const std::string& b) { return a.length() > b.length(); });

    for (const auto& term : sorted_terms) {
        std::regex term_regex(
            "\\b" + std::regex_replace(term, std::regex("[.*+?^${}()|[\\]\\\\]"), "\\$&") + "\\b",
            std::regex_constants::icase);
        result = std::regex_replace(result, term_regex, start_tag + "$&" + end_tag);
    }

    return result;
}

std::string generateSummary(const std::string& content, size_t max_length) {
    if (content.length() <= max_length) {
        return content;
    }

    // Find the best place to cut (prefer sentence boundaries)
    size_t cut_pos = max_length;
    while (cut_pos > max_length / 2 && content[cut_pos] != '.' && content[cut_pos] != '!' &&
           content[cut_pos] != '?') {
        cut_pos--;
    }

    if (cut_pos <= max_length / 2) {
        cut_pos = max_length;
    }

    std::string summary = content.substr(0, cut_pos);
    if (cut_pos < content.length()) {
        summary += "...";
    }

    return summary;
}

} // namespace result_utils

} // namespace yams::api