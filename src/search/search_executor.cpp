#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <regex>
#include <sstream>
#include <yams/search/parallel_post_processor.hpp>
#include <yams/search/search_executor.h>

namespace yams::search {

SearchExecutor::SearchExecutor(std::shared_ptr<metadata::Database> database,
                               std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                               const SearchConfig& config)
    : database_(database), metadataRepo_(metadataRepo), config_(config) {
    queryParser_ = std::make_unique<QueryParser>();
    ranker_ = std::make_unique<ResultRanker>(config.rankingConfig);
}

Result<SearchResults> SearchExecutor::search(const SearchRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();

    if (request.query.empty()) {
        return Error{ErrorCode::InvalidArgument, "Query must not be empty"};
    }

    totalSearches_.fetch_add(1, std::memory_order_relaxed);
    queuedSearches_.fetch_add(1, std::memory_order_relaxed);
    concurrencyLimiter_.acquire();
    queuedSearches_.fetch_sub(1, std::memory_order_relaxed);
    activeSearches_.fetch_add(1, std::memory_order_relaxed);
    auto concurrencyStart = std::chrono::steady_clock::now();
    bool releaseCalled = false;
    auto release = [&](bool recordLatency = true) {
        if (releaseCalled)
            return;
        if (recordLatency) {
            const auto end = std::chrono::steady_clock::now();
            const auto micros =
                std::chrono::duration_cast<std::chrono::microseconds>(end - concurrencyStart)
                    .count();
            if (micros > 0) {
                totalLatencyUs_.fetch_add(static_cast<std::uint64_t>(micros),
                                          std::memory_order_relaxed);
            }
        }
        activeSearches_.fetch_sub(1, std::memory_order_relaxed);
        concurrencyLimiter_.release();
        releaseCalled = true;
    };

    // Check cache first
    std::string cacheKey = generateCacheKey(request);
    if (config_.enableQueryCache) {
        auto cachedResult = getCachedResult(cacheKey);
        if (cachedResult) {
            updateStatistics(std::chrono::milliseconds(0), std::chrono::milliseconds(0), true);
            totalCacheHits_.fetch_add(1, std::memory_order_relaxed);
            release();
            return *cachedResult;
        }
    }

    totalCacheMisses_.fetch_add(1, std::memory_order_relaxed);

    SearchResults response;
    auto& stats = response.getStatistics();
    stats.originalQuery = request.query;
    // Note: offset and limit are not stored in SearchResults

    // Parse query
    auto parseStartTime = std::chrono::high_resolution_clock::now();
    QueryParser::ParseOptions parseOptions;
    parseOptions.literalText = request.literalText;
    auto parseResult = queryParser_->parse(request.query, parseOptions);
    auto parseEndTime = std::chrono::high_resolution_clock::now();
    stats.queryTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(parseEndTime - parseStartTime);

    if (!parseResult) {
        auto err =
            createErrorResponse("Query parsing failed: " + parseResult.error().message, request);
        release();
        return err;
    }

    auto& queryAst = parseResult.value();

    // Convert to FTS5 query
    std::string ftsQuery = queryParser_->toFTS5Query(queryAst.get());
    // processedQuery is not a member of SearchResults, store in stats instead

    // Execute search
    auto searchStartTime = std::chrono::high_resolution_clock::now();
    auto searchResult = executeFTSQuery(ftsQuery, request);
    auto searchEndTime = std::chrono::high_resolution_clock::now();
    stats.searchTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(searchEndTime - searchStartTime);

    if (!searchResult) {
        auto err = createErrorResponse("Search execution failed: " + searchResult.error().message,
                                       request);
        release();
        return err;
    }

    auto results = std::move(searchResult.value());
    stats.totalResults = results.size();

    // Use ParallelPostProcessor for filtering, faceting, highlights, and snippets
    auto postProcessResult = ParallelPostProcessor::process(
        std::move(results), request.filters.hasFilters() ? &request.filters : nullptr,
        (request.includeFacets && config_.enableFaceting) ? request.facetFields
                                                          : std::vector<std::string>{},
        queryAst.get(),
        (request.includeSnippets && config_.enableSnippets) ? config_.snippetLength : 0,
        config_.maxHighlights);

    // Extract processed results and facets
    results = std::move(postProcessResult.filteredResults);

    // Add facets to response
    for (auto& facet : postProcessResult.facets) {
        response.addFacet(std::move(facet));
    }

    // Rank results
    auto rankStartTime = std::chrono::high_resolution_clock::now();
    ranker_->rankResults(results, queryAst.get());
    auto rankEndTime = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] auto rankingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(rankEndTime - rankStartTime);

    // Sort results if requested
    if (request.sortOrder != SearchConfig::SortOrder::Relevance) {
        sortResults(results, request.sortOrder);
    }

    // Apply pagination
    // Note: SearchResults doesn't have pagination fields
    // bool hasPreviousPage = request.offset > 0;
    // bool hasNextPage = (request.offset + request.limit) < results.size();

    // Paginate results
    size_t totalBeforePagination = results.size();
    if (request.offset < results.size()) {
        size_t endPos = std::min(request.offset + request.limit, results.size());
        std::vector<SearchResultItem> paginatedResults(results.begin() + request.offset,
                                                       results.begin() + endPos);
        results = std::move(paginatedResults);
    } else {
        results.clear();
    }

    // Add results to response
    for (auto& result : results) {
        response.addItem(std::move(result));
    }

    // Set statistics (stats already declared at beginning of function)
    stats.totalResults = totalBeforePagination;
    stats.returnedResults = response.getItems().size();
    stats.originalQuery = request.query;
    stats.executedQuery = ftsQuery;

    auto endTime = std::chrono::high_resolution_clock::now();
    stats.totalTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    response.setStatistics(stats);

    // Cache result
    if (config_.enableQueryCache) {
        cacheResult(cacheKey, response);
    }

    // Update statistics
    updateStatistics(stats.searchTime + std::chrono::milliseconds(0), std::chrono::milliseconds(0),
                     false);

    release();
    return response;
}

Result<SearchResults> SearchExecutor::search(const std::string& query, size_t offset,
                                             size_t limit) {
    SearchRequest request;
    request.query = query;
    request.offset = offset;
    request.limit = limit;
    return search(request);
}

Result<std::vector<std::string>> SearchExecutor::getSuggestions(const std::string& partialQuery,
                                                                size_t maxSuggestions) {
    // Simplified suggestion implementation - would need more sophisticated logic
    std::vector<std::string> suggestions;

    // Basic approach: find similar terms in the index
    try {
        // Query for similar terms using FTS5's term suggestion capabilities
        std::string sql = R"(
            SELECT DISTINCT term FROM documents_fts_vocab
            WHERE term LIKE ? || '%'
            ORDER BY term
            LIMIT ?
        )";

        auto stmtResult = database_->prepare(sql);
        if (!stmtResult) {
            return Error{ErrorCode::DatabaseError, "Failed to prepare suggestion query"};
        }

        auto stmt = std::move(stmtResult).value();

        auto bindPrefix = stmt.bind(1, partialQuery);
        if (!bindPrefix) {
            return Error{bindPrefix.error()};
        }

        auto bindLimit = stmt.bind(2, static_cast<int>(maxSuggestions));
        if (!bindLimit) {
            return Error{bindLimit.error()};
        }

        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) {
                return Error{stepResult.error()};
            }

            if (!stepResult.value()) {
                break;
            }

            suggestions.push_back(stmt.getString(0));
        }

    } catch (const std::exception& e) {
        return Error{ErrorCode::DatabaseError,
                     "Failed to get suggestions: " + std::string(e.what())};
    }

    return suggestions;
}

Result<std::vector<SearchFacet>>
SearchExecutor::getFacets(const std::string& query, const std::vector<std::string>& facetFields) {
    // Execute search to get base results
    auto searchResult = search(query, 0, config_.maxResults);
    if (!searchResult) {
        return Error{searchResult.error().code, searchResult.error().message};
    }

    auto facets = generateFacets(searchResult.value().getItems(), facetFields);
    return facets;
}

void SearchExecutor::clearCache() {
    queryCache_.clear();
    cacheOrder_.clear();
}

Result<std::vector<SearchResultItem>>
SearchExecutor::executeFTSQuery(const std::string& ftsQuery, const SearchRequest& request) {
    (void)request;
    std::vector<SearchResultItem> results;

    try {
        // Execute FTS5 query
        std::string sql = R"(
            SELECT 
                d.id,
                d.file_name AS title,
                d.file_path AS path,
                d.mime_type AS content_type,
                d.file_size,
                d.modified_time,
                d.indexed_time,
                '' AS detected_language,
                0.0 AS language_confidence,
                bm25(documents_fts) as relevance_score,
                snippet(documents_fts, 1, '<mark>', '</mark>', '...', 32) as snippet
            FROM documents_fts 
            JOIN documents d ON documents_fts.rowid = d.id
            WHERE documents_fts MATCH ?
            ORDER BY relevance_score DESC
            LIMIT ?
        )";

        auto stmtResult = database_->prepare(sql);
        if (!stmtResult) {
            return Error{ErrorCode::DatabaseError, "Failed to prepare FTS query"};
        }

        auto stmt = std::move(stmtResult).value();
        auto bindQuery = stmt.bind(1, ftsQuery);
        if (!bindQuery) {
            return Error{bindQuery.error()};
        }

        auto bindLimit = stmt.bind(2, static_cast<int>(config_.maxResults));
        if (!bindLimit) {
            return Error{bindLimit.error()};
        }

        while (true) {
            auto stepResult = stmt.step();
            if (!stepResult) {
                return Error{stepResult.error()};
            }
            if (!stepResult.value()) {
                break;
            }

            SearchResultItem item;

            item.documentId = stmt.getInt64(0);
            item.title = stmt.getString(1);
            item.path = stmt.getString(2);
            item.contentType = stmt.getString(3);
            item.fileSize = static_cast<size_t>(stmt.getInt64(4));

            auto modifiedSeconds = stmt.isNull(5) ? int64_t{0} : stmt.getInt64(5);
            if (modifiedSeconds > 0) {
                item.lastModified =
                    std::chrono::system_clock::time_point{std::chrono::seconds{modifiedSeconds}};
            } else {
                item.lastModified = std::chrono::system_clock::now();
            }

            auto indexedSeconds = stmt.isNull(6) ? int64_t{0} : stmt.getInt64(6);
            if (indexedSeconds > 0) {
                item.indexedAt =
                    std::chrono::system_clock::time_point{std::chrono::seconds{indexedSeconds}};
            } else {
                item.indexedAt = std::chrono::system_clock::now();
            }

            item.detectedLanguage = stmt.getString(7);
            item.languageConfidence = static_cast<float>(stmt.getDouble(8));
            item.relevanceScore = static_cast<float>(stmt.getDouble(9));
            item.contentPreview = stmt.getString(10);

            // Basic term frequency calculation (would be more sophisticated in practice)
            item.termFrequency = 1.0f; // TODO: Calculate actual TF from content

            results.push_back(std::move(item));
        }

    } catch (const std::exception& e) {
        return Error{ErrorCode::DatabaseError,
                     "FTS query execution failed: " + std::string(e.what())};
    }

    return results;
}

void SearchExecutor::generateHighlights(std::vector<SearchResultItem>& results,
                                        const QueryNode* queryAst) {
    auto queryTerms = ranker_->extractQueryTerms(queryAst);

    for (auto& result : results) {
        // Generate highlights for content preview
        if (!result.contentPreview.empty()) {
            SearchHighlight highlight;
            highlight.field = "content";
            highlight.snippet = result.contentPreview;
            highlight.startOffset = 0;
            highlight.endOffset = result.contentPreview.length();

            // Find term positions (simplified)
            for (const auto& term : queryTerms) {
                size_t pos = result.contentPreview.find(term);
                if (pos != std::string::npos) {
                    highlight.highlights.emplace_back(pos, pos + term.length());
                }
            }

            if (!highlight.highlights.empty()) {
                result.highlights.push_back(std::move(highlight));
            }
        }

        // Generate highlights for title
        for (const auto& term : queryTerms) {
            size_t pos = result.title.find(term);
            if (pos != std::string::npos) {
                SearchHighlight titleHighlight;
                titleHighlight.field = "title";
                titleHighlight.snippet = result.title;
                titleHighlight.startOffset = 0;
                titleHighlight.endOffset = result.title.length();
                titleHighlight.highlights.emplace_back(pos, pos + term.length());

                result.highlights.push_back(std::move(titleHighlight));
                break; // Only one title highlight
            }
        }

        // Limit highlights per result
        if (result.highlights.size() > config_.maxHighlights) {
            result.highlights.resize(config_.maxHighlights);
        }
    }
}

std::vector<SearchFacet>
SearchExecutor::generateFacets(const std::vector<SearchResultItem>& results,
                               const std::vector<std::string>& facetFields) {
    std::vector<SearchFacet> facets;

    for (const auto& fieldName : facetFields) {
        SearchFacet facet;
        facet.name = fieldName;
        facet.displayName = fieldName; // Could be improved with proper display names

        std::unordered_map<std::string, size_t> valueCounts;

        // Count values for this facet field
        for (const auto& result : results) {
            std::string value;

            if (fieldName == "contentType") {
                value = result.contentType;
            } else if (fieldName == "language") {
                value = result.detectedLanguage;
            } else {
                // Check metadata
                auto it = result.metadata.find(fieldName);
                if (it != result.metadata.end()) {
                    value = it->second;
                }
            }

            if (!value.empty()) {
                valueCounts[value]++;
            }
        }

        // Convert to facet values
        for (const auto& [value, count] : valueCounts) {
            SearchFacet::FacetValue facetValue;
            facetValue.value = value;
            facetValue.display = value; // Could be improved with proper display formatting
            facetValue.count = count;

            facet.values.push_back(std::move(facetValue));
        }

        // Sort facet values by count (descending)
        std::sort(facet.values.begin(), facet.values.end(),
                  [](const SearchFacet::FacetValue& a, const SearchFacet::FacetValue& b) {
                      return a.count > b.count;
                  });

        facet.totalValues = facet.values.size();

        if (!facet.values.empty()) {
            facets.push_back(std::move(facet));
        }
    }

    return facets;
}

void SearchExecutor::generateSnippets(std::vector<SearchResultItem>& results) {
    for (auto& result : results) {
        if (result.contentPreview.length() > config_.snippetLength) {
            result.contentPreview = result.contentPreview.substr(0, config_.snippetLength) + "...";
        }
        result.previewLength = result.contentPreview.length();
    }
}

std::vector<SearchResultItem>
SearchExecutor::applyFilters(const std::vector<SearchResultItem>& results,
                             const SearchFilters& filters) {
    return filters.apply(results);
}

void SearchExecutor::sortResults(std::vector<SearchResultItem>& results,
                                 SearchConfig::SortOrder sortOrder) {
    switch (sortOrder) {
        case SearchConfig::SortOrder::Relevance:
            // Already sorted by relevance from ranking
            break;

        case SearchConfig::SortOrder::DateDesc:
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.lastModified > b.lastModified;
                      });
            break;

        case SearchConfig::SortOrder::DateAsc:
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.lastModified < b.lastModified;
                      });
            break;

        case SearchConfig::SortOrder::TitleAsc:
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.title < b.title;
                      });
            break;

        case SearchConfig::SortOrder::TitleDesc:
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.title > b.title;
                      });
            break;

        case SearchConfig::SortOrder::SizeAsc:
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.fileSize < b.fileSize;
                      });
            break;

        case SearchConfig::SortOrder::SizeDesc:
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.fileSize > b.fileSize;
                      });
            break;
    }
}

void SearchExecutor::cacheResult(const std::string& cacheKey, const SearchResults& response) const {
    if (queryCache_.size() >= config_.cacheSize) {
        evictOldestCacheEntry();
    }

    queryCache_[cacheKey] = response;
    cacheOrder_.push_back(cacheKey);
}

std::optional<SearchResults> SearchExecutor::getCachedResult(const std::string& cacheKey) const {
    auto it = queryCache_.find(cacheKey);
    if (it != queryCache_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::string SearchExecutor::generateCacheKey(const SearchRequest& request) const {
    std::ostringstream oss;
    oss << request.query << "|" << request.offset << "|" << request.limit << "|"
        << static_cast<int>(request.sortOrder) << "|" << request.includeHighlights << "|"
        << request.includeFacets << "|lt=" << (request.literalText ? 1 : 0);

    // Include filter information in cache key
    if (request.filters.hasFilters()) {
        oss << "|filters:" << request.filters.getFilterCount();
    }

    return oss.str();
}

void SearchExecutor::evictOldestCacheEntry() const {
    if (!cacheOrder_.empty()) {
        std::string oldestKey = cacheOrder_.front();
        cacheOrder_.erase(cacheOrder_.begin());
        queryCache_.erase(oldestKey);
    }
}

void SearchExecutor::ConcurrencyLimiter::set_limit(std::uint32_t limit) {
    std::lock_guard<std::mutex> lock(mutex_);
    limit_ = (limit == 0) ? kUnlimited : limit;
    cv_.notify_all();
}

void SearchExecutor::ConcurrencyLimiter::acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return limit_ == kUnlimited || inFlight_ < limit_; });
    ++inFlight_;
}

void SearchExecutor::ConcurrencyLimiter::release() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (inFlight_ > 0) {
        --inFlight_;
        cv_.notify_one();
    }
}

std::uint32_t SearchExecutor::ConcurrencyLimiter::limit() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return limit_ == kUnlimited ? 0 : limit_;
}

std::uint32_t SearchExecutor::ConcurrencyLimiter::in_flight() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return inFlight_;
}

SearchExecutor::LoadMetrics SearchExecutor::getLoadMetrics() const {
    LoadMetrics snapshot;
    snapshot.active = activeSearches_.load(std::memory_order_relaxed);
    snapshot.queued = queuedSearches_.load(std::memory_order_relaxed);
    snapshot.executed = totalSearches_.load(std::memory_order_relaxed);
    snapshot.cacheHits = totalCacheHits_.load(std::memory_order_relaxed);
    snapshot.cacheMisses = totalCacheMisses_.load(std::memory_order_relaxed);
    const auto executed = snapshot.executed;
    snapshot.avgLatencyUs =
        executed > 0 ? totalLatencyUs_.load(std::memory_order_relaxed) / executed : 0;
    snapshot.concurrencyLimit = concurrencyLimiter_.limit();
    return snapshot;
}

void SearchExecutor::setConcurrencyLimit(std::uint32_t limit) {
    concurrencyLimiter_.set_limit(limit);
}

SearchResults SearchExecutor::createErrorResponse(const std::string& /*error*/,
                                                  const SearchRequest& request) const {
    SearchResults response;
    auto& errorStats = response.getStatistics();
    errorStats.originalQuery = request.query;
    // Note: SearchResults doesn't have hasError/errorMessage fields
    // Will need to handle errors differently

    stats_.errorCount++;

    return response;
}

void SearchExecutor::updateStatistics(const std::chrono::milliseconds& searchTime,
                                      const std::chrono::milliseconds& rankingTime,
                                      bool cacheHit) const {
    stats_.totalSearches++;

    if (cacheHit) {
        stats_.cacheHits++;
    } else {
        stats_.cacheMisses++;
    }

    // Update timing statistics
    auto totalTime = searchTime + rankingTime;
    stats_.maxSearchTime = std::max(stats_.maxSearchTime, totalTime);

    // Calculate running average (simplified)
    auto totalMs = stats_.avgSearchTime.count() * (stats_.totalSearches - 1) + totalTime.count();
    stats_.avgSearchTime = std::chrono::milliseconds(totalMs / stats_.totalSearches);

    auto rankMs = stats_.avgRankingTime.count() * (stats_.totalSearches - 1) + rankingTime.count();
    stats_.avgRankingTime = std::chrono::milliseconds(rankMs / stats_.totalSearches);
}

// SearchExecutorFactory implementation

std::unique_ptr<SearchExecutor>
SearchExecutorFactory::create(std::shared_ptr<metadata::Database> database,
                              std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    return std::make_unique<SearchExecutor>(database, metadataRepo);
}

std::unique_ptr<SearchExecutor>
SearchExecutorFactory::create(std::shared_ptr<metadata::Database> database,
                              std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                              const SearchConfig& config) {
    return std::make_unique<SearchExecutor>(database, metadataRepo, config);
}

std::unique_ptr<SearchExecutor> SearchExecutorFactory::createHighPerformance(
    std::shared_ptr<metadata::Database> database,
    std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    SearchConfig config;
    config.maxResults = 10000;
    config.defaultPageSize = 50;
    config.enableQueryCache = true;
    config.cacheSize = 5000;
    config.timeout = std::chrono::milliseconds(60000);

    return std::make_unique<SearchExecutor>(database, metadataRepo, config);
}

} // namespace yams::search
