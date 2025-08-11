# YAMS Search API Reference

This document provides a comprehensive reference for the YAMS search system APIs.

## Table of Contents

1. [Search Executor API](#search-executor-api)
2. [Query Parser API](#query-parser-api)
3. [Search Cache API](#search-cache-api)
4. [Search Filters API](#search-filters-api)
5. [Result Types](#result-types)
6. [Error Handling](#error-handling)
7. [Configuration](#configuration)
8. [Examples](#examples)

## Search Executor API

The `SearchExecutor` class is the main interface for executing search queries.

### Class: `SearchExecutor`

```cpp
class SearchExecutor {
public:
    SearchExecutor(std::shared_ptr<Database> database,
                   std::shared_ptr<MetadataRepository> repository,
                   std::shared_ptr<DocumentIndexer> indexer,
                   std::shared_ptr<QueryParser> parser);
    
    Result<SearchResponse> search(const SearchRequest& request);
    Result<std::vector<std::string>> getSuggestions(const std::string& partialQuery, 
                                                     size_t maxSuggestions = 10);
};
```

#### Methods

##### `search(const SearchRequest& request)`

Executes a search query and returns results.

**Parameters:**
- `request`: Search request containing query, filters, and options

**Returns:**
- `Result<SearchResponse>`: Search results or error

**Example:**
```cpp
SearchRequest request;
request.query = "database performance";
request.maxResults = 20;
request.offset = 0;

auto result = executor->search(request);
if (result.isSuccess()) {
    auto response = result.value();
    std::cout << "Found " << response.totalResults << " results\n";
}
```

##### `getSuggestions(const std::string& partialQuery, size_t maxSuggestions)`

Get query suggestions based on partial input.

**Parameters:**
- `partialQuery`: Partial query string
- `maxSuggestions`: Maximum number of suggestions to return

**Returns:**
- `Result<std::vector<std::string>>`: List of suggestions or error

**Example:**
```cpp
auto suggestions = executor->getSuggestions("datab", 5);
if (suggestions.isSuccess()) {
    for (const auto& suggestion : suggestions.value()) {
        std::cout << "Suggestion: " << suggestion << "\n";
    }
}
```

## Query Parser API

The `QueryParser` class parses search query strings into abstract syntax trees.

### Class: `QueryParser`

```cpp
class QueryParser {
public:
    Result<std::unique_ptr<QueryNode>> parse(const std::string& query);
    bool validate(const std::string& query);
    std::string getLastError() const;
};
```

#### Methods

##### `parse(const std::string& query)`

Parses a query string into an AST.

**Parameters:**
- `query`: Query string to parse

**Returns:**
- `Result<std::unique_ptr<QueryNode>>`: Parsed query AST or error

**Supported Query Syntax:**
- **Simple terms**: `database`
- **Phrase queries**: `"database system"`
- **Boolean operators**: `database AND performance`, `cache OR memory`
- **Negation**: `database NOT legacy`
- **Field queries**: `title:performance`, `author:smith`
- **Range queries**: `date:[2020 TO 2024]`, `size:>1000`
- **Wildcard queries**: `data*`, `stor?ge`
- **Proximity queries**: `"database"~5"performance"`
- **Grouping**: `(database OR storage) AND performance`

**Example:**
```cpp
QueryParser parser;
auto result = parser.parse("(database OR storage) AND performance NOT legacy");
if (result.isSuccess()) {
    auto ast = result.take_value();
    // Process AST
}
```

## Search Cache API

The `SearchCache` class provides high-performance caching of search results.

### Class: `SearchCache`

```cpp
class SearchCache {
public:
    explicit SearchCache(const SearchCacheConfig& config = {});
    
    std::optional<SearchResponse> get(const CacheKey& key);
    void put(const CacheKey& key, const SearchResponse& response, 
             std::chrono::seconds ttl = std::chrono::seconds{0});
    bool contains(const CacheKey& key) const;
    void invalidate(const CacheKey& key);
    size_t invalidatePattern(const std::string& pattern);
    void clear();
    
    CacheStats getStats() const;
    void resetStats();
};
```

#### Configuration

```cpp
struct SearchCacheConfig {
    size_t maxEntries = 1000;
    size_t maxMemoryMB = 100;
    std::chrono::seconds defaultTTL{300};
    bool enableStatistics = true;
    EvictionPolicy evictionPolicy = EvictionPolicy::LRU;
};
```

#### Methods

##### `get(const CacheKey& key)`

Retrieves cached search results.

**Example:**
```cpp
auto key = CacheKey::fromQuery("database performance", nullptr, 0, 20);
auto cached = cache->get(key);
if (cached.has_value()) {
    auto response = cached.value();
    // Use cached results
}
```

##### `put(const CacheKey& key, const SearchResponse& response, std::chrono::seconds ttl)`

Stores search results in cache.

**Example:**
```cpp
auto key = CacheKey::fromQuery("database performance", nullptr, 0, 20);
cache->put(key, response, std::chrono::seconds{600}); // 10-minute TTL
```

## Search Filters API

Filters allow you to narrow search results based on various criteria.

### Filter Types

#### `DateRangeFilter`

```cpp
struct DateRangeFilter {
    std::optional<std::chrono::system_clock::time_point> from;
    std::optional<std::chrono::system_clock::time_point> to;
};
```

#### `SizeFilter`

```cpp
struct SizeFilter {
    std::optional<size_t> minSize;
    std::optional<size_t> maxSize;
};
```

#### `ContentTypeFilter`

```cpp
struct ContentTypeFilter {
    std::vector<std::string> types;
    bool exclude = false;
};
```

#### `PathFilter`

```cpp
struct PathFilter {
    std::vector<std::string> patterns;
    std::vector<std::string> excludePatterns;
};
```

### Example Usage

```cpp
SearchRequest request;
request.query = "database";
request.filters = std::make_unique<SearchFilters>();

// Date filter
request.filters->dateFilter = DateRangeFilter();
request.filters->dateFilter->from = std::chrono::system_clock::now() - std::chrono::hours(24 * 30);

// Size filter
request.filters->sizeFilter = SizeFilter();
request.filters->sizeFilter->minSize = 1000;
request.filters->sizeFilter->maxSize = 100000;

// Content type filter
request.filters->contentTypeFilter = ContentTypeFilter();
request.filters->contentTypeFilter->types = {"text/plain", "application/pdf"};
```

## Result Types

### `SearchResponse`

```cpp
struct SearchResponse {
    std::vector<SearchResultItem> results;
    size_t totalResults;
    size_t offset;
    size_t limit;
    bool hasMore;
    std::chrono::milliseconds queryTime;
    std::vector<SearchFacet> facets;
    std::vector<SpellCorrection> spellCorrections;
};
```

### `SearchResultItem`

```cpp
struct SearchResultItem {
    DocumentId documentId;
    std::string title;
    std::string snippet;
    std::string path;
    float score;
    std::vector<Highlight> highlights;
    std::unordered_map<std::string, size_t> termFrequencies;
};
```

### `Highlight`

```cpp
struct Highlight {
    size_t start;
    size_t end;
    std::string text;
    std::string field;
};
```

### `SearchFacet`

```cpp
struct SearchFacet {
    std::string field;
    std::vector<FacetValue> values;
};

struct FacetValue {
    std::string value;
    size_t count;
};
```

## Error Handling

All API methods return `Result<T>` types that encapsulate either a successful result or an error.

### Error Codes

```cpp
enum class ErrorCode {
    Success,
    InvalidArgument,
    NotFound,
    DatabaseError,
    ParseError,
    InternalError,
    Timeout
};
```

### Error Handling Pattern

```cpp
auto result = executor->search(request);
if (result.isSuccess()) {
    auto response = result.value();
    // Handle success
} else {
    auto error = result.error();
    std::cerr << "Search failed: " << error.message << std::endl;
    
    switch (error.code) {
        case ErrorCode::ParseError:
            // Handle parse error
            break;
        case ErrorCode::DatabaseError:
            // Handle database error
            break;
        default:
            // Handle other errors
            break;
    }
}
```

## Configuration

### Search Configuration

```cpp
struct SearchConfig {
    size_t maxResults = 1000;
    size_t defaultLimit = 20;
    std::chrono::milliseconds timeout{30000};
    bool enableHighlighting = true;
    bool enableSpellCorrection = false;
    bool enableFaceting = true;
};
```

### Performance Tuning

- **Cache Configuration**: Adjust cache size based on available memory
- **Result Limits**: Set appropriate limits to balance performance and completeness
- **Timeout Settings**: Configure timeouts based on acceptable response times
- **Index Optimization**: Regular index maintenance for optimal performance

## Examples

### Basic Search

```cpp
#include <yams/search/search_executor.h>

using namespace yams::search;

// Create search executor
auto database = std::make_shared<Database>("search.db");
auto repository = std::make_shared<MetadataRepository>(database);
auto indexer = std::make_shared<DocumentIndexer>(database);
auto parser = std::make_shared<QueryParser>();

SearchExecutor executor(database, repository, indexer, parser);

// Execute search
SearchRequest request;
request.query = "database performance optimization";
request.maxResults = 10;

auto result = executor.search(request);
if (result.isSuccess()) {
    auto response = result.value();
    
    std::cout << "Found " << response.totalResults << " results\n";
    
    for (const auto& item : response.results) {
        std::cout << "Title: " << item.title << "\n";
        std::cout << "Score: " << item.score << "\n";
        std::cout << "Snippet: " << item.snippet << "\n\n";
    }
}
```

### Advanced Search with Caching

```cpp
#include <yams/search/search_cache.h>

// Set up cache
SearchCacheConfig cacheConfig;
cacheConfig.maxEntries = 1000;
cacheConfig.maxMemoryMB = 100;
cacheConfig.defaultTTL = std::chrono::minutes(5);

SearchCache cache(cacheConfig);

// Function to search with caching
std::optional<SearchResponse> searchWithCache(const SearchRequest& request) {
    auto cacheKey = CacheKey::fromQuery(
        request.query, 
        request.filters.get(), 
        request.offset, 
        request.maxResults
    );
    
    // Check cache first
    auto cached = cache.get(cacheKey);
    if (cached.has_value()) {
        return cached;
    }
    
    // Execute search
    auto result = executor.search(request);
    if (result.isSuccess()) {
        auto response = result.value();
        
        // Cache the result
        cache.put(cacheKey, response);
        
        return response;
    }
    
    return std::nullopt;
}
```

### Faceted Search

```cpp
SearchRequest request;
request.query = "*"; // Match all
request.maxResults = 0; // Only want facets
request.includeFacets = true;
request.facetFields = {"contentType", "author", "language"};

auto result = executor.search(request);
if (result.isSuccess()) {
    auto response = result.value();
    
    for (const auto& facet : response.facets) {
        std::cout << "Facet: " << facet.field << "\n";
        
        for (const auto& value : facet.values) {
            std::cout << "  " << value.value 
                     << " (" << value.count << ")\n";
        }
    }
}
```

### Complex Boolean Query

```cpp
SearchRequest request;
request.query = R"(
    (database OR storage OR "data management") AND 
    (performance OR optimization OR "query speed") AND 
    NOT (legacy OR deprecated)
)";
request.maxResults = 50;
request.includeHighlights = true;

auto result = executor.search(request);
if (result.isSuccess()) {
    auto response = result.value();
    
    for (const auto& item : response.results) {
        std::cout << "Document: " << item.title << "\n";
        
        // Show highlights
        for (const auto& highlight : item.highlights) {
            std::cout << "Highlight: \"" << highlight.text 
                     << "\" at " << highlight.start 
                     << "-" << highlight.end << "\n";
        }
    }
}
```