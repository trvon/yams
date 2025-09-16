# YAMS Search User Guide

This guide provides comprehensive instructions for using the YAMS search system effectively.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Basic Search](#basic-search)
3. [Advanced Query Syntax](#advanced-query-syntax)
4. [Search Filters](#search-filters)
5. [Search Results](#search-results)
6. [Performance Tips](#performance-tips)
7. [Common Use Cases](#common-use-cases)
8. [FAQ](#faq)

## Getting Started

The YAMS search system provides powerful full-text search capabilities for your document collection. It supports complex queries, filters, faceting, and result ranking to help you find exactly what you're looking for.

### Quick Start

```cpp
#include <yams/search/search_executor.h>

// Initialize search system
auto database = std::make_shared<Database>("search.db");
auto repository = std::make_shared<MetadataRepository>(database);
auto indexer = std::make_shared<DocumentIndexer>(database);
auto parser = std::make_shared<QueryParser>();

SearchExecutor executor(database, repository, indexer, parser);

// Perform a simple search
SearchRequest request;
request.query = "machine learning";
request.maxResults = 10;

auto result = executor.search(request);
```

## Basic Search

### Simple Text Search

The most basic search is a single word or phrase:

```cpp
SearchRequest request;
request.query = "database";  // Find documents containing "database"

// Or search for multiple words
request.query = "machine learning";  // Documents with both words
```

### Phrase Search

Use quotes to search for exact phrases:

```cpp
SearchRequest request;
request.query = "\"machine learning algorithms\"";  // Exact phrase
```

### Case Sensitivity

Searches are case-insensitive by default:

```cpp
// These are equivalent
request.query = "Database";
request.query = "database";
request.query = "DATABASE";
```

## Advanced Query Syntax

The YAMS search system supports a rich query language for complex searches.

### Boolean Operators

#### AND Operator

Find documents containing all terms:

```cpp
request.query = "database AND performance";
request.query = "machine AND learning AND algorithms";
```

#### OR Operator

Find documents containing any of the terms:

```cpp
request.query = "database OR storage";
request.query = "python OR java OR cpp";
```

#### NOT Operator

Exclude documents containing specific terms:

```cpp
request.query = "database NOT legacy";
request.query = "programming NOT javascript";
```

#### Combining Operators

Use parentheses to control operator precedence:

```cpp
request.query = "(database OR storage) AND performance";
request.query = "machine AND (learning OR intelligence) NOT artificial";
```

### Field-Specific Searches

Search within specific document fields:

```cpp
// Search in title field
request.query = "title:performance";

// Search in author field
request.query = "author:smith";

// Search in content field
request.query = "content:\"data structures\"";

// Combine field searches
request.query = "title:database AND author:johnson";
```

### Wildcards

#### Asterisk (*) - Multiple Characters

```cpp
request.query = "data*";        // Matches: data, database, dataset, etc.
request.query = "*base";        // Matches: database, codebase, etc.
request.query = "data*base";    // Matches: database
```

#### Question Mark (?) - Single Character

```cpp
request.query = "stor?ge";      // Matches: storage
request.query = "colo?r";       // Matches: color, colour
```

### Range Queries

Search for values within a range:

#### Numeric Ranges

```cpp
request.query = "size:[1000 TO 50000]";     // Size between 1KB and 50KB
request.query = "pages:[10 TO *]";          // 10 or more pages
request.query = "score:[* TO 95]";          // Score up to 95
```

#### Date Ranges

```cpp
request.query = "date:[2020-01-01 TO 2023-12-31]";
request.query = "modified:[2023-01-01 TO *]";
```

### Proximity Queries

Find terms within a specified distance:

```cpp
request.query = "\"machine learning\"~10";  // Words within 10 positions
request.query = "\"database\"~5\"performance\"";  // Specific distance
```

### Fuzzy Matching

Find similar terms with edit distance:

```cpp
request.query = "algoritm~";     // Finds "algorithm" (1 edit)
request.query = "databse~2";     // Finds "database" (up to 2 edits)
```

## Search Filters

Filters allow you to narrow results based on document metadata.

### Date Range Filter

Filter by document creation or modification dates:

```cpp
SearchRequest request;
request.query = "database";
request.filters = std::make_unique<SearchFilters>();

// Last 30 days
auto now = std::chrono::system_clock::now();
auto thirtyDaysAgo = now - std::chrono::hours(24 * 30);

request.filters->dateFilter = DateRangeFilter();
request.filters->dateFilter->from = thirtyDaysAgo;
request.filters->dateFilter->to = now;
```

### Size Filter

Filter by file size:

```cpp
request.filters->sizeFilter = SizeFilter();
request.filters->sizeFilter->minSize = 1024;        // At least 1KB
request.filters->sizeFilter->maxSize = 1024 * 1024; // At most 1MB
```

### Content Type Filter

Filter by file type or MIME type:

```cpp
request.filters->contentTypeFilter = ContentTypeFilter();
request.filters->contentTypeFilter->types = {
    "text/plain",
    "application/pdf",
    "text/markdown"
};
```

### Path Filter

Filter by file path patterns:

```cpp
request.filters->pathFilter = PathFilter();
request.filters->pathFilter->patterns = {
    "*/docs/*",          // Files in docs directories
    "*.cpp",             // C++ source files
    "/project/src/*"     // Files in specific path
};

// Exclude patterns
request.filters->pathFilter->excludePatterns = {
    "*/temp/*",          // Exclude temp directories
    "*.tmp"              // Exclude temporary files
};
```

## Search Results

### Understanding Results

Search results include detailed information about each match:

```cpp
auto result = executor.search(request);
if (result.isSuccess()) {
    auto response = result.value();
    
    std::cout << "Total results: " << response.totalResults << std::endl;
    std::cout << "Query time: " << response.queryTime.count() << "ms" << std::endl;
    
    for (const auto& item : response.results) {
        std::cout << "Title: " << item.title << std::endl;
        std::cout << "Path: " << item.path << std::endl;
        std::cout << "Score: " << item.score << std::endl;
        std::cout << "Snippet: " << item.snippet << std::endl;
        
        // Show highlights
        for (const auto& highlight : item.highlights) {
            std::cout << "Highlight: \"" << highlight.text << "\"" << std::endl;
        }
    }
}
```

### Pagination

Handle large result sets with pagination:

```cpp
SearchRequest request;
request.query = "database";
request.offset = 0;      // Start from first result
request.maxResults = 20; // Get 20 results per page

// Get next page
request.offset = 20;     // Next 20 results
```

### Sorting Results

Results are automatically sorted by relevance score (highest first). The score represents how well the document matches your query.

### Result Highlighting

Highlights show where your search terms appear in the document:

```cpp
for (const auto& item : response.results) {
    for (const auto& highlight : item.highlights) {
        std::cout << "Found \"" << highlight.text << "\" in " 
                  << highlight.field << " at position " 
                  << highlight.start << "-" << highlight.end << std::endl;
    }
}
```

## Performance Tips

### Optimize Your Queries

1. **Use Specific Terms**: More specific terms return better results faster
2. **Limit Result Count**: Request only the results you need
3. **Use Filters**: Filters are processed efficiently and reduce result set size
4. **Avoid Leading Wildcards**: `*term` is slower than `term*`

### Query Examples by Performance

**Fast Queries:**
```cpp
request.query = "machine learning";           // Specific terms
request.query = "title:performance";          // Field-specific
request.query = "database AND optimization";  // AND operations
```

**Slower Queries:**
```cpp
request.query = "*";                          // Match everything
request.query = "a*";                         // Very common prefixes
request.query = "NOT common";                 // Pure NOT queries
```

### Use Caching

Enable result caching for frequently accessed queries:

```cpp
SearchCacheConfig cacheConfig;
cacheConfig.maxEntries = 1000;
cacheConfig.defaultTTL = std::chrono::minutes(10);

SearchCache cache(cacheConfig);

// Cache results automatically improve performance for repeated queries
```

### Batch Operations

Process multiple searches efficiently:

```cpp
std::vector<SearchRequest> requests = {
    // ... multiple requests
};

for (const auto& request : requests) {
    auto result = executor.search(request);
    // Process result
}
```

## Common Use Cases

### 1. Document Discovery

Find documents by content or metadata:

```cpp
// Find all documentation
request.query = "README OR documentation OR manual";
request.filters->contentTypeFilter->types = {"text/markdown", "text/plain"};

// Find recent presentations
request.query = "presentation OR slides";
request.filters->dateFilter->from = last_month;
request.filters->contentTypeFilter->types = {"application/pdf", "application/pptx"};
```

### 2. Code Search

Search through source code:

```cpp
// Find function definitions
request.query = "function AND definition";
request.filters->contentTypeFilter->types = {"text/x-c++src", "text/javascript"};

// Find error handling code
request.query = "try AND catch OR error AND handling";
request.filters->pathFilter->patterns = {"*.cpp", "*.js"};
```

### 3. Research and Analysis

Find research papers or analysis documents:

```cpp
// Academic papers
request.query = "(analysis OR research OR study) AND (machine learning OR AI)";
request.filters->contentTypeFilter->types = {"application/pdf"};

// Data analysis results
request.query = "results AND (statistics OR metrics OR performance)";
request.filters->dateFilter->from = this_year;
```

### 4. Project Management

Find project-related documents:

```cpp
// Meeting notes
request.query = "meeting OR minutes OR discussion";
request.filters->pathFilter->patterns = {"*/meetings/*", "*/notes/*"};

// Project status documents
request.query = "status OR progress OR milestone";
request.filters->dateFilter->from = last_week;
```

## FAQ

### Q: Why am I not finding documents I know exist?

**A:** Check these common issues:
- Ensure the document has been indexed (recent changes may not be indexed yet)
- Try different search terms or use wildcards
- Check if filters are too restrictive
- Verify the document contains the exact terms you're searching for

### Q: How do I search for documents modified recently?

**A:** Use a date filter:
```cpp
auto now = std::chrono::system_clock::now();
auto lastWeek = now - std::chrono::hours(24 * 7);

request.filters->dateFilter = DateRangeFilter();
request.filters->dateFilter->from = lastWeek;
```

### Q: Can I search within specific directories?

**A:** Yes, use path filters:
```cpp
request.filters->pathFilter = PathFilter();
request.filters->pathFilter->patterns = {"*/project/docs/*"};
```

### Q: How do I exclude certain types of files?

**A:** Use exclude patterns in path filter or content type filter:
```cpp
// Exclude by path
request.filters->pathFilter->excludePatterns = {"*.tmp", "*/temp/*"};

// Exclude by content type
request.filters->contentTypeFilter->exclude = true;
request.filters->contentTypeFilter->types = {"application/octet-stream"};
```

### Q: My searches are slow. How can I improve performance?

**A:** Try these optimizations:
1. Use more specific search terms
2. Add appropriate filters to reduce result set size
3. Limit the number of results with `maxResults`
4. Avoid leading wildcards (`*term`)
5. Enable caching for frequently used queries

### Q: How do I search for exact file names?

**A:** Use a path or filename-specific query:
```cpp
request.query = "path:\"exact_filename.txt\"";
// or
request.query = "filename:\"exact_filename.txt\"";
```

### Q: Can I get suggestions for partial queries?

**A:** Yes, use the suggestions API:
```cpp
auto suggestions = executor.getSuggestions("datab", 5);
if (suggestions.isSuccess()) {
    for (const auto& suggestion : suggestions.value()) {
        std::cout << "Suggestion: " << suggestion << std::endl;
    }
}
```

### Q: How do I handle special characters in queries?

**A:** Escape special characters or use phrase queries:
```cpp
// Use quotes for phrases with special characters
request.query = "\"C++ programming\"";

// Or escape individual characters
request.query = "C\\+\\+ programming";
```

## Next Steps

- Learn about [Performance Tuning](../admin/performance_tuning.md) for production deployments
- See [Configuration Guide](../admin/configuration.md) for system setup
- Check [Troubleshooting Guide](../troubleshooting/search_issues.md) if you encounter issues
- Explore [Code Examples](../examples/search_examples.md) for implementation details
