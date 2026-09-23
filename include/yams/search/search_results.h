#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>

namespace yams::search {

/**
 * @brief Search result highlight - represents highlighted text snippets
 */
struct SearchHighlight {
    // Default constructors and assignment operators
    SearchHighlight() : startOffset(0), endOffset(0) {}
    SearchHighlight(const SearchHighlight&) = default;
    SearchHighlight(SearchHighlight&&) = default;
    SearchHighlight& operator=(const SearchHighlight&) = default;
    SearchHighlight& operator=(SearchHighlight&&) = default;
    std::string field;                                 // Field name (content, title, etc.)
    std::string snippet;                               // Text snippet with highlights
    size_t startOffset;                                // Start offset in original text
    size_t endOffset;                                  // End offset in original text
    std::vector<std::pair<size_t, size_t>> highlights; // Highlight positions within snippet
};

/**
 * @brief Individual search result item
 */
struct SearchResultItem {
    // Default constructors and assignment operators
    SearchResultItem() : documentId(0), fileSize(0) {}
    SearchResultItem(const SearchResultItem&) = default;
    SearchResultItem(SearchResultItem&&) = default;
    SearchResultItem& operator=(const SearchResultItem&) = default;
    SearchResultItem& operator=(SearchResultItem&&) = default;
    DocumentId documentId;
    std::string title;
    std::string path;
    std::string contentType;
    size_t fileSize;
    std::chrono::system_clock::time_point lastModified;
    std::chrono::system_clock::time_point indexedAt;

    // Relevance scoring
    float relevanceScore = 0.0f;
    float termFrequency = 0.0f;
    float inverseDocumentFrequency = 0.0f;

    // Highlighted snippets
    std::vector<SearchHighlight> highlights;

    // Additional metadata
    std::unordered_map<std::string, std::string> metadata;

    // Language detection
    std::string detectedLanguage;
    float languageConfidence = 0.0f;

    // Content preview
    std::string contentPreview;
    size_t previewLength = 200;
};

/**
 * @brief Search facet - for grouping and filtering results
 */
struct SearchFacet {
    std::string name;        // Facet name (e.g., "contentType", "author")
    std::string displayName; // Human-readable name

    struct FacetValue {
        std::string value;     // Facet value (e.g., "pdf", "docx")
        std::string display;   // Display value
        size_t count;          // Number of results with this value
        bool selected = false; // Whether this facet is currently selected
    };

    std::vector<FacetValue> values;
    size_t totalValues = 0; // Total number of unique values (may be > values.size())
};

/**
 * @brief Search statistics and performance metrics
 */
struct SearchStatistics {
    size_t totalResults = 0;                 // Total number of matching documents
    size_t returnedResults = 0;              // Number of results in current page
    std::chrono::milliseconds queryTime{0};  // Time to parse query
    std::chrono::milliseconds searchTime{0}; // Time to execute search
    std::chrono::milliseconds totalTime{0};  // Total response time

    // Query analysis
    size_t termsMatched = 0;     // Number of search terms that matched
    size_t documentsScanned = 0; // Number of documents scanned
    bool queryRewritten = false; // Whether query was rewritten/optimized
    std::string originalQuery;   // Original query string
    std::string executedQuery;   // Actual FTS5 query executed
};

} // namespace yams::search
