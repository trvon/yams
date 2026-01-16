#pragma once

#include <functional>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::search {

/**
 * @brief Extracted concept from a query
 *
 * Represents a domain concept identified in the query text via GLiNER NER.
 * Used for CONCEPT_ANCHOR fusion to boost documents containing matching concepts.
 */
struct QueryConcept {
    std::string text;     ///< The concept text span
    std::string type;     ///< Concept type: person, organization, location, technology, concept
    float confidence;     ///< GLiNER confidence score [0, 1]
    uint32_t startOffset; ///< Start character offset in query
    uint32_t endOffset;   ///< End character offset in query
};

/**
 * @brief Result of query concept extraction
 */
struct QueryConceptResult {
    std::vector<QueryConcept> concepts;
    bool usedGliner{false}; ///< True if GLiNER was used, false if fallback
    double extractionTimeMs{0.0};
};

/**
 * @brief Function type for entity extraction backend
 *
 * Allows the search layer to use entity extraction without depending on the daemon layer.
 * The daemon layer provides an implementation that wraps AbiEntityExtractorAdapter.
 *
 * @param content Text to extract entities from
 * @param entityTypes Optional list of entity types to filter (empty = default types)
 * @return Extracted concepts
 */
using EntityExtractionFunc = std::function<Result<QueryConceptResult>(
    const std::string& content, const std::vector<std::string>& entityTypes)>;

/**
 * @brief Query concept extractor using pluggable entity extraction backend
 *
 * Extracts domain concepts from search queries for use in CONCEPT_ANCHOR fusion.
 * The extraction backend is provided at construction time, allowing the daemon
 * layer to inject the GLiNER entity extractor without creating a circular dependency.
 *
 * Thread-safe: multiple threads can call extractConcepts concurrently.
 */
class QueryConceptExtractor {
public:
    /**
     * @brief Construct with entity extraction function
     * @param extractFunc Function to perform entity extraction (from daemon layer)
     */
    explicit QueryConceptExtractor(EntityExtractionFunc extractFunc);

    /**
     * @brief Construct without backend (returns empty results)
     */
    QueryConceptExtractor();

    ~QueryConceptExtractor();

    /**
     * @brief Check if entity extraction is available
     * @return true if an extraction function was provided
     */
    [[nodiscard]] bool isAvailable() const;

    /**
     * @brief Extract concepts from a query string
     * @param query The search query text
     * @return Extracted concepts with confidence scores
     */
    [[nodiscard]] Result<QueryConceptResult> extractConcepts(const std::string& query) const;

    /**
     * @brief Extract concepts with type filtering
     * @param query The search query text
     * @param types Entity types to extract (e.g., {"technology", "concept", "organization"})
     * @return Filtered extracted concepts
     */
    [[nodiscard]] Result<QueryConceptResult>
    extractConcepts(const std::string& query, const std::vector<std::string>& types) const;

private:
    EntityExtractionFunc extractFunc_;
};

} // namespace yams::search
