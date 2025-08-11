#pragma once

#include <yams/core/types.h>
#include <yams/search/query_parser.h>
#include <yams/search/search_filters.h>
#include <string>
#include <vector>
#include <functional>

namespace yams::search {

/**
 * @brief Represents a unique key for caching search results
 */
class CacheKey {
public:
    /**
     * @brief Generate cache key from search parameters
     */
    static CacheKey fromQuery(
        const std::string& queryText,
        const SearchFilters* filters = nullptr,
        size_t offset = 0,
        size_t limit = 10);
    
    /**
     * @brief Generate cache key from parsed query
     */
    static CacheKey fromParsedQuery(
        const QueryNode* query,
        const SearchFilters* filters = nullptr,
        size_t offset = 0,
        size_t limit = 10);
    
    /**
     * @brief Default constructor
     */
    CacheKey() = default;
    
    /**
     * @brief Get the hash value of the key
     */
    size_t hash() const { return hashValue_; }
    
    /**
     * @brief Get string representation
     */
    const std::string& toString() const { return keyString_; }
    
    /**
     * @brief Equality comparison
     */
    bool operator==(const CacheKey& other) const {
        return hashValue_ == other.hashValue_ && keyString_ == other.keyString_;
    }
    
    /**
     * @brief Inequality comparison
     */
    bool operator!=(const CacheKey& other) const {
        return !(*this == other);
    }
    
    /**
     * @brief Check if key matches a pattern (for invalidation)
     */
    bool matchesPattern(const std::string& pattern) const;
    
    /**
     * @brief Get query components
     */
    struct Components {
        std::string queryText;
        std::vector<std::string> filters;
        size_t offset;
        size_t limit;
    };
    
    Components getComponents() const { return components_; }
    
private:
    std::string keyString_;
    size_t hashValue_ = 0;
    Components components_;
    
    /**
     * @brief Build key string from components
     */
    void buildKey();
    
    /**
     * @brief Serialize filters to string
     */
    static std::string serializeFilters(const SearchFilters* filters);
    
    /**
     * @brief Serialize query node to string
     */
    static std::string serializeQuery(const QueryNode* query);
};

/**
 * @brief Hash function for CacheKey (for use in unordered containers)
 */
struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
        return key.hash();
    }
};

} // namespace yams::search