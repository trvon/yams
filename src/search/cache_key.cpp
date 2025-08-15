#include <functional>
#include <iomanip>
#include <sstream>
#include <yams/search/cache_key.h>

namespace yams::search {

CacheKey CacheKey::fromQuery(const std::string& queryText, const SearchFilters* filters,
                             size_t offset, size_t limit) {
    CacheKey key;
    key.components_.queryText = queryText;
    key.components_.offset = offset;
    key.components_.limit = limit;

    // Add filters to components if provided
    if (filters) {
        key.components_.filters = serializeFilters(filters).empty()
                                      ? std::vector<std::string>{}
                                      : std::vector<std::string>{serializeFilters(filters)};
    }

    key.buildKey();
    return key;
}

CacheKey CacheKey::fromParsedQuery(const QueryNode* query, const SearchFilters* filters,
                                   size_t offset, size_t limit) {
    CacheKey key;

    // Serialize the query node to text
    if (query) {
        key.components_.queryText = serializeQuery(query);
    }

    key.components_.offset = offset;
    key.components_.limit = limit;

    // Add filters to components if provided
    if (filters) {
        key.components_.filters = serializeFilters(filters).empty()
                                      ? std::vector<std::string>{}
                                      : std::vector<std::string>{serializeFilters(filters)};
    }

    key.buildKey();
    return key;
}

void CacheKey::buildKey() {
    std::stringstream ss;

    // Query text
    ss << "q:" << components_.queryText << "|";

    // Pagination
    ss << "o:" << components_.offset << ",l:" << components_.limit << "|";

    // Filters
    if (!components_.filters.empty()) {
        ss << "f:";
        for (size_t i = 0; i < components_.filters.size(); ++i) {
            if (i > 0)
                ss << ";";
            ss << components_.filters[i];
        }
        ss << "|";
    }

    keyString_ = ss.str();

    // Calculate hash
    std::hash<std::string> hasher;
    hashValue_ = hasher(keyString_);
}

bool CacheKey::matchesPattern(const std::string& pattern) const {
    // Simple pattern matching (could be enhanced with regex)
    if (pattern == "*")
        return true;

    // Check if pattern is contained in key string
    return keyString_.find(pattern) != std::string::npos;
}

std::string CacheKey::serializeFilters(const SearchFilters* filters) {
    if (!filters || !filters->hasFilters())
        return "";

    std::stringstream ss;

    // Simple serialization based on filter count
    // The actual filter content is opaque to the cache key
    // We just need to ensure different filter combinations produce different keys
    ss << "filters:" << filters->getFilterCount();

    // Add a simple hash of the filters object address for uniqueness
    // In a real implementation, we'd serialize the actual filter content
    std::hash<const void*> hasher;
    ss << ":" << hasher(filters);

    return ss.str();
}

std::string CacheKey::serializeQuery(const QueryNode* query) {
    if (!query)
        return "";

    // Use the QueryNode's own toString() method for serialization
    // This is polymorphic and will call the correct derived class method
    return query->toString();
}

} // namespace yams::search