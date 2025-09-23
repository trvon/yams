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
    // Support simple globbing with '*' wildcard (match any sequence, including empty).
    // Examples:
    //  - "*" matches everything
    //  - "q:user:123*" matches any key starting with "q:user:123"
    //  - "*profile" matches any key ending with "profile"
    //  - "*user*profile*" matches if both substrings appear in order
    if (pattern.empty() || pattern == "*")
        return true;

    // Fast-path: no wildcard -> substring check
    if (pattern.find('*') == std::string::npos) {
        return keyString_.find(pattern) != std::string::npos;
    }

    // Split pattern by '*', then ensure all parts appear in order
    size_t pos = 0;
    bool anchorStart = pattern.front() != '*';
    bool anchorEnd = pattern.back() != '*';

    size_t cursor = 0; // position in keyString_
    bool first = true;
    while (pos <= pattern.size()) {
        size_t next = pattern.find('*', pos);
        std::string token =
            (next == std::string::npos) ? pattern.substr(pos) : pattern.substr(pos, next - pos);
        if (!token.empty()) {
            size_t found = keyString_.find(token, cursor);
            if (found == std::string::npos)
                return false;
            if (first && anchorStart && found != 0)
                return false; // must start with token
            cursor = found + token.size();
            first = false;
        }
        if (next == std::string::npos)
            break;
        pos = next + 1;
    }

    if (anchorEnd) {
        // Last token must end at the end of keyString_
        // If pattern ended with '*', anchorEnd=false and we skip this check
        // Find last non-wildcard segment
        size_t lastStar = pattern.find_last_of('*');
        std::string tail = (lastStar == std::string::npos) ? pattern : pattern.substr(lastStar + 1);
        if (!tail.empty()) {
            if (keyString_.size() < tail.size())
                return false;
            if (keyString_.rfind(tail) != keyString_.size() - tail.size())
                return false;
        }
    }

    return true;
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
