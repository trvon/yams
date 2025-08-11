#pragma once

#include <yams/core/types.h>
#include <yams/search/search_results.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <optional>
#include <functional>

namespace yams::search {

/**
 * @brief Date range filter
 */
struct DateRangeFilter {
    std::optional<std::chrono::system_clock::time_point> from;
    std::optional<std::chrono::system_clock::time_point> to;
    
    enum class DateField {
        LastModified,
        Created,
        Indexed
    };
    
    DateField field = DateField::LastModified;
    
    bool matches(const std::chrono::system_clock::time_point& date) const {
        if (from && date < *from) return false;
        if (to && date > *to) return false;
        return true;
    }
};

/**
 * @brief Size range filter (in bytes)
 */
struct SizeRangeFilter {
    std::optional<size_t> minSize;
    std::optional<size_t> maxSize;
    
    bool matches(size_t size) const {
        if (minSize && size < *minSize) return false;
        if (maxSize && size > *maxSize) return false;
        return true;
    }
};

/**
 * @brief Content type filter
 */
struct ContentTypeFilter {
    std::unordered_set<std::string> allowedTypes;
    std::unordered_set<std::string> excludedTypes;
    
    bool matches(const std::string& contentType) const {
        if (!excludedTypes.empty() && excludedTypes.count(contentType)) {
            return false;
        }
        if (!allowedTypes.empty() && !allowedTypes.count(contentType)) {
            return false;
        }
        return true;
    }
};

/**
 * @brief Language filter
 */
struct LanguageFilter {
    std::unordered_set<std::string> allowedLanguages;
    std::unordered_set<std::string> excludedLanguages;
    float minConfidence = 0.0f;
    
    bool matches(const std::string& language, float confidence) const {
        if (confidence < minConfidence) return false;
        if (!excludedLanguages.empty() && excludedLanguages.count(language)) {
            return false;
        }
        if (!allowedLanguages.empty() && !allowedLanguages.count(language)) {
            return false;
        }
        return true;
    }
};

/**
 * @brief Path filter (for filtering by directory or file patterns)
 */
struct PathFilter {
    std::vector<std::string> includePaths;    // Paths that must be included
    std::vector<std::string> excludePaths;    // Paths to exclude
    std::vector<std::string> patterns;        // Glob patterns to match
    
    bool matches(const std::string& path) const {
        // Check exclude paths first
        for (const auto& excludePath : excludePaths) {
            if (path.find(excludePath) != std::string::npos) {
                return false;
            }
        }
        
        // Check include paths
        if (!includePaths.empty()) {
            bool found = false;
            for (const auto& includePath : includePaths) {
                if (path.find(includePath) != std::string::npos) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        
        // Check patterns (simplified - would need proper glob matching)
        if (!patterns.empty()) {
            // TODO: Implement proper glob pattern matching
            return true;
        }
        
        return true;
    }
};

/**
 * @brief Metadata filter (for custom metadata fields)
 */
struct MetadataFilter {
    std::string fieldName;
    std::vector<std::string> allowedValues;
    std::vector<std::string> excludedValues;
    
    bool matches(const std::unordered_map<std::string, std::string>& metadata) const {
        auto it = metadata.find(fieldName);
        if (it == metadata.end()) {
            return allowedValues.empty(); // Field not present - allow if no specific values required
        }
        
        const std::string& value = it->second;
        
        // Check excluded values
        for (const auto& excluded : excludedValues) {
            if (value == excluded) return false;
        }
        
        // Check allowed values
        if (!allowedValues.empty()) {
            for (const auto& allowed : allowedValues) {
                if (value == allowed) return true;
            }
            return false;
        }
        
        return true;
    }
};

/**
 * @brief Relevance score filter
 */
struct RelevanceFilter {
    float minScore = 0.0f;
    float maxScore = 1.0f;
    
    bool matches(float score) const {
        return score >= minScore && score <= maxScore;
    }
};

/**
 * @brief Composite search filter that combines multiple filter types
 */
class SearchFilters {
public:
    SearchFilters() = default;
    
    /**
     * @brief Add filters
     */
    void addDateRangeFilter(DateRangeFilter filter) { dateFilters_.push_back(std::move(filter)); }
    void addSizeRangeFilter(SizeRangeFilter filter) { sizeFilters_.push_back(std::move(filter)); }
    void addContentTypeFilter(ContentTypeFilter filter) { contentTypeFilters_.push_back(std::move(filter)); }
    void addLanguageFilter(LanguageFilter filter) { languageFilters_.push_back(std::move(filter)); }
    void addPathFilter(PathFilter filter) { pathFilters_.push_back(std::move(filter)); }
    void addMetadataFilter(MetadataFilter filter) { metadataFilters_.push_back(std::move(filter)); }
    void addRelevanceFilter(RelevanceFilter filter) { relevanceFilters_.push_back(std::move(filter)); }
    
    /**
     * @brief Add custom filter function
     */
    void addCustomFilter(std::function<bool(const SearchResultItem&)> filter) {
        customFilters_.push_back(std::move(filter));
    }
    
    /**
     * @brief Check if a search result item passes all filters
     */
    bool matches(const SearchResultItem& item) const;
    
    /**
     * @brief Apply filters to a collection of search results
     */
    std::vector<SearchResultItem> apply(const std::vector<SearchResultItem>& items) const;
    
    /**
     * @brief Check if any filters are active
     */
    bool hasFilters() const {
        return !dateFilters_.empty() || !sizeFilters_.empty() || 
               !contentTypeFilters_.empty() || !languageFilters_.empty() ||
               !pathFilters_.empty() || !metadataFilters_.empty() ||
               !relevanceFilters_.empty() || !customFilters_.empty();
    }
    
    /**
     * @brief Clear all filters
     */
    void clear() {
        dateFilters_.clear();
        sizeFilters_.clear();
        contentTypeFilters_.clear();
        languageFilters_.clear();
        pathFilters_.clear();
        metadataFilters_.clear();
        relevanceFilters_.clear();
        customFilters_.clear();
    }
    
    /**
     * @brief Get filter counts
     */
    size_t getFilterCount() const {
        return dateFilters_.size() + sizeFilters_.size() + 
               contentTypeFilters_.size() + languageFilters_.size() +
               pathFilters_.size() + metadataFilters_.size() +
               relevanceFilters_.size() + customFilters_.size();
    }

private:
    std::vector<DateRangeFilter> dateFilters_;
    std::vector<SizeRangeFilter> sizeFilters_;
    std::vector<ContentTypeFilter> contentTypeFilters_;
    std::vector<LanguageFilter> languageFilters_;
    std::vector<PathFilter> pathFilters_;
    std::vector<MetadataFilter> metadataFilters_;
    std::vector<RelevanceFilter> relevanceFilters_;
    std::vector<std::function<bool(const SearchResultItem&)>> customFilters_;
};

} // namespace yams::search