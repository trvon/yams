#include <algorithm>
#include <yams/search/search_filters.h>

namespace yams::search {

bool SearchFilters::matches(const SearchResultItem& item) const {
    // Apply date filters
    for (const auto& filter : dateFilters_) {
        std::chrono::system_clock::time_point dateToCheck;
        switch (filter.field) {
            case DateRangeFilter::DateField::LastModified:
                dateToCheck = item.lastModified;
                break;
            case DateRangeFilter::DateField::Created:
                // Assuming created date is stored in metadata
                // For now, use lastModified as fallback
                dateToCheck = item.lastModified;
                break;
            case DateRangeFilter::DateField::Indexed:
                dateToCheck = item.indexedAt;
                break;
        }

        if (!filter.matches(dateToCheck)) {
            return false;
        }
    }

    // Apply size filters
    for (const auto& filter : sizeFilters_) {
        if (!filter.matches(item.fileSize)) {
            return false;
        }
    }

    // Apply content type filters
    for (const auto& filter : contentTypeFilters_) {
        if (!filter.matches(item.contentType)) {
            return false;
        }
    }

    // Apply language filters
    for (const auto& filter : languageFilters_) {
        if (!filter.matches(item.detectedLanguage, item.languageConfidence)) {
            return false;
        }
    }

    // Apply path filters
    for (const auto& filter : pathFilters_) {
        if (!filter.matches(item.path)) {
            return false;
        }
    }

    // Apply metadata filters
    for (const auto& filter : metadataFilters_) {
        if (!filter.matches(item.metadata)) {
            return false;
        }
    }

    // Apply relevance filters
    for (const auto& filter : relevanceFilters_) {
        if (!filter.matches(item.relevanceScore)) {
            return false;
        }
    }

    // Apply custom filters
    for (const auto& filter : customFilters_) {
        if (!filter(item)) {
            return false;
        }
    }

    return true;
}

std::vector<SearchResultItem>
SearchFilters::apply(const std::vector<SearchResultItem>& items) const {
    if (!hasFilters()) {
        return items; // No filters, return all items
    }

    std::vector<SearchResultItem> filteredItems;
    filteredItems.reserve(items.size());

    for (const auto& item : items) {
        if (matches(item)) {
            filteredItems.push_back(item);
        }
    }

    return filteredItems;
}

} // namespace yams::search