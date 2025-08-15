#include <algorithm>
#include <yams/search/search_results.h>

namespace yams::search {

void SearchResults::sortByRelevance() {
    std::sort(items_.begin(), items_.end(),
              [](const SearchResultItem& a, const SearchResultItem& b) {
                  return a.relevanceScore > b.relevanceScore;
              });
}

void SearchResults::sortByDate() {
    std::sort(items_.begin(), items_.end(),
              [](const SearchResultItem& a, const SearchResultItem& b) {
                  return a.lastModified > b.lastModified;
              });
}

void SearchResults::sortByTitle() {
    std::sort(
        items_.begin(), items_.end(),
        [](const SearchResultItem& a, const SearchResultItem& b) { return a.title < b.title; });
}

void SearchResults::paginate(size_t offset, size_t limit) {
    if (offset >= items_.size()) {
        items_.clear();
        return;
    }

    size_t endPos = std::min(offset + limit, items_.size());

    if (offset > 0 || endPos < items_.size()) {
        std::vector<SearchResultItem> paginatedItems(items_.begin() + offset,
                                                     items_.begin() + endPos);
        items_ = std::move(paginatedItems);
    }
}

} // namespace yams::search