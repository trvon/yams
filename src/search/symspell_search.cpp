#include <spdlog/spdlog.h>
#include <symspell/symspell.hpp>
#include <symspell/symspell_sqlite.hpp>
#include <yams/search/symspell_search.h>

namespace yams::search {

SymSpellSearch::SymSpellSearch(sqlite3* db, int maxEditDistance, int prefixLength)
    : maxEditDistance_(maxEditDistance), prefixLength_(prefixLength) {
    store_ = std::make_unique<symspell::SQLiteStore>(db, maxEditDistance, prefixLength);
    symspell_ = std::make_unique<symspell::SymSpell>(
        std::make_unique<symspell::SQLiteStore>(db, maxEditDistance, prefixLength), maxEditDistance,
        prefixLength);
}

SymSpellSearch::~SymSpellSearch() = default;

Result<void> SymSpellSearch::initializeSchema(sqlite3* db) {
    auto result = symspell::SQLiteStore::initializeDatabase(db);
    if (!result) {
        return Result<void>(Error(ErrorCode::DatabaseError, result.error().message));
    }
    return Result<void>();
}

bool SymSpellSearch::addTerm(std::string_view term, int64_t frequency) {
    std::unique_lock lock(mutex_);
    return symspell_->createDictionaryEntry(term, frequency);
}

void SymSpellSearch::addTermsBatch(const std::vector<std::pair<std::string, int64_t>>& terms) {
    std::unique_lock lock(mutex_);

    // Use transaction for batch performance
    store_->beginTransaction();
    try {
        for (const auto& [term, freq] : terms) {
            symspell_->createDictionaryEntry(term, freq);
        }
        store_->commitTransaction();
    } catch (...) {
        store_->rollbackTransaction();
        throw;
    }
}

std::vector<SymSpellSearch::SearchResult>
SymSpellSearch::search(const std::string& query, const SearchOptions& options) const {
    std::shared_lock lock(mutex_);

    // Determine verbosity based on options
    symspell::Verbosity verbosity =
        options.returnAll ? symspell::Verbosity::All : symspell::Verbosity::Closest;

    // Perform SymSpell lookup
    auto suggestions = symspell_->lookup(query, verbosity, options.maxEditDistance);

    // Convert to SearchResult format
    std::vector<SearchResult> results;
    results.reserve(std::min(suggestions.size(), options.maxResults));

    for (size_t i = 0; i < suggestions.size() && i < options.maxResults; ++i) {
        const auto& s = suggestions[i];
        SearchResult result;
        result.term = s.term;
        result.editDistance = s.distance;
        result.frequency = s.frequency;

        // Set match type based on edit distance
        if (s.distance == 0) {
            result.matchType = "exact";
        } else {
            result.matchType = "fuzzy_" + std::to_string(s.distance);
        }

        results.push_back(std::move(result));
    }

    return results;
}

bool SymSpellSearch::hasExactMatch(std::string_view term) const {
    std::shared_lock lock(mutex_);
    return store_->termExists(term);
}

SymSpellSearch::IndexStats SymSpellSearch::getStats() const {
    // Note: Getting accurate stats would require querying the database
    // For now, return the configured values
    IndexStats stats;
    stats.maxEditDistance = maxEditDistance_;
    stats.prefixLength = prefixLength_;
    // termCount and deleteCount would need SQL queries to populate
    return stats;
}

void SymSpellSearch::clear() {
    std::unique_lock lock(mutex_);
    // Would need to execute DELETE FROM symspell_terms; DELETE FROM symspell_deletes;
    // For now, this is a placeholder - full implementation needs db access
    spdlog::warn("SymSpellSearch::clear() not fully implemented");
}

} // namespace yams::search
