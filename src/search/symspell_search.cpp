#include <symspell/symspell.hpp>
#include <symspell/symspell_sqlite.hpp>
#include <yams/search/symspell_search.h>

namespace yams::search {

namespace {

template <typename T> Result<T> convertResult(symspell::Result<T> result) {
    if (!result) {
        return Error{ErrorCode::DatabaseError, result.error().message};
    }
    return result.value();
}

Result<void> convertResult(symspell::Result<void> result) {
    if (!result) {
        return Error{ErrorCode::DatabaseError, result.error().message};
    }
    return {};
}

} // namespace

SymSpellSearch::SymSpellSearch(sqlite3* db, int maxEditDistance, int prefixLength, bool readOnly)
    : maxEditDistance_(maxEditDistance), prefixLength_(prefixLength) {
    symspell_ = std::make_unique<symspell::SymSpell>(
        std::make_unique<symspell::SQLiteStore>(db, maxEditDistance, prefixLength, readOnly),
        maxEditDistance, prefixLength);
}

Result<void> SymSpellSearch::initializeSchema(sqlite3* db) {
    auto result = symspell::SQLiteStore::initializeDatabase(db);
    if (!result) {
        return Result<void>(Error(ErrorCode::DatabaseError, result.error().message));
    }
    return Result<void>();
}

Result<bool> SymSpellSearch::addTerm(std::string_view term, int64_t frequency) {
    std::unique_lock lock(mutex_);
    return convertResult(symspell_->createDictionaryEntry(term, frequency));
}

Result<void>
SymSpellSearch::addTermsBatch(const std::vector<std::pair<std::string, int64_t>>& terms) {
    std::unique_lock lock(mutex_);

    // Use transaction for batch performance
    auto begin = symspell_->beginTransaction();
    if (!begin) {
        return Error{ErrorCode::DatabaseError, begin.error().message};
    }
    try {
        for (const auto& [term, freq] : terms) {
            auto inserted = symspell_->createDictionaryEntry(term, freq);
            if (!inserted) {
                (void)symspell_->rollbackTransaction();
                return Error{ErrorCode::DatabaseError, inserted.error().message};
            }
        }
        auto commit = symspell_->commitTransaction();
        if (!commit) {
            return Error{ErrorCode::DatabaseError, commit.error().message};
        }
    } catch (...) {
        (void)symspell_->rollbackTransaction();
        throw;
    }
    return {};
}

std::vector<SymSpellSearch::SearchResult>
SymSpellSearch::search(const std::string& query, const SearchOptions& options) const {
    // The SQLiteStore implementation reuses prepared statements for lookup. Even read-only
    // lookups mutate those statements (bind/step/reset), so concurrent readers must serialize.
    std::unique_lock lock(mutex_);

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
    std::unique_lock lock(mutex_);
    return symspell_->termExists(term);
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

Result<void> SymSpellSearch::clear() {
    std::unique_lock lock(mutex_);
    return convertResult(symspell_->clear());
}

SymSpellSearch::~SymSpellSearch() = default;

} // namespace yams::search
