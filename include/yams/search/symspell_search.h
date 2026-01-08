#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <yams/core/types.h>

// Forward declarations to avoid header dependencies
struct sqlite3;

namespace yams::symspell {
class SQLiteStore;
class SymSpell;
enum class Verbosity;
} // namespace yams::symspell

namespace yams::search {

/**
 * @brief SymSpell-based fuzzy search for YAMS
 *
 * Provides fast edit-distance fuzzy search using the SymSpell algorithm
 * with SQLite persistence.
 *
 * Key features:
 * - O(1) lookup via precomputed delete variants
 * - SQLite-backed persistence (survives daemon restart)
 * - Incremental updates during document ingest
 */
class SymSpellSearch {
public:
    /**
     * @brief Search result from SymSpell lookup
     */
    struct SearchResult {
        std::string term;      ///< The matched dictionary term
        int editDistance;      ///< Edit distance from query to term
        int64_t frequency;     ///< Term frequency (popularity)
        std::string matchType; ///< "exact", "fuzzy_1", "fuzzy_2", etc.
    };

    /**
     * @brief Search options
     */
    struct SearchOptions {
        int maxEditDistance; ///< Maximum edit distance (1-3 typical)
        bool returnAll;      ///< Return all matches within distance (vs just closest)
        size_t maxResults;   ///< Maximum results to return

        SearchOptions() : maxEditDistance(2), returnAll(false), maxResults(10) {}
    };

    /**
     * @brief Index statistics
     */
    struct IndexStats {
        size_t termCount = 0;    ///< Number of unique terms in dictionary
        size_t deleteCount = 0;  ///< Number of delete variant entries
        int maxEditDistance = 2; ///< Configured max edit distance
        int prefixLength = 7;    ///< Configured prefix length
    };

    /**
     * @brief Create a SymSpell search index backed by SQLite
     * @param db SQLite database handle (must remain valid for lifetime)
     * @param maxEditDistance Maximum edit distance for lookups (default 2)
     * @param prefixLength Prefix length for optimization (default 7)
     */
    explicit SymSpellSearch(sqlite3* db, int maxEditDistance = 2, int prefixLength = 7);

    ~SymSpellSearch();

    // Non-copyable, non-movable (owns database resources)
    SymSpellSearch(const SymSpellSearch&) = delete;
    SymSpellSearch& operator=(const SymSpellSearch&) = delete;
    SymSpellSearch(SymSpellSearch&&) = delete;
    SymSpellSearch& operator=(SymSpellSearch&&) = delete;

    /**
     * @brief Initialize the database schema (call once per database)
     * @param db SQLite database handle
     * @return Success or error
     */
    static Result<void> initializeSchema(sqlite3* db);

    /**
     * @brief Add a term to the dictionary
     * @param term The term to add
     * @param frequency Initial frequency count (default 1)
     * @return true if term was added (new), false if frequency was updated
     *
     * Thread-safe: acquires exclusive lock
     */
    bool addTerm(std::string_view term, int64_t frequency = 1);

    /**
     * @brief Add multiple terms in a batch (faster than individual adds)
     * @param terms Vector of (term, frequency) pairs
     *
     * Thread-safe: acquires exclusive lock, uses transaction
     */
    void addTermsBatch(const std::vector<std::pair<std::string, int64_t>>& terms);

    /**
     * @brief Search for terms similar to query
     * @param query The query string
     * @param options Search options
     * @return Vector of search results sorted by (distance, frequency desc)
     *
     * Thread-safe: acquires shared lock
     */
    std::vector<SearchResult> search(const std::string& query,
                                     const SearchOptions& options = SearchOptions{}) const;

    /**
     * @brief Check if a term exists exactly in the dictionary
     * @param term The term to check
     * @return true if term exists
     *
     * Thread-safe: acquires shared lock
     */
    bool hasExactMatch(std::string_view term) const;

    /**
     * @brief Get index statistics
     * @return Current index stats
     */
    IndexStats getStats() const;

    /**
     * @brief Clear all terms from the index
     *
     * Thread-safe: acquires exclusive lock
     */
    void clear();

private:
    std::unique_ptr<symspell::SQLiteStore> store_;
    std::unique_ptr<symspell::SymSpell> symspell_;
    mutable std::shared_mutex mutex_;
    int maxEditDistance_;
    int prefixLength_;
};

} // namespace yams::search
