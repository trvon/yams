#pragma once

#include <array>
#include <string>
#include <string_view>
#include <vector>

namespace yams {
namespace app {
namespace services {

/// Extracts literal substrings from regex patterns for fast pre-filtering
/// Based on ripgrep's literal extraction strategy
struct LiteralExtractor {
    /// Result of literal extraction from a pattern
    struct ExtractionResult {
        std::vector<std::string> literals; ///< Extracted literal strings
        bool isComplete;                   ///< True if pattern is purely literal
        size_t longestLength;              ///< Length of longest extracted literal

        bool empty() const { return literals.empty(); }
        const std::string& longest() const;
    };

    /// Extract literals from a regex pattern
    /// @param pattern The regex pattern (not escaped)
    /// @param ignoreCase Whether the pattern is case-insensitive
    /// @return Extracted literals, or empty if none found
    static ExtractionResult extract(std::string_view pattern, bool ignoreCase);

private:
    /// Check if a character is a regex metacharacter
    static bool isMetaChar(char c);

    /// Extract the longest contiguous literal substring
    static std::string extractLongestLiteral(std::string_view pattern, bool* found);

    /// Split pattern into literal and non-literal segments
    static std::vector<std::string> extractAllLiterals(std::string_view pattern);
};

/// Boyer-Moore-Horspool string search implementation
/// Uses std::string::find for patterns < 16 chars (benchmark-optimized)
class BMHSearcher {
public:
    /// Minimum pattern length for BMH algorithm
    /// Below this threshold, std::string::find is faster
    static constexpr size_t kMinBMHLength = 16;

    /// Construct searcher with pattern
    explicit BMHSearcher(std::string_view pattern, bool ignoreCase = false);

    /// Find first occurrence of pattern in text
    /// @return Position of match, or std::string::npos if not found
    size_t find(std::string_view text, size_t startPos = 0) const;

    /// Find all occurrences of pattern in text
    std::vector<size_t> findAll(std::string_view text) const;

    /// Get the pattern being searched for
    std::string_view pattern() const { return pattern_; }

    /// Check if using fast path (std::string::find)
    bool usesFastPath() const { return useFastPath_; }

private:
    std::string pattern_;
    std::array<size_t, 256> shift_; ///< Bad character shift table
    bool ignoreCase_;
    bool useFastPath_; ///< True if using std::string::find for short patterns

    void buildShiftTable();
    static unsigned char toLower(unsigned char c);
    size_t findFast(std::string_view text, size_t startPos) const;
    size_t findBMH(std::string_view text, size_t startPos) const;
};

} // namespace services
} // namespace app
} // namespace yams
