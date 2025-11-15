#pragma once

#include <yams/core/types.h>
#include <yams/metadata/database.h>

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

namespace yams::metadata {

// C++20 concept for scoreable documents
template <typename T>
concept Scoreable = requires(T t) {
    { t.id } -> std::convertible_to<int64_t>;
    { t.score } -> std::convertible_to<double>;
};

/**
 * Document candidate for fuzzy index with relevance score
 */
struct FuzzyIndexCandidate {
    int64_t id;
    std::string fileName;
    std::string filePath;
    double score; // Composite relevance score

    // Scoring factors (for transparency)
    int tagCount{0};
    int entityCount{0};
    int recencyDays{0};
    bool isCodeFile{false};

    // Auto-compute score from factors
    void computeScore() noexcept {
        score = (tagCount * 3.0) + (entityCount * 2.0) +
                (recencyDays < 7     ? 4.0
                 : recencyDays < 30  ? 3.0
                 : recencyDays < 90  ? 2.0
                 : recencyDays < 180 ? 1.0
                                     : 0.0) +
                (isCodeFile ? 2.0 : 0.0);
    }

    // Comparison for sorting
    auto operator<=>(const FuzzyIndexCandidate& other) const noexcept {
        return score <=> other.score;
    }
};

/**
 * Modern C++20 fuzzy index builder using ranges and lazy evaluation
 */
class FuzzyIndexBuilder {
public:
    struct Options {
        size_t maxDocuments = std::numeric_limits<size_t>::max();
        bool includeContent = true;
        size_t maxContentLength = 5000;
        size_t maxKeywords = 100;

        static Options fromEnvironment() noexcept {
            Options opts;
            if (const char* limit = std::getenv("YAMS_FUZZY_INDEX_LIMIT")) {
                try {
                    auto parsed = std::stoull(limit);
                    if (parsed > 0) {
                        opts.maxDocuments = parsed;
                    }
                } catch (...) {
                    // Keep default
                }
            }
            return opts;
        }
    };

    explicit FuzzyIndexBuilder(Database& db, Options opts = Options::fromEnvironment())
        : db_(db), options_(std::move(opts)) {}

    // Build candidate list using composable scoring pipeline
    Result<std::vector<FuzzyIndexCandidate>> buildCandidates();

private:
    Database& db_;
    Options options_;

    // Extract scoring factors efficiently (single query)
    struct ScoringFactors {
        int tagCount;
        int entityCount;
        std::string indexedAt;
        std::string mimeType;
        std::string fileName;
    };

    Result<std::vector<ScoringFactors>> fetchScoringFactors();

    // Parse indexed date to days ago
    static int daysAgo(std::string_view isoDate) noexcept;

    // Check if file is code by extension/mime (uses magic_numbers.hpp patterns)
    static bool isCodeFile(std::string_view fileName, std::string_view mimeType) noexcept;
};

/**
 * Extract path components as keywords (C++20 ranges)
 */
inline auto extractPathKeywords(std::string_view path) {
    return path | std::views::split('/') | std::views::transform([](auto&& seg) {
               return std::string_view(seg.begin(), seg.end());
           }) |
           std::views::filter(
               [](std::string_view s) { return !s.empty() && s != "." && s != ".."; });
}

/**
 * Extract content keywords (lazy evaluation)
 */
inline auto extractContentKeywords(std::string_view content, size_t maxKeywords = 100) {
    // TODO: Use std::ranges::lazy_split (C++23) when available
    // For now, return a vector - can optimize later with generator coroutine
    std::vector<std::string> keywords;
    keywords.reserve(maxKeywords);

    std::string_view remaining = content;
    std::string prevWord;

    while (!remaining.empty() && keywords.size() < maxKeywords) {
        // Find next word boundary
        auto start = remaining.find_first_not_of(" \t\n\r");
        if (start == std::string_view::npos)
            break;

        remaining.remove_prefix(start);
        auto end = remaining.find_first_of(" \t\n\r");

        std::string_view word = remaining.substr(0, end);
        remaining.remove_prefix(end != std::string_view::npos ? end : remaining.size());

        // Clean word: keep only alphanumeric, dash, underscore
        std::string cleaned;
        cleaned.reserve(word.size());
        for (char c : word) {
            if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_') {
                cleaned += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
        }

        if (cleaned.size() > 2) { // Skip very short words
            keywords.push_back(cleaned);

            // Add bigram
            if (!prevWord.empty()) {
                keywords.push_back(prevWord + " " + cleaned);
            }
            prevWord = std::move(cleaned);
        }
    }

    return keywords;
}

} // namespace yams::metadata
