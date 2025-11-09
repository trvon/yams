#include <algorithm>
#include <cctype>
#include <yams/app/services/literal_extractor.hpp>

namespace yams {
namespace app {
namespace services {

// Regex metacharacters that break literal sequences
static const char* const kMetaChars = "\\^$.|?*+()[]{}";

bool LiteralExtractor::isMetaChar(char c) {
    for (const char* p = kMetaChars; *p; ++p) {
        if (*p == c)
            return true;
    }
    return false;
}

const std::string& LiteralExtractor::ExtractionResult::longest() const {
    static const std::string empty;
    if (literals.empty())
        return empty;

    auto maxElem = std::max_element(
        literals.begin(), literals.end(),
        [](const std::string& a, const std::string& b) { return a.size() < b.size(); });
    return *maxElem;
}

LiteralExtractor::ExtractionResult LiteralExtractor::extract(std::string_view pattern,
                                                             bool ignoreCase) {
    ExtractionResult result;
    result.isComplete = true;
    result.longestLength = 0;

    std::string currentLiteral;
    bool escaped = false;

    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];

        if (escaped) {
            // After backslash, most chars become literals (except special sequences like \d, \s,
            // etc)
            if (std::isalnum(static_cast<unsigned char>(c))) {
                // Special sequences like \d, \w, \s, \D, \W, \S, \b, \B
                result.isComplete = false;
                if (!currentLiteral.empty()) {
                    result.literals.push_back(currentLiteral);
                    currentLiteral.clear();
                }
            } else {
                // Escaped literal: \., \*, etc
                currentLiteral += c;
            }
            escaped = false;
        } else if (c == '\\') {
            escaped = true;
        } else if (isMetaChar(c)) {
            result.isComplete = false;
            if (!currentLiteral.empty()) {
                result.literals.push_back(currentLiteral);
                currentLiteral.clear();
            }
        } else {
            currentLiteral += c;
        }
    }

    if (!currentLiteral.empty()) {
        result.literals.push_back(currentLiteral);
    }

    // Calculate longest literal length
    for (const auto& lit : result.literals) {
        if (lit.size() > result.longestLength) {
            result.longestLength = lit.size();
        }
    }

    // If case-insensitive, convert literals to lowercase
    if (ignoreCase) {
        for (auto& lit : result.literals) {
            std::transform(lit.begin(), lit.end(), lit.begin(),
                           [](unsigned char c) { return std::tolower(c); });
        }
    }

    return result;
}

std::string LiteralExtractor::extractLongestLiteral(std::string_view pattern, bool* found) {
    auto result = extract(pattern, false);
    if (found) {
        *found = !result.literals.empty();
    }
    return result.empty() ? std::string{} : result.longest();
}

std::vector<std::string> LiteralExtractor::extractAllLiterals(std::string_view pattern) {
    return extract(pattern, false).literals;
}

// --- Boyer-Moore-Horspool Implementation ---

BMHSearcher::BMHSearcher(std::string_view pattern, bool ignoreCase)
    : pattern_(pattern), ignoreCase_(ignoreCase) {
    if (ignoreCase_) {
        std::transform(pattern_.begin(), pattern_.end(), pattern_.begin(),
                       [](unsigned char c) { return std::tolower(c); });
    }
    buildShiftTable();
}

void BMHSearcher::buildShiftTable() {
    const size_t m = pattern_.size();

    // Initialize all shifts to pattern length
    shift_.fill(m);

    // Build bad character shift table
    // For each character, store distance from end (excluding last char)
    for (size_t i = 0; i < m - 1; ++i) {
        unsigned char c = static_cast<unsigned char>(pattern_[i]);
        shift_[c] = m - 1 - i;
    }
}

unsigned char BMHSearcher::toLower(unsigned char c) {
    return static_cast<unsigned char>(std::tolower(c));
}

size_t BMHSearcher::find(std::string_view text, size_t startPos) const {
    const size_t m = pattern_.size();
    const size_t n = text.size();

    if (m == 0 || n == 0 || m > n || startPos > n - m) {
        return std::string::npos;
    }

    size_t pos = startPos;

    while (pos <= n - m) {
        // Check from end of pattern backwards
        size_t j = m - 1;
        while (j != static_cast<size_t>(-1)) {
            unsigned char textChar = static_cast<unsigned char>(text[pos + j]);
            unsigned char patternChar = static_cast<unsigned char>(pattern_[j]);

            if (ignoreCase_) {
                textChar = toLower(textChar);
            }

            if (textChar != patternChar) {
                break;
            }
            --j;
        }

        if (j == static_cast<size_t>(-1)) {
            // Found match
            return pos;
        }

        // Shift by bad character rule
        unsigned char badChar = static_cast<unsigned char>(text[pos + m - 1]);
        if (ignoreCase_) {
            badChar = toLower(badChar);
        }
        pos += shift_[badChar];
    }

    return std::string::npos;
}

std::vector<size_t> BMHSearcher::findAll(std::string_view text) const {
    std::vector<size_t> matches;
    size_t pos = 0;

    while (pos != std::string::npos && pos < text.size()) {
        pos = find(text, pos);
        if (pos != std::string::npos) {
            matches.push_back(pos);
            pos += 1; // Move to next position
        }
    }

    return matches;
}

} // namespace services
} // namespace app
} // namespace yams
