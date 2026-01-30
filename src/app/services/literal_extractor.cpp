#include <algorithm>
#include <cctype>
#include <cstring>
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
                (void)currentLiteral.clear();
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
    : pattern_(pattern), ignoreCase_(ignoreCase), useFastPath_(false) {
    if (pattern_.size() < kMinBMHLength) {
        useFastPath_ = true;
        return;
    }
    if (ignoreCase_) {
        std::transform(pattern_.begin(), pattern_.end(), pattern_.begin(),
                       [](unsigned char c) { return std::tolower(c); });
    }
    buildShiftTable();
}

void BMHSearcher::buildShiftTable() {
    const size_t m = pattern_.size();

    shift_.fill(m);

    for (size_t i = 0; i < m - 1; ++i) {
        unsigned char c = static_cast<unsigned char>(pattern_[i]);
        shift_[c] = m - 1 - i;
    }
}

unsigned char BMHSearcher::toLower(unsigned char c) {
    return static_cast<unsigned char>(std::tolower(c));
}

size_t BMHSearcher::find(std::string_view text, size_t startPos) const {
    if (useFastPath_) {
        return findFast(text, startPos);
    }
    return findBMH(text, startPos);
}

size_t BMHSearcher::findFast(std::string_view text, size_t startPos) const {
    const size_t m = pattern_.size();
    const size_t n = text.size();

    if (m == 0 || n == 0 || m > n || startPos > n - m) {
        return std::string::npos;
    }

    if (ignoreCase_) {
        std::string textStr(text);
        std::transform(textStr.begin(), textStr.end(), textStr.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return textStr.find(pattern_, startPos);
    }

    const char* base = text.data();
    const char* scan = base + startPos;
    size_t remaining = n - startPos;
    const char first = pattern_.front();

    while (remaining >= m) {
        const void* found = std::memchr(scan, first, remaining - m + 1);
        if (!found) {
            return std::string::npos;
        }
        const char* candidate = static_cast<const char*>(found);
        if (std::memcmp(candidate, pattern_.data(), m) == 0) {
            return static_cast<size_t>(candidate - base);
        }
        const size_t consumed = static_cast<size_t>(candidate - scan) + 1;
        scan = candidate + 1;
        remaining -= consumed;
    }

    return std::string::npos;
}

size_t BMHSearcher::findBMH(std::string_view text, size_t startPos) const {
    const size_t m = pattern_.size();
    const size_t n = text.size();

    if (m == 0 || n == 0 || m > n || startPos > n - m) {
        return std::string::npos;
    }

    size_t pos = startPos;

    while (pos <= n - m) {
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
            return pos;
        }

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
            pos += 1;
        }
    }

    return matches;
}

} // namespace services
} // namespace app
} // namespace yams
