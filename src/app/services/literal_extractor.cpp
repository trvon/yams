#include <optional>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <yams/app/services/literal_extractor.hpp>
#include <yams/app/services/simd_memmem.hpp>
#include <yams/common/string_utils.h>

namespace yams {
namespace app {
namespace services {

bool LiteralExtractor::isMetaChar(char c) {
    return yams::common::isRegexMetaChar(c);
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

namespace {

// Index just past the character class starting at `pos` ('['). A ']' right after '[' or "[^" is
// a member, and escapes are honoured. An unterminated class runs to the end.
std::size_t skipCharacterClass(std::string_view p, std::size_t pos) {
    std::size_t i = pos + 1;
    if (i < p.size() && p[i] == '^')
        ++i;
    if (i < p.size() && p[i] == ']')
        ++i;
    while (i < p.size()) {
        if (p[i] == '\\') {
            i += 2;
            continue;
        }
        if (p[i] == ']')
            return i + 1;
        ++i;
    }
    return p.size();
}

// Index just past the group starting at `pos` ('('), counting nesting and skipping escapes and
// classes. An unbalanced group runs to the end.
std::size_t skipGroup(std::string_view p, std::size_t pos) {
    int depth = 0;
    std::size_t i = pos;
    while (i < p.size()) {
        const char c = p[i];
        if (c == '\\') {
            i += 2;
            continue;
        }
        if (c == '[') {
            i = skipCharacterClass(p, i);
            continue;
        }
        if (c == '(')
            ++depth;
        else if (c == ')' && --depth == 0)
            return i + 1;
        ++i;
    }
    return p.size();
}

bool hasTopLevelAlternation(std::string_view p) {
    for (std::size_t i = 0; i < p.size();) {
        const char c = p[i];
        if (c == '\\') {
            i += 2;
        } else if (c == '[') {
            i = skipCharacterClass(p, i);
        } else if (c == '(') {
            i = skipGroup(p, i);
        } else if (c == '|') {
            return true;
        } else {
            ++i;
        }
    }
    return false;
}

struct Quantifier {
    std::size_t length = 0; // 0: none
    bool allowsZero = false;
};

// A quantifier at `pos`: *, +, ?, {n}, {n,}, {n,m}, each optionally lazy or possessive.
Quantifier parseQuantifier(std::string_view p, std::size_t pos) {
    Quantifier q;
    if (pos >= p.size())
        return q;
    const char c = p[pos];
    if (c == '*' || c == '?') {
        q = {1, true};
    } else if (c == '+') {
        q = {1, false};
    } else if (c == '{') {
        std::size_t i = pos + 1;
        std::size_t digits = 0;
        unsigned long minimum = 0;
        while (i < p.size() && std::isdigit(static_cast<unsigned char>(p[i]))) {
            minimum = minimum * 10 + static_cast<unsigned long>(p[i] - '0');
            ++i;
            ++digits;
        }
        if (digits == 0)
            return q; // not a counted repetition; '{' is handled as its own atom
        if (i < p.size() && p[i] == ',') {
            ++i;
            while (i < p.size() && std::isdigit(static_cast<unsigned char>(p[i])))
                ++i;
        }
        if (i >= p.size() || p[i] != '}')
            return q;
        q = {i + 1 - pos, minimum == 0};
    }
    if (q.length > 0 && pos + q.length < p.size() &&
        (p[pos + q.length] == '?' || p[pos + q.length] == '+'))
        ++q.length;
    return q;
}

} // namespace

// Literals every match must contain. Grep requires the longest of each line, and the trigram
// index requires all of them of each document, so anything a match could avoid (one side of an
// alternation, an optional atom, a class or group's contents) must be left out.
LiteralExtractor::ExtractionResult LiteralExtractor::extract(std::string_view pattern,
                                                             bool ignoreCase) {
    ExtractionResult result;
    result.isComplete = true;
    result.longestLength = 0;

    if (hasTopLevelAlternation(pattern)) {
        result.isComplete = false;
        return result;
    }

    std::string current;
    const auto flush = [&] {
        if (!current.empty()) {
            result.literals.push_back(current);
            current.clear();
        }
    };

    for (std::size_t i = 0; i < pattern.size();) {
        const char c = pattern[i];
        std::optional<char> atom; // the literal character this atom matches, if it is one
        std::size_t next = i + 1;
        if (c == '\\') {
            if (i + 1 < pattern.size()) {
                const char escaped = pattern[i + 1];
                next = i + 2;
                // \d \w \s \b, backreferences and other letter/digit escapes are not literal.
                if (!std::isalnum(static_cast<unsigned char>(escaped)))
                    atom = escaped;
            }
        } else if (c == '[') {
            next = skipCharacterClass(pattern, i);
        } else if (c == '(') {
            next = skipGroup(pattern, i);
        } else if (!isMetaChar(c)) {
            atom = c;
        }
        i = next;

        const auto quantifier = parseQuantifier(pattern, i);
        if (quantifier.length > 0) {
            i += quantifier.length;
            result.isComplete = false;
            // x+ and x{2,} still need one x where the run ends; x? x* x{0,} need none.
            if (atom && !quantifier.allowsZero)
                current += *atom;
            flush();
            continue;
        }
        if (atom) {
            current += *atom;
        } else {
            result.isComplete = false;
            flush();
        }
    }
    flush();

    for (const auto& lit : result.literals) {
        result.longestLength = std::max(result.longestLength, lit.size());
    }
    if (ignoreCase) {
        for (auto& lit : result.literals) {
            std::transform(lit.begin(), lit.end(), lit.begin(),
                           [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        }
    }
    return result;
}

// --- Boyer-Moore-Horspool Implementation ---

BMHSearcher::BMHSearcher(std::string_view pattern, bool ignoreCase)
    : pattern_(pattern), ignoreCase_(ignoreCase), useFastPath_(false) {
    if (ignoreCase_) {
        std::transform(pattern_.begin(), pattern_.end(), pattern_.begin(),
                       [](unsigned char c) { return std::tolower(c); });
    }
    if (pattern_.size() < kMinBMHLength) {
        useFastPath_ = true;
        return;
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
        const size_t result =
            simdMemmemCI(text.data() + startPos, n - startPos, pattern_.data(), m);
        return (result == kMemmemNpos) ? std::string::npos : startPos + result;
    }

    return text.find(pattern_, startPos);
}

size_t BMHSearcher::findBMH(std::string_view text, size_t startPos) const {
    const size_t m = pattern_.size();
    const size_t n = text.size();

    if (m == 0 || n == 0 || m > n || startPos > n - m) {
        return std::string::npos;
    }

    if (ignoreCase_) {
        const size_t result =
            simdMemmemCI(text.data() + startPos, n - startPos, pattern_.data(), m);
        return (result == kMemmemNpos) ? std::string::npos : startPos + result;
    }

    return text.find(pattern_, startPos);
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
