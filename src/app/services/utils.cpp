#include <yams/app/services/services.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <yams/common/string_utils.h>
#include <yams/detection/file_type_detector.h>

namespace yams::app::services::utils {

// Simple glob matcher supporting '*' and '?' wildcards.
// - '*' matches zero or more characters
// - '?' matches exactly one character
// Pattern is matched against the entire text.
namespace {

std::string trimCopy(const std::string& value) {
    const auto isSpace = [](unsigned char c) { return std::isspace(c) != 0; };
    auto begin = std::find_if_not(value.begin(), value.end(), isSpace);
    auto end = std::find_if_not(value.rbegin(), value.rend(), isSpace).base();
    if (begin >= end) {
        return {};
    }
    return std::string(begin, end);
}

std::int64_t nowEpochSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool allDigits(const std::string& value) {
    return !value.empty() && std::all_of(value.begin(), value.end(),
                                         [](unsigned char c) { return std::isdigit(c) != 0; });
}

std::vector<std::string> querySnippetTerms(std::string_view query) {
    static const std::unordered_set<std::string> kStopWords = {
        "and", "does", "for", "from", "how", "the", "this", "what", "when", "where", "with"};
    constexpr std::size_t kMaxTerms = 16;
    std::vector<std::string> terms;
    std::unordered_set<std::string> seen;
    std::string current;
    const auto flush = [&]() {
        if (current.size() >= 2 && !kStopWords.contains(current) && seen.insert(current).second) {
            terms.push_back(current);
        }
        current.clear();
    };
    for (const unsigned char c : query) {
        if (std::isalnum(c) != 0 || c == '_') {
            current.push_back(static_cast<char>(std::tolower(c)));
        } else {
            flush();
            if (terms.size() >= kMaxTerms) {
                return terms;
            }
        }
    }
    flush();
    if (terms.size() > kMaxTerms) {
        terms.resize(kMaxTerms);
    }
    return terms;
}

std::string lowerAsciiPrefix(std::string_view content, std::size_t limit) {
    std::string lower;
    lower.reserve(limit);
    for (std::size_t i = 0; i < limit; ++i) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(content[i]))));
    }
    return lower;
}

Result<std::int64_t> parseIso8601ish(std::string value) {
    // Accept basic forms:
    // YYYY-MM-DD
    // YYYY-MM-DD HH:MM:SS
    // YYYY-MM-DDTHH:MM:SS[Z]
    value = trimCopy(value);
    if (!value.empty() && (value.back() == 'Z' || value.back() == 'z')) {
        value.pop_back();
    }

    std::replace(value.begin(), value.end(), 'T', ' ');

    std::tm tm{};
    {
        std::istringstream ss(value);
        if (value.size() <= 10) {
            ss >> std::get_time(&tm, "%Y-%m-%d");
        } else {
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        }
        if (ss.fail()) {
            return Error{ErrorCode::InvalidArgument,
                         "Invalid time expression: expected ISO 8601/date format"};
        }
    }

#if defined(_WIN32)
    std::time_t t = _mkgmtime(&tm);
#else
    std::time_t t = timegm(&tm);
#endif
    if (t == static_cast<std::time_t>(-1)) {
        return Error{ErrorCode::InvalidArgument, "Invalid time expression: out of range"};
    }
    return static_cast<std::int64_t>(t);
}

bool hasPathWildcards(const std::string& path) {
    for (char c : path) {
        if (c == '*' || c == '?' || c == '[' || c == ']')
            return true;
    }
    return false;
}

} // namespace

namespace {

bool matchGlobRaw(const std::string& text, const std::string& pattern) {
    const char* s = text.c_str();
    const char* p = pattern.c_str();
    const char* star = nullptr;
    const char* ss = nullptr;

    while (*s) {
        if (*p == '?' || *p == *s) {
            ++s;
            ++p;
        } else if (*p == '*') {
            star = p++;
            ss = s;
        } else if (star) {
            p = star + 1;
            s = ++ss;
        } else {
            return false;
        }
    }
    while (*p == '*')
        ++p;
    return *p == '\0';
}

bool containsDoubleStarDir(std::string_view pattern) {
    return pattern.find("/**/") != std::string_view::npos ||
           pattern.find("\\**\\") != std::string_view::npos || pattern.rfind("**/", 0) == 0 ||
           pattern.rfind("**\\", 0) == 0;
}

std::string collapseDoubleStarDir(const std::string& pattern) {
    std::string out = pattern;
    std::size_t pos = 0;
    while ((pos = out.find("/**/", pos)) != std::string::npos) {
        out.replace(pos, 4, "/");
        pos += 1;
    }
    pos = 0;
    while ((pos = out.find("\\**\\", pos)) != std::string::npos) {
        out.replace(pos, 4, "\\");
        pos += 1;
    }
    if (out.rfind("**/", 0) == 0 || out.rfind("**\\", 0) == 0) {
        out.erase(0, 3);
    }
    return out;
}

} // namespace

Result<std::int64_t> parseTimeExpression(const std::string& timeExpr) {
    const std::string trimmed = trimCopy(timeExpr);
    if (trimmed.empty()) {
        return Error{ErrorCode::InvalidArgument, "Time expression is empty"};
    }

    const std::string lowered = yams::common::asciiToLowerCopy(trimmed);
    const std::int64_t now = nowEpochSeconds();

    if (lowered == "now") {
        return now;
    }
    constexpr std::int64_t kSecPerMinute = 60;
    constexpr std::int64_t kSecPerHour = 60 * kSecPerMinute;
    constexpr std::int64_t kSecPerDay = 24 * kSecPerHour;
    constexpr std::int64_t kSecPerWeek = 7 * kSecPerDay;
    constexpr std::int64_t kSecPerMonth = 30 * kSecPerDay;
    constexpr std::int64_t kSecPerYear = 365 * kSecPerDay;

    if (lowered == "today") {
        return now;
    }
    if (lowered == "yesterday") {
        return now - kSecPerDay;
    }
    if (lowered == "last week") {
        return now - kSecPerWeek;
    }
    if (lowered == "last month") {
        return now - kSecPerMonth;
    }
    if (lowered == "last year") {
        return now - kSecPerYear;
    }

    if (allDigits(trimmed)) {
        try {
            return std::stoll(trimmed);
        } catch (...) {
            return Error{ErrorCode::InvalidArgument, "Invalid numeric epoch time"};
        }
    }

    // Relative format: <number><unit> where unit in [s,m,h,d,w]
    // e.g. 30m, 2h, 7d
    if (trimmed.size() >= 2) {
        const char unit = static_cast<char>(std::tolower(trimmed.back()));
        const std::string numberPart = trimmed.substr(0, trimmed.size() - 1);
        if (allDigits(numberPart)) {
            try {
                const std::int64_t magnitude = std::stoll(numberPart);
                std::int64_t seconds = 0;
                switch (unit) {
                    case 's':
                        seconds = magnitude;
                        break;
                    case 'm':
                        seconds = magnitude * kSecPerMinute;
                        break;
                    case 'h':
                        seconds = magnitude * kSecPerHour;
                        break;
                    case 'd':
                        seconds = magnitude * kSecPerDay;
                        break;
                    case 'w':
                        seconds = magnitude * kSecPerWeek;
                        break;
                    default:
                        seconds = 0;
                        break;
                }
                if (seconds > 0) {
                    return now - seconds;
                }
            } catch (...) {
                return Error{ErrorCode::InvalidArgument,
                             "Invalid relative time expression magnitude"};
            }
        }
    }

    return parseIso8601ish(trimmed);
}

bool matchGlob(const std::string& text, const std::string& pattern) {
    if (matchGlobRaw(text, pattern))
        return true;
    if (containsDoubleStarDir(pattern)) {
        auto collapsed = collapseDoubleStarDir(pattern);
        if (collapsed != pattern && matchGlobRaw(text, collapsed))
            return true;
    }
    return false;
}

// Create a short content snippet with basic cleanup and optional word-boundary preservation.
std::string createSnippet(const std::string& content, size_t maxLength, bool preserveWordBoundary) {
    if (content.empty() || maxLength == 0)
        return std::string();

    // Collapse whitespace and strip control chars for a compact snippet
    std::string cleaned;
    cleaned.reserve(std::min<size_t>(content.size(), maxLength * 2));
    bool lastWasSpace = false;
    for (char ch : content) {
        unsigned char c = static_cast<unsigned char>(ch);
        if (c == '\n' || c == '\r' || c == '\t' || std::isspace(c)) {
            if (!lastWasSpace) {
                cleaned.push_back(' ');
                lastWasSpace = true;
            }
        } else if (std::isprint(c)) {
            cleaned.push_back(static_cast<char>(c));
            lastWasSpace = false;
        }
        if (cleaned.size() > maxLength * 2)
            break; // safety bound
    }

    if (cleaned.size() <= maxLength)
        return cleaned;

    // Truncate with optional word boundary preservation
    size_t cut = maxLength;
    if (preserveWordBoundary) {
        // Try to find the last space within the last 30% of the window
        size_t windowStart = static_cast<size_t>(maxLength * 0.7);
        size_t pos = cleaned.rfind(' ', maxLength);
        if (pos != std::string::npos && pos >= windowStart) {
            cut = pos;
        }
    }
    std::string out = cleaned.substr(0, cut);
    // Trim trailing spaces
    while (!out.empty() && std::isspace(static_cast<unsigned char>(out.back())))
        out.pop_back();
    out.append("...");
    return out;
}

QuerySnippetResult createQuerySnippet(const std::string& content, const std::string& query,
                                      size_t maxLength, bool preserveWordBoundary) {
    if (content.empty() || maxLength == 0) {
        return {};
    }
    const auto terms = querySnippetTerms(query);
    if (terms.empty()) {
        return {.text = createSnippet(content, maxLength, preserveWordBoundary)};
    }

    constexpr std::size_t kMaxScanBytes = 128 * 1024;
    constexpr std::size_t kMaxOccurrencesPerTerm = 32;
    const auto scanBytes = std::min(content.size(), kMaxScanBytes);
    const auto lowerContent = lowerAsciiPrefix(content, scanBytes);
    const auto scaledCoverage = maxLength >= kMaxScanBytes / 3 ? kMaxScanBytes : maxLength * 3;
    const auto coverageWindow = std::max<std::size_t>(256, scaledCoverage);

    std::optional<std::size_t> bestPosition;
    std::size_t bestCoverage = 0;
    std::size_t bestMatchedChars = 0;
    for (const auto& anchorTerm : terms) {
        std::size_t offset = 0;
        for (std::size_t occurrence = 0; occurrence < kMaxOccurrencesPerTerm; ++occurrence) {
            const auto position = lowerContent.find(anchorTerm, offset);
            if (position == std::string::npos) {
                break;
            }
            const auto windowStart =
                position > coverageWindow / 4 ? position - coverageWindow / 4 : 0;
            const auto windowEnd = std::min(scanBytes, windowStart + coverageWindow);
            std::size_t coverage = 0;
            std::size_t matchedChars = 0;
            std::size_t firstMatch = windowEnd;
            for (const auto& term : terms) {
                const auto match = lowerContent.find(term, windowStart);
                if (match != std::string::npos && match < windowEnd) {
                    ++coverage;
                    matchedChars += term.size();
                    firstMatch = std::min(firstMatch, match);
                }
            }
            if (!bestPosition.has_value() || coverage > bestCoverage ||
                (coverage == bestCoverage && matchedChars > bestMatchedChars)) {
                bestPosition = firstMatch;
                bestCoverage = coverage;
                bestMatchedChars = matchedChars;
            }
            offset = position + 1;
        }
    }
    if (!bestPosition.has_value()) {
        return {.text = createSnippet(content, maxLength, preserveWordBoundary)};
    }

    auto snippetStart = *bestPosition > maxLength / 4 ? *bestPosition - maxLength / 4 : 0;
    if (const auto lineBreak = content.rfind('\n', *bestPosition);
        lineBreak != std::string::npos && *bestPosition - lineBreak <= maxLength / 3) {
        snippetStart = lineBreak + 1;
    }
    const auto scaledSlice = maxLength >= kMaxScanBytes / 2 ? kMaxScanBytes : maxLength * 2;
    const auto sliceLength = std::min(content.size() - snippetStart, scaledSlice);
    auto snippet =
        createSnippet(content.substr(snippetStart, sliceLength), maxLength, preserveWordBoundary);
    if (snippetStart != 0 && !snippet.empty()) {
        snippet.insert(0, "... ");
    }
    return {.text = std::move(snippet),
            .sourceOffset = snippetStart,
            .matchedTerms = bestCoverage,
            .queryMatched = true};
}

NormalizedLookupPath normalizeLookupPath(const std::string& path) {
    NormalizedLookupPath out;
    out.original = path;
    out.normalized = path;

    if (path.empty() || path == "-")
        return out;

    if (hasPathWildcards(path)) {
        out.hasWildcards = true;

        // Attempt to canonicalize the directory portion before the first wildcard to ensure
        // paths under symlinked locations (e.g., /var -> /private/var) still align with stored
        // canonical paths. Best-effort: if we cannot resolve the prefix, fall back to the
        // original pattern.
        const std::string wildcardChars = "*?";
        const auto firstWildcard = path.find_first_of(wildcardChars);
        if (firstWildcard == std::string::npos)
            return out;

        const auto lastSep = path.find_last_of("/\\", firstWildcard);
        if (lastSep == std::string::npos)
            return out; // no directory component to normalize

        std::string dirPart = path.substr(0, lastSep + 1);
        std::string remainder = path.substr(lastSep + 1);

        namespace fs = std::filesystem;
        std::error_code ec;
        fs::path dirPath{dirPart};

        if (!dirPath.is_absolute()) {
            auto abs = fs::absolute(dirPath, ec);
            if (!ec)
                dirPath = abs;
        }

        auto canon = fs::weakly_canonical(dirPath, ec);
        if (ec || canon.empty())
            return out; // cannot canonicalize; keep original pattern

        auto preferred = canon.make_preferred().string();
        if (preferred.empty())
            return out;

        if (preferred.back() != fs::path::preferred_separator)
            preferred.push_back(fs::path::preferred_separator);

        out.normalized = preferred + remainder;
        out.changed = (out.normalized != out.original);
        return out;
    }

    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p{path};

    if (!p.is_absolute()) {
        auto abs = fs::absolute(p, ec);
        if (!ec)
            p = abs;
    }

    auto canon = fs::weakly_canonical(p, ec);
    if (!ec && !canon.empty())
        p = canon;

    auto preferred = p.make_preferred().string();
    if (!preferred.empty() && preferred != path) {
        out.normalized = preferred;
        out.changed = true;
    }

    return out;
}

std::string classifyFileType(const std::string& mimeType, const std::string& extension) {
    auto toLower = [](std::string in) { return yams::common::asciiToLowerCopy(std::move(in)); };

    std::string normalizedExt = extension;
    if (!normalizedExt.empty() && normalizedExt.front() != '.')
        normalizedExt.insert(normalizedExt.begin(), '.');
    normalizedExt = toLower(normalizedExt);

    std::string mime = toLower(mimeType);
    if (mime.empty()) {
        mime = yams::detection::FileTypeDetector::getMimeTypeFromExtension(normalizedExt);
    }

    std::string category;
    try {
        // Ensure detector has patterns available (safe to call repeatedly)
        (void)yams::detection::FileTypeDetector::initializeWithMagicNumbers();
        auto& detector = yams::detection::FileTypeDetector::instance();
        category = detector.getFileTypeCategory(mime);
        if (category.empty()) {
            if (detector.isTextMimeType(mime))
                category = "text";
            else if (detector.isBinaryMimeType(mime))
                category = "binary";
        }
    } catch (...) {
        // MIME detection is best-effort; fall back to extension heuristics below.
    }

    if (!category.empty())
        return category;

    if (!mime.empty()) {
        auto slash = mime.find('/');
        if (slash != std::string::npos && slash > 0)
            return mime.substr(0, slash);
        return mime;
    }

    if (!normalizedExt.empty())
        return normalizedExt.substr(1);

    return "unknown";
}

std::vector<std::string> buildCwdScopePatterns(const std::string& directory) {
    std::vector<std::string> patterns;
    if (directory.empty())
        return patterns;

    std::error_code ec;
    std::filesystem::path base{directory};
    if (!base.is_absolute()) {
        base = std::filesystem::current_path(ec) / base;
    }
    auto resolved = std::filesystem::weakly_canonical(base, ec);
    if (ec)
        resolved = base;

    std::string prefix = resolved.string();
    std::replace(prefix.begin(), prefix.end(), '\\', '/');
    if (!prefix.empty() && prefix.back() != '/') {
        prefix += '/';
    }

    // Absolute path variant
    patterns.push_back(prefix + "**/*");

    // Without leading slash (repo-relative paths)
    std::string noLeadingSlash = prefix;
    if (!noLeadingSlash.empty() && noLeadingSlash.front() == '/') {
        noLeadingSlash.erase(noLeadingSlash.begin());
    }
    if (!noLeadingSlash.empty()) {
        patterns.push_back(noLeadingSlash + "**/*");
    }

    // Basename variant (e.g. "myproject/**/*")
    auto baseName = std::filesystem::path(prefix).filename().string();
    if (!baseName.empty()) {
        patterns.push_back(baseName + "/**/*");
    }

    return patterns;
}

} // namespace yams::app::services::utils
