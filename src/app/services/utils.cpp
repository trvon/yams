#include <yams/app/services/services.hpp>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <mutex>
#include <string>

#include <yams/detection/file_type_detector.h>

namespace yams::app::services::utils {

// Simple glob matcher supporting '*' and '?' wildcards.
// - '*' matches zero or more characters
// - '?' matches exactly one character
// Pattern is matched against the entire text.
namespace {

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
    if (out.rfind("**/", 0) == 0) {
        out.erase(0, 3);
    } else if (out.rfind("**\\", 0) == 0) {
        out.erase(0, 3);
    }
    return out;
}

} // namespace

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
    auto toLower = [](std::string in) {
        std::transform(in.begin(), in.end(), in.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return in;
    };

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

} // namespace yams::app::services::utils
