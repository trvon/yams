#include <yams/app/services/services.hpp>

#include <algorithm>
#include <cctype>
#include <string>

namespace yams::app::services::utils {

// Simple glob matcher supporting '*' and '?' wildcards.
// - '*' matches zero or more characters
// - '?' matches exactly one character
// Pattern is matched against the entire text.
bool matchGlob(const std::string& text, const std::string& pattern) {
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

} // namespace yams::app::services::utils
