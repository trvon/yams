#include <yams/extraction/title_util.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <vector>

namespace yams::extraction::util {

namespace {

constexpr size_t kMaxTitleLen = 120;

std::string trimCopy(std::string_view input) {
    size_t start = 0;
    size_t end = input.size();
    while (start < end && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return std::string(input.substr(start, end - start));
}

std::string collapseWhitespace(std::string s) {
    std::string out;
    out.reserve(s.size());
    bool inSpace = false;
    for (unsigned char c : s) {
        if (std::isspace(c)) {
            if (!inSpace) {
                out.push_back(' ');
                inSpace = true;
            }
        } else {
            out.push_back(static_cast<char>(c));
            inSpace = false;
        }
    }
    return out;
}

std::string stripCommentPrefix(std::string_view line) {
    std::string s = trimCopy(line);
    if (s.rfind("//", 0) == 0) {
        return trimCopy(std::string_view(s).substr(2));
    }
    if (s.rfind("#", 0) == 0) {
        return trimCopy(std::string_view(s).substr(1));
    }
    if (s.rfind("--", 0) == 0) {
        return trimCopy(std::string_view(s).substr(2));
    }
    if (s.rfind("/*", 0) == 0) {
        s = trimCopy(std::string_view(s).substr(2));
    }
    if (s.rfind("*", 0) == 0) {
        return trimCopy(std::string_view(s).substr(1));
    }
    if (s.rfind("*/", 0) == 0) {
        return trimCopy(std::string_view(s).substr(2));
    }
    return s;
}

} // namespace

std::string normalizeTitleCandidate(std::string s) {
    s = trimCopy(s);
    if (s.empty()) {
        return s;
    }
    s = collapseWhitespace(std::move(s));
    if (s.size() > kMaxTitleLen) {
        s.resize(kMaxTitleLen);
    }
    return s;
}

std::string extractHtmlTitle(std::string_view text) {
    const size_t maxScan = std::min(text.size(), static_cast<size_t>(4096));
    std::string lower;
    lower.reserve(maxScan);
    for (size_t i = 0; i < maxScan; ++i) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(text[i]))));
    }
    const std::string_view lowerView(lower);
    const auto openPos = lowerView.find("<title");
    if (openPos == std::string_view::npos) {
        return {};
    }
    const auto gtPos = lowerView.find('>', openPos);
    if (gtPos == std::string_view::npos) {
        return {};
    }
    const auto closePos = lowerView.find("</title>", gtPos);
    if (closePos == std::string_view::npos) {
        return {};
    }
    const auto start = gtPos + 1;
    const auto len = closePos - start;
    return normalizeTitleCandidate(std::string(text.substr(start, len)));
}

std::string extractMarkdownHeading(std::string_view text) {
    size_t pos = 0;
    size_t lines = 0;
    const size_t maxLines = 200;
    while (pos < text.size() && lines < maxLines) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto line = trimCopy(text.substr(pos, end - pos));
        if (!line.empty()) {
            if (line.rfind("#", 0) == 0) {
                size_t i = 0;
                while (i < line.size() && line[i] == '#') {
                    ++i;
                }
                auto heading = trimCopy(std::string_view(line).substr(i));
                return normalizeTitleCandidate(std::move(heading));
            }
        }
        pos = end + 1;
        ++lines;
    }
    return {};
}

std::string extractCodeSignature(std::string_view text) {
    size_t pos = 0;
    size_t lines = 0;
    const size_t maxLines = 200;
    while (pos < text.size() && lines < maxLines) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto rawLine = text.substr(pos, end - pos);
        auto line = stripCommentPrefix(rawLine);
        if (!line.empty()) {
            static constexpr std::string_view kPrefixes[] = {
                "class ",    "struct ", "interface ", "enum ",    "def ",
                "function ", "fn ",     "module ",    "package ", "namespace "};
            for (const auto& prefix : kPrefixes) {
                if (line.rfind(prefix, 0) == 0) {
                    return normalizeTitleCandidate(std::move(line));
                }
            }
        }
        pos = end + 1;
        ++lines;
    }
    return {};
}

std::string extractFirstMeaningfulLine(std::string_view text) {
    size_t pos = 0;
    size_t lines = 0;
    const size_t maxLines = 200;
    while (pos < text.size() && lines < maxLines) {
        size_t end = text.find('\n', pos);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto rawLine = text.substr(pos, end - pos);
        auto line = stripCommentPrefix(rawLine);
        if (!line.empty()) {
            return normalizeTitleCandidate(std::move(line));
        }
        pos = end + 1;
        ++lines;
    }
    return {};
}

} // namespace yams::extraction::util
