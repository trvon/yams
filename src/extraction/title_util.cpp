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

DocumentSections detectDocumentSections(std::string_view text) {
    DocumentSections result;
    if (text.empty())
        return result;

    // Language-agnostic section detection using structural heuristics:
    //   - Section header: short line (≤8 words), standalone, often numbered/ALL_CAPS
    //   - Title: first 1-3 meaningful lines
    //   - Abstract: first substantial block after title, before first major section
    //   - Body: everything else
    // No language-specific word matching — works on any language.

    constexpr std::size_t kMaxScan = 16384;
    const std::size_t scanEnd = std::min(text.size(), kMaxScan);

    struct Line {
        std::size_t start;
        std::size_t end; // position after newline
        std::size_t wordCount;
        bool allCaps;
        bool hasNumbering;
        bool isEmpty;
    };
    std::vector<Line> lines;

    // Parse lines in the scan window
    {
        std::size_t pos = 0;
        while (pos < scanEnd && lines.size() < 200) {
            auto nl = text.find('\n', pos);
            if (nl == std::string_view::npos)
                nl = scanEnd;
            if (nl > pos) {
                Line l;
                l.start = pos;
                l.end = nl + 1;
                l.wordCount = 0;
                l.allCaps = true;
                l.hasNumbering = false;
                l.isEmpty = true;
                bool inWord = false;
                bool hasLetter = false;
                for (std::size_t i = pos; i < nl; ++i) {
                    unsigned char c = static_cast<unsigned char>(text[i]);
                    if (std::isspace(c)) {
                        inWord = false;
                    } else {
                        if (!inWord) {
                            ++l.wordCount;
                            inWord = true;
                        }
                        l.isEmpty = false;
                        if (std::isdigit(c))
                            l.hasNumbering = true;
                        if (std::isalpha(c)) {
                            hasLetter = true;
                            if (!std::isupper(c))
                                l.allCaps = false;
                        }
                    }
                }
                if (!hasLetter)
                    l.allCaps = false;
                lines.push_back(l);
            }
            pos = nl + 1;
        }
    }

    if (lines.empty()) {
        result.body = std::string(text);
        return result;
    }

    // Detect section-header lines: short (1-8 words), not empty,
    // optionally numbered or ALL_CAPS.
    auto is_section_header = [](const Line& l) -> bool {
        if (l.isEmpty || l.wordCount > 8)
            return false;
        if (l.allCaps && l.wordCount >= 1 && l.wordCount <= 6)
            return true;
        if (l.hasNumbering && l.wordCount >= 1 && l.wordCount <= 8)
            return true;
        if (l.wordCount >= 1 && l.wordCount <= 4)
            return true;
        return false;
    };

    // Find the first section-header line
    std::size_t firstHeaderIdx = lines.size();
    for (std::size_t i = 0; i < lines.size(); ++i) {
        if (is_section_header(lines[i]) && i > 0 && lines[i - 1].isEmpty) {
            firstHeaderIdx = i;
            break;
        }
    }

    // If we found a section header, title is everything before it
    if (firstHeaderIdx < lines.size()) {
        std::string titleText;
        for (std::size_t i = 0; i < firstHeaderIdx; ++i) {
            if (!lines[i].isEmpty) {
                if (!titleText.empty())
                    titleText += ' ';
                titleText +=
                    trimCopy(text.substr(lines[i].start, lines[i].end - lines[i].start - 1));
                if (lines[i].wordCount >= 4)
                    break; // stop after first substantial line
            }
        }
        result.title = normalizeTitleCandidate(std::move(titleText));

        // Walk sections: each header + its content block
        std::size_t secStart = lines[firstHeaderIdx].start;
        std::string secName;
        std::size_t secIdx = 0;
        for (std::size_t i = firstHeaderIdx; i < lines.size(); ++i) {
            if (is_section_header(lines[i]) && i > firstHeaderIdx && lines[i - 1].isEmpty) {
                // End previous section
                std::size_t secEnd = lines[i].start;
                std::string sectionText = trimCopy(text.substr(secStart, secEnd - secStart));
                if (!sectionText.empty()) {
                    result.sections.push_back(
                        {secName.empty() ? std::string("sec" + std::to_string(secIdx)) : secName,
                         secStart, secEnd});
                    ++secIdx;
                }
                // Start new section
                secStart = lines[i].start;
                secName.clear();
            }
            if (secStart == lines[i].start && lines[i].wordCount >= 1 && lines[i].wordCount <= 8) {
                secName = trimCopy(text.substr(lines[i].start, lines[i].end - lines[i].start - 1));
            }
        }
        // Final section
        std::size_t finalEnd = std::min(scanEnd, text.size());
        std::string sectionText = trimCopy(text.substr(secStart, finalEnd - secStart));
        if (!sectionText.empty()) {
            result.sections.push_back(
                {secName.empty() ? std::string("sec" + std::to_string(secIdx)) : secName, secStart,
                 finalEnd});
        }

        // Extract abstract: first content block after title, before any section header
        // Usually section 0 or the text between title and first header
        if (!result.sections.empty() && result.abstract.empty()) {
            const auto& first = result.sections[0];
            std::string firstBlock =
                trimCopy(text.substr(first.startOffset, first.endOffset - first.startOffset));
            if (firstBlock.size() > 40 && firstBlock.size() < 4000) {
                result.abstract = collapseWhitespace(std::move(firstBlock));
            }
        }
    } else {
        // No section headers found — use first-line-as-title heuristic
        result.title = extractFirstMeaningfulLine(text);
        result.body = std::string(text);
    }

    // Body: everything after the title region
    if (result.body.empty() && !result.title.empty()) {
        auto titlePos = text.find(result.title);
        if (titlePos != std::string_view::npos) {
            std::size_t bodyStart = titlePos + result.title.size();
            while (bodyStart < text.size() &&
                   std::isspace(static_cast<unsigned char>(text[bodyStart])))
                ++bodyStart;
            if (bodyStart < text.size())
                result.body = collapseWhitespace(trimCopy(text.substr(bodyStart)));
        }
    }

    return result;
}

} // namespace yams::extraction::util
