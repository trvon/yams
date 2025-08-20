#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <regex>
#include <sstream>
#include <yams/extraction/html_text_extractor.h>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction {

// Register the HTML extractor
REGISTER_EXTRACTOR(HtmlTextExtractor, ".html", ".htm", ".xhtml");

Result<ExtractionResult> HtmlTextExtractor::extract(const std::filesystem::path& path,
                                                    const ExtractionConfig& config) {
    ExtractionResult result;
    result.extractionMethod = "html_text";

    try {
        // Check file size
        auto fileSize = std::filesystem::file_size(path);
        if (fileSize > config.maxFileSize) {
            return Error{ErrorCode::ResourceExhausted,
                         "File exceeds maximum size: " + std::to_string(fileSize)};
        }

        // Read file content
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            return Error{ErrorCode::FileNotFound, "Cannot open file: " + path.string()};
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string html = buffer.str();

        // Extract text from HTML
        result.text = extractTextFromHtml(html);

        // Extract metadata
        if (config.extractMetadata) {
            result.metadata["filename"] = path.filename().string();
            result.metadata["filepath"] = path.string();
            result.metadata["format"] = "html";

            // Extract title
            std::string title = extractTitle(html);
            if (!title.empty()) {
                result.metadata["title"] = title;
            }

            // Extract meta description
            std::string description = extractMetaDescription(html);
            if (!description.empty()) {
                result.metadata["description"] = description;
            }
        }

        result.success = true;
        return result;

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, "HTML extraction failed: " + std::string(e.what())};
    }
}

Result<ExtractionResult> HtmlTextExtractor::extractFromBuffer(std::span<const std::byte> data,
                                                              const ExtractionConfig& config) {
    ExtractionResult result;
    result.extractionMethod = "html_text_buffer";

    try {
        // Convert bytes to string
        std::string html(reinterpret_cast<const char*>(data.data()), data.size());

        // Extract text from HTML
        result.text = extractTextFromHtml(html);

        // Extract metadata
        if (config.extractMetadata) {
            result.metadata["format"] = "html";

            // Extract title
            std::string title = extractTitle(html);
            if (!title.empty()) {
                result.metadata["title"] = title;
            }

            // Extract meta description
            std::string description = extractMetaDescription(html);
            if (!description.empty()) {
                result.metadata["description"] = description;
            }
        }

        result.success = true;
        return result;

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, "HTML buffer extraction failed: " + std::string(e.what())};
    }
}

std::vector<std::string> HtmlTextExtractor::supportedExtensions() const {
    return {".html", ".htm", ".xhtml"};
}

bool HtmlTextExtractor::canHandle(const std::string& mimeType) const {
    return mimeType == "text/html" || mimeType == "application/xhtml+xml";
}

std::string HtmlTextExtractor::extractTextFromHtml(const std::string& html) {
    if (html.empty()) {
        return "";
    }

    std::string text = html;

    // 1. Remove script and style blocks
    text = removeScriptAndStyle(text);

    // 2. Convert block tags to newlines
    text = convertBlockTagsToNewlines(text);

    // 3. Strip remaining HTML tags
    text = stripHtmlTags(text);

    // 4. Decode HTML entities
    text = decodeHtmlEntities(text);

    // 5. Clean up whitespace
    text = cleanWhitespace(text);

    return text;
}

std::string HtmlTextExtractor::removeScriptAndStyle(const std::string& html) {
    std::string result = html;

    // Remove script blocks (including content)
    std::regex scriptRegex(R"(<script[^>]*>[\s\S]*?</script>)", std::regex::icase);
    result = std::regex_replace(result, scriptRegex, "");

    // Remove style blocks (including content)
    std::regex styleRegex(R"(<style[^>]*>[\s\S]*?</style>)", std::regex::icase);
    result = std::regex_replace(result, styleRegex, "");

    // Remove comments
    std::regex commentRegex(R"(<!--[\s\S]*?-->)");
    result = std::regex_replace(result, commentRegex, "");

    return result;
}

std::string HtmlTextExtractor::convertBlockTagsToNewlines(const std::string& html) {
    std::string result = html;

    // List of block-level tags that should be converted to newlines
    const std::vector<std::string> blockTags = {
        "p",       "div",     "h1",         "h2",     "h3",  "h4",    "h5",  "h6", "ul",
        "ol",      "li",      "blockquote", "pre",    "hr",  "table", "tr",  "td", "th",
        "section", "article", "header",     "footer", "nav", "aside", "main"};

    // Add newlines before and after block tags
    for (const auto& tag : blockTags) {
        // Opening tags
        std::regex openRegex("<" + tag + R"(\s[^>]*>|>)", std::regex::icase);
        result = std::regex_replace(result, openRegex, "\n");

        // Closing tags
        std::regex closeRegex("</" + tag + ">", std::regex::icase);
        result = std::regex_replace(result, closeRegex, "\n");
    }

    // Convert <br> tags to newlines
    std::regex brRegex(R"(<br\s*/?>)", std::regex::icase);
    result = std::regex_replace(result, brRegex, "\n");

    return result;
}

std::string HtmlTextExtractor::stripHtmlTags(const std::string& html) {
    // Remove all remaining HTML tags
    std::regex tagRegex(R"(<[^>]+>)");
    return std::regex_replace(html, tagRegex, "");
}

std::string HtmlTextExtractor::decodeHtmlEntities(const std::string& text) {
    std::string result = text;

    // Common HTML entities
    const std::vector<std::pair<std::string, std::string>> entities = {
        {"&amp;", "&"},      {"&lt;", "<"},   {"&gt;", ">"},     {"&quot;", "\""},
        {"&apos;", "'"},     {"&#39;", "'"},  {"&nbsp;", " "},   {"&ndash;", "-"},
        {"&mdash;", "--"},   {"&copy;", "©"}, {"&reg;", "®"},    {"&trade;", "™"},
        {"&hellip;", "..."}, {"&bull;", "•"}, {"&ldquo;", "\""}, {"&rdquo;", "\""},
        {"&lsquo;", "'"},    {"&rsquo;", "'"}};

    for (const auto& [entity, replacement] : entities) {
        size_t pos = 0;
        while ((pos = result.find(entity, pos)) != std::string::npos) {
            result.replace(pos, entity.length(), replacement);
            pos += replacement.length();
        }
    }

    // Decode numeric entities (&#123;)
    std::regex numericRegex(R"(&#(\d+);)");
    std::smatch numMatch;
    std::string temp = result;
    result.clear();
    auto searchStart = temp.cbegin();
    while (std::regex_search(searchStart, temp.cend(), numMatch, numericRegex)) {
        result.append(searchStart, numMatch[0].first);
        int code = std::stoi(numMatch[1]);
        if (code < 128) {
            result += static_cast<char>(code);
        } else {
            result += numMatch[0].str(); // Keep as-is if not ASCII
        }
        searchStart = numMatch[0].second;
    }
    result.append(searchStart, temp.cend());

    // Decode hex entities (&#xABC;)
    std::regex hexRegex(R"(&#x([0-9A-Fa-f]+);)");
    std::smatch hexMatch;
    temp = result;
    result.clear();
    searchStart = temp.cbegin();
    while (std::regex_search(searchStart, temp.cend(), hexMatch, hexRegex)) {
        result.append(searchStart, hexMatch[0].first);
        int code = std::stoi(hexMatch[1], nullptr, 16);
        if (code < 128) {
            result += static_cast<char>(code);
        } else {
            result += hexMatch[0].str(); // Keep as-is if not ASCII
        }
        searchStart = hexMatch[0].second;
    }
    result.append(searchStart, temp.cend());

    return result;
}

std::string HtmlTextExtractor::cleanWhitespace(const std::string& text) {
    std::string result;
    result.reserve(text.size());

    bool lastWasSpace = false;
    bool lastWasNewline = false;
    int consecutiveNewlines = 0;

    for (char c : text) {
        if (c == '\n' || c == '\r') {
            if (!lastWasNewline) {
                consecutiveNewlines = 1;
                lastWasNewline = true;
                lastWasSpace = false;
            } else {
                consecutiveNewlines++;
            }

            // Limit to maximum 2 consecutive newlines
            if (consecutiveNewlines <= 2) {
                result += '\n';
            }
        } else if (std::isspace(c)) {
            if (!lastWasSpace && !lastWasNewline) {
                result += ' ';
                lastWasSpace = true;
            }
        } else {
            result += c;
            lastWasSpace = false;
            lastWasNewline = false;
            consecutiveNewlines = 0;
        }
    }

    // Trim leading and trailing whitespace
    auto start = result.find_first_not_of(" \n\r\t");
    if (start == std::string::npos) {
        return "";
    }

    auto end = result.find_last_not_of(" \n\r\t");
    return result.substr(start, end - start + 1);
}

std::string HtmlTextExtractor::extractTitle(const std::string& html) {
    std::regex titleRegex(R"(<title[^>]*>(.*?)</title>)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(html, match, titleRegex)) {
        std::string title = match[1];
        // Clean up the title
        title = stripHtmlTags(title);
        title = decodeHtmlEntities(title);
        title = cleanWhitespace(title);
        return title;
    }

    return "";
}

std::string HtmlTextExtractor::extractMetaDescription(const std::string& html) {
    // Try different meta description patterns
    std::vector<std::regex> patterns = {
        std::regex(R"(<meta\s+name=["']description["']\s+content=["']([^"']+)["'])",
                   std::regex::icase),
        std::regex(R"(<meta\s+content=["']([^"']+)["']\s+name=["']description["'])",
                   std::regex::icase),
        std::regex(R"(<meta\s+property=["']og:description["']\s+content=["']([^"']+)["'])",
                   std::regex::icase),
        std::regex(R"(<meta\s+content=["']([^"']+)["']\s+property=["']og:description["'])",
                   std::regex::icase)};

    for (const auto& pattern : patterns) {
        std::smatch match;
        if (std::regex_search(html, match, pattern)) {
            std::string description = match[1];
            description = decodeHtmlEntities(description);
            return description;
        }
    }

    return "";
}

} // namespace yams::extraction