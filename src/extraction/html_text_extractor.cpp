#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <fstream>
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

    // Safety limit: Skip regex processing on very large HTML to avoid stack overflow
    // Regex backtracking can cause exponential time complexity on large strings
    constexpr size_t MAX_REGEX_SIZE = 5 * 1024 * 1024; // 5MB limit for regex operations
    if (html.size() > MAX_REGEX_SIZE) {
        spdlog::warn(
            "HTML file too large for regex processing ({} bytes), using simple tag stripping",
            html.size());
        // Fallback to simple stripping without regex
        std::string text = html;
        text = removeScriptAndStyle(text);
        text = stripHtmlTags(text); // This doesn't use regex
        text = cleanWhitespace(text);
        return text;
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

namespace {
// Helper for case-insensitive find
size_t find_caseless(const std::string& haystack, const std::string& needle, size_t offset = 0) {
    auto it = std::search(
        haystack.begin() + offset, haystack.end(), needle.begin(), needle.end(),
        [](unsigned char c1, unsigned char c2) { return std::tolower(c1) == std::tolower(c2); });
    if (it == haystack.end()) {
        return std::string::npos;
    }
    return std::distance(haystack.begin(), it);
}
} // namespace

std::string HtmlTextExtractor::removeScriptAndStyle(const std::string& html) {
    std::string result;
    result.reserve(html.size());
    size_t last_pos = 0;

    while (last_pos < html.size()) {
        size_t script_start = find_caseless(html, "<script", last_pos);
        size_t style_start = find_caseless(html, "<style", last_pos);
        size_t comment_start = html.find("<!--", last_pos);

        size_t next_block = std::string::npos;
        if (script_start != std::string::npos)
            next_block = script_start;
        if (style_start != std::string::npos)
            next_block = std::min(next_block, style_start);
        if (comment_start != std::string::npos)
            next_block = std::min(next_block, comment_start);

        if (next_block == std::string::npos) {
            result.append(html, last_pos, std::string::npos);
            break;
        }

        result.append(html, last_pos, next_block - last_pos);

        if (next_block == script_start) {
            size_t end_tag = find_caseless(html, "</script>", next_block);
            if (end_tag == std::string::npos) {
                last_pos = next_block + 1; // Malformed, skip '<'
            } else {
                last_pos = end_tag + 9;
            }
        } else if (next_block == style_start) {
            size_t end_tag = find_caseless(html, "</style>", next_block);
            if (end_tag == std::string::npos) {
                last_pos = next_block + 1;
            } else {
                last_pos = end_tag + 8;
            }
        } else { // comment
            size_t end_tag = html.find("-->", next_block);
            if (end_tag == std::string::npos) {
                last_pos = next_block + 1;
            } else {
                last_pos = end_tag + 3;
            }
        }
    }
    return result;
}

std::string HtmlTextExtractor::convertBlockTagsToNewlines(const std::string& html) {
    // Use robust string parsing instead of regex to avoid stack overflow on large HTML
    std::string result;
    result.reserve(html.size() + html.size() / 10); // Reserve extra space for newlines

    // List of block-level tags that should be converted to newlines
    const std::vector<std::string> blockTags = {
        "p",       "div",     "h1",         "h2",     "h3",  "h4",    "h5",   "h6", "ul",
        "ol",      "li",      "blockquote", "pre",    "hr",  "table", "tr",   "td", "th",
        "section", "article", "header",     "footer", "nav", "aside", "main", "br"};

    size_t pos = 0;
    while (pos < html.size()) {
        if (html[pos] != '<') {
            result += html[pos];
            pos++;
            continue;
        }

        // Found a tag, check if it's a block tag
        size_t tag_end = html.find('>', pos);
        if (tag_end == std::string::npos) {
            result += html[pos];
            pos++;
            continue;
        }

        // Extract the tag content
        std::string tag_content = html.substr(pos + 1, tag_end - pos - 1);

        // Check if it's a closing tag
        bool is_closing = !tag_content.empty() && tag_content[0] == '/';
        if (is_closing) {
            tag_content = tag_content.substr(1);
        }

        // Get the tag name (first word, case-insensitive)
        size_t space_pos = tag_content.find_first_of(" \t\n\r/");
        std::string tag_name = tag_content.substr(0, space_pos);

        // Convert to lowercase for comparison
        std::string tag_lower = tag_name;
        std::transform(tag_lower.begin(), tag_lower.end(), tag_lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        // Check if this is a block tag
        bool is_block_tag =
            std::find(blockTags.begin(), blockTags.end(), tag_lower) != blockTags.end();

        if (is_block_tag) {
            result += '\n';
        }

        pos = tag_end + 1;
    }

    return result;
}

std::string HtmlTextExtractor::stripHtmlTags(const std::string& html) {
    std::string result;
    result.reserve(html.length());
    bool in_tag = false;
    for (char c : html) {
        if (c == '<') {
            in_tag = true;
        } else if (c == '>') {
            in_tag = false;
        } else if (!in_tag) {
            result += c;
        }
    }
    return result;
}

std::string HtmlTextExtractor::decodeHtmlEntities(const std::string& text) {
    std::string result;
    result.reserve(text.size());

    // Common HTML entities
    const std::vector<std::pair<std::string, std::string>> entities = {
        {"&amp;", "&"},      {"&lt;", "<"},   {"&gt;", ">"},     {"&quot;", "\""},
        {"&apos;", "'"},     {"&#39;", "'"},  {"&nbsp;", " "},   {"&ndash;", "-"},
        {"&mdash;", "--"},   {"&copy;", "©"}, {"&reg;", "®"},    {"&trade;", "™"},
        {"&hellip;", "..."}, {"&bull;", "•"}, {"&ldquo;", "\""}, {"&rdquo;", "\""},
        {"&lsquo;", "'"},    {"&rsquo;", "'"}};

    size_t pos = 0;
    while (pos < text.size()) {
        if (text[pos] != '&') {
            result += text[pos];
            pos++;
            continue;
        }

        // Found '&', check for entity
        bool decoded = false;

        // Check for named entities
        for (const auto& [entity, replacement] : entities) {
            if (text.compare(pos, entity.length(), entity) == 0) {
                result += replacement;
                pos += entity.length();
                decoded = true;
                break;
            }
        }

        if (decoded) {
            continue;
        }

        // Check for numeric entities &#123;
        if (pos + 2 < text.size() && text[pos + 1] == '#' && std::isdigit(text[pos + 2])) {
            size_t end = text.find(';', pos + 2);
            if (end != std::string::npos && end - pos < 10) { // Reasonable length
                std::string num_str = text.substr(pos + 2, end - pos - 2);
                try {
                    int code = std::stoi(num_str);
                    if (code > 0 && code < 128) {
                        result += static_cast<char>(code);
                        pos = end + 1;
                        continue;
                    }
                } catch (...) {
                    // Invalid number, keep as-is
                }
            }
        }

        // Check for hex entities &#x1A;
        if (pos + 3 < text.size() && text[pos + 1] == '#' &&
            (text[pos + 2] == 'x' || text[pos + 2] == 'X') && std::isxdigit(text[pos + 3])) {
            size_t end = text.find(';', pos + 3);
            if (end != std::string::npos && end - pos < 12) { // Reasonable length
                std::string hex_str = text.substr(pos + 3, end - pos - 3);
                try {
                    int code = std::stoi(hex_str, nullptr, 16);
                    if (code > 0 && code < 128) {
                        result += static_cast<char>(code);
                        pos = end + 1;
                        continue;
                    }
                } catch (...) {
                    // Invalid hex, keep as-is
                }
            }
        }

        // Not a recognized entity, keep the '&'
        result += text[pos];
        pos++;
    }

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
    // Use robust string parsing instead of regex to avoid stack overflow
    size_t title_start = find_caseless(html, "<title");
    if (title_start == std::string::npos) {
        return "";
    }

    // Find the end of the opening tag
    size_t content_start = html.find('>', title_start);
    if (content_start == std::string::npos) {
        return "";
    }
    content_start++; // Move past the '>'

    // Find the closing tag
    size_t content_end = find_caseless(html, "</title>", content_start);
    if (content_end == std::string::npos) {
        return "";
    }

    // Extract title content
    std::string title = html.substr(content_start, content_end - content_start);

    // Clean up the title
    title = stripHtmlTags(title);
    title = decodeHtmlEntities(title);
    title = cleanWhitespace(title);

    return title;
}

std::string HtmlTextExtractor::extractMetaDescription(const std::string& html) {
    // Use robust string parsing to find meta tags
    // Look for: <meta name="description" content="..."> or variations
    size_t pos = 0;
    while (pos < html.size()) {
        size_t meta_start = find_caseless(html, "<meta", pos);
        if (meta_start == std::string::npos) {
            break;
        }

        // Find the end of this meta tag
        size_t meta_end = html.find('>', meta_start);
        if (meta_end == std::string::npos) {
            break;
        }

        std::string meta_tag = html.substr(meta_start, meta_end - meta_start + 1);
        std::string meta_lower = meta_tag;
        std::transform(meta_lower.begin(), meta_lower.end(), meta_lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        // Check if this is a description meta tag
        bool is_description =
            (meta_lower.find("name=\"description\"") != std::string::npos ||
             meta_lower.find("name='description'") != std::string::npos ||
             meta_lower.find("property=\"og:description\"") != std::string::npos ||
             meta_lower.find("property='og:description'") != std::string::npos);

        if (is_description) {
            // Extract the content attribute value
            size_t content_pos = meta_lower.find("content=");
            if (content_pos != std::string::npos) {
                content_pos += 8; // Skip "content="

                // Find the quote character
                while (content_pos < meta_tag.size() && std::isspace(meta_tag[content_pos])) {
                    content_pos++;
                }

                if (content_pos < meta_tag.size()) {
                    char quote = meta_tag[content_pos];
                    if (quote == '"' || quote == '\'') {
                        content_pos++; // Skip opening quote
                        size_t end_quote = meta_tag.find(quote, content_pos);
                        if (end_quote != std::string::npos) {
                            std::string description =
                                meta_tag.substr(content_pos, end_quote - content_pos);
                            description = decodeHtmlEntities(description);
                            return description;
                        }
                    }
                }
            }
        }

        pos = meta_end + 1;
    }

    return "";
}

} // namespace yams::extraction