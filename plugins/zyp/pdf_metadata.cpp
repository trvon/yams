/**
 * @file pdf_metadata.cpp
 * @brief Minimal PDF metadata extraction implementation
 */

#include "pdf_metadata.h"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <string_view>

namespace yams::zyp {

namespace {

const uint8_t* findBytes(const uint8_t* haystack, size_t haystackLen, const uint8_t* needle,
                         size_t needleLen) {
    if (needleLen == 0 || haystackLen < needleLen) {
        return nullptr;
    }

    const auto* haystackEnd = haystack + haystackLen;
    auto it = std::search(haystack, haystackEnd, needle, needle + needleLen);
    if (it == haystackEnd) {
        return nullptr;
    }
    return it;
}

/**
 * Find a byte sequence in data, searching backwards from the end.
 */
const uint8_t* findBackwards(std::span<const uint8_t> data, std::string_view needle) {
    if (data.size() < needle.size()) {
        return nullptr;
    }

    const auto* needleData = reinterpret_cast<const uint8_t*>(needle.data());
    const auto* start = data.data();
    const auto* end = data.data() + data.size() - needle.size();

    for (const auto* p = end; p >= start; --p) {
        if (std::memcmp(p, needleData, needle.size()) == 0) {
            return p;
        }
    }
    return nullptr;
}

/**
 * Skip whitespace characters.
 */
const uint8_t* skipWhitespace(const uint8_t* p, const uint8_t* end) {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) {
        ++p;
    }
    return p;
}

/**
 * Parse a PDF string (parentheses or hex).
 * Returns the string content without delimiters.
 */
std::optional<std::string> parsePdfString(const uint8_t*& p, const uint8_t* end) {
    p = skipWhitespace(p, end);
    if (p >= end) {
        return std::nullopt;
    }

    if (*p == '(') {
        // Literal string
        ++p;
        std::string result;
        int parenDepth = 1;

        while (p < end && parenDepth > 0) {
            if (*p == '\\' && p + 1 < end) {
                // Escape sequence
                ++p;
                switch (*p) {
                    case 'n':
                        result += '\n';
                        break;
                    case 'r':
                        result += '\r';
                        break;
                    case 't':
                        result += '\t';
                        break;
                    case 'b':
                        result += '\b';
                        break;
                    case 'f':
                        result += '\f';
                        break;
                    case '(':
                        result += '(';
                        break;
                    case ')':
                        result += ')';
                        break;
                    case '\\':
                        result += '\\';
                        break;
                    default:
                        // Octal or just the character
                        if (*p >= '0' && *p <= '7') {
                            int octal = *p - '0';
                            if (p + 1 < end && p[1] >= '0' && p[1] <= '7') {
                                ++p;
                                octal = octal * 8 + (*p - '0');
                                if (p + 1 < end && p[1] >= '0' && p[1] <= '7') {
                                    ++p;
                                    octal = octal * 8 + (*p - '0');
                                }
                            }
                            result += static_cast<char>(octal);
                        } else {
                            result += static_cast<char>(*p);
                        }
                        break;
                }
            } else if (*p == '(') {
                ++parenDepth;
                result += '(';
            } else if (*p == ')') {
                --parenDepth;
                if (parenDepth > 0) {
                    result += ')';
                }
            } else {
                result += static_cast<char>(*p);
            }
            ++p;
        }
        return result;

    } else if (*p == '<' && p + 1 < end && p[1] != '<') {
        // Hex string
        ++p;
        std::string result;

        while (p < end && *p != '>') {
            if (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
                ++p;
                continue;
            }

            auto hexVal = [](uint8_t c) -> int {
                if (c >= '0' && c <= '9')
                    return c - '0';
                if (c >= 'A' && c <= 'F')
                    return c - 'A' + 10;
                if (c >= 'a' && c <= 'f')
                    return c - 'a' + 10;
                return -1;
            };

            int high = hexVal(*p);
            if (high < 0) {
                ++p;
                continue;
            }
            ++p;

            int low = 0;
            if (p < end && *p != '>') {
                low = hexVal(*p);
                if (low >= 0) {
                    ++p;
                }
            }

            result += static_cast<char>((high << 4) | (low >= 0 ? low : 0));
        }

        if (p < end && *p == '>') {
            ++p;
        }
        return result;
    }

    return std::nullopt;
}

/**
 * Parse an indirect object reference (e.g., "5 0 R").
 * Returns the object number, or -1 on failure.
 */
int parseIndirectRef(const uint8_t*& p, const uint8_t* end) {
    p = skipWhitespace(p, end);

    // Parse object number
    const auto* numStart = p;
    while (p < end && *p >= '0' && *p <= '9') {
        ++p;
    }
    if (p == numStart) {
        return -1;
    }

    int objNum = 0;
    std::from_chars(reinterpret_cast<const char*>(numStart), reinterpret_cast<const char*>(p),
                    objNum);

    // Skip generation number and 'R'
    p = skipWhitespace(p, end);
    while (p < end && *p >= '0' && *p <= '9') {
        ++p;
    }
    p = skipWhitespace(p, end);
    if (p < end && *p == 'R') {
        ++p;
    }

    return objNum;
}

/**
 * Find an object by number in the PDF.
 * Returns pointer to start of object content (after "obj" keyword).
 */
const uint8_t* findObject(std::span<const uint8_t> data, int objNum) {
    char pattern[32];
    int len = std::snprintf(pattern, sizeof(pattern), "%d 0 obj", objNum);
    std::string_view needle(pattern, len);

    // Search forward (objects are typically near the start)
    auto it = std::search(data.begin(), data.end(), needle.begin(), needle.end());
    if (it == data.end()) {
        // Try with different whitespace
        len = std::snprintf(pattern, sizeof(pattern), "%d 0 obj", objNum);
        it = std::search(data.begin(), data.end(), needle.begin(), needle.end());
    }

    if (it == data.end()) {
        return nullptr;
    }

    // Skip past "obj"
    const auto* p = &*it + needle.size();
    return skipWhitespace(p, data.data() + data.size());
}

/**
 * Extract a named entry from a dictionary.
 */
std::optional<std::string> extractDictEntry(const uint8_t* dictStart, const uint8_t* end,
                                            std::string_view name) {
    // Look for /Name pattern
    std::string pattern = "/" + std::string(name);
    const auto* p = dictStart;

    while (p < end - pattern.size()) {
        if (std::memcmp(p, pattern.data(), pattern.size()) == 0) {
            p += pattern.size();
            p = skipWhitespace(p, end);
            return parsePdfString(p, end);
        }
        ++p;
    }

    return std::nullopt;
}

} // anonymous namespace

std::unordered_map<std::string, std::string> PdfMetadata::toMap() const {
    std::unordered_map<std::string, std::string> result;

    if (title)
        result["pdf_title"] = *title;
    if (author)
        result["pdf_author"] = *author;
    if (subject)
        result["pdf_subject"] = *subject;
    if (keywords)
        result["pdf_keywords"] = *keywords;
    if (creator)
        result["pdf_creator"] = *creator;
    if (producer)
        result["pdf_producer"] = *producer;
    if (creationDate)
        result["creation_date"] = *creationDate;
    if (modDate)
        result["modification_date"] = *modDate;

    return result;
}

bool PdfMetadata::empty() const {
    return !title && !author && !subject && !keywords && !creator && !producer && !creationDate &&
           !modDate;
}

std::optional<PdfMetadata> extractMetadata(std::span<const uint8_t> data) {
    if (data.size() < 100) {
        return std::nullopt; // Too small to be a valid PDF
    }

    // Find trailer
    const auto* trailer = findBackwards(data, "trailer");
    if (!trailer) {
        // Try startxref for linearized PDFs
        return std::nullopt;
    }

    const auto* end = data.data() + data.size();

    // Find /Info reference in trailer
    const auto* p = trailer;
    const auto* infoRef =
        findBytes(p, static_cast<size_t>(end - p), reinterpret_cast<const uint8_t*>("/Info"), 5);

    if (!infoRef) {
        return std::nullopt;
    }

    // Parse the /Info reference
    p = infoRef + 5;
    p = skipWhitespace(p, end);

    int infoObjNum = parseIndirectRef(p, end);
    if (infoObjNum < 0) {
        return std::nullopt;
    }

    // Find the info object
    const auto* infoObj = findObject(data, infoObjNum);
    if (!infoObj) {
        return std::nullopt;
    }

    // Find the dictionary start
    p = skipWhitespace(infoObj, end);
    if (p >= end || *p != '<' || p + 1 >= end || p[1] != '<') {
        return std::nullopt;
    }
    p += 2; // Skip "<<"

    // Find dictionary end
    const auto* dictEnd =
        findBytes(p, static_cast<size_t>(end - p), reinterpret_cast<const uint8_t*>(">>"), 2);
    if (!dictEnd) {
        dictEnd = end;
    }

    // Extract metadata fields
    PdfMetadata metadata;
    metadata.title = extractDictEntry(p, dictEnd, "Title");
    metadata.author = extractDictEntry(p, dictEnd, "Author");
    metadata.subject = extractDictEntry(p, dictEnd, "Subject");
    metadata.keywords = extractDictEntry(p, dictEnd, "Keywords");
    metadata.creator = extractDictEntry(p, dictEnd, "Creator");
    metadata.producer = extractDictEntry(p, dictEnd, "Producer");
    metadata.creationDate = extractDictEntry(p, dictEnd, "CreationDate");
    metadata.modDate = extractDictEntry(p, dictEnd, "ModDate");

    return metadata;
}

} // namespace yams::zyp
