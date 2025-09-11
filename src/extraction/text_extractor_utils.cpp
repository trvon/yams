#include <yams/extraction/text_extractor.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace yams::extraction {

// Basic encoding detection implementation (centralized)
std::string EncodingDetector::detectEncoding(std::span<const std::byte> data, double* confidence) {
    if (data.size() >= 3) {
        if (static_cast<uint8_t>(data[0]) == 0xEF && static_cast<uint8_t>(data[1]) == 0xBB &&
            static_cast<uint8_t>(data[2]) == 0xBF) {
            if (confidence)
                *confidence = 1.0;
            return "UTF-8";
        }
    }

    if (data.size() >= 2) {
        if (static_cast<uint8_t>(data[0]) == 0xFF && static_cast<uint8_t>(data[1]) == 0xFE) {
            if (confidence)
                *confidence = 1.0;
            return "UTF-16LE";
        }
        if (static_cast<uint8_t>(data[0]) == 0xFE && static_cast<uint8_t>(data[1]) == 0xFF) {
            if (confidence)
                *confidence = 1.0;
            return "UTF-16BE";
        }
    }

    // Heuristic: check for valid UTF-8
    bool isValidUtf8 = true;
    size_t i = 0;
    while (i < data.size() && isValidUtf8) {
        uint8_t byte = static_cast<uint8_t>(data[i]);
        if (byte <= 0x7F) {
            i++;
        } else if ((byte & 0xE0) == 0xC0) {
            if (i + 1 >= data.size() || (static_cast<uint8_t>(data[i + 1]) & 0xC0) != 0x80) {
                isValidUtf8 = false;
            }
            i += 2;
        } else if ((byte & 0xF0) == 0xE0) {
            if (i + 2 >= data.size() || (static_cast<uint8_t>(data[i + 1]) & 0xC0) != 0x80 ||
                (static_cast<uint8_t>(data[i + 2]) & 0xC0) != 0x80) {
                isValidUtf8 = false;
            }
            i += 3;
        } else if ((byte & 0xF8) == 0xF0) {
            if (i + 3 >= data.size() || (static_cast<uint8_t>(data[i + 1]) & 0xC0) != 0x80 ||
                (static_cast<uint8_t>(data[i + 2]) & 0xC0) != 0x80 ||
                (static_cast<uint8_t>(data[i + 3]) & 0xC0) != 0x80) {
                isValidUtf8 = false;
            }
            i += 4;
        } else {
            isValidUtf8 = false;
        }
    }

    if (isValidUtf8) {
        if (confidence)
            *confidence = 0.9;
        return "UTF-8";
    }

    if (confidence)
        *confidence = 0.5;
    return "ISO-8859-1"; // default fallback
}

// Minimal UTF-16 (LE/BE) to UTF-8 converter for common cases
static inline void appendUtf8FromCodepoint(uint32_t cp, std::string& out) {
    if (cp <= 0x7F) {
        out.push_back(static_cast<char>(cp));
    } else if (cp <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

Result<std::string> EncodingDetector::convertToUtf8(const std::string& text,
                                                    const std::string& fromEncoding) {
    if (fromEncoding == "UTF-8" || fromEncoding == "utf-8" || fromEncoding == "ASCII") {
        return text;
    }

    const auto toBytes = [&](size_t i) { return static_cast<uint8_t>(text[i]); };

    if (fromEncoding == "ISO-8859-1" || fromEncoding == "iso-8859-1" || fromEncoding == "latin1") {
        std::string out;
        out.reserve(text.size());
        for (unsigned char b : text) {
            appendUtf8FromCodepoint(static_cast<uint32_t>(b), out);
        }
        return out;
    }

    if (fromEncoding == "UTF-16LE" || fromEncoding == "UTF-16BE") {
        bool le = (fromEncoding == "UTF-16LE");
        size_t i = 0;
        // Skip BOM if present
        if (text.size() >= 2) {
            uint16_t bom = le ? (toBytes(1) << 8 | toBytes(0)) : (toBytes(0) << 8 | toBytes(1));
            if (bom == 0xFEFF) {
                i = 2;
            }
        }
        std::string out;
        out.reserve(text.size());
        while (i + 1 < text.size()) {
            uint16_t w =
                le ? (toBytes(i + 1) << 8 | toBytes(i)) : (toBytes(i) << 8 | toBytes(i + 1));
            i += 2;
            if (w >= 0xD800 && w <= 0xDBFF) {
                // high surrogate; need next
                if (i + 1 >= text.size()) {
                    appendUtf8FromCodepoint(0xFFFD, out);
                    break;
                }
                uint16_t w2 =
                    le ? (toBytes(i + 1) << 8 | toBytes(i)) : (toBytes(i) << 8 | toBytes(i + 1));
                i += 2;
                if (w2 < 0xDC00 || w2 > 0xDFFF) {
                    appendUtf8FromCodepoint(0xFFFD, out);
                    continue;
                }
                uint32_t cp = 0x10000 + (((w - 0xD800) << 10) | (w2 - 0xDC00));
                appendUtf8FromCodepoint(cp, out);
            } else if (w >= 0xDC00 && w <= 0xDFFF) {
                // stray low surrogate
                appendUtf8FromCodepoint(0xFFFD, out);
            } else {
                appendUtf8FromCodepoint(w, out);
            }
        }
        return out;
    }

    return Error{ErrorCode::InvalidOperation, "Unsupported encoding conversion: " + fromEncoding};
}

// Basic language detection implementation (centralized)
std::string LanguageDetector::detectLanguage(const std::string& text, double* confidence) {
    std::unordered_map<std::string, int> langScores;
    std::string lowerText = text;
    std::transform(lowerText.begin(), lowerText.end(), lowerText.begin(), ::tolower);

    auto hasWholeWord = [&lowerText](const std::string& word) {
        size_t pos = 0;
        while ((pos = lowerText.find(word, pos)) != std::string::npos) {
            bool validStart =
                (pos == 0 || !std::isalpha(static_cast<unsigned char>(lowerText[pos - 1])));
            bool validEnd =
                (pos + word.length() >= lowerText.length() ||
                 !std::isalpha(static_cast<unsigned char>(lowerText[pos + word.length()])));
            if (validStart && validEnd) {
                return true;
            }
            pos++;
        }
        return false;
    };

    for (const auto& w : std::vector<std::string>{"the", "is", "are", "and", "or", "but", "in",
                                                  "on", "at", "to", "for", "of"})
        if (hasWholeWord(w))
            langScores["en"]++;

    for (const auto& w : std::vector<std::string>{"el", "la", "de", "que", "y", "en", "un", "una",
                                                  "es", "por", "con"})
        if (hasWholeWord(w))
            langScores["es"]++;

    for (const auto& w : std::vector<std::string>{"le", "de", "un", "une", "et", "est", "pour",
                                                  "dans", "que", "avec"})
        if (hasWholeWord(w))
            langScores["fr"]++;

    std::string detectedLang = "en";
    int maxScore = 0;
    for (const auto& [lang, score] : langScores) {
        if (score > maxScore) {
            maxScore = score;
            detectedLang = lang;
        }
    }

    if (confidence) {
        if (maxScore > 5)
            *confidence = 0.9;
        else if (maxScore > 2)
            *confidence = 0.7;
        else
            *confidence = 0.3;
    }

    return detectedLang;
}

} // namespace yams::extraction
