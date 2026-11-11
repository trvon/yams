#include <yams/common/utf8_utils.h>
#include <yams/extraction/text_extractor.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <cctype>
#include <cstddef>
#include <string>
#include <string_view>
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

    // Heuristic: the data is UTF-8 only if every sequence is well-formed (no overlongs,
    // surrogates, or values above U+10FFFF); otherwise it must go through conversion.
    const auto* bytes = reinterpret_cast<const unsigned char*>(data.data());
    bool isValidUtf8 = true;
    for (size_t i = 0; i < data.size();) {
        const size_t len = common::wellFormedUtf8Length(bytes, i, data.size());
        if (len == 0) {
            isValidUtf8 = false;
            break;
        }
        i += len;
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
                if (w2 < 0xDC00 || w2 > 0xDFFF) {
                    // Unpaired high surrogate: replace it and decode the next unit normally.
                    appendUtf8FromCodepoint(0xFFFD, out);
                    continue;
                }
                i += 2;
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

namespace {

// Stop words that vote for a language. A word may vote for more than one.
enum LanguageBit : unsigned { kEnglish = 1U, kSpanish = 2U, kFrench = 4U };

struct StopWord {
    std::string_view word;
    unsigned languages;
};

constexpr std::array<StopWord, 30> kStopWords{{
    {"the", kEnglish},
    {"is", kEnglish},
    {"are", kEnglish},
    {"and", kEnglish},
    {"or", kEnglish},
    {"but", kEnglish},
    {"in", kEnglish},
    {"on", kEnglish},
    {"at", kEnglish},
    {"to", kEnglish},
    {"for", kEnglish},
    {"of", kEnglish},
    {"el", kSpanish},
    {"la", kSpanish},
    {"y", kSpanish},
    {"en", kSpanish},
    {"una", kSpanish},
    {"es", kSpanish},
    {"por", kSpanish},
    {"con", kSpanish},
    {"le", kFrench},
    {"une", kFrench},
    {"et", kFrench},
    {"est", kFrench},
    {"pour", kFrench},
    {"dans", kFrench},
    {"avec", kFrench},
    {"de", kSpanish | kFrench},
    {"un", kSpanish | kFrench},
    {"que", kSpanish | kFrench},
}};

constexpr std::size_t kLongestStopWord = 4;

} // namespace

// Basic language detection: count which stop words of each language occur as whole words.
std::string LanguageDetector::detectLanguage(const std::string& text, double* confidence) {
    std::bitset<kStopWords.size()> seen;
    std::array<char, kLongestStopWord> word{};

    const auto record = [&seen](std::string_view candidate) {
        for (std::size_t index = 0; index < kStopWords.size(); ++index) {
            if (kStopWords[index].word == candidate) {
                seen.set(index);
                return;
            }
        }
    };

    // One pass over maximal alphabetic runs. Runs longer than any stop word are skipped
    // without being copied, and the scan ends once every stop word has been seen.
    // Stop words are ASCII, and in the C and UTF-8 locales no byte above 0x7F is alphabetic,
    // so an ASCII test bounds words the same way without a libc call per byte.
    const auto lowerIfAlpha = [](char c) -> char {
        const auto lowered = static_cast<unsigned char>(static_cast<unsigned char>(c) | 0x20U);
        return (lowered >= 'a' && lowered <= 'z') ? static_cast<char>(lowered) : '\0';
    };

    const std::size_t n = text.size();
    std::size_t i = 0;
    while (i < n && !seen.all()) {
        while (i < n && lowerIfAlpha(text[i]) == '\0') {
            ++i;
        }
        std::size_t length = 0;
        for (char lowered = '\0'; i < n && (lowered = lowerIfAlpha(text[i])) != '\0'; ++i) {
            if (length < kLongestStopWord) {
                word[length] = lowered;
            }
            ++length;
        }
        if (length > 0 && length <= kLongestStopWord) {
            record(std::string_view(word.data(), length));
        }
    }

    int english = 0;
    int spanish = 0;
    int french = 0;
    for (std::size_t index = 0; index < kStopWords.size(); ++index) {
        if (seen.test(index)) {
            const unsigned languages = kStopWords[index].languages;
            english += (languages & kEnglish) ? 1 : 0;
            spanish += (languages & kSpanish) ? 1 : 0;
            french += (languages & kFrench) ? 1 : 0;
        }
    }

    // Ties go to the earlier language in this order, English first.
    std::string detectedLang = "en";
    int maxScore = english;
    if (spanish > maxScore) {
        maxScore = spanish;
        detectedLang = "es";
    }
    if (french > maxScore) {
        maxScore = french;
        detectedLang = "fr";
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
