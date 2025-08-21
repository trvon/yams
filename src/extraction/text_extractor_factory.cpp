#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <yams/extraction/html_text_extractor.h>
#include <yams/extraction/pdf_extractor.h>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction {

// Constructor - registers built-in extractors
TextExtractorFactory::TextExtractorFactory() {
    // Register PDF extractor
#ifdef YAMS_HAS_PDF_SUPPORT
    registerExtractor({".pdf"}, []() { return std::make_unique<PdfExtractor>(); });
    spdlog::debug("Registered PDF text extractor for .pdf files");
#endif

    // Register HTML extractor
    registerExtractor({".html", ".htm"}, []() { return std::make_unique<HtmlTextExtractor>(); });
    spdlog::debug("Registered HTML text extractor for .html/.htm files");

    // Log all registered extensions
    auto exts = supportedExtensions();
    if (!exts.empty()) {
        std::string extList;
        for (const auto& e : exts) {
            if (!extList.empty())
                extList += ", ";
            extList += e;
        }
        spdlog::debug("TextExtractorFactory initialized with extensions: {}", extList);
    }
}

TextExtractorFactory& TextExtractorFactory::instance() {
    static TextExtractorFactory instance;
    return instance;
}

std::unique_ptr<ITextExtractor> TextExtractorFactory::create(const std::string& extension) {
    std::string ext = extension;
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = extractors_.find(ext);
    if (it != extractors_.end()) {
        return it->second();
    }
    return nullptr;
}

std::unique_ptr<ITextExtractor>
TextExtractorFactory::createForFile(const std::filesystem::path& path) {
    return create(path.extension().string());
}

void TextExtractorFactory::registerExtractor(const std::vector<std::string>& extensions,
                                             ExtractorCreator creator) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& ext : extensions) {
        std::string normalized = ext;
        std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
        extractors_[normalized] = creator;
    }
}

std::vector<std::string> TextExtractorFactory::supportedExtensions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> extensions;
    extensions.reserve(extractors_.size());
    for (const auto& [ext, _] : extractors_) {
        extensions.push_back(ext);
    }
    std::sort(extensions.begin(), extensions.end());
    return extensions;
}

bool TextExtractorFactory::isSupported(const std::string& extension) const {
    std::string ext = extension;
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    std::lock_guard<std::mutex> lock(mutex_);
    return extractors_.find(ext) != extractors_.end();
}

// Basic encoding detection implementation
std::string EncodingDetector::detectEncoding(std::span<const std::byte> data, double* confidence) {
    // Simple detection based on BOM and heuristics
    if (data.size() >= 3) {
        // UTF-8 BOM
        if (static_cast<uint8_t>(data[0]) == 0xEF && static_cast<uint8_t>(data[1]) == 0xBB &&
            static_cast<uint8_t>(data[2]) == 0xBF) {
            if (confidence)
                *confidence = 1.0;
            return "UTF-8";
        }
    }

    if (data.size() >= 2) {
        // UTF-16 LE BOM
        if (static_cast<uint8_t>(data[0]) == 0xFF && static_cast<uint8_t>(data[1]) == 0xFE) {
            if (confidence)
                *confidence = 1.0;
            return "UTF-16LE";
        }
        // UTF-16 BE BOM
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
            // ASCII
            i++;
        } else if ((byte & 0xE0) == 0xC0) {
            // 2-byte sequence
            if (i + 1 >= data.size() || (static_cast<uint8_t>(data[i + 1]) & 0xC0) != 0x80) {
                isValidUtf8 = false;
            }
            i += 2;
        } else if ((byte & 0xF0) == 0xE0) {
            // 3-byte sequence
            if (i + 2 >= data.size() || (static_cast<uint8_t>(data[i + 1]) & 0xC0) != 0x80 ||
                (static_cast<uint8_t>(data[i + 2]) & 0xC0) != 0x80) {
                isValidUtf8 = false;
            }
            i += 3;
        } else if ((byte & 0xF8) == 0xF0) {
            // 4-byte sequence
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

    // Default to ISO-8859-1 (Latin-1)
    if (confidence)
        *confidence = 0.5;
    return "ISO-8859-1";
}

Result<std::string> EncodingDetector::convertToUtf8(const std::string& text,
                                                    const std::string& fromEncoding) {
    // For now, just pass through if already UTF-8
    if (fromEncoding == "UTF-8" || fromEncoding == "utf-8") {
        return text;
    }

    // TODO: Implement proper encoding conversion
    return Error{ErrorCode::InvalidOperation, "Encoding conversion not yet implemented"};
}

// Basic language detection implementation
std::string LanguageDetector::detectLanguage(const std::string& text, double* confidence) {
    // Very basic detection based on common words
    // In production, use a proper language detection library

    // Count occurrences of common words in different languages
    std::unordered_map<std::string, int> langScores;

    // Convert text to lowercase for comparison
    std::string lowerText = text;
    std::transform(lowerText.begin(), lowerText.end(), lowerText.begin(), ::tolower);

    // Helper function to check whole word match
    auto hasWholeWord = [&lowerText](const std::string& word) {
        size_t pos = 0;
        while ((pos = lowerText.find(word, pos)) != std::string::npos) {
            // Check if it's a whole word (preceded and followed by non-letter)
            bool validStart = (pos == 0 || !std::isalpha(lowerText[pos - 1]));
            bool validEnd = (pos + word.length() >= lowerText.length() ||
                             !std::isalpha(lowerText[pos + word.length()]));
            if (validStart && validEnd) {
                return true;
            }
            pos++;
        }
        return false;
    };

    // English
    std::vector<std::string> englishWords = {"the", "is", "are", "and", "or",  "but",
                                             "in",  "on", "at",  "to",  "for", "of"};
    for (const auto& word : englishWords) {
        if (hasWholeWord(word)) {
            langScores["en"]++;
        }
    }

    // Spanish
    std::vector<std::string> spanishWords = {"el", "la",  "de", "que", "y",  "en",
                                             "un", "una", "es", "por", "con"};
    for (const auto& word : spanishWords) {
        if (hasWholeWord(word)) {
            langScores["es"]++;
        }
    }

    // French
    std::vector<std::string> frenchWords = {"le",  "de",   "un",   "une", "et",
                                            "est", "pour", "dans", "que", "avec"};
    for (const auto& word : frenchWords) {
        if (hasWholeWord(word)) {
            langScores["fr"]++;
        }
    }

    // Find language with highest score
    std::string detectedLang = "en"; // Default to English
    int maxScore = 0;
    for (const auto& [lang, score] : langScores) {
        if (score > maxScore) {
            maxScore = score;
            detectedLang = lang;
        }
    }

    // Set confidence based on score
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