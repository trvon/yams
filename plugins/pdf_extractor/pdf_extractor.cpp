#include "pdf_extractor.h"
#include <spdlog/spdlog.h>
#include <yams/extraction/text_extractor.h>

// QPDF headers
#include <qpdf/QPDF.hh>
#include <qpdf/QPDFObjectHandle.hh>
#include <qpdf/QPDFPageDocumentHelper.hh>
#include <qpdf/QPDFPageObjectHelper.hh>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <map>
#include <ranges>
#include <regex>
#include <sstream>

namespace yams::extraction {

// Main extraction methods
Result<ExtractionResult> PdfExtractor::extract(const std::filesystem::path& path,
                                               [[maybe_unused]] const ExtractionConfig& config) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "PDF file not found: " + path.string()};
    }

    PdfExtractOptions options;
    options.extractAll = true;
    options.extractMetadata = true;

    try {
        QPDF pdf;
        pdf.processFile(path.string().c_str());
        return extractFromDocument(pdf, options);
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData, "Failed to load PDF: " + std::string(e.what())};
    }
}

Result<ExtractionResult>
PdfExtractor::extractFromBuffer(std::span<const std::byte> data,
                                [[maybe_unused]] const ExtractionConfig& config) {
    PdfExtractOptions options;
    options.extractAll = true;
    options.extractMetadata = true;

    try {
        QPDF pdf;
        pdf.processMemoryFile("input.pdf", reinterpret_cast<const char*>(data.data()), data.size());
        return extractFromDocument(pdf, options);
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData,
                     "Failed to load PDF from buffer: " + std::string(e.what())};
    }
}

Result<ExtractionResult> PdfExtractor::extractPage(const std::filesystem::path& path, int pageNum) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "PDF file not found"};
    }

    try {
        QPDF pdf;
        pdf.processFile(path.string().c_str());

        QPDFPageDocumentHelper dh(pdf);
        auto pages = dh.getAllPages();

        if (pageNum < 0 || pageNum >= static_cast<int>(pages.size())) {
            return Error{ErrorCode::InvalidData, "Page number out of range"};
        }

        ExtractionResult result;
        result.extractionMethod = "qpdf";
        result.metadata["content_type"] = "application/pdf";
        result.text = extractPageText(pages[pageNum]);

        return result;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData, "Failed to extract page: " + std::string(e.what())};
    }
}

Result<ExtractionResult> PdfExtractor::extractWithOptions(const std::filesystem::path& path,
                                                          const PdfExtractOptions& options) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "PDF file not found"};
    }

    try {
        QPDF pdf;
        pdf.processFile(path.string().c_str());
        return extractFromDocument(pdf, options);
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData, "Failed to load PDF: " + std::string(e.what())};
    }
}

// Internal implementation using QPDF
Result<ExtractionResult> PdfExtractor::extractFromDocument(QPDF& pdf,
                                                           const PdfExtractOptions& options) {
    ExtractionResult result;
    result.extractionMethod = "qpdf";
    result.metadata["content_type"] = "application/pdf";
    result.metadata["encoding"] = "UTF-8";

    // Extract metadata if requested
    if (options.extractMetadata) {
        extractMetadata(pdf, result);
    }

    // Get pages
    QPDFPageDocumentHelper dh(pdf);
    auto pages = dh.getAllPages();

    int pageCount = static_cast<int>(pages.size());
    if (pageCount <= 0) {
        return Error{ErrorCode::InvalidData, "PDF has no pages"};
    }
    result.metadata["page_count"] = std::to_string(pageCount);

    // Determine pages to extract
    int maxPages = (options.maxPages > 0) ? std::min(options.maxPages, pageCount) : pageCount;

    std::stringstream textStream;

    // Extract text from pages
    for (int i = 0; i < maxPages; ++i) {
        try {
            std::string pageText = extractPageText(pages[i]);
            if (!pageText.empty()) {
                if (i > 0) {
                    textStream << "\n\n--- Page " << (i + 1) << " ---\n\n";
                }
                textStream << pageText;
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to extract text from page {}: {}", i, e.what());
        }
    }

    result.text = textStream.str();

    // Clean the extracted text
    result.text = cleanText(result.text);

    // Detect language
    result.language = detectLanguage(result.text);

    // Search for specific text if requested
    if (!options.searchText.empty()) {
        std::vector<std::string> lines;
        std::stringstream ss(result.text);
        std::string line;
        while (std::getline(ss, line)) {
            lines.push_back(line);
        }

        auto contextLines = findTextInContext(lines, options.searchText, options.contextLines);
        if (!contextLines.empty()) {
            std::stringstream contextStream;
            for (const auto& contextLine : contextLines) {
                contextStream << contextLine << "\n";
            }
            result.text = contextStream.str();
        }
    }

    return result;
}

// Extract metadata from PDF using QPDF
void PdfExtractor::extractMetadata(QPDF& pdf, ExtractionResult& result) {
    try {
        auto trailer = pdf.getTrailer();
        if (trailer.hasKey("/Info")) {
            QPDFObjectHandle info = trailer.getKey("/Info");

            auto getStringValue = [](QPDFObjectHandle obj, const std::string& key) -> std::string {
                if (obj.hasKey(key)) {
                    auto val = obj.getKey(key);
                    if (val.isString()) {
                        return val.getUTF8Value();
                    }
                }
                return "";
            };

            std::string title = getStringValue(info, "/Title");
            std::string author = getStringValue(info, "/Author");
            std::string subject = getStringValue(info, "/Subject");
            std::string keywords = getStringValue(info, "/Keywords");
            std::string creator = getStringValue(info, "/Creator");
            std::string producer = getStringValue(info, "/Producer");

            if (!title.empty())
                result.metadata["title"] = title;
            if (!author.empty())
                result.metadata["author"] = author;
            if (!subject.empty())
                result.metadata["subject"] = subject;
            if (!keywords.empty())
                result.metadata["keywords"] = keywords;
            if (!creator.empty())
                result.metadata["creator"] = creator;
            if (!producer.empty())
                result.metadata["producer"] = producer;
        }

        // PDF version
        result.metadata["pdf_version"] = pdf.getPDFVersion();
    } catch (const std::exception& e) {
        spdlog::warn("Failed to extract PDF metadata: {}", e.what());
    }
}

// Text extraction callback for QPDF parser
class TextExtractorCallback : public QPDFObjectHandle::ParserCallbacks {
public:
    std::stringstream& text;
    std::string lastOperator;

    explicit TextExtractorCallback(std::stringstream& t) : text(t) {}

    void handleObject(QPDFObjectHandle obj) override {
        if (obj.isOperator()) {
            lastOperator = obj.getOperatorValue();
        } else if (lastOperator == "Tj" || lastOperator == "'" || lastOperator == "\"") {
            // Text showing operators
            if (obj.isString()) {
                text << obj.getUTF8Value() << " ";
            }
        } else if (lastOperator == "TJ") {
            // Array of strings and positioning info
            if (obj.isArray()) {
                auto arr = obj.getArrayAsVector();
                for (auto& item : arr) {
                    if (item.isString()) {
                        text << item.getUTF8Value();
                    }
                }
                text << " ";
            }
        }
    }

    void handleEOF() override {}
};

// Extract text from a single page using QPDF
std::string PdfExtractor::extractPageText(QPDFPageObjectHelper page) {
    std::stringstream text;

    try {
        // Use QPDF's parser callback interface for proper text extraction
        TextExtractorCallback callback(text);
        page.parsePageContents(&callback);
    } catch (const std::exception& e) {
        spdlog::warn("Failed to extract page text: {}", e.what());
    }

    return text.str();
}

// Clean extracted text
std::string PdfExtractor::cleanText(const std::string& rawText) {
    std::string cleaned = rawText;

    // Remove excessive whitespace
    cleaned = std::regex_replace(cleaned, std::regex("[ \\t]+"), " ");

    // Normalize line endings
    cleaned = std::regex_replace(cleaned, std::regex("\\r\\n|\\r"), "\n");

    // Remove excessive newlines (more than 2)
    cleaned = std::regex_replace(cleaned, std::regex("\\n{3,}"), "\n\n");

    // Trim leading and trailing whitespace
    size_t start = cleaned.find_first_not_of(" \n\t");
    size_t end = cleaned.find_last_not_of(" \n\t");

    if (start != std::string::npos && end != std::string::npos) {
        cleaned = cleaned.substr(start, end - start + 1);
    }

    return cleaned;
}

// Find text in context
std::vector<std::string> PdfExtractor::findTextInContext(const std::vector<std::string>& lines,
                                                         const std::string& searchText,
                                                         int contextLines) {
    std::vector<std::string> result;

    // Convert search text to lowercase for case-insensitive search
    std::string searchLower = searchText;
    std::transform(searchLower.begin(), searchLower.end(), searchLower.begin(), ::tolower);

    for (size_t i = 0; i < lines.size(); ++i) {
        std::string lineLower = lines[i];
        std::transform(lineLower.begin(), lineLower.end(), lineLower.begin(), ::tolower);

        if (lineLower.find(searchLower) != std::string::npos) {
            // Add context before
            size_t startIdx = (i >= static_cast<size_t>(contextLines)) ? i - contextLines : 0;

            // Add context after
            size_t endIdx = std::min(i + contextLines + 1, lines.size());

            // Add marker for found line
            for (size_t j = startIdx; j < endIdx; ++j) {
                if (j == i) {
                    result.push_back(">>> " + lines[j] + " <<<");
                } else {
                    result.push_back(lines[j]);
                }
            }

            // Add separator if more matches might follow
            if (i + contextLines + 1 < lines.size()) {
                result.push_back("...");
            }
        }
    }

    return result;
}

// Extract sections from text
std::vector<TextSection> PdfExtractor::extractSections(const std::string& text) {
    std::vector<TextSection> sections;

    // Common section patterns in academic papers
    std::regex sectionRegex("(?:^|\\n)\\s*(?:"
                            "(?:\\d+\\.?\\s+)|"   // Numbered sections (1. or 1)
                            "(?:[IVX]+\\.?\\s+)|" // Roman numerals
                            "(?:[A-Z]\\.?\\s+)"   // Letter sections
                            ")?"
                            "(Abstract|Introduction|Background|"
                            "Related Work|Methodology|Methods|"
                            "Results|Discussion|Conclusion|References|"
                            "Appendix|Acknowledgments)",
                            std::regex::icase);

    std::string::const_iterator searchStart(text.cbegin());
    std::smatch match;
    size_t lastEnd = 0;

    while (std::regex_search(searchStart, text.cend(), match, sectionRegex)) {
        if (match.position() > 0 && lastEnd > 0) {
            // Add content to previous section
            if (!sections.empty()) {
                size_t contentStart = lastEnd;
                size_t contentEnd = match.position() + (searchStart - text.cbegin());
                sections.back().content = text.substr(contentStart, contentEnd - contentStart);
            }
        }

        TextSection section;
        section.title = match[1].str();
        section.level = 1; // Could be enhanced to detect actual level
        section.startOffset = match.position() + (searchStart - text.cbegin());

        sections.push_back(section);

        lastEnd = section.startOffset + match.length();
        searchStart = match.suffix().first;
    }

    // Add content to last section
    if (!sections.empty() && lastEnd < text.length()) {
        sections.back().content = text.substr(lastEnd);
    }

    // If no sections found, create a single section with all content
    if (sections.empty()) {
        TextSection mainSection;
        mainSection.title = "Content";
        mainSection.level = 1;
        mainSection.startOffset = 0;
        mainSection.content = text;
        sections.push_back(mainSection);
    }

    return sections;
}

// Detect language (simplified implementation)
std::string PdfExtractor::detectLanguage(const std::string& text) {
    // This is a very basic implementation
    // In production, use a proper language detection library

    // Count occurrences of common words
    std::map<std::string, std::vector<std::string>> langWords = {
        {"en", {"the", "and", "of", "to", "in", "is", "that", "for", "with", "as"}},
        {"es", {"el", "la", "de", "que", "y", "en", "un", "por", "con", "para"}},
        {"fr", {"le", "de", "un", "et", "est", "pour", "dans", "que", "avec", "sur"}},
        {"de", {"der", "die", "und", "in", "das", "von", "zu", "mit", "auf", "ist"}}};

    std::map<std::string, int> scores;

    // Convert text to lowercase for comparison
    std::string lowerText = text;
    std::transform(lowerText.begin(), lowerText.end(), lowerText.begin(), ::tolower);

    for (const auto& [lang, words] : langWords) {
        for (const auto& word : words) {
            size_t pos = 0;
            while ((pos = lowerText.find(" " + word + " ", pos)) != std::string::npos) {
                scores[lang]++;
                pos += word.length();
            }
        }
    }

    // Find language with highest score
    std::string detectedLang = "en"; // Default to English
    int maxScore = 0;

    for (const auto& [lang, score] : scores) {
        if (score > maxScore) {
            maxScore = score;
            detectedLang = lang;
        }
    }

    return detectedLang;
}

// Register the PDF extractor automatically
#ifdef YAMS_HAS_PDF_SUPPORT
REGISTER_EXTRACTOR(PdfExtractor, ".pdf");

// Optional manual registration function
void registerPdfExtractor() {
    // Already registered via REGISTER_EXTRACTOR macro
    spdlog::info("PDF text extractor available");
}
#else
void registerPdfExtractor() {
    spdlog::warn("PDF text extraction support not compiled in");
}
#endif

} // namespace yams::extraction