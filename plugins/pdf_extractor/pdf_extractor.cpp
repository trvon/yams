#include "pdf_extractor.h"
#include <spdlog/spdlog.h>
#include <yams/extraction/text_extractor.h>

// PDFium headers
#include <fpdf_doc.h>
#include <fpdf_save.h>
#include <fpdf_text.h>
#include <fpdfview.h>

#include <algorithm>
#include <codecvt>
#include <fstream>
#include <locale>
#include <map>
#include <regex>
#include <sstream>

namespace yams::extraction {

// Static member definitions
bool PdfExtractor::pdfiumInitialized = false;
std::mutex PdfExtractor::pdfiumMutex;

// Destructor implementations for RAII wrappers
PdfExtractor::PdfDocument::~PdfDocument() {
    if (doc) {
        FPDF_CloseDocument(doc);
        doc = nullptr;
    }
}

PdfExtractor::PdfPage::~PdfPage() {
    if (textPage) {
        FPDFText_ClosePage(textPage);
        textPage = nullptr;
    }
    if (page) {
        FPDF_ClosePage(page);
        page = nullptr;
    }
}

// Static initialization/cleanup
void PdfExtractor::initializePdfium() {
    std::lock_guard<std::mutex> lock(pdfiumMutex);
    if (!pdfiumInitialized) {
        FPDF_LIBRARY_CONFIG config = {};
        config.version = 2;
        config.m_pUserFontPaths = nullptr;
        config.m_pIsolate = nullptr;
        config.m_v8EmbedderSlot = 0;
        config.m_pPlatform = nullptr;

        FPDF_InitLibraryWithConfig(&config);
        pdfiumInitialized = true;

        // Register cleanup at exit
        std::atexit(cleanupPdfium);

        spdlog::debug("PDFium library initialized");
    }
}

void PdfExtractor::cleanupPdfium() {
    std::lock_guard<std::mutex> lock(pdfiumMutex);
    if (pdfiumInitialized) {
        FPDF_DestroyLibrary();
        pdfiumInitialized = false;
        spdlog::debug("PDFium library cleaned up");
    }
}

bool PdfExtractor::isPdfiumInitialized() {
    std::lock_guard<std::mutex> lock(pdfiumMutex);
    return pdfiumInitialized;
}

// Constructor/Destructor
/* defaulted in header; PDFium initialized lazily in extractInternal() */

/* destructor defaulted in header */

// Main extraction methods
Result<ExtractionResult> PdfExtractor::extract(const std::filesystem::path& path,
                                               [[maybe_unused]] const ExtractionConfig& config) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "PDF file not found: " + path.string()};
    }

    // Read file into memory
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        return Error{ErrorCode::FileNotFound, "Cannot open PDF file: " + path.string()};
    }

    auto size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> buffer(size);
    file.read(reinterpret_cast<char*>(buffer.data()), size);

    if (!file) {
        return Error{ErrorCode::InvalidData, "Failed to read PDF file"};
    }

    PdfExtractOptions options;
    options.extractAll = true;
    options.extractMetadata = true;

    return extractInternal(buffer, options);
}

Result<ExtractionResult>
PdfExtractor::extractFromBuffer(std::span<const std::byte> data,
                                [[maybe_unused]] const ExtractionConfig& config) {
    PdfExtractOptions options;
    options.extractAll = true;
    options.extractMetadata = true;

    return extractInternal(data, options);
}

Result<ExtractionResult> PdfExtractor::extractPage(const std::filesystem::path& path,
                                                   [[maybe_unused]] int page) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        return Error{ErrorCode::FileNotFound, "Cannot open PDF file"};
    }

    auto size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> buffer(size);
    file.read(reinterpret_cast<char*>(buffer.data()), size);

    PdfExtractOptions options;
    options.extractAll = false;
    options.maxPages = 1;
    // Note: Would need to modify to support specific page extraction

    return extractInternal(buffer, options);
}

Result<ExtractionResult> PdfExtractor::extractWithOptions(const std::filesystem::path& path,
                                                          const PdfExtractOptions& options) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        return Error{ErrorCode::FileNotFound, "Cannot open PDF file"};
    }

    auto size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::byte> buffer(size);
    file.read(reinterpret_cast<char*>(buffer.data()), size);

    return extractInternal(buffer, options);
}

// Internal implementation
Result<ExtractionResult> PdfExtractor::extractInternal(std::span<const std::byte> data,
                                                       const PdfExtractOptions& options) {
    // Lock the entire extraction process to ensure thread safety
    // PDFium is not thread-safe and requires global synchronization
    std::lock_guard<std::mutex> lock(pdfiumMutex);

    if (!pdfiumInitialized) {
        FPDF_LIBRARY_CONFIG config = {};
        config.version = 2;
        config.m_pUserFontPaths = nullptr;
        config.m_pIsolate = nullptr;
        config.m_v8EmbedderSlot = 0;
        config.m_pPlatform = nullptr;

        FPDF_InitLibraryWithConfig(&config);
        pdfiumInitialized = true;

        // Register cleanup at exit
        std::atexit(cleanupPdfium);

        spdlog::debug("PDFium library initialized");
    }

    // Load PDF document
    PdfDocument doc;
    doc.doc = FPDF_LoadMemDocument(data.data(), static_cast<int>(data.size()),
                                   nullptr // No password
    );

    if (!doc.doc) {
        unsigned long error = FPDF_GetLastError();
        std::string errorMsg = "Failed to load PDF document. Error code: " + std::to_string(error);
        return Error{ErrorCode::InvalidData, errorMsg};
    }

    ExtractionResult result;
    result.extractionMethod = "pdfium";
    result.metadata["content_type"] = "application/pdf";
    result.metadata["encoding"] = "UTF-8";

    // Extract metadata if requested
    if (options.extractMetadata) {
        extractMetadata(doc.doc, result);
    }

    // Get page count
    int pageCount = FPDF_GetPageCount(doc.doc);
    if (pageCount <= 0) {
        return Error{ErrorCode::InvalidData, "PDF has no pages"};
    }

    // Determine pages to extract
    int maxPages = (options.maxPages > 0) ? std::min(options.maxPages, pageCount) : pageCount;

    std::stringstream textStream;

    // Extract text from pages
    for (int i = 0; i < maxPages; ++i) {
        PdfPage page;
        page.page = FPDF_LoadPage(doc.doc, i);
        if (!page.page) {
            spdlog::warn("Failed to load PDF page {}", i);
            continue;
        }

        page.textPage = FPDFText_LoadPage(page.page);
        if (!page.textPage) {
            spdlog::warn("Failed to load text from PDF page {}", i);
            continue;
        }

        // Get character count
        int charCount = FPDFText_CountChars(page.textPage);
        if (charCount > 0) {
            // Allocate buffer for UTF-16 text (2 bytes per character + null terminator)
            std::vector<unsigned short> buffer(charCount + 1, 0);

            // Extract text
            int extracted = FPDFText_GetText(page.textPage, 0, charCount, buffer.data());
            if (extracted > 0) {
                // Convert UTF-16 to UTF-8
                std::string pageText = convertUtf16ToUtf8(buffer);

                // Add page separator if not first page
                if (i > 0) {
                    textStream << "\n\n--- Page " << (i + 1) << " ---\n\n";
                }

                textStream << pageText;
            }
        }
    }

    result.text = textStream.str();

    // Clean the extracted text
    result.text = cleanText(result.text);

    // Extract sections if text is substantial
    if (result.text.length() > 100) {
        // sections extraction skipped (ExtractionResult has no sections field)
    }

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

// Extract metadata from PDF
// Note: This function is called from within extractInternal() which already holds the PDFium mutex
void PdfExtractor::extractMetadata(FPDF_DOCUMENT doc, ExtractionResult& result) {
    // Helper to get metadata string
    auto getMetaString = [doc](const std::string& tag) -> std::string {
        // First call to get buffer size
        unsigned long bufferLen = FPDF_GetMetaText(doc, tag.c_str(), nullptr, 0);
        if (bufferLen <= 2)
            return ""; // Empty or just null terminator

        // Allocate buffer and get text
        std::vector<unsigned short> buffer(bufferLen / 2);
        FPDF_GetMetaText(doc, tag.c_str(), buffer.data(), bufferLen);

        // Convert UTF-16 to UTF-8
        return convertUtf16ToUtf8(buffer);
    };

    // Extract standard metadata
    std::string title = getMetaString("Title");
    std::string author = getMetaString("Author");
    std::string subject = getMetaString("Subject");
    std::string keywords = getMetaString("Keywords");
    std::string creator = getMetaString("Creator");
    std::string producer = getMetaString("Producer");
    std::string creationDate = getMetaString("CreationDate");
    std::string modDate = getMetaString("ModDate");

    // Add to metadata map
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
    if (!creationDate.empty())
        result.metadata["creation_date"] = creationDate;
    if (!modDate.empty())
        result.metadata["modification_date"] = modDate;

    // Get page count
    int pageCount = FPDF_GetPageCount(doc);
    result.metadata["page_count"] = std::to_string(pageCount);
}

// Convert UTF-16 to UTF-8
std::string PdfExtractor::convertUtf16ToUtf8(const std::vector<unsigned short>& utf16) {
    if (utf16.empty())
        return "";

    // Find actual string length (excluding null terminator)
    size_t len = 0;
    for (size_t i = 0; i < utf16.size(); ++i) {
        if (utf16[i] == 0)
            break;
        len++;
    }

    if (len == 0)
        return "";

    // Use codecvt for conversion
    try {
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
        std::u16string u16str(reinterpret_cast<const char16_t*>(utf16.data()), len);
        return converter.to_bytes(u16str);
#ifdef __clang__
#pragma clang diagnostic pop
#endif
    } catch (const std::exception& e) {
        spdlog::warn("UTF-16 to UTF-8 conversion failed: {}", e.what());

        // Fallback: basic ASCII extraction
        std::string result;
        for (size_t i = 0; i < len; ++i) {
            if (utf16[i] < 128) {
                result += static_cast<char>(utf16[i]);
            } else {
                result += '?'; // Replace non-ASCII with placeholder
            }
        }
        return result;
    }
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
    std::regex sectionRegex("^\\s*(?:"
                            "(?:\\d+\\.?\\s+)|"   // Numbered sections (1. or 1)
                            "(?:[IVX]+\\.?\\s+)|" // Roman numerals
                            "(?:[A-Z]\\.?\\s+)"   // Letter sections
                            ")?"
                            "(Abstract|Introduction|Background|"
                            "Related Work|Methodology|Methods|"
                            "Results|Discussion|Conclusion|References|"
                            "Appendix|Acknowledgments)",
                            std::regex::icase | std::regex::multiline);

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