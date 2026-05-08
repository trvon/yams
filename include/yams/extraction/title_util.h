#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::extraction::util {

std::string extractHtmlTitle(std::string_view text);
std::string extractMarkdownHeading(std::string_view text);
std::string extractCodeSignature(std::string_view text);
std::string extractFirstMeaningfulLine(std::string_view text);

// Helper to normalize a title candidate (trim, collapse whitespace, truncate)
std::string normalizeTitleCandidate(std::string s);

// IMRAD-aware document structure extraction.
// Detects section boundaries in scientific/technical prose using regex
// patterns for common section headers. Training-free.
struct DocumentSections {
    std::string title;    // text before any recognized section header
    std::string abstract; // text in abstract/summary section
    std::string body;     // everything else

    struct Section {
        std::string name; // "abstract", "introduction", "methods", etc.
        std::size_t startOffset = 0;
        std::size_t endOffset = 0;
    };
    std::vector<Section> sections;

    bool has_abstract() const noexcept { return !abstract.empty(); }
};

DocumentSections detectDocumentSections(std::string_view text);

} // namespace yams::extraction::util
