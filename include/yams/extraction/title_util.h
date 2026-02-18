#pragma once

#include <string>
#include <string_view>

namespace yams::extraction::util {

std::string extractHtmlTitle(std::string_view text);
std::string extractMarkdownHeading(std::string_view text);
std::string extractCodeSignature(std::string_view text);
std::string extractFirstMeaningfulLine(std::string_view text);

// Helper to normalize a title candidate (trim, collapse whitespace, truncate)
std::string normalizeTitleCandidate(std::string s);

} // namespace yams::extraction::util
