#pragma once

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon::detail {

/// Upper bound on sentences scanned per document, so pathological inputs cannot make fragment
/// extraction proportional to document size.
inline constexpr std::size_t kMaxScannedSentences = 512;
/// Length of the document-head fragment that gives MaxSim a coarse whole-document view.
inline constexpr std::size_t kDocumentHeadChars = 512;

/// Split text into sentence-like fragments (terminators . ? ! ; followed by whitespace, or line
/// breaks), dropping fragments shorter than five characters.
inline std::vector<std::string_view> splitSentences(std::string_view text,
                                                    std::size_t maxSentences) {
    std::vector<std::string_view> sentences;
    std::size_t start = 0;
    while (start < text.size() && sentences.size() < maxSentences) {
        while (start < text.size() && std::isspace(static_cast<unsigned char>(text[start]))) {
            ++start;
        }
        if (start >= text.size()) {
            break;
        }
        std::size_t end = start;
        while (end < text.size()) {
            const char c = text[end];
            if (c == '\n' || c == '\r') {
                break;
            }
            if ((c == '.' || c == '?' || c == '!' || c == ';') &&
                (end + 1 == text.size() ||
                 std::isspace(static_cast<unsigned char>(text[end + 1])))) {
                ++end;
                break;
            }
            ++end;
        }
        std::string_view seg = text.substr(start, end - start);
        while (!seg.empty() && std::isspace(static_cast<unsigned char>(seg.back()))) {
            seg.remove_suffix(1);
        }
        if (seg.size() >= 5) {
            sentences.push_back(seg);
        }
        start = end + 1;
    }
    return sentences;
}

/// Select at most maxFragments fragments for outer MaxSim. Sentences are sampled evenly across
/// the whole document (first and last always included) instead of taking only the leading ones,
/// so late relevant passages remain reachable. When a document has more than one sentence, one
/// slot is reserved for a document-head fragment that gives a coarse global view.
inline std::vector<std::string> selectMaxSimFragments(std::string_view text,
                                                      std::size_t maxFragments) {
    std::vector<std::string> fragments;
    if (text.empty() || maxFragments == 0) {
        return fragments;
    }
    const auto sentences = splitSentences(text, kMaxScannedSentences);
    if (sentences.empty()) {
        fragments.emplace_back(text.substr(0, std::min(text.size(), kDocumentHeadChars)));
        return fragments;
    }
    if (sentences.size() == 1 || maxFragments == 1) {
        fragments.emplace_back(sentences.front());
        return fragments;
    }

    const std::string_view head = text.substr(0, std::min(text.size(), kDocumentHeadChars));
    const std::size_t sentenceSlots = maxFragments - 1;
    const std::size_t take = std::min(sentences.size(), sentenceSlots);
    fragments.reserve(take + 1);
    for (std::size_t k = 0; k < take; ++k) {
        // Evenly spaced indices over [0, size-1], hitting both ends.
        const std::size_t index =
            take == 1 ? 0 : (k * (sentences.size() - 1) + (take - 1) / 2) / (take - 1);
        fragments.emplace_back(sentences[index]);
    }
    if (std::find(fragments.begin(), fragments.end(), head) == fragments.end()) {
        fragments.emplace_back(head);
    }
    return fragments;
}

/// Per-document fragment budget that keeps a rerank call's total fragment embeddings bounded.
inline std::size_t fragmentsPerDocument(std::size_t documentCount, std::size_t perDocumentCap,
                                        std::size_t totalCap) {
    if (documentCount == 0) {
        return perDocumentCap;
    }
    return std::clamp<std::size_t>(totalCap / documentCount, 1, perDocumentCap);
}

} // namespace yams::daemon::detail
