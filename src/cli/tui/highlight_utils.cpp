#include <yams/cli/tui/highlight_utils.hpp>

#include <algorithm>
#include <cctype>

namespace yams::cli::tui {

namespace {
std::string toLower(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    for (char c : input) {
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    return out;
}
} // namespace

std::vector<HighlightSegment> computeHighlightSegments(std::string_view text,
                                                       std::string_view query) {
    std::vector<HighlightSegment> segments;
    if (query.empty()) {
        segments.push_back({std::string(text), false});
        return segments;
    }

    std::string lowerText = toLower(text);
    std::string lowerQuery = toLower(query);

    std::size_t pos = 0;
    while (pos < text.size()) {
        auto match = lowerText.find(lowerQuery, pos);
        if (match == std::string::npos) {
            segments.push_back({std::string(text.substr(pos)), false});
            break;
        }
        if (match > pos) {
            segments.push_back({std::string(text.substr(pos, match - pos)), false});
        }
        segments.push_back({std::string(text.substr(match, lowerQuery.size())), true});
        pos = match + lowerQuery.size();
    }

    if (segments.empty()) {
        segments.push_back({std::string(text), false});
    }

    return segments;
}

} // namespace yams::cli::tui
