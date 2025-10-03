#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace yams::cli::tui {

struct HighlightSegment {
    std::string text;
    bool highlighted = false;
};

std::vector<HighlightSegment> computeHighlightSegments(std::string_view text,
                                                       std::string_view query);

} // namespace yams::cli::tui
