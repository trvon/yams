// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <regex>
#include <string>
#include <string_view>

namespace yams::app::services {

// Path glob compiled once and matched many times. '*' matches within one path segment,
// '**' crosses segments, '?' matches one character; an invalid pattern degrades to a
// substring test, as the previous per-call matcher did.
class GlobMatcher {
public:
    explicit GlobMatcher(std::string pattern);

    [[nodiscard]] bool matches(std::string_view text) const;
    [[nodiscard]] const std::string& pattern() const noexcept { return pattern_; }

    static std::string toRegex(std::string_view glob);

private:
    std::string pattern_;
    std::regex regex_;
    bool valid_{false};
};

} // namespace yams::app::services
