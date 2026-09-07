// SPDX-License-Identifier: GPL-3.0-or-later
#include "glob_matcher.h"

#include <spdlog/spdlog.h>

namespace yams::app::services {

std::string GlobMatcher::toRegex(std::string_view glob) {
    std::string regexStr;
    regexStr.reserve(glob.size() * 2);
    for (size_t i = 0; i < glob.size(); ++i) {
        const char c = glob[i];
        if (c == '*') {
            if (i + 1 < glob.size() && glob[i + 1] == '*') {
                regexStr += ".*"; // '**' crosses path separators
                ++i;
            } else {
                regexStr += "[^/]*"; // '*' stays within one segment
            }
        } else if (c == '?') {
            regexStr += '.';
        } else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '{' || c == '}' ||
                   c == '[' || c == ']' || c == '^' || c == '|' || c == '\\') {
            regexStr += '\\';
            regexStr += c;
        } else {
            regexStr += c;
        }
    }
    return regexStr;
}

GlobMatcher::GlobMatcher(std::string pattern) : pattern_(std::move(pattern)) {
    try {
        regex_ = std::regex(toRegex(pattern_));
        valid_ = true;
    } catch (const std::regex_error& e) {
        spdlog::warn("Invalid glob pattern '{}' converted to regex: {}", pattern_, e.what());
        valid_ = false;
    }
}

bool GlobMatcher::matches(std::string_view text) const {
    if (!valid_) {
        return text.find(pattern_) != std::string_view::npos;
    }
    return std::regex_match(text.begin(), text.end(), regex_);
}

} // namespace yams::app::services
