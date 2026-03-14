// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::search {

struct QueryToken {
    std::string original;
    std::string normalized;
    size_t index = 0;
};

std::string toLowerCopy(std::string_view input);
std::string trimAndCollapseWhitespace(std::string_view input);
std::string normalizeEntityTextForKey(std::string_view input);
std::string normalizeGraphSurface(std::string_view input);

std::vector<std::string> tokenizeLower(const std::string& input);
std::vector<QueryToken> tokenizeQueryTokens(const std::string& input);

} // namespace yams::search
