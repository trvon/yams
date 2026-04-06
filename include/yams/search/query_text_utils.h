// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::search {

enum class SurfaceVariantKind {
    General,
    Biomedical,
    CodeSymbol,
};

struct QueryToken {
    std::string original;
    std::string normalized;
    size_t index = 0;
};

std::string toLowerCopy(std::string_view input);
std::string trimAndCollapseWhitespace(std::string_view input);
std::string normalizeEntityTextForKey(std::string_view input);
std::string normalizeGraphSurface(std::string_view input);
const std::array<std::string_view, 19>& defaultQueryEntityTypes();
bool looksLikeCompactBiomedicalSymbol(std::string_view input);
std::string canonicalizeEntityType(std::string_view rawType, std::string_view rawText = {});
bool isLowValueEntityText(std::string_view normalizedText, std::string_view normalizedType);
SurfaceVariantKind surfaceVariantKindForEntityType(std::string_view entityType);
std::vector<std::string> generateSurfaceVariants(std::string_view input, SurfaceVariantKind kind,
                                                 size_t maxVariants = 16);

std::vector<std::string> tokenizeLower(const std::string& input);
std::vector<QueryToken> tokenizeQueryTokens(const std::string& input);

} // namespace yams::search
