// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/query_text_utils.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <unordered_map>
#include <unordered_set>

namespace yams::search {

namespace {

std::string replaceAllCopy(std::string input, std::string_view from, std::string_view to) {
    if (from.empty()) {
        return input;
    }
    size_t pos = 0;
    while ((pos = input.find(from.data(), pos, from.size())) != std::string::npos) {
        input.replace(pos, from.size(), to.data(), to.size());
        pos += to.size();
    }
    return input;
}

std::string expandCommonGreekLetters(std::string_view input) {
    std::string out(input);
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 18> kGreekMap = {{
        {"\xCE\xB1", " alpha "},
        {"\xCE\x91", " alpha "},
        {"\xCE\xB2", " beta "},
        {"\xCE\x92", " beta "},
        {"\xCE\xB3", " gamma "},
        {"\xCE\x93", " gamma "},
        {"\xCE\xB4", " delta "},
        {"\xCE\x94", " delta "},
        {"\xCE\xBA", " kappa "},
        {"\xCE\x9A", " kappa "},
        {"\xCE\xBB", " lambda "},
        {"\xCE\x9B", " lambda "},
        {"\xCE\xBC", " mu "},
        {"\xCE\x9C", " mu "},
        {"\xCF\x84", " tau "},
        {"\xCE\xA4", " tau "},
        {"\xCE\xBD", " nu "},
        {"\xCE\x9D", " nu "},
    }};
    for (const auto& [needle, replacement] : kGreekMap) {
        out = replaceAllCopy(std::move(out), needle, replacement);
    }
    return out;
}

std::vector<std::string> splitCodeSymbolTokens(std::string_view input) {
    std::vector<std::string> tokens;
    std::string current;
    current.reserve(input.size());

    const auto flush = [&]() {
        if (!current.empty()) {
            tokens.push_back(current);
            current.clear();
        }
    };

    char prev = '\0';
    for (unsigned char uc : input) {
        const char c = static_cast<char>(uc);
        const bool delimiter = c == ':' || c == '.' || c == '/' || c == '\\' || c == '_' ||
                               c == '-' || std::isspace(uc);
        if (delimiter) {
            flush();
            prev = '\0';
            continue;
        }

        if (std::isupper(uc) && !current.empty() &&
            (std::islower(static_cast<unsigned char>(prev)) ||
             std::isdigit(static_cast<unsigned char>(prev)))) {
            flush();
        }

        current.push_back(static_cast<char>(std::tolower(uc)));
        prev = c;
    }
    flush();
    return tokens;
}

void addUniqueVariant(std::vector<std::string>& out, std::unordered_set<std::string>& seen,
                      std::string value, size_t maxVariants) {
    value = trimAndCollapseWhitespace(value);
    if (value.size() < 2) {
        return;
    }
    if (!seen.insert(value).second) {
        return;
    }
    out.push_back(std::move(value));
    if (out.size() > maxVariants) {
        out.resize(maxVariants);
    }
}

} // namespace

std::string toLowerCopy(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    for (unsigned char c : input) {
        out.push_back(static_cast<char>(std::tolower(c)));
    }
    return out;
}

std::string trimAndCollapseWhitespace(std::string_view input) {
    std::string out;
    out.reserve(input.size());

    bool inWs = false;
    size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }

    for (size_t i = start; i < end; ++i) {
        unsigned char c = static_cast<unsigned char>(input[i]);
        if (std::isspace(c)) {
            if (!inWs) {
                out.push_back(' ');
                inWs = true;
            }
        } else {
            out.push_back(static_cast<char>(c));
            inWs = false;
        }
    }

    return out;
}

std::string normalizeEntityTextForKey(std::string_view input) {
    return toLowerCopy(trimAndCollapseWhitespace(input));
}

std::string normalizeGraphSurface(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    bool inWs = false;
    for (unsigned char c : input) {
        if (std::isalnum(c)) {
            out.push_back(static_cast<char>(std::tolower(c)));
            inWs = false;
        } else if (!out.empty() && !inWs) {
            out.push_back(' ');
            inWs = true;
        }
    }
    while (!out.empty() && out.back() == ' ') {
        out.pop_back();
    }
    return out;
}

const std::array<std::string_view, 19>& defaultQueryEntityTypes() {
    static const std::array<std::string_view, 19> kDefaultEntityTypes = {
        "technology", "concept", "organization", "person",
        "location",   "product", "language",     "framework",
        "protein",    "gene",    "cell",         "disease",
        "chemical",   "drug",    "pathway",      "biological_process",
        "biomarker",  "anatomy", "organism",
    };
    return kDefaultEntityTypes;
}

bool looksLikeCompactBiomedicalSymbol(std::string_view input) {
    size_t alphaCount = 0;
    size_t digitCount = 0;
    size_t upperCount = 0;
    for (unsigned char c : input) {
        if (std::isalpha(c)) {
            ++alphaCount;
            if (std::isupper(c)) {
                ++upperCount;
            }
        } else if (std::isdigit(c)) {
            ++digitCount;
        }
    }
    if (input.size() < 2 || input.size() > 24 || alphaCount == 0) {
        return false;
    }
    if (digitCount > 0 && upperCount > 0) {
        return true;
    }
    return upperCount >= 2 && alphaCount == upperCount && input.size() <= 8;
}

std::string canonicalizeEntityType(std::string_view rawType, std::string_view rawText) {
    static const std::unordered_map<std::string, std::string> kTypeAliases = {
        {"org", "organization"},  {"company", "organization"}, {"institution", "organization"},
        {"loc", "location"},      {"place", "location"},       {"tool", "technology"},
        {"library", "framework"},
    };

    (void)rawText;
    std::string type = normalizeEntityTextForKey(rawType);
    if (auto it = kTypeAliases.find(type); it != kTypeAliases.end()) {
        type = it->second;
    }
    if (type == "technology") {
        return "method";
    }
    return type.empty() ? "concept" : type;
}

bool isLowValueEntityText(std::string_view normalizedText, std::string_view normalizedType) {
    if (normalizedText.empty()) {
        return true;
    }
    if (normalizedType == "date" || normalizedType == "time" || normalizedType == "duration" ||
        normalizedType == "number" || normalizedType == "percentage" ||
        normalizedType == "ordinal") {
        return true;
    }
    return false;
}

SurfaceVariantKind surfaceVariantKindForEntityType(std::string_view entityType) {
    const std::string normalized = canonicalizeEntityType(entityType, {});
    if (normalized == "protein" || normalized == "gene" || normalized == "cell" ||
        normalized == "disease" || normalized == "drug" || normalized == "chemical" ||
        normalized == "pathway" || normalized == "biological_process" ||
        normalized == "biomarker" || normalized == "anatomy" || normalized == "organism") {
        return SurfaceVariantKind::Biomedical;
    }
    if (normalized == "class" || normalized == "function" || normalized == "method" ||
        normalized == "namespace" || normalized == "symbol" || normalized == "module" ||
        normalized == "field") {
        return SurfaceVariantKind::CodeSymbol;
    }
    return SurfaceVariantKind::General;
}

std::vector<std::string> generateSurfaceVariants(std::string_view input, SurfaceVariantKind kind,
                                                 size_t maxVariants) {
    if (maxVariants == 0 || input.empty()) {
        return {};
    }

    std::vector<std::string> variants;
    variants.reserve(std::min<size_t>(maxVariants, 16));
    std::unordered_set<std::string> seen;
    seen.reserve(maxVariants * 2);

    auto addFromText = [&](std::string_view text) {
        addUniqueVariant(variants, seen, normalizeEntityTextForKey(text), maxVariants);
        if (variants.size() >= maxVariants) {
            return;
        }
        const auto graphSurface = normalizeGraphSurface(text);
        if (!graphSurface.empty()) {
            addUniqueVariant(variants, seen, graphSurface, maxVariants);
        }
    };

    std::string greekExpanded = expandCommonGreekLetters(input);
    addFromText(input);
    if (variants.size() >= maxVariants) {
        return variants;
    }
    if (greekExpanded != input) {
        addFromText(greekExpanded);
        if (variants.size() >= maxVariants) {
            return variants;
        }
    }

    auto emitTokenVariants = [&](const std::vector<std::string>& tokens) {
        if (tokens.empty()) {
            return;
        }
        for (const auto& token : tokens) {
            const size_t minLen = kind == SurfaceVariantKind::General ? 3u : 2u;
            if (token.size() < minLen) {
                continue;
            }
            addUniqueVariant(variants, seen, token, maxVariants);
            if (variants.size() >= maxVariants) {
                return;
            }
        }

        if (tokens.size() >= 2) {
            std::string joined = tokens.front();
            for (size_t i = 1; i < tokens.size(); ++i) {
                joined.push_back(' ');
                joined += tokens[i];
            }
            addUniqueVariant(variants, seen, joined, maxVariants);
        }
    };

    emitTokenVariants(tokenizeLower(greekExpanded));
    if (variants.size() >= maxVariants) {
        return variants;
    }

    if (kind == SurfaceVariantKind::CodeSymbol) {
        const auto codeTokens = splitCodeSymbolTokens(greekExpanded);
        emitTokenVariants(codeTokens);
        if (variants.size() >= maxVariants) {
            return variants;
        }

        if (codeTokens.size() >= 2) {
            for (size_t i = 1; i < codeTokens.size(); ++i) {
                std::string suffix = codeTokens[i];
                for (size_t j = i + 1; j < codeTokens.size(); ++j) {
                    suffix.push_back(' ');
                    suffix += codeTokens[j];
                }
                addUniqueVariant(variants, seen, suffix, maxVariants);
                if (variants.size() >= maxVariants) {
                    return variants;
                }
            }
        }
    }

    return variants;
}

std::vector<std::string> tokenizeLower(const std::string& input) {
    std::string normalized = input;
    for (char& c : normalized) {
        if (c == '\\') {
            c = '/';
        }
    }

    std::vector<std::string> tokens;
    std::string current;
    for (unsigned char c : normalized) {
        if (std::isalnum(c)) {
            current.push_back(static_cast<char>(std::tolower(c)));
        } else if (!current.empty()) {
            tokens.push_back(std::move(current));
            current.clear();
        }
    }
    if (!current.empty()) {
        tokens.push_back(std::move(current));
    }
    return tokens;
}

std::vector<QueryToken> tokenizeQueryTokens(const std::string& input) {
    std::vector<QueryToken> tokens;
    std::string currentOriginal;
    std::string currentNormalized;

    const auto flush = [&]() {
        if (currentNormalized.empty()) {
            currentOriginal.clear();
            currentNormalized.clear();
            return;
        }

        QueryToken token;
        token.original = currentOriginal;
        token.normalized = currentNormalized;
        token.index = tokens.size();
        tokens.push_back(std::move(token));
        currentOriginal.clear();
        currentNormalized.clear();
    };

    for (unsigned char c : input) {
        if (std::isalnum(c)) {
            currentOriginal.push_back(static_cast<char>(c));
            currentNormalized.push_back(static_cast<char>(std::tolower(c)));
        } else {
            flush();
        }
    }
    flush();
    return tokens;
}

} // namespace yams::search
