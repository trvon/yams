// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/query_text_utils.h>

#include <cctype>

namespace yams::search {

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
