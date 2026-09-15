#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace yams::common {

[[nodiscard]] inline char asciiToLower(unsigned char c) noexcept {
    return static_cast<char>(std::tolower(c));
}

[[nodiscard]] inline std::string asciiToLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return asciiToLower(c); });
    return value;
}

[[nodiscard]] inline std::string asciiToLowerCopy(std::string_view value) {
    return asciiToLowerCopy(std::string(value));
}

[[nodiscard]] inline std::string asciiToLowerCopy(const char* value) {
    return asciiToLowerCopy(std::string_view(value ? value : ""));
}

[[nodiscard]] inline std::string sanitizeProjectName(std::string value) {
    if (value.empty()) {
        return "project";
    }
    for (auto& character : value) {
        const auto byte = static_cast<unsigned char>(character);
        if (!(std::isalnum(byte) || character == '-' || character == '_')) {
            character = '-';
        } else {
            character = asciiToLower(byte);
        }
    }
    return value;
}

[[nodiscard]] inline std::string sanitizeForTerminal(std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (unsigned char c : input) {
        if (c >= 0x20 && c <= 0x7E) {
            output.push_back(static_cast<char>(c));
        } else if (c == '\n' || c == '\r' || c == '\t') {
            output.push_back(static_cast<char>(c));
        } else {
            output.push_back('?');
        }
    }
    return output;
}

[[nodiscard]] inline std::string trimCopy(std::string_view value) {
    std::size_t start = 0;
    std::size_t end = value.size();
    while (start < end && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return std::string(value.substr(start, end - start));
}

/**
 * @brief Whether a string contains glob metacharacters (* or ?).
 */
[[nodiscard]] inline bool hasWildcard(std::string_view value) noexcept {
    return value.find('*') != std::string_view::npos || value.find('?') != std::string_view::npos;
}

// Regex metacharacters that break literal sequences.
inline constexpr std::string_view kRegexMetaChars = "\\^$.|?*+()[]{}";

/**
 * @brief Whether a character is a regex metacharacter.
 */
[[nodiscard]] inline bool isRegexMetaChar(char c) noexcept {
    return kRegexMetaChars.find(c) != std::string_view::npos;
}

/**
 * @brief Escape regex metacharacters so the input matches literally.
 */
[[nodiscard]] inline std::string escapeRegex(std::string_view text) {
    std::string escaped;
    escaped.reserve(text.size() * 2);
    for (char c : text) {
        if (isRegexMetaChar(c)) {
            escaped += '\\';
        }
        escaped += c;
    }
    return escaped;
}

} // namespace yams::common
