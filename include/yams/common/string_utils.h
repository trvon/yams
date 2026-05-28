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

} // namespace yams::common
