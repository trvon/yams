#pragma once

#include <algorithm>
#include <cctype>
#include <string>

namespace yams::cli {

inline bool envValueTruthy(const char* raw) {
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "on" || value == "yes";
}

} // namespace yams::cli
