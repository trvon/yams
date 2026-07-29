#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <optional>
#include <string>
#include <string_view>

namespace yams::config::detail {

inline std::string_view trimView(std::string_view raw) {
    while (!raw.empty() && std::isspace(static_cast<unsigned char>(raw.front())) != 0) {
        raw.remove_prefix(1);
    }
    while (!raw.empty() && std::isspace(static_cast<unsigned char>(raw.back())) != 0) {
        raw.remove_suffix(1);
    }
    return raw;
}

template <typename T> std::optional<T> parseUnsignedIntegral(std::string_view raw) {
    raw = trimView(raw);
    if (raw.empty() || raw.front() == '-') {
        return std::nullopt;
    }
    T value{};
    const char* begin = raw.data();
    const char* end = begin + raw.size();
    auto [ptr, error] = std::from_chars(begin, end, value);
    if (error != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

inline std::optional<double> parseDouble(std::string_view raw) {
    auto view = trimView(raw);
    if (!view.empty() && view.front() == '+') {
        view.remove_prefix(1);
    }
    if (view.empty()) {
        return std::nullopt;
    }
    double parsed{};
    const char* const begin = view.data();
    const char* const end = begin + view.size();
    const auto [ptr, error] = std::from_chars(begin, end, parsed, std::chars_format::general);
    if (error != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return parsed;
}

inline std::optional<bool> parseTomlBool(std::string_view raw) {
    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (value == "true" || value == "1" || value == "yes" || value == "on") {
        return true;
    }
    if (value == "false" || value == "0" || value == "no" || value == "off") {
        return false;
    }
    return std::nullopt;
}

} // namespace yams::config::detail
