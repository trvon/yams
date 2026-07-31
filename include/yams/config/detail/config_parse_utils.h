#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace yams::config::detail {

// True when the standard library provides the floating-point overload of
// std::from_chars (i.e. with a std::chars_format argument). Some Apple libc++
// versions shipped with older Xcode SDKs only provide the integral overload,
// so detect the capability instead of depending on version macros.
template <typename T, typename = void> struct hasFloatingPointFromChars : std::false_type {};

template <typename T>
struct hasFloatingPointFromChars<T, std::void_t<decltype(std::from_chars(
                                        std::declval<const char*>(), std::declval<const char*>(),
                                        std::declval<T&>(), std::declval<std::chars_format>()))>>
    : std::true_type {};

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

// strtod-based double parsing mirroring std::from_chars(std::chars_format::general) for
// standard libraries without the floating-point overload (e.g. some Apple libc++).
// Rejects partial consumption, hex floats, and the inf/nan spellings that from_chars rejects.
inline std::optional<double> parseDoubleViaStrtod(std::string_view view) {
    if (view.empty() || std::isspace(static_cast<unsigned char>(view.front())) != 0) {
        return std::nullopt;
    }
    if (view.size() >= 2 && view[0] == '0' && (view[1] == 'x' || view[1] == 'X')) {
        return std::nullopt;
    }
    const std::string value(view);
    char* endPtr = nullptr;
    const double result = std::strtod(value.c_str(), &endPtr);
    if (endPtr == value.c_str() || *endPtr != '\0' || !std::isfinite(result)) {
        return std::nullopt;
    }
    return result;
}

// Template indirection: the std::from_chars overload taking std::chars_format is resolved only
// when this helper is instantiated, so on standard libraries without the floating-point
// overload the discarded branch below never instantiates it (overload resolution failure would
// otherwise be diagnosed in a non-template if constexpr branch).
template <typename T> inline std::optional<double> parseDoubleViaFromChars(std::string_view view) {
    const char* const begin = view.data();
    const char* const end = begin + view.size();
    T parsed{};
    const auto [ptr, error] = std::from_chars(begin, end, parsed, std::chars_format::general);
    if (error != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return parsed;
}

inline std::optional<double> parseDouble(std::string_view raw) {
    auto view = trimView(raw);
    if (!view.empty() && view.front() == '+') {
        view.remove_prefix(1);
    }
    if (view.empty()) {
        return std::nullopt;
    }
    if constexpr (hasFloatingPointFromChars<double>::value) {
        return parseDoubleViaFromChars<double>(view);
    }
    return parseDoubleViaStrtod(view);
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
