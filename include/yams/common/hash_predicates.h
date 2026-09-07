// yams/common/hash_predicates.h - shared "does this look like a content hash" predicates
#pragma once

#include <cctype>
#include <string_view>

namespace yams::common {

/// True when every character is a hexadecimal digit (an empty string qualifies).
[[nodiscard]] inline bool isHexDigits(std::string_view value) noexcept {
    for (unsigned char ch : value) {
        if (std::isxdigit(ch) == 0)
            return false;
    }
    return true;
}

/// A bare search token is only treated as a hash prefix once it reaches 8 hex characters, so
/// short code-like tokens ("cafe", "add") keep routing to text search. Upper bound is a full
/// SHA-256 (64 characters).
[[nodiscard]] inline bool looksLikeHashQueryToken(std::string_view value) noexcept {
    return value.size() >= 8 && value.size() <= 64 && isHexDigits(value);
}

/// An explicit hash argument (--hash, a resolver target the caller marked as a hash) may be as
/// short as 6 hex characters: the operator asked for a hash, so ambiguity is reported rather
/// than silently ignored. Upper bound is a full SHA-256 (64 characters).
[[nodiscard]] inline bool looksLikePartialHashArgument(std::string_view value) noexcept {
    return value.size() >= 6 && value.size() <= 64 && isHexDigits(value);
}

} // namespace yams::common
