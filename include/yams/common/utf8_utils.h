#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace yams::common {

// Length of the well-formed UTF-8 sequence starting at data[i], or 0 if it is malformed.
// Rejects overlong encodings, UTF-16 surrogate code points (U+D800..U+DFFF) and values above
// U+10FFFF, matching what protobuf accepts for proto3 string fields.
inline size_t wellFormedUtf8Length(const unsigned char* data, size_t i, size_t size) noexcept {
    const unsigned char c = data[i];
    if (c < 0x80) {
        return 1;
    }
    auto cont = [&](size_t k) { return (data[i + k] & 0xC0) == 0x80; };
    if (c >= 0xC2 && c <= 0xDF) {
        return i + 1 < size && cont(1) ? 2 : 0;
    }
    if (c >= 0xE0 && c <= 0xEF) {
        if (i + 2 >= size || !cont(1) || !cont(2)) {
            return 0;
        }
        const unsigned char c1 = data[i + 1];
        if ((c == 0xE0 && c1 < 0xA0) || (c == 0xED && c1 >= 0xA0)) {
            return 0; // overlong or surrogate
        }
        return 3;
    }
    if (c >= 0xF0 && c <= 0xF4) {
        if (i + 3 >= size || !cont(1) || !cont(2) || !cont(3)) {
            return 0;
        }
        const unsigned char c1 = data[i + 1];
        if ((c == 0xF0 && c1 < 0x90) || (c == 0xF4 && c1 >= 0x90)) {
            return 0; // overlong or above U+10FFFF
        }
        return 4;
    }
    return 0;
}

// Replace each byte that does not start a well-formed UTF-8 sequence with '?' so the result
// satisfies Protobuf string constraints.
inline std::string sanitizeUtf8(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    const auto* data = reinterpret_cast<const unsigned char*>(input.data());
    const size_t size = input.size();
    size_t i = 0;
    while (i < size) {
        if (const size_t len = wellFormedUtf8Length(data, i, size); len > 0) {
            out.append(input, i, len);
            i += len;
        } else {
            out.push_back('?');
            ++i;
        }
    }
    return out;
}

// Preserve well-formed UTF-8 and replace each malformed input byte with U+FFFD.
// This rejects overlong encodings, UTF-16 surrogate code points, and values above U+10FFFF.
inline std::string sanitizeUtf8Strict(std::string_view input) {
    constexpr std::string_view kReplacement = "\xEF\xBF\xBD";

    std::string out;
    out.reserve(input.size());
    const auto* data = reinterpret_cast<const unsigned char*>(input.data());
    const size_t size = input.size();
    size_t i = 0;
    while (i < size) {
        if (const size_t len = wellFormedUtf8Length(data, i, size); len > 0) {
            out.append(input, i, len);
            i += len;
        } else {
            out.append(kReplacement);
            ++i;
        }
    }
    return out;
}

// Validate UTF-8 and return a view to the original data if valid.
// If invalid, sanitize into `storage` and return a view to it.
// Callers can hoist `storage` outside loops for capacity reuse.
inline std::string_view ensureValidUtf8(std::string_view input, std::string& storage) {
    const auto* data = reinterpret_cast<const unsigned char*>(input.data());
    const size_t size = input.size();
    size_t i = 0;
    while (i < size) {
        const size_t len = wellFormedUtf8Length(data, i, size);
        if (len == 0) {
            storage = sanitizeUtf8(input);
            return storage;
        }
        i += len;
    }
    return input;
}

} // namespace yams::common
