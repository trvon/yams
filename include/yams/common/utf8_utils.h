#pragma once

#include <string>
#include <string_view>

namespace yams::common {

// Replace invalid UTF-8 byte sequences with '?' to satisfy Protobuf string constraints.
inline std::string sanitizeUtf8(std::string_view input) {
    std::string out;
    out.reserve(input.size());

    const unsigned char* data = reinterpret_cast<const unsigned char*>(input.data());
    size_t i = 0;
    const size_t n = input.size();
    while (i < n) {
        unsigned char c = data[i];
        if (c < 0x80) { // ASCII
            out.push_back(static_cast<char>(c));
            ++i;
        } else if (c >= 0xC2 && c <= 0xDF && i + 1 < n) { // 2-byte sequence
            unsigned char c1 = data[i + 1];
            if ((c1 & 0xC0) == 0x80) {
                out.push_back(static_cast<char>(c));
                out.push_back(static_cast<char>(c1));
                i += 2;
            } else {
                out.push_back('?');
                ++i;
            }
        } else if (c >= 0xE0 && c <= 0xEF && i + 2 < n) { // 3-byte sequence
            unsigned char c1 = data[i + 1];
            unsigned char c2 = data[i + 2];
            if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80) {
                out.push_back(static_cast<char>(c));
                out.push_back(static_cast<char>(c1));
                out.push_back(static_cast<char>(c2));
                i += 3;
            } else {
                out.push_back('?');
                ++i;
            }
        } else if (c >= 0xF0 && c <= 0xF4 && i + 3 < n) { // 4-byte sequence
            unsigned char c1 = data[i + 1];
            unsigned char c2 = data[i + 2];
            unsigned char c3 = data[i + 3];
            if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80 && (c3 & 0xC0) == 0x80) {
                out.push_back(static_cast<char>(c));
                out.push_back(static_cast<char>(c1));
                out.push_back(static_cast<char>(c2));
                out.push_back(static_cast<char>(c3));
                i += 4;
            } else {
                out.push_back('?');
                ++i;
            }
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
    size_t i = 0;
    const size_t size = input.size();
    while (i < size) {
        const unsigned char c = data[i];
        if (c < 0x80) {
            out.push_back(static_cast<char>(c));
            ++i;
            continue;
        }

        if (c >= 0xC2 && c <= 0xDF && i + 1 < size) {
            const unsigned char c1 = data[i + 1];
            if ((c1 & 0xC0) == 0x80) {
                out.append(input, i, 2);
                i += 2;
                continue;
            }
        }

        if (c >= 0xE0 && c <= 0xEF && i + 2 < size) {
            const unsigned char c1 = data[i + 1];
            const unsigned char c2 = data[i + 2];
            const bool validContinuation = (c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80;
            const bool isOverlong = c == 0xE0 && c1 < 0xA0;
            const bool isSurrogate = c == 0xED && c1 >= 0xA0;
            if (validContinuation && !isOverlong && !isSurrogate) {
                out.append(input, i, 3);
                i += 3;
                continue;
            }
        }

        if (c >= 0xF0 && c <= 0xF4 && i + 3 < size) {
            const unsigned char c1 = data[i + 1];
            const unsigned char c2 = data[i + 2];
            const unsigned char c3 = data[i + 3];
            const bool validContinuation =
                (c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80 && (c3 & 0xC0) == 0x80;
            const bool isOverlong = c == 0xF0 && c1 < 0x90;
            const bool isOutOfRange = c == 0xF4 && c1 >= 0x90;
            if (validContinuation && !isOverlong && !isOutOfRange) {
                out.append(input, i, 4);
                i += 4;
                continue;
            }
        }

        out.append(kReplacement);
        ++i;
    }

    return out;
}

// Validate UTF-8 and return a view to the original data if valid.
// If invalid, sanitize into `storage` and return a view to it.
// Callers can hoist `storage` outside loops for capacity reuse.
inline std::string_view ensureValidUtf8(std::string_view input, std::string& storage) {
    const unsigned char* d = reinterpret_cast<const unsigned char*>(input.data());
    size_t i = 0;
    const size_t n = input.size();
    while (i < n) {
        unsigned char c = d[i];
        if (c < 0x80) {
            ++i;
        } else if (c >= 0xC2 && c <= 0xDF && i + 1 < n && (d[i + 1] & 0xC0) == 0x80) {
            i += 2;
        } else if (c >= 0xE0 && c <= 0xEF && i + 2 < n && (d[i + 1] & 0xC0) == 0x80 &&
                   (d[i + 2] & 0xC0) == 0x80) {
            i += 3;
        } else if (c >= 0xF0 && c <= 0xF4 && i + 3 < n && (d[i + 1] & 0xC0) == 0x80 &&
                   (d[i + 2] & 0xC0) == 0x80 && (d[i + 3] & 0xC0) == 0x80) {
            i += 4;
        } else {
            storage = sanitizeUtf8(input);
            return storage;
        }
    }
    return input;
}

} // namespace yams::common
