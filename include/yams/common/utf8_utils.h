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

} // namespace yams::common
