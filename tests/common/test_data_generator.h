// Minimal test data generator placeholder used by some integration tests.
#pragma once
#include <string>
#include <vector>

namespace yams::tests {
inline std::string random_text(std::size_t n, char base = 'a') {
    std::string s;
    s.reserve(n);
    for (std::size_t i = 0; i < n; ++i)
        s.push_back(static_cast<char>(base + (i % 26)));
    return s;
}

inline std::vector<unsigned char> random_bytes(std::size_t n) {
    std::vector<unsigned char> v(n);
    for (std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<unsigned char>(i & 0xFF);
    return v;
}
} // namespace yams::tests
