#pragma once

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace yams::bench {

template <typename T> T percentileNearestRank(std::vector<T> values, double q) {
    if (values.empty()) {
        return T{};
    }
    std::sort(values.begin(), values.end());
    const auto idx = std::min(values.size() - 1,
                              static_cast<std::size_t>(q * static_cast<double>(values.size() - 1)));
    return values[idx];
}

inline std::string isoTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const auto tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &tt);
#else
    gmtime_r(&tt, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

inline int readIntEnv(const char* name, int fallback, int minVal, int maxVal) {
    const char* val = std::getenv(name);
    if (!val || !*val) {
        return fallback;
    }
    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(val, &end, 10);
    if (errno != 0 || end == val || *end != '\0') {
        return fallback;
    }
    return static_cast<int>(std::clamp<long>(parsed, minVal, maxVal));
}

inline bool readBoolEnv(const char* name, bool fallback) {
    const char* val = std::getenv(name);
    if (!val || !*val) {
        return fallback;
    }
    std::string s(val);
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (s == "1" || s == "true" || s == "yes" || s == "on") {
        return true;
    }
    if (s == "0" || s == "false" || s == "no" || s == "off") {
        return false;
    }
    return fallback;
}

} // namespace yams::bench
