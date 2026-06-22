#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

namespace yams::cli {

inline bool cli_perf_trace_enabled() {
    const char* raw = std::getenv("YAMS_CLI_PERF_TRACE"); // NOLINT(concurrency-mt-unsafe)
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "on" || value == "yes";
}

inline void cli_perf_trace(std::string_view stage, std::chrono::microseconds elapsed,
                           std::string_view note = {}) {
    if (!cli_perf_trace_enabled()) {
        return;
    }
    if (note.empty()) {
        std::fprintf(stderr, "[yams-cli-perf] stage=%.*s elapsed_us=%lld\n",
                     static_cast<int>(stage.size()), stage.data(),
                     static_cast<long long>(elapsed.count()));
    } else {
        std::fprintf(stderr, "[yams-cli-perf] stage=%.*s elapsed_us=%lld note=%.*s\n",
                     static_cast<int>(stage.size()), stage.data(),
                     static_cast<long long>(elapsed.count()), static_cast<int>(note.size()),
                     note.data());
    }
    std::fflush(stderr);
}

} // namespace yams::cli
