// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Per-process CPU/RSS measurement helpers used by the Catch2 resource listener in
// catch2_main.cpp. Header-only so the unit test can exercise them without the listener.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
// psapi.h needs windows.h first; keep this order.
#include <psapi.h>
#else
#include <sys/resource.h>
#include <sys/time.h>
#endif

namespace yams::test::rusage {

struct Sample {
    std::uint64_t cpuMicros{0};    // user + system CPU of this process
    std::uint64_t peakRssBytes{0}; // high-water resident set size
};

// ru_maxrss is bytes on macOS and kilobytes on Linux/BSD.
constexpr std::uint64_t normalizeMaxRss(std::uint64_t raw, bool rawIsBytes) noexcept {
    return rawIsBytes ? raw : raw * 1024ULL;
}

inline Sample sampleSelf() noexcept {
    Sample out;
#if defined(_WIN32)
    FILETIME creation{}, exit{}, kernel{}, user{};
    if (::GetProcessTimes(::GetCurrentProcess(), &creation, &exit, &kernel, &user)) {
        const auto toMicros = [](const FILETIME& ft) {
            ULARGE_INTEGER v;
            v.LowPart = ft.dwLowDateTime;
            v.HighPart = ft.dwHighDateTime;
            return static_cast<std::uint64_t>(v.QuadPart / 10ULL); // 100 ns ticks
        };
        out.cpuMicros = toMicros(kernel) + toMicros(user);
    }
    PROCESS_MEMORY_COUNTERS counters{};
    if (::GetProcessMemoryInfo(::GetCurrentProcess(), &counters, sizeof(counters))) {
        out.peakRssBytes = static_cast<std::uint64_t>(counters.PeakWorkingSetSize);
    }
#else
    ::rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) == 0) {
        const auto toMicros = [](const timeval& tv) {
            return static_cast<std::uint64_t>(tv.tv_sec) * 1000000ULL +
                   static_cast<std::uint64_t>(tv.tv_usec);
        };
        out.cpuMicros = toMicros(usage.ru_utime) + toMicros(usage.ru_stime);
#if defined(__APPLE__)
        constexpr bool kRawBytes = true;
#else
        constexpr bool kRawBytes = false;
#endif
        out.peakRssBytes = normalizeMaxRss(static_cast<std::uint64_t>(usage.ru_maxrss), kRawBytes);
    }
#endif
    return out;
}

inline std::uint64_t cpuDeltaMicros(const Sample& before, const Sample& after) noexcept {
    return after.cpuMicros >= before.cpuMicros ? after.cpuMicros - before.cpuMicros : 0;
}

struct CaseCost {
    std::string name;
    std::uint64_t cpuMicros{0};
};

// The n most CPU-expensive cases, heaviest first; ties keep insertion order.
inline std::vector<CaseCost> topByCpu(std::vector<CaseCost> cases, std::size_t n) {
    std::stable_sort(cases.begin(), cases.end(), [](const CaseCost& a, const CaseCost& b) {
        return a.cpuMicros > b.cpuMicros;
    });
    if (cases.size() > n) {
        cases.resize(n);
    }
    return cases;
}

// Names are clamped so one pathological test name cannot flood the summary line.
inline std::string clampName(const std::string& name, std::size_t maxLen = 60) {
    return name.size() <= maxLen ? name : name.substr(0, maxLen - 3) + "...";
}

inline std::string formatTopCpu(const std::vector<CaseCost>& top) {
    std::string line = "[rusage] top cpu:";
    bool first = true;
    for (const auto& c : top) {
        line += first ? " " : ", ";
        first = false;
        line += clampName(c.name) + " " + std::to_string(c.cpuMicros / 1000ULL) + "ms";
    }
    if (top.empty()) {
        line += " (none)";
    }
    return line;
}

inline std::string formatPeakRss(std::uint64_t bytes) {
    return "[rusage] peak rss: " + std::to_string(bytes / (1024ULL * 1024ULL)) + " MB";
}

} // namespace yams::test::rusage
