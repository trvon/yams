// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/ResourceGovernor.h>

using yams::daemon::memoryPressureRatio;
using yams::daemon::parseProcStatusMemory;

TEST_CASE("parseProcStatusMemory splits RSS into anonymous and file-backed",
          "[daemon][governor][memory][catch2]") {
    const char* text = "Name:\tyams-daemon\nVmPeak:\t 900000 kB\nVmRSS:\t  724400 kB\n"
                       "RssAnon:\t  438000 kB\nRssFile:\t  286000 kB\nRssShmem:\t     400 kB\n"
                       "Threads:\t90\n";
    const auto mem = parseProcStatusMemory(text);
    CHECK(mem.rssBytes == 724400ull * 1024ull);
    CHECK(mem.anonBytes == 438000ull * 1024ull);
    CHECK(mem.fileBytes == (286000ull + 400ull) * 1024ull);
    // Older kernels without RssAnon: anon stays 0 (unknown), rss still parsed.
    const auto old = parseProcStatusMemory("VmRSS:\t 1000 kB\nThreads:\t3\n");
    CHECK(old.rssBytes == 1000ull * 1024ull);
    CHECK(old.anonBytes == 0);
    CHECK(parseProcStatusMemory("").rssBytes == 0);
}

TEST_CASE("memoryPressureRatio judges pressure on anonymous memory when known",
          "[daemon][governor][memory][catch2]") {
    const std::uint64_t budget = 1000;
    // 700 MiB RSS of which 300 anon: pressure follows anon, not the reclaimable file pages.
    CHECK(memoryPressureRatio(700, 300, budget) == Catch::Approx(0.3));
    // anon unknown (0): fall back to rss
    CHECK(memoryPressureRatio(700, 0, budget) == Catch::Approx(0.7));
    CHECK(memoryPressureRatio(700, 300, 0) == 0.0);
}
