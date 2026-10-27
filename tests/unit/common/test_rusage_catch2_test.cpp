// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <support/test_rusage.hpp>

#include <cstdint>
#include <string>
#include <vector>

using namespace yams::test::rusage;

TEST_CASE("rusage: maxrss normalisation", "[unit][common][rusage]") {
    CHECK(normalizeMaxRss(2048, true) == 2048);
    CHECK(normalizeMaxRss(2048, false) == 2048ULL * 1024ULL);
}

TEST_CASE("rusage: CPU delta tracks burned work and is monotonic", "[unit][common][rusage]") {
    const Sample before = sampleSelf();
    volatile std::uint64_t sink = 0;
    for (std::uint64_t i = 0; i < 50'000'000ULL; ++i) {
        sink = sink + i;
    }
    const Sample after = sampleSelf();
    CHECK(cpuDeltaMicros(before, after) > 0);
    CHECK(cpuDeltaMicros(after, before) == 0);
    CHECK(after.peakRssBytes > 0);
    CHECK(after.peakRssBytes >= before.peakRssBytes);
}

TEST_CASE("rusage: top-N ordering and formatting", "[unit][common][rusage]") {
    std::vector<CaseCost> cases{{"a", 1000}, {"b", 9000}, {"c", 5000}, {"d", 5000}, {"e", 2000}};
    const auto top = topByCpu(cases, 3);
    REQUIRE(top.size() == 3);
    CHECK(top[0].name == "b");
    CHECK(top[1].name == "c");
    CHECK(top[2].name == "d"); // stable on ties
    CHECK(formatTopCpu(top) == "[rusage] top cpu: b 9ms, c 5ms, d 5ms");
    CHECK(formatTopCpu({}) == "[rusage] top cpu: (none)");
    CHECK(formatPeakRss(84ULL * 1024ULL * 1024ULL) == "[rusage] peak rss: 84 MB");
    CHECK(clampName(std::string(100, 'x')).size() == 60);
}
