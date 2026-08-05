// SPDX-License-Identifier: GPL-3.0-or-later

#include "../../common/benchmark_invocation.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>

using yams::test::isBenchmarkListArgument;
using yams::test::normalizeBenchmarkListArguments;

TEST_CASE("benchmark list aliases are registration-only", "[benchmark][harness][arguments]") {
    CHECK(isBenchmarkListArgument("--benchmark_list_tests"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=true"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=1"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=yes"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=on"));
    CHECK(isBenchmarkListArgument("--list-tests"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=false"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=0"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=no"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=off"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_filter=BM_Ingestion"));
}

TEST_CASE("Catch2 list alias is normalized for Google Benchmark",
          "[benchmark][harness][arguments]") {
    std::array<std::string, 3> storage{"bench", "--list-tests", "--benchmark_min_time=1ms"};
    std::array<char*, 3> argv{storage[0].data(), storage[1].data(), storage[2].data()};

    CHECK(normalizeBenchmarkListArguments(static_cast<int>(argv.size()), argv.data()));
    CHECK((std::string{argv[1]} == "--benchmark_list_tests"));
}

TEST_CASE("normal benchmark invocation still requires setup", "[benchmark][harness][arguments]") {
    std::array<std::string, 2> storage{"bench", "--benchmark_filter=BM_Ingestion"};
    std::array<char*, 2> argv{storage[0].data(), storage[1].data()};

    CHECK_FALSE(normalizeBenchmarkListArguments(static_cast<int>(argv.size()), argv.data()));
    CHECK((std::string{argv[1]} == "--benchmark_filter=BM_Ingestion"));
}
