#include <catch2/catch_test_macros.hpp>

#include <yams/common/time_utils.h>

#include <chrono>
#include <cstdint>

using namespace std::chrono_literals;

TEST_CASE("time utils convert system_clock time points to epoch seconds", "[common][time]") {
    const std::chrono::system_clock::time_point epoch{};
    CHECK(yams::common::timePointToEpochSeconds(epoch) == 0);

    const auto positive = epoch + 42s + 999ms;
    CHECK(yams::common::timePointToEpochSeconds(positive) == 42);

    const auto negative = epoch - 42s;
    CHECK(yams::common::timePointToEpochSeconds(negative) == -42);
}

TEST_CASE("time utils convert epoch seconds to system_clock time points", "[common][time]") {
    const std::int64_t seconds = 1'704'067'200;
    const auto tp = yams::common::epochSecondsToTimePoint(seconds);

    CHECK(yams::common::timePointToEpochSeconds(tp) == seconds);
    CHECK(yams::common::epochSecondsToTimePoint(-42) ==
          std::chrono::system_clock::time_point{std::chrono::seconds{-42}});
}

TEST_CASE("time utils nowEpochSeconds uses system_clock seconds", "[common][time]") {
    const auto before = yams::common::timePointToEpochSeconds(std::chrono::system_clock::now());
    const auto now = yams::common::nowEpochSeconds();
    const auto after = yams::common::timePointToEpochSeconds(std::chrono::system_clock::now());

    CHECK(now >= before);
    CHECK(now <= after);
}
