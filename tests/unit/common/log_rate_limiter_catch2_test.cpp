#include <catch2/catch_test_macros.hpp>

#include <yams/common/log_rate_limiter.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

using yams::common::LogRateLimiter;
using namespace std::chrono_literals;

TEST_CASE("LogRateLimiter lets one message through per interval",
          "[common][logging][rate-limit][catch2]") {
    LogRateLimiter limiter(30s);
    const auto t0 = LogRateLimiter::Clock::time_point{} + 1h;

    auto first = limiter.admit(t0);
    REQUIRE(first.has_value());
    CHECK(*first == 0);

    CHECK_FALSE(limiter.admit(t0 + 1s).has_value());
    CHECK_FALSE(limiter.admit(t0 + 10s).has_value());
    CHECK_FALSE(limiter.admit(t0 + 29s).has_value());

    // The next admitted message reports how many were held back.
    auto second = limiter.admit(t0 + 30s);
    REQUIRE(second.has_value());
    CHECK(*second == 3);

    CHECK_FALSE(limiter.admit(t0 + 31s).has_value());
    auto third = limiter.admit(t0 + 5min);
    REQUIRE(third.has_value());
    CHECK(*third == 1);
}

TEST_CASE("LogRateLimiter suffix names suppressed messages",
          "[common][logging][rate-limit][catch2]") {
    CHECK(LogRateLimiter::suppressedSuffix(0).empty());
    CHECK(LogRateLimiter::suppressedSuffix(1) == " (1 similar message suppressed)");
    CHECK(LogRateLimiter::suppressedSuffix(11000) == " (11000 similar messages suppressed)");
}

TEST_CASE("LogRateLimiter admits exactly one caller per interval under contention",
          "[common][logging][rate-limit][catch2]") {
    LogRateLimiter limiter(1h);
    const auto now = LogRateLimiter::Clock::now();
    std::atomic<int> admitted{0};
    std::atomic<std::uint64_t> reported{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 1000; ++j) {
                if (auto held = limiter.admit(now)) {
                    admitted.fetch_add(1);
                    reported.fetch_add(*held);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    CHECK(admitted.load() == 1);
    // Every held-back occurrence is reported exactly once across the admitted messages.
    auto next = limiter.admit(now + 1h);
    REQUIRE(next.has_value());
    CHECK(reported.load() + *next == 7999);
}
