#include <catch2/catch_test_macros.hpp>
#include <yams/cli/time_parser.h>

#include <chrono>
#include <cmath>
#include <ctime>
#include <string>

using namespace yams::cli;
using namespace std::chrono;

namespace {

constexpr auto kTolerance = seconds(5);

static system_clock::duration absDuration(system_clock::duration d) {
    return (d < system_clock::duration::zero()) ? -d : d;
}

static std::tm toLocalTm(const system_clock::time_point& tp) {
    const std::time_t t = system_clock::to_time_t(tp);
    return *std::localtime(&t);
}

static bool contains(const std::string& s, const std::string& needle) {
    return s.find(needle) != std::string::npos;
}

} // namespace

TEST_CASE("TimeParser::parse() main entry point", "[cli][time_parser][catch2]") {
    SECTION("Empty string returns error") {
        const auto res = TimeParser::parse("");
        CHECK_FALSE(res.has_value());
        CHECK(res.error().code == yams::ErrorCode::InvalidArgument);
        CHECK(contains(res.error().message, "Empty time string"));
    }

    SECTION("Valid relative '7d' succeeds") {
        const auto before = system_clock::now();
        const auto res = TimeParser::parse("7d");
        const auto after = system_clock::now();

        REQUIRE(res.has_value());

        const auto tp = res.value();
        const auto minExpected = before - hours(24 * 7) - kTolerance;
        const auto maxExpected = after - hours(24 * 7) + kTolerance;
        CHECK(tp >= minExpected);
        CHECK(tp <= maxExpected);
    }

    SECTION("Invalid format returns error with message") {
        const auto res = TimeParser::parse("not-a-time-format");
        CHECK_FALSE(res.has_value());
        CHECK(res.error().code == yams::ErrorCode::InvalidArgument);
        CHECK(contains(res.error().message, "Invalid time format"));
    }
}

TEST_CASE("TimeParser::parseRelative()", "[cli][time_parser][catch2]") {
    SECTION("Hours: '24h' -> ~24 hours ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseRelative("24h");
        const auto after = system_clock::now();
        REQUIRE(tp);
        CHECK(*tp >= before - hours(24) - kTolerance);
        CHECK(*tp <= after - hours(24) + kTolerance);
    }

    SECTION("Days: '7d' -> ~7 days ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseRelative("7d");
        const auto after = system_clock::now();
        REQUIRE(tp);
        CHECK(*tp >= before - hours(24 * 7) - kTolerance);
        CHECK(*tp <= after - hours(24 * 7) + kTolerance);
    }

    SECTION("Weeks: '2w' -> ~14 days ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseRelative("2w");
        const auto after = system_clock::now();
        REQUIRE(tp);
        CHECK(*tp >= before - hours(24 * 14) - kTolerance);
        CHECK(*tp <= after - hours(24 * 14) + kTolerance);
    }

    SECTION("Months: '3m' -> ~90 days ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseRelative("3m");
        const auto after = system_clock::now();
        REQUIRE(tp);
        CHECK(*tp >= before - hours(24 * 90) - kTolerance);
        CHECK(*tp <= after - hours(24 * 90) + kTolerance);
    }

    SECTION("Years: '1y' -> ~365 days ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseRelative("1y");
        const auto after = system_clock::now();
        REQUIRE(tp);
        CHECK(*tp >= before - hours(24 * 365) - kTolerance);
        CHECK(*tp <= after - hours(24 * 365) + kTolerance);
    }

    SECTION("Case insensitive: '7D' works same as '7d'") {
        const auto before = system_clock::now();
        const auto tpUpper = TimeParser::parseRelative("7D");
        const auto tpLower = TimeParser::parseRelative("7d");
        const auto after = system_clock::now();

        REQUIRE(tpUpper);
        REQUIRE(tpLower);

        const auto expectedMin = before - hours(24 * 7) - kTolerance;
        const auto expectedMax = after - hours(24 * 7) + kTolerance;
        CHECK(*tpUpper >= expectedMin);
        CHECK(*tpUpper <= expectedMax);
        CHECK(*tpLower >= expectedMin);
        CHECK(*tpLower <= expectedMax);

        CHECK(absDuration(*tpUpper - *tpLower) < kTolerance);
    }

    SECTION("Invalid unit: '5x' -> nullopt") {
        CHECK_FALSE(TimeParser::parseRelative("5x").has_value());
    }

    SECTION("Empty string -> nullopt") {
        CHECK_FALSE(TimeParser::parseRelative("").has_value());
    }

    SECTION("Missing number: 'd' -> nullopt") {
        CHECK_FALSE(TimeParser::parseRelative("d").has_value());
    }
}

TEST_CASE("TimeParser::parseISO8601()", "[cli][time_parser][catch2]") {
    SECTION("Date-only '2024-01-01' succeeds and is start of day") {
        const auto tp = TimeParser::parseISO8601("2024-01-01");
        REQUIRE(tp);

        const auto tm = toLocalTm(*tp);
        CHECK((tm.tm_year + 1900) == 2024);
        CHECK((tm.tm_mon + 1) == 1);
        CHECK(tm.tm_mday == 1);
        CHECK(tm.tm_hour == 0);
        CHECK(tm.tm_min == 0);
        CHECK(tm.tm_sec == 0);
    }

    SECTION("Invalid string returns nullopt") {
        CHECK_FALSE(TimeParser::parseISO8601("not-a-date").has_value());
    }

    SECTION("Full ISO with T format works") {
        const auto tp = TimeParser::parseISO8601("2024-01-02T03:04:05Z");
        REQUIRE(tp);

        const auto tm = toLocalTm(*tp);
        CHECK((tm.tm_year + 1900) == 2024);
        CHECK((tm.tm_mon + 1) == 1);
        CHECK(tm.tm_mday == 2);
        CHECK(tm.tm_min == 4);
        CHECK(tm.tm_sec == 5);
    }
}

TEST_CASE("TimeParser::parseUnixTimestamp()", "[cli][time_parser][catch2]") {
    SECTION("10-digit seconds timestamp parsed as seconds") {
        const auto tp = TimeParser::parseUnixTimestamp("1704067200");
        REQUIRE(tp);

        const auto secs = duration_cast<seconds>(tp->time_since_epoch()).count();
        CHECK(secs == 1704067200);
    }

    SECTION("13-digit milliseconds timestamp parsed as milliseconds (same instant)") {
        const auto tpSec = TimeParser::parseUnixTimestamp("1704067200");
        const auto tpMs = TimeParser::parseUnixTimestamp("1704067200000");
        REQUIRE(tpSec);
        REQUIRE(tpMs);

        CHECK(duration_cast<seconds>(tpSec->time_since_epoch()).count() ==
              duration_cast<seconds>(tpMs->time_since_epoch()).count());
        CHECK(duration_cast<milliseconds>(tpMs->time_since_epoch()).count() == 1704067200000LL);
    }

    SECTION("Non-numeric 'abc' returns nullopt") {
        CHECK_FALSE(TimeParser::parseUnixTimestamp("abc").has_value());
    }

    SECTION("Mixed '123abc' returns nullopt") {
        CHECK_FALSE(TimeParser::parseUnixTimestamp("123abc").has_value());
    }
}

TEST_CASE("TimeParser::parseNatural()", "[cli][time_parser][catch2]") {
    SECTION("'now' is close to system_clock::now()") {
        const auto tp = TimeParser::parseNatural("now");
        REQUIRE(tp);
        CHECK(absDuration(system_clock::now() - *tp) < kTolerance);
    }

    SECTION("'today' is start of current day") {
        const auto now = system_clock::now();
        const auto expected = TimeParser::startOfDay(now);
        const auto tp = TimeParser::parseNatural("today");
        REQUIRE(tp);
        CHECK(absDuration(*tp - expected) < kTolerance);

        const auto tm = toLocalTm(*tp);
        CHECK(tm.tm_hour == 0);
        CHECK(tm.tm_min == 0);
        CHECK(tm.tm_sec == 0);
    }

    SECTION("'yesterday' is start of day minus 24h") {
        const auto now = system_clock::now();
        const auto expected = TimeParser::startOfDay(now) - hours(24);
        const auto tp = TimeParser::parseNatural("yesterday");
        REQUIRE(tp);
        CHECK(absDuration(*tp - expected) < kTolerance);

        const auto tm = toLocalTm(*tp);
        CHECK(tm.tm_hour == 0);
        CHECK(tm.tm_min == 0);
        CHECK(tm.tm_sec == 0);
    }

    SECTION("'tomorrow' is start of day plus 24h") {
        const auto now = system_clock::now();
        const auto expected = TimeParser::startOfDay(now) + hours(24);
        const auto tp = TimeParser::parseNatural("tomorrow");
        REQUIRE(tp);
        CHECK(absDuration(*tp - expected) < kTolerance);

        const auto tm = toLocalTm(*tp);
        CHECK(tm.tm_hour == 0);
        CHECK(tm.tm_min == 0);
        CHECK(tm.tm_sec == 0);
    }

    SECTION("'last-week' is ~7 days ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseNatural("last-week");
        const auto after = system_clock::now();
        REQUIRE(tp);

        CHECK(*tp >= before - hours(24 * 7) - kTolerance);
        CHECK(*tp <= after - hours(24 * 7) + kTolerance);
    }

    SECTION("'last_month' and 'lastmonth' variants work") {
        const auto before = system_clock::now();
        const auto tpUnderscore = TimeParser::parseNatural("last_month");
        const auto tpNoSep = TimeParser::parseNatural("lastmonth");
        const auto after = system_clock::now();

        REQUIRE(tpUnderscore);
        REQUIRE(tpNoSep);

        const auto expectedMin = before - hours(24 * 30) - kTolerance;
        const auto expectedMax = after - hours(24 * 30) + kTolerance;
        CHECK(*tpUnderscore >= expectedMin);
        CHECK(*tpUnderscore <= expectedMax);
        CHECK(*tpNoSep >= expectedMin);
        CHECK(*tpNoSep <= expectedMax);
    }

    SECTION("'last-year' is ~365 days ago") {
        const auto before = system_clock::now();
        const auto tp = TimeParser::parseNatural("last-year");
        const auto after = system_clock::now();
        REQUIRE(tp);

        CHECK(*tp >= before - hours(24 * 365) - kTolerance);
        CHECK(*tp <= after - hours(24 * 365) + kTolerance);
    }

    SECTION("Case insensitive: 'YESTERDAY' works") {
        const auto now = system_clock::now();
        const auto expected = TimeParser::startOfDay(now) - hours(24);
        const auto tp = TimeParser::parseNatural("YESTERDAY");
        REQUIRE(tp);
        CHECK(absDuration(*tp - expected) < kTolerance);
    }

    SECTION("Unrecognized 'nextmonth' returns nullopt") {
        CHECK_FALSE(TimeParser::parseNatural("nextmonth").has_value());
    }
}

TEST_CASE("TimeParser::formatISO8601()", "[cli][time_parser][catch2]") {
    SECTION("Known epoch (time_t=0) formats as UTC with Z") {
        const auto tp = system_clock::from_time_t(0);
        CHECK(TimeParser::formatISO8601(tp) == "1970-01-01T00:00:00Z");
    }

    SECTION("Specific time formats correctly with Z suffix") {
        const auto tp = system_clock::from_time_t(1704067200); // 2024-01-01T00:00:00Z
        const auto s = TimeParser::formatISO8601(tp);
        CHECK(s == "2024-01-01T00:00:00Z");
        CHECK_FALSE(s.empty());
        CHECK(s.back() == 'Z');
    }
}

TEST_CASE("TimeParser::formatRelative()", "[cli][time_parser][catch2]") {
    SECTION("Just now (< 1 second ago) -> 'just now'") {
        const auto tp = system_clock::now() - milliseconds(100);
        CHECK(TimeParser::formatRelative(tp) == "just now");
    }

    SECTION("~30 seconds ago -> contains 'second' and 'ago'") {
        const auto tp = system_clock::now() - seconds(30);
        const auto s = TimeParser::formatRelative(tp);
        CHECK(contains(s, "second"));
        CHECK(contains(s, "ago"));
    }

    SECTION("~2 hours ago -> contains 'hour' and 'ago'") {
        const auto tp = system_clock::now() - hours(2);
        const auto s = TimeParser::formatRelative(tp);
        CHECK(contains(s, "hour"));
        CHECK(contains(s, "ago"));
    }

    SECTION("Yesterday (~36 hours ago) -> 'yesterday'") {
        const auto tp = system_clock::now() - hours(36);
        CHECK(TimeParser::formatRelative(tp) == "yesterday");
    }

    SECTION("Future time -> contains 'in'") {
        const auto tp = system_clock::now() + hours(2);
        const auto s = TimeParser::formatRelative(tp);
        CHECK(contains(s, "in"));
    }
}

TEST_CASE("TimeParser::startOfDay()/endOfDay()", "[cli][time_parser][catch2]") {
    SECTION("startOfDay returns time at 00:00:00") {
        const auto now = system_clock::now();
        const auto tp = TimeParser::startOfDay(now);
        const auto tm = toLocalTm(tp);
        CHECK(tm.tm_hour == 0);
        CHECK(tm.tm_min == 0);
        CHECK(tm.tm_sec == 0);
    }

    SECTION("endOfDay returns time at 23:59:59") {
        const auto now = system_clock::now();
        const auto tp = TimeParser::endOfDay(now);
        const auto tm = toLocalTm(tp);
        CHECK(tm.tm_hour == 23);
        CHECK(tm.tm_min == 59);
        CHECK(tm.tm_sec == 59);
    }
}
