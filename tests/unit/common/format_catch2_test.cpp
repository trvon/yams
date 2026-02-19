// Tests for yams::fmt_format compatibility wrapper.

#include <catch2/catch_test_macros.hpp>

#include <yams/common/format.h>

TEST_CASE("yams::fmt_format formats simple strings", "[common][format][catch2]") {
    CHECK(yams::fmt_format("hello") == "hello");
    CHECK(yams::fmt_format("x{}", 1) == "x1");
    CHECK(yams::fmt_format("{} {}", "a", 2) == "a 2");
}
