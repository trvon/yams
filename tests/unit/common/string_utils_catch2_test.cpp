// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/common/string_utils.h>

namespace {

TEST_CASE("trimCopy removes leading and trailing whitespace", "[common][string][trim]") {
    CHECK(yams::common::trimCopy("  hello  ") == "hello");
    CHECK(yams::common::trimCopy("\thello\n") == "hello");
    CHECK(yams::common::trimCopy("hello") == "hello");
    CHECK(yams::common::trimCopy("  ") == "");
    CHECK(yams::common::trimCopy("") == "");
    CHECK(yams::common::trimCopy(" \t a b \t ") == "a b");
}

TEST_CASE("hasWildcard detects glob metacharacters", "[common][string][wildcard]") {
    CHECK_FALSE(yams::common::hasWildcard("plain"));
    CHECK_FALSE(yams::common::hasWildcard(""));
    CHECK(yams::common::hasWildcard("*.cpp"));
    CHECK(yams::common::hasWildcard("foo?"));
    CHECK(yams::common::hasWildcard("*"));
    CHECK(yams::common::hasWildcard("a*b?c"));
}

TEST_CASE("escapeRegex escapes regex metacharacters", "[common][string][regex]") {
    CHECK(yams::common::escapeRegex("plain") == "plain");
    CHECK(yams::common::escapeRegex("a.b") == "a\\.b");
    CHECK(yams::common::escapeRegex("") == "");
    CHECK(yams::common::escapeRegex("\\^$.|?*+()[]{}") ==
          "\\\\\\^\\$\\.\\|\\?\\*\\+\\(\\)\\[\\]\\{\\}");
    CHECK(yams::common::escapeRegex("a+b*c?d") == "a\\+b\\*c\\?d");
    CHECK(yams::common::escapeRegex("no-meta-here_1") == "no-meta-here_1");
}

TEST_CASE("isRegexMetaChar classifies characters", "[common][string][regex]") {
    CHECK(yams::common::isRegexMetaChar('.'));
    CHECK(yams::common::isRegexMetaChar('*'));
    CHECK(yams::common::isRegexMetaChar('['));
    CHECK(yams::common::isRegexMetaChar('\\'));
    CHECK_FALSE(yams::common::isRegexMetaChar('a'));
    CHECK_FALSE(yams::common::isRegexMetaChar('1'));
    CHECK_FALSE(yams::common::isRegexMetaChar('_'));
}

} // namespace
