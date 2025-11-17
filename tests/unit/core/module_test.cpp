// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Test for C++20 module interface
// This test verifies that yams.core module works correctly

#include <catch2/catch_test_macros.hpp>

// Test both header-based and module-based usage
#ifdef YAMS_USE_MODULES
import yams.core;
#else
#include "yams/core/result.hpp"
#include "yams/core/types.hpp"
#endif

TEST_CASE("yams.core module Hash type", "[core][module]") {
    yams::core::Hash hash;

    SECTION("default constructor creates null hash") {
        REQUIRE(hash.is_null());
    }

    SECTION("hex conversion round-trip") {
        auto test_hash = yams::core::Hash::from_hex(
            "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3");
        REQUIRE(test_hash.has_value());

        auto hex = test_hash->to_hex();
        REQUIRE(hex == "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3");
    }

    SECTION("invalid hex string fails") {
        auto result = yams::core::Hash::from_hex("invalid");
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().find("Invalid hash length") != std::string::npos);
    }
}

TEST_CASE("yams.core module Result type", "[core][module]") {
    using yams::core::Result;

    SECTION("successful result") {
        Result<int> result = 42;
        REQUIRE(result.has_value());
        REQUIRE(*result == 42);
    }

    SECTION("error result") {
        Result<int> result = tl::unexpected<std::string>("test error");
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error() == "test error");
    }
}

TEST_CASE("yams.core module error codes", "[core][module]") {
    using yams::core::error_code_string;
    using yams::core::ErrorCode;

    REQUIRE(std::string(error_code_string(ErrorCode::Success)) == "Success");
    REQUIRE(std::string(error_code_string(ErrorCode::NotFound)) == "Not found");
    REQUIRE(std::string(error_code_string(ErrorCode::IoError)) == "I/O error");
}

TEST_CASE("yams.core module Metadata type", "[core][module]") {
    yams::core::Metadata meta;

    SECTION("default construction") {
        REQUIRE_FALSE(meta.name.has_value());
        REQUIRE(meta.tags.empty());
        REQUIRE(meta.size_bytes == 0);
    }

    SECTION("populate metadata") {
        meta.name = "test.txt";
        meta.mime_type = "text/plain";
        meta.tags = {"test", "sample"};
        meta.size_bytes = 1024;

        REQUIRE(meta.name.value() == "test.txt");
        REQUIRE(meta.mime_type.value() == "text/plain");
        REQUIRE(meta.tags.size() == 2);
        REQUIRE(meta.size_bytes == 1024);
    }
}

TEST_CASE("yams.core module StorageStats", "[core][module]") {
    yams::core::StorageStats stats;

    stats.total_documents = 100;
    stats.total_bytes_logical = 10000;
    stats.total_bytes_stored = 7000;

    REQUIRE(stats.bytes_saved() == 3000);

    // Calculate deduplication ratio
    stats.deduplication_ratio =
        static_cast<double>(stats.total_bytes_logical - stats.total_bytes_stored) /
        stats.total_bytes_logical;

    REQUIRE(stats.deduplication_ratio > 0.29);
    REQUIRE(stats.deduplication_ratio < 0.31);
}

#ifdef YAMS_USE_MODULES
TEST_CASE("module-specific features", "[core][module][modules-only]") {
    // This test only runs when compiled with module support

    SECTION("verify module types are accessible") {
        // These should be available via the module import
        std::string str;
        std::string_view sv = str;
        std::vector<int> vec;
        std::optional<int> opt;

        REQUIRE(sv.empty());
        REQUIRE(vec.empty());
        REQUIRE_FALSE(opt.has_value());
    }
}
#endif
