#include <catch2/catch_test_macros.hpp>
#include <yams/core/uuid.h>

#include <set>
#include <string>

using namespace yams::core;

TEST_CASE("generateUUID - format and uniqueness", "[core][uuid]") {
    SECTION("UUID has correct format") {
        auto uuid = generateUUID();
        REQUIRE(uuid.size() == 36);
        // Check dash positions: 8-4-4-4-12
        CHECK(uuid[8] == '-');
        CHECK(uuid[13] == '-');
        CHECK(uuid[18] == '-');
        CHECK(uuid[23] == '-');
        // Version 4 bit: character at position 14 should be '4'
        CHECK(uuid[14] == '4');
        // Variant bits: character at position 19 should be 8, 9, a, or b
        char v = uuid[19];
        CHECK((v == '8' || v == '9' || v == 'a' || v == 'b'));
    }

    SECTION("Two UUIDs are different") {
        auto a = generateUUID();
        auto b = generateUUID();
        CHECK(a != b);
    }
}

TEST_CASE("shortHash - deterministic and format", "[core][uuid]") {
    SECTION("Same input gives same output") {
        auto a = shortHash("hello");
        auto b = shortHash("hello");
        CHECK(a == b);
    }

    SECTION("Output is hex characters, max 8 chars") {
        auto h = shortHash("test-input");
        CHECK(h.size() <= 8);
        for (char c : h) {
            CHECK(((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')));
        }
    }

    SECTION("Different inputs give different hashes") {
        CHECK(shortHash("aaa") != shortHash("bbb"));
    }
}

TEST_CASE("generateSnapshotId - format", "[core][uuid]") {
    auto id = generateSnapshotId();
    CHECK_FALSE(id.empty());
    CHECK(id.find('T') != std::string::npos);
    CHECK(id.find('Z') != std::string::npos);
}

TEST_CASE("generateId - prefixed format", "[core][uuid]") {
    SECTION("Default prefix") {
        auto id = generateId();
        CHECK(id.substr(0, 2) == "s-");
    }

    SECTION("Custom prefix") {
        auto id = generateId("inst");
        CHECK(id.substr(0, 5) == "inst-");
    }
}
