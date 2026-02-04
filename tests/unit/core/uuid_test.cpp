#include <catch2/catch_test_macros.hpp>
#include <yams/core/uuid.h>

#include <cstddef>
#include <set>
#include <string>

using namespace yams::core;

namespace {

bool is_ascii_digit(char c) {
    return c >= '0' && c <= '9';
}

bool is_ascii_lower_hex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

} // namespace

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

        for (std::size_t i = 0; i < uuid.size(); ++i) {
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                CHECK(uuid[i] == '-');
            } else {
                CHECK(is_ascii_lower_hex(uuid[i]));
            }
        }
    }

    SECTION("Two UUIDs are different") {
        auto a = generateUUID();
        auto b = generateUUID();
        CHECK(a != b);
    }

    SECTION("Many UUIDs are unique") {
        std::set<std::string> seen;
        for (int i = 0; i < 64; ++i) {
            seen.insert(generateUUID());
        }
        CHECK(seen.size() == 64);
    }
}

TEST_CASE("shortHash - deterministic and format", "[core][uuid]") {
    SECTION("Same input gives same output") {
        auto a = shortHash("hello");
        auto b = shortHash("hello");
        CHECK(a == b);
    }

    SECTION("Known vectors") {
        CHECK(shortHash("") == "739d0383");
        CHECK(shortHash("hello") == "131ec7a1");
        CHECK(shortHash("test-input") == "418229b2");
        CHECK(shortHash("aaa") == "dc287ec");
        CHECK(shortHash("bbb") == "75f7013");
    }

    SECTION("Output is hex characters, max 8 chars") {
        auto h = shortHash("test-input");
        CHECK(h.size() <= 8);
        CHECK_FALSE(h.empty());
        for (char c : h) {
            CHECK(is_ascii_lower_hex(c));
        }
    }

    SECTION("Different inputs give different hashes") {
        CHECK(shortHash("aaa") != shortHash("bbb"));
    }
}

TEST_CASE("generateSnapshotId - format", "[core][uuid]") {
    auto id = generateSnapshotId();
    CHECK_FALSE(id.empty());
    REQUIRE(id.size() == 27);
    CHECK(id[4] == '-');
    CHECK(id[7] == '-');
    CHECK(id[10] == 'T');
    CHECK(id[13] == ':');
    CHECK(id[16] == ':');
    CHECK(id[19] == '.');
    CHECK(id[26] == 'Z');

    for (std::size_t i = 0; i < id.size(); ++i) {
        if (i == 4 || i == 7) {
            continue;
        }
        if (i == 10) {
            continue;
        }
        if (i == 13 || i == 16) {
            continue;
        }
        if (i == 19) {
            continue;
        }
        if (i == 26) {
            continue;
        }
        CHECK(is_ascii_digit(id[i]));
    }
}

TEST_CASE("generateId - prefixed format", "[core][uuid]") {
    SECTION("Default prefix") {
        auto id = generateId();
        CHECK(id.substr(0, 2) == "s-");
    }

    SECTION("Custom prefix") {
        auto id = generateId("inst");
        REQUIRE(id.rfind("inst-", 0) == 0);

        auto first_dash = id.find('-');
        REQUIRE(first_dash != std::string::npos);
        auto second_dash = id.find('-', first_dash + 1);
        REQUIRE(second_dash != std::string::npos);

        auto prefix = id.substr(0, first_dash);
        CHECK(prefix == "inst");

        auto timestamp = id.substr(first_dash + 1, second_dash - (first_dash + 1));
        CHECK_FALSE(timestamp.empty());
        for (char c : timestamp) {
            CHECK(is_ascii_digit(c));
        }

        auto rand_hex = id.substr(second_dash + 1);
        CHECK(rand_hex.size() == 6);
        for (char c : rand_hex) {
            CHECK(is_ascii_lower_hex(c));
        }
    }
}
