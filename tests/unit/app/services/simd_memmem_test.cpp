/// @file simd_memmem_test.cpp
/// @brief Catch2 unit tests for SIMD-accelerated memmem (case-sensitive and case-insensitive).

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <yams/app/services/simd_memmem.hpp>

using namespace yams::app::services;

// ---------------------------------------------------------------------------
// simdMemmem — case-sensitive
// ---------------------------------------------------------------------------

TEST_CASE("simdMemmem: empty needle returns 0 (memmem convention)", "[simd_memmem]") {
    const char* hay = "anything";
    REQUIRE(simdMemmem(hay, 8, "", 0) == 0);
}

TEST_CASE("simdMemmem: empty haystack returns npos", "[simd_memmem]") {
    REQUIRE(simdMemmem("", 0, "x", 1) == kMemmemNpos);
}

TEST_CASE("simdMemmem: needle longer than haystack returns npos", "[simd_memmem]") {
    REQUIRE(simdMemmem("abc", 3, "abcdef", 6) == kMemmemNpos);
}

TEST_CASE("simdMemmem: single-byte needle", "[simd_memmem]") {
    SECTION("found at start") {
        REQUIRE(simdMemmem("hello", 5, "h", 1) == 0);
    }
    SECTION("found in middle") {
        REQUIRE(simdMemmem("hello", 5, "l", 1) == 2);
    }
    SECTION("found at end") {
        REQUIRE(simdMemmem("hello", 5, "o", 1) == 4);
    }
    SECTION("not found") {
        REQUIRE(simdMemmem("hello", 5, "z", 1) == kMemmemNpos);
    }
}

TEST_CASE("simdMemmem: two-byte needle", "[simd_memmem]") {
    // Two-byte needle is the minimum length that uses the SIMD two-byte technique.
    SECTION("found") {
        REQUIRE(simdMemmem("abcdef", 6, "cd", 2) == 2);
    }
    SECTION("at start") {
        REQUIRE(simdMemmem("abcdef", 6, "ab", 2) == 0);
    }
    SECTION("at end") {
        REQUIRE(simdMemmem("abcdef", 6, "ef", 2) == 4);
    }
    SECTION("not found") {
        REQUIRE(simdMemmem("abcdef", 6, "xz", 2) == kMemmemNpos);
    }
}

TEST_CASE("simdMemmem: short needle (3 bytes)", "[simd_memmem]") {
    const std::string hay = "the quick brown fox";
    REQUIRE(simdMemmem(hay.data(), hay.size(), "qui", 3) == 4);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "fox", 3) == 16);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "cat", 3) == kMemmemNpos);
}

TEST_CASE("simdMemmem: medium needle (4-16 bytes)", "[simd_memmem]") {
    const std::string hay = "the quick brown fox jumps over the lazy dog";

    SECTION("4 bytes") {
        REQUIRE(simdMemmem(hay.data(), hay.size(), "quic", 4) == 4);
    }
    SECTION("8 bytes") {
        REQUIRE(simdMemmem(hay.data(), hay.size(), "brown fo", 8) == 10);
    }
    SECTION("16 bytes") {
        REQUIRE(simdMemmem(hay.data(), hay.size(), "fox jumps over t", 16) == 16);
    }
}

TEST_CASE("simdMemmem: long needle (>16 bytes)", "[simd_memmem]") {
    const std::string hay = "the quick brown fox jumps over the lazy dog";
    const std::string needle = "fox jumps over the lazy";
    REQUIRE(simdMemmem(hay.data(), hay.size(), needle.data(), needle.size()) == 16);
}

TEST_CASE("simdMemmem: exact match (haystack == needle)", "[simd_memmem]") {
    const std::string s = "exact";
    REQUIRE(simdMemmem(s.data(), s.size(), s.data(), s.size()) == 0);

    // Longer exact match that exercises SIMD path.
    const std::string longStr(64, 'a');
    REQUIRE(simdMemmem(longStr.data(), longStr.size(), longStr.data(), longStr.size()) == 0);
}

TEST_CASE("simdMemmem: needle not present", "[simd_memmem]") {
    const std::string hay = "aaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    REQUIRE(simdMemmem(hay.data(), hay.size(), "ab", 2) == kMemmemNpos);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "baa", 3) == kMemmemNpos);
}

TEST_CASE("simdMemmem: repeated patterns and overlapping candidates", "[simd_memmem]") {
    SECTION("finds first occurrence among repeats") {
        const std::string hay = "abcabcabcabc";
        REQUIRE(simdMemmem(hay.data(), hay.size(), "abc", 3) == 0);
    }
    SECTION("overlapping near-match") {
        // "aaab" — the two-byte filter will see 'aa' many times before 'ab'.
        const std::string hay = "aaaaab";
        REQUIRE(simdMemmem(hay.data(), hay.size(), "aab", 3) == 3);
    }
    SECTION("needle appears only once amid similar prefixes") {
        const std::string hay = "abxabyabzabc";
        REQUIRE(simdMemmem(hay.data(), hay.size(), "abc", 3) == 9);
    }
}

TEST_CASE("simdMemmem: haystack with null bytes (binary data)", "[simd_memmem]") {
    const char hay[] = {'a', '\0', 'b', '\0', 'c', '\0', 'd'};
    const char needle[] = {'b', '\0', 'c'};
    REQUIRE(simdMemmem(hay, sizeof(hay), needle, sizeof(needle)) == 2);
}

TEST_CASE("simdMemmem: haystack exactly 16 bytes (register boundary)", "[simd_memmem]") {
    const std::string hay = "0123456789abcdef"; // 16 bytes
    REQUIRE(hay.size() == 16);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "cdef", 4) == 12);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "0123", 4) == 0);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "xyz", 3) == kMemmemNpos);
}

TEST_CASE("simdMemmem: haystack exactly 32 bytes (register boundary)", "[simd_memmem]") {
    const std::string hay = "0123456789abcdef0123456789ABCDEF"; // 32 bytes
    REQUIRE(hay.size() == 32);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "ABCDEF", 6) == 26);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "ef01", 4) == 14);
}

TEST_CASE("simdMemmem: large haystack with match near end", "[simd_memmem]") {
    // 64 KB of 'x' followed by a unique needle — exercises the full SIMD loop + scalar tail.
    std::string hay(65536, 'x');
    hay += "NEEDLE_HERE";
    const std::string needle = "NEEDLE_HERE";
    REQUIRE(simdMemmem(hay.data(), hay.size(), needle.data(), needle.size()) == 65536);
}

TEST_CASE("simdMemmem: large haystack with no match", "[simd_memmem]") {
    std::string hay(65536, 'a');
    REQUIRE(simdMemmem(hay.data(), hay.size(), "b", 1) == kMemmemNpos);
    REQUIRE(simdMemmem(hay.data(), hay.size(), "aab", 3) == kMemmemNpos);
}

TEST_CASE("simdMemmem: match at every alignment mod 16", "[simd_memmem]") {
    // Place needle at offsets 0..15 to test all SIMD register alignments.
    const std::string needle = "XY";
    for (size_t offset = 0; offset < 16; ++offset) {
        std::string hay(32, '.');
        hay[offset] = 'X';
        hay[offset + 1] = 'Y';
        INFO("offset = " << offset);
        REQUIRE(simdMemmem(hay.data(), hay.size(), needle.data(), needle.size()) == offset);
    }
}

// ---------------------------------------------------------------------------
// simdMemmemCI — case-insensitive (needle must be lowercase)
// ---------------------------------------------------------------------------

TEST_CASE("simdMemmemCI: empty needle returns 0 (memmem convention)", "[simd_memmem][ci]") {
    REQUIRE(simdMemmemCI("anything", 8, "", 0) == 0);
}

TEST_CASE("simdMemmemCI: empty haystack returns npos", "[simd_memmem][ci]") {
    REQUIRE(simdMemmemCI("", 0, "x", 1) == kMemmemNpos);
}

TEST_CASE("simdMemmemCI: basic case folding", "[simd_memmem][ci]") {
    SECTION("uppercase haystack, lowercase needle") {
        REQUIRE(simdMemmemCI("HELLO WORLD", 11, "hello", 5) == 0);
    }
    SECTION("mixed case haystack") {
        REQUIRE(simdMemmemCI("HeLLo WoRLd", 11, "hello", 5) == 0);
    }
    SECTION("exact lowercase match") {
        REQUIRE(simdMemmemCI("hello world", 11, "hello", 5) == 0);
    }
}

TEST_CASE("simdMemmemCI: non-alpha characters are unchanged", "[simd_memmem][ci]") {
    // Digits, symbols, punctuation should match literally.
    REQUIRE(simdMemmemCI("abc123!@#", 9, "123!@#", 6) == 3);
    // Same chars but needle is already lowercase, haystack uppercase won't affect digits.
    REQUIRE(simdMemmemCI("ABC123!@#", 9, "abc123!@#", 9) == 0);
}

TEST_CASE("simdMemmemCI: single-byte needle", "[simd_memmem][ci]") {
    REQUIRE(simdMemmemCI("Hello", 5, "h", 1) == 0);
    REQUIRE(simdMemmemCI("Hello", 5, "o", 1) == 4);
    REQUIRE(simdMemmemCI("HELLO", 5, "z", 1) == kMemmemNpos);
}

TEST_CASE("simdMemmemCI: two-byte needle with case folding", "[simd_memmem][ci]") {
    REQUIRE(simdMemmemCI("AbCdEf", 6, "cd", 2) == 2);
    REQUIRE(simdMemmemCI("ABCDEF", 6, "ef", 2) == 4);
}

TEST_CASE("simdMemmemCI: medium needle with mixed case", "[simd_memmem][ci]") {
    const std::string hay = "The Quick Brown Fox Jumps Over The Lazy Dog";
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), "quick brown", 11) == 4);
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), "lazy dog", 8) == 35);
}

TEST_CASE("simdMemmemCI: long needle (>16 bytes)", "[simd_memmem][ci]") {
    const std::string hay = "The Quick Brown Fox Jumps Over The Lazy Dog";
    const std::string needle = "fox jumps over the lazy";
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), needle.data(), needle.size()) == 16);
}

TEST_CASE("simdMemmemCI: needle not found", "[simd_memmem][ci]") {
    const std::string hay = "AAAAAAAAAAAAA";
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), "ab", 2) == kMemmemNpos);
}

TEST_CASE("simdMemmemCI: large haystack", "[simd_memmem][ci]") {
    std::string hay(65536, 'A');
    hay += "NeEdLe_HeRe";
    // Needle must be lowercase.
    const std::string needle = "needle_here";
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), needle.data(), needle.size()) == 65536);
}

TEST_CASE("simdMemmemCI: match at every alignment mod 16", "[simd_memmem][ci]") {
    const std::string needle = "xy";
    for (size_t offset = 0; offset < 16; ++offset) {
        std::string hay(32, '.');
        // Put uppercase version in haystack.
        hay[offset] = 'X';
        hay[offset + 1] = 'Y';
        INFO("offset = " << offset);
        REQUIRE(simdMemmemCI(hay.data(), hay.size(), needle.data(), needle.size()) == offset);
    }
}

TEST_CASE("simdMemmemCI: haystack exactly 16 bytes", "[simd_memmem][ci]") {
    const std::string hay = "ABCDEFGHIJKLMNOP"; // 16 bytes
    REQUIRE(hay.size() == 16);
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), "mnop", 4) == 12);
}

TEST_CASE("simdMemmemCI: haystack exactly 32 bytes", "[simd_memmem][ci]") {
    const std::string hay = "ABCDEFGHIJKLMNOPabcdefghijklmnop"; // 32 bytes
    REQUIRE(hay.size() == 32);
    REQUIRE(simdMemmemCI(hay.data(), hay.size(), "klmnopab", 8) == 10);
}

// ---------------------------------------------------------------------------
// string_view overloads
// ---------------------------------------------------------------------------

TEST_CASE("simdMemmem: string_view overload matches raw-pointer version", "[simd_memmem][sv]") {
    std::string_view hay = "the quick brown fox";
    std::string_view needle = "brown";
    size_t raw = simdMemmem(hay.data(), hay.size(), needle.data(), needle.size());
    size_t sv = simdMemmem(hay, needle);
    REQUIRE(raw == sv);
    REQUIRE(sv == 10);
}

TEST_CASE("simdMemmemCI: string_view overload matches raw-pointer version", "[simd_memmem][sv]") {
    std::string_view hay = "The Quick Brown Fox";
    std::string_view needle = "brown";
    size_t raw = simdMemmemCI(hay.data(), hay.size(), needle.data(), needle.size());
    size_t sv = simdMemmemCI(hay, needle);
    REQUIRE(raw == sv);
    REQUIRE(sv == 10);
}

// ---------------------------------------------------------------------------
// kMemmemNpos sentinel value
// ---------------------------------------------------------------------------

TEST_CASE("kMemmemNpos equals size_t(-1)", "[simd_memmem]") {
    REQUIRE(kMemmemNpos == static_cast<size_t>(-1));
    // It should also equal std::string::npos on all platforms we care about.
    REQUIRE(kMemmemNpos == std::string::npos);
}
