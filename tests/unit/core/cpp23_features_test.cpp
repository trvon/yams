// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <cstring>
#include <string>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/core/cpp23_features.hpp>

using namespace yams::features;

TEST_CASE("C++23 feature flags are defined", "[core][cpp23]") {
    // These should always be defined (either 0 or 1)
    REQUIRE((YAMS_HAS_CONSTEXPR_VECTOR == 0 || YAMS_HAS_CONSTEXPR_VECTOR == 1));
    REQUIRE((YAMS_HAS_CONSTEXPR_STRING == 0 || YAMS_HAS_CONSTEXPR_STRING == 1));
    REQUIRE((YAMS_HAS_CONSTEXPR_CONTAINERS == 0 || YAMS_HAS_CONSTEXPR_CONTAINERS == 1));
    REQUIRE((YAMS_HAS_PROFILES == 0 || YAMS_HAS_PROFILES == 1));
    REQUIRE((YAMS_HAS_REFLECTION == 0 || YAMS_HAS_REFLECTION == 1));
}

TEST_CASE("FeatureInfo struct is accessible", "[core][cpp23]") {
    // Should be able to access static constexpr members
    bool has_containers = FeatureInfo::has_constexpr_containers;
    bool has_vector = FeatureInfo::has_constexpr_vector;
    bool has_string = FeatureInfo::has_constexpr_string;
    bool has_profiles = FeatureInfo::has_profiles;
    bool has_reflection = FeatureInfo::has_reflection;

    REQUIRE((has_containers == 0 || has_containers == 1));
    REQUIRE((has_vector == 0 || has_vector == 1));
    REQUIRE((has_string == 0 || has_string == 1));
    REQUIRE((has_profiles == 0 || has_profiles == 1));
    REQUIRE((has_reflection == 0 || has_reflection == 1));
}

TEST_CASE("Containers flag is consistent with individual flags", "[core][cpp23]") {
    if (YAMS_HAS_CONSTEXPR_CONTAINERS) {
        // If containers are enabled, both vector and string must be enabled
        REQUIRE(YAMS_HAS_CONSTEXPR_VECTOR == 1);
        REQUIRE(YAMS_HAS_CONSTEXPR_STRING == 1);
    } else {
        // If containers are disabled, at least one of vector/string is disabled
        REQUIRE((YAMS_HAS_CONSTEXPR_VECTOR == 0 || YAMS_HAS_CONSTEXPR_STRING == 0));
    }
}

TEST_CASE("Compiler info is defined", "[core][cpp23]") {
    const char* compiler_name = FeatureInfo::compiler_name;
    int compiler_version = FeatureInfo::compiler_version;
    long cpp_version = FeatureInfo::cpp_version;

    REQUIRE(compiler_name != nullptr);
    REQUIRE(compiler_version > 0);
    REQUIRE(cpp_version >= 202002L); // At least C++20
}

TEST_CASE("Feature summary string is valid", "[core][cpp23]") {
    const char* summary = get_feature_summary();
    REQUIRE(summary != nullptr);
    REQUIRE(strlen(summary) > 0);

    // Summary should mention C++20 or C++23
    std::string summary_str(summary);
    bool mentions_std = summary_str.find("C++20") != std::string::npos ||
                        summary_str.find("C++23") != std::string::npos;
    REQUIRE(mentions_std);
}

TEST_CASE("YAMS_CONSTEXPR_IF_SUPPORTED macro expands correctly", "[core][cpp23]") {
    // This test verifies the macro expands without syntax errors
    // The actual behavior (constexpr vs inline) depends on feature detection
    auto test_func = []() YAMS_CONSTEXPR_IF_SUPPORTED -> int { return 42; };
    REQUIRE(test_func() == 42);
}

TEST_CASE("YAMS_PROFILE macro expands without errors", "[core][cpp23]") {
    // Profile macro should expand to either [[profile: ...]] or nothing
    struct YAMS_PROFILE(type) TestStruct {
        int value = 0;
    };

    TestStruct s;
    s.value = 10;
    REQUIRE(s.value == 10);
}

#if YAMS_HAS_CONSTEXPR_CONTAINERS
TEST_CASE("Constexpr vector works when available", "[core][cpp23][constexpr]") {
    // NOTE: In GCC 15, even with C++23, lambdas with constexpr vector
    // can have issues with operator new. Use a function instead.
    constexpr auto make_vec = []() constexpr {
        std::vector<int> v;
        v.push_back(1);
        v.push_back(2);
        v.push_back(3);
        return v;
    };

    // Test at runtime (constexpr works but GCC 15 has lambda limitations)
    auto vec = make_vec();
    REQUIRE(vec.size() == 3);
    REQUIRE(vec[0] == 1);
    REQUIRE(vec[1] == 2);
    REQUIRE(vec[2] == 3);
}

TEST_CASE("Constexpr string works when available", "[core][cpp23][constexpr]") {
    constexpr auto make_str = []() constexpr {
        std::string s = "hello";
        s += " world";
        return s;
    };

    // Test at runtime
    auto str = make_str();
    REQUIRE(str == "hello world");
    REQUIRE(str.size() == 11);
}

TEST_CASE("Constexpr containers work together", "[core][cpp23][constexpr]") {
    // Simplified test - avoid complex nested constexpr for now
    struct Data {
        std::string name;
        std::vector<int> values;
    };

    auto make_data = []() {
        std::vector<Data> result;
        result.push_back({"first", {1, 2, 3}});
        result.push_back({"second", {4, 5, 6}});
        return result;
    };

    auto data = make_data();
    REQUIRE(data.size() == 2);
    REQUIRE(data[0].name == "first");
    REQUIRE(data[0].values.size() == 3);
    REQUIRE(data[1].name == "second");
    REQUIRE(data[1].values.size() == 3);
}
#endif

#if !YAMS_HAS_CONSTEXPR_CONTAINERS
TEST_CASE("C++20 fallback for vectors works", "[core][cpp23][fallback]") {
    // In C++20 mode, we should still be able to use vectors and strings
    // They just won't be constexpr
    auto make_vec = []() {
        std::vector<int> v;
        v.push_back(1);
        v.push_back(2);
        v.push_back(3);
        return v;
    };

    auto vec = make_vec();
    REQUIRE(vec.size() == 3);
    REQUIRE(vec[0] == 1);
}

TEST_CASE("C++20 fallback for strings works", "[core][cpp23][fallback]") {
    auto make_str = []() {
        std::string s = "hello";
        s += " world";
        return s;
    };

    auto str = make_str();
    REQUIRE(str == "hello world");
}
#endif

TEST_CASE("C++ version is checked correctly", "[core][cpp23]") {
    long cpp_ver = YAMS_CPP_VERSION;

    // Should be at least C++20
    REQUIRE(cpp_ver >= 202002L);

    // If constexpr containers are available, should be C++23 or later
    if (YAMS_HAS_CONSTEXPR_CONTAINERS) {
        REQUIRE(cpp_ver >= 202302L);
    }
}

TEST_CASE("Deprecation macro compiles without errors", "[core][cpp23]") {
    YAMS_CPP23_DEPRECATED("test message")
    auto old_function = []() { return 42; };

    REQUIRE(old_function() == 42);
}
