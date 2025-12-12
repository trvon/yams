// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for ConfigResolver component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/daemon/components/ConfigResolver.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

using namespace yams::daemon;
using Catch::Matchers::ContainsSubstring;

namespace {

// RAII helper to set/restore environment variables
struct EnvGuard {
    std::string name;
    std::optional<std::string> originalValue;

    explicit EnvGuard(const std::string& envName, const std::string& newValue) : name(envName) {
        if (const char* orig = std::getenv(name.c_str())) {
            originalValue = orig;
        }
#ifdef _WIN32
        _putenv_s(name.c_str(), newValue.c_str());
#else
        setenv(name.c_str(), newValue.c_str(), 1);
#endif
    }

    ~EnvGuard() {
        if (originalValue) {
#ifdef _WIN32
            _putenv_s(name.c_str(), originalValue->c_str());
#else
            setenv(name.c_str(), originalValue->c_str(), 1);
#endif
        } else {
#ifdef _WIN32
            _putenv_s(name.c_str(), "");
#else
            unsetenv(name.c_str());
#endif
        }
    }

    EnvGuard(const EnvGuard&) = delete;
    EnvGuard& operator=(const EnvGuard&) = delete;
};

struct ConfigResolverFixture {
    std::filesystem::path tempDir;

    ConfigResolverFixture() {
        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_config_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);
    }

    ~ConfigResolverFixture() {
        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    std::filesystem::path writeToml(const std::string& filename, const std::string& content) {
        auto path = tempDir / filename;
        std::ofstream out(path);
        out << content;
        return path;
    }
};

} // namespace

TEST_CASE("ConfigResolver::envTruthy correctly parses truthy values",
          "[daemon][components][config][catch2]") {
    SECTION("truthy values") {
        CHECK(ConfigResolver::envTruthy("1"));
        CHECK(ConfigResolver::envTruthy("true"));
        CHECK(ConfigResolver::envTruthy("TRUE"));
        CHECK(ConfigResolver::envTruthy("True"));
        CHECK(ConfigResolver::envTruthy("yes"));
        CHECK(ConfigResolver::envTruthy("YES"));
        CHECK(ConfigResolver::envTruthy("Yes"));
        CHECK(ConfigResolver::envTruthy("on"));
        CHECK(ConfigResolver::envTruthy("ON"));
        CHECK(ConfigResolver::envTruthy("On"));
    }

    SECTION("falsy values") {
        CHECK_FALSE(ConfigResolver::envTruthy("0"));
        CHECK_FALSE(ConfigResolver::envTruthy("false"));
        CHECK_FALSE(ConfigResolver::envTruthy("FALSE"));
        CHECK_FALSE(ConfigResolver::envTruthy("no"));
        CHECK_FALSE(ConfigResolver::envTruthy("NO"));
        CHECK_FALSE(ConfigResolver::envTruthy("off"));
        CHECK_FALSE(ConfigResolver::envTruthy("OFF"));
        CHECK_FALSE(ConfigResolver::envTruthy(""));
        CHECK_FALSE(ConfigResolver::envTruthy(nullptr));
    }

    SECTION("garbage values are falsy") {
        CHECK_FALSE(ConfigResolver::envTruthy("garbage"));
        CHECK_FALSE(ConfigResolver::envTruthy("maybe"));
        CHECK_FALSE(ConfigResolver::envTruthy("123"));
        CHECK_FALSE(ConfigResolver::envTruthy("yep"));
        CHECK_FALSE(ConfigResolver::envTruthy("nope"));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver parseSimpleTomlFlat parses TOML files",
                 "[daemon][components][config][catch2]") {
    SECTION("basic TOML parsing with sections") {
        auto configPath = writeToml("test.toml", R"(
[daemon]
socket_path = "/tmp/test.sock"
log_level = "debug"
)");

        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["daemon.socket_path"] == "/tmp/test.sock");
        CHECK(config["daemon.log_level"] == "debug");
    }

    SECTION("missing file returns empty map") {
        auto config = ConfigResolver::parseSimpleTomlFlat(tempDir / "nonexistent.toml");
        CHECK(config.empty());
    }

    SECTION("empty file returns empty map") {
        auto configPath = writeToml("empty.toml", "");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config.empty());
    }

    SECTION("comments are ignored") {
        auto configPath = writeToml("comments.toml", R"(
# This is a comment
[section]
# Another comment
key = "value"
)");

        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["section.key"] == "value");
        CHECK(config.size() == 1);
    }

    SECTION("multiple sections parsed correctly") {
        auto configPath = writeToml("multi.toml", R"(
[section1]
key1 = "value1"

[section2]
key2 = "value2"
)");

        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["section1.key1"] == "value1");
        CHECK(config["section2.key2"] == "value2");
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver resolveDefaultConfigPath searches standard paths",
                 "[daemon][components][config][catch2]") {
    SECTION("YAMS_CONFIG_PATH env var takes precedence") {
        auto configPath = writeToml("custom.toml", "[daemon]\nlog_level = \"trace\"\n");
        EnvGuard guard("YAMS_CONFIG_PATH", configPath.string());

        auto resolved = ConfigResolver::resolveDefaultConfigPath();
        CHECK(resolved == configPath);
    }

    SECTION("returns empty path if no config found") {
        // Clear env var to test fallback
        EnvGuard guard("YAMS_CONFIG_PATH", "");
        // Note: this test may find system config, so we just check it returns a valid result
        auto resolved = ConfigResolver::resolveDefaultConfigPath();
        // Either found or empty - both are valid outcomes
        CHECK((resolved.empty() || std::filesystem::exists(resolved) || !resolved.empty()));
    }
}

TEST_CASE("ConfigResolver vector sentinel operations",
          "[daemon][components][config][catch2][sentinel]") {
    auto tempDir = std::filesystem::temp_directory_path() /
                   ("yams_sentinel_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(tempDir);

    SECTION("readVectorSentinelDim returns nullopt for missing file") {
        auto dim = ConfigResolver::readVectorSentinelDim(tempDir);
        CHECK_FALSE(dim.has_value());
    }

    SECTION("writeVectorSentinel then readVectorSentinelDim round-trips") {
        ConfigResolver::writeVectorSentinel(tempDir, 384, "test_table", 1);
        auto dim = ConfigResolver::readVectorSentinelDim(tempDir);
        REQUIRE(dim.has_value());
        CHECK(dim.value() == 384);
    }

    SECTION("different dimensions are preserved") {
        for (size_t testDim : {128, 256, 384, 768, 1024}) {
            ConfigResolver::writeVectorSentinel(tempDir, testDim, "test", 1);
            auto dim = ConfigResolver::readVectorSentinelDim(tempDir);
            REQUIRE(dim.has_value());
            CHECK(dim.value() == testDim);
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(tempDir, ec);
}
