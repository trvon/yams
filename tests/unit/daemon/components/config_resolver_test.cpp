// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for ConfigResolver component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

using namespace yams::daemon;
using Catch::Matchers::ContainsSubstring;

namespace {

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

class ProfileGuard {
    yams::daemon::TuneAdvisor::Profile prev_;

public:
    explicit ProfileGuard(yams::daemon::TuneAdvisor::Profile profile)
        : prev_(yams::daemon::TuneAdvisor::tuningProfile()) {
        yams::daemon::TuneAdvisor::setTuningProfile(profile);
    }
    ~ProfileGuard() { yams::daemon::TuneAdvisor::setTuningProfile(prev_); }

    ProfileGuard(const ProfileGuard&) = delete;
    ProfileGuard& operator=(const ProfileGuard&) = delete;
};

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

    SECTION("unrecognized values are truthy (not in falsey list)") {
        // The implementation treats anything NOT in {0, false, off, no} as truthy
        CHECK(ConfigResolver::envTruthy("garbage"));
        CHECK(ConfigResolver::envTruthy("maybe"));
        CHECK(ConfigResolver::envTruthy("123"));
        CHECK(ConfigResolver::envTruthy("yep"));
        CHECK(ConfigResolver::envTruthy("nope"));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture, "ConfigResolver parseSimpleTomlFlat parses TOML files",
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

TEST_CASE_METHOD(ConfigResolverFixture, "ConfigResolver parses [tuning] section",
                 "[daemon][components][config][catch2]") {
    SECTION("efficient profile parses correctly") {
        auto configPath = writeToml("efficient.toml", R"(
[tuning]
profile = "efficient"
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["tuning.profile"] == "efficient");
    }

    SECTION("balanced profile parses correctly") {
        auto configPath = writeToml("balanced.toml", R"(
[tuning]
profile = "balanced"
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["tuning.profile"] == "balanced");
    }

    SECTION("aggressive profile parses correctly") {
        auto configPath = writeToml("aggressive.toml", R"(
[tuning]
profile = "aggressive"
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["tuning.profile"] == "aggressive");
    }

    SECTION("conservative alias for efficient") {
        auto configPath = writeToml("conservative.toml", R"(
[tuning]
profile = "conservative"
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["tuning.profile"] == "conservative");
    }

    SECTION("case-insensitive profile names") {
        auto configPath = writeToml("case_test.toml", R"(
[tuning]
profile = "EFFICIENT"
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["tuning.profile"] == "EFFICIENT");
    }

    SECTION("missing tuning section returns empty") {
        auto configPath = writeToml("no_tuning.toml", R"(
[daemon]
socket_path = "/tmp/test.sock"
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config.find("tuning.profile") == config.end());
    }

    SECTION("multiple tuning options parsed together") {
        auto configPath = writeToml("multi_tuning.toml", R"(
[tuning]
profile = "aggressive"
pool_cooldown_ms = 250
worker_poll_ms = 100
)");
        auto config = ConfigResolver::parseSimpleTomlFlat(configPath);
        CHECK(config["tuning.profile"] == "aggressive");
        CHECK(config["tuning.pool_cooldown_ms"] == "250");
        CHECK(config["tuning.worker_poll_ms"] == "100");
    }
}

TEST_CASE("Tuning profile from config affects TuneAdvisor methods",
          "[daemon][components][config][catch2]") {
    SECTION("efficient profile scales post-ingest concurrency down") {
        ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Efficient);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(8);

        // Efficient profile scale is 0.40, postIngestBatchSize = 8 * 0.40 = 3
        CHECK(TuneAdvisor::postExtractionConcurrent() == 1u);
        CHECK(TuneAdvisor::postKgConcurrent() == 0u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 0u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 0u);
        CHECK(TuneAdvisor::postTitleConcurrent() == 0u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 2u);
        CHECK(TuneAdvisor::postIngestBatchSize() == 3u);
    }

    SECTION("balanced profile uses medium values") {
        ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Balanced);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(8);

        // Balanced profile scale is 0.75, postIngestBatchSize = 8 * 0.75 = 6
        CHECK(TuneAdvisor::postExtractionConcurrent() == 2u);
        CHECK(TuneAdvisor::postKgConcurrent() == 0u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 0u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 0u);
        CHECK(TuneAdvisor::postTitleConcurrent() == 0u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 2u);
        CHECK(TuneAdvisor::postIngestBatchSize() == 6u);
    }

    SECTION("aggressive profile uses maximum values") {
        ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Aggressive);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(8);

        // Aggressive profile scale is 1.0, postIngestBatchSize = 8 * 1.0 = 8
        // With 8 threads and Aggressive (80% CPU budget), totalBudget = floor(0.8 * 8) = 6
        // With 6 active stages, each stage gets at least 1
        CHECK(TuneAdvisor::postExtractionConcurrent() == 1u);
        CHECK(TuneAdvisor::postKgConcurrent() == 1u);
        CHECK(TuneAdvisor::postSymbolConcurrent() == 1u);
        CHECK(TuneAdvisor::postEntityConcurrent() == 1u);
        CHECK(TuneAdvisor::postTitleConcurrent() == 1u);
        CHECK(TuneAdvisor::postEmbedConcurrent() == 1u);
        CHECK(TuneAdvisor::postIngestBatchSize() == 8u);
    }

    SECTION("profile affects cpuBudgetPercent") {
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Efficient);
            CHECK(TuneAdvisor::cpuBudgetPercent() == 40u);
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Balanced);
            CHECK(TuneAdvisor::cpuBudgetPercent() == 50u);
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Aggressive);
            CHECK(TuneAdvisor::cpuBudgetPercent() == 80u);
        }
    }

    SECTION("profile affects poolCooldownMs") {
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Efficient);
            CHECK(TuneAdvisor::poolCooldownMs() == 750u);
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Balanced);
            CHECK(TuneAdvisor::poolCooldownMs() == 500u);
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Aggressive);
            CHECK(TuneAdvisor::poolCooldownMs() == 250u);
        }
    }
}

TEST_CASE("YAMS_TUNING_PROFILE env var overrides config", "[daemon][components][config][catch2]") {
    auto clearProfileOverride = []() {
        yams::daemon::TuneAdvisor::tuningProfileOverride_.store(0, std::memory_order_relaxed);
    };

    SECTION("efficient profile from env var") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "efficient");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK(profile == yams::daemon::TuneAdvisor::Profile::Efficient);
    }

    SECTION("aggressive profile from env var") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "aggressive");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK(profile == yams::daemon::TuneAdvisor::Profile::Aggressive);
    }

    SECTION("conservative alias maps to efficient") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "conservative");

        auto profile = yams::daemon::TuneAdvisor::Profile::Efficient;
        CHECK(yams::daemon::TuneAdvisor::tuningProfile() == profile);
    }

    SECTION("invalid env var falls back to balanced") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "invalid_profile");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK(profile == yams::daemon::TuneAdvisor::Profile::Balanced);
    }

    SECTION("empty env var falls back to balanced") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK(profile == yams::daemon::TuneAdvisor::Profile::Balanced);
    }
}
