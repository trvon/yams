// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/enhanced_search_executor.h>

#include "../../../common/test_helpers_catch2.h"

#include <filesystem>
#include <fstream>
#include <string>

TEST_CASE("enhanced search reads the canonical shared config path",
          "[services][config][enhanced_search]") {
    yams::test::TempDirGuard directory{"yams_enhanced_search_config_"};
    const auto configPath = directory.path() / "config.toml";
    std::ofstream(configPath) << R"toml(
[experimental.enhanced_search]
enable = false
hotzone_weight = 0.9

[search.enhanced]
enable = true # canonical section wins
classification_weight = 0.25
hotzone_weight = 0.15
enhanced_search_timeout_ms = 1750

[search.hotzones]
decay_interval_hours = 8.5
max_boost_factor = 3.25
enable_persistence = true
data_file = "cache#1.json" # quoted hash survives
)toml";

    yams::test::ScopedEnvVar compatibilityConfig{"YAMS_CONFIG_PATH", std::nullopt};
    yams::test::ScopedEnvVar canonicalConfig{"YAMS_CONFIG", configPath.string()};

    const auto config = yams::app::services::EnhancedSearchExecutor::loadConfigFromToml();
    CHECK(config.enable);
    CHECK((config.classification_weight == Catch::Approx(0.25)));
    CHECK((config.hotzone_weight == Catch::Approx(0.15)));
    CHECK((config.enhanced_search_timeout_ms == 1750));
    CHECK((config.hotzones.half_life_hours == Catch::Approx(8.5)));
    CHECK((config.hotzones.max_boost_factor == Catch::Approx(3.25)));
    CHECK(config.hotzones.enable_persistence);
    CHECK((config.hotzones.data_file == "cache#1.json"));
}

TEST_CASE("enhanced search ignores invalid typed values", "[services][config][enhanced_search]") {
    yams::test::TempDirGuard directory{"yams_enhanced_search_invalid_"};
    const auto configPath = directory.path() / "config.toml";
    std::ofstream(configPath) << R"toml(
[search.enhanced]
enable = true
classification_weight = "not-a-number"
enhanced_search_timeout_ms = -4
)toml";

    yams::test::ScopedEnvVar compatibilityConfig{"YAMS_CONFIG_PATH", std::nullopt};
    yams::test::ScopedEnvVar canonicalConfig{"YAMS_CONFIG", configPath.string()};

    const auto config = yams::app::services::EnhancedSearchExecutor::loadConfigFromToml();
    CHECK_FALSE(config.enable);
    CHECK((config.classification_weight == Catch::Approx(0.15)));
    CHECK((config.enhanced_search_timeout_ms == 2000));
}
