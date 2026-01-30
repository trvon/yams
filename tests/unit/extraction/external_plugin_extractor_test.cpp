// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 Trevon Sides

#include <filesystem>
#include <string>
#include <vector>
#include <catch2/catch_test_macros.hpp>

#include "yams/extraction/external_plugin_extractor.hpp"

using namespace yams::extraction;
namespace fs = std::filesystem;

namespace {
// Helper to get platform-appropriate python executable
std::string getPythonExecutable() {
#ifdef _WIN32
    return "python"; // Windows uses 'python' not 'python3'
#else
    return "/usr/bin/env"; // Unix uses env to find python3
#endif
}

std::vector<std::string> getPythonArgs() {
#ifdef _WIN32
    return {"-u"}; // Just unbuffered flag, python is the executable
#else
    return {"python3", "-u"}; // Use env to find python3
#endif
}
} // namespace

TEST_CASE("ExternalPluginExtractorConfig builder pattern", "[extraction][external_plugin]") {
    auto config = ExternalPluginExtractorConfig{}
                      .with_executable("/usr/bin/python3")
                      .with_arg("-u")
                      .with_arg("plugin.py")
                      .with_env("DEBUG", "1")
                      .in_directory("/tmp")
                      .with_timeout(std::chrono::milliseconds(2000))
                      .with_startup_timeout(std::chrono::milliseconds(3000));

    REQUIRE(config.executable() == "/usr/bin/python3");
    REQUIRE(config.args().size() == 2);
    REQUIRE(config.args()[0] == "-u");
    REQUIRE(config.args()[1] == "plugin.py");
    REQUIRE(config.env().at("DEBUG") == "1");
    REQUIRE(config.working_directory() == "/tmp");
    REQUIRE(config.timeout() == std::chrono::milliseconds(2000));
    REQUIRE(config.startup_timeout() == std::chrono::milliseconds(3000));
}

TEST_CASE("ExternalPluginExtractorConfig with_args batch", "[extraction][external_plugin]") {
    std::vector<std::string> args = {"-u", "--verbose", "plugin.py"};
    auto config = ExternalPluginExtractorConfig{}
                      .with_executable("/usr/bin/python3")
                      .with_args(std::move(args));

    REQUIRE(config.args().size() == 3);
    REQUIRE(config.args()[0] == "-u");
    REQUIRE(config.args()[1] == "--verbose");
    REQUIRE(config.args()[2] == "plugin.py");
}

TEST_CASE("ExternalPluginExtractor full lifecycle", "[extraction][external_plugin]") {
    // Get test fixture path - navigate from source file location
    // __FILE__ is tests/unit/extraction/external_plugin_extractor_test.cpp
    // Navigate up 3 levels to tests/, then into fixtures/
    auto plugin_path = fs::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures" /
                       "mock_plugin.py";

    INFO("Plugin path: " << plugin_path.string());
    REQUIRE(fs::exists(plugin_path));

    // Create extractor with platform-appropriate python
    auto config = ExternalPluginExtractorConfig{}
                      .with_executable(getPythonExecutable())
                      .with_timeout(std::chrono::milliseconds(3000));

    for (const auto& arg : getPythonArgs()) {
        config = std::move(config).with_arg(arg);
    }
    config = std::move(config).with_arg(plugin_path.string());

    auto extractor = ExternalPluginExtractor(std::move(config));

    SECTION("initialization") {
        auto result = extractor.init();
        REQUIRE(result.has_value());
        REQUIRE(extractor.is_ready());
        REQUIRE(extractor.name() == "mock_plugin");
        REQUIRE(extractor.version() == "1.0.0");

        auto manifest = extractor.manifest();
        REQUIRE(manifest.name == "mock_plugin");
        REQUIRE(manifest.version == "1.0.0");
        REQUIRE_FALSE(manifest.supported_mime_types.empty());
    }

    SECTION("health check") {
        REQUIRE(extractor.init().has_value());

        auto health_result = extractor.health_check();
        REQUIRE(health_result.has_value());
    }

    SECTION("supports() method") {
        REQUIRE(extractor.init().has_value());

        REQUIRE(extractor.supports("text/plain", "txt"));
        REQUIRE_FALSE(extractor.supports("application/json", "json"));
        REQUIRE_FALSE(extractor.supports("image/png", "png"));
    }

    SECTION("extractText() method") {
        REQUIRE(extractor.init().has_value());

        std::string test_content = "Hello, World!";
        std::vector<std::byte> content_bytes;
        content_bytes.reserve(test_content.size());
        for (char c : test_content) {
            content_bytes.push_back(static_cast<std::byte>(c));
        }

        auto result = extractor.extractText(content_bytes, "text/plain", "txt");
        REQUIRE(result.has_value());
        REQUIRE_FALSE(result.value().empty());
    }

    SECTION("graceful shutdown") {
        REQUIRE(extractor.init().has_value());
        REQUIRE(extractor.is_ready());

        auto shutdown_result = extractor.shutdown();
        REQUIRE(shutdown_result.has_value());
        REQUIRE_FALSE(extractor.is_ready());
    }
}

TEST_CASE("ExternalPluginExtractor multiple extractions", "[extraction][external_plugin]") {
    // Navigate from source file location to fixtures directory
    auto plugin_path = fs::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures" /
                       "mock_plugin.py";

    INFO("Plugin path: " << plugin_path.string());
    REQUIRE(fs::exists(plugin_path));

    auto config = ExternalPluginExtractorConfig{}
                      .with_executable(getPythonExecutable())
                      .with_timeout(std::chrono::milliseconds(3000));

    for (const auto& arg : getPythonArgs()) {
        config = std::move(config).with_arg(arg);
    }
    config = std::move(config).with_arg(plugin_path.string());

    auto extractor = ExternalPluginExtractor(std::move(config));
    REQUIRE(extractor.init().has_value());

    // Multiple extractions
    for (int i = 0; i < 5; ++i) {
        std::string test_content = "Test content " + std::to_string(i);
        std::vector<std::byte> content_bytes;
        content_bytes.reserve(test_content.size());
        for (char c : test_content) {
            content_bytes.push_back(static_cast<std::byte>(c));
        }

        auto result = extractor.extractText(content_bytes, "text/plain", "txt");
        REQUIRE(result.has_value());
    }
}

TEST_CASE("ExternalPluginExtractor invalid executable", "[extraction][external_plugin]") {
    auto config = ExternalPluginExtractorConfig{}.with_executable("/nonexistent/plugin");

    auto extractor = ExternalPluginExtractor(std::move(config));
    auto result = extractor.init();

    REQUIRE_FALSE(result.has_value());
    REQUIRE_FALSE(extractor.is_ready());
}

TEST_CASE("ExternalPluginExtractor RAII cleanup", "[extraction][external_plugin]") {
    // Navigate from source file location to fixtures directory
    auto plugin_path = fs::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures" /
                       "mock_plugin.py";

    INFO("Plugin path: " << plugin_path.string());
    REQUIRE(fs::exists(plugin_path));

    {
        auto config = ExternalPluginExtractorConfig{}.with_executable(getPythonExecutable());

        for (const auto& arg : getPythonArgs()) {
            config = std::move(config).with_arg(arg);
        }
        config = std::move(config).with_arg(plugin_path.string());

        auto extractor = ExternalPluginExtractor(std::move(config));
        REQUIRE(extractor.init().has_value());
        REQUIRE(extractor.is_ready());

        // Destructor should clean up
    }

    // If we get here without hanging, RAII worked
    REQUIRE(true);
}
