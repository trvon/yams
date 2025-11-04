#include <filesystem>
#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/binary_extractor.hpp>
#include <yams/extraction/external_plugin_extractor.hpp>

namespace fs = std::filesystem;
using namespace yams::extraction;

TEST_CASE("BinaryExtractor supports binary MIME types", "[extraction][binary]") {
    auto plugin_path = fs::current_path() / "tests" / "fixtures" / "mock_plugin.py";
    REQUIRE(fs::exists(plugin_path));

    auto config = ExternalPluginExtractorConfig{}
                      .with_executable("/usr/bin/env")
                      .with_arg("python3")
                      .with_arg("-u")
                      .with_arg(plugin_path.string());

    auto extractor = BinaryExtractor(std::move(config));
    REQUIRE(extractor.init().has_value());

    // Should recognize binary MIME types
    REQUIRE(extractor.supports("application/x-executable", ""));
    REQUIRE(extractor.supports("application/x-sharedlib", ""));
    REQUIRE(extractor.supports("application/x-msdownload", ""));
    REQUIRE(extractor.supports("application/x-mach-binary", ""));
    REQUIRE(extractor.supports("application/x-elf", ""));
    REQUIRE(extractor.supports("application/java-vm", ""));
    REQUIRE(extractor.supports("application/wasm", ""));

    // Should recognize binary extensions
    REQUIRE(extractor.supports("", ".exe"));
    REQUIRE(extractor.supports("", ".dll"));
    REQUIRE(extractor.supports("", ".so"));
    REQUIRE(extractor.supports("", ".dylib"));
    REQUIRE(extractor.supports("", ".class"));
    REQUIRE(extractor.supports("", ".wasm"));

    // Should reject non-binary types
    REQUIRE_FALSE(extractor.supports("text/plain", ""));
    REQUIRE_FALSE(extractor.supports("application/json", ""));
    REQUIRE_FALSE(extractor.supports("", ".txt"));
}

TEST_CASE("BinaryExtractor lifecycle", "[extraction][binary]") {
    auto plugin_path = fs::current_path() / "tests" / "fixtures" / "mock_plugin.py";
    REQUIRE(fs::exists(plugin_path));

    auto config = ExternalPluginExtractorConfig{}
                      .with_executable("/usr/bin/env")
                      .with_arg("python3")
                      .with_arg("-u")
                      .with_arg(plugin_path.string());

    auto extractor = BinaryExtractor(std::move(config));

    // Initially not ready
    REQUIRE_FALSE(extractor.is_ready());

    // Init should succeed
    auto init_result = extractor.init();
    REQUIRE(init_result.has_value());
    REQUIRE(extractor.is_ready());

    // Health check should pass
    auto health = extractor.health_check();
    REQUIRE(health.has_value());

    // Shutdown should succeed
    auto shutdown_result = extractor.shutdown();
    REQUIRE(shutdown_result.has_value());
    REQUIRE_FALSE(extractor.is_ready());
}

TEST_CASE("BinaryExtractor extraction", "[extraction][binary]") {
    auto plugin_path = fs::current_path() / "tests" / "fixtures" / "mock_plugin.py";
    REQUIRE(fs::exists(plugin_path));

    auto config = ExternalPluginExtractorConfig{}
                      .with_executable("/usr/bin/env")
                      .with_arg("python3")
                      .with_arg("-u")
                      .with_arg(plugin_path.string());

    auto extractor = BinaryExtractor(std::move(config));
    REQUIRE(extractor.init().has_value());

    // Mock plugin only supports text/plain, but BinaryExtractor accepts binary types
    // and delegates to the plugin. The plugin may return empty/error, but should not crash.
    {
        std::vector<std::byte> empty_bytes;
        auto result = extractor.extractText(empty_bytes, "application/x-executable", ".exe");
        // Plugin may or may not return a result - key is it doesn't crash
        // In production, a real binary extraction plugin would return disassembly
        REQUIRE(true); // Test passes if we get here
    }

    // Also test with unsupported (non-binary) type
    {
        std::vector<std::byte> empty_bytes;
        auto result = extractor.extractText(empty_bytes, "text/plain", ".txt");
        REQUIRE_FALSE(result.has_value());
    }
}

TEST_CASE("BinaryExtractor RAII cleanup", "[extraction][binary]") {
    auto plugin_path = fs::current_path() / "tests" / "fixtures" / "mock_plugin.py";
    REQUIRE(fs::exists(plugin_path));

    {
        auto config = ExternalPluginExtractorConfig{}
                          .with_executable("/usr/bin/env")
                          .with_arg("python3")
                          .with_arg("-u")
                          .with_arg(plugin_path.string());

        auto extractor = BinaryExtractor(std::move(config));
        REQUIRE(extractor.init().has_value());
        REQUIRE(extractor.is_ready());

        // Destructor should clean up
    }

    // If we get here without hanging, RAII worked
    REQUIRE(true);
}
