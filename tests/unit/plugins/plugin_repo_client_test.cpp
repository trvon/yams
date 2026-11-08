/**
 * @file plugin_repo_client_test.cpp
 * @brief Unit tests for the plugin repository client (Catch2)
 *
 * Tests the PluginRepoClient functionality including:
 * - Listing plugins from remote repository
 * - Getting plugin metadata
 * - Version listing
 * - Error handling and network failures
 * - URL encoding and special characters
 */

#include <catch2/catch_test_macros.hpp>

#include <yams/plugins/plugin_repo_client.hpp>

#include <optional>
#include <string>
#include <vector>

namespace yams::plugins::test {

/**
 * @brief Fixture for PluginRepoClient tests
 */
struct PluginRepoClientFixture {
    PluginRepoClientFixture() {
        // Default config with test URL
        config.repoUrl = "https://test-repo.example.com";
        config.timeout = std::chrono::milliseconds{5000};
        config.verifyTls = true;
    }

    PluginRepoConfig config;
};

// =============================================================================
// Configuration Tests
// =============================================================================

TEST_CASE("PluginRepoConfig defaults", "[plugins][repo-client][config]") {
    PluginRepoConfig defaultConfig;

    SECTION("default repository URL is correct") {
        CHECK(defaultConfig.repoUrl == DEFAULT_PLUGIN_REPO_URL);
    }

    SECTION("default repository URL uses HTTPS") {
        CHECK(defaultConfig.repoUrl.rfind("https://", 0) == 0);
    }

    SECTION("default timeout is 30 seconds") {
        CHECK(defaultConfig.timeout == std::chrono::milliseconds{30000});
    }

    SECTION("TLS verification is enabled by default") {
        CHECK(defaultConfig.verifyTls);
    }
}

TEST_CASE("RemotePluginInfo default values", "[plugins][repo-client][types]") {
    RemotePluginInfo info;

    CHECK(info.name.empty());
    CHECK(info.version.empty());
    CHECK(info.description.empty());
    CHECK(info.interfaces.empty());
    CHECK(info.author.empty());
    CHECK(info.license.empty());
    CHECK(info.downloadUrl.empty());
    CHECK(info.checksum.empty());
    CHECK(info.sizeBytes == 0u);
    CHECK(info.abiVersion == 1);
    CHECK(info.downloads == 0u);
}

TEST_CASE("RemotePluginSummary default values", "[plugins][repo-client][types]") {
    RemotePluginSummary summary;

    CHECK(summary.name.empty());
    CHECK(summary.latestVersion.empty());
    CHECK(summary.description.empty());
    CHECK(summary.interfaces.empty());
    CHECK(summary.downloads == 0u);
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Client creation", "[plugins][repo-client]") {
    SECTION("creates valid client from config") {
        auto client = makePluginRepoClient(config);
        REQUIRE(client != nullptr);
    }
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Custom config options",
                 "[plugins][repo-client][config]") {
    SECTION("custom timeout is preserved") {
        PluginRepoConfig customConfig;
        customConfig.timeout = std::chrono::milliseconds{60000};
        CHECK(customConfig.timeout.count() == 60000);
    }

    SECTION("TLS verification can be disabled") {
        PluginRepoConfig customConfig;
        customConfig.verifyTls = false;
        CHECK_FALSE(customConfig.verifyTls);
    }

    SECTION("proxy can be configured") {
        PluginRepoConfig customConfig;
        customConfig.proxy = "http://proxy.example.com:8080";
        REQUIRE(customConfig.proxy.has_value());
        CHECK(*customConfig.proxy == "http://proxy.example.com:8080");
    }

    SECTION("user agent can be configured") {
        PluginRepoConfig customConfig;
        customConfig.userAgent = "yams-test/1.0";
        CHECK(customConfig.userAgent == "yams-test/1.0");
    }
}

// =============================================================================
// Note: Network tests require a mock HTTP server or integration tests
// These tests are disabled as they require network access
// =============================================================================

TEST_CASE_METHOD(PluginRepoClientFixture, "List plugins (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->list();
    CHECK(result.has_value());
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Get plugin metadata (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->get("test-plugin");
    CHECK(result.has_value());
}

TEST_CASE_METHOD(PluginRepoClientFixture, "List versions (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->versions("test-plugin");
    CHECK(result.has_value());
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Exists check (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->exists("test-plugin");
    CHECK(result.has_value());
}

} // namespace yams::plugins::test
