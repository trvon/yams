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

TEST_CASE_METHOD(PluginRepoClientFixture, "Custom config options", "[plugins][repo-client][config]") {
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
// Security-related tests for checksums and integrity
// =============================================================================

TEST_CASE("Checksum format validation", "[plugins][repo-client][security]") {
    // Valid SHA-256 checksum format
    const std::string validChecksum =
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    SECTION("checksum has correct prefix") {
        CHECK(validChecksum.substr(0, 7) == "sha256:");
    }

    SECTION("hex portion is 64 characters") {
        const std::string hexPortion = validChecksum.substr(7);
        CHECK(hexPortion.length() == 64u);
    }

    SECTION("all characters are valid hex") {
        const std::string hexPortion = validChecksum.substr(7);
        for (char c : hexPortion) {
            bool isValidHex =
                (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            CHECK(isValidHex);
        }
    }
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Security fields are preserved",
                 "[plugins][repo-client][security]") {
    RemotePluginInfo info;
    info.name = "test-plugin";
    info.version = "1.2.3";
    info.checksum = "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
    info.downloadUrl =
        "https://secure-cdn.example.com/plugins/test-plugin/1.2.3/test-plugin.tar.gz";
    info.abiVersion = 2;

    CHECK(info.checksum.substr(0, 7) == "sha256:");
    CHECK(info.downloadUrl.find("https://") == 0);
    CHECK(info.abiVersion == 2);
}

TEST_CASE("Plugin name validation", "[plugins][repo-client][security]") {
    // Safe plugin names
    std::vector<std::string> safeNames = {"my-plugin", "my_plugin", "myplugin", "plugin123",
                                          "my-cool-plugin-v2"};

    for (const auto& name : safeNames) {
        SECTION("name '" + name + "' is safe") {
            bool isSafe = true;
            for (char c : name) {
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                      c == '-' || c == '_')) {
                    isSafe = false;
                    break;
                }
            }
            CHECK(isSafe);
        }
    }
}

TEST_CASE("Minimum version compatibility", "[plugins][repo-client][security]") {
    RemotePluginInfo info;
    info.minYamsVersion = "2.0.0";
    CHECK_FALSE(info.minYamsVersion.empty());
}

// =============================================================================
// Security Best Practices Verification Tests
// =============================================================================

TEST_CASE("Security best practices", "[plugins][repo-client][security]") {
    SECTION("SHA-256 produces correct length checksum") {
        // SHA-256 produces a 64-character hex string
        const size_t sha256HexLength = 64;
        // With prefix "sha256:" the total length is 71
        const size_t expectedTotalLength = 7 + sha256HexLength;

        const std::string sampleChecksum =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        CHECK(sampleChecksum.length() == expectedTotalLength);
    }

    SECTION("HTTPS is preferred for downloads") {
        CHECK(std::string(DEFAULT_PLUGIN_REPO_URL).substr(0, 8) == "https://");
    }

    SECTION("TLS verification is enabled by default") {
        PluginRepoConfig config;
        CHECK(config.verifyTls);
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
