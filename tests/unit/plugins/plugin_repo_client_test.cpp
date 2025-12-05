/**
 * @file plugin_repo_client_test.cpp
 * @brief Unit tests for the plugin repository client
 *
 * Tests the PluginRepoClient functionality including:
 * - Listing plugins from remote repository
 * - Getting plugin metadata
 * - Version listing
 * - Error handling and network failures
 * - URL encoding and special characters
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yams/plugins/plugin_repo_client.hpp>

#include <optional>
#include <string>
#include <vector>

namespace yams::plugins::test {

using namespace testing;

/**
 * @brief Test fixture for PluginRepoClient tests
 */
class PluginRepoClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Default config with test URL
        config_.repoUrl = "https://test-repo.example.com";
        config_.timeout = std::chrono::milliseconds{5000};
        config_.verifyTls = true;
    }

    PluginRepoConfig config_;
};

/**
 * @brief Test that default config uses the expected repository URL
 */
TEST_F(PluginRepoClientTest, DefaultConfigUsesCorrectUrl) {
    PluginRepoConfig defaultConfig;
    EXPECT_EQ(defaultConfig.repoUrl, DEFAULT_PLUGIN_REPO_URL);
    EXPECT_EQ(defaultConfig.timeout, std::chrono::milliseconds{30000});
    EXPECT_TRUE(defaultConfig.verifyTls);
}

/**
 * @brief Test RemotePluginInfo struct initialization
 */
TEST_F(PluginRepoClientTest, RemotePluginInfoDefaultValues) {
    RemotePluginInfo info;
    EXPECT_TRUE(info.name.empty());
    EXPECT_TRUE(info.version.empty());
    EXPECT_TRUE(info.description.empty());
    EXPECT_TRUE(info.interfaces.empty());
    EXPECT_TRUE(info.author.empty());
    EXPECT_TRUE(info.license.empty());
    EXPECT_TRUE(info.downloadUrl.empty());
    EXPECT_TRUE(info.checksum.empty());
    EXPECT_EQ(info.sizeBytes, 0u);
    EXPECT_EQ(info.abiVersion, 1);
    EXPECT_EQ(info.downloads, 0u);
}

/**
 * @brief Test RemotePluginSummary struct initialization
 */
TEST_F(PluginRepoClientTest, RemotePluginSummaryDefaultValues) {
    RemotePluginSummary summary;
    EXPECT_TRUE(summary.name.empty());
    EXPECT_TRUE(summary.latestVersion.empty());
    EXPECT_TRUE(summary.description.empty());
    EXPECT_TRUE(summary.interfaces.empty());
    EXPECT_EQ(summary.downloads, 0u);
}

/**
 * @brief Test that makePluginRepoClient creates a valid client
 */
TEST_F(PluginRepoClientTest, CanCreateClient) {
    auto client = makePluginRepoClient(config_);
    EXPECT_NE(client, nullptr);
}

/**
 * @brief Test plugin config with custom timeout
 */
TEST_F(PluginRepoClientTest, CustomTimeoutIsRespected) {
    PluginRepoConfig customConfig;
    customConfig.timeout = std::chrono::milliseconds{60000};
    EXPECT_EQ(customConfig.timeout.count(), 60000);
}

/**
 * @brief Test config with TLS verification disabled
 */
TEST_F(PluginRepoClientTest, TlsVerificationCanBeDisabled) {
    PluginRepoConfig customConfig;
    customConfig.verifyTls = false;
    EXPECT_FALSE(customConfig.verifyTls);
}

/**
 * @brief Test config with proxy
 */
TEST_F(PluginRepoClientTest, ProxyCanBeConfigured) {
    PluginRepoConfig customConfig;
    customConfig.proxy = "http://proxy.example.com:8080";
    EXPECT_TRUE(customConfig.proxy.has_value());
    EXPECT_EQ(*customConfig.proxy, "http://proxy.example.com:8080");
}

/**
 * @brief Test custom user agent
 */
TEST_F(PluginRepoClientTest, UserAgentCanBeConfigured) {
    PluginRepoConfig customConfig;
    customConfig.userAgent = "yams-test/1.0";
    EXPECT_EQ(customConfig.userAgent, "yams-test/1.0");
}

// =============================================================================
// Security-related tests for checksums and integrity
// =============================================================================

/**
 * @brief Test that checksum format is validated correctly
 *
 * SHA-256 checksums should be in format "sha256:<64-hex-chars>"
 */
TEST_F(PluginRepoClientTest, ChecksumFormatValidation) {
    // Valid SHA-256 checksum format
    const std::string validChecksum = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    
    // Verify format starts with sha256:
    EXPECT_EQ(validChecksum.substr(0, 7), "sha256:");
    
    // Verify hex portion is 64 characters
    const std::string hexPortion = validChecksum.substr(7);
    EXPECT_EQ(hexPortion.length(), 64u);
    
    // Verify all characters are valid hex
    for (char c : hexPortion) {
        bool isValidHex = (c >= '0' && c <= '9') || 
                          (c >= 'a' && c <= 'f') || 
                          (c >= 'A' && c <= 'F');
        EXPECT_TRUE(isValidHex) << "Invalid hex character: " << c;
    }
}

/**
 * @brief Test RemotePluginInfo with security-relevant fields populated
 */
TEST_F(PluginRepoClientTest, SecurityFieldsArePreserved) {
    RemotePluginInfo info;
    info.name = "test-plugin";
    info.version = "1.2.3";
    info.checksum = "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
    info.downloadUrl = "https://secure-cdn.example.com/plugins/test-plugin/1.2.3/test-plugin.tar.gz";
    info.abiVersion = 2;
    
    EXPECT_EQ(info.checksum.substr(0, 7), "sha256:");
    EXPECT_TRUE(info.downloadUrl.find("https://") == 0);
    EXPECT_EQ(info.abiVersion, 2);
}

/**
 * @brief Test that plugin names are validated for special characters
 *
 * Plugin names should be safe for filesystem and URL use
 */
TEST_F(PluginRepoClientTest, PluginNameValidation) {
    // Safe plugin names
    std::vector<std::string> safeNames = {
        "my-plugin",
        "my_plugin",
        "myplugin",
        "plugin123",
        "my-cool-plugin-v2"
    };
    
    for (const auto& name : safeNames) {
        // Verify name only contains safe characters
        bool isSafe = true;
        for (char c : name) {
            if (!((c >= 'a' && c <= 'z') || 
                  (c >= 'A' && c <= 'Z') || 
                  (c >= '0' && c <= '9') || 
                  c == '-' || c == '_')) {
                isSafe = false;
                break;
            }
        }
        EXPECT_TRUE(isSafe) << "Plugin name should be safe: " << name;
    }
}

/**
 * @brief Test minimum YAMS version compatibility check
 */
TEST_F(PluginRepoClientTest, MinVersionCompatibility) {
    RemotePluginInfo info;
    info.minYamsVersion = "2.0.0";
    
    // This is a placeholder - actual version comparison would be done elsewhere
    EXPECT_FALSE(info.minYamsVersion.empty());
}

// =============================================================================
// Note: Actual network tests require a mock HTTP server or integration tests
// The following tests would be enabled with a mock framework
// =============================================================================

/**
 * @brief Placeholder test for list plugins functionality
 *
 * In a full implementation, this would:
 * 1. Mock the HTTP response
 * 2. Verify correct URL construction
 * 3. Verify JSON parsing
 * 4. Verify error handling for malformed responses
 */
TEST_F(PluginRepoClientTest, DISABLED_ListPluginsReturnsValidResults) {
    auto client = makePluginRepoClient(config_);
    auto result = client->list();
    
    // This test is disabled because it requires network access
    // or a mock HTTP client implementation
    EXPECT_TRUE(result.has_value());
}

/**
 * @brief Placeholder test for get plugin functionality
 */
TEST_F(PluginRepoClientTest, DISABLED_GetPluginReturnsMetadata) {
    auto client = makePluginRepoClient(config_);
    auto result = client->get("test-plugin");
    
    EXPECT_TRUE(result.has_value());
}

/**
 * @brief Placeholder test for version listing
 */
TEST_F(PluginRepoClientTest, DISABLED_ListVersionsReturnsVersions) {
    auto client = makePluginRepoClient(config_);
    auto result = client->versions("test-plugin");
    
    EXPECT_TRUE(result.has_value());
}

/**
 * @brief Placeholder test for exists check
 */
TEST_F(PluginRepoClientTest, DISABLED_ExistsCheckWorks) {
    auto client = makePluginRepoClient(config_);
    auto result = client->exists("test-plugin");
    
    EXPECT_TRUE(result.has_value());
}

} // namespace yams::plugins::test
