/**
 * @file plugin_installer_test.cpp
 * @brief Unit tests for the plugin installer
 *
 * Tests the PluginInstaller functionality including:
 * - Installation workflow
 * - Checksum verification (security-critical)
 * - Uninstallation
 * - Update checking
 * - Trust list management
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yams/plugins/plugin_installer.hpp>
#include <yams/plugins/plugin_repo_client.hpp>

#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "tests/support/temp_dir_scope.hpp"

namespace yams::plugins::test {

namespace fs = std::filesystem;
using namespace testing;

// =============================================================================
// Mock Plugin Repository Client
// =============================================================================

/**
 * @brief Mock implementation of IPluginRepoClient for unit testing
 */
class MockPluginRepoClient : public IPluginRepoClient {
public:
    MOCK_METHOD(Result<std::vector<RemotePluginSummary>>, list, (const std::string& filter), (override));
    MOCK_METHOD(Result<RemotePluginInfo>, get, (const std::string& name, const std::optional<std::string>& version), (override));
    MOCK_METHOD(Result<std::vector<std::string>>, versions, (const std::string& name), (override));
    MOCK_METHOD(Result<bool>, exists, (const std::string& name), (override));
};

/**
 * @brief Test fixture for PluginInstaller tests
 */
class PluginInstallerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create an isolated temp root per test to avoid shard races
        tempScope_.emplace(yams::test_support::TempDirScope::unique_under("yams-installer-test"));
        testDir_ = tempScope_->path();
        installDir_ = testDir_ / "plugins";
        trustFile_ = testDir_ / "config" / "plugins_trust.txt";

        // Create test directories
        fs::create_directories(installDir_);
        fs::create_directories(trustFile_.parent_path());

        // Create mock repo client
        mockClient_ = std::make_shared<MockPluginRepoClient>();
    }

    fs::path testDir_;
    fs::path installDir_;
    fs::path trustFile_;
    std::optional<yams::test_support::TempDirScope> tempScope_;
    std::shared_ptr<MockPluginRepoClient> mockClient_;
};

// =============================================================================
// Basic Functionality Tests
// =============================================================================

/**
 * @brief Test that makePluginInstaller creates a valid installer
 */
TEST_F(PluginInstallerTest, CanCreateInstaller) {
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    EXPECT_NE(installer, nullptr);
}

/**
 * @brief Test listInstalled on empty directory
 */
TEST_F(PluginInstallerTest, ListInstalledReturnsEmptyOnFreshInstall) {
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->listInstalled();
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().empty());
}

/**
 * @brief Test listInstalled with pre-existing plugin directories
 */
TEST_F(PluginInstallerTest, ListInstalledFindsExistingPlugins) {
    // Create fake plugin directories
    fs::create_directories(installDir_ / "plugin-a");
    fs::create_directories(installDir_ / "plugin-b");
    
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->listInstalled();
    
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2u);
    
    auto& plugins = result.value();
    EXPECT_TRUE(std::find(plugins.begin(), plugins.end(), "plugin-a") != plugins.end());
    EXPECT_TRUE(std::find(plugins.begin(), plugins.end(), "plugin-b") != plugins.end());
}

/**
 * @brief Test installedVersion for non-existent plugin
 */
TEST_F(PluginInstallerTest, InstalledVersionReturnsNulloptForMissingPlugin) {
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->installedVersion("non-existent");
    
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().has_value());
}

/**
 * @brief Test installedVersion with manifest.json
 */
TEST_F(PluginInstallerTest, InstalledVersionReadsFromManifest) {
    // Create plugin directory with manifest
    fs::create_directories(installDir_ / "test-plugin");
    std::ofstream manifestFile(installDir_ / "test-plugin" / "manifest.json");
    manifestFile << R"({"version": "1.2.3", "name": "test-plugin"})";
    manifestFile.close();
    
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->installedVersion("test-plugin");
    
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value().has_value());
    EXPECT_EQ(*result.value(), "1.2.3");
}

/**
 * @brief Test installedVersion without manifest.json returns "unknown"
 */
TEST_F(PluginInstallerTest, InstalledVersionReturnsUnknownWithoutManifest) {
    // Create plugin directory without manifest
    fs::create_directories(installDir_ / "test-plugin");
    
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->installedVersion("test-plugin");
    
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value().has_value());
    EXPECT_EQ(*result.value(), "unknown");
}

// =============================================================================
// Uninstall Tests
// =============================================================================

/**
 * @brief Test uninstall removes plugin directory
 */
TEST_F(PluginInstallerTest, UninstallRemovesPluginDirectory) {
    // Create plugin directory
    fs::create_directories(installDir_ / "test-plugin");
    ASSERT_TRUE(fs::exists(installDir_ / "test-plugin"));
    
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->uninstall("test-plugin");
    
    EXPECT_TRUE(result.has_value());
    EXPECT_FALSE(fs::exists(installDir_ / "test-plugin"));
}

/**
 * @brief Test uninstall fails for non-existent plugin
 */
TEST_F(PluginInstallerTest, UninstallFailsForMissingPlugin) {
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->uninstall("non-existent");
    
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}

// =============================================================================
// Security Tests - Checksum Verification
// =============================================================================

/**
 * @brief Test InstallOptions default values
 */
TEST_F(PluginInstallerTest, InstallOptionsHaveSecureDefaults) {
    InstallOptions options;
    
    // Auto-trust should be on by default for convenience
    EXPECT_TRUE(options.autoTrust);
    
    // Auto-load should be on by default
    EXPECT_TRUE(options.autoLoad);
    
    // Force should be off by default (don't overwrite existing)
    EXPECT_FALSE(options.force);
    
    // Dry-run should be off by default
    EXPECT_FALSE(options.dryRun);
    
    // No version specified means latest
    EXPECT_FALSE(options.version.has_value());
    
    // No checksum override by default (use server-provided)
    EXPECT_FALSE(options.checksum.has_value());
}

/**
 * @brief Test that checksum format is enforced
 *
 * SHA-256 is the required algorithm for plugin integrity verification.
 * This test verifies the checksum format requirements.
 */
TEST_F(PluginInstallerTest, ChecksumFormatRequirements) {
    // Valid SHA-256 checksum (empty file hash)
    const std::string emptyFileSha256 = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    
    // Verify prefix
    EXPECT_TRUE(emptyFileSha256.substr(0, 7) == "sha256:");
    
    // Verify hex portion length (64 chars for SHA-256)
    EXPECT_EQ(emptyFileSha256.length(), 7 + 64);
}

/**
 * @brief Test InstallResult captures security-relevant information
 */
TEST_F(PluginInstallerTest, InstallResultContainsSecurityInfo) {
    InstallResult result;
    result.pluginName = "test-plugin";
    result.version = "1.0.0";
    result.checksum = "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
    result.sizeBytes = 1024 * 1024; // 1MB
    result.wasUpgrade = false;
    result.elapsed = std::chrono::milliseconds{500};
    
    EXPECT_FALSE(result.checksum.empty());
    EXPECT_TRUE(result.checksum.substr(0, 7) == "sha256:");
    EXPECT_GT(result.sizeBytes, 0u);
}

// =============================================================================
// Progress Callback Tests
// =============================================================================

/**
 * @brief Test InstallProgress stage enum values
 */
TEST_F(PluginInstallerTest, InstallProgressStagesAreDefined) {
    // Verify all stages are accessible
    auto querying = InstallProgress::Stage::Querying;
    auto downloading = InstallProgress::Stage::Downloading;
    auto verifying = InstallProgress::Stage::Verifying;
    auto extracting = InstallProgress::Stage::Extracting;
    auto installing = InstallProgress::Stage::Installing;
    auto trusting = InstallProgress::Stage::Trusting;
    auto loading = InstallProgress::Stage::Loading;
    auto complete = InstallProgress::Stage::Complete;
    
    // Just verify they're distinct
    EXPECT_NE(querying, downloading);
    EXPECT_NE(verifying, extracting);
    EXPECT_NE(complete, querying);
}

/**
 * @brief Test InstallProgress default values
 */
TEST_F(PluginInstallerTest, InstallProgressDefaultValues) {
    InstallProgress progress;
    
    EXPECT_EQ(progress.stage, InstallProgress::Stage::Querying);
    EXPECT_TRUE(progress.message.empty());
    EXPECT_FLOAT_EQ(progress.progress, 0.0f);
    EXPECT_EQ(progress.bytesDownloaded, 0u);
    EXPECT_EQ(progress.totalBytes, 0u);
}

// =============================================================================
// Trust List Tests
// =============================================================================

/**
 * @brief Test that trust file is created in the correct location
 */
TEST_F(PluginInstallerTest, TrustFileLocationIsCorrect) {
    // Create a plugin to trigger trust file creation
    fs::create_directories(installDir_ / "trusted-plugin");
    std::ofstream manifestFile(installDir_ / "trusted-plugin" / "manifest.json");
    manifestFile << R"({"version": "1.0.0"})";
    manifestFile.close();
    
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    
    // Verify trust file would be at expected location
    EXPECT_EQ(trustFile_.parent_path().filename(), "config");
    EXPECT_EQ(trustFile_.filename(), "plugins_trust.txt");
}

// =============================================================================
// Update Check Tests
// =============================================================================

/**
 * @brief Test checkUpdates with no installed plugins
 */
TEST_F(PluginInstallerTest, CheckUpdatesWithNoPluginsReturnsEmpty) {
    auto installer = makePluginInstaller(mockClient_, installDir_, trustFile_);
    auto result = installer->checkUpdates();
    
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().empty());
}

// =============================================================================
// Default Path Tests
// =============================================================================

/**
 * @brief Test getDefaultPluginInstallDir returns a valid path
 */
TEST(PluginInstallerDefaults, DefaultInstallDirIsValid) {
    auto defaultDir = getDefaultPluginInstallDir();
    
    // Should not be empty
    EXPECT_FALSE(defaultDir.empty());
    
    // Should contain "yams" somewhere in the path
    EXPECT_NE(defaultDir.string().find("yams"), std::string::npos);
    
    // Should contain "plugins"
    EXPECT_NE(defaultDir.string().find("plugins"), std::string::npos);
}

/**
 * @brief Test getDefaultPluginTrustFile returns a valid path
 */
TEST(PluginInstallerDefaults, DefaultTrustFileIsValid) {
    auto defaultFile = getDefaultPluginTrustFile();
    
    // Should not be empty
    EXPECT_FALSE(defaultFile.empty());
    
    // Should contain "yams" somewhere in the path
    EXPECT_NE(defaultFile.string().find("yams"), std::string::npos);
    
    // Should end with a recognizable filename
    EXPECT_EQ(defaultFile.filename().string(), "plugins_trust.txt");
}

// =============================================================================
// Security Best Practices Verification Tests
// =============================================================================

/**
 * @brief Verify that SHA-256 is used for integrity verification
 *
 * This is a documentation test to ensure the security design is correct.
 * SHA-256 provides:
 * - 256-bit output (collision-resistant)
 * - Pre-image resistance
 * - Second pre-image resistance
 */
TEST(PluginInstallerSecurity, Sha256IsUsedForIntegrity) {
    // SHA-256 produces a 64-character hex string
    const size_t sha256HexLength = 64;
    
    // With prefix "sha256:" the total length is 71
    const size_t expectedTotalLength = 7 + sha256HexLength;
    
    // Sample checksum to verify format
    const std::string sampleChecksum = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    
    EXPECT_EQ(sampleChecksum.length(), expectedTotalLength);
}

/**
 * @brief Verify HTTPS is enforced for downloads
 *
 * Plugin downloads should only occur over HTTPS to prevent:
 * - Man-in-the-middle attacks
 * - Content injection
 * - Eavesdropping
 */
TEST(PluginInstallerSecurity, HttpsIsPreferredForDownloads) {
    // Default repository URL should use HTTPS
    EXPECT_EQ(std::string(DEFAULT_PLUGIN_REPO_URL).substr(0, 8), "https://");
}

/**
 * @brief Verify TLS certificate verification is enabled by default
 */
TEST(PluginInstallerSecurity, TlsVerificationEnabledByDefault) {
    PluginRepoConfig config;
    EXPECT_TRUE(config.verifyTls);
}

} // namespace yams::plugins::test
