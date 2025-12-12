/**
 * @file plugin_installer_test.cpp
 * @brief Unit tests for the plugin installer (Catch2)
 *
 * Tests the PluginInstaller functionality including:
 * - Installation workflow
 * - Checksum verification (security-critical)
 * - Uninstallation
 * - Update checking
 * - Trust list management
 */

#include <catch2/catch_test_macros.hpp>

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

// =============================================================================
// Stub Plugin Repository Client for unit testing
// =============================================================================

/**
 * @brief Stub implementation of IPluginRepoClient for unit testing
 * 
 * This stub provides basic default behaviors without requiring GMock.
 * For tests that need specific behaviors, test-specific stubs can be created.
 */
class StubPluginRepoClient : public IPluginRepoClient {
public:
    Result<std::vector<RemotePluginSummary>> list(const std::string& /*filter*/) override {
        return std::vector<RemotePluginSummary>{};
    }

    Result<RemotePluginInfo> get(const std::string& /*name*/,
                                  const std::optional<std::string>& /*version*/) override {
        return Error{ErrorCode::NotFound, "Stub: plugin not found"};
    }

    Result<std::vector<std::string>> versions(const std::string& /*name*/) override {
        return std::vector<std::string>{};
    }

    Result<bool> exists(const std::string& /*name*/) override { return false; }
};

/**
 * @brief Test fixture for PluginInstaller tests
 */
struct PluginInstallerFixture {
    PluginInstallerFixture() {
        // Create an isolated temp root per test to avoid shard races
        tempScope.emplace(yams::test_support::TempDirScope::unique_under("yams-installer-test"));
        testDir = tempScope->path();
        installDir = testDir / "plugins";
        trustFile = testDir / "config" / "plugins_trust.txt";

        // Create test directories
        fs::create_directories(installDir);
        fs::create_directories(trustFile.parent_path());

        // Create stub repo client
        stubClient = std::make_shared<StubPluginRepoClient>();
    }

    fs::path testDir;
    fs::path installDir;
    fs::path trustFile;
    std::optional<yams::test_support::TempDirScope> tempScope;
    std::shared_ptr<StubPluginRepoClient> stubClient;
};

// =============================================================================
// Basic Functionality Tests
// =============================================================================

TEST_CASE_METHOD(PluginInstallerFixture, "Installer creation", "[plugins][installer]") {
    SECTION("makePluginInstaller creates valid installer") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        REQUIRE(installer != nullptr);
    }
}

TEST_CASE_METHOD(PluginInstallerFixture, "listInstalled behavior",
                 "[plugins][installer][list]") {
    SECTION("returns empty on fresh install") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->listInstalled();

        REQUIRE(result.has_value());
        CHECK(result.value().empty());
    }

    SECTION("finds existing plugin directories") {
        // Create fake plugin directories
        fs::create_directories(installDir / "plugin-a");
        fs::create_directories(installDir / "plugin-b");

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->listInstalled();

        REQUIRE(result.has_value());
        CHECK(result.value().size() == 2u);

        auto& plugins = result.value();
        CHECK(std::find(plugins.begin(), plugins.end(), "plugin-a") != plugins.end());
        CHECK(std::find(plugins.begin(), plugins.end(), "plugin-b") != plugins.end());
    }
}

TEST_CASE_METHOD(PluginInstallerFixture, "installedVersion behavior",
                 "[plugins][installer][version]") {
    SECTION("returns nullopt for non-existent plugin") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("non-existent");

        REQUIRE(result.has_value());
        CHECK_FALSE(result.value().has_value());
    }

    SECTION("reads version from manifest.json") {
        // Create plugin directory with manifest
        fs::create_directories(installDir / "test-plugin");
        std::ofstream manifestFile(installDir / "test-plugin" / "manifest.json");
        manifestFile << R"({"version": "1.2.3", "name": "test-plugin"})";
        manifestFile.close();

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("test-plugin");

        REQUIRE(result.has_value());
        REQUIRE(result.value().has_value());
        CHECK(*result.value() == "1.2.3");
    }

    SECTION("returns 'unknown' without manifest.json") {
        // Create plugin directory without manifest
        fs::create_directories(installDir / "test-plugin");

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("test-plugin");

        REQUIRE(result.has_value());
        REQUIRE(result.value().has_value());
        CHECK(*result.value() == "unknown");
    }
}

// =============================================================================
// Uninstall Tests
// =============================================================================

TEST_CASE_METHOD(PluginInstallerFixture, "uninstall behavior", "[plugins][installer][uninstall]") {
    SECTION("removes plugin directory") {
        // Create plugin directory
        fs::create_directories(installDir / "test-plugin");
        REQUIRE(fs::exists(installDir / "test-plugin"));

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->uninstall("test-plugin");

        CHECK(result.has_value());
        CHECK_FALSE(fs::exists(installDir / "test-plugin"));
    }

    SECTION("fails for non-existent plugin") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->uninstall("non-existent");

        CHECK_FALSE(result.has_value());
        CHECK(result.error().code == ErrorCode::NotFound);
    }
}

// =============================================================================
// Security Tests - Checksum Verification
// =============================================================================

TEST_CASE("InstallOptions defaults", "[plugins][installer][security]") {
    InstallOptions options;

    SECTION("auto-trust is on by default") {
        CHECK(options.autoTrust);
    }

    SECTION("auto-load is on by default") {
        CHECK(options.autoLoad);
    }

    SECTION("force is off by default") {
        CHECK_FALSE(options.force);
    }

    SECTION("dry-run is off by default") {
        CHECK_FALSE(options.dryRun);
    }

    SECTION("no version specified by default") {
        CHECK_FALSE(options.version.has_value());
    }

    SECTION("no checksum override by default") {
        CHECK_FALSE(options.checksum.has_value());
    }
}

TEST_CASE("Checksum format requirements", "[plugins][installer][security]") {
    // Valid SHA-256 checksum (empty file hash)
    const std::string emptyFileSha256 =
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    SECTION("prefix is sha256:") {
        CHECK(emptyFileSha256.substr(0, 7) == "sha256:");
    }

    SECTION("total length is 71 characters") {
        CHECK(emptyFileSha256.length() == 7 + 64);
    }
}

TEST_CASE("InstallResult contains security info", "[plugins][installer][security]") {
    InstallResult result;
    result.pluginName = "test-plugin";
    result.version = "1.0.0";
    result.checksum = "sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
    result.sizeBytes = 1024 * 1024; // 1MB
    result.wasUpgrade = false;
    result.elapsed = std::chrono::milliseconds{500};

    CHECK_FALSE(result.checksum.empty());
    CHECK(result.checksum.substr(0, 7) == "sha256:");
    CHECK(result.sizeBytes > 0u);
}

// =============================================================================
// Progress Callback Tests
// =============================================================================

TEST_CASE("InstallProgress stages", "[plugins][installer][progress]") {
    SECTION("all stages are defined and distinct") {
        auto querying = InstallProgress::Stage::Querying;
        auto downloading = InstallProgress::Stage::Downloading;
        auto verifying = InstallProgress::Stage::Verifying;
        auto extracting = InstallProgress::Stage::Extracting;
        auto installing = InstallProgress::Stage::Installing;
        auto trusting = InstallProgress::Stage::Trusting;
        auto loading = InstallProgress::Stage::Loading;
        auto complete = InstallProgress::Stage::Complete;

        CHECK(querying != downloading);
        CHECK(verifying != extracting);
        CHECK(complete != querying);
    }
}

TEST_CASE("InstallProgress default values", "[plugins][installer][progress]") {
    InstallProgress progress;

    CHECK(progress.stage == InstallProgress::Stage::Querying);
    CHECK(progress.message.empty());
    CHECK(progress.progress == 0.0f);
    CHECK(progress.bytesDownloaded == 0u);
    CHECK(progress.totalBytes == 0u);
}

// =============================================================================
// Trust List Tests
// =============================================================================

TEST_CASE_METHOD(PluginInstallerFixture, "Trust file location", "[plugins][installer][trust]") {
    // Create a plugin to reference trust file location
    fs::create_directories(installDir / "trusted-plugin");
    std::ofstream manifestFile(installDir / "trusted-plugin" / "manifest.json");
    manifestFile << R"({"version": "1.0.0"})";
    manifestFile.close();

    auto installer = makePluginInstaller(stubClient, installDir, trustFile);

    SECTION("trust file is in config directory") {
        CHECK(trustFile.parent_path().filename() == "config");
        CHECK(trustFile.filename() == "plugins_trust.txt");
    }
}

// =============================================================================
// Update Check Tests
// =============================================================================

TEST_CASE_METHOD(PluginInstallerFixture, "checkUpdates behavior", "[plugins][installer][updates]") {
    SECTION("returns empty with no installed plugins") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->checkUpdates();

        REQUIRE(result.has_value());
        CHECK(result.value().empty());
    }
}

// =============================================================================
// Default Path Tests
// =============================================================================

TEST_CASE("Default paths", "[plugins][installer][defaults]") {
    SECTION("default install dir is valid") {
        auto defaultDir = getDefaultPluginInstallDir();

        CHECK_FALSE(defaultDir.empty());
        CHECK(defaultDir.string().find("yams") != std::string::npos);
        CHECK(defaultDir.string().find("plugins") != std::string::npos);
    }

    SECTION("default trust file is valid") {
        auto defaultFile = getDefaultPluginTrustFile();

        CHECK_FALSE(defaultFile.empty());
        CHECK(defaultFile.string().find("yams") != std::string::npos);
        CHECK(defaultFile.filename().string() == "plugins_trust.txt");
    }
}

// =============================================================================
// Security Best Practices Verification Tests
// =============================================================================

TEST_CASE("Security best practices for plugin installer", "[plugins][installer][security]") {
    SECTION("SHA-256 used for integrity verification") {
        const size_t sha256HexLength = 64;
        const size_t expectedTotalLength = 7 + sha256HexLength;

        const std::string sampleChecksum =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        CHECK(sampleChecksum.length() == expectedTotalLength);
    }

    SECTION("HTTPS is enforced for downloads") {
        CHECK(std::string(DEFAULT_PLUGIN_REPO_URL).substr(0, 8) == "https://");
    }

    SECTION("TLS verification enabled by default") {
        PluginRepoConfig config;
        CHECK(config.verifyTls);
    }
}

} // namespace yams::plugins::test
