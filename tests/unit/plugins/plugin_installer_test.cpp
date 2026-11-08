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

#include <yams/crypto/hasher.h>
#include <yams/daemon/resource/plugin_trust.h>
#include <yams/plugins/plugin_installer.hpp>
#include <yams/plugins/plugin_repo_client.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <zlib.h>

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

namespace {

/**
 * @brief Repository client that serves one fixed release, so install() runs end to end.
 */
class FixedPluginRepoClient : public IPluginRepoClient {
public:
    Result<std::vector<RemotePluginSummary>> list(const std::string& /*filter*/) override {
        return std::vector<RemotePluginSummary>{};
    }

    Result<RemotePluginInfo> get(const std::string& name,
                                 const std::optional<std::string>& /*version*/) override {
        if (name != info.name) {
            return Error{ErrorCode::NotFound, "Fixed repo: plugin not found"};
        }
        return info;
    }

    Result<std::vector<std::string>> versions(const std::string& /*name*/) override {
        return std::vector<std::string>{info.version};
    }

    Result<bool> exists(const std::string& name) override { return name == info.name; }

    RemotePluginInfo info;
};

struct TarEntry {
    std::string name;
    std::string body; // ignored for directories
    bool isDirectory{false};
};

// Writes `entries` as a gzip-compressed ustar archive to `archivePath`. Built in-process so the
// tests need no external tar tool or binary fixture.
fs::path writePluginArchive(const fs::path& archivePath, const std::vector<TarEntry>& entries) {
    constexpr size_t kBlock = 512;
    std::string tar;
    for (const auto& entry : entries) {
        REQUIRE((entry.name.size() < 100u));
        const std::string body = entry.isDirectory ? std::string{} : entry.body;

        std::array<char, kBlock> header{};
        std::memcpy(header.data(), entry.name.data(), entry.name.size());
        auto putOctal = [&header](size_t offset, size_t width, std::uint64_t value) {
            std::snprintf(header.data() + offset, width, "%0*llo", static_cast<int>(width - 1),
                          static_cast<unsigned long long>(value));
        };
        putOctal(100, 8, entry.isDirectory ? 0755 : 0644); // mode
        putOctal(108, 8, 0);                               // uid
        putOctal(116, 8, 0);                               // gid
        putOctal(124, 12, body.size());                    // size
        putOctal(136, 12, 0);                              // mtime
        header[156] = entry.isDirectory ? '5' : '0';       // typeflag
        std::memcpy(header.data() + 257, "ustar", 6);
        std::memcpy(header.data() + 263, "00", 2);
        std::memset(header.data() + 148, ' ', 8);
        unsigned int sum = 0;
        for (char ch : header) {
            sum += static_cast<unsigned char>(ch);
        }
        std::snprintf(header.data() + 148, 7, "%06o", sum);
        header[155] = ' ';

        tar.append(header.data(), header.size());
        tar += body;
        tar.append((kBlock - body.size() % kBlock) % kBlock, '\0');
    }
    tar.append(2 * kBlock, '\0'); // end-of-archive marker

    z_stream zs{};
    REQUIRE((deflateInit2(&zs, Z_BEST_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) ==
             Z_OK));
    std::vector<unsigned char> gz(deflateBound(&zs, static_cast<uLong>(tar.size())) + 64);
    zs.next_in = reinterpret_cast<Bytef*>(tar.data());
    zs.avail_in = static_cast<uInt>(tar.size());
    zs.next_out = gz.data();
    zs.avail_out = static_cast<uInt>(gz.size());
    const int rc = deflate(&zs, Z_FINISH);
    gz.resize(zs.total_out);
    deflateEnd(&zs);
    REQUIRE((rc == Z_STREAM_END));

    fs::create_directories(archivePath.parent_path());
    std::ofstream out(archivePath, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(gz.data()), static_cast<std::streamsize>(gz.size()));
    out.close();
    REQUIRE((fs::file_size(archivePath) == gz.size()));
    return archivePath;
}

// A single-file plugin archive: "fake-plugin/yams-plugin.json" at version 1.0.0.
fs::path writeFakePluginArchive(const fs::path& archivePath) {
    return writePluginArchive(archivePath, {{"fake-plugin/yams-plugin.json",
                                             R"({"name": "fake-plugin", "version": "1.0.0"})"
                                             "\n"}});
}

std::string sha256Of(const fs::path& path) {
    return "sha256:" + yams::crypto::SHA256Hasher{}.hashFile(path);
}

// The installer downloads through libcurl, which serves file:// URLs locally.
std::string fileUrl(const fs::path& path) {
    std::string generic = fs::absolute(path).generic_string();
    if (generic.empty() || generic.front() != '/') {
        generic.insert(generic.begin(), '/'); // Windows drive paths: file:///C:/...
    }
    return "file://" + generic;
}

bool isTrusted(const fs::path& trustFile, const fs::path& pluginDir) {
    std::set<fs::path> entries;
    if (!yams::daemon::plugin_trust::readTrustStore(trustFile, entries)) {
        return false;
    }
    return entries.count(yams::daemon::plugin_trust::normalizePath(pluginDir)) > 0;
}

} // namespace

/**
 * @brief Test fixture for PluginInstaller tests
 */
struct PluginInstallerFixture {
    PluginInstallerFixture() {
        // Create an isolated temp root per test to avoid shard races
        tempScope.emplace(yams::test_support::TempDirScope::unique_under("yams-installer-test"));
        testDir = tempScope->path();
        installDir = testDir / "plugins";
        trustFile = testDir / "data" / "plugins.trust";

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
        REQUIRE((installer != nullptr));
    }
}

TEST_CASE_METHOD(PluginInstallerFixture, "listInstalled behavior", "[plugins][installer][list]") {
    SECTION("returns empty on fresh install") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->listInstalled();

        REQUIRE((result.has_value()));
        CHECK((result.value().empty()));
    }

    SECTION("finds existing plugin directories") {
        // Create fake plugin directories
        fs::create_directories(installDir / "plugin-a");
        fs::create_directories(installDir / "plugin-b");

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->listInstalled();

        REQUIRE((result.has_value()));
        CHECK((result.value().size() == 2u));

        auto& plugins = result.value();
        CHECK((std::find(plugins.begin(), plugins.end(), "plugin-a") != plugins.end()));
        CHECK((std::find(plugins.begin(), plugins.end(), "plugin-b") != plugins.end()));
    }
}

TEST_CASE_METHOD(PluginInstallerFixture, "installedVersion behavior",
                 "[plugins][installer][version]") {
    SECTION("returns nullopt for non-existent plugin") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("non-existent");

        REQUIRE((result.has_value()));
        CHECK_FALSE(result.value().has_value());
    }

    SECTION("reads version from yams-plugin.json") {
        fs::create_directories(installDir / "test-plugin");
        std::ofstream manifestFile(installDir / "test-plugin" / "yams-plugin.json");
        manifestFile << R"({"version": "1.2.3", "name": "test-plugin"})";
        manifestFile.close();

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("test-plugin");

        REQUIRE((result.has_value()));
        REQUIRE((result.value().has_value()));
        CHECK((*result.value() == "1.2.3"));
    }

    SECTION("reads version from manifest.json as a fallback") {
        fs::create_directories(installDir / "test-plugin");
        std::ofstream manifestFile(installDir / "test-plugin" / "manifest.json");
        manifestFile << R"({"version": "1.2.3", "name": "test-plugin"})";
        manifestFile.close();

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("test-plugin");

        REQUIRE((result.has_value()));
        REQUIRE((result.value().has_value()));
        CHECK((*result.value() == "1.2.3"));
    }

    SECTION("returns 'unknown' without either manifest file") {
        // Create plugin directory without manifest
        fs::create_directories(installDir / "test-plugin");

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("test-plugin");

        REQUIRE((result.has_value()));
        REQUIRE((result.value().has_value()));
        CHECK((*result.value() == "unknown"));
    }

    SECTION("rejects path traversal names") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->installedVersion("../test-plugin");

        CHECK_FALSE(result.has_value());
        CHECK((result.error().code == ErrorCode::InvalidArgument));
    }
}

// =============================================================================
// Uninstall Tests
// =============================================================================

TEST_CASE_METHOD(PluginInstallerFixture, "uninstall behavior", "[plugins][installer][uninstall]") {
    SECTION("removes plugin directory") {
        // Create plugin directory
        fs::create_directories(installDir / "test-plugin");
        REQUIRE((fs::exists(installDir / "test-plugin")));

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->uninstall("test-plugin");

        CHECK((result.has_value()));
        CHECK_FALSE(fs::exists(installDir / "test-plugin"));
    }

    SECTION("fails for non-existent plugin") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->uninstall("non-existent");

        CHECK_FALSE(result.has_value());
        CHECK((result.error().code == ErrorCode::NotFound));
    }

    SECTION("rejects path traversal names without deleting outside install root") {
        auto outside = testDir / "outside";
        fs::create_directories(outside);
        std::ofstream(outside / "sentinel.txt") << "keep";
        REQUIRE((fs::exists(outside / "sentinel.txt")));

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->uninstall("../outside");

        CHECK_FALSE(result.has_value());
        CHECK((result.error().code == ErrorCode::InvalidArgument));
        CHECK((fs::exists(outside / "sentinel.txt")));
    }

#if !defined(_WIN32)
    SECTION("trust file remains private after trust update on uninstall") {
        fs::create_directories(installDir / "test-plugin");
        {
            std::ofstream trustOut(trustFile, std::ios::trunc);
            trustOut << "# YAMS Plugin Trust List\n";
            trustOut << fs::weakly_canonical(installDir / "test-plugin").string() << "\n";
        }

        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->uninstall("test-plugin");
        REQUIRE((result.has_value()));
        REQUIRE((fs::exists(trustFile)));

        std::error_code ec;
        auto perms = fs::status(trustFile, ec).permissions();
        REQUIRE_FALSE(ec);
        CHECK(((perms & (fs::perms::group_all | fs::perms::others_all)) == fs::perms::none));
    }
#endif
}

// =============================================================================
// Security Tests - Checksum Verification
// =============================================================================

TEST_CASE("InstallOptions defaults", "[plugins][installer][security]") {
    InstallOptions options;

    SECTION("auto-trust is on by default") {
        CHECK((options.autoTrust));
    }

    SECTION("auto-load is off by default") {
        CHECK_FALSE(options.autoLoad);
    }

    SECTION("no load callback is configured by default") {
        CHECK_FALSE(static_cast<bool>(options.loadAfterInstall));
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

TEST_CASE_METHOD(PluginInstallerFixture, "install verifies the archive checksum before trusting it",
                 "[plugins][installer][security]") {
    const fs::path archive = writeFakePluginArchive(testDir / "source" / "fake-plugin.tar.gz");
    const std::string actualChecksum = sha256Of(archive);
    const fs::path pluginDir = installDir / "fake-plugin";

    auto repo = std::make_shared<FixedPluginRepoClient>();
    repo->info.name = "fake-plugin";
    repo->info.version = "1.0.0";
    repo->info.downloadUrl = fileUrl(archive);

    InstallOptions options;
    REQUIRE((options.autoTrust)); // trust is the default, so verification must gate it

    SECTION("checksum mismatch is rejected and nothing is installed or trusted") {
        repo->info.checksum = "sha256:" + std::string(64, '0');
        auto installer = makePluginInstaller(repo, installDir, trustFile);

        auto result = installer->install("fake-plugin", options);

        REQUIRE_FALSE(result.has_value());
        CHECK((result.error().code == ErrorCode::HashMismatch));
        CHECK_FALSE(fs::exists(pluginDir));
        CHECK_FALSE(isTrusted(trustFile, pluginDir));
    }

    SECTION("a release without a published checksum is refused") {
        repo->info.checksum.clear();
        auto installer = makePluginInstaller(repo, installDir, trustFile);

        auto result = installer->install("fake-plugin", options);

        REQUIRE_FALSE(result.has_value());
        CHECK((result.error().code == ErrorCode::InvalidData));
        CHECK((result.error().message.find("--checksum") != std::string::npos));
        CHECK_FALSE(fs::exists(pluginDir));
        CHECK_FALSE(isTrusted(trustFile, pluginDir));
    }

    SECTION("a matching repository checksum installs and trusts the plugin") {
        repo->info.checksum = actualChecksum;
        auto installer = makePluginInstaller(repo, installDir, trustFile);

        auto result = installer->install("fake-plugin", options);

        REQUIRE((result.has_value()));
        CHECK((result.value().checksum == actualChecksum));
        CHECK((fs::exists(pluginDir / "yams-plugin.json")));
        auto version = installer->installedVersion("fake-plugin");
        REQUIRE((version.has_value()));
        REQUIRE((version.value().has_value()));
        CHECK((*version.value() == "1.0.0"));
        CHECK((isTrusted(trustFile, pluginDir)));
    }

    SECTION("a caller-supplied checksum verifies a release the repository left unsigned") {
        repo->info.checksum.clear();
        // Bare upper-case hex, as copied from `sha256sum` output, is accepted.
        std::string bareHex = actualChecksum.substr(std::string("sha256:").size());
        for (auto& ch : bareHex) {
            ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
        }
        options.checksum = bareHex;
        auto installer = makePluginInstaller(repo, installDir, trustFile);

        auto result = installer->install("fake-plugin", options);

        REQUIRE((result.has_value()));
        CHECK((result.value().checksum == actualChecksum));
        CHECK((isTrusted(trustFile, pluginDir)));
    }
}

TEST_CASE_METHOD(PluginInstallerFixture, "install extracts tar-style archive entries safely",
                 "[plugins][installer][security]") {
    const fs::path pluginDir = installDir / "fake-plugin";
    auto repo = std::make_shared<FixedPluginRepoClient>();
    repo->info.name = "fake-plugin";
    repo->info.version = "1.0.0";

    auto installFrom = [&](const std::vector<TarEntry>& entries) {
        const fs::path archive =
            writePluginArchive(testDir / "source" / "fake-plugin.tar.gz", entries);
        repo->info.downloadUrl = fileUrl(archive);
        repo->info.checksum = sha256Of(archive);
        auto installer = makePluginInstaller(repo, installDir, trustFile);
        return installer->install("fake-plugin", InstallOptions{});
    };

    SECTION("a directory entry with a trailing slash installs") {
        // What `tar czf out.tar.gz plugin` produces.
        auto result = installFrom({{"plugin/", "", true}, {"plugin/lib.so", "ELF"}});

        REQUIRE((result.has_value()));
        CHECK((fs::is_regular_file(pluginDir / "lib.so")));
        CHECK((isTrusted(trustFile, pluginDir)));
    }

    SECTION("'./' entries install") {
        // What `tar -C dir -czf out.tar.gz .` produces.
        auto result = installFrom({{"./", "", true}, {"./plugin.so", "ELF"}});

        REQUIRE((result.has_value()));
        CHECK((fs::is_regular_file(pluginDir / "plugin.so")));
        CHECK((isTrusted(trustFile, pluginDir)));
    }

    SECTION("a '..' entry is rejected and nothing escapes the extraction root") {
        // The installer extracts into <temp>/<install-temp>/extracted, so "../../<name>" would
        // land directly in the system temp directory if it were honored.
        const std::string escapeName = "esc-" + testDir.filename().string();
        const fs::path escapeTarget = fs::temp_directory_path() / escapeName;
        REQUIRE_FALSE(fs::exists(escapeTarget));

        auto result = installFrom({{"../../" + escapeName, "evil"}});

        REQUIRE_FALSE(result.has_value());
        CHECK((result.error().code == ErrorCode::InvalidPath));
        CHECK_FALSE(fs::exists(escapeTarget));
        CHECK_FALSE(fs::exists(pluginDir));
        CHECK_FALSE(isTrusted(trustFile, pluginDir));
    }
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
        auto complete = InstallProgress::Stage::Complete;

        CHECK((querying != downloading));
        CHECK((verifying != extracting));
        CHECK((complete != querying));
    }
}

TEST_CASE("InstallProgress default values", "[plugins][installer][progress]") {
    InstallProgress progress;

    CHECK((progress.stage == InstallProgress::Stage::Querying));
    CHECK((progress.message.empty()));
    CHECK((progress.progress == 0.0f));
    CHECK((progress.bytesDownloaded == 0u));
    CHECK((progress.totalBytes == 0u));
}

// =============================================================================
// Update Check Tests
// =============================================================================

TEST_CASE_METHOD(PluginInstallerFixture, "checkUpdates behavior", "[plugins][installer][updates]") {
    SECTION("returns empty with no installed plugins") {
        auto installer = makePluginInstaller(stubClient, installDir, trustFile);
        auto result = installer->checkUpdates();

        REQUIRE((result.has_value()));
        CHECK((result.value().empty()));
    }
}

TEST_CASE_METHOD(PluginInstallerFixture, "install preflight rejects auto-load without callback",
                 "[plugins][installer][install]") {
    auto installer = makePluginInstaller(stubClient, installDir, trustFile);

    InstallOptions options;
    options.autoLoad = true;

    auto result = installer->install("missing-plugin", options);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().code == ErrorCode::InvalidArgument));
}

// =============================================================================
// Default Path Tests
// =============================================================================

TEST_CASE("Default paths", "[plugins][installer][defaults]") {
    SECTION("default install dir is valid") {
        auto defaultDir = getDefaultPluginInstallDir();

        CHECK_FALSE(defaultDir.empty());
        CHECK((defaultDir.string().find("yams") != std::string::npos));
        CHECK((defaultDir.string().find("plugins") != std::string::npos));
    }

    SECTION("default trust file is valid") {
        auto defaultFile = getDefaultPluginTrustFile();

        CHECK_FALSE(defaultFile.empty());
        CHECK((defaultFile.string().find("yams") != std::string::npos));
        CHECK((defaultFile.filename().string() == "plugins.trust"));
    }
}

} // namespace yams::plugins::test
