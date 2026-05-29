/**
 * @file plugin_installer.cpp
 * @brief Implementation of the plugin installer
 */

#include <yams/common/fs_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/resource/plugin_trust.h>
#include <yams/plugins/plugin_installer.hpp>

#include <cctype>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <regex>
#include <set>
#include <sstream>
#include <string_view>

#ifdef _WIN32
#include <ShlObj.h>
#include <Windows.h>
#else
#include <pwd.h>
#include <unistd.h>
#endif

// Archive extraction (minitar or platform-specific)
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <archive.h>
#include <archive_entry.h>
#include <curl/curl.h>
#include <openssl/evp.h>

namespace yams::plugins {

namespace fs = std::filesystem;

namespace {

// cURL progress callback data
struct ProgressData {
    InstallProgressCallback callback;
    InstallProgress progress;
};

// cURL progress callback
int curlProgressCallback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                         curl_off_t /*ultotal*/, curl_off_t /*ulnow*/) {
    auto* data = static_cast<ProgressData*>(clientp);
    if (data && data->callback) {
        data->progress.bytesDownloaded = static_cast<uint64_t>(dlnow);
        data->progress.totalBytes = dltotal > 0 ? static_cast<uint64_t>(dltotal) : 0;
        if (dltotal > 0) {
            data->progress.progress = static_cast<float>(dlnow) / static_cast<float>(dltotal);
        }
        data->callback(data->progress);
    }
    return 0;
}

// cURL write callback
size_t curlWriteToFile(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* file = static_cast<std::ofstream*>(userdata);
    if (file && file->is_open()) {
        file->write(ptr, static_cast<std::streamsize>(size * nmemb));
        return size * nmemb;
    }
    return 0;
}

bool isSafePathSegment(std::string_view value, bool allowPlus = false) {
    if (value.empty() || value == "." || value == "..") {
        return false;
    }
    for (unsigned char ch : value) {
        if (std::isalnum(ch) || ch == '_' || ch == '-' || ch == '.' || (allowPlus && ch == '+')) {
            continue;
        }
        return false;
    }
    return true;
}

Result<void> validatePluginName(std::string_view name) {
    if (!isSafePathSegment(name)) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid plugin name. Use only letters, numbers, '.', '_' and '-'."};
    }
    return {};
}

Result<void> validatePluginVersion(std::string_view version) {
    if (!isSafePathSegment(version, true)) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid plugin version. Use only letters, numbers, '.', '_', '-' and '+'."};
    }
    return {};
}

bool isRelativePathSafe(const fs::path& path) {
    if (path.empty() || path.is_absolute()) {
        return false;
    }
    for (const auto& part : path) {
        if (part == "." || part == ".." || part.empty()) {
            return false;
        }
    }
    return true;
}

fs::path absoluteLexical(const fs::path& path) {
    std::error_code ec;
    auto absolute = fs::absolute(path, ec);
    if (ec) {
        return path.lexically_normal();
    }
    return absolute.lexically_normal();
}

bool isPathWithin(const fs::path& root, const fs::path& child) {
    const auto rootNorm = absoluteLexical(root);
    const auto childNorm = absoluteLexical(child);
    auto rootIt = rootNorm.begin();
    auto childIt = childNorm.begin();
    for (; rootIt != rootNorm.end(); ++rootIt, ++childIt) {
        if (childIt == childNorm.end() || *rootIt != *childIt) {
            return false;
        }
    }
    return true;
}

Result<void> removeAllScoped(const fs::path& path, const fs::path& root,
                             std::string_view description) {
    const auto rootNorm = absoluteLexical(root);
    const auto pathNorm = absoluteLexical(path);
    if (pathNorm == rootNorm || !isPathWithin(rootNorm, pathNorm)) {
        return Error{ErrorCode::InvalidPath, "Refusing recursive delete outside scoped " +
                                                 std::string(description) +
                                                 " root: " + path.string()};
    }

    std::error_code ec;
    fs::remove_all(pathNorm, ec);
    if (ec) {
        return Error{ErrorCode::IOError,
                     "Failed to remove " + std::string(description) + ": " + ec.message()};
    }
    return {};
}

Result<fs::path> makePluginTempDir(const std::string& name, const std::string& version) {
    auto tempDir = yams::common::createTempDirectory("yams-plugin-" + name + "-" + version + "-");
    if (tempDir.empty()) {
        return Error{ErrorCode::IOError, "Failed to create plugin temp directory"};
    }
    return tempDir;
}

// Compute SHA-256 hash of a file using modern EVP API
std::string computeSha256(const fs::path& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        return "";
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return "";
    }

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        return "";
    }

    char buffer[8192];
    while (file.read(buffer, sizeof(buffer)) || file.gcount() > 0) {
        if (EVP_DigestUpdate(ctx, buffer, static_cast<size_t>(file.gcount())) != 1) {
            EVP_MD_CTX_free(ctx);
            return "";
        }
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, hash, &hashLen) != 1) {
        EVP_MD_CTX_free(ctx);
        return "";
    }

    EVP_MD_CTX_free(ctx);

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < hashLen; ++i) {
        oss << std::setw(2) << static_cast<int>(hash[i]);
    }
    return "sha256:" + oss.str();
}

// Extract tar.gz archive
Result<void> extractArchive(const fs::path& archivePath, const fs::path& destDir) {
    struct archive* a = archive_read_new();
    struct archive* ext = archive_write_disk_new();
    struct archive_entry* entry;
    int r;

    archive_read_support_format_tar(a);
    archive_read_support_filter_gzip(a);

    archive_write_disk_set_options(ext, ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM |
                                            ARCHIVE_EXTRACT_ACL | ARCHIVE_EXTRACT_FFLAGS |
                                            ARCHIVE_EXTRACT_SECURE_NODOTDOT);

    r = archive_read_open_filename(a, archivePath.string().c_str(), 10240);
    if (r != ARCHIVE_OK) {
        archive_read_free(a);
        archive_write_free(ext);
        return Error{ErrorCode::IOError,
                     std::string("Failed to open archive: ") + archive_error_string(a)};
    }

    // Create destination directory
    if (!yams::common::ensureDirectories(destDir)) {
        archive_read_free(a);
        archive_write_free(ext);
        return Error{ErrorCode::IOError, "Failed to create destination directory"};
    }

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        // Construct full path in destination
        const char* rawName = archive_entry_pathname(entry);
        if (rawName == nullptr) {
            archive_read_free(a);
            archive_write_free(ext);
            return Error{ErrorCode::CorruptedData, "Archive entry is missing a path"};
        }

        fs::path relativeEntryPath{rawName};
        if (!isRelativePathSafe(relativeEntryPath)) {
            archive_read_free(a);
            archive_write_free(ext);
            return Error{ErrorCode::InvalidPath,
                         "Archive entry escapes extraction root: " + std::string(rawName)};
        }

        fs::path entryPath = (destDir / relativeEntryPath).lexically_normal();
        if (!isPathWithin(destDir, entryPath)) {
            archive_read_free(a);
            archive_write_free(ext);
            return Error{ErrorCode::InvalidPath,
                         "Archive entry escapes extraction root: " + std::string(rawName)};
        }
        archive_entry_set_pathname(entry, entryPath.string().c_str());

        r = archive_write_header(ext, entry);
        if (r != ARCHIVE_OK) {
            spdlog::warn("Failed to extract header: {}", archive_error_string(ext));
        } else if (archive_entry_size(entry) > 0) {
            const void* buff;
            size_t size;
            la_int64_t offset;

            while (archive_read_data_block(a, &buff, &size, &offset) == ARCHIVE_OK) {
                archive_write_data_block(ext, buff, size, offset);
            }
        }
        archive_write_finish_entry(ext);
    }

    archive_read_free(a);
    archive_write_free(ext);

    return {};
}

} // namespace

class PluginInstallerImpl : public IPluginInstaller {
public:
    PluginInstallerImpl(std::shared_ptr<IPluginRepoClient> repoClient, fs::path installDir,
                        fs::path trustFile)
        : repoClient_(std::move(repoClient)), installDir_(std::move(installDir)),
          trustFile_(std::move(trustFile)) {
        if (installDir_.empty()) {
            installDir_ = getDefaultPluginInstallDir();
        }
        if (trustFile_.empty()) {
            trustFile_ = getDefaultPluginTrustFile();
        }
    }

    Result<InstallResult> install(const std::string& nameOrUrl,
                                  const InstallOptions& options) override {
        auto startTime = std::chrono::steady_clock::now();
        InstallResult result;

        auto notifyProgress = [&](InstallProgress::Stage stage, const std::string& msg,
                                  float pct = 0.0f) {
            if (options.onProgress) {
                InstallProgress progress;
                progress.stage = stage;
                progress.message = msg;
                progress.progress = pct;
                options.onProgress(progress);
            }
        };

        // Parse name[@version]
        std::string pluginName = nameOrUrl;
        std::optional<std::string> version = options.version;
        bool isUrl = nameOrUrl.find("://") != std::string::npos;

        if (!isUrl) {
            // Check for name@version format
            auto atPos = nameOrUrl.find('@');
            if (atPos != std::string::npos) {
                pluginName = nameOrUrl.substr(0, atPos);
                version = nameOrUrl.substr(atPos + 1);
            }
        }

        notifyProgress(InstallProgress::Stage::Querying, "Fetching plugin metadata...");

        RemotePluginInfo info;
        if (isUrl) {
            // Direct URL install - minimal metadata
            info.downloadUrl = nameOrUrl;
            // Try to extract name from URL pattern first, fall back to stem-based extraction:
            // /plugins/<name>/<version>/<name>-<version>.tar.gz
            std::regex urlPattern(R"(/plugins/([^/]+)/([^/]+)/[^/]+\.tar\.gz$)");
            std::smatch match;
            if (std::regex_search(nameOrUrl, match, urlPattern)) {
                info.name = match[1].str();
                info.version = match[2].str();
            } else {
                info.name = fs::path(nameOrUrl).stem().string();
                info.version = version.value_or("unknown");
            }
        } else {
            auto infoRes = repoClient_->get(pluginName, version);
            if (!infoRes) {
                return infoRes.error();
            }
            info = infoRes.value();
        }

        if (auto nameValidation = validatePluginName(info.name); !nameValidation) {
            return nameValidation.error();
        }
        if (auto versionValidation = validatePluginVersion(info.version); !versionValidation) {
            return versionValidation.error();
        }

        result.pluginName = info.name;
        result.version = info.version;

        // Determine install directory
        fs::path targetDir = options.installDir.empty() ? installDir_ : options.installDir;
        fs::path pluginDir = targetDir / info.name;

        // Check if already installed
        auto installedVer = installedVersion(info.name);
        if (installedVer && installedVer.value()) {
            result.wasUpgrade = true;
            result.previousVersion = *installedVer.value();
            if (!options.force && result.previousVersion == info.version) {
                return Error{ErrorCode::InvalidOperation,
                             "Plugin " + info.name + "@" + info.version +
                                 " is already installed. Use --force to reinstall."};
            }
        }

        if (options.dryRun) {
            result.installedPath = pluginDir;
            result.checksum = info.checksum;
            result.sizeBytes = info.sizeBytes;
            spdlog::info("[dry-run] Would install {} {} to {}", info.name, info.version,
                         pluginDir.string());
            return result;
        }

        // Create a unique scoped temp directory for download/extract.
        auto tempDirRes = makePluginTempDir(info.name, info.version);
        if (!tempDirRes) {
            return tempDirRes.error();
        }
        fs::path tempDir = tempDirRes.value();
        const fs::path tempRoot = fs::temp_directory_path();
        std::error_code ec;
        fs::path archivePath = tempDir / (info.name + "-" + info.version + ".tar.gz");

        // Download
        notifyProgress(InstallProgress::Stage::Downloading, "Downloading " + info.name + "...");
        auto downloadRes = downloadFile(info.downloadUrl, archivePath, options);
        if (!downloadRes) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return downloadRes.error();
        }

        // Verify checksum
        notifyProgress(InstallProgress::Stage::Verifying, "Verifying integrity...", 0.0f);
        std::string expectedChecksum = options.checksum.value_or(info.checksum);
        if (!expectedChecksum.empty()) {
            std::string actualChecksum = computeSha256(archivePath);
            if (actualChecksum != expectedChecksum) {
                (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
                return Error{ErrorCode::HashMismatch, "Checksum mismatch: expected " +
                                                          expectedChecksum + ", got " +
                                                          actualChecksum};
            }
            result.checksum = actualChecksum;
        } else {
            result.checksum = computeSha256(archivePath);
        }

        // Extract
        notifyProgress(InstallProgress::Stage::Extracting, "Extracting archive...");
        fs::path extractDir = tempDir / "extracted";
        auto extractRes = extractArchive(archivePath, extractDir);
        if (!extractRes) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return extractRes.error();
        }

        // Install (move to plugin directory)
        notifyProgress(InstallProgress::Stage::Installing, "Installing plugin...");

        // Remove existing installation if upgrading
        if (result.wasUpgrade) {
            auto removeRes = removeAllScoped(pluginDir, targetDir, "plugin install");
            if (!removeRes) {
                (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
                return removeRes.error();
            }
        }

        if (!yams::common::ensureDirectories(targetDir)) {
            (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
            return Error{ErrorCode::IOError, "Failed to create plugin directory"};
        }

        // Find the actual plugin content (may be in a subdirectory)
        fs::path sourceDir = extractDir;
        for (const auto& entry : fs::directory_iterator(extractDir)) {
            if (entry.is_directory()) {
                // If there's exactly one subdirectory, use that
                sourceDir = entry.path();
                break;
            }
        }

        // Move extracted content to plugin directory
        fs::rename(sourceDir, pluginDir, ec);
        if (ec) {
            // If rename fails (cross-device), fall back to copy
            fs::copy(sourceDir, pluginDir, fs::copy_options::recursive, ec);
            if (ec) {
                (void)removeAllScoped(tempDir, tempRoot, "plugin temp");
                return Error{ErrorCode::IOError, "Failed to install plugin: " + ec.message()};
            }
        }

        result.installedPath = pluginDir;
        result.sizeBytes = 0;
        for (const auto& entry : fs::recursive_directory_iterator(pluginDir)) {
            if (entry.is_regular_file()) {
                result.sizeBytes += entry.file_size();
            }
        }

        // Cleanup temp directory
        (void)removeAllScoped(tempDir, tempRoot, "plugin temp");

        // Add to trust list
        if (options.autoTrust) {
            notifyProgress(InstallProgress::Stage::Trusting, "Adding to trust list...");
            addToTrustList(pluginDir);
        }

        // Load plugin (if daemon is available)
        if (options.autoLoad) {
            notifyProgress(InstallProgress::Stage::Loading, "Loading plugin...");
            // TODO: Implement daemon communication to load plugin
            // For now, the user can run `yams plugin load <name>` manually
        }

        notifyProgress(InstallProgress::Stage::Complete, "Installation complete!");

        auto endTime = std::chrono::steady_clock::now();
        result.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        return result;
    }

    Result<void> uninstall(const std::string& name, bool removeFromTrust) override {
        if (auto nameValidation = validatePluginName(name); !nameValidation) {
            return nameValidation.error();
        }
        fs::path pluginDir = installDir_ / name;

        if (!fs::exists(pluginDir)) {
            return Error{ErrorCode::NotFound, "Plugin not installed: " + name};
        }

        if (removeFromTrust) {
            removeFromTrustList(pluginDir);
        }

        auto removeRes = removeAllScoped(pluginDir, installDir_, "plugin install");
        if (!removeRes) {
            return removeRes.error();
        }

        return {};
    }

    Result<std::vector<std::string>> listInstalled() override {
        std::vector<std::string> installed;

        if (!fs::exists(installDir_)) {
            return installed;
        }

        for (const auto& entry : fs::directory_iterator(installDir_)) {
            if (entry.is_directory()) {
                installed.push_back(entry.path().filename().string());
            }
        }

        return installed;
    }

    Result<std::optional<std::string>> installedVersion(const std::string& name) override {
        if (auto nameValidation = validatePluginName(name); !nameValidation) {
            return nameValidation.error();
        }
        fs::path pluginDir = installDir_ / name;

        if (!fs::exists(pluginDir)) {
            return std::optional<std::string>(std::nullopt);
        }

        // Try to read version from manifest.json
        fs::path manifestPath = pluginDir / "manifest.json";
        if (fs::exists(manifestPath)) {
            std::ifstream file(manifestPath);
            if (file) {
                try {
                    nlohmann::json manifest;
                    file >> manifest;
                    return std::optional<std::string>(manifest.value("version", "unknown"));
                } catch (...) {
                    // Malformed manifests fall through to the conservative "unknown" version.
                }
            }
        }

        return std::optional<std::string>("unknown");
    }

    Result<std::map<std::string, std::string>> checkUpdates(const std::string& name) override {
        std::map<std::string, std::string> updates;

        auto installed = listInstalled();
        if (!installed) {
            return installed.error();
        }

        for (const auto& pluginName : installed.value()) {
            if (!name.empty() && pluginName != name) {
                continue;
            }

            auto currentVer = installedVersion(pluginName);
            if (!currentVer || !currentVer.value()) {
                continue;
            }

            auto remoteInfo = repoClient_->get(pluginName);
            if (!remoteInfo) {
                continue; // Plugin not in repository
            }

            if (remoteInfo.value().version != *currentVer.value()) {
                updates[pluginName] = remoteInfo.value().version;
            }
        }

        return updates;
    }

private:
    Result<void> downloadFile(const std::string& url, const fs::path& destPath,
                              const InstallOptions& options) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Error{ErrorCode::NetworkError, "Failed to initialize cURL"};
        }

        std::ofstream file(destPath, std::ios::binary);
        if (!file) {
            curl_easy_cleanup(curl);
            return Error{ErrorCode::IOError, "Failed to create file: " + destPath.string()};
        }

        ProgressData progressData;
        progressData.callback = options.onProgress;
        progressData.progress.stage = InstallProgress::Stage::Downloading;
        progressData.progress.message = "Downloading...";

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlWriteToFile);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, curlProgressCallback);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &progressData);

        CURLcode res = curl_easy_perform(curl);
        file.close();
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            fs::remove(destPath);
            return Error{ErrorCode::NetworkError,
                         std::string("Download failed: ") + curl_easy_strerror(res)};
        }

        return {};
    }

    void addToTrustList(const fs::path& pluginPath) {
        auto loadResult = yams::daemon::plugin_trust::loadTrustStore(
            trustFile_, yams::config::get_daemon_plugin_trust_file(),
            yams::config::get_legacy_plugin_trust_file());
        auto trusted = std::move(loadResult.entries);
        trusted.insert(yams::daemon::plugin_trust::normalizePath(pluginPath));

        if (!yams::daemon::plugin_trust::writeTrustStore(trustFile_, trusted)) {
            spdlog::warn("Failed to persist plugin trust list '{}'.", trustFile_.string());
        }
    }

    void removeFromTrustList(const fs::path& pluginPath) {
        auto loadResult = yams::daemon::plugin_trust::loadTrustStore(
            trustFile_, yams::config::get_daemon_plugin_trust_file(),
            yams::config::get_legacy_plugin_trust_file());
        if (!loadResult.loaded && !fs::exists(trustFile_)) {
            return;
        }

        auto trusted = std::move(loadResult.entries);
        trusted.erase(yams::daemon::plugin_trust::normalizePath(pluginPath));

        if (!yams::daemon::plugin_trust::writeTrustStore(trustFile_, trusted)) {
            spdlog::warn("Failed to update plugin trust list '{}' during removal.",
                         trustFile_.string());
        }
    }

    std::shared_ptr<IPluginRepoClient> repoClient_;
    fs::path installDir_;
    fs::path trustFile_;
};

std::unique_ptr<IPluginInstaller> makePluginInstaller(std::shared_ptr<IPluginRepoClient> repoClient,
                                                      const fs::path& installDir,
                                                      const fs::path& trustFile) {
    return std::make_unique<PluginInstallerImpl>(std::move(repoClient), installDir, trustFile);
}

fs::path getDefaultPluginInstallDir() {
    // Check environment variable first
    if (const char* envDir = std::getenv("YAMS_PLUGIN_DIR")) {
        return fs::path(envDir);
    }

#ifdef _WIN32
    // Windows: %LOCALAPPDATA%/yams/plugins
    wchar_t* localAppData = nullptr;
    if (SUCCEEDED(SHGetKnownFolderPath(FOLDERID_LocalAppData, 0, nullptr, &localAppData))) {
        fs::path result = fs::path(localAppData) / "yams" / "plugins";
        CoTaskMemFree(localAppData);
        return result;
    }
    return fs::path(std::getenv("LOCALAPPDATA")) / "yams" / "plugins";
#else
    // Unix: ~/.local/lib/yams/plugins
    fs::path homeDir;
    if (const char* home = std::getenv("HOME")) {
        homeDir = home;
    } else {
        struct passwd* pw = getpwuid(getuid());
        if (pw) {
            homeDir = pw->pw_dir;
        } else {
            homeDir = "/tmp";
        }
    }
    return homeDir / ".local" / "lib" / "yams" / "plugins";
#endif
}

fs::path getDefaultPluginTrustFile() {
    return yams::config::get_daemon_plugin_trust_file();
}

} // namespace yams::plugins
