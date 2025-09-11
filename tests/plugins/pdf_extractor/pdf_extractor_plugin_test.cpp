#include <gtest/gtest.h>
#include <yams/daemon/resource/abi_plugin_loader.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using yams::ErrorCode;
using yams::Result;

namespace {

// Return first directory that looks like the plugin build output.
// Order:
// 1) YAMS_PLUGIN_DIR env (if set)
// 2) CWD
// 3) CWD/plugins/pdf_extractor_build
// 4) CWD/../plugins/pdf_extractor_build
// 5) CWD/../../plugins/pdf_extractor_build
std::optional<fs::path> findCandidatePluginDir() {
    if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
        fs::path p(env);
        std::error_code ec;
        if (fs::exists(p, ec) && fs::is_directory(p, ec))
            return p;
    }

    std::vector<fs::path> candidates;
    const fs::path cwd = fs::current_path();
    candidates.emplace_back(cwd);
    candidates.emplace_back(cwd / "plugins" / "pdf_extractor_build");
    candidates.emplace_back(cwd.parent_path() / "plugins" / "pdf_extractor_build");
    candidates.emplace_back(cwd.parent_path().parent_path() / "plugins" / "pdf_extractor_build");

    std::error_code ec;
    for (const auto& c : candidates) {
        if (fs::exists(c, ec) && fs::is_directory(c, ec)) {
            return c;
        }
    }
    return std::nullopt;
}

// Find a plugin binary whose name contains "pdf_extractor" and extension is platform-appropriate.
std::optional<fs::path> findPdfExtractorBinary(const fs::path& dir) {
#if defined(_WIN32)
    const std::string ext = ".dll";
#elif defined(__APPLE__)
    const std::string ext = ".dylib";
#else
    const std::string ext = ".so";
#endif

    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (ec)
            break;
        if (!entry.is_regular_file(ec))
            continue;
        const auto p = entry.path();
        if (p.extension().string() == ext) {
            std::string file = p.filename().string();
            if (file.find("pdf_extractor") != std::string::npos) {
                return p;
            }
        }
    }
    return std::nullopt;
}

} // namespace

TEST(PdfExtractorPlugin, ScanLoadAndQuery) {
    using yams::daemon::AbiPluginLoader;

    auto pluginDirOpt = findCandidatePluginDir();
    ASSERT_TRUE(pluginDirOpt.has_value()) << "Could not locate candidate plugin dir. "
                                          << "Set YAMS_PLUGIN_DIR to override search.";
    const fs::path pluginDir = *pluginDirOpt;

    auto pluginFileOpt = findPdfExtractorBinary(pluginDir);
    ASSERT_TRUE(pluginFileOpt.has_value())
        << "No pdf_extractor plugin binary found in: " << pluginDir;
    const fs::path pluginFile = *pluginFileOpt;

    AbiPluginLoader loader;

    // Trust file stored under build tree or CWD (must not be world-writable; CWD/build dirs are
    // fine).
    const fs::path trustFile = fs::current_path() / "yams_test_trust_plugins.txt";
    loader.setTrustFile(trustFile);

    // Trust the directory containing the plugin (prefix-based trust).
    {
        auto tr = loader.trustAdd(pluginDir);
        ASSERT_TRUE(tr) << "trustAdd failed";
    }

    // Scan the directory and ensure we can see the plugin.
    auto scanRes = loader.scanDirectory(pluginDir);
    ASSERT_TRUE(scanRes) << "scanDirectory failed: " << scanRes.error().message;
    const auto scanned = scanRes.value();

    bool foundByName = false;
    for (const auto& sr : scanned) {
        if (sr.name == "pdf_extractor") {
            foundByName = true;
            // Basic manifest sanity: should mention content_extractor_v1
            EXPECT_NE(sr.manifestJson.find("content_extractor_v1"), std::string::npos)
                << "Manifest should contain content_extractor_v1 interface";
            break;
        }
    }
    EXPECT_TRUE(foundByName) << "pdf_extractor was not discovered by scanDirectory()";

    // Load the plugin
    auto loadRes = loader.load(pluginFile, "{}");
    ASSERT_TRUE(loadRes) << "Failed to load plugin: " << loadRes.error().message;
    EXPECT_EQ(loadRes.value().name, "pdf_extractor");
    EXPECT_EQ(loadRes.value().path, pluginFile);

    // Health is optional. Our plugin exposes the symbol but returns NOT_IMPLEMENTED.
    // The loader currently treats non-OK return as InternalError "Health query failed",
    // so accept either NotImplemented or InternalError.
    auto healthRes = loader.health("pdf_extractor");
    EXPECT_FALSE(healthRes);
    if (!healthRes) {
        EXPECT_TRUE(healthRes.error().code == ErrorCode::NotImplemented ||
                    healthRes.error().code == ErrorCode::InternalError)
            << "Unexpected health error code: " << static_cast<int>(healthRes.error().code)
            << " message: " << healthRes.error().message;
    }

    // Unload the plugin
    auto unloadRes = loader.unload("pdf_extractor");
    EXPECT_TRUE(unloadRes) << "Unload failed: " << unloadRes.error().message;
}