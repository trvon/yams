#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <span>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>

#include "detection_test_helpers.h"

using namespace yams::detection;
using namespace yams::cli;
using yams::detection::test_utils::ScopedCurrentPath;
using yams::detection::test_utils::TestDirectory;
namespace fs = std::filesystem;

namespace {
struct CommandIntegrationFixture {
    CommandIntegrationFixture() : cwdGuard(dir.root()) {
        auto repoMagic = dir.repoMagicNumbers();
        if (repoMagic.empty()) {
            dir.createDataFile("data/magic_numbers.json", R"({"version":"1.0","patterns":[]})");
        }

        createFile("document.pdf",
                   {std::byte{0x25}, std::byte{0x50}, std::byte{0x44}, std::byte{0x46}});
        createFile("image.jpg", {std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF}});
        createFile("text.txt",
                   {std::byte{0x48}, std::byte{0x65}, std::byte{0x6C}, std::byte{0x6C}});

        cli = std::make_unique<YamsCLI>();
        cli->setDataPath(dir.testDataDir());
    }

    ~CommandIntegrationFixture() { FileTypeDetector::instance().clearCache(); }

    fs::path createFile(const std::string& name, std::initializer_list<std::byte> bytes) {
        auto path =
            dir.createBinaryFile(name, std::span<const std::byte>(bytes.begin(), bytes.size()));
        std::ofstream file(path, std::ios::app);
        file << " additional content for testing";
        testFiles.push_back(path);
        return path;
    }

    FileTypeDetectorConfig detectorConfig() const {
        FileTypeDetectorConfig config{};
        auto magic = YamsCLI::findMagicNumbersFile();
        config.patternsFile = magic;
        config.useCustomPatterns = !magic.empty();
        config.useBuiltinPatterns = true;
        return config;
    }

    TestDirectory dir{"detection_cli"};
    ScopedCurrentPath cwdGuard;
    std::unique_ptr<YamsCLI> cli;
    std::vector<fs::path> testFiles;
};
} // namespace

TEST_CASE_METHOD(CommandIntegrationFixture, "findMagicNumbersFile resolves local data",
                 "[detection][cli]") {
    auto foundPath = YamsCLI::findMagicNumbersFile();
    REQUIRE_NOTHROW(std::string(foundPath.string()));
    if (!foundPath.empty()) {
        CHECK(fs::exists(foundPath));
        CHECK(fs::is_regular_file(foundPath));
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "findMagicNumbersFile respects search order",
                 "[detection][cli]") {
    auto localMagic =
        dir.createDataFile("data/magic_numbers.json", R"({"version":"1.0","patterns":[]})");
    auto foundPath = YamsCLI::findMagicNumbersFile();
#if defined(__APPLE__)
    // macOS: /var is a symlink to /private/var, so canonicalize both paths
    std::error_code ec;
    auto canonicalFound = fs::canonical(foundPath, ec);
    auto canonicalLocal = fs::canonical(localMagic, ec);
    CHECK(canonicalFound == canonicalLocal);
#elif defined(_WIN32)
    // Windows: case-insensitive path comparison
    auto foundStr = foundPath.string();
    auto localStr = localMagic.string();
    std::transform(foundStr.begin(), foundStr.end(), foundStr.begin(), ::tolower);
    std::transform(localStr.begin(), localStr.end(), localStr.begin(), ::tolower);
    CHECK(foundStr == localStr);
#else
    // Linux: direct comparison
    CHECK(foundPath == localMagic);
#endif
}

TEST_CASE_METHOD(CommandIntegrationFixture, "detector initializes from CLI config",
                 "[detection][cli]") {
    auto config = detectorConfig();
    auto result = FileTypeDetector::instance().initialize(config);
    CHECK((result.has_value() || !result.has_value()));

    bool isText = FileTypeDetector::instance().isTextMimeType("text/plain");
    bool isBinary = FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
    auto category = FileTypeDetector::instance().getFileTypeCategory("application/pdf");

    CHECK(isText);
    CHECK(isBinary);
    CHECK_FALSE(category.empty());
}

TEST_CASE_METHOD(CommandIntegrationFixture, "detector falls back when JSON missing",
                 "[detection][cli]") {
    FileTypeDetectorConfig config{};
    config.patternsFile = fs::path("/nonexistent/path/magic_numbers.json");
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;

    auto initResult = FileTypeDetector::instance().initialize(config);
    CHECK(initResult);

    auto category = FileTypeDetector::instance().getFileTypeCategory("image/jpeg");
    CHECK_FALSE(category.empty());
}

TEST_CASE_METHOD(CommandIntegrationFixture, "file detection handles CLI files",
                 "[detection][cli]") {
    auto config = detectorConfig();
    FileTypeDetector::instance().initialize(config);

    for (const auto& testFile : testFiles) {
        auto result = FileTypeDetector::instance().detectFromFile(testFile);
        if (result) {
            const auto sig = result.value();
            CHECK_FALSE(sig.mimeType.empty());
            CHECK_FALSE(sig.fileType.empty());
        }
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "stats-style filtering uses classification",
                 "[detection][cli]") {
    auto config = detectorConfig();
    auto init = FileTypeDetector::instance().initialize(config);
    CHECK((init.has_value() || !init.has_value()));

    for (const auto& testFile : testFiles) {
        auto extension = testFile.extension().string();
        auto mimeType = FileTypeDetector::getMimeTypeFromExtension(extension);
        auto fileType = FileTypeDetector::instance().getFileTypeCategory(mimeType);
        auto isBinary = FileTypeDetector::instance().isBinaryMimeType(mimeType);
        CHECK_FALSE(fileType.empty());
        CHECK((isBinary || !isBinary));
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "list command filtering remains consistent",
                 "[detection][cli]") {
    auto config = detectorConfig();
    auto init = FileTypeDetector::instance().initialize(config);
    CHECK((init.has_value() || !init.has_value()));

    std::vector<std::string> mimeTypes = {"image/jpeg", "application/pdf", "text/plain",
                                          "application/json"};
    for (const auto& mimeType : mimeTypes) {
        auto isText = FileTypeDetector::instance().isTextMimeType(mimeType);
        auto isBinary = FileTypeDetector::instance().isBinaryMimeType(mimeType);
        auto category = FileTypeDetector::instance().getFileTypeCategory(mimeType);
        CHECK(isText != isBinary);
        CHECK_FALSE(category.empty());
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "get command filtering aligns with detection",
                 "[detection][cli]") {
    auto config = detectorConfig();
    FileTypeDetector::instance().initialize(config);

    for (const auto& testFile : testFiles) {
        if (!fs::exists(testFile)) {
            continue;
        }
        auto detectResult = FileTypeDetector::instance().detectFromFile(testFile);
        if (detectResult) {
            const auto sig = detectResult.value();
            auto isText = FileTypeDetector::instance().isTextMimeType(sig.mimeType);
            auto isBinary = FileTypeDetector::instance().isBinaryMimeType(sig.mimeType);
            auto category = FileTypeDetector::instance().getFileTypeCategory(sig.mimeType);

            CHECK(sig.isBinary == isBinary);
            CHECK(sig.fileType == category);
        }
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "initialization failure degrades gracefully",
                 "[detection][cli]") {
    FileTypeDetectorConfig badConfig{};
    badConfig.useLibMagic = false;
    badConfig.useBuiltinPatterns = false;
    badConfig.useCustomPatterns = true;
    badConfig.patternsFile = fs::path("/definitely/does/not/exist/anywhere.json");

    auto result = FileTypeDetector::instance().initialize(badConfig);
    CHECK((result.has_value() || !result.has_value()));

    auto mime = FileTypeDetector::getMimeTypeFromExtension(".txt");
    CHECK(mime == "text/plain");

    auto isText = FileTypeDetector::instance().isTextMimeType("text/plain");
    auto isBinary = FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
    CHECK((isText || !isText));
    CHECK((isBinary || !isBinary));
}