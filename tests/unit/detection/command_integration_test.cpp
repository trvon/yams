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
    struct ExpectedFile {
        fs::path path;
        const char* mimeType;
        const char* category;
        bool isBinary;
    };

    CommandIntegrationFixture() : cwdGuard(dir.root()) {
        magicFile =
            dir.createDataFile("data/magic_numbers.json", R"({"version":"1.0","patterns":[]})");

        // Include a complete PDF signature, not the ambiguous four-byte "%PDF" prefix.
        createFile("document.pdf",
                   {std::byte{0x25}, std::byte{0x50}, std::byte{0x44}, std::byte{0x46},
                    std::byte{0x2D}, std::byte{0x31}, std::byte{0x2E}, std::byte{0x37},
                    std::byte{0x0A}},
                   "application/pdf", "document", true);
        createFile("image.jpg", {std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF}}, "image/jpeg",
                   "image", true);
        createFile("text.txt", {std::byte{0x48}, std::byte{0x65}, std::byte{0x6C}, std::byte{0x6C}},
                   "text/plain", "text", false);

        cli = std::make_unique<YamsCLI>();
        cli->setDataPath(dir.testDataDir());
    }

    ~CommandIntegrationFixture() { FileTypeDetector::instance().clearCache(); }

    void createFile(const std::string& name, std::initializer_list<std::byte> bytes,
                    const char* mimeType, const char* category, bool isBinary) {
        auto path =
            dir.createBinaryFile(name, std::span<const std::byte>(bytes.begin(), bytes.size()));
        std::ofstream file(path, std::ios::app);
        file << " additional content for testing";
        testFiles.push_back({path, mimeType, category, isBinary});
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
    fs::path magicFile;
    std::vector<ExpectedFile> testFiles;
};
} // namespace

TEST_CASE_METHOD(CommandIntegrationFixture, "findMagicNumbersFile resolves local data",
                 "[detection][cli]") {
    auto foundPath = YamsCLI::findMagicNumbersFile();
    REQUIRE_FALSE(foundPath.empty());
    REQUIRE(fs::is_regular_file(foundPath));
    CHECK(fs::equivalent(foundPath, magicFile));
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
    auto normalizedPathString = [](const fs::path& path) {
        std::error_code ec;
        auto resolved = fs::weakly_canonical(path, ec);
        if (ec) {
            resolved = path.lexically_normal();
        }
        auto normalized = resolved.generic_string();
        std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return normalized;
    };
    CHECK(normalizedPathString(foundPath) == normalizedPathString(localMagic));
#else
    // Linux: direct comparison
    CHECK(foundPath == localMagic);
#endif
}

TEST_CASE_METHOD(CommandIntegrationFixture, "detector initializes from CLI config",
                 "[detection][cli]") {
    auto config = detectorConfig();
    auto result = FileTypeDetector::instance().initialize(config);
    REQUIRE(result);

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
    REQUIRE(initResult);

    auto category = FileTypeDetector::instance().getFileTypeCategory("image/jpeg");
    CHECK_FALSE(category.empty());
}

TEST_CASE_METHOD(CommandIntegrationFixture, "file detection handles CLI files",
                 "[detection][cli]") {
    REQUIRE(FileTypeDetector::instance().initialize(detectorConfig()));

    for (const auto& expected : testFiles) {
        CAPTURE(expected.path);
        auto result = FileTypeDetector::instance().detectFromFile(expected.path);
        REQUIRE(result);
        CHECK(result.value().mimeType == expected.mimeType);
        CHECK(result.value().fileType == expected.category);
        CHECK(result.value().isBinary == expected.isBinary);
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "stats-style filtering uses classification",
                 "[detection][cli]") {
    REQUIRE(FileTypeDetector::instance().initialize(detectorConfig()));

    for (const auto& expected : testFiles) {
        CAPTURE(expected.path);
        auto mimeType =
            FileTypeDetector::getMimeTypeFromExtension(expected.path.extension().string());
        CHECK(mimeType == expected.mimeType);
        CHECK(FileTypeDetector::instance().getFileTypeCategory(mimeType) == expected.category);
        CHECK(FileTypeDetector::instance().isBinaryMimeType(mimeType) == expected.isBinary);
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture, "list command filtering remains consistent",
                 "[detection][cli]") {
    REQUIRE(FileTypeDetector::instance().initialize(detectorConfig()));

    for (const auto& expected : testFiles) {
        CAPTURE(expected.mimeType);
        CHECK(FileTypeDetector::instance().isTextMimeType(expected.mimeType) == !expected.isBinary);
        CHECK(FileTypeDetector::instance().isBinaryMimeType(expected.mimeType) ==
              expected.isBinary);
        CHECK(FileTypeDetector::instance().getFileTypeCategory(expected.mimeType) ==
              expected.category);
    }
    CHECK(FileTypeDetector::instance().isTextMimeType("application/json"));
    CHECK_FALSE(FileTypeDetector::instance().isBinaryMimeType("application/json"));
}

TEST_CASE_METHOD(CommandIntegrationFixture, "get command filtering aligns with detection",
                 "[detection][cli]") {
    REQUIRE(FileTypeDetector::instance().initialize(detectorConfig()));

    for (const auto& expected : testFiles) {
        CAPTURE(expected.path);
        REQUIRE(fs::is_regular_file(expected.path));
        auto detectResult = FileTypeDetector::instance().detectFromFile(expected.path);
        REQUIRE(detectResult);
        const auto& sig = detectResult.value();
        CHECK(sig.mimeType == expected.mimeType);
        CHECK(sig.isBinary == expected.isBinary);
        CHECK(sig.fileType == expected.category);
        CHECK(FileTypeDetector::instance().isTextMimeType(sig.mimeType) == !expected.isBinary);
    }
}

TEST_CASE_METHOD(CommandIntegrationFixture,
                 "missing optional patterns preserve MIME classification", "[detection][cli]") {
    FileTypeDetectorConfig badConfig{};
    badConfig.useLibMagic = false;
    badConfig.useBuiltinPatterns = false;
    badConfig.useCustomPatterns = true;
    badConfig.patternsFile = dir.root() / "missing" / "magic_numbers.json";
    REQUIRE_FALSE(fs::exists(badConfig.patternsFile));

    // A missing optional pattern file is tolerated; extension classification remains usable.
    auto result = FileTypeDetector::instance().initialize(badConfig);
    REQUIRE(result);

    auto mime = FileTypeDetector::getMimeTypeFromExtension(".txt");
    CHECK(mime == "text/plain");

    auto isText = FileTypeDetector::instance().isTextMimeType("text/plain");
    auto isBinary = FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
    CHECK(isText);
    CHECK(isBinary);
}
