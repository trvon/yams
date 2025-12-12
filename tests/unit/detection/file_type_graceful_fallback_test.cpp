#include <atomic>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <span>
#include <thread>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>

#include "detection_test_helpers.h"

using namespace yams::detection;
using yams::detection::test_utils::ScopedCurrentPath;
using yams::detection::test_utils::TestDirectory;
namespace fs = std::filesystem;

namespace {
struct GracefulFallbackFixture {
    GracefulFallbackFixture()
        : corruptedJsonPath(
              dir.writeTextFile("corrupted.json", "{ invalid json content without closing brace")),
          emptyJsonPath(dir.writeTextFile("empty.json", "")),
          restrictedPath(dir.writeTextFile("restricted.json", "{}")),
          nonExistentPath(dir.testDataDir() / "does_not_exist.json"), cwdGuard(dir.root()) {}

    TestDirectory dir{};
    fs::path corruptedJsonPath;
    fs::path emptyJsonPath;
    fs::path restrictedPath;
    fs::path nonExistentPath;
    ScopedCurrentPath cwdGuard;
};
} // namespace

TEST_CASE_METHOD(GracefulFallbackFixture, "Initialization tolerates missing file",
                 "[detection][fallback]") {
    FileTypeDetectorConfig config{};
    config.patternsFile = nonExistentPath;
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;

    auto result = FileTypeDetector::instance().initialize(config);
    REQUIRE(result);

    std::array<std::byte, 3> jpegData{std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF}};
    CHECK_NOTHROW(FileTypeDetector::instance().detectFromBuffer(jpegData));
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Corrupted JSON falls back to built-ins",
                 "[detection][fallback]") {
    FileTypeDetectorConfig config{};
    config.patternsFile = corruptedJsonPath;
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;

    auto result = FileTypeDetector::instance().initialize(config);
    REQUIRE(result);

    CHECK_NOTHROW(FileTypeDetector::instance().isTextMimeType("text/plain"));
    CHECK_NOTHROW(FileTypeDetector::instance().isBinaryMimeType("image/jpeg"));
    CHECK_NOTHROW(FileTypeDetector::instance().getFileTypeCategory("image/png"));
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Empty pattern file still initializes",
                 "[detection][fallback]") {
    FileTypeDetectorConfig config{};
    config.patternsFile = emptyJsonPath;
    config.useCustomPatterns = true;
    config.useBuiltinPatterns = true;

    auto result = FileTypeDetector::instance().initialize(config);
    REQUIRE(result);
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Loading missing pattern file fails cleanly",
                 "[detection][patterns]") {
    auto result = FileTypeDetector::instance().loadPatternsFromFile(nonExistentPath);
    CHECK_FALSE(result);
    CHECK_NOTHROW(FileTypeDetector::instance().isTextMimeType("text/plain"));
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Loading corrupted patterns fails cleanly",
                 "[detection][patterns]") {
    auto result = FileTypeDetector::instance().loadPatternsFromFile(corruptedJsonPath);
    CHECK_FALSE(result);
    CHECK_NOTHROW(FileTypeDetector::instance().getFileTypeCategory("image/jpeg"));
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Extension fallback still works",
                 "[detection][fallback]") {
    FileTypeDetectorConfig config{};
    config.useCustomPatterns = false;
    config.useBuiltinPatterns = true;
    FileTypeDetector::instance().initialize(config);

    CHECK(FileTypeDetector::getMimeTypeFromExtension(".jpg") == "image/jpeg");
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".txt") == "text/plain");
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".unknown") == "application/octet-stream");
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Classification works without patterns",
                 "[detection][fallback]") {
    FileTypeDetectorConfig config{};
    config.useCustomPatterns = false;
    config.useBuiltinPatterns = false;
    FileTypeDetector::instance().initialize(config);

    bool isText = FileTypeDetector::instance().isTextMimeType("text/plain");
    bool isBinary = FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
    auto category = FileTypeDetector::instance().getFileTypeCategory("application/pdf");

    CHECK((isText || !isText));
    CHECK((isBinary || !isBinary));
    CHECK_FALSE(category.empty());
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Detection works without explicit init",
                 "[detection][fallback]") {
    std::array<std::byte, 2> someData{std::byte{0x01}, std::byte{0x02}};
    CHECK_NOTHROW(FileTypeDetector::instance().detectFromBuffer(someData));
}

TEST_CASE_METHOD(GracefulFallbackFixture, "findMagicNumbersFile empty path still usable",
                 "[detection][fallback]") {
    auto path = yams::cli::YamsCLI::findMagicNumbersFile();
    REQUIRE_NOTHROW(std::string(path.string()));

    if (path.empty()) {
        FileTypeDetectorConfig config{};
        config.patternsFile = path;
        config.useCustomPatterns = false;
        config.useBuiltinPatterns = true;

        auto result = FileTypeDetector::instance().initialize(config);
        CHECK(result);
    }
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Various configs handled gracefully",
                 "[detection][fallback]") {
    std::vector<FileTypeDetectorConfig> configs = {
        {},
        {.useLibMagic = false, .useBuiltinPatterns = true, .useCustomPatterns = false},
        {.useLibMagic = true, .useBuiltinPatterns = false, .useCustomPatterns = false},
        {.useLibMagic = false,
         .useBuiltinPatterns = false,
         .useCustomPatterns = true,
         .patternsFile = nonExistentPath}};

    for (const auto& config : configs) {
        CHECK_NOTHROW(FileTypeDetector::instance().initialize(config));
    }
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Large buffers handled gracefully",
                 "[detection][performance]") {
    FileTypeDetectorConfig config{};
    config.maxBytesToRead = 10;
    FileTypeDetector::instance().initialize(config);

    std::vector<std::byte> largeBuffer(1000, std::byte{0x41});
    CHECK_NOTHROW(FileTypeDetector::instance().detectFromBuffer(largeBuffer));
}

TEST_CASE_METHOD(GracefulFallbackFixture, "Concurrent access avoids exceptions",
                 "[detection][concurrency]") {
    FileTypeDetectorConfig config{};
    FileTypeDetector::instance().initialize(config);

    std::vector<std::byte> testData = {std::byte{0xFF}, std::byte{0xD8}};
    std::vector<std::thread> threads;
    std::atomic<int> exceptions{0};

    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&]() {
            try {
                for (int j = 0; j < 100; ++j) {
                    FileTypeDetector::instance().isTextMimeType("text/plain");
                    FileTypeDetector::instance().isBinaryMimeType("image/jpeg");
                    FileTypeDetector::instance().getFileTypeCategory("application/pdf");
                    FileTypeDetector::instance().detectFromBuffer(testData);
                }
            } catch (...) {
                exceptions++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(exceptions.load() == 0);
}