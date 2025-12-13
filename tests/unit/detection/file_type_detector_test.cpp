#include <array>
#include <catch2/catch_test_macros.hpp>
#include <yams/detection/file_type_detector.h>

#include "detection_test_helpers.h"

using namespace yams::detection;
using yams::detection::test_utils::ScopedCurrentPath;
using yams::detection::test_utils::TestDirectory;

namespace {
struct DetectorFixture {
    DetectorFixture() = default;
    ~DetectorFixture() = default;

    FileTypeDetectorConfig configWithCustomPatterns() const {
        FileTypeDetectorConfig config{};
        config.patternsFile = dir.validJson();
        config.useCustomPatterns = true;
        config.useBuiltinPatterns = true;
        return config;
    }

    FileTypeDetectorConfig configWithRepoFallback() const {
        FileTypeDetectorConfig config{};
        auto repoMagic = dir.repoMagicNumbers();
        if (!repoMagic.empty()) {
            config.patternsFile = repoMagic;
            config.useCustomPatterns = true;
        } else {
            config.useCustomPatterns = false;
        }
        config.useBuiltinPatterns = true;
        return config;
    }

    TestDirectory dir{};
    FileTypeDetector& detector = FileTypeDetector::instance();
};
} // namespace

TEST_CASE_METHOD(DetectorFixture, "Initialize succeeds with valid config", "[detection][init]") {
    auto config = configWithCustomPatterns();
    auto result = detector.initialize(config);
    REQUIRE(result);
}

TEST_CASE_METHOD(DetectorFixture, "Initialize uses built-in patterns when file missing",
                 "[detection][init]") {
    auto config = configWithCustomPatterns();
    config.patternsFile = dir.nonExistentJson();

    auto result = detector.initialize(config);
    REQUIRE(result);
}

TEST_CASE_METHOD(DetectorFixture, "Buffer detection succeeds for known formats",
                 "[detection][buffer]") {
    auto config = configWithRepoFallback();
    REQUIRE(detector.initialize(config));

    SECTION("JPEG buffer") {
        std::array<std::byte, 4> jpegData{std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF},
                                          std::byte{0xE0}};

        auto result = detector.detectFromBuffer(jpegData);
        REQUIRE(result);
        const auto sig = result.value();
        CHECK(sig.mimeType == "image/jpeg");
        CHECK(sig.fileType == "image");
        CHECK(sig.isBinary);
    }

    SECTION("PNG buffer") {
        std::array<std::byte, 8> pngData{std::byte{0x89}, std::byte{0x50}, std::byte{0x4E},
                                         std::byte{0x47}, std::byte{0x0D}, std::byte{0x0A},
                                         std::byte{0x1A}, std::byte{0x0A}};

        auto result = detector.detectFromBuffer(pngData);
        REQUIRE(result);
        const auto sig = result.value();
        CHECK(sig.mimeType == "image/png");
        CHECK(sig.fileType == "image");
        CHECK(sig.isBinary);
    }
}

TEST_CASE_METHOD(DetectorFixture, "Empty buffer returns no detection", "[detection][buffer]") {
    auto result = detector.detectFromBuffer(std::span<const std::byte>{});
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DetectorFixture, "File detection works for known types", "[detection][file]") {
    auto config = configWithCustomPatterns();
    REQUIRE(detector.initialize(config));

    auto jpegPath = dir.createBinaryFile(
        "test.jpg", std::array<std::byte, 4>{std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF},
                                             std::byte{0xE0}});

    auto result = detector.detectFromFile(jpegPath);
    REQUIRE(result);
    const auto sig = result.value();
    CHECK(sig.mimeType == "image/jpeg");
    CHECK(sig.fileType == "image");
}

TEST_CASE_METHOD(DetectorFixture, "File detection handles missing files", "[detection][file]") {
    auto result = detector.detectFromFile(dir.nonExistentJson());
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DetectorFixture, "Classification utilities report mime details",
                 "[detection][classification]") {
    auto config = configWithCustomPatterns();
    REQUIRE(detector.initialize(config));

    CHECK(detector.isTextMimeType("application/json"));
    CHECK(detector.isTextMimeType("text/plain"));
    CHECK_FALSE(detector.isTextMimeType("image/jpeg"));
    CHECK_FALSE(detector.isTextMimeType("application/pdf"));

    CHECK(detector.isBinaryMimeType("image/jpeg"));
    CHECK(detector.isBinaryMimeType("application/pdf"));
    CHECK_FALSE(detector.isBinaryMimeType("application/json"));
    CHECK_FALSE(detector.isBinaryMimeType("text/plain"));

    CHECK(detector.getFileTypeCategory("image/jpeg") == "image");
    CHECK(detector.getFileTypeCategory("application/pdf") == "document");
    CHECK(detector.getFileTypeCategory("application/json") == "text");
    CHECK(detector.getFileTypeCategory("application/zip") == "archive");
}

TEST_CASE("Extension-based mime fallback", "[detection][classification]") {
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".jpg") == "image/jpeg");
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".png") == "image/png");
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".pdf") == "application/pdf");
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".txt") == "text/plain");
    CHECK(FileTypeDetector::getMimeTypeFromExtension(".unknown") == "application/octet-stream");
}

TEST_CASE_METHOD(DetectorFixture, "Cache statistics are reported", "[detection][cache]") {
    auto config = configWithCustomPatterns();
    config.cacheResults = true;
    config.cacheSize = 10;
    REQUIRE(detector.initialize(config));

    std::array<std::byte, 4> jpegData{std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF},
                                      std::byte{0xE0}};

    auto result1 = detector.detectFromBuffer(jpegData);
    REQUIRE(result1);

    auto result2 = detector.detectFromBuffer(jpegData);
    REQUIRE(result2);
    CHECK(result1.value().mimeType == result2.value().mimeType);

    auto cacheStats = detector.getCacheStats();
    CHECK(cacheStats.hits > 0);
    CHECK(cacheStats.entries > 0);
}

TEST_CASE_METHOD(DetectorFixture, "Cache can be cleared", "[detection][cache]") {
    auto config = configWithCustomPatterns();
    config.cacheResults = true;
    REQUIRE(detector.initialize(config));

    std::array<std::byte, 3> jpegData{std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF}};
    detector.detectFromBuffer(jpegData);

    std::array<std::byte, 8> pngData{std::byte{0x89}, std::byte{0x50}, std::byte{0x4E},
                                     std::byte{0x47}, std::byte{0x0D}, std::byte{0x0A},
                                     std::byte{0x1A}, std::byte{0x0A}};
    detector.detectFromBuffer(pngData);

    auto statsBefore = detector.getCacheStats();
    if (statsBefore.entries == 0) {
        SKIP("Cache functionality not available or not populated in this environment");
    }

    detector.clearCache();
    auto statsAfter = detector.getCacheStats();
    CHECK(statsAfter.entries == 0);
}

TEST_CASE_METHOD(DetectorFixture, "Patterns load from JSON", "[detection][patterns]") {
    auto loadResult = detector.loadPatternsFromFile(dir.validJson());
    REQUIRE(loadResult);

    auto patterns = detector.getPatterns();
    REQUIRE_FALSE(patterns.empty());
}

TEST_CASE("Hex helpers convert values", "[detection][utilities]") {
    auto bytes = FileTypeDetector::hexToBytes("FFD8FF");
    REQUIRE(bytes);
    const auto vec = bytes.value();
    REQUIRE(vec.size() == 3);
    CHECK(vec.at(0) == std::byte{0xFF});
    CHECK(vec.at(1) == std::byte{0xD8});
    CHECK(vec.at(2) == std::byte{0xFF});

    auto invalid = FileTypeDetector::hexToBytes("INVALID");
    CHECK_FALSE(invalid);

    std::vector<std::byte> expected{std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF}};
    CHECK(FileTypeDetector::bytesToHex(expected) == "ffd8ff");
}