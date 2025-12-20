// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <yams/api/content_store.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>

#include <cstddef>
#include <string>
#include <vector>

namespace yams::daemon::test {

using namespace yams::detection;
using namespace yams::extraction;

/**
 * @brief Test fixture for binary file filtering in FTS5 repair
 *
 * This tests the logic that prevents binary files from being continuously
 * queued for FTS5 extraction when no custom plugins are available.
 */
struct RepairBinaryFilterFixture {
    RepairBinaryFilterFixture() {
        // Initialize FileTypeDetector
        auto result = FileTypeDetector::initializeWithMagicNumbers();
        if (!result) {
            // Fall back to default initialization
            FileTypeDetectorConfig config;
            FileTypeDetector::instance().initialize(config);
        }
    }

    ~RepairBinaryFilterFixture() { FileTypeDetector::instance().clearCache(); }
};

TEST_CASE_METHOD(RepairBinaryFilterFixture, "RepairBinaryFilter: recognizes text MIME types",
                 "[daemon]") {
    auto& detector = FileTypeDetector::instance();

    SECTION("Common text types") {
        CHECK(detector.isTextMimeType("text/plain"));
        CHECK(detector.isTextMimeType("text/html"));
        CHECK(detector.isTextMimeType("text/markdown"));
        CHECK(detector.isTextMimeType("text/csv"));
    }

    SECTION("Structured text formats") {
        CHECK(detector.isTextMimeType("application/json"));
        CHECK(detector.isTextMimeType("application/xml"));
        CHECK(detector.isTextMimeType("application/yaml"));
        CHECK(detector.isTextMimeType("application/x-yaml"));
    }
}

TEST_CASE_METHOD(RepairBinaryFilterFixture, "RepairBinaryFilter: recognizes binary MIME types",
                 "[daemon]") {
    auto& detector = FileTypeDetector::instance();

    SECTION("Image formats") {
        CHECK_FALSE(detector.isTextMimeType("image/jpeg"));
        CHECK_FALSE(detector.isTextMimeType("image/png"));
        CHECK_FALSE(detector.isTextMimeType("image/gif"));
    }

    SECTION("Archives") {
        CHECK_FALSE(detector.isTextMimeType("application/zip"));
        CHECK_FALSE(detector.isTextMimeType("application/x-tar"));
    }

    SECTION("Executables") {
        CHECK_FALSE(detector.isTextMimeType("application/x-executable"));
        CHECK_FALSE(detector.isTextMimeType("application/x-sharedlib"));
    }

    SECTION("Video/Audio") {
        CHECK_FALSE(detector.isTextMimeType("video/mp4"));
        CHECK_FALSE(detector.isTextMimeType("audio/mpeg"));
    }
}

TEST_CASE_METHOD(RepairBinaryFilterFixture, "RepairBinaryFilter: detects binary data", "[daemon]") {
    SECTION("Pure text content is not detected as binary") {
        std::string textContent = "This is plain text with newlines\nand tabs\t.";
        std::vector<std::byte> textBytes;
        for (char c : textContent) {
            textBytes.push_back(static_cast<std::byte>(c));
        }
        CHECK_FALSE(isBinaryData(textBytes));
    }

    SECTION("Binary content with null bytes is detected as binary") {
        std::vector<std::byte> binaryWithNull = {
            std::byte{0x89}, std::byte{0x50}, std::byte{0x4E}, std::byte{0x47}, // PNG header
            std::byte{0x0D}, std::byte{0x0A}, std::byte{0x1A}, std::byte{0x0A},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x0D}};
        CHECK(isBinaryData(binaryWithNull));
    }

    SECTION("Binary content with high concentration of non-printable chars") {
        std::vector<std::byte> binaryNonPrintable;
        for (int i = 0; i < 100; ++i) {
            binaryNonPrintable.push_back(static_cast<std::byte>(i % 256));
        }
        CHECK(isBinaryData(binaryNonPrintable));
    }
}

TEST_CASE_METHOD(RepairBinaryFilterFixture, "RepairBinaryFilter: recognizes source code as text",
                 "[daemon]") {
    auto& detector = FileTypeDetector::instance();

    // Various programming language MIME types
    CHECK(detector.isTextMimeType("text/x-c"));
    CHECK(detector.isTextMimeType("text/x-c++"));
    CHECK(detector.isTextMimeType("text/x-python"));
    CHECK(detector.isTextMimeType("text/javascript"));
    CHECK(detector.isTextMimeType("application/javascript"));
}

TEST_CASE_METHOD(RepairBinaryFilterFixture, "RepairBinaryFilter: handles unknown MIME types",
                 "[daemon]") {
    auto& detector = FileTypeDetector::instance();

    // Generic/unknown MIME types should not be automatically classified
    CHECK_FALSE(detector.isTextMimeType("application/octet-stream"));
    CHECK_FALSE(detector.isTextMimeType(""));
    CHECK_FALSE(detector.isTextMimeType("application/unknown"));
}

TEST_CASE_METHOD(RepairBinaryFilterFixture,
                 "RepairBinaryFilter: conceptual integration test documents expected behavior",
                 "[daemon]") {
    // This test documents the expected behavior:
    //
    // BEFORE FIX:
    // - All documents with contentExtracted=false get queued for FTS5 repair
    // - Binary files (images, executables, etc.) fail extraction
    // - They remain in failed state and get re-queued continuously
    //
    // AFTER FIX:
    // - Documents are filtered using isTextLikeDocument() before queuing
    // - Binary files without custom plugins are skipped
    // - Only text-like files or files with matching plugins are queued
    // - This prevents wasted CPU cycles and log spam

    INFO("Fix prevents continuous retry of binary files without plugins");
    SUCCEED();
}

TEST_CASE_METHOD(RepairBinaryFilterFixture, "RepairBinaryFilter: filtering logic flow",
                 "[daemon]") {
    auto& detector = FileTypeDetector::instance();

    SECTION("Scenario 1: Text file with clear MIME type -> should extract") {
        std::string mime1 = "text/plain";
        CHECK(detector.isTextMimeType(mime1));
    }

    SECTION("Scenario 2: Binary file (image) -> should NOT extract without plugin") {
        std::string mime2 = "image/jpeg";
        CHECK_FALSE(detector.isTextMimeType(mime2));
    }

    SECTION("Scenario 3: Structured text (JSON) -> should extract") {
        std::string mime3 = "application/json";
        CHECK(detector.isTextMimeType(mime3));
    }

    SECTION("Scenario 4: Unknown MIME -> requires content inspection") {
        std::string mime4 = "application/octet-stream";
        CHECK_FALSE(detector.isTextMimeType(mime4));
    }
}

} // namespace yams::daemon::test
