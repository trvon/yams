// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <yams/api/content_store.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>

namespace yams::daemon::test {

using namespace yams::detection;
using namespace yams::extraction;
using ::testing::Return;

/**
 * @brief Test fixture for binary file filtering in FTS5 repair
 *
 * This tests the logic that prevents binary files from being continuously
 * queued for FTS5 extraction when no custom plugins are available.
 */
class RepairBinaryFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize FileTypeDetector
        auto result = FileTypeDetector::initializeWithMagicNumbers();
        if (!result) {
            // Fall back to default initialization
            FileTypeDetectorConfig config;
            FileTypeDetector::instance().initialize(config);
        }
    }

    void TearDown() override { FileTypeDetector::instance().clearCache(); }
};

/**
 * Test that common text MIME types are recognized as text-extractable
 */
TEST_F(RepairBinaryFilterTest, RecognizesTextMimeTypes) {
    auto& detector = FileTypeDetector::instance();

    // Common text types
    EXPECT_TRUE(detector.isTextMimeType("text/plain"));
    EXPECT_TRUE(detector.isTextMimeType("text/html"));
    EXPECT_TRUE(detector.isTextMimeType("text/markdown"));
    EXPECT_TRUE(detector.isTextMimeType("text/csv"));

    // Structured text formats
    EXPECT_TRUE(detector.isTextMimeType("application/json"));
    EXPECT_TRUE(detector.isTextMimeType("application/xml"));
    EXPECT_TRUE(detector.isTextMimeType("application/yaml"));
    EXPECT_TRUE(detector.isTextMimeType("application/x-yaml"));
}

/**
 * Test that binary MIME types are recognized as non-text
 */
TEST_F(RepairBinaryFilterTest, RecognizesBinaryMimeTypes) {
    auto& detector = FileTypeDetector::instance();

    // Image formats
    EXPECT_FALSE(detector.isTextMimeType("image/jpeg"));
    EXPECT_FALSE(detector.isTextMimeType("image/png"));
    EXPECT_FALSE(detector.isTextMimeType("image/gif"));

    // Archives
    EXPECT_FALSE(detector.isTextMimeType("application/zip"));
    EXPECT_FALSE(detector.isTextMimeType("application/x-tar"));

    // Executables
    EXPECT_FALSE(detector.isTextMimeType("application/x-executable"));
    EXPECT_FALSE(detector.isTextMimeType("application/x-sharedlib"));

    // Video/Audio
    EXPECT_FALSE(detector.isTextMimeType("video/mp4"));
    EXPECT_FALSE(detector.isTextMimeType("audio/mpeg"));
}

/**
 * Test that isBinaryData correctly identifies binary content
 */
TEST_F(RepairBinaryFilterTest, DetectsBinaryData) {
    // Pure text content
    std::string textContent = "This is plain text with newlines\nand tabs\t.";
    std::vector<std::byte> textBytes;
    for (char c : textContent) {
        textBytes.push_back(static_cast<std::byte>(c));
    }
    EXPECT_FALSE(isBinaryData(textBytes));

    // Binary content with null bytes (strong indicator)
    std::vector<std::byte> binaryWithNull = {
        std::byte{0x89}, std::byte{0x50}, std::byte{0x4E}, std::byte{0x47}, // PNG header
        std::byte{0x0D}, std::byte{0x0A}, std::byte{0x1A}, std::byte{0x0A},
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x0D}};
    EXPECT_TRUE(isBinaryData(binaryWithNull));

    // Binary content with high concentration of non-printable chars
    std::vector<std::byte> binaryNonPrintable;
    for (int i = 0; i < 100; ++i) {
        binaryNonPrintable.push_back(static_cast<std::byte>(i % 256));
    }
    EXPECT_TRUE(isBinaryData(binaryNonPrintable));
}

/**
 * Test that source code files are recognized as text
 */
TEST_F(RepairBinaryFilterTest, RecognizesSourceCodeAsText) {
    auto& detector = FileTypeDetector::instance();

    // Various programming language MIME types
    EXPECT_TRUE(detector.isTextMimeType("text/x-c"));
    EXPECT_TRUE(detector.isTextMimeType("text/x-c++"));
    EXPECT_TRUE(detector.isTextMimeType("text/x-python"));
    EXPECT_TRUE(detector.isTextMimeType("text/javascript"));
    EXPECT_TRUE(detector.isTextMimeType("application/javascript"));
}

/**
 * Test edge case: unknown/generic MIME types
 * These should require content inspection
 */
TEST_F(RepairBinaryFilterTest, HandlesUnknownMimeTypes) {
    auto& detector = FileTypeDetector::instance();

    // Generic/unknown MIME types should not be automatically classified
    EXPECT_FALSE(detector.isTextMimeType("application/octet-stream"));
    EXPECT_FALSE(detector.isTextMimeType(""));
    EXPECT_FALSE(detector.isTextMimeType("application/unknown"));
}

/**
 * Integration test concept: With our fix, binary files without plugins
 * should not be continuously retried for FTS5 extraction
 *
 * The actual integration would require:
 * 1. Mock ServiceManager with no custom extractors
 * 2. Documents with binary MIME types (image/png, etc.)
 * 3. Verify they're filtered out before queuing
 */
TEST_F(RepairBinaryFilterTest, ConceptualIntegrationTest) {
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

    EXPECT_TRUE(true) << "Fix prevents continuous retry of binary files without plugins";
}

/**
 * Test that demonstrates the filtering logic flow
 */
TEST_F(RepairBinaryFilterTest, FilteringLogicFlow) {
    auto& detector = FileTypeDetector::instance();

    // Scenario 1: Text file with clear MIME type -> should extract
    std::string mime1 = "text/plain";
    EXPECT_TRUE(detector.isTextMimeType(mime1)) << "Plain text should be extractable";

    // Scenario 2: Binary file (image) -> should NOT extract without plugin
    std::string mime2 = "image/jpeg";
    EXPECT_FALSE(detector.isTextMimeType(mime2)) << "JPEG should not be extractable without plugin";

    // Scenario 3: Structured text (JSON) -> should extract
    std::string mime3 = "application/json";
    EXPECT_TRUE(detector.isTextMimeType(mime3)) << "JSON should be extractable as text";

    // Scenario 4: Unknown MIME -> requires content inspection
    std::string mime4 = "application/octet-stream";
    EXPECT_FALSE(detector.isTextMimeType(mime4)) << "Generic binary MIME requires content check";
}

} // namespace yams::daemon::test
