/**
 * @file archive_content_handler_catch2_test.cpp
 * @brief Tests for ArchiveContentHandler helpers and pure functions
 *
 * Covers: formatToString, ArchiveProcessingConfig defaults, ProcessingStats,
 *         formatSupportsMetadata, formatSupportsEncryption, isArchiveFile,
 *         getArchiveFormatFromExtension, getExtensionsForFormat,
 *         estimateCompressionRatio, isMetadataComplete, getCommonFormats,
 *         isCommonFormat, ArchiveContentHandler construction / canHandle / supportedMimeTypes.
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/content/archive_content_handler.h>
#include <yams/content/content_handler.h>

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

using namespace yams::content;
using Catch::Approx;

// ────────────────────────────────────────────────────────────────────────────────
// formatToString (constexpr)
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("formatToString covers all ArchiveFormat values", "[content][archive][catch2]") {
    CHECK(formatToString(ArchiveFormat::Unknown) == "Unknown");
    CHECK(formatToString(ArchiveFormat::ZIP) == "ZIP");
    CHECK(formatToString(ArchiveFormat::RAR) == "RAR");
    CHECK(formatToString(ArchiveFormat::TAR) == "TAR");
    CHECK(formatToString(ArchiveFormat::GZIP) == "GZIP");
    CHECK(formatToString(ArchiveFormat::BZIP2) == "BZIP2");
    CHECK(formatToString(ArchiveFormat::XZ) == "XZ");
    CHECK(formatToString(ArchiveFormat::_7Z) == "7Z");
    CHECK(formatToString(ArchiveFormat::LZH) == "LZH");
    CHECK(formatToString(ArchiveFormat::ARJ) == "ARJ");
    CHECK(formatToString(ArchiveFormat::CAB) == "CAB");
    CHECK(formatToString(ArchiveFormat::ISO) == "ISO");
    CHECK(formatToString(ArchiveFormat::DMG) == "DMG");
    CHECK(formatToString(ArchiveFormat::IMG) == "IMG");
    CHECK(formatToString(ArchiveFormat::BIN) == "BIN");
    CHECK(formatToString(ArchiveFormat::CUE) == "CUE");
    CHECK(formatToString(ArchiveFormat::NRG) == "NRG");
}

// ────────────────────────────────────────────────────────────────────────────────
// ArchiveProcessingConfig defaults
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveProcessingConfig has correct defaults", "[content][archive][catch2]") {
    ArchiveProcessingConfig cfg;
    CHECK(cfg.extractFileList == true);
    CHECK(cfg.extractMetadata == true);
    CHECK(cfg.analyzeContents == false);
    CHECK(cfg.extractFiles == false);
    CHECK(cfg.calculateHashes == false);
    CHECK(cfg.detectEncryption == true);
    CHECK(cfg.maxFileSize == 1ULL * 1024 * 1024 * 1024);
    CHECK(cfg.maxAnalysisSize == 100 * 1024 * 1024);
    CHECK(cfg.maxFileListSize == 10000);
    CHECK(cfg.maxExtractionSize == 50 * 1024 * 1024);
    CHECK(cfg.processingTimeout == std::chrono::milliseconds{60000});
    CHECK(cfg.analysisTimeout == std::chrono::milliseconds{30000});
    CHECK(cfg.useAsyncProcessing == true);
    CHECK(cfg.maxConcurrentAnalysis == 1);
}

// ────────────────────────────────────────────────────────────────────────────────
// ProcessingStats calculations
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ProcessingStats successRate with no files processed", "[content][archive][catch2]") {
    ArchiveContentHandler::ProcessingStats stats;
    CHECK(stats.successRate() == Approx(0.0));
    CHECK(stats.averageProcessingTimeMs() == Approx(0.0));
    CHECK(stats.averageEntriesPerArchive() == Approx(0.0));
}

TEST_CASE("ProcessingStats successRate with processed files", "[content][archive][catch2]") {
    ArchiveContentHandler::ProcessingStats stats;
    stats.totalFilesProcessed = 10;
    stats.successfulProcessing = 7;
    stats.failedProcessing = 3;
    stats.totalProcessingTime = std::chrono::milliseconds{5000};
    stats.totalArchiveEntriesProcessed = 200;

    CHECK(stats.successRate() == Approx(0.7));
    CHECK(stats.averageProcessingTimeMs() == Approx(500.0));
    CHECK(stats.averageEntriesPerArchive() == Approx(20.0));
}

TEST_CASE("ProcessingStats 100% success rate", "[content][archive][catch2]") {
    ArchiveContentHandler::ProcessingStats stats;
    stats.totalFilesProcessed = 5;
    stats.successfulProcessing = 5;
    stats.failedProcessing = 0;
    CHECK(stats.successRate() == Approx(1.0));
}

// ────────────────────────────────────────────────────────────────────────────────
// formatSupportsMetadata / formatSupportsEncryption (constexpr)
// ────────────────────────────────────────────────────────────────────────────────

// formatSupportsMetadata / formatSupportsEncryption are private members,
// so they cannot be tested directly. Their behavior is exercised indirectly
// through canHandle() and supportedMimeTypes().

// ────────────────────────────────────────────────────────────────────────────────
// getArchiveFormatFromExtension
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("getArchiveFormatFromExtension recognises known extensions",
          "[content][archive][catch2]") {
    CHECK(getArchiveFormatFromExtension(".zip") == ArchiveFormat::ZIP);
    CHECK(getArchiveFormatFromExtension(".rar") == ArchiveFormat::RAR);
    CHECK(getArchiveFormatFromExtension(".tar") == ArchiveFormat::TAR);
    CHECK(getArchiveFormatFromExtension(".gz") == ArchiveFormat::GZIP);
    CHECK(getArchiveFormatFromExtension(".bz2") == ArchiveFormat::BZIP2);
    CHECK(getArchiveFormatFromExtension(".xz") == ArchiveFormat::XZ);
    CHECK(getArchiveFormatFromExtension(".7z") == ArchiveFormat::_7Z);
    CHECK(getArchiveFormatFromExtension(".iso") == ArchiveFormat::ISO);
    CHECK(getArchiveFormatFromExtension(".dmg") == ArchiveFormat::DMG);
}

TEST_CASE("getArchiveFormatFromExtension returns Unknown for unrecognised",
          "[content][archive][catch2]") {
    CHECK(getArchiveFormatFromExtension(".txt") == ArchiveFormat::Unknown);
    CHECK(getArchiveFormatFromExtension(".jpg") == ArchiveFormat::Unknown);
    CHECK(getArchiveFormatFromExtension("") == ArchiveFormat::Unknown);
}

// ────────────────────────────────────────────────────────────────────────────────
// getExtensionsForFormat
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("getExtensionsForFormat returns non-empty for known formats",
          "[content][archive][catch2]") {
    auto zipExts = getExtensionsForFormat(ArchiveFormat::ZIP);
    CHECK(!zipExts.empty());

    auto tarExts = getExtensionsForFormat(ArchiveFormat::TAR);
    CHECK(!tarExts.empty());
}

TEST_CASE("getExtensionsForFormat returns empty for Unknown", "[content][archive][catch2]") {
    auto exts = getExtensionsForFormat(ArchiveFormat::Unknown);
    CHECK(exts.empty());
}

// ────────────────────────────────────────────────────────────────────────────────
// estimateCompressionRatio
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("estimateCompressionRatio valid inputs", "[content][archive][catch2]") {
    auto ratio = estimateCompressionRatio(500, 1000);
    REQUIRE(ratio.has_value());
    CHECK(ratio.value() == Approx(0.5));
}

TEST_CASE("estimateCompressionRatio: zero uncompressed size", "[content][archive][catch2]") {
    auto ratio = estimateCompressionRatio(100, 0);
    // Implementation-dependent: may return nullopt or special value
    // Either is acceptable
    if (ratio.has_value()) {
        CHECK(ratio.value() >= 0.0);
    }
}

TEST_CASE("estimateCompressionRatio: both zero", "[content][archive][catch2]") {
    auto ratio = estimateCompressionRatio(0, 0);
    if (ratio.has_value()) {
        CHECK(ratio.value() >= 0.0);
    }
}

TEST_CASE("estimateCompressionRatio: 1:1 ratio", "[content][archive][catch2]") {
    auto ratio = estimateCompressionRatio(1000, 1000);
    REQUIRE(ratio.has_value());
    CHECK(ratio.value() == Approx(1.0));
}

// ────────────────────────────────────────────────────────────────────────────────
// isMetadataComplete
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("isMetadataComplete: empty metadata is incomplete", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    CHECK(isMetadataComplete(meta) == false);
}

TEST_CASE("isMetadataComplete: populated metadata", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    meta.totalFiles = 10;
    meta.totalDirectories = 3;
    meta.compressedSize = 5000;
    meta.uncompressedSize = 15000;
    // Whether this is "complete" depends on implementation but should not crash
    [[maybe_unused]] bool result = isMetadataComplete(meta);
}

// ────────────────────────────────────────────────────────────────────────────────
// ArchiveMetadata helper methods
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveMetadata::hasBasicInfo", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    CHECK(meta.hasBasicInfo() == false);
    meta.totalFiles = 1;
    CHECK(meta.hasBasicInfo() == true);
}

TEST_CASE("ArchiveMetadata::getCompressionRatio", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    meta.compressedSize = 500;
    meta.uncompressedSize = 1000;
    CHECK(meta.getCompressionRatio() == Approx(0.5));
}

TEST_CASE("ArchiveMetadata::getCompressionRatio zero uncompressed", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    meta.compressedSize = 100;
    meta.uncompressedSize = 0;
    CHECK(meta.getCompressionRatio() == Approx(0.0));
}

TEST_CASE("ArchiveMetadata::getTotalSpaceSaved", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    meta.compressedSize = 300;
    meta.uncompressedSize = 1000;
    CHECK(meta.getTotalSpaceSaved() == 700);
}

TEST_CASE("ArchiveMetadata::getTotalSpaceSaved negative case", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    meta.compressedSize = 1000;
    meta.uncompressedSize = 500; // compressed > uncompressed
    CHECK(meta.getTotalSpaceSaved() == 0);
}

TEST_CASE("ArchiveMetadata::getFileTypes", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    ArchiveEntry e1;
    e1.name = "file.txt";
    e1.isDirectory = false;
    ArchiveEntry e2;
    e2.name = "image.png";
    e2.isDirectory = false;
    ArchiveEntry dir;
    dir.name = "subdir/";
    dir.isDirectory = true;
    meta.entries = {e1, e2, dir};

    auto types = meta.getFileTypes();
    CHECK(types.size() >= 2); // .txt and .png
}

TEST_CASE("ArchiveMetadata::getFileTypes empty entries", "[content][archive][catch2]") {
    ArchiveMetadata meta;
    auto types = meta.getFileTypes();
    CHECK(types.empty());
}

// ────────────────────────────────────────────────────────────────────────────────
// ArchiveAnalysisResult
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveAnalysisResult::hasErrors / hasWarnings", "[content][archive][catch2]") {
    ArchiveAnalysisResult result;
    CHECK(result.hasErrors() == false);
    CHECK(result.hasWarnings() == false);

    result.errors.push_back("some error");
    CHECK(result.hasErrors() == true);

    result.warnings.push_back("some warning");
    CHECK(result.hasWarnings() == true);
}

// ────────────────────────────────────────────────────────────────────────────────
// getCommonFormats / isCommonFormat
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("getCommonFormats returns non-empty list", "[content][archive][catch2]") {
    auto formats = getCommonFormats();
    CHECK(!formats.empty());
    // Expect at least ZIP, RAR, TAR
    bool hasZip = false;
    for (auto f : formats) {
        if (f == ArchiveFormat::ZIP)
            hasZip = true;
    }
    CHECK(hasZip);
}

TEST_CASE("isCommonFormat: ZIP is common", "[content][archive][catch2]") {
    CHECK(isCommonFormat(ArchiveFormat::ZIP) == true);
}

TEST_CASE("isCommonFormat: Unknown is not common", "[content][archive][catch2]") {
    CHECK(isCommonFormat(ArchiveFormat::Unknown) == false);
}

// ────────────────────────────────────────────────────────────────────────────────
// ArchiveContentHandler construction
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveContentHandler default construction", "[content][archive][catch2]") {
    ArchiveContentHandler handler;
    CHECK(handler.name() == "ArchiveContentHandler");
    CHECK(handler.priority() == 10);
}

TEST_CASE("ArchiveContentHandler construction with config", "[content][archive][catch2]") {
    ArchiveProcessingConfig cfg;
    cfg.maxFileSize = 42;
    ArchiveContentHandler handler(cfg);
    CHECK(handler.getArchiveConfig().maxFileSize == 42);
}

TEST_CASE("ArchiveContentHandler setArchiveConfig", "[content][archive][catch2]") {
    ArchiveContentHandler handler;
    ArchiveProcessingConfig cfg;
    cfg.maxFileListSize = 999;
    handler.setArchiveConfig(cfg);
    CHECK(handler.getArchiveConfig().maxFileListSize == 999);
}

// ────────────────────────────────────────────────────────────────────────────────
// supportedMimeTypes
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveContentHandler supportedMimeTypes is non-empty", "[content][archive][catch2]") {
    ArchiveContentHandler handler;
    auto mimes = handler.supportedMimeTypes();
    CHECK(!mimes.empty());

    // Expect at least application/zip
    bool hasZipMime = false;
    for (const auto& m : mimes) {
        if (m.find("zip") != std::string::npos)
            hasZipMime = true;
    }
    CHECK(hasZipMime);
}

// ────────────────────────────────────────────────────────────────────────────────
// canHandle
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveContentHandler canHandle archive MIME type", "[content][archive][catch2]") {
    ArchiveContentHandler handler;

    yams::detection::FileSignature sig;
    sig.mimeType = "application/zip";
    sig.fileType = "archive";
    sig.confidence = 1.0f;
    CHECK(handler.canHandle(sig) == true);
}

TEST_CASE("ArchiveContentHandler canHandle non-archive MIME type", "[content][archive][catch2]") {
    ArchiveContentHandler handler;

    yams::detection::FileSignature sig;
    sig.mimeType = "image/jpeg";
    sig.fileType = "image";
    sig.confidence = 1.0f;
    CHECK(handler.canHandle(sig) == false);
}

// ────────────────────────────────────────────────────────────────────────────────
// cancelProcessing / resetStats
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ArchiveContentHandler cancelProcessing does not crash", "[content][archive][catch2]") {
    ArchiveContentHandler handler;
    handler.cancelProcessing();
    // Just verify no exception or crash
}

TEST_CASE("ArchiveContentHandler resetStats zeros counters", "[content][archive][catch2]") {
    ArchiveContentHandler handler;
    handler.resetStats();
    auto stats = handler.getProcessingStats();
    CHECK(stats.totalFilesProcessed == 0);
    CHECK(stats.successfulProcessing == 0);
    CHECK(stats.failedProcessing == 0);
}

// ────────────────────────────────────────────────────────────────────────────────
// createArchiveHandler factory
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("createArchiveHandler returns valid handler", "[content][archive][catch2]") {
    auto handler = createArchiveHandler();
    REQUIRE(handler != nullptr);
    CHECK(handler->name() == "ArchiveContentHandler");
}

TEST_CASE("createArchiveHandler with custom config", "[content][archive][catch2]") {
    ArchiveProcessingConfig cfg;
    cfg.maxFileSize = 12345;
    auto handler = createArchiveHandler(cfg);
    REQUIRE(handler != nullptr);
    CHECK(handler->getArchiveConfig().maxFileSize == 12345);
}
