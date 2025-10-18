// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <array>
#include <cstring> // for strlen
#include <span>
#include <vector>
#include <gtest/gtest.h>
#include <yams/core/magic_numbers.hpp>

namespace yams::magic {

// Helper to create byte spans from hex strings
std::vector<uint8_t> hex_to_vector(const std::string& hex) {
    std::vector<uint8_t> result;
    for (size_t i = 0; i < hex.length(); i += 2) {
        uint8_t byte = std::stoi(hex.substr(i, 2), nullptr, 16);
        result.push_back(byte);
    }
    return result;
}

//=============================================================================
// Pattern Matching Tests
//=============================================================================

TEST(MagicNumbersTest, JPEGDetection) {
    auto jpeg_data = hex_to_vector("FFD8FF");
    auto mime = detect_mime_type(std::span(jpeg_data));
    EXPECT_EQ(mime, "image/jpeg");
}

TEST(MagicNumbersTest, PNGDetection) {
    auto png_data = hex_to_vector("89504E470D0A1A0A");
    auto mime = detect_mime_type(std::span(png_data));
    EXPECT_EQ(mime, "image/png");
}

TEST(MagicNumbersTest, GIFDetection) {
    // GIF87a
    auto gif87_data = hex_to_vector("474946383761");
    EXPECT_EQ(detect_mime_type(std::span(gif87_data)), "image/gif");

    // GIF89a
    auto gif89_data = hex_to_vector("474946383961");
    EXPECT_EQ(detect_mime_type(std::span(gif89_data)), "image/gif");
}

TEST(MagicNumbersTest, PDFDetection) {
    auto pdf_data = hex_to_vector("255044462D");
    auto mime = detect_mime_type(std::span(pdf_data));
    EXPECT_EQ(mime, "application/pdf");
}

TEST(MagicNumbersTest, ZIPDetection) {
    auto zip_data = hex_to_vector("504B0304");
    auto mime = detect_mime_type(std::span(zip_data));
    EXPECT_EQ(mime, "application/zip");
}

TEST(MagicNumbersTest, GZIPDetection) {
    auto gzip_data = hex_to_vector("1F8B");
    auto mime = detect_mime_type(std::span(gzip_data));
    EXPECT_EQ(mime, "application/gzip");
}

TEST(MagicNumbersTest, ELFDetection) {
    auto elf_data = hex_to_vector("7F454C46");
    auto mime = detect_mime_type(std::span(elf_data));
    EXPECT_EQ(mime, "application/x-executable");
}

TEST(MagicNumbersTest, MP3Detection) {
    auto mp3_data = hex_to_vector("494433");
    auto mime = detect_mime_type(std::span(mp3_data));
    EXPECT_EQ(mime, "audio/mpeg");
}

TEST(MagicNumbersTest, SQLiteDetection) {
    auto sqlite_data = hex_to_vector("53514C69746520666F726D6174");
    auto mime = detect_mime_type(std::span(sqlite_data));
    EXPECT_EQ(mime, "application/x-sqlite3");
}

//=============================================================================
// Pattern Structure Tests
//=============================================================================

TEST(MagicNumbersTest, PatternDatabaseNotEmpty) {
    const auto& patterns = get_magic_patterns();
#if YAMS_HAS_CONSTEXPR_CONTAINERS
    EXPECT_GT(patterns.size(), 0);
    EXPECT_GE(patterns.size(), 90); // Should have at least 90 patterns
#endif
}

TEST(MagicNumbersTest, MagicDatabaseInfo) {
    size_t count = MagicDatabaseInfo::pattern_count();
    const char* mode = MagicDatabaseInfo::mode();

    EXPECT_GT(count, 0);
    EXPECT_NE(mode, nullptr);
    EXPECT_GT(strlen(mode), 0); // Use ::strlen

#if YAMS_HAS_CONSTEXPR_CONTAINERS
    // In C++23 mode, should report compile-time
    std::string mode_str(mode);
    EXPECT_NE(mode_str.find("compile-time"), std::string::npos);
#endif
}

TEST(MagicNumbersTest, PatternFieldsValid) {
    const auto& patterns = get_magic_patterns();
#if YAMS_HAS_CONSTEXPR_CONTAINERS
    ASSERT_GT(patterns.size(), 0);

    // Check first pattern has valid fields
    const auto& first = patterns[0];
    EXPECT_GT(first.length, 0);
    EXPECT_LE(first.length, 32);
    EXPECT_FALSE(first.mime_type.empty());
    EXPECT_FALSE(first.file_type.empty());
    EXPECT_FALSE(first.description.empty());
    EXPECT_GE(first.confidence, 0.0f);
    EXPECT_LE(first.confidence, 1.0f);
#endif
}

//=============================================================================
// Edge Cases
//=============================================================================

TEST(MagicNumbersTest, EmptyDataReturnsDefault) {
    std::vector<uint8_t> empty;
    auto mime = detect_mime_type(std::span(empty));
    EXPECT_EQ(mime, "application/octet-stream");
}

TEST(MagicNumbersTest, UnknownDataReturnsDefault) {
    auto unknown_data = hex_to_vector("DEADBEEFCAFE");
    auto mime = detect_mime_type(std::span(unknown_data));
    EXPECT_EQ(mime, "application/octet-stream");
}

TEST(MagicNumbersTest, PartialMatchFails) {
    // PNG signature is 8 bytes, provide only 4
    auto partial_png = hex_to_vector("89504E47");
    auto mime = detect_mime_type(std::span(partial_png));
    // Should not match PNG (needs full 8 bytes)
    EXPECT_NE(mime, "image/png");
}

TEST(MagicNumbersTest, ShortDataHandledGracefully) {
    std::vector<uint8_t> short_data = {0x00};
    auto mime = detect_mime_type(std::span(short_data));
    EXPECT_EQ(mime, "application/octet-stream");
}

//=============================================================================
// Hex Conversion Tests
//=============================================================================

TEST(MagicNumbersTest, HexCharToByte) {
    EXPECT_EQ(hex_char_to_byte('0'), 0x0);
    EXPECT_EQ(hex_char_to_byte('9'), 0x9);
    EXPECT_EQ(hex_char_to_byte('A'), 0xA);
    EXPECT_EQ(hex_char_to_byte('F'), 0xF);
    EXPECT_EQ(hex_char_to_byte('a'), 0xa);
    EXPECT_EQ(hex_char_to_byte('f'), 0xf);
}

TEST(MagicNumbersTest, HexToBytes) {
    auto bytes = hex_to_bytes("FFD8FF");
    EXPECT_EQ(bytes[0], 0xFF);
    EXPECT_EQ(bytes[1], 0xD8);
    EXPECT_EQ(bytes[2], 0xFF);
}

TEST(MagicNumbersTest, HexLength) {
    EXPECT_EQ(hex_length("FF"), 1);
    EXPECT_EQ(hex_length("FFD8"), 2);
    EXPECT_EQ(hex_length("FFD8FF"), 3);
    EXPECT_EQ(hex_length("89504E470D0A1A0A"), 8);
}

//=============================================================================
// Pattern Matching Logic Tests
//=============================================================================

TEST(MagicNumbersTest, PatternMatchesAtCorrectOffset) {
    MagicPattern pattern{hex_to_bytes("7573746172"),
                         hex_length("7573746172"),
                         257, // TAR magic at offset 257
                         "archive",
                         "application/x-tar",
                         "POSIX tar archive",
                         1.0f};

    // Create buffer with pattern at offset 257
    std::vector<uint8_t> buffer(300, 0x00);
    auto tar_magic = hex_to_vector("7573746172");
    std::copy(tar_magic.begin(), tar_magic.end(), buffer.begin() + 257);

    // Should NOT match at offset 0
    EXPECT_FALSE(pattern.matches(std::span(buffer.data(), 10), 0));

    // Should match at offset 257
    EXPECT_TRUE(pattern.matches(std::span(buffer.data() + 257, 10), 257));
}

TEST(MagicNumbersTest, PatternMatchFailsOnMismatch) {
    MagicPattern pattern{hex_to_bytes("FFD8FF"), 3, 0, "image", "image/jpeg", "JPEG", 1.0f};

    auto wrong_data = hex_to_vector("89504E");
    EXPECT_FALSE(pattern.matches(std::span(wrong_data), 0));
}

TEST(MagicNumbersTest, PatternMatchFailsOnInsufficientData) {
    MagicPattern pattern{hex_to_bytes("89504E470D0A1A0A"), 8, 0, "image", "image/png", "PNG", 1.0f};

    // Only 4 bytes provided, pattern needs 8
    auto short_data = hex_to_vector("89504E47");
    EXPECT_FALSE(pattern.matches(std::span(short_data), 0));
}

//=============================================================================
// Comprehensive Format Tests
//=============================================================================

TEST(MagicNumbersTest, ArchiveFormats) {
    struct TestCase {
        const char* hex;
        const char* expected_mime;
    };

    TestCase cases[] = {
        {"504B0304", "application/zip"},
        {"1F8B", "application/gzip"},
        {"425A68", "application/x-bzip2"},
        {"377ABCAF271C", "application/x-7z-compressed"},
        {"526172211A0700", "application/x-rar-compressed"},
        {"FD377A585A00", "application/x-xz"},
    };

    for (const auto& tc : cases) {
        auto data = hex_to_vector(tc.hex);
        EXPECT_EQ(detect_mime_type(std::span(data)), tc.expected_mime)
            << "Failed for hex: " << tc.hex;
    }
}

TEST(MagicNumbersTest, ImageFormats) {
    struct TestCase {
        const char* hex;
        const char* expected_mime;
    };

    TestCase cases[] = {
        {"FFD8FF", "image/jpeg"}, {"89504E470D0A1A0A", "image/png"}, {"474946383761", "image/gif"},
        {"424D", "image/bmp"},    {"49492A00", "image/tiff"},
    };

    for (const auto& tc : cases) {
        auto data = hex_to_vector(tc.hex);
        EXPECT_EQ(detect_mime_type(std::span(data)), tc.expected_mime)
            << "Failed for hex: " << tc.hex;
    }
}

TEST(MagicNumbersTest, ExecutableFormats) {
    struct TestCase {
        const char* hex;
        const char* expected_mime;
    };

    TestCase cases[] = {
        {"4D5A", "application/x-msdownload"},
        {"7F454C46", "application/x-executable"},
        {"CAFEBABE", "application/java-archive"},
    };

    for (const auto& tc : cases) {
        auto data = hex_to_vector(tc.hex);
        EXPECT_EQ(detect_mime_type(std::span(data)), tc.expected_mime)
            << "Failed for hex: " << tc.hex;
    }
}

TEST(MagicNumbersTest, VideoFormats) {
    auto mp4_data = hex_to_vector("000000146674797069736F6D");
    EXPECT_EQ(detect_mime_type(std::span(mp4_data)), "video/mp4");

    auto mkv_data = hex_to_vector("1A45DFA3");
    EXPECT_EQ(detect_mime_type(std::span(mkv_data)), "video/x-matroska");

    auto flv_data = hex_to_vector("464C56");
    EXPECT_EQ(detect_mime_type(std::span(flv_data)), "video/x-flv");
}

TEST(MagicNumbersTest, AudioFormats) {
    auto mp3_data = hex_to_vector("494433");
    EXPECT_EQ(detect_mime_type(std::span(mp3_data)), "audio/mpeg");

    auto flac_data = hex_to_vector("664C6143");
    EXPECT_EQ(detect_mime_type(std::span(flac_data)), "audio/flac");

    auto ogg_data = hex_to_vector("4F676753");
    EXPECT_EQ(detect_mime_type(std::span(ogg_data)), "audio/ogg");
}

TEST(MagicNumbersTest, FontFormats) {
    auto ttf_data = hex_to_vector("0001000000");
    EXPECT_EQ(detect_mime_type(std::span(ttf_data)), "font/ttf");

    auto otf_data = hex_to_vector("4F54544F");
    EXPECT_EQ(detect_mime_type(std::span(otf_data)), "font/otf");

    auto woff_data = hex_to_vector("774F4646");
    EXPECT_EQ(detect_mime_type(std::span(woff_data)), "font/woff");
}

TEST(MagicNumbersTest, TextEncodingFormats) {
    auto utf8_bom = hex_to_vector("EFBBBF");
    EXPECT_EQ(detect_mime_type(std::span(utf8_bom)), "text/plain");

    auto utf16be_bom = hex_to_vector("FEFF");
    EXPECT_EQ(detect_mime_type(std::span(utf16be_bom)), "text/plain");

    auto utf16le_bom = hex_to_vector("FFFE");
    EXPECT_EQ(detect_mime_type(std::span(utf16le_bom)), "text/plain");
}

//=============================================================================
// C++23 Constexpr Tests (only run in C++23 mode)
//=============================================================================

#if YAMS_HAS_CONSTEXPR_CONTAINERS

TEST(MagicNumbersTest, ConstexprDetection) {
    // This test verifies detection works (avoid full constexpr due to GCC 15 limitations)
    std::array<uint8_t, 3> jpeg_magic = {0xFF, 0xD8, 0xFF};
    auto mime = detect_mime_type(std::span(jpeg_magic.data(), jpeg_magic.size()));

    EXPECT_EQ(mime, "image/jpeg");
}

TEST(MagicNumbersTest, ConstexprPatternCount) {
    // Pattern count is known at compile time
    size_t count = MagicDatabaseInfo::pattern_count();
    // Updated: pattern count reduced from 125 to 86 after removing low-confidence patterns
    EXPECT_GT(count, 80); // Reduced from 90 to 80 to accommodate actual pattern count
}

TEST(MagicNumbersTest, ConstexprPatternsAccessible) {
    // Can access patterns
    const auto& patterns = get_magic_patterns();
    EXPECT_GT(patterns.size(), 0);

    // First pattern should be valid
    const auto& first = patterns[0];
    EXPECT_GT(first.length, 0);
}

#endif // YAMS_HAS_CONSTEXPR_CONTAINERS

//=============================================================================
// Performance/Stress Tests
//=============================================================================

TEST(MagicNumbersTest, LargeDataHandled) {
    // Create 1MB of data
    std::vector<uint8_t> large_data(1024 * 1024, 0x00);

    // Put JPEG magic at start
    large_data[0] = 0xFF;
    large_data[1] = 0xD8;
    large_data[2] = 0xFF;

    // Should still detect correctly
    EXPECT_EQ(detect_mime_type(std::span(large_data.data(), 100)), "image/jpeg");
}

TEST(MagicNumbersTest, MultipleDetectionsConsistent) {
    auto jpeg_data = hex_to_vector("FFD8FF");

    // Run detection 100 times, should always return same result
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(detect_mime_type(std::span(jpeg_data)), "image/jpeg");
    }
}

} // namespace yams::magic
