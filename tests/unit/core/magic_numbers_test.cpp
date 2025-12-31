// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <array>
#include <cstring>
#include <span>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <yams/core/magic_numbers.hpp>

using namespace yams::magic;

// Helper to create byte vectors from hex strings
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

TEST_CASE("Image format detection", "[core][magic][image]") {
    SECTION("JPEG detection") {
        auto jpeg_data = hex_to_vector("FFD8FF");
        REQUIRE(detect_mime_type(std::span(jpeg_data)) == "image/jpeg");
    }

    SECTION("PNG detection") {
        auto png_data = hex_to_vector("89504E470D0A1A0A");
        REQUIRE(detect_mime_type(std::span(png_data)) == "image/png");
    }

    SECTION("GIF87a detection") {
        auto gif87_data = hex_to_vector("474946383761");
        REQUIRE(detect_mime_type(std::span(gif87_data)) == "image/gif");
    }

    SECTION("GIF89a detection") {
        auto gif89_data = hex_to_vector("474946383961");
        REQUIRE(detect_mime_type(std::span(gif89_data)) == "image/gif");
    }

    SECTION("BMP detection") {
        auto bmp_data = hex_to_vector("424D");
        REQUIRE(detect_mime_type(std::span(bmp_data)) == "image/bmp");
    }

    SECTION("TIFF detection") {
        auto tiff_data = hex_to_vector("49492A00");
        REQUIRE(detect_mime_type(std::span(tiff_data)) == "image/tiff");
    }
}

TEST_CASE("Archive format detection", "[core][magic][archive]") {
    SECTION("PDF detection") {
        auto pdf_data = hex_to_vector("255044462D");
        REQUIRE(detect_mime_type(std::span(pdf_data)) == "application/pdf");
    }

    SECTION("ZIP detection") {
        auto zip_data = hex_to_vector("504B0304");
        REQUIRE(detect_mime_type(std::span(zip_data)) == "application/zip");
    }

    SECTION("GZIP detection") {
        auto gzip_data = hex_to_vector("1F8B");
        REQUIRE(detect_mime_type(std::span(gzip_data)) == "application/gzip");
    }

    SECTION("BZIP2 detection") {
        auto bzip2_data = hex_to_vector("425A68");
        REQUIRE(detect_mime_type(std::span(bzip2_data)) == "application/x-bzip2");
    }

    SECTION("7-Zip detection") {
        auto sevenz_data = hex_to_vector("377ABCAF271C");
        REQUIRE(detect_mime_type(std::span(sevenz_data)) == "application/x-7z-compressed");
    }

    SECTION("RAR detection") {
        auto rar_data = hex_to_vector("526172211A0700");
        REQUIRE(detect_mime_type(std::span(rar_data)) == "application/x-rar-compressed");
    }

    SECTION("XZ detection") {
        auto xz_data = hex_to_vector("FD377A585A00");
        REQUIRE(detect_mime_type(std::span(xz_data)) == "application/x-xz");
    }
}

TEST_CASE("Executable format detection", "[core][magic][executable]") {
    SECTION("ELF detection") {
        auto elf_data = hex_to_vector("7F454C46");
        REQUIRE(detect_mime_type(std::span(elf_data)) == "application/x-executable");
    }

    SECTION("Windows PE detection") {
        auto pe_data = hex_to_vector("4D5A");
        REQUIRE(detect_mime_type(std::span(pe_data)) == "application/x-msdownload");
    }

    SECTION("Java class file detection") {
        // Java class file: magic (CAFEBABE) + minor version (0000) + major version (0034 = Java 8)
        // The version bytes distinguish it from Mach-O fat binary which also uses CAFEBABE
        auto class_data = hex_to_vector("CAFEBABE00000034");
        REQUIRE(detect_mime_type(std::span(class_data)) == "application/java-archive");
    }
}

TEST_CASE("Audio format detection", "[core][magic][audio]") {
    SECTION("MP3 detection") {
        auto mp3_data = hex_to_vector("494433");
        REQUIRE(detect_mime_type(std::span(mp3_data)) == "audio/mpeg");
    }

    SECTION("FLAC detection") {
        auto flac_data = hex_to_vector("664C6143");
        REQUIRE(detect_mime_type(std::span(flac_data)) == "audio/flac");
    }

    SECTION("Ogg detection") {
        auto ogg_data = hex_to_vector("4F676753");
        REQUIRE(detect_mime_type(std::span(ogg_data)) == "audio/ogg");
    }
}

TEST_CASE("Video format detection", "[core][magic][video]") {
    SECTION("MP4 detection") {
        auto mp4_data = hex_to_vector("000000146674797069736F6D");
        REQUIRE(detect_mime_type(std::span(mp4_data)) == "video/mp4");
    }

    SECTION("Matroska detection") {
        auto mkv_data = hex_to_vector("1A45DFA3");
        REQUIRE(detect_mime_type(std::span(mkv_data)) == "video/x-matroska");
    }

    SECTION("FLV detection") {
        auto flv_data = hex_to_vector("464C56");
        REQUIRE(detect_mime_type(std::span(flv_data)) == "video/x-flv");
    }
}

TEST_CASE("Font format detection", "[core][magic][font]") {
    SECTION("TrueType detection") {
        auto ttf_data = hex_to_vector("0001000000");
        REQUIRE(detect_mime_type(std::span(ttf_data)) == "font/ttf");
    }

    SECTION("OpenType detection") {
        auto otf_data = hex_to_vector("4F54544F");
        REQUIRE(detect_mime_type(std::span(otf_data)) == "font/otf");
    }

    SECTION("WOFF detection") {
        auto woff_data = hex_to_vector("774F4646");
        REQUIRE(detect_mime_type(std::span(woff_data)) == "font/woff");
    }
}

TEST_CASE("Database format detection", "[core][magic][database]") {
    auto sqlite_data = hex_to_vector("53514C69746520666F726D6174");
    REQUIRE(detect_mime_type(std::span(sqlite_data)) == "application/x-sqlite3");
}

TEST_CASE("Text encoding detection", "[core][magic][text]") {
    SECTION("UTF-8 BOM detection") {
        auto utf8_bom = hex_to_vector("EFBBBF");
        REQUIRE(detect_mime_type(std::span(utf8_bom)) == "text/plain");
    }

    SECTION("UTF-16 BE BOM detection") {
        auto utf16be_bom = hex_to_vector("FEFF");
        REQUIRE(detect_mime_type(std::span(utf16be_bom)) == "text/plain");
    }

    SECTION("UTF-16 LE BOM detection") {
        auto utf16le_bom = hex_to_vector("FFFE");
        REQUIRE(detect_mime_type(std::span(utf16le_bom)) == "text/plain");
    }
}

//=============================================================================
// Prune Category Tests
//=============================================================================

TEST_CASE("Prune category detection", "[core][magic][prune]") {
    SECTION("Build output directories") {
        REQUIRE(matchesPruneGroup(getPruneCategory("build/output.o"), "build"));
        REQUIRE(matchesPruneGroup(getPruneCategory("dist/bundle.js"), "build"));
        REQUIRE(matchesPruneGroup(getPruneCategory("cmake-build-debug/CMakeFiles/app.dir/main.obj"),
                                  "build"));
        REQUIRE(matchesPruneGroup(getPruneCategory("target/release/app"), "build"));
        REQUIRE(matchesPruneGroup(getPruneCategory(".next/static/chunks/app.js"), "build"));
        REQUIRE(matchesPruneGroup(getPruneCategory("obj/Debug/app.pdb"), "build"));
    }

    SECTION("Package caches and deps") {
        REQUIRE(matchesPruneGroup(getPruneCategory("node_modules/pkg/index.js"), "packages"));
        REQUIRE(matchesPruneGroup(getPruneCategory(".pytest_cache/v/cache.db"), "packages"));
        REQUIRE(matchesPruneGroup(getPruneCategory(".gradle/caches/modules.bin"), "packages"));
    }

    SECTION("Git artifacts") {
        REQUIRE(getPruneCategory(".git/objects/aa/bb") == PruneCategory::GitArtifacts);
    }
}

//=============================================================================
// Pattern Structure Tests
//=============================================================================

TEST_CASE("Pattern database is valid", "[core][magic]") {
    SECTION("Database is not empty") {
        const auto& patterns = get_magic_patterns();
#if YAMS_HAS_CONSTEXPR_CONTAINERS
        REQUIRE(patterns.size() > 0);
        REQUIRE(patterns.size() == 97); // High-confidence patterns only
#endif
    }

    SECTION("Magic database info is accessible") {
        size_t count = MagicDatabaseInfo::pattern_count();
        const char* mode = MagicDatabaseInfo::mode();

        REQUIRE(count > 0);
        REQUIRE(mode != nullptr);
        REQUIRE(strlen(mode) > 0);

#if YAMS_HAS_CONSTEXPR_CONTAINERS
        std::string mode_str(mode);
        REQUIRE(mode_str.find("compile-time") != std::string::npos);
#endif
    }

    SECTION("Pattern fields are valid") {
        const auto& patterns = get_magic_patterns();
#if YAMS_HAS_CONSTEXPR_CONTAINERS
        REQUIRE(patterns.size() > 0);

        const auto& first = patterns[0];
        REQUIRE(first.length > 0);
        REQUIRE(first.length <= 32);
        REQUIRE_FALSE(first.mime_type.empty());
        REQUIRE_FALSE(first.file_type.empty());
        REQUIRE_FALSE(first.description.empty());
        REQUIRE(first.confidence >= 0.0f);
        REQUIRE(first.confidence <= 1.0f);
#endif
    }
}

//=============================================================================
// Edge Cases
//=============================================================================

TEST_CASE("Edge case handling", "[core][magic][edge]") {
    SECTION("Empty data returns default") {
        std::vector<uint8_t> empty;
        REQUIRE(detect_mime_type(std::span(empty)) == "application/octet-stream");
    }

    SECTION("Unknown data returns default") {
        auto unknown_data = hex_to_vector("DEADBEEFCAFE");
        REQUIRE(detect_mime_type(std::span(unknown_data)) == "application/octet-stream");
    }

    SECTION("Partial match fails") {
        // PNG signature is 8 bytes, provide only 4
        auto partial_png = hex_to_vector("89504E47");
        auto mime = detect_mime_type(std::span(partial_png));
        REQUIRE(mime != "image/png");
    }

    SECTION("Short data handled gracefully") {
        std::vector<uint8_t> short_data = {0x00};
        REQUIRE(detect_mime_type(std::span(short_data)) == "application/octet-stream");
    }
}

//=============================================================================
// Hex Conversion Tests
//=============================================================================

TEST_CASE("Hex conversion utilities", "[core][magic][hex]") {
    SECTION("hex_char_to_byte") {
        REQUIRE(hex_char_to_byte('0') == 0x0);
        REQUIRE(hex_char_to_byte('9') == 0x9);
        REQUIRE(hex_char_to_byte('A') == 0xA);
        REQUIRE(hex_char_to_byte('F') == 0xF);
        REQUIRE(hex_char_to_byte('a') == 0xa);
        REQUIRE(hex_char_to_byte('f') == 0xf);
    }

    SECTION("hex_to_bytes") {
        auto bytes = hex_to_bytes("FFD8FF");
        REQUIRE(bytes[0] == 0xFF);
        REQUIRE(bytes[1] == 0xD8);
        REQUIRE(bytes[2] == 0xFF);
    }

    SECTION("hex_length") {
        REQUIRE(hex_length("FF") == 1);
        REQUIRE(hex_length("FFD8") == 2);
        REQUIRE(hex_length("FFD8FF") == 3);
        REQUIRE(hex_length("89504E470D0A1A0A") == 8);
    }
}

//=============================================================================
// Pattern Matching Logic Tests
//=============================================================================

TEST_CASE("Pattern matching logic", "[core][magic][matching]") {
    SECTION("Pattern matches at correct offset") {
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
        REQUIRE_FALSE(pattern.matches(std::span(buffer.data(), 10), 0));

        // Should match at offset 257
        REQUIRE(pattern.matches(std::span(buffer.data() + 257, 10), 257));
    }

    SECTION("Pattern match fails on mismatch") {
        MagicPattern pattern{hex_to_bytes("FFD8FF"), 3, 0, "image", "image/jpeg", "JPEG", 1.0f};

        auto wrong_data = hex_to_vector("89504E");
        REQUIRE_FALSE(pattern.matches(std::span(wrong_data), 0));
    }

    SECTION("Pattern match fails on insufficient data") {
        MagicPattern pattern{
            hex_to_bytes("89504E470D0A1A0A"), 8, 0, "image", "image/png", "PNG", 1.0f};

        // Only 4 bytes provided, pattern needs 8
        auto short_data = hex_to_vector("89504E47");
        REQUIRE_FALSE(pattern.matches(std::span(short_data), 0));
    }
}

//=============================================================================
// C++23 Constexpr Tests
//=============================================================================

#if YAMS_HAS_CONSTEXPR_CONTAINERS
TEST_CASE("Constexpr magic number detection", "[core][magic][constexpr]") {
    SECTION("Constexpr detection works") {
        std::array<uint8_t, 3> jpeg_magic = {0xFF, 0xD8, 0xFF};
        auto mime = detect_mime_type(std::span(jpeg_magic.data(), jpeg_magic.size()));
        REQUIRE(mime == "image/jpeg");
    }

    SECTION("Pattern count is compile-time") {
        size_t count = MagicDatabaseInfo::pattern_count();
        REQUIRE(count > 80);
    }

    SECTION("Patterns are accessible at compile-time") {
        const auto& patterns = get_magic_patterns();
        REQUIRE(patterns.size() > 0);

        const auto& first = patterns[0];
        REQUIRE(first.length > 0);
    }
}
#endif

//=============================================================================
// Performance/Stress Tests
//=============================================================================

TEST_CASE("Performance and stress testing", "[core][magic][performance]") {
    SECTION("Large data handled efficiently") {
        // Create 1MB of data
        std::vector<uint8_t> large_data(1024 * 1024, 0x00);

        // Put JPEG magic at start
        large_data[0] = 0xFF;
        large_data[1] = 0xD8;
        large_data[2] = 0xFF;

        // Should still detect correctly
        REQUIRE(detect_mime_type(std::span(large_data.data(), 100)) == "image/jpeg");
    }

    SECTION("Multiple detections are consistent") {
        auto jpeg_data = hex_to_vector("FFD8FF");

        // Run detection 100 times, should always return same result
        for (int i = 0; i < 100; ++i) {
            REQUIRE(detect_mime_type(std::span(jpeg_data)) == "image/jpeg");
        }
    }
}
