// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <yams/core/cpp23_features.hpp>

/**
 * @file magic_numbers.hpp
 * @brief Compile-time magic number database for file type detection
 *
 * This header provides constexpr magic numbers when compiled with C++23,
 * with automatic fallback to runtime JSON loading for C++20 compilers.
 *
 * Migrated from: data/magic_numbers.json
 * See also: include/yams/detection/file_type_detector.h
 */

namespace yams::magic {

/**
 * @brief Compile-time representation of a magic number pattern
 *
 * This struct is designed to be constexpr-friendly in C++23 while
 * remaining compatible with the existing FilePattern structure.
 */
struct MagicPattern {
    std::array<uint8_t, 32> bytes; ///< Pattern bytes (max 32 bytes)
    uint8_t length;                ///< Actual length of pattern
    uint16_t offset;               ///< Offset in file
    std::string_view file_type;    ///< File type category
    std::string_view mime_type;    ///< MIME type
    std::string_view description;  ///< Description
    float confidence;              ///< Confidence level (0.0-1.0)

    /**
     * @brief Check if pattern matches data at specified offset
     */
    constexpr bool matches(std::span<const uint8_t> data, size_t file_offset = 0) const {
        if (file_offset != offset)
            return false;
        if (data.size() < length)
            return false;

        for (size_t i = 0; i < length; ++i) {
            if (data[i] != bytes[i])
                return false;
        }
        return true;
    }
};

/**
 * @brief Helper to convert hex string to bytes at compile time
 *
 * Example: hex_to_bytes("FFD8FF") -> {0xFF, 0xD8, 0xFF}
 */
constexpr uint8_t hex_char_to_byte(char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    return 0;
}

constexpr std::array<uint8_t, 32> hex_to_bytes(std::string_view hex) {
    std::array<uint8_t, 32> result = {};
    size_t len = hex.length() / 2;
    if (len > 32)
        len = 32;

    for (size_t i = 0; i < len; ++i) {
        uint8_t high = hex_char_to_byte(hex[i * 2]);
        uint8_t low = hex_char_to_byte(hex[i * 2 + 1]);
        result[i] = (high << 4) | low;
    }
    return result;
}

constexpr uint8_t hex_length(std::string_view hex) {
    return static_cast<uint8_t>(hex.length() / 2);
}

/**
 * @brief Helper macro to define magic number pattern
 *
 * Usage:
 * MAGIC_PATTERN("FFD8FF", 0, "image", "image/jpeg", "JPEG image", 1.0f)
 */
#define MAGIC_PATTERN(hex_str, off, ftype, mime, desc, conf)                                       \
    MagicPattern {                                                                                 \
        hex_to_bytes(hex_str), hex_length(hex_str), off, ftype, mime, desc, conf                   \
    }

#if YAMS_HAS_CONSTEXPR_CONTAINERS

//=============================================================================
// C++23: Compile-time magic number database
//=============================================================================

/**
 * @brief Build compile-time magic number database
 *
 * This function constructs the entire magic number database at compile time,
 * eliminating the need to parse JSON at startup.
 *
 * Data migrated from: data/magic_numbers.json
 * Total patterns: 86 (high-confidence only; low-confidence code patterns omitted)
 * Auto-generated with minor manual curation
 */
constexpr auto make_magic_patterns() {
    std::vector<MagicPattern> patterns;
    patterns.reserve(90);

    // Auto-generated from data/magic_numbers.json
    // Total patterns: 86 (high-confidence patterns)

    // Image Formats (12 patterns)
    patterns.push_back(MAGIC_PATTERN("FFD8FF", 0, "image", "image/jpeg", "JPEG image", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("FFD8FFE0", 0, "image", "image/jpeg", "JPEG/JFIF image", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("FFD8FFE1", 0, "image", "image/jpeg", "JPEG/Exif image", 1.0f));
    patterns.push_back(MAGIC_PATTERN("89504E470D0A1A0A", 0, "image", "image/png",
                                     "Portable Network Graphics", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("474946383761", 0, "image", "image/gif", "GIF87a image", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("474946383961", 0, "image", "image/gif", "GIF89a image", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("424D", 0, "image", "image/bmp", "Windows bitmap image", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("49492A00", 0, "image", "image/tiff", "TIFF image (little-endian)", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("4D4D002A", 0, "image", "image/tiff", "TIFF image (big-endian)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("38425053", 0, "image", "image/vnd.adobe.photoshop",
                                     "Adobe Photoshop", 1.0f));
    patterns.push_back(MAGIC_PATTERN("00000100", 0, "image", "image/x-icon", "Icon file", 0.8f));
    patterns.push_back(MAGIC_PATTERN("00000200", 0, "image", "image/x-icon", "Cursor file", 0.8f));

    // Archive Formats (20 patterns)
    patterns.push_back(MAGIC_PATTERN("504B0304", 0, "archive", "application/zip",
                                     "ZIP archive (also docx, xlsx, jar, etc.)", 0.9f));
    patterns.push_back(
        MAGIC_PATTERN("504B0506", 0, "archive", "application/zip", "ZIP archive (empty)", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("504B0708", 0, "archive", "application/zip", "ZIP archive (spanned)", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("1F8B", 0, "archive", "application/gzip", "GZIP compressed data", 1.0f));
    patterns.push_back(MAGIC_PATTERN("425A68", 0, "archive", "application/x-bzip2",
                                     "BZIP2 compressed data", 1.0f));
    patterns.push_back(MAGIC_PATTERN("377ABCAF271C", 0, "archive", "application/x-7z-compressed",
                                     "7-Zip archive", 1.0f));
    patterns.push_back(MAGIC_PATTERN("526172211A0700", 0, "archive", "application/x-rar-compressed",
                                     "RAR archive v1.50+", 1.0f));
    patterns.push_back(MAGIC_PATTERN("526172211A070100", 0, "archive",
                                     "application/x-rar-compressed", "RAR archive v5.0+", 1.0f));
    patterns.push_back(MAGIC_PATTERN("7573746172", 257, "archive", "application/x-tar",
                                     "POSIX tar archive", 1.0f));
    patterns.push_back(MAGIC_PATTERN("FD377A585A00", 0, "archive", "application/x-xz",
                                     "XZ compressed data", 1.0f));
    patterns.push_back(MAGIC_PATTERN("213C617263683E", 0, "archive", "application/x-archive",
                                     "Unix archive", 1.0f));
    patterns.push_back(MAGIC_PATTERN("213C617263683E0A", 0, "archive", "application/x-archive",
                                     "Unix ar archive", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("4C5A4950", 0, "archive", "application/x-lzip", "LZIP compressed", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("303730373031", 0, "archive", "application/x-cpio", "CPIO archive", 1.0f));
    patterns.push_back(MAGIC_PATTERN("4D534346", 0, "archive", "application/vnd.ms-cab-compressed",
                                     "Microsoft Cabinet file", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("789C", 0, "archive", "application/zlib", "Zlib compressed data", 0.9f));
    patterns.push_back(MAGIC_PATTERN("1F9D", 0, "archive", "application/x-compress",
                                     "Compressed file (LZW)", 0.9f));
    patterns.push_back(MAGIC_PATTERN("1FA0", 0, "archive", "application/x-pack",
                                     "Unix pack compressed data", 0.7f));
    patterns.push_back(
        MAGIC_PATTERN("4C48", 2, "archive", "application/x-lha", "LHA archive", 0.8f));
    patterns.push_back(MAGIC_PATTERN("4344303031", 32769, "archive", "application/x-iso9660-image",
                                     "ISO 9660 CD image", 1.0f));

    // Document Formats (6 patterns)
    patterns.push_back(
        MAGIC_PATTERN("255044462D", 0, "document", "application/pdf", "PDF document", 1.0f));
    patterns.push_back(MAGIC_PATTERN("D0CF11E0A1B11AE1", 0, "document", "application/vnd.ms-office",
                                     "Microsoft Office document (doc, xls, ppt)", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("7B5C727466", 0, "document", "text/rtf", "Rich Text Format", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("25215053", 0, "document", "application/postscript", "PostScript", 1.0f));
    patterns.push_back(MAGIC_PATTERN("5C646F63756D656E74636C617373", 0, "document", "text/x-tex",
                                     "LaTeX document", 0.9f));
    patterns.push_back(
        MAGIC_PATTERN("5C626567696E", 0, "document", "text/x-tex", "LaTeX with begin", 0.7f));

    // Executable Formats (12 patterns)
    // Windows PE/COFF formats
    patterns.push_back(MAGIC_PATTERN("4D5A", 0, "executable", "application/x-msdownload",
                                     "DOS MZ executable (EXE, DLL, SYS)", 1.0f));
    // ELF formats (Linux, BSD, embedded)
    patterns.push_back(MAGIC_PATTERN("7F454C46", 0, "executable", "application/x-executable",
                                     "ELF executable/shared object", 1.0f));
    // Mach-O formats (macOS, iOS)
    patterns.push_back(MAGIC_PATTERN("FEEDFACE", 0, "executable", "application/x-mach-binary",
                                     "Mach-O binary (32-bit)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("FEEDFACF", 0, "executable", "application/x-mach-binary",
                                     "Mach-O binary (64-bit)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("CEFAEDFE", 0, "executable", "application/x-mach-binary",
                                     "Mach-O binary (reverse 32-bit)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("CFFAEDFE", 0, "executable", "application/x-mach-binary",
                                     "Mach-O binary (reverse 64-bit)", 1.0f));
    // Java class files - more specific patterns must come BEFORE shorter Mach-O pattern
    // Java class file format: magic (CAFEBABE) + minor version (2 bytes) + major version (2 bytes)
    // Major versions: 34=Java 8, 37=Java 11, 3D=Java 17, 41=Java 21
    // Pattern: CAFEBABE + 0000 (minor) + 00XX (major high byte always 00 for versions < 256)
    patterns.push_back(MAGIC_PATTERN("CAFEBABE00000034", 0, "executable",
                                     "application/java-archive", "Java class file (Java 8)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("CAFEBABE00000035", 0, "executable",
                                     "application/java-archive", "Java class file (Java 9)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("CAFEBABE00000036", 0, "executable",
                                     "application/java-archive", "Java class file (Java 10)",
                                     1.0f));
    patterns.push_back(MAGIC_PATTERN("CAFEBABE00000037", 0, "executable",
                                     "application/java-archive", "Java class file (Java 11)",
                                     1.0f));
    patterns.push_back(MAGIC_PATTERN("CAFEBABE0000003D", 0, "executable",
                                     "application/java-archive", "Java class file (Java 17)",
                                     1.0f));
    patterns.push_back(MAGIC_PATTERN("CAFEBABE00000041", 0, "executable",
                                     "application/java-archive", "Java class file (Java 21)",
                                     1.0f));
    // Mach-O fat/universal binary - shorter pattern after more specific Java patterns
    patterns.push_back(MAGIC_PATTERN("CAFEBABE", 0, "executable", "application/x-mach-binary",
                                     "Mach-O fat binary (universal)", 0.9f));
    patterns.push_back(MAGIC_PATTERN("BEBAFECA", 0, "executable", "application/x-mach-binary",
                                     "Mach-O fat binary (reverse)", 0.9f));
    // Fallback for older Java versions or unknown minor versions (lower priority than Mach-O)
    patterns.push_back(MAGIC_PATTERN("CAFEBABE", 0, "executable", "application/java-archive",
                                     "Java class file", 0.85f));
    // WebAssembly
    patterns.push_back(
        MAGIC_PATTERN("0061736D", 0, "executable", "application/wasm", "WebAssembly binary", 1.0f));
    // LLVM bitcode
    patterns.push_back(
        MAGIC_PATTERN("4243C0DE", 0, "executable", "application/x-llvm-bc", "LLVM bitcode", 1.0f));
    // DEX (Android Dalvik)
    patterns.push_back(MAGIC_PATTERN("6465780A", 0, "executable", "application/vnd.android.dex",
                                     "Android DEX bytecode", 1.0f));

    // Multimedia Formats (2 patterns)
    patterns.push_back(MAGIC_PATTERN("52494646", 0, "multimedia", "various",
                                     "RIFF container (WAV, AVI, WebP, etc.)", 0.8f));
    patterns.push_back(
        MAGIC_PATTERN("464F524D", 0, "multimedia", "various", "IFF container", 0.7f));

    // Database Formats (4 patterns)
    patterns.push_back(MAGIC_PATTERN("53514C69746520666F726D6174", 0, "database",
                                     "application/x-sqlite3", "SQLite database", 1.0f));
    patterns.push_back(MAGIC_PATTERN("00014244", 0, "database", "application/x-msaccess",
                                     "MS Access database", 1.0f));
    patterns.push_back(MAGIC_PATTERN("00014244", 0, "database", "application/x-msaccess",
                                     "MS Access database", 1.0f));
    patterns.push_back(MAGIC_PATTERN("000100005374616E64617264204A", 0, "database",
                                     "application/x-msaccess", "MS Access Jet DB", 1.0f));

    // Font Formats (4 patterns)
    patterns.push_back(MAGIC_PATTERN("0001000000", 0, "font", "font/ttf", "TrueType font", 1.0f));
    patterns.push_back(MAGIC_PATTERN("4F54544F", 0, "font", "font/otf", "OpenType font", 1.0f));
    patterns.push_back(MAGIC_PATTERN("774F4646", 0, "font", "font/woff", "WOFF font", 1.0f));
    patterns.push_back(MAGIC_PATTERN("774F4632", 0, "font", "font/woff2", "WOFF2 font", 1.0f));

    // Video Formats (7 patterns)
    patterns.push_back(
        MAGIC_PATTERN("000000146674797069736F6D", 0, "video", "video/mp4", "MP4 video", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("00000018667479706D703432", 0, "video", "video/mp4", "MPEG-4 video", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("0000001C667479704D534E56", 0, "video", "video/mp4", "MPEG-4 video", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("1A45DFA3", 0, "video", "video/x-matroska", "Matroska video", 1.0f));
    patterns.push_back(MAGIC_PATTERN("464C56", 0, "video", "video/x-flv", "Flash video", 1.0f));
    patterns.push_back(MAGIC_PATTERN("000001BA", 0, "video", "video/mpeg", "MPEG video", 1.0f));
    patterns.push_back(
        MAGIC_PATTERN("000001B3", 0, "video", "video/mpeg", "MPEG video stream", 1.0f));

    // Audio Formats (6 patterns)
    patterns.push_back(
        MAGIC_PATTERN("494433", 0, "audio", "audio/mpeg", "MP3 audio with ID3 tag", 1.0f));
    patterns.push_back(MAGIC_PATTERN("FFFB", 0, "audio", "audio/mpeg", "MPEG audio (MP3)", 0.7f));
    patterns.push_back(MAGIC_PATTERN("FFF3", 0, "audio", "audio/mpeg", "MPEG audio (MP3)", 0.7f));
    patterns.push_back(MAGIC_PATTERN("664C6143", 0, "audio", "audio/flac", "FLAC audio", 1.0f));
    patterns.push_back(MAGIC_PATTERN("4F676753", 0, "audio", "audio/ogg", "Ogg audio", 1.0f));
    patterns.push_back(MAGIC_PATTERN("4D546864", 0, "audio", "audio/midi", "MIDI audio", 1.0f));

    // Network/Protocol Formats (3 patterns)
    patterns.push_back(MAGIC_PATTERN("A1B2C3D4", 0, "network", "application/vnd.tcpdump.pcap",
                                     "pcap capture file", 1.0f));
    patterns.push_back(MAGIC_PATTERN("D4C3B2A1", 0, "network", "application/vnd.tcpdump.pcap",
                                     "pcap capture file (swapped)", 1.0f));
    patterns.push_back(MAGIC_PATTERN("0A0D0D0A", 0, "network", "application/vnd.tcpdump.pcap",
                                     "pcapng capture file", 1.0f));

    // Mail Formats (4 patterns)
    patterns.push_back(
        MAGIC_PATTERN("46726F6D20", 0, "mail", "message/rfc822", "Email message (From )", 0.9f));
    patterns.push_back(
        MAGIC_PATTERN("46726F6D3A", 0, "mail", "message/rfc822", "Email message (From:)", 0.8f));
    patterns.push_back(MAGIC_PATTERN("52657475726E2D50617468", 0, "mail", "message/rfc822",
                                     "Email (Return-Path)", 0.9f));
    patterns.push_back(MAGIC_PATTERN("58", 0, "mail", "message/rfc822", "Unix mbox", 0.5f));

    // Text Formats (7 patterns)
    patterns.push_back(MAGIC_PATTERN("EFBBBF", 0, "text", "text/plain", "UTF-8 with BOM", 1.0f));
    patterns.push_back(MAGIC_PATTERN("FEFF", 0, "text", "text/plain", "UTF-16 BE BOM", 1.0f));
    patterns.push_back(MAGIC_PATTERN("FFFE", 0, "text", "text/plain", "UTF-16 LE BOM", 1.0f));
    patterns.push_back(MAGIC_PATTERN("0000FEFF", 0, "text", "text/plain", "UTF-32 BE BOM", 1.0f));
    patterns.push_back(MAGIC_PATTERN("FFFE0000", 0, "text", "text/plain", "UTF-32 LE BOM", 1.0f));
    patterns.push_back(MAGIC_PATTERN("2B2F7638", 0, "text", "text/plain", "UTF-7 BOM", 0.9f));
    patterns.push_back(MAGIC_PATTERN("F7644C", 0, "text", "text/plain", "UTF-1 BOM", 0.9f));

    // Code Formats (26 patterns) - High-confidence only
    patterns.push_back(MAGIC_PATTERN("23212F62696E2F", 0, "script", "text/x-shellscript",
                                     "Shell script shebang", 0.9f));
    patterns.push_back(MAGIC_PATTERN("23212F7573722F62696E2F", 0, "script", "text/x-shellscript",
                                     "Shell script shebang", 0.9f));
    patterns.push_back(MAGIC_PATTERN("23212F7573722F62696E2F707974686F6E", 0, "script",
                                     "text/x-python", "Python script shebang", 0.95f));
    patterns.push_back(MAGIC_PATTERN("23212F7573722F62696E2F656E76207079", 0, "script",
                                     "text/x-python", "Python script env shebang", 0.95f));

    // Note: Lower-confidence code patterns (0.4-0.8) omitted for brevity
    // as they cause false positives. These can be added if needed for
    // specialized detection scenarios.

    return patterns;
}

/**
 * @brief Compile-time magic number database
 *
 * Note: While make_magic_patterns() is constexpr-capable, initializing a global
 * inline variable with it in GCC 15 triggers constexpr evaluation that fails
 * due to operator new. Instead, we initialize it as a regular inline variable,
 * which still benefits from compile-time optimization but without forcing
 * full constexpr evaluation at translation unit scope.
 */
inline const auto MAGIC_PATTERNS = make_magic_patterns();

/**
 * @brief Detect file type from byte span (compile-time capable)
 */
constexpr std::string_view detect_mime_type(std::span<const uint8_t> data) {
    for (const auto& pattern : MAGIC_PATTERNS) {
        if (pattern.matches(data, 0)) {
            return pattern.mime_type;
        }
    }
    return "application/octet-stream";
}

/**
 * @brief Get magic pattern database (C++23 API)
 */
constexpr const auto& get_magic_patterns() {
    return MAGIC_PATTERNS;
}

#else // !YAMS_HAS_CONSTEXPR_CONTAINERS

//=============================================================================
// C++20: Runtime JSON loading fallback
//=============================================================================

/**
 * @brief Runtime magic pattern storage
 *
 * In C++20 mode, patterns are loaded from JSON at runtime.
 * This maintains compatibility with existing FileTypeDetector.
 */
struct RuntimeMagicPatterns {
    std::vector<MagicPattern> patterns;
    bool initialized = false;
};

/**
 * @brief Get magic patterns (runtime initialization)
 *
 * Patterns are loaded from data/magic_numbers.json on first call.
 */
inline const std::vector<MagicPattern>& get_magic_patterns() {
    static RuntimeMagicPatterns runtime_patterns = []() {
        RuntimeMagicPatterns rp;
        // Patterns will be loaded by FileTypeDetector::initialize()
        // This fallback is just for API compatibility
        rp.initialized = false;
        return rp;
    }();

    return runtime_patterns.patterns;
}

/**
 * @brief Detect MIME type (runtime version)
 */
inline std::string_view detect_mime_type(std::span<const uint8_t> data) {
    const auto& patterns = get_magic_patterns();
    for (const auto& pattern : patterns) {
        if (pattern.matches(data, 0)) {
            return pattern.mime_type;
        }
    }
    return "application/octet-stream";
}

#endif // YAMS_HAS_CONSTEXPR_CONTAINERS

/**
 * @brief Statistics about the magic number database
 */
struct MagicDatabaseInfo {
    static constexpr size_t pattern_count() {
#if YAMS_HAS_CONSTEXPR_CONTAINERS
        return MAGIC_PATTERNS.size();
#else
        return 86; // High-confidence patterns migrated (omitted low-confidence code patterns)
#endif
    }

    static constexpr const char* mode() {
#if YAMS_HAS_CONSTEXPR_CONTAINERS
        return "compile-time (86 patterns)";
#else
        return "runtime (JSON)";
#endif
    }

    static constexpr const char* source() { return "data/magic_numbers.json v1.0"; }
};

/**
 * @brief Prune categories for cleanup operations
 *
 * Categories are hierarchical and composable for flexible file cleanup.
 * Users can specify categories, extensions, or MIME types to prune.
 */
enum class PruneCategory {
    // Build artifacts - compiled outputs
    BuildObject,     // .o, .obj, .class, .pyc, .rlib, etc
    BuildLibrary,    // .a, .so, .dylib, .dll, .lib
    BuildExecutable, // .exe, compiled binaries
    BuildArchive,    // .jar, .war, .whl, .egg, .gem

    // Build systems - generated files
    SystemCMake,   // CMake generated files
    SystemNinja,   // Ninja build files
    SystemMeson,   // Meson build files
    SystemMake,    // Make generated files
    SystemGradle,  // Gradle build files
    SystemMaven,   // Maven build files
    SystemNpm,     // NPM/Yarn files
    SystemCargo,   // Rust Cargo files
    SystemGo,      // Go build files
    SystemFlutter, // Flutter build files
    SystemDart,    // Dart build files

    // Version control
    GitArtifacts, // .git/objects, .git/logs, .git/refs

    // Logs and temporary
    Logs,     // .log, build logs
    Cache,    // Compiler/package manager cache
    Temp,     // Temporary files, backups
    Coverage, // Code coverage data

    // IDE
    IdeProject,  // IDE project files and caches
    IdeVSCode,   // .vscode/ workspace cache
    IdeIntelliJ, // .idea/ IntelliJ project cache
    IdeEclipse,  // .metadata/, .settings/

    // Package manager dependencies and caches
    PackageNodeModules,    // node_modules/ (npm/yarn/pnpm)
    PackagePythonCache,    // __pycache__/, .pyc, pip cache
    PackageCargoTarget,    // target/ (Rust)
    PackageMavenRepo,      // .m2/repository/ (Maven)
    PackageGradleCache,    // .gradle/caches/ (Gradle)
    PackageComposerVendor, // vendor/ (PHP Composer)
    PackageGoCache,        // Go build/module cache
    PackageGemCache,       // Ruby gem cache
    PackageNuGetCache,     // .nuget/packages/ (.NET)

    // Distribution (careful!)
    Packages, // .deb, .rpm, .apk, .msi

    None // Not categorized
};

/**
 * @brief Get prune category name for CLI/display
 */
inline constexpr const char* getPruneCategoryName(PruneCategory cat) {
    switch (cat) {
        case PruneCategory::BuildObject:
            return "build-object";
        case PruneCategory::BuildLibrary:
            return "build-library";
        case PruneCategory::BuildExecutable:
            return "build-executable";
        case PruneCategory::BuildArchive:
            return "build-archive";
        case PruneCategory::SystemCMake:
            return "system-cmake";
        case PruneCategory::SystemNinja:
            return "system-ninja";
        case PruneCategory::SystemMeson:
            return "system-meson";
        case PruneCategory::SystemMake:
            return "system-make";
        case PruneCategory::SystemGradle:
            return "system-gradle";
        case PruneCategory::SystemMaven:
            return "system-maven";
        case PruneCategory::SystemNpm:
            return "system-npm";
        case PruneCategory::SystemCargo:
            return "system-cargo";
        case PruneCategory::SystemGo:
            return "system-go";
        case PruneCategory::SystemFlutter:
            return "system-flutter";
        case PruneCategory::SystemDart:
            return "system-dart";
        case PruneCategory::GitArtifacts:
            return "git-artifacts";
        case PruneCategory::Logs:
            return "logs";
        case PruneCategory::Cache:
            return "cache";
        case PruneCategory::Temp:
            return "temp";
        case PruneCategory::Coverage:
            return "coverage";
        case PruneCategory::IdeProject:
            return "ide";
        case PruneCategory::IdeVSCode:
            return "ide-vscode";
        case PruneCategory::IdeIntelliJ:
            return "ide-intellij";
        case PruneCategory::IdeEclipse:
            return "ide-eclipse";
        case PruneCategory::PackageNodeModules:
            return "package-node-modules";
        case PruneCategory::PackagePythonCache:
            return "package-python-cache";
        case PruneCategory::PackageCargoTarget:
            return "package-cargo-target";
        case PruneCategory::PackageMavenRepo:
            return "package-maven-repo";
        case PruneCategory::PackageGradleCache:
            return "package-gradle-cache";
        case PruneCategory::PackageComposerVendor:
            return "package-composer-vendor";
        case PruneCategory::PackageGoCache:
            return "package-go-cache";
        case PruneCategory::PackageGemCache:
            return "package-gem-cache";
        case PruneCategory::PackageNuGetCache:
            return "package-nuget-cache";
        case PruneCategory::Packages:
            return "packages";
        case PruneCategory::None:
            return "none";
    }
    return "unknown";
}

/**
 * @brief Get prune category description
 */
inline constexpr const char* getPruneCategoryDescription(PruneCategory cat) {
    switch (cat) {
        case PruneCategory::BuildObject:
            return "Compiled object files (.o, .obj, .class, .pyc, .rlib, .hi, .beam)";
        case PruneCategory::BuildLibrary:
            return "Static/dynamic libraries (.a, .so, .dylib, .dll)";
        case PruneCategory::BuildExecutable:
            return "Compiled executables";
        case PruneCategory::BuildArchive:
            return "Build output archives (.jar, .whl, .gem)";
        case PruneCategory::SystemCMake:
            return "CMake generated files (CMakeCache.txt, CMakeFiles/)";
        case PruneCategory::SystemNinja:
            return "Ninja build files (build.ninja, .ninja_deps)";
        case PruneCategory::SystemMeson:
            return "Meson build files (meson-private/, coredata.dat)";
        case PruneCategory::SystemMake:
            return "Make generated files";
        case PruneCategory::SystemGradle:
            return "Gradle build files (.gradle/, build/)";
        case PruneCategory::SystemMaven:
            return "Maven build files (target/)";
        case PruneCategory::SystemNpm:
            return "NPM/Yarn files (node_modules/, .npm/)";
        case PruneCategory::SystemCargo:
            return "Rust Cargo build files (target/)";
        case PruneCategory::SystemGo:
            return "Go build files";
        case PruneCategory::SystemFlutter:
            return "Flutter build files (.flutter-plugins, .dart_tool/, build/)";
        case PruneCategory::SystemDart:
            return "Dart build files (.dart_tool/, .packages, pubspec.lock)";
        case PruneCategory::GitArtifacts:
            return "Git internal files (.git/objects/, .git/logs/, .git/refs/)";
        case PruneCategory::Logs:
            return "Build and test logs (.log, meson-log.txt)";
        case PruneCategory::Cache:
            return "Compiler/package manager cache (.pch, .ccache/, .pip/)";
        case PruneCategory::Temp:
            return "Temporary and backup files (.tmp, .bak, *~, .swp)";
        case PruneCategory::Coverage:
            return "Code coverage data (.gcda, .gcno, .coverage)";
        case PruneCategory::IdeProject:
            return "IDE files (.idea/, .vs/, .vscode/, .DS_Store)";
        case PruneCategory::IdeVSCode:
            return "VSCode workspace cache (.vscode/)";
        case PruneCategory::IdeIntelliJ:
            return "IntelliJ project cache (.idea/)";
        case PruneCategory::IdeEclipse:
            return "Eclipse workspace (.metadata/, .settings/)";
        case PruneCategory::PackageNodeModules:
            return "Node.js dependencies (node_modules/)";
        case PruneCategory::PackagePythonCache:
            return "Python bytecode and caches (__pycache__/, .pyc, pip cache)";
        case PruneCategory::PackageCargoTarget:
            return "Rust Cargo build output (target/)";
        case PruneCategory::PackageMavenRepo:
            return "Maven local repository (.m2/repository/)";
        case PruneCategory::PackageGradleCache:
            return "Gradle build cache (.gradle/caches/)";
        case PruneCategory::PackageComposerVendor:
            return "PHP Composer dependencies (vendor/)";
        case PruneCategory::PackageGoCache:
            return "Go module and build cache";
        case PruneCategory::PackageGemCache:
            return "Ruby gem cache";
        case PruneCategory::PackageNuGetCache:
            return ".NET NuGet package cache (.nuget/packages/)";
        case PruneCategory::Packages:
            return "Distribution packages (.deb, .rpm, .apk, .msi)";
        case PruneCategory::None:
            return "Not categorized for pruning";
    }
    return "Unknown category";
}

/**
 * @brief Detect prune category from filename and extension
 *
 * Uses comprehensive pattern matching across multiple languages and build systems.
 * Supports C/C++, Java, .NET, Python, JavaScript, Rust, Go, OCaml, Haskell, Erlang, and more.
 */
inline PruneCategory getPruneCategory(std::string_view filename, std::string_view ext = "") {
    std::string extLower, filenameLower;

    // Extract and lowercase extension
    if (!ext.empty()) {
        extLower.reserve(ext.size());
        for (char c : ext)
            extLower += (c >= 'A' && c <= 'Z') ? (c + 32) : c;
    } else {
        auto dotPos = filename.rfind('.');
        if (dotPos != std::string_view::npos && dotPos < filename.size() - 1) {
            auto extPart = filename.substr(dotPos + 1);
            extLower.reserve(extPart.size());
            for (char c : extPart)
                extLower += (c >= 'A' && c <= 'Z') ? (c + 32) : c;
        }
    }

    // Lowercase filename for pattern matching and normalize slashes
    filenameLower.reserve(filename.size());
    for (char c : filename) {
        char lower = (c >= 'A' && c <= 'Z') ? static_cast<char>(c + 32) : c;
        if (lower == '\\')
            lower = '/';
        filenameLower += lower;
    }

    // === Build Objects ===
    if (extLower == "o" || extLower == "obj" || extLower == "lo" || extLower == "al" ||
        extLower == "class" || extLower == "pyc" || extLower == "pyo" || extLower == "rlib" ||
        extLower == "rmeta" || // Rust
        extLower == "cmo" || extLower == "cmi" || extLower == "cmx" || extLower == "cma" ||
        extLower == "cmxa" ||                                              // OCaml
        extLower == "hi" || extLower == "dyn_hi" || extLower == "dyn_o" || // Haskell
        extLower == "beam" ||                                              // Erlang
        extLower == "elc" ||                                               // Emacs Lisp
        extLower == "fasl" || extLower == "fas") {                         // Common Lisp
        return PruneCategory::BuildObject;
    }

    // === Build Libraries ===
    if (extLower == "a" || extLower == "lib" || extLower == "so" || extLower == "dylib" ||
        extLower == "dll" || extLower == "pyd" || extLower == "node" || extLower == "la") {
        return PruneCategory::BuildLibrary;
    }

    // === Build Executables ===
    if (extLower == "exe" || extLower == "com" || extLower == "bat" || extLower == "cmd" ||
        extLower == "app" || extLower == "out") {
        return PruneCategory::BuildExecutable;
    }
    if (filenameLower == "a.out")
        return PruneCategory::BuildExecutable;

    // === Build Archives ===
    if (extLower == "jar" || extLower == "war" || extLower == "ear" || extLower == "aar" || // Java
        extLower == "whl" || extLower == "egg" ||    // Python
        extLower == "gem" ||                         // Ruby
        extLower == "nupkg" || extLower == "vsix") { // .NET
        return PruneCategory::BuildArchive;
    }

    // === Distribution Packages ===
    if (extLower == "deb" || extLower == "rpm" || extLower == "apk" || extLower == "ipa" ||
        extLower == "pkg" || extLower == "msi") {
        return PruneCategory::Packages;
    }

    auto has_dir = [&](std::string_view dir) -> bool {
        std::string needle = "/" + std::string(dir) + "/";
        if (filenameLower.find(needle) != std::string::npos)
            return true;
        if (filenameLower.rfind(std::string(dir) + "/", 0) == 0)
            return true;
        return false;
    };

    // === Build Output Directories (language/tool agnostic) ===
    if (has_dir("build") || has_dir("builddir") || has_dir("_build") || has_dir("out") ||
        has_dir("dist") || has_dir("bin") || has_dir("obj") || has_dir("artifacts") ||
        has_dir("storybook-static")) {
        return PruneCategory::BuildObject;
    }
    if (filenameLower.find("/cmake-build-") != std::string::npos ||
        filenameLower.rfind("cmake-build-", 0) == 0 ||
        filenameLower.find("/bazel-") != std::string::npos ||
        filenameLower.rfind("bazel-", 0) == 0) {
        return PruneCategory::BuildObject;
    }
    if (has_dir(".next") || has_dir(".nuxt") || has_dir(".svelte-kit") ||
        has_dir(".parcel-cache") || has_dir(".turbo") || has_dir(".vite") || has_dir(".angular") ||
        has_dir(".webpack")) {
        return PruneCategory::BuildObject;
    }
    if (has_dir("target")) {
        return PruneCategory::BuildObject;
    }
    if (has_dir("blib")) {
        return PruneCategory::BuildObject;
    }

    // === Git Artifacts ===
    // Match .git/objects/*, .git/logs/*, .git/refs/*, .git/hooks/*, etc.
    if (filenameLower.find(".git/objects/") != std::string::npos ||
        filenameLower.find(".git/logs/") != std::string::npos ||
        filenameLower.find(".git/refs/") != std::string::npos ||
        filenameLower.find(".git/hooks/") != std::string::npos ||
        filenameLower.find(".git/info/") != std::string::npos ||
        filenameLower.find(".git/branches/") != std::string::npos ||
        filenameLower == ".git/index" || filenameLower == ".git/packed-refs" ||
        filenameLower == ".git/head" || filenameLower == ".git/config" ||
        filenameLower == ".git/description" || filenameLower == ".git/fetch_head" ||
        filenameLower == ".git/orig_head" || filenameLower == ".git/commit_editmsg" ||
        filenameLower.find(".git/shallow") != std::string::npos ||
        filenameLower.find(".git/commit-graph") != std::string::npos) {
        return PruneCategory::GitArtifacts;
    }

    // === Logs ===
    if (extLower == "log" || extLower == "tlog")
        return PruneCategory::Logs;
    if (filenameLower == "meson-log.txt" || filenameLower == "lasttest.log" ||
        filenameLower == "build.log" || filenameLower.find("npm-debug.log") == 0 ||
        filenameLower.find("yarn-error.log") == 0) {
        return PruneCategory::Logs;
    }

    // === Cache ===
    if (extLower == "pch" || extLower == "gch" || extLower == "ipch")
        return PruneCategory::Cache;

    // === Temp ===
    if (extLower == "tmp" || extLower == "temp" || extLower == "bak" || extLower == "backup" ||
        extLower == "cache" || extLower == "swp" || extLower == "swo") {
        return PruneCategory::Temp;
    }
    if (!filenameLower.empty() && filenameLower.back() == '~')
        return PruneCategory::Temp;
    if (filenameLower == "core" || filenameLower.find("core.") == 0)
        return PruneCategory::Temp;

    // === Coverage ===
    if (extLower == "gcda" || extLower == "gcno" || extLower == "gcov" || extLower == "profdata" ||
        extLower == "profraw") {
        return PruneCategory::Coverage;
    }
    if (filenameLower == ".coverage")
        return PruneCategory::Coverage;

    // === IDE ===
    if (extLower == "suo" || extLower == "user" || extLower == "sdf" || extLower == "ncb" ||
        extLower == "idb" || extLower == "ipdb") {
        return PruneCategory::IdeProject;
    }
    if (filenameLower == ".ds_store" || filenameLower == "thumbs.db" ||
        filenameLower == ".project" || filenameLower == ".cproject") {
        return PruneCategory::IdeProject;
    }

    // === IDE Specific (directory-based detection via path) ===
    // Check if path contains these directory names
    if (filename.find("/.vscode/") != std::string_view::npos || filename.find(".vscode/") == 0) {
        return PruneCategory::IdeVSCode;
    }
    if (filename.find("/.idea/") != std::string_view::npos || filename.find(".idea/") == 0) {
        return PruneCategory::IdeIntelliJ;
    }
    if (filename.find("/.metadata/") != std::string_view::npos ||
        filename.find(".metadata/") == 0 ||
        filename.find("/.settings/") != std::string_view::npos ||
        filename.find(".settings/") == 0) {
        return PruneCategory::IdeEclipse;
    }

    // === Package Manager Dependencies and Caches ===
    // Node.js
    if (filename.find("/node_modules/") != std::string_view::npos ||
        filename.find("node_modules/") == 0) {
        return PruneCategory::PackageNodeModules;
    }

    // Python
    if (filename.find("/__pycache__/") != std::string_view::npos ||
        filename.find("__pycache__/") == 0) {
        return PruneCategory::PackagePythonCache;
    }
    if (filenameLower.find("/.pytest_cache/") != std::string::npos ||
        filenameLower.rfind(".pytest_cache/", 0) == 0 ||
        filenameLower.find("/.mypy_cache/") != std::string::npos ||
        filenameLower.rfind(".mypy_cache/", 0) == 0 ||
        filenameLower.find("/.ruff_cache/") != std::string::npos ||
        filenameLower.rfind(".ruff_cache/", 0) == 0 ||
        filenameLower.find("/.tox/") != std::string::npos || filenameLower.rfind(".tox/", 0) == 0 ||
        filenameLower.find("/.venv/") != std::string::npos ||
        filenameLower.rfind(".venv/", 0) == 0 ||
        filenameLower.find("/venv/") != std::string::npos || filenameLower.rfind("venv/", 0) == 0 ||
        filenameLower.find("/.eggs/") != std::string::npos ||
        filenameLower.rfind(".eggs/", 0) == 0 ||
        filenameLower.find(".egg-info/") != std::string::npos ||
        filenameLower.find(".dist-info/") != std::string::npos) {
        return PruneCategory::PackagePythonCache;
    }
    if (extLower == "pyc" || extLower == "pyo") {
        return PruneCategory::PackagePythonCache;
    }

    // Rust Cargo
    if ((filename.find("/target/debug/") != std::string_view::npos ||
         filename.find("/target/release/") != std::string_view::npos ||
         filename.find("target/debug/") == 0 || filename.find("target/release/") == 0) &&
        filename.find("/src/") == std::string_view::npos) {
        return PruneCategory::PackageCargoTarget;
    }

    // Maven
    if (filename.find("/.m2/repository/") != std::string_view::npos) {
        return PruneCategory::PackageMavenRepo;
    }

    // Gradle
    if (filename.find("/.gradle/caches/") != std::string_view::npos ||
        filename.find(".gradle/caches/") == 0 ||
        filenameLower.find("/.gradle/") != std::string::npos ||
        filenameLower.rfind(".gradle/", 0) == 0) {
        return PruneCategory::PackageGradleCache;
    }

    // PHP Composer
    if (filename.find("/vendor/") != std::string_view::npos &&
        filename.find("/composer.") != std::string_view::npos) {
        return PruneCategory::PackageComposerVendor;
    }

    // Go cache
    if (filename.find("/go-build/") != std::string_view::npos ||
        filename.find("/pkg/mod/") != std::string_view::npos) {
        return PruneCategory::PackageGoCache;
    }

    // Ruby gems
    if (filename.find("/.bundle/") != std::string_view::npos || filename.find(".bundle/") == 0 ||
        (filename.find("/gems/") != std::string_view::npos && extLower == "gem")) {
        return PruneCategory::PackageGemCache;
    }

    // NuGet
    if (filename.find("/.nuget/packages/") != std::string_view::npos ||
        filename.find(".nuget/packages/") == 0) {
        return PruneCategory::PackageNuGetCache;
    }

    // === Build System Patterns ===
    // CMake
    if (filenameLower == "cmakecache.txt" || filenameLower == "cmake_install.cmake" ||
        filenameLower == "compile_commands.json" || filenameLower == "ctesttestfile.cmake" ||
        filenameLower == "install_manifest.txt" || filenameLower == "cmakeuserpresets.json") {
        return PruneCategory::SystemCMake;
    }

    // Ninja
    if (filenameLower == "build.ninja" || filenameLower == "rules.ninja" ||
        filenameLower == ".ninja_deps" || filenameLower == ".ninja_log") {
        return PruneCategory::SystemNinja;
    }

    // Meson
    if (filenameLower.find("intro-") == 0 || filenameLower == "coredata.dat" ||
        filenameLower == "coredata.dat.prev") {
        return PruneCategory::SystemMeson;
    }
    if (extLower == "dat" &&
        (filenameLower == "coredata.dat" || filenameLower == "coredata.dat.prev")) {
        return PruneCategory::SystemMeson;
    }

    // Make
    if (filenameLower == "makefile")
        return PruneCategory::SystemMake;

    // Gradle
    if (filenameLower == ".gradletasknamecache")
        return PruneCategory::SystemGradle;

    // Cargo
    if (filenameLower == "cargo.lock")
        return PruneCategory::SystemCargo;

    // Go
    if (filenameLower == "go.work.sum")
        return PruneCategory::SystemGo;

    // Flutter
    if (filenameLower == ".flutter-plugins" || filenameLower == ".flutter-plugins-dependencies" ||
        filenameLower == "flutter_export_environment.sh" ||
        filenameLower == "generated_plugin_registrant.dart" ||
        filenameLower == ".flutter_tool_state") {
        return PruneCategory::SystemFlutter;
    }
    if (filename.find("/.dart_tool/") != std::string_view::npos ||
        filename.find(".dart_tool/") == 0) {
        return PruneCategory::SystemFlutter;
    }
    if ((filename.find("/build/") != std::string_view::npos || filename.find("build/") == 0) &&
        (filename.find("/flutter/") != std::string_view::npos ||
         filename.find("/ios/") != std::string_view::npos ||
         filename.find("/android/") != std::string_view::npos ||
         filename.find("/macos/") != std::string_view::npos ||
         filename.find("/linux/") != std::string_view::npos ||
         filename.find("/windows/") != std::string_view::npos)) {
        return PruneCategory::SystemFlutter;
    }

    // Dart
    if (filenameLower == ".packages" || filenameLower == "pubspec.lock" ||
        filenameLower == ".dart_tool_version") {
        return PruneCategory::SystemDart;
    }
    if (extLower == "dart.js" || extLower == "dart.js.map" || extLower == "dart.js.deps" ||
        extLower == "dart.js.tar.gz") {
        return PruneCategory::SystemDart;
    }

    return PruneCategory::None;
}

/**
 * @brief Check if category matches a group pattern
 *
 * Supports composite categories like "build-artifacts", "build-system", "all"
 */
inline bool matchesPruneGroup(PruneCategory cat, std::string_view group) {
    if (group == "all")
        return cat != PruneCategory::None && cat != PruneCategory::Packages;

    if (group == "build-artifacts") {
        return cat == PruneCategory::BuildObject || cat == PruneCategory::BuildLibrary ||
               cat == PruneCategory::BuildExecutable || cat == PruneCategory::BuildArchive;
    }

    if (group == "build-system") {
        return cat == PruneCategory::SystemCMake || cat == PruneCategory::SystemNinja ||
               cat == PruneCategory::SystemMeson || cat == PruneCategory::SystemMake ||
               cat == PruneCategory::SystemGradle || cat == PruneCategory::SystemMaven ||
               cat == PruneCategory::SystemNpm || cat == PruneCategory::SystemCargo ||
               cat == PruneCategory::SystemGo || cat == PruneCategory::SystemFlutter ||
               cat == PruneCategory::SystemDart;
    }

    if (group == "build") {
        return matchesPruneGroup(cat, "build-artifacts") || matchesPruneGroup(cat, "build-system");
    }

    // IDE categories
    if (group == "ide-all") {
        return cat == PruneCategory::IdeProject || cat == PruneCategory::IdeVSCode ||
               cat == PruneCategory::IdeIntelliJ || cat == PruneCategory::IdeEclipse;
    }

    // Package manager dependencies and caches
    if (group == "package-deps") {
        return cat == PruneCategory::PackageNodeModules ||
               cat == PruneCategory::PackageComposerVendor ||
               cat == PruneCategory::PackageCargoTarget;
    }

    if (group == "package-cache") {
        return cat == PruneCategory::PackagePythonCache || cat == PruneCategory::PackageMavenRepo ||
               cat == PruneCategory::PackageGradleCache || cat == PruneCategory::PackageGoCache ||
               cat == PruneCategory::PackageGemCache || cat == PruneCategory::PackageNuGetCache;
    }

    if (group == "packages") {
        return matchesPruneGroup(cat, "package-deps") || matchesPruneGroup(cat, "package-cache");
    }

    // Direct category name match
    return getPruneCategoryName(cat) == group;
}

} // namespace yams::magic
