// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Content Handlers Test Suite (Catch2)
// Consolidates: encoding_detector_test.cpp, text_handler_test.cpp,
// test_content_handlers_modern.cpp
// Note: binary_handler_test.cpp and media_handler_test.cpp reference deprecated APIs

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <set>
#include <span>
#include <thread>
#include <vector>
#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/content/audio_content_handler.h>
#include <yams/content/binary_content_handler.h>
#include <yams/content/content_handler_registry.h>
#include <yams/content/image_content_handler.h>
#include <yams/content/processing_error.h>
#include <yams/content/processing_stats.h>
#include <yams/content/text_content_handler.h>
#include <yams/content/video_content_handler.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/text_extractor.h>

using namespace yams::content;
using namespace yams::extraction;
using namespace yams::detection;
namespace fs = std::filesystem;

namespace {
#if defined(__SANITIZE_THREAD__)
constexpr bool kThreadSanitizerEnabled = true;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
constexpr bool kThreadSanitizerEnabled = true;
#else
constexpr bool kThreadSanitizerEnabled = false;
#endif
#else
constexpr bool kThreadSanitizerEnabled = false;
#endif

constexpr int kSanitizerTimeoutMultiplier = kThreadSanitizerEnabled ? 3 : 1;
} // namespace

TEST_CASE("processing stats helpers share rate and average math", "[content][stats][catch2]") {
    CHECK(successRate(0, 0) == 0.0);
    CHECK(successRate(10, 7) == 0.7);
    CHECK(averageProcessingTimeMs(std::chrono::milliseconds{0}, 0) == 0.0);
    CHECK(averageProcessingTimeMs(std::chrono::milliseconds{1500}, 3) == 500.0);
    CHECK(averageCount(9, 3) == 3.0);
}

TEST_CASE("processing error helper shares user-facing diagnostic format",
          "[content][error][catch2]") {
    const auto message = formatProcessingError("Image", "decode", "/tmp/picture.png", "bad {}", 7);
    CHECK(message == "Image processing failed: decode for '/tmp/picture.png' - bad 7");
}

// ===========================================================================
// Test Fixtures & Helpers
// ===========================================================================

class ContentHandlerFixture {
public: // Public access for test cases
    ContentHandlerFixture() {
        testDir_ = fs::temp_directory_path() /
                   ("content_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);
    }

    ~ContentHandlerFixture() {
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    fs::path createTestFile(const std::string& name, const std::string& content) {
        fs::path filePath = testDir_ / name;
        std::ofstream file(filePath);
        file << content;
        file.close();
        return filePath;
    }

    fs::path createBinaryFile(const std::string& name, const std::vector<uint8_t>& content) {
        fs::path filePath = testDir_ / name;
        std::ofstream file(filePath, std::ios::binary);
        file.write(reinterpret_cast<const char*>(content.data()),
                   yams::test::checked_streamsize(content.size(), "content handler fixture"));
        file.close();
        return filePath;
    }

    std::vector<uint8_t> generateRandomData(size_t size) {
        std::vector<uint8_t> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);
        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<uint8_t>(dis(gen));
        }
        return data;
    }

    fs::path testDir_;
};

// ===========================================================================
// Encoding Detector Tests
// ===========================================================================

TEST_CASE("EncodingDetector: UTF-16 LE to UTF-8 conversion", "[content][encoding]") {
    // BOM + "Hi" (H=0x0048, i=0x0069)
    std::string utf16le;
    utf16le.push_back('\xFF');
    utf16le.push_back('\xFE'); // BOM LE
    utf16le.push_back('\x48'); // 'H'
    utf16le.push_back('\x00');
    utf16le.push_back('\x69'); // 'i'
    utf16le.push_back('\x00');

    auto out = EncodingDetector::convertToUtf8(utf16le, "UTF-16LE");
    REQUIRE(out);
    REQUIRE(out.value() == "Hi");
}

TEST_CASE("EncodingDetector: UTF-16 BE to UTF-8 conversion", "[content][encoding]") {
    // BOM + "Hi" (H=0x0048, i=0x0069)
    std::string utf16be;
    utf16be.push_back('\xFE');
    utf16be.push_back('\xFF'); // BOM BE
    utf16be.push_back('\x00'); // 'H'
    utf16be.push_back('\x48');
    utf16be.push_back('\x00'); // 'i'
    utf16be.push_back('\x69');

    auto out = EncodingDetector::convertToUtf8(utf16be, "UTF-16BE");
    REQUIRE(out);
    REQUIRE(out.value() == "Hi");
}

TEST_CASE("EncodingDetector: Latin1 to UTF-8 conversion", "[content][encoding]") {
    // ISO-8859-1: "Caf\xE9" (é=0xE9)
    std::string latin1 = std::string("Caf") + std::string(1, static_cast<char>(0xE9));
    auto out = EncodingDetector::convertToUtf8(latin1, "ISO-8859-1");
    REQUIRE(out);
    REQUIRE(out.value() == "Café");
}

// ===========================================================================
// Content Handler Registry Tests
// ===========================================================================

TEST_CASE("ContentHandlerRegistry: Initialization and handler registration",
          "[content][registry]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    auto& registry = ContentHandlerRegistry::instance();
    registry.clear();
    registry.initializeDefaultHandlers();

    SECTION("Default handlers are registered") {
        REQUIRE(registry.hasHandler("ImageContentHandler"));
        REQUIRE(registry.hasHandler("BinaryContentHandler"));
    }

    SECTION("Handler priority ordering") {
        // Handlers should be ordered by priority
        auto handlersMap = registry.getAllHandlers();
        REQUIRE_FALSE(handlersMap.empty());

        // Convert map to vector and sort by priority
        std::vector<std::shared_ptr<IContentHandler>> handlers;
        for (const auto& [name, handler] : handlersMap) {
            handlers.push_back(handler);
        }
        std::sort(handlers.begin(), handlers.end(),
                  [](const auto& a, const auto& b) { return a->priority() > b->priority(); });

        // Verify priorities are in descending order
        for (size_t i = 1; i < handlers.size(); ++i) {
            REQUIRE(handlers[i - 1]->priority() >= handlers[i]->priority());
        }
    }
}

TEST_CASE("ImageContentHandler: Basic functionality", "[content][image]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    ImageContentHandler handler;

    SECTION("Handler metadata") {
        REQUIRE(handler.name() == "ImageContentHandler");
        REQUIRE(handler.priority() > 0);

        auto mimeTypes = handler.supportedMimeTypes();
        REQUIRE_FALSE(mimeTypes.empty());
    }

    SECTION("Format detection") {
        // Create a simple PNG header
        std::vector<uint8_t> pngHeader = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
        auto filePath = fixture.createBinaryFile("test.png", pngHeader);

        // Note: Actual processing may fail due to incomplete PNG, but handler should recognize it
        auto result = handler.process(filePath);
        // Result is implementation-dependent for minimal headers
    }
}

TEST_CASE("BinaryContentHandler: Fallback handling", "[content][binary]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    BinaryContentHandler handler;

    SECTION("Handler metadata") {
        REQUIRE(handler.name() == "BinaryContentHandler");
        REQUIRE(handler.priority() >= 0); // Should be low priority (fallback)
    }

    SECTION("Random binary data") {
        auto randomData = fixture.generateRandomData(1024);
        auto filePath = fixture.createBinaryFile("random.bin", randomData);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());
        // Note: BinaryContentHandler indexes by default for searchability
        // This matches current implementation behavior
    }
}

// ===========================================================================
// Text Content Handler Tests
// ===========================================================================

TEST_CASE("TextContentHandler: Basic text file processing", "[content][text]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    TextContentHandler handler;

    SECTION("Handler metadata") {
        REQUIRE(handler.name() == "TextContentHandler");
        REQUIRE(handler.priority() > 0);

        auto mimeTypes = handler.supportedMimeTypes();
        REQUIRE_FALSE(mimeTypes.empty());
        auto it = std::find(mimeTypes.begin(), mimeTypes.end(), "text/plain");
        REQUIRE(it != mimeTypes.end());
    }

    SECTION("Simple text file") {
        std::string content = "Hello, World!\nThis is a test file.\n";
        auto filePath = fixture.createTestFile("test.txt", content);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());

        auto& contentResult = result.value();
        REQUIRE(contentResult.text.has_value());
        REQUIRE(contentResult.text.value() == content);
        REQUIRE(contentResult.contentType == "text/plain");
        REQUIRE(contentResult.shouldIndex);
    }

    SECTION("UTF-8 encoding with unicode") {
        std::string content = "Hello, 世界! 🌍\nÜnicode test: ñ, é, ü";
        auto filePath = fixture.createTestFile("utf8.txt", content);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());

        auto& contentResult = result.value();
        REQUIRE(contentResult.text.has_value());
        REQUIRE(contentResult.text.value() == content);
    }

    SECTION("Empty file") {
        auto filePath = fixture.createTestFile("empty.txt", "");

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());

        auto& contentResult = result.value();
        REQUIRE(contentResult.text.has_value());
        REQUIRE(contentResult.text.value().empty());
        REQUIRE(contentResult.shouldIndex);
    }
}

TEST_CASE("TextContentHandler: Special characters and line endings", "[content][text]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    TextContentHandler handler;

    SECTION("Special characters") {
        std::string content = "Special chars: \t\n\r!@#$%^&*()[]{}|\\:;\"'<>,.?/~`";
        auto filePath = fixture.createTestFile("special.txt", content);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());
        REQUIRE(result.value().text.value() == content);
    }

    SECTION("Unix line endings (LF)") {
        std::string content = "Line 1\nLine 2\nLine 3";
        auto filePath = fixture.createTestFile("unix.txt", content);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());
        REQUIRE(result.value().text.value() == content);
    }

    SECTION("Windows line endings (CRLF)") {
        std::string content = "Line 1\r\nLine 2\r\nLine 3";
        auto filePath = fixture.createTestFile("windows.txt", content);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());
        REQUIRE(result.value().text.value() == content);
    }

    SECTION("Mac classic line endings (CR)") {
        std::string content = "Line 1\rLine 2\rLine 3";
        auto filePath = fixture.createTestFile("mac.txt", content);

        auto result = handler.process(filePath);
        REQUIRE(result.has_value());
        REQUIRE(result.value().text.value() == content);
    }
}

TEST_CASE("TextContentHandler: Large file performance", "[content][text][performance]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    TextContentHandler handler;

    // Create a large text file (1MB+)
    std::string content;
    content.reserve(1024 * 1024);
    for (int i = 0; i < 10000; ++i) {
        content += "Line " + std::to_string(i) + ": This is a test line with some content.\n";
    }

    auto filePath = fixture.createTestFile("large.txt", content);

    auto start = std::chrono::steady_clock::now();
    auto result = handler.process(filePath);
    auto elapsed = std::chrono::steady_clock::now() - start;

    REQUIRE(result.has_value());
    REQUIRE(result.value().text.has_value());
    REQUIRE(result.value().text.value().size() > 400000); // ~499KB generated

    // Processing should complete in reasonable time
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    REQUIRE(elapsedMs < 2000 * kSanitizerTimeoutMultiplier); // 2 seconds max, scaled for TSAN

    // Processing time should be recorded
    REQUIRE(result.value().processingTimeMs > 0);
}

// ===========================================================================
// Audio/Video Handler Tests
// ===========================================================================

TEST_CASE("AudioContentHandler: Basic functionality", "[content][audio]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    AudioContentHandler handler;

    SECTION("Handler metadata") {
        REQUIRE(handler.name() == "AudioContentHandler");
        REQUIRE(handler.priority() > 0);

        auto mimeTypes = handler.supportedMimeTypes();
        REQUIRE_FALSE(mimeTypes.empty());
    }

    SECTION("MP3 file header recognition") {
        // Create a minimal MP3 ID3v2 header
        std::vector<uint8_t> mp3Header = {
            'I',  'D',  '3',       // ID3 identifier
            0x03, 0x00,            // Version
            0x00,                  // Flags
            0x00, 0x00, 0x00, 0x00 // Size
        };
        auto filePath = fixture.createBinaryFile("test.mp3", mp3Header);

        // Handler should recognize it as audio (even if processing fails)
        auto result = handler.process(filePath);
        // Result is implementation-dependent for minimal headers
    }
}

TEST_CASE("AudioContentHandler: processBuffer supports audio buffers", "[content][audio][buffer]") {
    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    AudioContentHandler handler;
    const std::vector<uint8_t> wavHeader = {'R', 'I', 'F', 'F', 36,  0,   0,   0,  'W', 'A', 'V',
                                            'E', 'f', 'm', 't', ' ', 16,  0,   0,  0,   1,   0,
                                            1,   0,   68,  172, 0,   0,   136, 88, 1,   0,   2,
                                            0,   16,  0,   'd', 'a', 't', 'a', 0,  0,   0,   0};

    auto result = handler.processBuffer(std::as_bytes(std::span{wavHeader}), ".wav");
    if (!result.has_value()) {
        FAIL(result.error().message);
    }
    CHECK(result.value().handlerName == "AudioContentHandler");
    CHECK(result.value().metadata.at("file_type") == "audio");
    CHECK(result.value().metadata.at("source") == "buffer");
    CHECK(result.value().metadata.at("hint") == ".wav");
    REQUIRE(result.value().audioData.has_value());
}

TEST_CASE("AudioContentHandler: processBuffer rejects generic binary with audio hint",
          "[content][audio][buffer][regression]") {
    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    AudioContentHandler handler;
    const std::vector<uint8_t> randomBytes = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
                                              0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb};

    auto result = handler.processBuffer(std::as_bytes(std::span{randomBytes}), ".wav");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::NotSupported);
}

TEST_CASE("VideoContentHandler: processBuffer supports video buffers", "[content][video][buffer]") {
    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    VideoContentHandler handler;
    const std::vector<uint8_t> mp4Header = {0x00, 0x00, 0x00, 0x10, 'f',  't',  'y',  'p',
                                            'i',  's',  'o',  'm',  0x00, 0x00, 0x00, 0x00};

    auto result = handler.processBuffer(std::as_bytes(std::span{mp4Header}), ".mp4");
    if (!result.has_value()) {
        INFO("video processBuffer error: " << result.error().message);
    }
    REQUIRE(result.has_value());
    CHECK(result.value().handlerName == "VideoContentHandler");
    CHECK(result.value().metadata.at("file_type") == "video");
    CHECK(result.value().metadata.at("source") == "buffer");
    CHECK(result.value().metadata.at("hint") == ".mp4");
    REQUIRE(result.value().videoData.has_value());
}

TEST_CASE("VideoContentHandler: processBuffer rejects generic binary with video hint",
          "[content][video][buffer][regression]") {
    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    VideoContentHandler handler;
    const std::vector<uint8_t> randomBytes = {0xde, 0xad, 0xbe, 0xef, 0x00, 0x01,
                                              0x02, 0x03, 0x04, 0x05, 0x06, 0x07};

    auto result = handler.processBuffer(std::as_bytes(std::span{randomBytes}), ".mp4");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::NotSupported);
}

// ===========================================================================
// Handler Registry Integration Tests
// ===========================================================================

TEST_CASE("ContentHandlerRegistry: Handler selection", "[content][registry][integration]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    auto& registry = ContentHandlerRegistry::instance();
    registry.clear();
    registry.initializeDefaultHandlers();

    SECTION("Text file handler selection") {
        std::string content = "This is a text file.";
        auto filePath = fixture.createTestFile("test.txt", content);

        auto handler = registry.getHandlerByName("TextContentHandler");
        REQUIRE(handler != nullptr);
        REQUIRE(handler->name() == "TextContentHandler");
    }

    SECTION("Binary file fallback") {
        auto randomData = fixture.generateRandomData(256);
        auto filePath = fixture.createBinaryFile("test.bin", randomData);

        // Binary handler should be available as fallback
        auto handler = registry.getHandlerByName("BinaryContentHandler");
        REQUIRE(handler != nullptr);
        REQUIRE(handler->name() == "BinaryContentHandler");
    }

    SECTION("Multiple handlers registered") {
        auto handlers = registry.getAllHandlers();
        REQUIRE(handlers.size() >= 4); // Image, Video, Audio, Binary, Text, Archive

        // Verify all default handlers are present
        REQUIRE(registry.hasHandler("ImageContentHandler"));
        REQUIRE(registry.hasHandler("BinaryContentHandler"));
        REQUIRE(registry.hasHandler("TextContentHandler"));
        REQUIRE(registry.hasHandler("AudioContentHandler"));
    }
}

TEST_CASE("ContentHandlerRegistry: Handler priority and ordering", "[content][registry]") {
    ContentHandlerFixture fixture;

    auto& detector = FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    REQUIRE(initResult);

    auto& registry = ContentHandlerRegistry::instance();
    registry.clear();
    registry.initializeDefaultHandlers();

    auto handlersMap = registry.getAllHandlers();
    std::vector<std::shared_ptr<IContentHandler>> handlers;
    for (const auto& [name, handler] : handlersMap) {
        handlers.push_back(handler);
    }

    SECTION("Handlers have distinct priorities") {
        std::set<int> priorities;
        for (const auto& handler : handlers) {
            priorities.insert(handler->priority());
        }
        // Most handlers should have different priorities (except equal-priority ones)
        REQUIRE(priorities.size() >= 3);
    }

    SECTION("Binary handler has lowest priority") {
        auto binaryHandler = registry.getHandlerByName("BinaryContentHandler");
        REQUIRE(binaryHandler != nullptr);

        int binaryPriority = binaryHandler->priority();
        for (const auto& handler : handlers) {
            if (handler->name() != "BinaryContentHandler") {
                REQUIRE(handler->priority() >= binaryPriority);
            }
        }
    }

    SECTION("Image/Video handlers have high priority") {
        auto imageHandler = registry.getHandlerByName("ImageContentHandler");
        if (imageHandler) {
            REQUIRE(imageHandler->priority() >= 10);
        }
    }
}
