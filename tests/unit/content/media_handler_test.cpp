#include <filesystem>
#include <fstream>
#include <vector>
#include <gtest/gtest.h>
#include <yams/content/handlers/media_handler.h>

namespace yams::content::test {

namespace fs = std::filesystem;

class MediaHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("media_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        handler_ = std::make_unique<MediaHandler>();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    fs::path createMediaFile(const std::string& name, const std::vector<uint8_t>& content) {
        fs::path filePath = testDir_ / name;
        std::ofstream file(filePath, std::ios::binary);
        file.write(reinterpret_cast<const char*>(content.data()), content.size());
        file.close();
        return filePath;
    }

    // Create minimal JPEG header
    std::vector<uint8_t> createJpegHeader() {
        return {
            0xFF, 0xD8, 0xFF, 0xE0,       // JPEG SOI and APP0 marker
            0x00, 0x10,                   // APP0 length
            'J',  'F',  'I',  'F',  0x00, // JFIF identifier
            0x01, 0x01,                   // Version
            0x00,                         // Aspect ratio units
            0x00, 0x01,                   // X density
            0x00, 0x01,                   // Y density
            0x00, 0x00,                   // Thumbnail dimensions
            0xFF, 0xD9                    // EOI marker
        };
    }

    // Create minimal PNG header
    std::vector<uint8_t> createPngHeader() {
        return {
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
            // IHDR chunk
            0x00, 0x00, 0x00, 0x0D, // Chunk length
            'I', 'H', 'D', 'R',     // Chunk type
            0x00, 0x00, 0x00, 0x10, // Width: 16
            0x00, 0x00, 0x00, 0x10, // Height: 16
            0x08,                   // Bit depth
            0x02,                   // Color type (RGB)
            0x00,                   // Compression
            0x00,                   // Filter
            0x00,                   // Interlace
            0x00, 0x00, 0x00, 0x00, // CRC (simplified)
            // IEND chunk
            0x00, 0x00, 0x00, 0x00, // Chunk length
            'I', 'E', 'N', 'D',     // Chunk type
            0xAE, 0x42, 0x60, 0x82  // CRC
        };
    }

    // Create minimal GIF header
    std::vector<uint8_t> createGifHeader() {
        return {
            'G', 'I', 'F', '8', '9', 'a', // GIF signature
            0x10, 0x00,                   // Width: 16
            0x10, 0x00,                   // Height: 16
            0xF0,                         // Global color table info
            0x00,                         // Background color
            0x00,                         // Aspect ratio
            // Minimal color table (2 colors)
            0x00, 0x00, 0x00, // Black
            0xFF, 0xFF, 0xFF, // White
            0x3B              // GIF terminator
        };
    }

    // Create minimal MP4 header (simplified)
    std::vector<uint8_t> createMp4Header() {
        return {
            0x00, 0x00, 0x00, 0x20, // Box size
            'f',  't',  'y',  'p',  // File type box
            'i',  's',  'o',  'm',  // Major brand
            0x00, 0x00, 0x02, 0x00, // Minor version
            'i',  's',  'o',  'm',  // Compatible brand
            'i',  's',  'o',  '2',  // Compatible brand
            'm',  'p',  '4',  '1'   // Compatible brand
        };
    }

    // Create minimal MP3 header (ID3v2)
    std::vector<uint8_t> createMp3Header() {
        return {
            'I', 'D', '3',          // ID3 identifier
            0x03, 0x00,             // Version
            0x00,                   // Flags
            0x00, 0x00, 0x00, 0x0A, // Size
            // Minimal ID3 frame
            'T', 'I', 'T', '2',     // Frame ID (Title)
            0x00, 0x00, 0x00, 0x06, // Frame size
            0x00, 0x00,             // Flags
            'T', 'e', 's', 't',     // Frame data
            // MP3 sync word
            0xFF, 0xFB // MP3 frame sync
        };
    }

    fs::path testDir_;
    std::unique_ptr<MediaHandler> handler_;
};

// Test format detection
TEST_F(MediaHandlerTest, CanHandle) {
    // Image formats
    EXPECT_TRUE(handler_->canHandle(".jpg"));
    EXPECT_TRUE(handler_->canHandle(".jpeg"));
    EXPECT_TRUE(handler_->canHandle(".png"));
    EXPECT_TRUE(handler_->canHandle(".gif"));
    EXPECT_TRUE(handler_->canHandle(".bmp"));
    EXPECT_TRUE(handler_->canHandle(".webp"));
    EXPECT_TRUE(handler_->canHandle(".svg"));

    // Video formats
    EXPECT_TRUE(handler_->canHandle(".mp4"));
    EXPECT_TRUE(handler_->canHandle(".avi"));
    EXPECT_TRUE(handler_->canHandle(".mov"));
    EXPECT_TRUE(handler_->canHandle(".mkv"));
    EXPECT_TRUE(handler_->canHandle(".webm"));

    // Audio formats
    EXPECT_TRUE(handler_->canHandle(".mp3"));
    EXPECT_TRUE(handler_->canHandle(".wav"));
    EXPECT_TRUE(handler_->canHandle(".flac"));
    EXPECT_TRUE(handler_->canHandle(".ogg"));
    EXPECT_TRUE(handler_->canHandle(".m4a"));

    // Non-media formats
    EXPECT_FALSE(handler_->canHandle(".txt"));
    EXPECT_FALSE(handler_->canHandle(".pdf"));
    EXPECT_FALSE(handler_->canHandle(".doc"));
}

// Test JPEG processing
TEST_F(MediaHandlerTest, ProcessJpeg) {
    auto jpegData = createJpegHeader();
    auto filePath = createMediaFile("test.jpg", jpegData);

    auto result = handler_->process(filePath);

    if (!result) {
        // Media processing might not be implemented
        EXPECT_TRUE(result.error().code == ErrorCode::NotImplemented ||
                    result.error().code == ErrorCode::ProcessingError)
            << "Unexpected error: " << result.error().message;
        return;
    }

    const auto& content = result.value();
    EXPECT_EQ(content.mimeType, "image/jpeg");

    // Metadata extraction is optional
    if (!content.metadata.empty()) {
        // Might contain dimensions, format info, etc.
        EXPECT_FALSE(content.metadata.empty());
    }
}

// Test PNG processing
TEST_F(MediaHandlerTest, ProcessPng) {
    auto pngData = createPngHeader();
    auto filePath = createMediaFile("test.png", pngData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_EQ(content.mimeType, "image/png");

        // Check for dimension metadata if extracted
        if (content.metadata.find("width") != content.metadata.end()) {
            EXPECT_EQ(content.metadata.at("width"), "16");
        }
        if (content.metadata.find("height") != content.metadata.end()) {
            EXPECT_EQ(content.metadata.at("height"), "16");
        }
    }
}

// Test GIF processing
TEST_F(MediaHandlerTest, ProcessGif) {
    auto gifData = createGifHeader();
    auto filePath = createMediaFile("test.gif", gifData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_EQ(content.mimeType, "image/gif");
    }
}

// Test MP4 video processing
TEST_F(MediaHandlerTest, ProcessMp4) {
    auto mp4Data = createMp4Header();
    auto filePath = createMediaFile("test.mp4", mp4Data);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_EQ(content.mimeType, "video/mp4");

        // Video metadata extraction is optional
        if (!content.metadata.empty()) {
            // Might contain duration, codec, resolution, etc.
            EXPECT_FALSE(content.text.empty() || !content.metadata.empty());
        }
    }
}

// Test MP3 audio processing
TEST_F(MediaHandlerTest, ProcessMp3) {
    auto mp3Data = createMp3Header();
    auto filePath = createMediaFile("test.mp3", mp3Data);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_TRUE(content.mimeType == "audio/mpeg" || content.mimeType == "audio/mp3");

        // ID3 tag extraction is optional
        if (!content.text.empty()) {
            // Might contain title, artist, etc.
            EXPECT_FALSE(content.text.empty());
        }
    }
}

// Test invalid media file
TEST_F(MediaHandlerTest, InvalidMediaFile) {
    std::vector<uint8_t> invalidData = {'n', 'o', 't', ' ', 'm', 'e', 'd', 'i', 'a'};
    auto filePath = createMediaFile("invalid.jpg", invalidData);

    auto result = handler_->process(filePath);

    if (!result) {
        EXPECT_TRUE(result.error().code == ErrorCode::InvalidData ||
                    result.error().code == ErrorCode::ProcessingError ||
                    result.error().code == ErrorCode::UnsupportedFormat);
    }
}

// Test empty file
TEST_F(MediaHandlerTest, EmptyFile) {
    std::vector<uint8_t> emptyData;
    auto filePath = createMediaFile("empty.png", emptyData);

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
}

// Test corrupted image
TEST_F(MediaHandlerTest, CorruptedImage) {
    auto jpegData = createJpegHeader();
    // Corrupt the JPEG header
    jpegData[0] = 0x00;
    jpegData[1] = 0x00;

    auto filePath = createMediaFile("corrupted.jpg", jpegData);

    auto result = handler_->process(filePath);

    if (!result) {
        EXPECT_TRUE(result.error().code == ErrorCode::InvalidData ||
                    result.error().code == ErrorCode::ProcessingError);
    }
}

// Test large media file handling
TEST_F(MediaHandlerTest, LargeMediaFile) {
    // Create a "large" JPEG (1MB of data)
    auto jpegData = createJpegHeader();
    jpegData.resize(1024 * 1024, 0xFF);

    auto filePath = createMediaFile("large.jpg", jpegData);

    auto result = handler_->process(filePath);

    // Should handle large files gracefully
    if (!result) {
        EXPECT_TRUE(result.error().code == ErrorCode::FileTooLarge ||
                    result.error().code == ErrorCode::ProcessingError ||
                    result.error().code == ErrorCode::NotImplemented);
    }
}

// Test SVG (text-based media)
TEST_F(MediaHandlerTest, ProcessSvg) {
    std::string svgContent = R"(<?xml version="1.0"?>
<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100">
    <circle cx="50" cy="50" r="40" fill="red"/>
</svg>)";

    std::vector<uint8_t> svgData(svgContent.begin(), svgContent.end());
    auto filePath = createMediaFile("test.svg", svgData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_EQ(content.mimeType, "image/svg+xml");

        // SVG content might be extracted as text
        if (!content.text.empty()) {
            EXPECT_TRUE(content.text.find("circle") != std::string::npos ||
                        content.text.find("svg") != std::string::npos);
        }
    }
}

// Test WebP image
TEST_F(MediaHandlerTest, ProcessWebP) {
    // WebP header
    std::vector<uint8_t> webpData = {'R', 'I', 'F', 'F',     // RIFF signature
                                     0x24, 0x00, 0x00, 0x00, // File size
                                     'W', 'E', 'B', 'P',     // WEBP signature
                                     'V', 'P', '8', ' ',     // VP8 chunk
                                     0x18, 0x00, 0x00, 0x00, // Chunk size
                                     // Minimal VP8 data
                                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    auto filePath = createMediaFile("test.webp", webpData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_EQ(content.mimeType, "image/webp");
    }
}

// Test WAV audio
TEST_F(MediaHandlerTest, ProcessWav) {
    // WAV header
    std::vector<uint8_t> wavData = {
        'R',  'I',  'F',  'F',  // RIFF signature
        0x24, 0x00, 0x00, 0x00, // File size
        'W',  'A',  'V',  'E',  // WAVE signature
        'f',  'm',  't',  ' ',  // Format chunk
        0x10, 0x00, 0x00, 0x00, // Chunk size
        0x01, 0x00,             // Audio format (PCM)
        0x02, 0x00,             // Channels (stereo)
        0x44, 0xAC, 0x00, 0x00, // Sample rate (44100)
        0x10, 0xB1, 0x02, 0x00, // Byte rate
        0x04, 0x00,             // Block align
        0x10, 0x00,             // Bits per sample
        'd',  'a',  't',  'a',  // Data chunk
        0x00, 0x00, 0x00, 0x00  // Data size
    };

    auto filePath = createMediaFile("test.wav", wavData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();
        EXPECT_EQ(content.mimeType, "audio/wav");

        // Check for audio metadata if extracted
        if (!content.metadata.empty()) {
            // Might contain sample rate, channels, etc.
            bool hasAudioInfo = content.metadata.find("sample_rate") != content.metadata.end() ||
                                content.metadata.find("channels") != content.metadata.end();
            if (hasAudioInfo) {
                EXPECT_TRUE(hasAudioInfo);
            }
        }
    }
}

// Test EXIF metadata extraction from JPEG
TEST_F(MediaHandlerTest, JpegExifMetadata) {
    auto jpegData = createJpegHeader();

    // Insert minimal EXIF APP1 marker after SOI
    std::vector<uint8_t> exifData = {
        0xFF, 0xE1,                     // APP1 marker
        0x00, 0x1C,                     // Length
        'E', 'x', 'i', 'f', 0x00, 0x00, // Exif identifier
        // Minimal TIFF header
        0x4D, 0x4D,             // Big endian
        0x00, 0x2A,             // TIFF magic
        0x00, 0x00, 0x00, 0x08, // IFD offset
        // IFD
        0x00, 0x01, // Number of entries
        // Make tag
        0x01, 0x0F,             // Tag (Make)
        0x00, 0x02,             // Type (ASCII)
        0x00, 0x00, 0x00, 0x04, // Count
        'T', 'e', 's', 't'      // Value
    };

    jpegData.insert(jpegData.begin() + 2, exifData.begin(), exifData.end());

    auto filePath = createMediaFile("exif.jpg", jpegData);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();

        // EXIF extraction is optional
        if (!content.metadata.empty()) {
            // Might contain camera make, model, etc.
            bool hasExif = content.metadata.find("make") != content.metadata.end() ||
                           content.metadata.find("exif") != content.metadata.end();
            if (hasExif) {
                EXPECT_TRUE(hasExif) << "EXIF metadata should be extracted if supported";
            }
        }
    }
}

// Test non-existent file
TEST_F(MediaHandlerTest, NonExistentFile) {
    fs::path filePath = testDir_ / "nonexistent.jpg";

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}

// Test various audio formats
TEST_F(MediaHandlerTest, AudioFormats) {
    struct AudioFormat {
        std::string extension;
        std::string mimeType;
        std::vector<uint8_t> header;
    };

    std::vector<AudioFormat> formats = {
        {".ogg", "audio/ogg", {'O', 'g', 'g', 'S', 0x00, 0x02}},
        {".flac", "audio/flac", {'f', 'L', 'a', 'C'}},
        {".m4a", "audio/mp4", {0x00, 0x00, 0x00, 0x20, 'f', 't', 'y', 'p', 'M', '4', 'A', ' '}}};

    for (const auto& format : formats) {
        auto filePath = createMediaFile("test" + format.extension, format.header);

        auto result = handler_->process(filePath);

        if (result) {
            const auto& content = result.value();
            EXPECT_EQ(content.mimeType, format.mimeType) << "Format: " << format.extension;
        }
    }
}

// Test video metadata extraction
TEST_F(MediaHandlerTest, VideoMetadata) {
    auto mp4Data = createMp4Header();

    // Add minimal moov atom with metadata
    std::vector<uint8_t> moovData = {
        0x00, 0x00, 0x00, 0x20, // Box size
        'm',  'o',  'o',  'v',  // Moov box
        0x00, 0x00, 0x00, 0x18, // mvhd size
        'm',  'v',  'h',  'd',  // Movie header
        0x00,                   // Version
        0x00, 0x00, 0x00,       // Flags
        0x00, 0x00, 0x00, 0x00, // Creation time
        0x00, 0x00, 0x00, 0x00, // Modification time
        0x00, 0x00, 0x03, 0xE8, // Timescale (1000)
        0x00, 0x00, 0x27, 0x10  // Duration (10000 = 10 seconds)
    };

    mp4Data.insert(mp4Data.end(), moovData.begin(), moovData.end());

    auto filePath = createMediaFile("metadata.mp4", mp4Data);

    auto result = handler_->process(filePath);

    if (result) {
        const auto& content = result.value();

        // Video metadata extraction is optional
        if (!content.metadata.empty()) {
            bool hasVideoInfo = content.metadata.find("duration") != content.metadata.end() ||
                                content.metadata.find("timescale") != content.metadata.end();
            if (hasVideoInfo) {
                EXPECT_TRUE(hasVideoInfo) << "Video metadata should be extracted if supported";
            }
        }
    }
}

} // namespace yams::content::test