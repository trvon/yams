#include <filesystem>
#include <fstream>
#include <random>
#include <vector>
#include <gtest/gtest.h>
#include <yams/content/handlers/binary_handler.h>

namespace yams::content::test {

namespace fs = std::filesystem;

class BinaryHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("binary_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        handler_ = std::make_unique<BinaryHandler>();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(testDir_, ec);
    }

    fs::path createBinaryFile(const std::string& name, const std::vector<uint8_t>& content) {
        fs::path filePath = testDir_ / name;
        std::ofstream file(filePath, std::ios::binary);
        file.write(reinterpret_cast<const char*>(content.data()), content.size());
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
    std::unique_ptr<BinaryHandler> handler_;
};

// Test format detection
TEST_F(BinaryHandlerTest, CanHandle) {
    // Binary formats
    EXPECT_TRUE(handler_->canHandle(".bin"));
    EXPECT_TRUE(handler_->canHandle(".dat"));
    EXPECT_TRUE(handler_->canHandle(".exe"));
    EXPECT_TRUE(handler_->canHandle(".dll"));
    EXPECT_TRUE(handler_->canHandle(".so"));
    EXPECT_TRUE(handler_->canHandle(".dylib"));
    EXPECT_TRUE(handler_->canHandle(".o"));
    EXPECT_TRUE(handler_->canHandle(".obj"));
    EXPECT_TRUE(handler_->canHandle(".class"));
    EXPECT_TRUE(handler_->canHandle(".pyc"));
    EXPECT_TRUE(handler_->canHandle(".wasm"));

    // Archive formats (often handled as binary)
    EXPECT_TRUE(handler_->canHandle(".zip"));
    EXPECT_TRUE(handler_->canHandle(".tar"));
    EXPECT_TRUE(handler_->canHandle(".gz"));
    EXPECT_TRUE(handler_->canHandle(".7z"));
    EXPECT_TRUE(handler_->canHandle(".rar"));

    // Should act as catch-all for unknown extensions
    EXPECT_TRUE(handler_->canHandle(".xyz"));
    EXPECT_TRUE(handler_->canHandle(".unknown"));
    EXPECT_TRUE(handler_->canHandle(""));
}

// Test processing random binary data
TEST_F(BinaryHandlerTest, ProcessRandomBinary) {
    auto binaryData = generateRandomData(256);
    auto filePath = createBinaryFile("random.bin", binaryData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result) << "Binary handler should process any file";

    const auto& content = result.value();
    EXPECT_EQ(content.mimeType, "application/octet-stream");

    // Binary handler might extract metadata about file
    if (!content.metadata.empty()) {
        // Should at least have file size
        EXPECT_TRUE(content.metadata.find("size") != content.metadata.end() ||
                    content.metadata.find("file_size") != content.metadata.end());
    }
}

// Test executable file detection (PE format)
TEST_F(BinaryHandlerTest, ProcessWindowsExecutable) {
    // Minimal PE header
    std::vector<uint8_t> peData = {
        'M',  'Z',  // DOS signature
        0x90, 0x00, // Bytes on last page
        0x03, 0x00, // Pages in file
        0x00, 0x00, // Relocations
        0x04, 0x00, // Size of header
        0x00, 0x00, // Minimum extra
        0xFF, 0xFF, // Maximum extra
        0x00, 0x00, // Initial SS
        0xB8, 0x00, // Initial SP
        0x00, 0x00, // Checksum
        0x00, 0x00, // Initial IP
        0x00, 0x00, // Initial CS
        0x40, 0x00, // Relocation table offset
        0x00, 0x00, // Overlay number
        // ... simplified, would have PE header at offset 0x3C
    };

    // Pad to make it look more like real PE
    peData.resize(512, 0);
    peData[0x3C] = 0x80; // PE header offset
    peData[0x80] = 'P';
    peData[0x81] = 'E';
    peData[0x82] = 0x00;
    peData[0x83] = 0x00;

    auto filePath = createBinaryFile("test.exe", peData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as executable
    bool isExecutable = content.mimeType == "application/x-msdownload" ||
                        content.mimeType == "application/x-executable" ||
                        content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isExecutable);

    // Might extract PE header info
    if (!content.metadata.empty() && content.metadata.find("format") != content.metadata.end()) {
        EXPECT_TRUE(content.metadata.at("format") == "PE" ||
                    content.metadata.at("format") == "PE32");
    }
}

// Test ELF executable detection (Linux/Unix)
TEST_F(BinaryHandlerTest, ProcessElfExecutable) {
    // ELF header
    std::vector<uint8_t> elfData = {
        0x7F, 'E',  'L',  'F',                    // ELF magic
        0x02,                                     // 64-bit
        0x01,                                     // Little endian
        0x01,                                     // Current version
        0x00,                                     // System V ABI
        0x00,                                     // ABI version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Padding
        0x02, 0x00,                               // Executable file
        0x3E, 0x00,                               // x86-64
        0x01, 0x00, 0x00, 0x00,                   // Version
        // ... rest of ELF header
    };

    elfData.resize(64, 0); // Minimal ELF header size

    auto filePath = createBinaryFile("test.so", elfData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as shared library or executable
    bool isElf = content.mimeType == "application/x-sharedlib" ||
                 content.mimeType == "application/x-executable" ||
                 content.mimeType == "application/x-elf" ||
                 content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isElf);
}

// Test Mach-O detection (macOS)
TEST_F(BinaryHandlerTest, ProcessMachO) {
    // Mach-O header (64-bit)
    std::vector<uint8_t> machoData = {
        0xCF, 0xFA, 0xED, 0xFE, // Mach-O 64-bit magic
        0x07, 0x00, 0x00, 0x01, // CPU type (x86_64)
        0x03, 0x00, 0x00, 0x00, // CPU subtype
        0x02, 0x00, 0x00, 0x00, // File type (executable)
        0x10, 0x00, 0x00, 0x00, // Number of load commands
        0x00, 0x00, 0x00, 0x00, // Size of load commands
        0x00, 0x00, 0x00, 0x00, // Flags
        0x00, 0x00, 0x00, 0x00, // Reserved
    };

    auto filePath = createBinaryFile("test.dylib", machoData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as Mach-O
    bool isMachO = content.mimeType == "application/x-mach-binary" ||
                   content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isMachO);
}

// Test Java class file
TEST_F(BinaryHandlerTest, ProcessJavaClass) {
    // Java class file header
    std::vector<uint8_t> classData = {
        0xCA, 0xFE, 0xBA, 0xBE, // Java class magic
        0x00, 0x00,             // Minor version
        0x00, 0x34,             // Major version (Java 8)
        0x00, 0x10,             // Constant pool count
        // ... simplified constant pool
    };

    classData.resize(100, 0);

    auto filePath = createBinaryFile("Test.class", classData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as Java class
    bool isJavaClass = content.mimeType == "application/java-vm" ||
                       content.mimeType == "application/x-java-class" ||
                       content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isJavaClass);
}

// Test Python bytecode
TEST_F(BinaryHandlerTest, ProcessPythonBytecode) {
    // Python 3.8 bytecode header
    std::vector<uint8_t> pycData = {
        0x55, 0x0D, 0x0D, 0x0A, // Python 3.8 magic
        0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x00, // File size
        // ... bytecode would follow
    };

    pycData.resize(50, 0);

    auto filePath = createBinaryFile("test.pyc", pycData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as Python bytecode
    bool isPyc = content.mimeType == "application/x-python-code" ||
                 content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isPyc);
}

// Test WebAssembly module
TEST_F(BinaryHandlerTest, ProcessWebAssembly) {
    // WASM header
    std::vector<uint8_t> wasmData = {
        0x00, 0x61, 0x73, 0x6D, // \0asm magic
        0x01, 0x00, 0x00, 0x00, // Version
        // ... module sections
    };

    auto filePath = createBinaryFile("module.wasm", wasmData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as WASM
    bool isWasm =
        content.mimeType == "application/wasm" || content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isWasm);
}

// Test ZIP archive
TEST_F(BinaryHandlerTest, ProcessZipArchive) {
    // ZIP header
    std::vector<uint8_t> zipData = {
        'P',  'K',  0x03, 0x04, // Local file header signature
        0x14, 0x00,             // Version needed
        0x00, 0x00,             // Flags
        0x00, 0x00,             // Compression method
        0x00, 0x00,             // Time
        0x00, 0x00,             // Date
        0x00, 0x00, 0x00, 0x00, // CRC-32
        0x00, 0x00, 0x00, 0x00, // Compressed size
        0x00, 0x00, 0x00, 0x00, // Uncompressed size
        0x00, 0x00,             // Filename length
        0x00, 0x00,             // Extra field length
    };

    auto filePath = createBinaryFile("archive.zip", zipData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as ZIP
    bool isZip = content.mimeType == "application/zip" ||
                 content.mimeType == "application/x-zip-compressed" ||
                 content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isZip);
}

// Test TAR archive
TEST_F(BinaryHandlerTest, ProcessTarArchive) {
    // TAR header (simplified)
    std::vector<uint8_t> tarData(512, 0);

    // File name
    std::string filename = "test.txt";
    std::copy(filename.begin(), filename.end(), tarData.begin());

    // ustar magic at offset 257
    tarData[257] = 'u';
    tarData[258] = 's';
    tarData[259] = 't';
    tarData[260] = 'a';
    tarData[261] = 'r';

    auto filePath = createBinaryFile("archive.tar", tarData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Might detect as TAR
    bool isTar =
        content.mimeType == "application/x-tar" || content.mimeType == "application/octet-stream";
    EXPECT_TRUE(isTar);
}

// Test empty file
TEST_F(BinaryHandlerTest, EmptyFile) {
    std::vector<uint8_t> emptyData;
    auto filePath = createBinaryFile("empty.bin", emptyData);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result) << "Binary handler should handle empty files";

    const auto& content = result.value();
    EXPECT_EQ(content.mimeType, "application/octet-stream");

    // Should note that file is empty
    if (!content.metadata.empty()) {
        if (content.metadata.find("size") != content.metadata.end()) {
            EXPECT_EQ(content.metadata.at("size"), "0");
        }
    }
}

// Test large binary file
TEST_F(BinaryHandlerTest, LargeBinaryFile) {
    // Create 10MB of random data
    auto largeData = generateRandomData(10 * 1024 * 1024);
    auto filePath = createBinaryFile("large.bin", largeData);

    auto result = handler_->process(filePath);

    // Binary handler should handle large files
    ASSERT_TRUE(result) << "Should handle large binary files";

    const auto& content = result.value();

    // Should not load entire content as text
    EXPECT_TRUE(content.text.empty() || content.text.size() < 1000)
        << "Should not load large binary as text";

    // Should note file size in metadata
    if (!content.metadata.empty()) {
        if (content.metadata.find("size") != content.metadata.end()) {
            size_t size = std::stoull(content.metadata.at("size"));
            EXPECT_EQ(size, largeData.size());
        }
    }
}

// Test file with null bytes
TEST_F(BinaryHandlerTest, FileWithNullBytes) {
    std::vector<uint8_t> data = {'H', 'e', 'l', 'l', 'o', 0x00, 'W', 'o', 'r', 'l', 'd', 0x00};
    auto filePath = createBinaryFile("nulls.bin", data);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Should handle null bytes properly
    EXPECT_EQ(content.mimeType, "application/octet-stream");
}

// Test entropy calculation (for detecting compressed/encrypted data)
TEST_F(BinaryHandlerTest, EntropyDetection) {
    // Low entropy (repeated pattern)
    std::vector<uint8_t> lowEntropy(1024, 0xAA);
    auto lowEntropyFile = createBinaryFile("low_entropy.bin", lowEntropy);

    // High entropy (random data - looks compressed/encrypted)
    auto highEntropy = generateRandomData(1024);
    auto highEntropyFile = createBinaryFile("high_entropy.bin", highEntropy);

    auto lowResult = handler_->process(lowEntropyFile);
    auto highResult = handler_->process(highEntropyFile);

    ASSERT_TRUE(lowResult);
    ASSERT_TRUE(highResult);

    // Binary handler might calculate entropy
    if (!lowResult.value().metadata.empty() &&
        lowResult.value().metadata.find("entropy") != lowResult.value().metadata.end()) {
        double lowEnt = std::stod(lowResult.value().metadata.at("entropy"));
        double highEnt = std::stod(highResult.value().metadata.at("entropy"));

        EXPECT_LT(lowEnt, highEnt) << "Random data should have higher entropy";
    }
}

// Test non-existent file
TEST_F(BinaryHandlerTest, NonExistentFile) {
    fs::path filePath = testDir_ / "nonexistent.bin";

    auto result = handler_->process(filePath);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}

// Test file type heuristics
TEST_F(BinaryHandlerTest, FileTypeHeuristics) {
    struct TestCase {
        std::string name;
        std::vector<uint8_t> header;
        std::string expectedType;
    };

    std::vector<TestCase> testCases = {
        {"sqlite.db",
         {'S', 'Q', 'L', 'i', 't', 'e', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '3', 0x00},
         "application/x-sqlite3"},
        {"data.json",
         {'{', '"', 'k', 'e', 'y', '"', ':', ' ', '"', 'v', 'a', 'l', 'u', 'e', '"', '}'},
         "application/json"},
        {"script.sh", {'#', '!', '/', 'b', 'i', 'n', '/', 's', 'h', '\n'}, "application/x-sh"},
        {"data.xml",
         {'<', '?', 'x', 'm', 'l', ' ', 'v', 'e', 'r', 's', 'i', 'o', 'n', '='},
         "application/xml"}};

    for (const auto& tc : testCases) {
        auto filePath = createBinaryFile(tc.name, tc.header);
        auto result = handler_->process(filePath);

        if (result) {
            const auto& content = result.value();

            // Binary handler might detect some text formats
            // But it's also OK to return application/octet-stream
            bool isExpectedOrBinary = content.mimeType == tc.expectedType ||
                                      content.mimeType == "application/octet-stream";

            EXPECT_TRUE(isExpectedOrBinary) << "File: " << tc.name << ", Got: " << content.mimeType
                                            << ", Expected: " << tc.expectedType;
        }
    }
}

// Test metadata extraction
TEST_F(BinaryHandlerTest, MetadataExtraction) {
    auto data = generateRandomData(12345);
    auto filePath = createBinaryFile("metadata_test.bin", data);

    auto result = handler_->process(filePath);

    ASSERT_TRUE(result);

    const auto& content = result.value();

    // Should extract basic metadata
    if (!content.metadata.empty()) {
        // Common metadata fields
        std::vector<std::string> possibleFields = {"size",        "file_size", "bytes",
                                                   "modified",    "created",   "accessed",
                                                   "permissions", "readable",  "writable"};

        bool hasMetadata = false;
        for (const auto& field : possibleFields) {
            if (content.metadata.find(field) != content.metadata.end()) {
                hasMetadata = true;
                break;
            }
        }

        EXPECT_TRUE(hasMetadata) << "Should extract at least some metadata";
    }
}

} // namespace yams::content::test