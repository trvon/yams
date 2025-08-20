#include <chrono>
#include <concepts>
#include <filesystem>
#include <format>
#include <fstream>
#include <memory>
#include <ranges>          // C++20
#include <source_location> // C++20
#include <span>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <yams/content/audio_content_handler.h>
#include <yams/content/binary_content_handler.h>
#include <yams/content/content_handler_registry.h>
#include <yams/content/image_content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content::test {

using namespace std::string_literals;
using namespace std::chrono_literals;

/**
 * @brief C++20 concept for testable content handlers
 */
template <typename T>
concept TestableHandler =
    requires(T handler, detection::FileSignature sig, std::filesystem::path path) {
        { handler.name() } -> std::convertible_to<std::string>;
        { handler.canHandle(sig) } -> std::same_as<bool>;
        { handler.supportedMimeTypes() } -> std::convertible_to<std::vector<std::string>>;
        { handler.process(path) } -> std::convertible_to<Result<ContentResult>>;
        { handler.priority() } -> std::convertible_to<int>;
    };

/**
 * @brief C++20 concept for content result validators
 */
template <typename T>
concept ContentValidator = requires(T validator, const ContentResult& result) {
    { validator.validate(result) } -> std::convertible_to<bool>;
    { validator.getErrorMessage() } -> std::convertible_to<std::string>;
};

/**
 * @brief Test fixture
class ContentHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        LogTestInfo();
        setupTestEnvironment();
        initializeHandlers();
    }

    void TearDown() override {
        cleanupTestFiles();
        LogTestCompletion();
    }

    /**
     * @brief Log test information using C++20 source_location
     */
void LogTestInfo(const std::source_location& loc = std::source_location::current()) {
    const auto testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
    SPDLOG_INFO("Running test: {}.{} at {}:{} in {}", testInfo->test_suite_name(), testInfo->name(),
                std::filesystem::path(loc.file_name()).filename().string(), loc.line(),
                loc.function_name());
}

/**
 * @brief Log test completion
 */
void LogTestCompletion() {
    const auto testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
    const auto result = testInfo->result();
    SPDLOG_INFO("Test completed: {}.{} - {} ({}ms)", testInfo->test_suite_name(), testInfo->name(),
                result->Passed() ? "PASSED" : "FAILED",
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - testStartTime_)
                    .count());
}

/**
 * @brief Setup test environment
 */
void setupTestEnvironment() {
    testStartTime_ = std::chrono::steady_clock::now();
    testDataDir_ = std::filesystem::temp_directory_path() / "yams_test_data";
    std::filesystem::create_directories(testDataDir_);

    // Initialize FileTypeDetector
    auto& detector = detection::FileTypeDetector::instance();
    auto initResult = detector.initializeWithMagicNumbers();
    ASSERT_TRUE(initResult) << "Failed to initialize FileTypeDetector: "
                            << initResult.error().message;
}

/**
 * @brief Initialize content handlers
 */
void initializeHandlers() {
    auto& registry = ContentHandlerRegistry::instance();
    registry.clear();
    registry.initializeDefaultHandlers();

    // Verify handlers are registered
    ASSERT_TRUE(registry.hasHandler("ImageContentHandler"));
    ASSERT_TRUE(registry.hasHandler("BinaryContentHandler"));
}

/**
 * @brief Create test file with specific content
 */
template <std::ranges::range R>
std::filesystem::path createTestFile(std::string_view filename, R&& content) {
    auto filepath = testDataDir_ / filename;
    std::ofstream file(filepath, std::ios::binary);
    EXPECT_TRUE(file.is_open()) << std::format("Failed to create test file: {}", filepath.string());

    if constexpr (std::same_as<std::ranges::range_value_t<R>, std::byte>) {
        // Binary content
        std::ranges::copy(content, std::ostreambuf_iterator<char>(file));
    } else {
        // Text content
        std::ranges::copy(content, std::ostreambuf_iterator<char>(file));
    }

    file.close();
    testFiles_.push_back(filepath);
    return filepath;
}

/**
 * @brief Create test image file with PNG signature
 */
std::filesystem::path createTestPngFile(std::string_view filename = "test.png") {
    // PNG signature: 89 50 4E 47 0D 0A 1A 0A
    constexpr std::array<std::byte, 8> pngSignature{
        std::byte{0x89}, std::byte{0x50}, std::byte{0x4E}, std::byte{0x47},
        std::byte{0x0D}, std::byte{0x0A}, std::byte{0x1A}, std::byte{0x0A}};

    // Add minimal PNG IHDR chunk for a 1x1 image
    constexpr std::array<std::byte, 25> pngHeader{
        // IHDR chunk length (13 bytes)
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x0D},
        // IHDR chunk type
        std::byte{0x49}, std::byte{0x48}, std::byte{0x44}, std::byte{0x52},
        // Width (1)
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
        // Height (1)
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
        // Bit depth, color type, compression, filter, interlace
        std::byte{0x08}, std::byte{0x02}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        // CRC32
        std::byte{0x90}, std::byte{0x77}, std::byte{0x53}, std::byte{0xDE}, std::byte{0x00}};

    std::vector<std::byte> pngData;
    pngData.reserve(pngSignature.size() + pngHeader.size());
    std::ranges::copy(pngSignature, std::back_inserter(pngData));
    std::ranges::copy(pngHeader, std::back_inserter(pngData));

    return createTestFile(filename, pngData);
}

/**
 * @brief Create test JPEG file with JPEG signature
 */
std::filesystem::path createTestJpegFile(std::string_view filename = "test.jpg") {
    // JPEG signature: FF D8 FF
    constexpr std::array<std::byte, 20> jpegData{
        std::byte{0xFF}, std::byte{0xD8}, std::byte{0xFF}, std::byte{0xE0}, std::byte{0x00},
        std::byte{0x10}, std::byte{0x4A}, std::byte{0x46}, std::byte{0x49}, std::byte{0x46},
        std::byte{0x00}, std::byte{0x01}, std::byte{0x01}, std::byte{0x01}, std::byte{0x00},
        std::byte{0x48}, std::byte{0x00}, std::byte{0x48}, std::byte{0xFF}, std::byte{0xD9}};

    return createTestFile(filename, jpegData);
}

/**
 * @brief Clean up test files
 */
void cleanupTestFiles() {
    for (const auto& file : testFiles_) {
        std::error_code ec;
        std::filesystem::remove(file, ec);
        if (ec) {
            SPDLOG_WARN("Failed to remove test file {}: {}", file.string(), ec.message());
        }
    }
    testFiles_.clear();

    std::error_code ec;
    std::filesystem::remove_all(testDataDir_, ec);
}

/**
 * @brief Test handler with concept validation
 */
template <TestableHandler HandlerType>
void testHandlerBasics(std::unique_ptr<HandlerType> handler,
                       const std::vector<std::string>& expectedMimeTypes) {
    ASSERT_NE(handler, nullptr);

    // Test basic properties
    EXPECT_FALSE(handler->name().empty());
    EXPECT_GE(handler->priority(), 0);

    // Test MIME types
    auto supportedTypes = handler->supportedMimeTypes();
    EXPECT_FALSE(supportedTypes.empty());

    // C++20: Use ranges to check MIME type coverage
    for (const auto& expectedType : expectedMimeTypes) {
        EXPECT_TRUE(std::ranges::any_of(supportedTypes,
                                        [&expectedType](const std::string& supportedType) {
                                            return supportedType == expectedType ||
                                                   supportedType == "*/*";
                                        }))
            << std::format("Handler {} does not support expected MIME type: {}", handler->name(),
                           expectedType);
    }
}

/**
 * @brief Validate content result using concepts
 */
template <ContentValidator ValidatorType>
void validateResult(const ContentResult& result, ValidatorType&& validator) {
    EXPECT_TRUE(validator.validate(result)) << validator.getErrorMessage();
}

protected:
std::filesystem::path testDataDir_;
std::vector<std::filesystem::path> testFiles_;
std::chrono::steady_clock::time_point testStartTime_;
};

/**
 * @brief Content result validator implementations
 */
struct BasicContentValidator {
    std::string errorMessage_;

    bool validate(const ContentResult& result) {
        if (result.handlerName.empty()) {
            errorMessage_ = "Handler name is empty";
            return false;
        }
        if (result.contentType.empty()) {
            errorMessage_ = "Content type is empty";
            return false;
        }
        return true;
    }

    std::string getErrorMessage() const { return errorMessage_; }
};

struct ImageContentValidator {
    std::string errorMessage_;

    bool validate(const ContentResult& result) {
        BasicContentValidator basic;
        if (!basic.validate(result)) {
            errorMessage_ = basic.getErrorMessage();
            return false;
        }

        if (!result.imageData.has_value()) {
            errorMessage_ = "Missing image metadata";
            return false;
        }

        const auto& imgData = result.imageData.value();
        if (imgData.width == 0 || imgData.height == 0) {
            errorMessage_ =
                std::format("Invalid image dimensions: {}x{}", imgData.width, imgData.height);
            return false;
        }

        return true;
    }

    std::string getErrorMessage() const { return errorMessage_; }
};

/**
 * @brief Test ImageContentHandler
 */
TEST_F(ContentHandlerTest, ImageHandlerBasicFunctionality) {
    auto imageHandler = createImageHandler();

    // Test basic handler properties
    testHandlerBasics(std::move(imageHandler), {"image/png", "image/jpeg", "image/gif"});
}

TEST_F(ContentHandlerTest, ImageHandlerPngProcessing) {
    auto imageHandler = createImageHandler();
    auto testFile = createTestPngFile();

    // Process the PNG file
    auto result = imageHandler->process(testFile);
    ASSERT_TRUE(result) << std::format("Failed to process PNG file: {}", result.error().message);

    // Validate result using concepts
    validateResult(result.value(), ImageContentValidator{});

    // Check specific PNG properties
    const auto& contentResult = result.value();
    EXPECT_EQ(contentResult.handlerName, "ImageContentHandler");
    EXPECT_TRUE(contentResult.contentType.starts_with("image/"));
    EXPECT_TRUE(contentResult.imageData.has_value());

    if (contentResult.imageData) {
        const auto& imgData = contentResult.imageData.value();
        EXPECT_GT(imgData.width, 0);
        EXPECT_GT(imgData.height, 0);
    }
}

TEST_F(ContentHandlerTest, ImageHandlerJpegProcessing) {
    auto imageHandler = createImageHandler();
    auto testFile = createTestJpegFile();

    auto result = imageHandler->process(testFile);
    ASSERT_TRUE(result) << std::format("Failed to process JPEG file: {}", result.error().message);

    validateResult(result.value(), ImageContentValidator{});
}

TEST_F(ContentHandlerTest, ContentHandlerRegistryIntegration) {
    auto& registry = ContentHandlerRegistry::instance();

    // Test handler selection using ranges
    auto& detector = detection::FileTypeDetector::instance();
    auto testFile = createTestPngFile();

    auto signatureResult = detector.detectFromFile(testFile);
    ASSERT_TRUE(signatureResult);

    auto handler = registry.getHandler(signatureResult.value());
    ASSERT_NE(handler, nullptr);
    EXPECT_EQ(handler->name(), "ImageContentHandler");

    // Test processing through registry
    auto result = handler->process(testFile);
    ASSERT_TRUE(result);
    validateResult(result.value(), ImageContentValidator{});
}

TEST_F(ContentHandlerTest, BinaryHandlerFallback) {
    auto binaryHandler = createBinaryHandler();

    // Create a random binary file
    std::vector<std::byte> randomData(1024);
    std::ranges::generate(randomData, []() { return static_cast<std::byte>(rand() % 256); });

    auto testFile = createTestFile("random.bin", randomData);

    auto result = binaryHandler->process(testFile);
    ASSERT_TRUE(result) << std::format("Failed to process binary file: {}", result.error().message);

    validateResult(result.value(), BasicContentValidator{});

    const auto& contentResult = result.value();
    EXPECT_EQ(contentResult.handlerName, "BinaryContentHandler");
    EXPECT_FALSE(contentResult.metadata.empty());
}

TEST_F(ContentHandlerTest, HandlerPriorityOrdering) {
    auto& registry = ContentHandlerRegistry::instance();

    // Create test signature that multiple handlers can handle
    detection::FileSignature genericSignature{.mimeType = "application/octet-stream",
                                              .fileType = "binary",
                                              .description = "Generic binary",
                                              .isBinary = true,
                                              .confidence = 0.5f};

    auto compatibleHandlers = registry.getCompatibleHandlers(genericSignature);
    EXPECT_FALSE(compatibleHandlers.empty());

    // Verify handlers are sorted by priority (highest first)
    auto priorities =
        compatibleHandlers |
        std::views::transform([](const auto& handler) { return handler->priority(); }) |
        std::ranges::to<std::vector>();

    EXPECT_TRUE(std::ranges::is_sorted(priorities, std::greater<>{}))
        << "Handlers are not sorted by priority in descending order";
}

/**
 * @brief Performance test using C++20 chrono
 */
TEST_F(ContentHandlerTest, PerformanceTest) {
    constexpr size_t numFiles = 10;
    constexpr auto maxProcessingTime = 100ms;

    auto imageHandler = createImageHandler();
    std::vector<std::filesystem::path> testFiles;

    // Create multiple test files
    for (size_t i = 0; i < numFiles; ++i) {
        testFiles.push_back(createTestPngFile(std::format("test_{}.png", i)));
    }

    auto start = std::chrono::steady_clock::now();

    // Process all files
    for (const auto& file : testFiles) {
        auto result = imageHandler->process(file);
        EXPECT_TRUE(result) << std::format("Failed to process {}: {}", file.filename().string(),
                                           result.error().message);
    }

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    SPDLOG_INFO("Processed {} files in {}ms (avg: {}ms per file)", numFiles, duration.count(),
                duration.count() / numFiles);

    // Performance assertion
    auto avgTimePerFile = duration / numFiles;
    EXPECT_LT(avgTimePerFile, maxProcessingTime)
        << std::format("Average processing time {}ms exceeds limit {}ms", avgTimePerFile.count(),
                       maxProcessingTime.count());
}

/**
 * @brief Error handling test using
 */
TEST_F(ContentHandlerTest, ErrorHandling) {
    auto imageHandler = createImageHandler();

    // Test with non-existent file
    auto nonExistentFile = testDataDir_ / "does_not_exist.png";
    auto result = imageHandler->process(nonExistentFile);
    EXPECT_FALSE(result);
    EXPECT_TRUE(result.error().code == ErrorCode::FileNotFound);

    // Test with empty file
    auto emptyFile = createTestFile("empty.png", std::vector<std::byte>{});
    result = imageHandler->process(emptyFile);
    EXPECT_FALSE(result);

    // Test with invalid image data
    std::vector<std::byte> invalidData(100, std::byte{0x00});
    auto invalidFile = createTestFile("invalid.png", invalidData);
    result = imageHandler->process(invalidFile);
    EXPECT_FALSE(result);
}

/**
 * @brief Thread safety test using C++20 jthread
 */
TEST_F(ContentHandlerTest, ThreadSafetyTest) {
    constexpr size_t numThreads = 4;
    constexpr size_t filesPerThread = 5;

    auto& registry = ContentHandlerRegistry::instance();
    std::vector<std::jthread> threads;
    std::atomic<size_t> successCount{0};
    std::atomic<size_t> errorCount{0};

    // Create test files
    std::vector<std::filesystem::path> testFiles;
    for (size_t i = 0; i < numThreads * filesPerThread; ++i) {
        testFiles.push_back(createTestPngFile(std::format("thread_test_{}.png", i)));
    }

    // Launch threads
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t](std::stop_token stop) {
            for (size_t i = 0; i < filesPerThread; ++i) {
                if (stop.stop_requested())
                    break;

                size_t fileIndex = t * filesPerThread + i;
                auto& detector = detection::FileTypeDetector::instance();
                auto signatureResult = detector.detectFromFile(testFiles[fileIndex]);

                if (signatureResult) {
                    auto handler = registry.getHandler(signatureResult.value());
                    if (handler) {
                        auto result = handler->process(testFiles[fileIndex]);
                        if (result) {
                            successCount++;
                        } else {
                            errorCount++;
                        }
                    }
                } else {
                    errorCount++;
                }
            }
        });
    }

    // Wait for completion
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(successCount.load(), numThreads * filesPerThread);
    EXPECT_EQ(errorCount.load(), 0);

    SPDLOG_INFO("Thread safety test completed: {} successes, {} errors", successCount.load(),
                errorCount.load());
}

} // namespace yams::content::test