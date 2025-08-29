#pragma once

#include <chrono>
#include <concepts>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>
#include <yams/content/content_handler.h>
#include <yams/core/format.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

/**
 * @brief concept for image format validation
 */
template <typename T>
concept ImageFormatTraits = requires(T t) {
    { t.supportedExtensions() } -> std::convertible_to<std::vector<std::string>>;
    { t.maxDimension() } -> std::convertible_to<uint32_t>;
    { t.hasAlpha() } -> std::convertible_to<bool>;
};

/**
 * @brief concept for image data validation
 */
template <typename T>
concept ImageDataProvider = requires(T t) {
    { t.data() } -> std::convertible_to<std::span<const std::byte>>;
    { t.size() } -> std::convertible_to<size_t>;
};

/**
 * @brief Image processing configuration
 */
struct ImageProcessingConfig {
    bool extractExif = true;
    bool generateThumbnail = false;
    bool detectText = false; // OCR capability
    uint32_t thumbnailSize = 256;
    uint32_t maxImageSize = 100 * 1024 * 1024;          // 100MB
    std::chrono::milliseconds processingTimeout{10000}; // 10s
};

/**
 * @brief Image format enumeration
 */
enum class ImageFormat { Unknown, JPEG, PNG, GIF, BMP, TIFF, WEBP, SVG, ICO, PSD, RAW };

/**
 * @brief Convert image format to string
 */
[[nodiscard]] constexpr std::string_view formatToString(ImageFormat format) noexcept {
    switch (format) {
        case ImageFormat::JPEG:
            return "JPEG";
        case ImageFormat::PNG:
            return "PNG";
        case ImageFormat::GIF:
            return "GIF";
        case ImageFormat::BMP:
            return "BMP";
        case ImageFormat::TIFF:
            return "TIFF";
        case ImageFormat::WEBP:
            return "WebP";
        case ImageFormat::SVG:
            return "SVG";
        case ImageFormat::ICO:
            return "ICO";
        case ImageFormat::PSD:
            return "PSD";
        case ImageFormat::RAW:
            return "RAW";
        default:
            return "Unknown";
    }
}

/**
 * @brief Extended image metadata
 */
struct ExtendedImageMetadata : public ImageMetadata {
    ImageFormat format = ImageFormat::Unknown;
    std::optional<std::chrono::system_clock::time_point> dateTaken;
    std::optional<std::string> cameraModel;
    std::optional<std::string> lens;
    std::optional<double> focalLength;
    std::optional<double> aperture;
    std::optional<double> shutterSpeed;
    std::optional<uint32_t> iso;
    std::optional<std::pair<double, double>> gpsCoordinates; // lat, lon
    std::vector<std::string> keywords;
    std::vector<std::string> faces; // Detected face regions
    float confidence = 1.0f;        // Detection confidence from FileTypeDetector

    // C++20: Helper methods with concepts
    template <std::convertible_to<std::string> T> void addKeyword(T&& keyword) {
        keywords.emplace_back(std::forward<T>(keyword));
    }

    [[nodiscard]] bool hasLocation() const noexcept { return gpsCoordinates.has_value(); }

    [[nodiscard]] bool hasCameraInfo() const noexcept {
        return cameraModel.has_value() || lens.has_value();
    }
};

/**
 * @brief Content handler for image files
 */
class ImageContentHandler : public IContentHandler {
public:
    ImageContentHandler();
    explicit ImageContentHandler(ImageProcessingConfig config);
    ~ImageContentHandler() override = default;

    // Disable copy and move (due to atomic members)
    ImageContentHandler(const ImageContentHandler&) = delete;
    ImageContentHandler& operator=(const ImageContentHandler&) = delete;
    ImageContentHandler(ImageContentHandler&&) = delete;
    ImageContentHandler& operator=(ImageContentHandler&&) = delete;

    /**
     * @brief Check if this handler can process the given file
     */
    [[nodiscard]] bool canHandle(const detection::FileSignature& signature) const override;

    /**
     * @brief Get handler priority (higher = preferred)
     */
    [[nodiscard]] int priority() const override { return 15; }

    /**
     * @brief Get handler name
     */
    [[nodiscard]] std::string name() const override { return "ImageContentHandler"; }

    /**
     * @brief Get supported MIME types
     */
    [[nodiscard]] std::vector<std::string> supportedMimeTypes() const override;

    /**
     * @brief Process image file and extract content/metadata
     */
    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    /**
     * @brief Process image from memory buffer
     */
    Result<ContentResult> processBuffer(std::span<const std::byte> data, const std::string& hint,
                                        const ContentConfig& config = {}) override;

    /**
     * @brief Validate image file before processing
     */
    Result<void> validate(const std::filesystem::path& path) const override;

    /**
     * @brief Get processing configuration
     */
    [[nodiscard]] const ImageProcessingConfig& getImageConfig() const noexcept {
        return imageConfig_;
    }

    /**
     * @brief Update processing configuration
     */
    void setImageConfig(ImageProcessingConfig config) noexcept { imageConfig_ = std::move(config); }

private:
    // C++20: Use consteval for compile-time validation
    static consteval uint32_t maxImageDimension() noexcept { return 65536; }
    static consteval uint32_t minImageDimension() noexcept { return 1; }
    static consteval size_t maxFileSize() noexcept { return 500 * 1024 * 1024; } // 500MB

    /**
     * @brief Convert MIME type to ImageFormat enum
     */
    [[nodiscard]] ImageFormat getFormatFromMimeType(const std::string& mimeType) const noexcept;

    /**
     * @brief Extract basic image metadata (dimensions, format, etc.)
     */
    [[nodiscard]] std::optional<ExtendedImageMetadata>
    extractBasicMetadata(std::span<const std::byte> data, ImageFormat format,
                         const detection::FileSignature& signature) const;

    /**
     * @brief Extract EXIF metadata using C++20 string_view
     */
    [[nodiscard]] std::optional<std::unordered_map<std::string, std::string>>
    extractExifMetadata(std::span<const std::byte> data, ImageFormat format) const;

    /**
     * @brief Generate thumbnail with modern error handling
     */
    [[nodiscard]] std::optional<std::vector<std::byte>>
    generateThumbnail(std::span<const std::byte> data, ImageFormat format,
                      uint32_t targetSize) const;

    /**
     * @brief Extract text using OCR (if enabled)
     */
    [[nodiscard]] std::optional<std::string> extractTextContent(std::span<const std::byte> data,
                                                                ImageFormat format) const;

    /**
     * @brief Convert processing result to ContentResult
     */
    [[nodiscard]] ContentResult buildContentResult(const ExtendedImageMetadata& metadata,
                                                   std::optional<std::string> extractedText,
                                                   std::optional<std::vector<std::byte>> thumbnail,
                                                   const detection::FileSignature& signature,
                                                   const std::filesystem::path& path = {}) const;

    /**
     * @brief Create error message using C++20 format
     */
    template <typename... Args>
    [[nodiscard]] std::string formatError(std::string_view operation,
                                          const std::filesystem::path& path, std::string_view fmt,
                                          Args&&... args) const {
        auto details = yams::format(fmt, std::forward<Args>(args)...);
        return yams::format("Image processing failed: {} for '{}' - {}", operation, path.string(),
                            details);
    }

    /**
     * @brief Validate image dimensions
     */
    template <std::integral T>
    [[nodiscard]] constexpr bool isValidDimension(T width, T height) const noexcept {
        return width >= minImageDimension() && height >= minImageDimension() &&
               width <= maxImageDimension() && height <= maxImageDimension();
    }

    /**
     * @brief Check if format supports transparency
     */
    [[nodiscard]] static constexpr bool formatSupportsAlpha(ImageFormat format) noexcept {
        return format == ImageFormat::PNG || format == ImageFormat::GIF ||
               format == ImageFormat::WEBP || format == ImageFormat::TIFF;
    }

    /**
     * @brief Check if format supports animation
     */
    [[nodiscard]] static constexpr bool formatSupportsAnimation(ImageFormat format) noexcept {
        return format == ImageFormat::GIF || format == ImageFormat::WEBP;
    }

    // Member variables
    ImageProcessingConfig imageConfig_;
    mutable std::atomic<size_t> processedImages_{0};
    mutable std::atomic<size_t> successfulProcessing_{0};
    mutable std::atomic<size_t> failedProcessing_{0};

    // Statistics
    mutable std::chrono::milliseconds totalProcessingTime_{0};
    mutable std::mutex statsMutex_;
};

/**
 * @brief Factory function for creating ImageContentHandler
 */
[[nodiscard]] std::unique_ptr<ImageContentHandler>
createImageHandler(ImageProcessingConfig config = {});

/**
 * @brief Helper function to check if file is a supported image format
 */
[[nodiscard]] bool isImageFile(const std::filesystem::path& path);

/**
 * @brief Helper function to get image format from file extension
 */
[[nodiscard]] ImageFormat getImageFormatFromExtension(std::string_view extension);

/**
 * @brief Helper function to get file extensions for image format
 */
[[nodiscard]] std::vector<std::string> getExtensionsForFormat(ImageFormat format);

} // namespace yams::content