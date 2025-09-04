#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <fstream>
#include <mutex>
#include <ranges>
#include <string>
#include <unordered_set>
#include <yams/common/format.h>
#include <yams/content/image_content_handler.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

namespace {

// C++20: consteval lookup tables for image-specific mappings
consteval auto createImageMimeTypes() {
    return std::array{"image/jpeg",
                      "image/jpg",
                      "image/png",
                      "image/gif",
                      "image/bmp",
                      "image/tiff",
                      "image/tif",
                      "image/webp",
                      "image/svg+xml",
                      "image/x-icon",
                      "image/vnd.adobe.photoshop"};
}

constexpr auto IMAGE_MIME_TYPES = createImageMimeTypes();

// Helper function to read file into memory
[[nodiscard]] Result<std::vector<std::byte>> readFileToMemory(const std::filesystem::path& path) {
    try {
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            return Error{ErrorCode::FileNotFound,
                         yams::fmt_format("Cannot open file: {}", path.string())};
        }

        const auto size = file.tellg();
        if (size <= 0) {
            return Error{ErrorCode::InvalidArgument, "File is empty or invalid"};
        }

        std::vector<std::byte> data(static_cast<size_t>(size));
        file.seekg(0);
        file.read(reinterpret_cast<char*>(data.data()), size);

        if (!file) {
            return Error{ErrorCode::IOError, "Failed to read file data"};
        }

        return data;
    } catch (const std::exception& e) {
        return Error{ErrorCode::IOError, yams::fmt_format("File read error: {}", e.what())};
    }
}

// Helper to extract basic dimensions from PNG
[[nodiscard]] std::optional<std::pair<uint32_t, uint32_t>>
extractPngDimensions(std::span<const std::byte> data) {
    if (data.size() < 24)
        return std::nullopt;

    // PNG IHDR chunk starts at offset 16
    auto width = (static_cast<uint32_t>(data[16]) << 24) | (static_cast<uint32_t>(data[17]) << 16) |
                 (static_cast<uint32_t>(data[18]) << 8) | static_cast<uint32_t>(data[19]);

    auto height = (static_cast<uint32_t>(data[20]) << 24) |
                  (static_cast<uint32_t>(data[21]) << 16) | (static_cast<uint32_t>(data[22]) << 8) |
                  static_cast<uint32_t>(data[23]);

    return std::make_pair(width, height);
}

// Helper to extract basic dimensions from JPEG
[[nodiscard]] std::optional<std::pair<uint32_t, uint32_t>>
extractJpegDimensions(std::span<const std::byte> data) {
    if (data.size() < 10)
        return std::nullopt;

    // Simple JPEG dimension extraction (SOF marker)
    for (size_t i = 0; i < data.size() - 9; ++i) {
        if (data[i] == std::byte{0xFF} &&
            (data[i + 1] == std::byte{0xC0} || data[i + 1] == std::byte{0xC2})) {
            if (i + 9 < data.size()) {
                auto height =
                    (static_cast<uint32_t>(data[i + 5]) << 8) | static_cast<uint32_t>(data[i + 6]);
                auto width =
                    (static_cast<uint32_t>(data[i + 7]) << 8) | static_cast<uint32_t>(data[i + 8]);
                return std::make_pair(width, height);
            }
        }
    }
    return std::nullopt;
}

// Helper to extract basic dimensions from GIF
[[nodiscard]] std::optional<std::pair<uint32_t, uint32_t>>
extractGifDimensions(std::span<const std::byte> data) {
    if (data.size() < 10)
        return std::nullopt;

    // GIF dimensions are at offset 6-9 (little endian)
    auto width = static_cast<uint32_t>(data[6]) | (static_cast<uint32_t>(data[7]) << 8);
    auto height = static_cast<uint32_t>(data[8]) | (static_cast<uint32_t>(data[9]) << 8);

    return std::make_pair(width, height);
}

// Helper to extract basic dimensions from BMP
[[nodiscard]] std::optional<std::pair<uint32_t, uint32_t>>
extractBmpDimensions(std::span<const std::byte> data) {
    if (data.size() < 26)
        return std::nullopt;

    // BMP dimensions are at offset 18-25 (little endian, 32-bit)
    auto width = static_cast<uint32_t>(data[18]) | (static_cast<uint32_t>(data[19]) << 8) |
                 (static_cast<uint32_t>(data[20]) << 16) | (static_cast<uint32_t>(data[21]) << 24);

    auto height = static_cast<uint32_t>(data[22]) | (static_cast<uint32_t>(data[23]) << 8) |
                  (static_cast<uint32_t>(data[24]) << 16) | (static_cast<uint32_t>(data[25]) << 24);

    return std::make_pair(width, height);
}

} // anonymous namespace

ImageContentHandler::ImageContentHandler() : imageConfig_{} {}

ImageContentHandler::ImageContentHandler(ImageProcessingConfig config)
    : imageConfig_{std::move(config)} {}

bool ImageContentHandler::canHandle(const detection::FileSignature& signature) const {
    // Use the FileTypeDetector's detection - check if MIME type is image/*
    if (signature.mimeType.starts_with("image/")) {
        return true;
    }

    // Check against our known image MIME types
    auto mimeRange = IMAGE_MIME_TYPES |
                     std::views::transform([](const auto& mime) { return std::string_view{mime}; });

    if (std::ranges::any_of(mimeRange, [&signature](const auto& supportedMime) {
            return signature.mimeType == supportedMime;
        })) {
        return true;
    }

    // Check if FileTypeDetector categorized this as an image
    if (signature.fileType == "image") {
        return true;
    }

    return false;
}

std::vector<std::string> ImageContentHandler::supportedMimeTypes() const {
    std::vector<std::string> mimeTypes;
    mimeTypes.reserve(IMAGE_MIME_TYPES.size());
    for (const auto& mime : IMAGE_MIME_TYPES) {
        mimeTypes.emplace_back(mime);
    }
    return mimeTypes;
}

Result<void> ImageContentHandler::validate(const std::filesystem::path& path) const {
    // First call base validation
    if (auto baseResult = IContentHandler::validate(path); !baseResult) {
        return baseResult;
    }

    // Check file size
    try {
        const auto fileSize = std::filesystem::file_size(path);
        if (fileSize > maxFileSize()) {
            return Error{ErrorCode::InvalidArgument,
                         yams::fmt_format("Image file too large: {} bytes (max: {})", fileSize,
                                          maxFileSize())};
        }

        if (fileSize == 0) {
            return Error{ErrorCode::InvalidArgument, "Image file is empty"};
        }

    } catch (const std::filesystem::filesystem_error& e) {
        return Error{ErrorCode::IOError, yams::fmt_format("Filesystem error: {}", e.what())};
    }

    return Result<void>{};
}

Result<ContentResult> ImageContentHandler::process(const std::filesystem::path& path,
                                                   const ContentConfig& config) {
    const auto startTime = std::chrono::steady_clock::now();

    // Validate input
    if (auto validationResult = validate(path); !validationResult) {
        ++failedProcessing_;
        return validationResult.error();
    }

    // Read file data
    auto dataResult = readFileToMemory(path);
    if (!dataResult) {
        ++failedProcessing_;
        return dataResult.error();
    }

    // Process the buffer
    auto result = processBuffer(dataResult.value(), path.extension().string(), config);

    // Update statistics
    const auto endTime = std::chrono::steady_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    {
        std::lock_guard<std::mutex> lock(statsMutex_);
        totalProcessingTime_ += duration;
    }

    ++processedImages_;
    if (result) {
        ++successfulProcessing_;
        // Handler name is already set in buildContentResult
    } else {
        ++failedProcessing_;
    }

    return result;
}

Result<ContentResult> ImageContentHandler::processBuffer(std::span<const std::byte> data,
                                                         const std::string& hint,
                                                         const ContentConfig& config) {
    (void)hint; // suppress unused parameter warning on some platforms
    if (data.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty image data"};
    }

    // Use FileTypeDetector to identify the image format
    auto& detector = detection::FileTypeDetector::instance();
    auto signatureResult = detector.detectFromBuffer(data);
    if (!signatureResult) {
        return Error{ErrorCode::NotSupported, yams::fmt_format("Failed to detect image format: {}",
                                                               signatureResult.error().message)};
    }

    const auto& signature = signatureResult.value();

    // Verify this is actually an image
    if (!signature.mimeType.starts_with("image/") && signature.fileType != "image") {
        return Error{ErrorCode::NotSupported,
                     yams::fmt_format("Not an image file: detected as {} ({})", signature.mimeType,
                                      signature.fileType)};
    }

    // Convert MIME type to our ImageFormat enum
    auto format = getFormatFromMimeType(signature.mimeType);

    // Extract basic metadata
    auto metadataOpt = extractBasicMetadata(data, format, signature);
    if (!metadataOpt) {
        return Error{ErrorCode::InternalError, "Failed to extract image metadata"};
    }

    auto& metadata = metadataOpt.value();

    // Validate dimensions
    if (!isValidDimension(metadata.width, metadata.height)) {
        return Error{ErrorCode::InvalidArgument, yams::fmt_format("Invalid image dimensions: {}x{}",
                                                                  metadata.width, metadata.height)};
    }

    // Extract EXIF data if requested
    if (imageConfig_.extractExif && config.extractMetadata) {
        if (auto exifData = extractExifMetadata(data, format)) {
            metadata.exifData = std::move(exifData.value());
        }
    }

    // Extract text content if requested (OCR)
    std::optional<std::string> extractedText;
    if (imageConfig_.detectText && config.extractText) {
        extractedText = extractTextContent(data, format);
    }

    // Generate thumbnail if requested
    std::optional<std::vector<std::byte>> thumbnail;
    if (imageConfig_.generateThumbnail && config.generateThumbnail) {
        thumbnail = generateThumbnail(data, format, imageConfig_.thumbnailSize);
    }

    // Build and return result
    return buildContentResult(metadata, extractedText, thumbnail, signature);
}

ImageFormat ImageContentHandler::getFormatFromMimeType(const std::string& mimeType) const noexcept {
    if (mimeType == "image/jpeg" || mimeType == "image/jpg")
        return ImageFormat::JPEG;
    if (mimeType == "image/png")
        return ImageFormat::PNG;
    if (mimeType == "image/gif")
        return ImageFormat::GIF;
    if (mimeType == "image/bmp")
        return ImageFormat::BMP;
    if (mimeType == "image/tiff" || mimeType == "image/tif")
        return ImageFormat::TIFF;
    if (mimeType == "image/webp")
        return ImageFormat::WEBP;
    if (mimeType == "image/svg+xml")
        return ImageFormat::SVG;
    if (mimeType == "image/x-icon")
        return ImageFormat::ICO;
    if (mimeType == "image/vnd.adobe.photoshop")
        return ImageFormat::PSD;

    return ImageFormat::Unknown;
}

std::optional<ExtendedImageMetadata>
ImageContentHandler::extractBasicMetadata(std::span<const std::byte> data, ImageFormat format,
                                          const detection::FileSignature& signature) const {
    ExtendedImageMetadata metadata;
    metadata.format = format;

    // Extract dimensions based on format detected by FileTypeDetector
    std::optional<std::pair<uint32_t, uint32_t>> dimensions;

    switch (format) {
        case ImageFormat::PNG:
            dimensions = extractPngDimensions(data);
            break;
        case ImageFormat::JPEG:
            dimensions = extractJpegDimensions(data);
            break;
        case ImageFormat::GIF:
            dimensions = extractGifDimensions(data);
            break;
        case ImageFormat::BMP:
            dimensions = extractBmpDimensions(data);
            break;
        default:
            // For other formats, we'd need format-specific libraries
            // For now, set reasonable defaults
            break;
    }

    if (dimensions) {
        metadata.width = dimensions->first;
        metadata.height = dimensions->second;
    } else {
        // If we couldn't extract dimensions, log warning but continue
        spdlog::warn("Could not extract dimensions for image format: {}",
                     std::string{formatToString(format)});
        metadata.width = 0;
        metadata.height = 0;
    }

    // Set format-specific properties
    metadata.hasAlpha = formatSupportsAlpha(format);
    metadata.isAnimated = formatSupportsAnimation(format);
    metadata.compression = std::string{formatToString(format)};

    // Use FileTypeDetector's confidence level
    metadata.confidence = signature.confidence;

    return metadata;
}

std::optional<std::unordered_map<std::string, std::string>>
ImageContentHandler::extractExifMetadata(std::span<const std::byte> data,
                                         ImageFormat format) const {
    // For now, return basic metadata from FileTypeDetector
    // In a full implementation, this would use libexif or similar
    std::unordered_map<std::string, std::string> exifData;

    if (format == ImageFormat::JPEG || format == ImageFormat::TIFF) {
        exifData["format"] = std::string{formatToString(format)};
        exifData["size"] = std::to_string(data.size());
        exifData["processing_date"] =
            yams::fmt_format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now());
        // Add more EXIF extraction logic here
    }

    return exifData.empty() ? std::nullopt : std::make_optional(exifData);
}

std::optional<std::vector<std::byte>>
ImageContentHandler::generateThumbnail(std::span<const std::byte> data, ImageFormat format,
                                       uint32_t targetSize) const {
    (void)format; // suppress unused parameter warning in placeholder implementation
    // Placeholder implementation
    // In a full implementation, this would use image processing libraries
    // like ImageMagick++, SFML, or similar

    if (data.size() < 100) {
        return std::nullopt; // Too small to generate thumbnail
    }

    std::vector<std::byte> thumbnail;
    thumbnail.reserve(targetSize * targetSize * 3); // RGB estimate

    // For now, just store a minimal header indicating it's a placeholder
    const std::string placeholder =
        yams::fmt_format("THUMBNAIL_PLACEHOLDER_{}x{}", targetSize, targetSize);
    std::ranges::transform(placeholder, std::back_inserter(thumbnail),
                           [](char c) { return static_cast<std::byte>(c); });

    return thumbnail.empty() ? std::nullopt : std::make_optional(thumbnail);
}

std::optional<std::string> ImageContentHandler::extractTextContent(std::span<const std::byte> data,
                                                                   ImageFormat format) const {
    // Placeholder for OCR implementation
    // In a full implementation, this would use Tesseract or similar

    if (format == ImageFormat::SVG) {
        // SVG files might contain readable text
        std::string svgContent;
        svgContent.reserve(data.size());
        std::ranges::transform(data, std::back_inserter(svgContent),
                               [](std::byte b) { return static_cast<char>(b); });

        // Simple extraction of text elements (very basic)
        if (svgContent.find("<text") != std::string::npos) {
            return "SVG contains text elements (OCR/text extraction not fully implemented)";
        }
    }

    return std::nullopt;
}

ContentResult ImageContentHandler::buildContentResult(
    const ExtendedImageMetadata& metadata, std::optional<std::string> extractedText,
    std::optional<std::vector<std::byte>> thumbnail, const detection::FileSignature& signature,
    const std::filesystem::path& path) const {
    (void)path; // suppress unused parameter warning (path not needed here)
    ContentResult result;

    // Set basic content info
    result.text = extractedText;
    result.handlerName = name();
    result.contentType = signature.mimeType;
    result.fileFormat = std::string{formatToString(metadata.format)};
    result.shouldIndex = extractedText.has_value() || !metadata.embeddedText.empty();
    result.isComplete = true;

    // Set thumbnail if available
    if (thumbnail) {
        result.thumbnail = std::move(thumbnail.value());
    }

    // Build metadata map
    result.metadata["width"] = std::to_string(metadata.width);
    result.metadata["height"] = std::to_string(metadata.height);
    result.metadata["format"] = std::string{formatToString(metadata.format)};
    result.metadata["mime_type"] = signature.mimeType;
    result.metadata["file_type"] = signature.fileType;
    result.metadata["description"] = signature.description;
    result.metadata["confidence"] = std::to_string(signature.confidence);
    result.metadata["has_alpha"] = metadata.hasAlpha ? "true" : "false";
    result.metadata["is_animated"] = metadata.isAnimated ? "true" : "false";
    result.metadata["magic_number"] = signature.magicNumber;

    if (!metadata.compression.empty()) {
        result.metadata["compression"] = metadata.compression;
    }

    if (!metadata.colorSpace.empty()) {
        result.metadata["color_space"] = metadata.colorSpace;
    }

    // Add camera/EXIF data
    if (metadata.cameraModel) {
        result.metadata["camera_model"] = *metadata.cameraModel;
    }
    if (metadata.lens) {
        result.metadata["lens"] = *metadata.lens;
    }
    if (metadata.focalLength) {
        result.metadata["focal_length"] = std::to_string(*metadata.focalLength);
    }
    if (metadata.aperture) {
        result.metadata["aperture"] = std::to_string(*metadata.aperture);
    }
    if (metadata.shutterSpeed) {
        result.metadata["shutter_speed"] = std::to_string(*metadata.shutterSpeed);
    }
    if (metadata.iso) {
        result.metadata["iso"] = std::to_string(*metadata.iso);
    }
    if (metadata.gpsCoordinates) {
        result.metadata["latitude"] = std::to_string(metadata.gpsCoordinates->first);
        result.metadata["longitude"] = std::to_string(metadata.gpsCoordinates->second);
    }

    // Add EXIF data with prefix
    for (const auto& [key, value] : metadata.exifData) {
        result.metadata["exif_" + key] = value;
    }

    // Add keywords
    if (!metadata.keywords.empty()) {
        std::string keywords = metadata.keywords[0];
        for (size_t i = 1; i < metadata.keywords.size(); ++i) {
            keywords += "," + metadata.keywords[i];
        }
        result.metadata["keywords"] = keywords;
    }

    // Set structured image data
    ImageMetadata imageData;
    imageData.width = metadata.width;
    imageData.height = metadata.height;
    imageData.bitsPerPixel = metadata.bitsPerPixel;
    imageData.colorSpace = metadata.colorSpace;
    imageData.compression = metadata.compression;
    imageData.exifData = metadata.exifData;
    imageData.hasAlpha = metadata.hasAlpha;
    imageData.isAnimated = metadata.isAnimated;
    imageData.embeddedText = metadata.embeddedText;

    result.imageData = imageData;

    return result;
}

// Factory function implementation
std::unique_ptr<ImageContentHandler> createImageHandler(ImageProcessingConfig config) {
    return std::make_unique<ImageContentHandler>(std::move(config));
}

// Helper function implementations
bool isImageFile(const std::filesystem::path& path) {
    auto& detector = detection::FileTypeDetector::instance();
    auto result = detector.detectFromFile(path);

    if (result) {
        const auto& signature = result.value();
        return signature.mimeType.starts_with("image/") || signature.fileType == "image";
    }

    // Fallback to extension-based detection
    static const std::unordered_set<std::string> imageExtensions{
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp", ".svg", ".ico", ".psd"};

    auto ext = path.extension().string();
    std::ranges::transform(ext, ext.begin(), ::tolower);
    return imageExtensions.contains(ext);
}

ImageFormat getImageFormatFromExtension(std::string_view extension) {
    std::string lowerExt{extension};
    std::ranges::transform(lowerExt, lowerExt.begin(), ::tolower);

    if (lowerExt == ".jpg" || lowerExt == ".jpeg")
        return ImageFormat::JPEG;
    if (lowerExt == ".png")
        return ImageFormat::PNG;
    if (lowerExt == ".gif")
        return ImageFormat::GIF;
    if (lowerExt == ".bmp")
        return ImageFormat::BMP;
    if (lowerExt == ".tiff" || lowerExt == ".tif")
        return ImageFormat::TIFF;
    if (lowerExt == ".webp")
        return ImageFormat::WEBP;
    if (lowerExt == ".svg")
        return ImageFormat::SVG;
    if (lowerExt == ".ico")
        return ImageFormat::ICO;
    if (lowerExt == ".psd")
        return ImageFormat::PSD;

    return ImageFormat::Unknown;
}

std::vector<std::string> getExtensionsForFormat(ImageFormat format) {
    switch (format) {
        case ImageFormat::JPEG:
            return {".jpg", ".jpeg"};
        case ImageFormat::PNG:
            return {".png"};
        case ImageFormat::GIF:
            return {".gif"};
        case ImageFormat::BMP:
            return {".bmp"};
        case ImageFormat::TIFF:
            return {".tiff", ".tif"};
        case ImageFormat::WEBP:
            return {".webp"};
        case ImageFormat::SVG:
            return {".svg"};
        case ImageFormat::ICO:
            return {".ico"};
        case ImageFormat::PSD:
            return {".psd"};
        default:
            return {};
    }
}

} // namespace yams::content
