#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <format>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <unordered_set>
#include <yams/content/content_handler.h>
#include <yams/crypto/hasher.h>
#include <yams/detection/file_type_detector.h>

namespace yams::content {

/**
 * @brief concept for binary data analyzers
 */
template <typename T>
concept BinaryAnalyzer = requires(T analyzer, std::span<const std::byte> data) {
    { analyzer.analyze(data) } -> std::convertible_to<std::unordered_map<std::string, std::string>>;
    { analyzer.canAnalyze(data) } -> std::convertible_to<bool>;
    { analyzer.name() } -> std::convertible_to<std::string>;
};

/**
 * @brief concept for hash algorithms
 */
template <typename T>
concept HashAlgorithm = requires(T hasher, std::span<const std::byte> data) {
    { hasher.hash(data) } -> std::convertible_to<std::string>;
    { hasher.algorithmName() } -> std::convertible_to<std::string>;
};

/**
 * @brief Configuration for binary content processing
 */
struct BinaryProcessingConfig {
    bool calculateHash = true;
    bool extractStrings = true;
    bool analyzeHeaders = true;
    bool detectEncoding = true;
    size_t maxAnalysisSize = 10 * 1024 * 1024; // 10MB
    size_t minStringLength = 4;
    size_t maxStringLength = 1024;
    std::chrono::milliseconds processingTimeout{5000}; // 5s
    std::vector<std::string> hashAlgorithms{"sha256"}; // Default algorithms
};

/**
 * @brief Binary file analysis result
 */
struct BinaryAnalysisResult {
    std::string fileType;
    std::string detectedMimeType;
    std::unordered_map<std::string, std::string> hashes;
    std::vector<std::string> extractedStrings;
    std::unordered_map<std::string, std::string> headerInfo;
    std::optional<std::string> detectedEncoding;
    bool isPossiblyText = false;
    bool isExecutable = false;
    bool isCompressed = false;
    bool isEncrypted = false;
    size_t entropy = 0; // Shannon entropy (0-100)

    // C++20: Helper methods
    [[nodiscard]] bool hasStrings() const noexcept { return !extractedStrings.empty(); }

    [[nodiscard]] bool hasHashes() const noexcept { return !hashes.empty(); }

    [[nodiscard]] size_t totalStringLength() const noexcept {
        size_t total = 0;
        for (const auto& str : extractedStrings) {
            total += str.length();
        }
        return total;
    }
};

/**
 * @brief Fallback content handler for binary/unknown files
 *
 * This handler provides comprehensive metadata extraction for files
 * that don't have specialized handlers. It extracts file properties,
 * hashes, embedded strings, and performs basic analysis.
 */
class BinaryContentHandler : public IContentHandler {
public:
    explicit BinaryContentHandler(BinaryProcessingConfig config = {});
    ~BinaryContentHandler() override = default;

    // Disable copy and move (atomic members prevent default move)
    BinaryContentHandler(const BinaryContentHandler&) = delete;
    BinaryContentHandler& operator=(const BinaryContentHandler&) = delete;
    BinaryContentHandler(BinaryContentHandler&&) = delete;
    BinaryContentHandler& operator=(BinaryContentHandler&&) = delete;

    /**
     * @brief Check if this handler can process the given file
     * @note This handler can handle any file as a fallback
     */
    [[nodiscard]] bool canHandle(const detection::FileSignature& /*signature*/) const override {
        return true; // Fallback handler accepts everything
    }

    /**
     * @brief Get handler priority (lowest for fallback)
     */
    [[nodiscard]] int priority() const override { return 1; }

    /**
     * @brief Get handler name
     */
    [[nodiscard]] std::string name() const override { return "BinaryContentHandler"; }

    /**
     * @brief Get supported MIME types (wildcard for everything)
     */
    [[nodiscard]] std::vector<std::string> supportedMimeTypes() const override;

    /**
     * @brief Process binary file and extract metadata
     */
    Result<ContentResult> process(const std::filesystem::path& path,
                                  const ContentConfig& config = {}) override;

    /**
     * @brief Process binary data from memory buffer
     */
    Result<ContentResult> processBuffer(std::span<const std::byte> data, const std::string& hint,
                                        const ContentConfig& config = {}) override;

    /**
     * @brief Validate binary file before processing
     */
    Result<void> validate(const std::filesystem::path& path) const override;

    /**
     * @brief Get processing configuration
     */
    [[nodiscard]] const BinaryProcessingConfig& getBinaryConfig() const noexcept {
        return binaryConfig_;
    }

    /**
     * @brief Update processing configuration
     */
    void setBinaryConfig(BinaryProcessingConfig config) noexcept {
        binaryConfig_ = std::move(config);
    }

    /**
     * @brief Get processing statistics
     */
    struct ProcessingStats {
        size_t totalFilesProcessed = 0;
        size_t successfulProcessing = 0;
        size_t failedProcessing = 0;
        std::chrono::milliseconds totalProcessingTime{0};
        size_t totalBytesProcessed = 0;

        [[nodiscard]] double successRate() const noexcept {
            return totalFilesProcessed > 0
                       ? static_cast<double>(successfulProcessing) / totalFilesProcessed
                       : 0.0;
        }

        [[nodiscard]] double averageProcessingTimeMs() const noexcept {
            return totalFilesProcessed > 0
                       ? static_cast<double>(totalProcessingTime.count()) / totalFilesProcessed
                       : 0.0;
        }
    };

    [[nodiscard]] ProcessingStats getStats() const noexcept;

private:
    // C++20: consteval for compile-time constants
    static consteval size_t maxAnalysisSize() noexcept { return 100 * 1024 * 1024; } // 100MB
    static consteval size_t minFileSize() noexcept { return 0; }
    static consteval size_t maxStringSearchSize() noexcept {
        return 1024 * 1024;
    } // 1MB for string search

    /**
     * @brief Perform comprehensive binary analysis
     */
    [[nodiscard]] BinaryAnalysisResult
    analyzeBinaryData(std::span<const std::byte> data,
                      const std::filesystem::path& path = {}) const;

    /**
     * @brief Calculate multiple hashes using different algorithms
     */
    /**
     * @brief Calculate file hash
     */
    [[nodiscard]] std::string calculateHash(std::span<const std::byte> data) const;

    template <HashAlgorithm... Hashers>
    [[nodiscard]] std::unordered_map<std::string, std::string>
    calculateHashes(std::span<const std::byte> data, Hashers&&... hashers) const {
        std::unordered_map<std::string, std::string> result;
        ((result[hashers.algorithmName()] = hashers.hash(data)), ...);
        return result;
    }

    /**
     * @brief Extract strings from binary data using ranges
     */
    [[nodiscard]] std::vector<std::string> extractStrings(std::span<const std::byte> data,
                                                          size_t minLength = 4,
                                                          size_t maxLength = 1024) const;

    /**
     * @brief Detect if data might contain text
     */
    [[nodiscard]] bool detectPossibleText(std::span<const std::byte> data) const noexcept;

    /**
     * @brief Legacy function name for compatibility
     */
    [[nodiscard]] bool mightContainText(std::span<const std::byte> data) const noexcept {
        return detectPossibleText(data);
    }

    /**
     * @brief Calculate Shannon entropy
     */
    [[nodiscard]] size_t calculateEntropy(std::span<const std::byte> data) const noexcept;

    /**
     * @brief Detect file characteristics (executable, compressed, etc.)
     */
    [[nodiscard]] std::tuple<bool, bool, bool>
    detectFileCharacteristics(std::span<const std::byte> data) const noexcept;

    /**
     * @brief Extract file header information
     */
    [[nodiscard]] std::unordered_map<std::string, std::string>
    extractHeaderInfo(std::span<const std::byte> data,
                      const detection::FileSignature& signature) const;

    /**
     * @brief Build ContentResult from analysis
     */
    [[nodiscard]] ContentResult buildContentResult(const BinaryAnalysisResult& analysis,
                                                   const std::filesystem::path& path = {}) const;

    /**
     * @brief Extract basic metadata from file (legacy compatibility)
     */
    void extractFileMetadata(const std::filesystem::path& path, ContentResult& result) const;

    /**
     * @brief Create error message using format
     */
    template <typename... Args>
    [[nodiscard]] std::string formatError(std::string_view operation,
                                          const std::filesystem::path& path,
                                          std::format_string<Args...> fmt, Args&&... args) const {
        auto details = std::format(fmt, std::forward<Args>(args)...);
        return std::format("Binary processing failed: {} for '{}' - {}", operation, path.string(),
                           details);
    }

    /**
     * @brief Check if byte is printable ASCII
     */
    [[nodiscard]] static constexpr bool isPrintableAscii(std::byte b) noexcept {
        auto value = static_cast<unsigned char>(b);
        return (value >= 32 && value <= 126) || value == 9 || value == 10 || value == 13;
    }

    /**
     * @brief Check if data looks like text using ranges
     */
    [[nodiscard]] bool looksLikeText(std::span<const std::byte> data) const noexcept {
        if (data.empty())
            return false;

        constexpr size_t sampleSize = 1024;
        auto sample = data.subspan(0, std::min(data.size(), sampleSize));

        auto printableCount = std::ranges::count_if(sample, isPrintableAscii);
        return static_cast<double>(printableCount) / sample.size() > 0.7;
    }

    // Member variables
    BinaryProcessingConfig binaryConfig_;

    // Statistics (atomic for thread safety)
    mutable std::atomic<size_t> processedFiles_{0};
    mutable std::atomic<size_t> successfulProcessing_{0};
    mutable std::atomic<size_t> failedProcessing_{0};
    mutable std::atomic<std::chrono::milliseconds> totalProcessingTime_{
        std::chrono::milliseconds{0}};
    mutable std::atomic<size_t> totalBytesProcessed_{0};
};

/**
 * @brief Factory function for creating BinaryContentHandler
 */
[[nodiscard]] std::unique_ptr<BinaryContentHandler>
createBinaryHandler(BinaryProcessingConfig config = {});

/**
 * @brief Helper function to detect if file is likely binary
 */
[[nodiscard]] bool isLikelyBinaryFile(const std::filesystem::path& path);

/**
 * @brief Helper function to get common binary file extensions
 */
[[nodiscard]] std::unordered_set<std::string> getBinaryFileExtensions();

} // namespace yams::content