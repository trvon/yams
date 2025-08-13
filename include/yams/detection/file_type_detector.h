#pragma once

#include <yams/core/types.h>
#include <yams/metadata/database.h>
#include <filesystem>
#include <vector>
#include <string>
#include <span>
#include <memory>
#include <optional>
#include <unordered_map>

namespace yams::detection {

/**
 * @brief File signature information
 */
struct FileSignature {
    std::string mimeType;           ///< MIME type (e.g., "image/jpeg")
    std::string fileType;           ///< File type category (e.g., "image", "archive")
    std::string description;        ///< Human-readable description
    std::string magicNumber;        ///< Hex representation of magic bytes
    bool isBinary = true;          ///< Whether file is binary or text
    float confidence = 1.0f;        ///< Confidence level of detection
};

/**
 * @brief File pattern for matching
 */
struct FilePattern {
    std::vector<std::byte> pattern;  ///< Binary pattern to match
    std::string patternHex;          ///< Hex representation
    size_t offset = 0;               ///< Offset in file where pattern should appear
    std::string fileType;            ///< File type this pattern indicates
    std::string mimeType;            ///< Associated MIME type
    std::string description;         ///< Human-readable description
    float confidence = 1.0f;         ///< Confidence level for this pattern
};

/**
 * @brief Configuration for FileTypeDetector
 */
struct FileTypeDetectorConfig {
    bool useLibMagic = true;         ///< Try to use libmagic if available
    bool useBuiltinPatterns = true;  ///< Use built-in pattern database
    bool useCustomPatterns = true;   ///< Allow custom patterns
    std::filesystem::path patternsFile; ///< Path to custom patterns JSON file
    size_t maxBytesToRead = 512;     ///< Maximum bytes to read for detection
    bool cacheResults = true;        ///< Cache detection results
    size_t cacheSize = 1000;         ///< Maximum cache entries
};

/**
 * @brief File type detection using magic numbers and patterns
 * 
 * This class provides file type detection using multiple methods:
 * 1. libmagic (if available at compile time)
 * 2. Built-in pattern database
 * 3. Custom patterns loaded from JSON
 * 4. Extension-based fallback
 */
class FileTypeDetector {
public:
    /**
     * @brief Get singleton instance
     */
    static FileTypeDetector& instance();
    
    /**
     * @brief Initialize detector with configuration
     */
    Result<void> initialize(const FileTypeDetectorConfig& config = {});
    
    /**
     * @brief Detect file type from buffer
     * @param data First bytes of file
     * @return Detected file signature or error
     */
    Result<FileSignature> detectFromBuffer(std::span<const std::byte> data);
    
    /**
     * @brief Detect file type from file path
     * @param path Path to file
     * @return Detected file signature or error
     */
    Result<FileSignature> detectFromFile(const std::filesystem::path& path);
    
    /**
     * @brief Load patterns from JSON file
     * @param patternsFile Path to JSON file with patterns
     * @return Success or error
     */
    Result<void> loadPatternsFromFile(const std::filesystem::path& patternsFile);
    
    /**
     * @brief Load patterns from database
     * @param db Database connection
     * @return Number of patterns loaded or error
     */
    Result<size_t> loadPatternsFromDatabase(metadata::Database& db);
    
    /**
     * @brief Save patterns to database
     * @param db Database connection
     * @return Number of patterns saved or error
     */
    Result<size_t> savePatternsToDatabase(metadata::Database& db);
    
    /**
     * @brief Add custom pattern
     * @param pattern Pattern to add
     * @return Success or error
     */
    Result<void> addPattern(const FilePattern& pattern);
    
    /**
     * @brief Get all loaded patterns
     * @return Vector of patterns
     */
    std::vector<FilePattern> getPatterns() const;
    
    /**
     * @brief Clear pattern cache
     */
    void clearCache();
    
    /**
     * @brief Get cache statistics
     */
    struct CacheStats {
        size_t hits = 0;
        size_t misses = 0;
        size_t entries = 0;
        size_t maxSize = 0;
    };
    CacheStats getCacheStats() const;
    
    /**
     * @brief Check if libmagic is available
     */
    bool hasLibMagic() const;
    
    /**
     * @brief Get MIME type from file extension
     * @param extension File extension (with or without dot)
     * @return MIME type or "application/octet-stream"
     */
    static std::string getMimeTypeFromExtension(const std::string& extension);
    
    /**
     * @brief Check if MIME type represents a text file
     * @param mimeType MIME type to check
     * @return True if text file, false otherwise
     */
    bool isTextMimeType(const std::string& mimeType) const;
    
    /**
     * @brief Check if MIME type represents a binary file
     * @param mimeType MIME type to check
     * @return True if binary file, false otherwise
     */
    bool isBinaryMimeType(const std::string& mimeType) const;
    
    /**
     * @brief Get file type category from MIME type
     * @param mimeType MIME type to categorize
     * @return File type category (image, document, text, binary, etc.)
     */
    std::string getFileTypeCategory(const std::string& mimeType) const;
    
    /**
     * @brief Convert bytes to hex string
     * @param data Binary data
     * @param maxLength Maximum length to convert
     * @return Hex string representation
     */
    static std::string bytesToHex(std::span<const std::byte> data, size_t maxLength = 16);
    
    /**
     * @brief Convert hex string to bytes
     * @param hex Hex string (e.g., "FFD8FF")
     * @return Binary data or error
     */
    static Result<std::vector<std::byte>> hexToBytes(const std::string& hex);
    
    ~FileTypeDetector();
    
    // Non-copyable, non-movable (singleton)
    FileTypeDetector(const FileTypeDetector&) = delete;
    FileTypeDetector& operator=(const FileTypeDetector&) = delete;
    FileTypeDetector(FileTypeDetector&&) = delete;
    FileTypeDetector& operator=(FileTypeDetector&&) = delete;
    
private:
    FileTypeDetector();
    
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * @brief Load default pattern database
 * 
 * This loads a comprehensive set of common file patterns
 * @return Vector of patterns
 */
std::vector<FilePattern> getDefaultPatterns();

/**
 * @brief Detect if buffer contains binary data
 * @param data Buffer to check
 * @return True if binary, false if likely text
 */
bool isBinaryData(std::span<const std::byte> data);

/**
 * @brief Extract magic number from buffer
 * @param data Buffer containing file start
 * @param length Number of bytes to extract (default 8)
 * @return Hex string of magic number
 */
std::string extractMagicNumber(std::span<const std::byte> data, size_t length = 8);

} // namespace yams::detection