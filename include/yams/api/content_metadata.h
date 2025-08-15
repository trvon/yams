#pragma once

#include <chrono>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>

namespace yams::api {

/**
 * @brief Metadata about content stored in the system
 */
struct ContentMetadata {
    std::string id;         ///< Unique content identifier
    std::string name;       ///< Original file name
    uint64_t size{0};       ///< Content size in bytes
    std::string mimeType;   ///< MIME type (e.g., "text/plain")
    Hash contentHash;       ///< SHA-256 hash of content
    TimePoint createdAt{};  ///< Creation timestamp (initialized to epoch)
    TimePoint modifiedAt{}; ///< Last modification timestamp (initialized to epoch)
    TimePoint accessedAt{}; ///< Last access timestamp (initialized to epoch)
    std::unordered_map<std::string, std::string> tags; ///< User-defined tags

    /**
     * @brief Check if metadata is valid
     * @return True if all required fields are present
     */
    [[nodiscard]] bool isValid() const noexcept {
        return !id.empty() && !name.empty() && size > 0 && !contentHash.empty();
    }

    /**
     * @brief Get file extension from name
     * @return Extension including dot (e.g., ".txt") or empty string
     */
    [[nodiscard]] std::string extension() const {
        auto pos = name.rfind('.');
        if (pos != std::string::npos && pos > 0) {
            return name.substr(pos);
        }
        return "";
    }

    /**
     * @brief Calculate age since creation
     * @return Duration since creation
     */
    [[nodiscard]] std::chrono::hours ageSinceCreation() const {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::hours>(now - createdAt);
    }

    /**
     * @brief Calculate time since last access
     * @return Duration since last access
     */
    [[nodiscard]] std::chrono::hours timeSinceAccess() const {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::hours>(now - accessedAt);
    }

    /**
     * @brief Deserialize metadata from binary data
     * @param data Binary data to deserialize
     * @return Deserialized metadata or error
     */
    static Result<ContentMetadata> deserialize(std::span<const std::byte> data);

    /**
     * @brief Convert metadata to JSON string
     * @return JSON representation
     */
    std::string toJson() const;

    /**
     * @brief Create metadata from JSON string
     * @param json JSON string to parse
     * @return Parsed metadata or error
     */
    static Result<ContentMetadata> fromJson(const std::string& json);
};

/**
 * @brief Query parameters for filtering ContentMetadata
 */
struct MetadataQuery {
    std::optional<std::string> mimeType;     ///< Filter by MIME type
    std::optional<std::string> namePattern;  ///< Regex pattern for name matching
    std::vector<std::string> requiredTags;   ///< All these tags must be present
    std::vector<std::string> anyTags;        ///< At least one of these tags must be present
    std::vector<std::string> excludeTags;    ///< None of these tags should be present
    std::optional<TimePoint> createdAfter;   ///< Filter by creation time (after)
    std::optional<TimePoint> createdBefore;  ///< Filter by creation time (before)
    std::optional<TimePoint> modifiedAfter;  ///< Filter by modification time (after)
    std::optional<TimePoint> modifiedBefore; ///< Filter by modification time (before)
    std::unordered_map<std::string, std::string> customFieldMatches; ///< Custom field exact matches

    /**
     * @brief Check if metadata matches this query
     * @param metadata The metadata to check
     * @return True if metadata matches all query criteria
     */
    bool matches(const ContentMetadata& metadata) const;
};

} // namespace yams::api