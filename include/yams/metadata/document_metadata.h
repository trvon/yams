#pragma once

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>
#include <yams/core/cpp23_features.hpp>
#include <yams/core/types.h>

namespace yams::metadata {

namespace detail {

template <typename E, std::size_t N>
constexpr std::string_view enumToStringImpl(E value,
                                            const std::pair<E, std::string_view> (&map)[N]) {
    for (const auto& [k, v] : map)
        if (k == value)
            return v;
    return "Unknown";
}

template <typename E, std::size_t N>
constexpr E enumFromStringImpl(std::string_view name,
                               const std::pair<E, std::string_view> (&map)[N]) {
    for (const auto& [k, v] : map)
        if (v == name)
            return k;
    return static_cast<E>(-1);
}
} // namespace detail

/**
 * @brief Document extraction status
 */
enum class ExtractionStatus {
    Pending, ///< Text extraction not yet attempted
    Success, ///< Text extraction completed successfully
    Failed,  ///< Text extraction failed
    Skipped  ///< Text extraction skipped (e.g., binary files)
};

/**
 * @brief Repair processing status for documents
 *
 * Tracks whether a document has been processed by the repair system
 * (embedding generation, FTS5 indexing, symbol extraction, etc.)
 * to prevent duplicate work and deadlocks.
 */
enum class RepairStatus {
    Pending,    ///< Not yet processed by repair system
    Processing, ///< Currently being processed
    Completed,  ///< Successfully processed
    Failed,     ///< Processing failed (may retry after backoff)
    Skipped     ///< Skipped (not applicable for this document type)
};

/**
 * @brief Document relationship types
 */
enum class RelationshipType {
    Contains,    ///< Parent contains child (e.g., archive contains file)
    References,  ///< Document references another
    VersionOf,   ///< Document is a version of another
    SimilarTo,   ///< Document is similar to another
    DerivedFrom, ///< Document is derived from another
    Custom       ///< Custom relationship type
};

/**
 * @brief Metadata value types for type-safe storage
 */
enum class MetadataValueType { String, Integer, Real, Blob, Boolean };

/**
 * @brief Generic metadata value with type information
 */
struct MetadataValue {
    // Backward-compat storage (DB serialization uses this and 'type')
    std::string value;
    MetadataValueType type;

    // Optional typed cache for faster/safer access (no schema change)
    using Variant = std::variant<std::string, int64_t, double, bool, std::vector<uint8_t>>;
    mutable std::optional<Variant> typedCache;

    MetadataValue() : type(MetadataValueType::String) {}

    // Constructors for different types
    explicit MetadataValue(const std::string& str) : value(str), type(MetadataValueType::String) {}
    explicit MetadataValue(const char* str) : value(str), type(MetadataValueType::String) {}
    explicit MetadataValue(int64_t num)
        : value(std::to_string(num)), type(MetadataValueType::Integer) {
        typedCache = Variant{num};
    }
    explicit MetadataValue(double num) : value(std::to_string(num)), type(MetadataValueType::Real) {
        typedCache = Variant{num};
    }
    explicit MetadataValue(bool b) : value(b ? "1" : "0"), type(MetadataValueType::Boolean) {
        typedCache = Variant{b};
    }
    static MetadataValue fromBlob(const std::vector<uint8_t>& blob) {
        MetadataValue mv;
        mv.type = MetadataValueType::Blob;
        mv.value.assign(blob.begin(), blob.end()); // lossy textual view for legacy paths
        mv.typedCache = Variant{std::vector<uint8_t>(blob)};
        return mv;
    }

    // Keep legacy accessors for compatibility
    [[nodiscard]] std::string asString() const { return value; }
    [[nodiscard]] int64_t asInteger() const { return std::stoll(value); }
    [[nodiscard]] double asReal() const { return std::stod(value); }
    [[nodiscard]] bool asBoolean() const { return value == "1"; }

    // New typed API
    [[nodiscard]] Variant asVariant() const {
        if (typedCache)
            return *typedCache;
        switch (type) {
            case MetadataValueType::String:
                typedCache = Variant{value};
                break;
            case MetadataValueType::Integer:
                typedCache = Variant{std::stoll(value)};
                break;
            case MetadataValueType::Real:
                typedCache = Variant{std::stod(value)};
                break;
            case MetadataValueType::Boolean:
                typedCache = Variant{value == "1"};
                break;
            case MetadataValueType::Blob: {
                std::vector<uint8_t> bytes(value.begin(), value.end());
                typedCache = Variant{std::move(bytes)};
                break;
            }
        }
        return *typedCache;
    }

    void setVariant(const Variant& v) {
        typedCache = v;
        std::visit(
            [this](auto&& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, std::string>) {
                    type = MetadataValueType::String;
                    value = val;
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    type = MetadataValueType::Integer;
                    value = std::to_string(val);
                } else if constexpr (std::is_same_v<T, double>) {
                    type = MetadataValueType::Real;
                    value = std::to_string(val);
                } else if constexpr (std::is_same_v<T, bool>) {
                    type = MetadataValueType::Boolean;
                    value = val ? "1" : "0";
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    type = MetadataValueType::Blob;
                    value.assign(val.begin(), val.end());
                }
            },
            v);
    }
};

/**
 * @brief Core document information
 */
struct DocumentInfo {
    int64_t id = 0;                        ///< Database ID
    std::string filePath;                  ///< Absolute file path
    std::string fileName;                  ///< File name only
    std::string fileExtension;             ///< File extension
    int64_t fileSize = 0;                  ///< File size in bytes
    std::string sha256Hash;                ///< SHA-256 hash
    std::string mimeType;                  ///< MIME type
    std::string pathPrefix;                ///< Directory prefix of path
    std::string reversePath;               ///< Reversed path string (for suffix search)
    std::string pathHash;                  ///< Hash of normalized path
    std::string parentHash;                ///< Hash of parent path
    int pathDepth = 0;                     ///< Number of path segments
    std::chrono::sys_seconds createdTime;  ///< File creation time (seconds precision)
    std::chrono::sys_seconds modifiedTime; ///< File modification time (seconds precision)
    std::chrono::sys_seconds indexedTime;  ///< When indexed (seconds precision)
    bool contentExtracted = false;         ///< Text extraction completed
    ExtractionStatus extractionStatus = ExtractionStatus::Pending; ///< Extraction status
    std::string extractionError;                                   ///< Error message if failed

    // Repair tracking fields (v21 migration)
    RepairStatus repairStatus = RepairStatus::Pending; ///< Repair processing status
    std::chrono::sys_seconds repairAttemptedAt;        ///< When repair was last attempted
    int repairAttempts = 0;                            ///< Number of repair attempts

    // Legacy Unix timestamp accessors removed; bind/get sys_seconds directly via Statement.

    /**
     * @brief Set time from Unix timestamp
     */
    void setCreatedTime(int64_t unixTime) { createdTime = yams::features::fromUnixTime(unixTime); }

    void setModifiedTime(int64_t unixTime) {
        modifiedTime = yams::features::fromUnixTime(unixTime);
    }

    void setIndexedTime(int64_t unixTime) { indexedTime = yams::features::fromUnixTime(unixTime); }
};

/**
 * @brief Extracted document content
 */
struct DocumentContent {
    int64_t documentId = 0;       ///< References DocumentInfo.id
    std::string contentText;      ///< Extracted text content
    int64_t contentLength = 0;    ///< Length of extracted text
    std::string extractionMethod; ///< Method used for extraction
    std::string language;         ///< Detected language code
};

/**
 * @brief Entry for batch content insertion and indexing.
 *
 * Used by batchInsertContentAndIndex() to wrap multiple documents in a single
 * transaction, reducing connection pool contention during bulk ingestion.
 */
struct BatchContentEntry {
    int64_t documentId = 0;       ///< Document ID in database
    std::string title;            ///< Document title (for FTS)
    std::string contentText;      ///< Extracted text content
    std::string mimeType;         ///< MIME type for FTS content_type
    std::string extractionMethod; ///< Method used for extraction
    std::string language;         ///< Detected language code
};

/**
 * @brief Document relationship information
 */
struct DocumentRelationship {
    int64_t id = 0;                                               ///< Database ID
    int64_t parentId = 0;                                         ///< Parent document ID (optional)
    int64_t childId = 0;                                          ///< Child document ID
    RelationshipType relationshipType = RelationshipType::Custom; ///< Relationship type
    std::string customType;                                       ///< Custom relationship name
    std::chrono::sys_seconds createdTime; ///< When relationship created (seconds)

    // Legacy Unix accessor removed

    void setCreatedTime(int64_t unixTime) { createdTime = yams::features::fromUnixTime(unixTime); }

    /**
     * @brief Get relationship type as string for database storage
     */
    [[nodiscard]] std::string getRelationshipTypeString() const {
        switch (relationshipType) {
            case RelationshipType::Contains:
                return "contains";
            case RelationshipType::References:
                return "references";
            case RelationshipType::VersionOf:
                return "version_of";
            case RelationshipType::SimilarTo:
                return "similar_to";
            case RelationshipType::DerivedFrom:
                return "derived_from";
            case RelationshipType::Custom:
                return customType;
        }
        return "unknown";
    }

    /**
     * @brief Set relationship type from string
     */
    void setRelationshipTypeFromString(const std::string& typeStr) {
        if (typeStr == "contains") {
            relationshipType = RelationshipType::Contains;
        } else if (typeStr == "references") {
            relationshipType = RelationshipType::References;
        } else if (typeStr == "version_of") {
            relationshipType = RelationshipType::VersionOf;
        } else if (typeStr == "similar_to") {
            relationshipType = RelationshipType::SimilarTo;
        } else if (typeStr == "derived_from") {
            relationshipType = RelationshipType::DerivedFrom;
        } else {
            relationshipType = RelationshipType::Custom;
            customType = typeStr;
        }
    }
};

/**
 * @brief Search query history entry
 */
struct SearchHistoryEntry {
    int64_t id = 0;                     ///< Database ID
    std::string query;                  ///< Search query string
    std::chrono::sys_seconds queryTime; ///< When query was executed (seconds precision)
    int64_t resultsCount = 0;           ///< Number of results returned
    int64_t executionTimeMs = 0;        ///< Query execution time in milliseconds
    std::string userContext;            ///< Optional user/session identifier

    // Legacy Unix accessor removed

    void setQueryTime(int64_t unixTime) { queryTime = yams::features::fromUnixTime(unixTime); }
};

/**
 * @brief Saved search query
 */
struct SavedQuery {
    int64_t id = 0;                       ///< Database ID
    std::string name;                     ///< User-friendly name
    std::string query;                    ///< Search query string
    std::string description;              ///< Optional description
    std::chrono::sys_seconds createdTime; ///< When saved (seconds precision)
    std::chrono::sys_seconds lastUsed;    ///< Last time used (seconds precision)
    int64_t useCount = 0;                 ///< Number of times used

    // Legacy Unix accessors removed

    void setCreatedTime(int64_t unixTime) { createdTime = yams::features::fromUnixTime(unixTime); }

    void setLastUsed(int64_t unixTime) { lastUsed = yams::features::fromUnixTime(unixTime); }
};

/**
 * @brief Complete document metadata including content and relationships
 */
struct DocumentMetadata {
    DocumentInfo info;                                       ///< Core document information
    std::optional<DocumentContent> content;                  ///< Extracted content (if available)
    std::unordered_map<std::string, MetadataValue> metadata; ///< Key-value metadata
    std::vector<DocumentRelationship> relationships;         ///< Document relationships

    /**
     * @brief Add metadata entry
     */
    void addMetadata(const std::string& key, const MetadataValue& value) { metadata[key] = value; }

    /**
     * @brief Get metadata entry
     */
    [[nodiscard]] std::optional<MetadataValue> getMetadata(const std::string& key) const {
        auto it = metadata.find(key);
        return it != metadata.end() ? std::make_optional(it->second) : std::nullopt;
    }

    /**
     * @brief Check if metadata key exists
     */
    [[nodiscard]] bool hasMetadata(const std::string& key) const {
        return metadata.find(key) != metadata.end();
    }

    /**
     * @brief Add relationship
     */
    void addRelationship(const DocumentRelationship& relationship) {
        relationships.push_back(relationship);
    }

    /**
     * @brief Get relationships of a specific type
     */
    [[nodiscard]] std::vector<DocumentRelationship> getRelationships(RelationshipType type) const {
        std::vector<DocumentRelationship> result;
        for (const auto& rel : relationships) {
            if (rel.relationshipType == type) {
                result.push_back(rel);
            }
        }
        return result;
    }
};

/**
 * @brief Search result with ranking information
 */
struct SearchResult {
    DocumentInfo document;                 ///< Document information
    double score = 0.0;                    ///< Search relevance score
    std::vector<std::string> matchedTerms; ///< Terms that matched
    std::string snippet;                   ///< Content snippet with highlights

    // Score breakdown by component (populated during fusion)
    std::optional<double> vectorScore;   ///< Contribution from vector/semantic search
    std::optional<double> keywordScore;  ///< Contribution from FTS5/keyword search
    std::optional<double> kgScore;       ///< Contribution from knowledge graph
    std::optional<double> pathScore;     ///< Contribution from path matching
    std::optional<double> tagScore;      ///< Contribution from tag matching
    std::optional<double> symbolScore;   ///< Contribution from symbol search
    std::optional<double> rerankerScore; ///< Cross-encoder reranker score (if enabled)
};

/**
 * @brief Search query results
 */
struct SearchResults {
    std::string query;                 ///< Original query
    std::vector<SearchResult> results; ///< Matching documents
    int64_t totalCount = 0;            ///< Total results (may be > results.size() if paginated)
    int64_t executionTimeMs = 0;       ///< Query execution time
    std::string errorMessage;          ///< Error message if query failed

    /**
     * @brief Check if search was successful
     */
    [[nodiscard]] bool isSuccess() const { return errorMessage.empty(); }

    /**
     * @brief Sort results by score (descending)
     */
    void sortByScore() {
        std::sort(results.begin(), results.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }
};

/**
 * @brief Utility functions for extraction status
 */
namespace ExtractionStatusUtils {
/**
 * @brief Convert extraction status to string
 */
[[nodiscard]] inline std::string toString(ExtractionStatus status) {
    switch (status) {
        case ExtractionStatus::Pending:
            return "pending";
        case ExtractionStatus::Success:
            return "success";
        case ExtractionStatus::Failed:
            return "failed";
        case ExtractionStatus::Skipped:
            return "skipped";
    }
    return "unknown";
}

/**
 * @brief Convert string to extraction status
 */
[[nodiscard]] inline ExtractionStatus fromString(const std::string& str) {
    if (str == "pending")
        return ExtractionStatus::Pending;
    if (str == "success")
        return ExtractionStatus::Success;
    if (str == "failed")
        return ExtractionStatus::Failed;
    if (str == "skipped")
        return ExtractionStatus::Skipped;
    return ExtractionStatus::Pending;
}
} // namespace ExtractionStatusUtils

/**
 * @brief Utility functions for repair status
 */
namespace RepairStatusUtils {
/**
 * @brief Convert repair status to string
 */
[[nodiscard]] inline std::string toString(RepairStatus status) {
    switch (status) {
        case RepairStatus::Pending:
            return "pending";
        case RepairStatus::Processing:
            return "processing";
        case RepairStatus::Completed:
            return "completed";
        case RepairStatus::Failed:
            return "failed";
        case RepairStatus::Skipped:
            return "skipped";
    }
    return "pending";
}

/**
 * @brief Convert string to repair status
 */
[[nodiscard]] inline RepairStatus fromString(const std::string& str) {
    if (str == "pending")
        return RepairStatus::Pending;
    if (str == "processing")
        return RepairStatus::Processing;
    if (str == "completed")
        return RepairStatus::Completed;
    if (str == "failed")
        return RepairStatus::Failed;
    if (str == "skipped")
        return RepairStatus::Skipped;
    return RepairStatus::Pending;
}
} // namespace RepairStatusUtils

/**
 * @brief Utility functions for metadata value types
 */
namespace MetadataValueTypeUtils {
/**
 * @brief Convert metadata value type to string
 */
[[nodiscard]] inline constexpr std::string_view toStringView(MetadataValueType type) {
    switch (type) {
        case MetadataValueType::String:
            return "string";
        case MetadataValueType::Integer:
            return "integer";
        case MetadataValueType::Real:
            return "real";
        case MetadataValueType::Blob:
            return "blob";
        case MetadataValueType::Boolean:
            return "boolean";
    }
    return "string";
}

[[nodiscard]] inline std::string toString(MetadataValueType type) {
    return std::string(toStringView(type));
}

/**
 * @brief Convert string to metadata value type
 */
[[nodiscard]] inline MetadataValueType fromString(const std::string& str) {
    if (str == "string")
        return MetadataValueType::String;
    if (str == "integer")
        return MetadataValueType::Integer;
    if (str == "real")
        return MetadataValueType::Real;
    if (str == "blob")
        return MetadataValueType::Blob;
    if (str == "boolean")
        return MetadataValueType::Boolean;
    return MetadataValueType::String;
}
} // namespace MetadataValueTypeUtils

} // namespace yams::metadata
