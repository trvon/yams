#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>

namespace yams::api {

/**
 * @brief Base response for metadata operations
 */
struct MetadataResponse {
    std::string requestId; // Matches request ID
    bool success = false;
    std::string message;
    ErrorCode errorCode = ErrorCode::Success;
    std::chrono::system_clock::time_point timestamp;
    std::chrono::milliseconds processingTime{0};

    MetadataResponse() : timestamp(std::chrono::system_clock::now()) {}
    virtual ~MetadataResponse() = default;

    void setError(ErrorCode code, const std::string& msg) {
        success = false;
        errorCode = code;
        message = msg;
    }

    void setSuccess(const std::string& msg = "") {
        success = true;
        errorCode = ErrorCode::Success;
        message = msg;
    }
};

/**
 * @brief Response for create metadata operation
 */
struct CreateMetadataResponse : public MetadataResponse {
    DocumentId documentId;
    metadata::DocumentMetadata createdMetadata;
    bool wasIndexed = false;
};

/**
 * @brief Response for get metadata operation
 */
struct GetMetadataResponse : public MetadataResponse {
    metadata::DocumentMetadata metadata;
    std::vector<metadata::DocumentMetadata> relatedDocuments;

    struct VersionInfo {
        int version;
        std::chrono::system_clock::time_point timestamp;
        std::string author;
        std::string comment;
    };

    std::vector<VersionInfo> history;
};

/**
 * @brief Response for update metadata operation
 */
struct UpdateMetadataResponse : public MetadataResponse {
    metadata::DocumentMetadata updatedMetadata;
    metadata::DocumentMetadata previousMetadata;
    bool wasCreated = false; // True if created instead of updated
    int newVersion = 1;
};

/**
 * @brief Response for delete metadata operation
 */
struct DeleteMetadataResponse : public MetadataResponse {
    metadata::DocumentMetadata deletedMetadata;
    size_t relatedDeleted = 0;
    bool wasSoftDeleted = true;
};

/**
 * @brief Response for bulk create operation
 */
struct BulkCreateResponse : public MetadataResponse {
    struct CreateResult {
        bool success;
        DocumentId documentId;
        std::string path;
        std::string error;
    };

    std::vector<CreateResult> results;
    size_t successCount = 0;
    size_t failureCount = 0;
    bool allSucceeded = false;
};

/**
 * @brief Response for bulk update operation
 */
struct BulkUpdateResponse : public MetadataResponse {
    struct UpdateResult {
        bool success;
        DocumentId documentId;
        std::string error;
        int newVersion;
    };

    std::vector<UpdateResult> results;
    size_t successCount = 0;
    size_t failureCount = 0;
    bool allSucceeded = false;
};

/**
 * @brief Response for bulk delete operation
 */
struct BulkDeleteResponse : public MetadataResponse {
    struct DeleteResult {
        bool success;
        DocumentId documentId;
        std::string error;
    };

    std::vector<DeleteResult> results;
    size_t successCount = 0;
    size_t failureCount = 0;
    bool allSucceeded = false;
};

/**
 * @brief Response for query metadata operation
 */
struct QueryMetadataResponse : public MetadataResponse {
    std::vector<metadata::DocumentMetadata> documents;
    size_t totalCount = 0;    // Total matching documents
    size_t returnedCount = 0; // Number returned (after pagination)
    size_t offset = 0;
    size_t limit = 0;
    bool hasMore = false;

    // Optional statistics
    struct Statistics {
        size_t totalSize = 0;
        size_t averageSize = 0;
        std::unordered_map<std::string, size_t> typeDistribution;
        std::chrono::system_clock::time_point oldestDocument;
        std::chrono::system_clock::time_point newestDocument;
    };

    std::optional<Statistics> stats;
};

/**
 * @brief Response for export metadata operation
 */
struct ExportMetadataResponse : public MetadataResponse {
    std::string exportPath;
    size_t documentsExported = 0;
    size_t totalSize = 0;
    bool wasCompressed = false;
    std::string checksum; // Checksum of export file

    struct ExportStats {
        std::chrono::milliseconds exportTime{0};
        size_t compressedSize = 0;
        float compressionRatio = 0.0f;
    };

    ExportStats stats;
};

/**
 * @brief Response for import metadata operation
 */
struct ImportMetadataResponse : public MetadataResponse {
    size_t documentsImported = 0;
    size_t documentsSkipped = 0;
    size_t documentsUpdated = 0;
    size_t documentsFailed = 0;

    struct ImportError {
        std::string documentPath;
        std::string error;
        size_t lineNumber = 0; // For CSV imports
    };

    std::vector<ImportError> errors;

    struct ImportStats {
        std::chrono::milliseconds importTime{0};
        size_t totalBytesProcessed = 0;
        std::unordered_map<std::string, size_t> typeCount;
    };

    ImportStats stats;
};

/**
 * @brief Response for get statistics operation
 */
struct GetStatisticsResponse : public MetadataResponse {
    size_t totalDocuments = 0;
    size_t totalSize = 0;

    // Type statistics
    std::unordered_map<std::string, size_t> documentsByType;
    std::unordered_map<std::string, size_t> sizeByType;

    // Date statistics
    struct DateStats {
        std::chrono::system_clock::time_point oldest;
        std::chrono::system_clock::time_point newest;
        size_t documentsLastDay = 0;
        size_t documentsLastWeek = 0;
        size_t documentsLastMonth = 0;
    };
    DateStats dateStats;

    // Size statistics
    struct SizeStats {
        size_t minSize = 0;
        size_t maxSize = 0;
        size_t avgSize = 0;
        size_t medianSize = 0;
    };
    SizeStats sizeStats;

    // Tag statistics
    std::unordered_map<std::string, size_t> tagFrequency;

    // Author statistics
    std::unordered_map<std::string, size_t> documentsByAuthor;
};

/**
 * @brief Response for validate metadata operation
 */
struct ValidateMetadataResponse : public MetadataResponse {
    bool isValid = false;

    struct ValidationError {
        std::string field;
        std::string error;
        std::string suggestion;
    };

    std::vector<ValidationError> errors;
    std::vector<std::string> warnings;

    // Validation details
    bool pathValid = true;
    bool hashValid = true;
    bool datesValid = true;
    bool uniquenessValid = true;
    bool customFieldsValid = true;
};

/**
 * @brief Response for get history operation
 */
struct GetHistoryResponse : public MetadataResponse {
    DocumentId documentId;

    struct HistoryEntry {
        int version;
        std::chrono::system_clock::time_point timestamp;
        std::string author;
        std::string operation; // create, update, delete
        std::string comment;
        metadata::DocumentMetadata metadata; // Optional full metadata
    };

    std::vector<HistoryEntry> history;
    size_t totalVersions = 0;
    int currentVersion = 0;
};

/**
 * @brief Batch response for multiple operations
 */
struct BatchMetadataResponse : public MetadataResponse {
    struct OperationResult {
        std::string operationType; // create, update, delete, query
        bool success;
        std::string error;
        std::chrono::milliseconds duration{0};
    };

    std::vector<OperationResult> operations;
    size_t totalOperations = 0;
    size_t successfulOperations = 0;
    size_t failedOperations = 0;
    std::chrono::milliseconds totalDuration{0};
};

} // namespace yams::api