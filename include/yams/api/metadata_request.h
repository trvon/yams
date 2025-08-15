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
 * @brief Base request for metadata operations
 */
struct MetadataRequest {
    std::string requestId; // Unique request identifier
    std::string clientId;  // Client identifier for tracking
    std::chrono::system_clock::time_point timestamp;

    MetadataRequest() : timestamp(std::chrono::system_clock::now()) {}
    virtual ~MetadataRequest() = default;
};

/**
 * @brief Request to create new document metadata
 */
struct CreateMetadataRequest : public MetadataRequest {
    metadata::DocumentMetadata metadata;
    bool validateUniqueness = true; // Check if document already exists
    bool indexImmediately = false;  // Trigger indexing after creation
};

/**
 * @brief Request to retrieve document metadata
 */
struct GetMetadataRequest : public MetadataRequest {
    std::optional<DocumentId> documentId;
    std::optional<std::string> path;
    std::optional<std::string> contentHash;
    bool includeHistory = false; // Include version history
    bool includeRelated = false; // Include related documents
};

/**
 * @brief Request to update document metadata
 */
struct UpdateMetadataRequest : public MetadataRequest {
    DocumentId documentId;
    metadata::DocumentMetadata metadata;
    bool createIfNotExists = false; // Create new if doesn't exist
    bool preserveHistory = true;    // Keep version history
    std::string updateReason;       // Reason for update (for audit)
};

/**
 * @brief Request to delete document metadata
 */
struct DeleteMetadataRequest : public MetadataRequest {
    DocumentId documentId;
    bool softDelete = true;     // Soft delete (mark as deleted) vs hard delete
    bool deleteRelated = false; // Delete related metadata
    std::string deleteReason;   // Reason for deletion (for audit)
};

/**
 * @brief Bulk create request
 */
struct BulkCreateRequest : public MetadataRequest {
    std::vector<metadata::DocumentMetadata> documents;
    bool validateAll = true;       // Validate all before inserting any
    bool continueOnError = false;  // Continue if some fail
    bool indexAfterCreate = false; // Trigger indexing after creation
};

/**
 * @brief Bulk update request
 */
struct BulkUpdateRequest : public MetadataRequest {
    struct UpdateItem {
        DocumentId documentId;
        metadata::DocumentMetadata metadata;
    };

    std::vector<UpdateItem> updates;
    bool validateAll = true;
    bool continueOnError = false;
    bool preserveHistory = true;
};

/**
 * @brief Bulk delete request
 */
struct BulkDeleteRequest : public MetadataRequest {
    std::vector<DocumentId> documentIds;
    bool softDelete = true;
    bool continueOnError = false;
};

/**
 * @brief Query filter for metadata operations
 */
struct MetadataFilter {
    // Field filters
    std::optional<std::string> contentType;
    std::optional<std::string> author;
    std::optional<std::string> pathPrefix;
    std::vector<std::string> tags;

    // Date range filters
    std::optional<std::chrono::system_clock::time_point> createdAfter;
    std::optional<std::chrono::system_clock::time_point> createdBefore;
    std::optional<std::chrono::system_clock::time_point> modifiedAfter;
    std::optional<std::chrono::system_clock::time_point> modifiedBefore;

    // Size filters
    std::optional<size_t> minSize;
    std::optional<size_t> maxSize;

    // Status filters
    bool includeDeleted = false;
    bool includeHidden = false;

    // Custom metadata filters
    std::unordered_map<std::string, std::string> customFields;
};

/**
 * @brief Query request for searching metadata
 */
struct QueryMetadataRequest : public MetadataRequest {
    MetadataFilter filter;

    // Pagination
    size_t offset = 0;
    size_t limit = 100;

    // Sorting
    enum class SortField { CreatedDate, ModifiedDate, Title, Size, ContentType, Path };

    SortField sortBy = SortField::ModifiedDate;
    bool ascending = false;

    // Result options
    bool includeCount = true;  // Include total count
    bool includeStats = false; // Include statistics
};

/**
 * @brief Request to export metadata
 */
struct ExportMetadataRequest : public MetadataRequest {
    enum class ExportFormat { JSON, CSV, XML, SQLite };

    MetadataFilter filter; // Filter what to export
    ExportFormat format = ExportFormat::JSON;
    bool includeContent = false; // Include document content
    bool compressOutput = false; // Compress the export file
    std::string outputPath;      // Where to save export
};

/**
 * @brief Request to import metadata
 */
struct ImportMetadataRequest : public MetadataRequest {
    enum class ImportFormat { JSON, CSV, XML, SQLite };

    std::string inputPath; // Path to import file
    ImportFormat format = ImportFormat::JSON;

    // Import options
    bool validateBeforeImport = true;
    bool overwriteExisting = false; // Overwrite if document exists
    bool preserveIds = false;       // Keep original document IDs
    bool continueOnError = true;

    // Conflict resolution
    enum class ConflictResolution {
        Skip,      // Skip conflicting documents
        Overwrite, // Overwrite existing
        Rename,    // Create with new name
        Version    // Create as new version
    };

    ConflictResolution conflictResolution = ConflictResolution::Skip;
};

/**
 * @brief Request for metadata statistics
 */
struct GetStatisticsRequest : public MetadataRequest {
    MetadataFilter filter;

    // What statistics to include
    bool includeTypeCounts = true;
    bool includeSizeStats = true;
    bool includeDateStats = true;
    bool includeTagStats = true;
    bool includeAuthorStats = true;
};

/**
 * @brief Request for metadata validation
 */
struct ValidateMetadataRequest : public MetadataRequest {
    metadata::DocumentMetadata metadata;

    // Validation options
    bool checkUniqueness = true;
    bool validatePaths = true;
    bool validateHashes = true;
    bool validateDates = true;
    bool validateCustomFields = true;
};

/**
 * @brief Request for metadata history
 */
struct GetHistoryRequest : public MetadataRequest {
    DocumentId documentId;

    // History options
    size_t maxVersions = 10;
    std::optional<std::chrono::system_clock::time_point> since;
    std::optional<std::chrono::system_clock::time_point> until;
    bool includeDetails = true;
};

} // namespace yams::api