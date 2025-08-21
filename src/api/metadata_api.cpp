#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <future>
#include <iomanip>
#include <random>
#include <sstream>
#include <yams/api/metadata_api.h>

namespace yams::api {

using json = nlohmann::json;

MetadataApi::MetadataApi(std::shared_ptr<metadata::MetadataRepository> repository,
                         std::shared_ptr<indexing::IDocumentIndexer> indexer,
                         const MetadataApiConfig& config)
    : repository_(repository), indexer_(indexer), config_(config) {
    if (!repository_) {
        throw std::invalid_argument("Repository cannot be null");
    }
}

// ===== CRUD Operations =====

CreateMetadataResponse MetadataApi::createMetadata(const CreateMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    CreateMetadataResponse response;
    response.requestId = request.requestId;

    try {
        // Validate request
        if (config_.enableValidation) {
            auto validation = validateFields(request.metadata);
            if (!validation.empty()) {
                response.setError(ErrorCode::InvalidArgument, "Validation failed");
                return response;
            }
        }

        // Check uniqueness if requested (using hash instead of path)
        if (request.validateUniqueness) {
            auto existing = repository_->getDocumentByHash(request.metadata.info.sha256Hash);
            if (existing && existing.value()) {
                response.setError(ErrorCode::InvalidArgument, "Document already exists");
                return response;
            }
        }

        // Insert metadata (use the DocumentInfo from DocumentMetadata)
        auto result = repository_->insertDocument(request.metadata.info);
        if (!result) {
            response.setError(ErrorCode::DatabaseError, result.error().message);
            return response;
        }

        response.documentId = result.value();
        response.createdMetadata = request.metadata;
        response.createdMetadata.info.id = response.documentId;

        // Trigger indexing if requested
        if (request.indexImmediately && indexer_) {
            indexing::IndexingConfig indexConfig;
            auto indexResult = indexer_->indexDocument(request.metadata.info.filePath, indexConfig);
            response.wasIndexed = indexResult.has_value();
        }

        response.setSuccess("Document created successfully");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("create", response.success, response.processingTime);

    return response;
}

std::future<CreateMetadataResponse>
MetadataApi::createMetadataAsync(const CreateMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return createMetadata(request); });
}

GetMetadataResponse MetadataApi::getMetadata(const GetMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    GetMetadataResponse response;
    response.requestId = request.requestId;

    try {
        std::optional<metadata::DocumentInfo> documentInfo;

        // Get by ID or hash (path lookup not available)
        if (request.documentId) {
            auto result = repository_->getDocument(*request.documentId);
            if (result && result.value()) {
                documentInfo = result.value();
            }
        } else if (request.contentHash) {
            auto result = repository_->getDocumentByHash(*request.contentHash);
            if (result && result.value()) {
                documentInfo = result.value();
            }
        } else if (request.path) {
            response.setError(ErrorCode::NotSupported, "Path-based lookup not supported");
            return response;
        } else {
            response.setError(ErrorCode::InvalidArgument, "No identifier provided");
            return response;
        }

        if (!documentInfo) {
            response.setError(ErrorCode::NotFound, "Document not found");
            return response;
        }

        // Convert DocumentInfo to DocumentMetadata
        metadata::DocumentMetadata docMetadata;
        docMetadata.info = *documentInfo;
        response.metadata = docMetadata;

        // Get related documents if requested
        if (request.includeRelated) {
            // TODO: Implement related document retrieval
        }

        // Get history if requested
        if (request.includeHistory) {
            // TODO: Implement version history retrieval
        }

        response.setSuccess();

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("get", response.success, response.processingTime);

    return response;
}

UpdateMetadataResponse MetadataApi::updateMetadata(const UpdateMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    UpdateMetadataResponse response;
    response.requestId = request.requestId;

    try {
        // Get existing document
        auto existing = repository_->getDocument(request.documentId);

        if (!existing || !existing.value()) {
            if (request.createIfNotExists) {
                // Create new document
                CreateMetadataRequest createReq;
                createReq.metadata = request.metadata;
                auto createResp = createMetadata(createReq);

                if (createResp.success) {
                    response.updatedMetadata = createResp.createdMetadata;
                    response.wasCreated = true;
                    response.setSuccess("Document created");
                } else {
                    response.setError(createResp.errorCode, createResp.message);
                }
                return response;
            } else {
                response.setError(ErrorCode::NotFound, "Document not found");
                return response;
            }
        }

        // Convert DocumentInfo to DocumentMetadata for response
        metadata::DocumentMetadata prevMetadata;
        prevMetadata.info = existing.value().value();
        response.previousMetadata = prevMetadata;

        // Update metadata (update the info from the request)
        auto updateResult = repository_->updateDocument(request.metadata.info);
        if (!updateResult) {
            response.setError(ErrorCode::DatabaseError, updateResult.error().message);
            return response;
        }

        response.updatedMetadata = request.metadata;
        response.updatedMetadata.info.id = request.documentId;
        response.setSuccess("Document updated successfully");

        // Log update reason if audit logging is enabled
        if (config_.enableAuditLog && !request.updateReason.empty()) {
            logOperation("update",
                         "Document " + std::to_string(request.documentId) + ": " +
                             request.updateReason,
                         true);
        }

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("update", response.success, response.processingTime);

    return response;
}

DeleteMetadataResponse MetadataApi::deleteMetadata(const DeleteMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    DeleteMetadataResponse response;
    response.requestId = request.requestId;

    try {
        // Get document before deletion
        auto existing = repository_->getDocument(request.documentId);
        if (!existing || !existing.value()) {
            response.setError(ErrorCode::NotFound, "Document not found");
            return response;
        }

        // Convert DocumentInfo to DocumentMetadata for response
        metadata::DocumentMetadata deletedMetadata;
        deletedMetadata.info = existing.value().value();
        response.deletedMetadata = deletedMetadata;

        // Perform deletion
        Result<void> deleteResult;
        if (request.softDelete) {
            // Soft delete - mark as deleted
            metadata::DocumentMetadata updatedMetadata;
            updatedMetadata.info = existing.value().value();
            // Set deleted flag (would need to add this field to DocumentInfo)
            deleteResult = repository_->updateDocument(updatedMetadata.info);
            response.wasSoftDeleted = true;
        } else {
            // Hard delete
            deleteResult = repository_->deleteDocument(request.documentId);
            response.wasSoftDeleted = false;
        }

        if (!deleteResult) {
            response.setError(ErrorCode::DatabaseError, deleteResult.error().message);
            return response;
        }

        response.setSuccess("Document deleted successfully");

        // Log deletion reason if audit logging is enabled
        if (config_.enableAuditLog && !request.deleteReason.empty()) {
            logOperation("delete",
                         "Document " + std::to_string(request.documentId) + ": " +
                             request.deleteReason,
                         true);
        }

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("delete", response.success, response.processingTime);

    return response;
}

// ===== Bulk Operations =====

BulkCreateResponse MetadataApi::bulkCreate(const BulkCreateRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    BulkCreateResponse response;
    response.requestId = request.requestId;

    if (request.documents.size() > config_.maxBulkOperations) {
        response.setError(ErrorCode::InvalidArgument,
                          "Too many documents. Maximum: " +
                              std::to_string(config_.maxBulkOperations));
        return response;
    }

    try {
        // Validate all documents first if requested
        if (request.validateAll) {
            for (const auto& doc : request.documents) {
                auto validation = validateFields(doc);
                if (!validation.empty()) {
                    if (!request.continueOnError) {
                        response.setError(ErrorCode::InvalidArgument,
                                          "Validation failed for " + doc.info.filePath);
                        return response;
                    }
                }
            }
        }

        // Process each document
        for (const auto& doc : request.documents) {
            BulkCreateResponse::CreateResult result;
            result.path = doc.info.filePath;

            CreateMetadataRequest createReq;
            createReq.metadata = doc;
            createReq.validateUniqueness = request.validateAll;
            createReq.indexImmediately = request.indexAfterCreate;

            auto createResp = createMetadata(createReq);

            result.success = createResp.success;
            if (createResp.success) {
                result.documentId = createResp.documentId;
                response.successCount++;
            } else {
                result.documentId = 0; // Invalid/unset document ID for failed operations
                result.error = createResp.message;
                response.failureCount++;

                if (!request.continueOnError) {
                    break;
                }
            }

            response.results.push_back(result);
        }

        response.allSucceeded = (response.failureCount == 0);
        response.setSuccess("Bulk create completed: " + std::to_string(response.successCount) +
                            " succeeded, " + std::to_string(response.failureCount) + " failed");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("bulk_create", response.success, response.processingTime);

    return response;
}

// ===== Query Operations =====

QueryMetadataResponse MetadataApi::queryMetadata(const QueryMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    QueryMetadataResponse response;
    response.requestId = request.requestId;

    try {
        // Get all documents - using findDocumentsByExtension with empty pattern as workaround
        auto allDocsResult = repository_->findDocumentsByExtension("");
        if (!allDocsResult) {
            response.setError(ErrorCode::DatabaseError, "Failed to retrieve documents");
            return response;
        }

        // Convert DocumentInfo to DocumentMetadata
        std::vector<metadata::DocumentMetadata> allDocs;
        for (const auto& info : allDocsResult.value()) {
            metadata::DocumentMetadata docMeta;
            docMeta.info = info;
            allDocs.push_back(docMeta);
        }

        // Apply filters
        auto filteredDocs = applyFilter(allDocs, request.filter);

        response.totalCount = filteredDocs.size();

        // Sort documents
        sortDocuments(filteredDocs, request.sortBy, request.ascending);

        // Apply pagination
        response.offset = request.offset;
        response.limit = request.limit;

        size_t startIdx = std::min(request.offset, filteredDocs.size());
        size_t endIdx = std::min(startIdx + request.limit, filteredDocs.size());

        response.documents.assign(filteredDocs.begin() + startIdx, filteredDocs.begin() + endIdx);
        response.returnedCount = response.documents.size();
        response.hasMore = (endIdx < filteredDocs.size());

        // Calculate statistics if requested
        if (request.includeStats && !filteredDocs.empty()) {
            QueryMetadataResponse::Statistics stats;

            for (const auto& doc : filteredDocs) {
                stats.totalSize += doc.info.fileSize;
                stats.typeDistribution[doc.info.mimeType]++;

                if (stats.oldestDocument == std::chrono::system_clock::time_point{} ||
                    doc.info.modifiedTime < stats.oldestDocument) {
                    stats.oldestDocument = doc.info.modifiedTime;
                }

                if (doc.info.modifiedTime > stats.newestDocument) {
                    stats.newestDocument = doc.info.modifiedTime;
                }
            }

            stats.averageSize = stats.totalSize / filteredDocs.size();
            response.stats = stats;
        }

        response.setSuccess();

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("query", response.success, response.processingTime);

    return response;
}

// ===== Export Operations =====

ExportMetadataResponse MetadataApi::exportMetadata(const ExportMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    ExportMetadataResponse response;
    response.requestId = request.requestId;

    try {
        // Get documents to export
        auto allDocsResult = repository_->findDocumentsByExtension("");
        if (!allDocsResult) {
            response.setError(ErrorCode::DatabaseError, "Failed to retrieve documents");
            return response;
        }

        // Convert DocumentInfo to DocumentMetadata
        std::vector<metadata::DocumentMetadata> allDocs;
        for (const auto& info : allDocsResult.value()) {
            metadata::DocumentMetadata docMeta;
            docMeta.info = info;
            allDocs.push_back(docMeta);
        }

        auto docsToExport = applyFilter(allDocs, request.filter);

        response.documentsExported = docsToExport.size();

        // Export based on format
        Result<void> exportResult;
        switch (request.format) {
            case ExportMetadataRequest::ExportFormat::JSON:
                exportResult =
                    exportToJson(docsToExport, request.outputPath, request.compressOutput);
                break;

            case ExportMetadataRequest::ExportFormat::CSV:
                exportResult =
                    exportToCsv(docsToExport, request.outputPath, request.compressOutput);
                break;

            default:
                response.setError(ErrorCode::InvalidArgument, "Unsupported export format");
                return response;
        }

        if (!exportResult) {
            response.setError(ErrorCode::InternalError, exportResult.error().message);
            return response;
        }

        response.exportPath = request.outputPath;
        response.wasCompressed = request.compressOutput;

        // Calculate file size
        std::ifstream file(request.outputPath, std::ios::ate | std::ios::binary);
        if (file) {
            response.totalSize = file.tellg();
        }

        response.setSuccess("Export completed successfully");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    response.stats.exportTime = response.processingTime;

    updateStatistics("export", response.success, response.processingTime);

    return response;
}

// ===== Import Operations =====

ImportMetadataResponse MetadataApi::importMetadata(const ImportMetadataRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    ImportMetadataResponse response;
    response.requestId = request.requestId;

    try {
        // Import based on format
        Result<std::vector<metadata::DocumentMetadata>> importResult(ErrorCode::InvalidArgument);

        switch (request.format) {
            case ImportMetadataRequest::ImportFormat::JSON:
                importResult = importFromJson(request.inputPath);
                break;

            case ImportMetadataRequest::ImportFormat::CSV:
                importResult = importFromCsv(request.inputPath);
                break;

            default:
                response.setError(ErrorCode::InvalidArgument, "Unsupported import format");
                return response;
        }

        if (!importResult) {
            response.setError(ErrorCode::InternalError, importResult.error().message);
            return response;
        }

        auto documents = importResult.value();

        // Process each document
        for (const auto& doc : documents) {
            // Check for existing document using path pattern matching
            auto existing = repository_->findDocumentsByPath(doc.info.filePath);

            if (existing && !existing.value().empty()) {
                switch (request.conflictResolution) {
                    case ImportMetadataRequest::ConflictResolution::Skip:
                        response.documentsSkipped++;
                        continue;

                    case ImportMetadataRequest::ConflictResolution::Overwrite: {
                        auto updateResult = repository_->updateDocument(doc.info);
                        if (updateResult) {
                            response.documentsUpdated++;
                        } else {
                            response.documentsFailed++;
                            if (!request.continueOnError) {
                                break;
                            }
                        }
                    } break;

                    default:
                        response.documentsSkipped++;
                }
            } else {
                // Create new document
                auto createResult = repository_->insertDocument(doc.info);
                if (createResult) {
                    response.documentsImported++;
                } else {
                    response.documentsFailed++;
                    if (!request.continueOnError) {
                        break;
                    }
                }
            }
        }

        response.setSuccess("Import completed: " + std::to_string(response.documentsImported) +
                            " imported, " + std::to_string(response.documentsUpdated) +
                            " updated, " + std::to_string(response.documentsSkipped) +
                            " skipped, " + std::to_string(response.documentsFailed) + " failed");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    response.stats.importTime = response.processingTime;

    updateStatistics("import", response.success, response.processingTime);

    return response;
}

// ===== Utility Operations =====

ValidateMetadataResponse MetadataApi::validateMetadata(const ValidateMetadataRequest& request) {
    ValidateMetadataResponse response;
    response.requestId = request.requestId;

    try {
        auto errors = validateFields(request.metadata);

        response.errors = errors;
        response.isValid = errors.empty();

        // Check uniqueness if requested
        if (request.checkUniqueness) {
            auto existing = repository_->findDocumentsByPath(request.metadata.info.filePath);
            if (existing && !existing.value().empty()) {
                ValidateMetadataResponse::ValidationError error;
                error.field = "path";
                error.error = "Document with this path already exists";
                response.errors.push_back(error);
                response.uniquenessValid = false;
                response.isValid = false;
            }
        }

        response.setSuccess(response.isValid ? "Validation passed" : "Validation failed");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    return response;
}

// ===== Helper Methods =====

std::vector<metadata::DocumentMetadata>
MetadataApi::applyFilter(const std::vector<metadata::DocumentMetadata>& documents,
                         const MetadataFilter& filter) {
    std::vector<metadata::DocumentMetadata> filtered;

    for (const auto& doc : documents) {
        bool matches = true;

        // Apply content type filter
        if (filter.contentType && doc.info.mimeType != *filter.contentType) {
            matches = false;
        }

        // Apply path prefix filter
        if (matches && filter.pathPrefix && doc.info.filePath.find(*filter.pathPrefix) != 0) {
            matches = false;
        }

        // Apply date filters
        if (matches && filter.modifiedAfter && doc.info.modifiedTime < *filter.modifiedAfter) {
            matches = false;
        }

        if (matches && filter.modifiedBefore && doc.info.modifiedTime > *filter.modifiedBefore) {
            matches = false;
        }

        // Apply size filters
        if (matches && filter.minSize && doc.info.fileSize < *filter.minSize) {
            matches = false;
        }

        if (matches && filter.maxSize && doc.info.fileSize > *filter.maxSize) {
            matches = false;
        }

        if (matches) {
            filtered.push_back(doc);
        }
    }

    return filtered;
}

void MetadataApi::sortDocuments(std::vector<metadata::DocumentMetadata>& documents,
                                QueryMetadataRequest::SortField sortBy, bool ascending) {
    auto comparator = [sortBy, ascending](const metadata::DocumentMetadata& a,
                                          const metadata::DocumentMetadata& b) {
        bool result = false;

        switch (sortBy) {
            case QueryMetadataRequest::SortField::ModifiedDate:
                result = a.info.modifiedTime < b.info.modifiedTime;
                break;
            case QueryMetadataRequest::SortField::Title:
                result = a.info.fileName < b.info.fileName;
                break;
            case QueryMetadataRequest::SortField::Size:
                result = a.info.fileSize < b.info.fileSize;
                break;
            case QueryMetadataRequest::SortField::ContentType:
                result = a.info.mimeType < b.info.mimeType;
                break;
            case QueryMetadataRequest::SortField::Path:
                result = a.info.filePath < b.info.filePath;
                break;
            default:
                result = a.info.modifiedTime < b.info.modifiedTime;
        }

        return ascending ? result : !result;
    };

    std::sort(documents.begin(), documents.end(), comparator);
}

Result<void> MetadataApi::exportToJson(const std::vector<metadata::DocumentMetadata>& documents,
                                       const std::string& path, bool compress) {
    try {
        json j = json::array();

        for (const auto& doc : documents) {
            json docJson;
            docJson["id"] = doc.info.id;
            docJson["title"] = doc.info.fileName;
            docJson["path"] = doc.info.filePath;
            docJson["contentType"] = doc.info.mimeType;
            docJson["fileSize"] = doc.info.fileSize;
            docJson["contentHash"] = doc.info.sha256Hash;
            // Add more fields as needed

            j.push_back(docJson);
        }

        std::ofstream file(path);
        if (!file) {
            return Error{ErrorCode::InternalError, "Failed to open output file"};
        }

        file << j.dump(2);

        // TODO: Add compression if requested

        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<void> MetadataApi::exportToCsv(const std::vector<metadata::DocumentMetadata>& documents,
                                      const std::string& path, bool compress) {
    try {
        std::ofstream file(path);
        if (!file) {
            return Error{ErrorCode::InternalError, "Failed to open output file"};
        }

        // Write CSV header
        file << "id,title,path,contentType,fileSize,contentHash\n";

        // Write data
        for (const auto& doc : documents) {
            file << doc.info.id << "," << "\"" << doc.info.fileName << "\"," << "\""
                 << doc.info.filePath << "\"," << doc.info.mimeType << "," << doc.info.fileSize
                 << "," << doc.info.sha256Hash << "\n";
        }

        // TODO: Add compression if requested

        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<std::vector<metadata::DocumentMetadata>>
MetadataApi::importFromJson(const std::string& path) {
    try {
        std::ifstream file(path);
        if (!file) {
            return Error{ErrorCode::FileNotFound, "Failed to open input file"};
        }

        json j;
        file >> j;

        std::vector<metadata::DocumentMetadata> documents;

        for (const auto& item : j) {
            metadata::DocumentMetadata doc;

            if (item.contains("id"))
                doc.info.id = item["id"];
            if (item.contains("title"))
                doc.info.fileName = item["title"];
            if (item.contains("path"))
                doc.info.filePath = item["path"];
            if (item.contains("contentType"))
                doc.info.mimeType = item["contentType"];
            if (item.contains("fileSize"))
                doc.info.fileSize = item["fileSize"];
            if (item.contains("contentHash"))
                doc.info.sha256Hash = item["contentHash"];
            // Parse more fields as needed

            documents.push_back(doc);
        }

        return documents;

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<std::vector<metadata::DocumentMetadata>>
MetadataApi::importFromCsv(const std::string& path) {
    try {
        std::ifstream file(path);
        if (!file) {
            return Error{ErrorCode::FileNotFound, "Failed to open input file"};
        }

        std::vector<metadata::DocumentMetadata> documents;
        std::string line;

        // Skip header
        std::getline(file, line);

        // Read data
        while (std::getline(file, line)) {
            std::stringstream ss(line);
            metadata::DocumentMetadata doc;

            // Simple CSV parsing (would need proper CSV parser in production)
            std::string field;
            int fieldIndex = 0;

            while (std::getline(ss, field, ',')) {
                switch (fieldIndex) {
                    case 0:
                        doc.info.id = std::stoll(field);
                        break;
                    case 1:
                        doc.info.fileName = field;
                        break;
                    case 2:
                        doc.info.filePath = field;
                        break;
                    case 3:
                        doc.info.mimeType = field;
                        break;
                    case 4:
                        doc.info.fileSize = std::stoull(field);
                        break;
                    case 5:
                        doc.info.sha256Hash = field;
                        break;
                }
                fieldIndex++;
            }

            documents.push_back(doc);
        }

        return documents;

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

std::vector<ValidateMetadataResponse::ValidationError>
MetadataApi::validateFields(const metadata::DocumentMetadata& metadata) {
    std::vector<ValidateMetadataResponse::ValidationError> errors;

    // Validate required fields
    if (metadata.info.fileName.empty()) {
        errors.push_back({"fileName", "File name is required", "Provide a document file name"});
    }

    if (metadata.info.filePath.empty()) {
        errors.push_back({"filePath", "File path is required", "Provide a file path"});
    }

    if (metadata.info.mimeType.empty()) {
        errors.push_back({"mimeType", "MIME type is required", "Specify MIME type"});
    }

    if (metadata.info.sha256Hash.empty()) {
        errors.push_back({"sha256Hash", "Content hash is required", "Calculate document hash"});
    }

    // Validate field formats
    if (!metadata.info.filePath.empty() && metadata.info.filePath[0] != '/') {
        errors.push_back(
            {"filePath", "Path must be absolute", "Use absolute path starting with /"});
    }

    return errors;
}

void MetadataApi::updateStatistics(const std::string& operationType, bool success,
                                   std::chrono::milliseconds duration) {
    stats_.totalRequests++;

    if (success) {
        stats_.successfulRequests++;
    } else {
        stats_.failedRequests++;
    }

    stats_.requestsByType[operationType]++;

    // Update average response time
    auto totalTime = stats_.avgResponseTime.count() * (stats_.totalRequests - 1) + duration.count();
    stats_.avgResponseTime = std::chrono::milliseconds(totalTime / stats_.totalRequests);

    // Update max response time
    if (duration > stats_.maxResponseTime) {
        stats_.maxResponseTime = duration;
    }
}

void MetadataApi::logOperation(const std::string& operation, const std::string& details,
                               bool success) {
    // In production, this would write to an audit log
    spdlog::info("Audit: {} - {} - Success: {}", operation, details, success);
}

bool MetadataApi::isHealthy() const {
    // Check repository health
    if (!repository_) {
        return false;
    }

    // Check error rate
    if (stats_.totalRequests > 100) {
        float errorRate =
            static_cast<float>(stats_.failedRequests) / static_cast<float>(stats_.totalRequests);
        if (errorRate > 0.1f) { // More than 10% errors
            return false;
        }
    }

    return true;
}

// ===== Additional Async Methods =====

std::future<GetMetadataResponse> MetadataApi::getMetadataAsync(const GetMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return getMetadata(request); });
}

std::future<UpdateMetadataResponse>
MetadataApi::updateMetadataAsync(const UpdateMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return updateMetadata(request); });
}

std::future<DeleteMetadataResponse>
MetadataApi::deleteMetadataAsync(const DeleteMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return deleteMetadata(request); });
}

std::future<BulkCreateResponse> MetadataApi::bulkCreateAsync(const BulkCreateRequest& request) {
    return std::async(std::launch::async, [this, request]() { return bulkCreate(request); });
}

std::future<QueryMetadataResponse>
MetadataApi::queryMetadataAsync(const QueryMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return queryMetadata(request); });
}

std::future<ExportMetadataResponse>
MetadataApi::exportMetadataAsync(const ExportMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return exportMetadata(request); });
}

std::future<ImportMetadataResponse>
MetadataApi::importMetadataAsync(const ImportMetadataRequest& request) {
    return std::async(std::launch::async, [this, request]() { return importMetadata(request); });
}

// ===== Bulk Update and Delete Operations =====

BulkUpdateResponse MetadataApi::bulkUpdate(const BulkUpdateRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    BulkUpdateResponse response;
    response.requestId = request.requestId;

    try {
        for (const auto& update : request.updates) {
            BulkUpdateResponse::UpdateResult result;
            result.documentId = update.documentId;

            UpdateMetadataRequest updateReq;
            updateReq.documentId = update.documentId;
            updateReq.metadata = update.metadata;
            updateReq.preserveHistory = request.preserveHistory;

            auto updateResp = updateMetadata(updateReq);

            result.success = updateResp.success;
            if (updateResp.success) {
                result.newVersion = updateResp.newVersion;
                response.successCount++;
            } else {
                result.newVersion = -1; // Invalid version for failed operations
                result.error = updateResp.message;
                response.failureCount++;

                if (!request.continueOnError) {
                    break;
                }
            }

            response.results.push_back(result);
        }

        response.allSucceeded = (response.failureCount == 0);
        response.setSuccess("Bulk update completed: " + std::to_string(response.successCount) +
                            " succeeded, " + std::to_string(response.failureCount) + " failed");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("bulk_update", response.success, response.processingTime);

    return response;
}

std::future<BulkUpdateResponse> MetadataApi::bulkUpdateAsync(const BulkUpdateRequest& request) {
    return std::async(std::launch::async, [this, request]() { return bulkUpdate(request); });
}

BulkDeleteResponse MetadataApi::bulkDelete(const BulkDeleteRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    BulkDeleteResponse response;
    response.requestId = request.requestId;

    try {
        for (const auto& docId : request.documentIds) {
            BulkDeleteResponse::DeleteResult result;
            result.documentId = docId;

            DeleteMetadataRequest deleteReq;
            deleteReq.documentId = docId;
            deleteReq.softDelete = request.softDelete;

            auto deleteResp = deleteMetadata(deleteReq);

            result.success = deleteResp.success;
            if (deleteResp.success) {
                response.successCount++;
            } else {
                result.error = deleteResp.message;
                response.failureCount++;

                if (!request.continueOnError) {
                    break;
                }
            }

            response.results.push_back(result);
        }

        response.allSucceeded = (response.failureCount == 0);
        response.setSuccess("Bulk delete completed: " + std::to_string(response.successCount) +
                            " succeeded, " + std::to_string(response.failureCount) + " failed");

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    response.processingTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    updateStatistics("bulk_delete", response.success, response.processingTime);

    return response;
}

std::future<BulkDeleteResponse> MetadataApi::bulkDeleteAsync(const BulkDeleteRequest& request) {
    return std::async(std::launch::async, [this, request]() { return bulkDelete(request); });
}

// ===== Additional Utility Operations =====

GetStatisticsResponse MetadataApi::getStatistics(const GetStatisticsRequest& request) {
    GetStatisticsResponse response;
    response.requestId = request.requestId;

    try {
        // Get all documents for statistics
        auto allDocsResult = repository_->findDocumentsByExtension("");
        if (!allDocsResult) {
            response.setError(ErrorCode::DatabaseError, "Failed to retrieve documents");
            return response;
        }

        // Convert DocumentInfo to DocumentMetadata
        std::vector<metadata::DocumentMetadata> allDocs;
        for (const auto& info : allDocsResult.value()) {
            metadata::DocumentMetadata docMeta;
            docMeta.info = info;
            allDocs.push_back(docMeta);
        }

        auto filteredDocs = applyFilter(allDocs, request.filter);

        response.totalDocuments = filteredDocs.size();

        for (const auto& doc : filteredDocs) {
            response.totalSize += doc.info.fileSize;

            if (request.includeTypeCounts) {
                response.documentsByType[doc.info.mimeType]++;
                response.sizeByType[doc.info.mimeType] += doc.info.fileSize;
            }

            if (request.includeDateStats) {
                if (response.dateStats.oldest == std::chrono::system_clock::time_point{} ||
                    doc.info.createdTime < response.dateStats.oldest) {
                    response.dateStats.oldest = doc.info.createdTime;
                }
                if (doc.info.createdTime > response.dateStats.newest) {
                    response.dateStats.newest = doc.info.createdTime;
                }
            }

            if (request.includeSizeStats) {
                if (response.sizeStats.minSize == 0 ||
                    doc.info.fileSize < response.sizeStats.minSize) {
                    response.sizeStats.minSize = doc.info.fileSize;
                }
                if (doc.info.fileSize > response.sizeStats.maxSize) {
                    response.sizeStats.maxSize = doc.info.fileSize;
                }
            }
        }

        if (!filteredDocs.empty()) {
            response.sizeStats.avgSize = response.totalSize / filteredDocs.size();
        }

        response.setSuccess();

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    return response;
}

GetHistoryResponse MetadataApi::getHistory(const GetHistoryRequest& request) {
    GetHistoryResponse response;
    response.requestId = request.requestId;
    response.documentId = request.documentId;

    try {
        // TODO: Implement version history retrieval
        // This would require version tracking in the repository

        response.setSuccess();

    } catch (const std::exception& e) {
        response.setError(ErrorCode::InternalError, e.what());
    }

    return response;
}

// ===== Factory Implementation =====

std::unique_ptr<MetadataApi>
MetadataApiFactory::create(std::shared_ptr<metadata::MetadataRepository> repository) {
    return std::make_unique<MetadataApi>(repository, nullptr);
}

std::unique_ptr<MetadataApi>
MetadataApiFactory::create(std::shared_ptr<metadata::MetadataRepository> repository,
                           std::shared_ptr<indexing::IDocumentIndexer> indexer) {
    return std::make_unique<MetadataApi>(repository, indexer);
}

std::unique_ptr<MetadataApi>
MetadataApiFactory::create(std::shared_ptr<metadata::MetadataRepository> repository,
                           std::shared_ptr<indexing::IDocumentIndexer> indexer,
                           const MetadataApiConfig& config) {
    return std::make_unique<MetadataApi>(repository, indexer, config);
}

} // namespace yams::api