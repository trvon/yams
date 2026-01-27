// PBI-066: Snapshot request handlers (collections use generic metadata query)
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>

namespace yams::daemon {

boost::asio::awaitable<Response>
RequestDispatcher::handleListSnapshotsRequest([[maybe_unused]] const ListSnapshotsRequest& req) {
    spdlog::debug("ListSnapshots request");

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    auto result = metaRepo->getSnapshots();
    if (!result) {
        co_return ErrorResponse{.code = result.error().code, .message = result.error().message};
    }

    ListSnapshotsResponse resp;
    for (const auto& snapshotId : result.value()) {
        SnapshotInfo info;
        info.id = snapshotId;
        info.label = "";        // Not available from getSnapshots()
        info.createdAt = "";    // Not available from getSnapshots()
        info.documentCount = 0; // Would need separate query
        resp.snapshots.push_back(std::move(info));
    }
    resp.totalCount = resp.snapshots.size();

    spdlog::debug("ListSnapshots: returning {} snapshots", resp.totalCount);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleRestoreCollectionRequest(const RestoreCollectionRequest& req) {
    spdlog::info("RestoreCollection request: collection='{}' output='{}' dry_run={}",
                 req.collection, req.outputDirectory, req.dryRun);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    auto contentStore = serviceManager_ ? serviceManager_->getContentStore() : nullptr;
    if (!contentStore) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Content store unavailable"};
    }

    if (req.collection.empty()) {
        co_return ErrorResponse{.code = ErrorCode::InvalidArgument,
                                .message = "Collection name is required"};
    }

    // Get documents from collection using generic metadata query
    metadata::DocumentQueryOptions queryOpts;
    queryOpts.metadataFilters.emplace_back("collection", req.collection);
    queryOpts.orderByIndexedDesc = true;
    auto docsResult = metaRepo->queryDocuments(queryOpts);
    if (!docsResult) {
        co_return ErrorResponse{.code = docsResult.error().code,
                                .message = "Failed to find collection documents: " +
                                           docsResult.error().message};
    }

    const auto& documents = docsResult.value();
    if (documents.empty()) {
        RestoreCollectionResponse resp;
        resp.filesRestored = 0;
        resp.dryRun = req.dryRun;
        spdlog::info("RestoreCollection: no documents found in collection '{}'", req.collection);
        co_return resp;
    }

    RestoreCollectionResponse resp;
    resp.dryRun = req.dryRun;

    // Create output directory if needed
    std::filesystem::path outputDir(req.outputDirectory);
    if (!req.dryRun && req.createDirs) {
        std::error_code ec;
        std::filesystem::create_directories(outputDir, ec);
        if (ec) {
            co_return ErrorResponse{.code = ErrorCode::IOError,
                                    .message =
                                        "Failed to create output directory: " + ec.message()};
        }
    }

    // Helper to check include/exclude patterns
    auto matchesPattern = [](const std::string& path, const std::string& pattern) -> bool {
        // Simple wildcard matching: convert glob * to regex .*
        std::string regexPattern = pattern;
        size_t pos = 0;
        while ((pos = regexPattern.find('*', pos)) != std::string::npos) {
            regexPattern.replace(pos, 1, ".*");
            pos += 2;
        }
        try {
            std::regex re(regexPattern);
            return std::regex_search(path, re);
        } catch (...) {
            return false;
        }
    };

    auto shouldInclude = [&](const metadata::DocumentInfo& doc) -> bool {
        // Check include patterns
        if (!req.includePatterns.empty()) {
            bool matches = false;
            for (const auto& pattern : req.includePatterns) {
                if (matchesPattern(doc.filePath, pattern)) {
                    matches = true;
                    break;
                }
            }
            if (!matches)
                return false;
        }

        // Check exclude patterns
        for (const auto& pattern : req.excludePatterns) {
            if (matchesPattern(doc.filePath, pattern)) {
                return false;
            }
        }

        return true;
    };

    // Helper to expand layout template
    auto expandLayout = [&](const metadata::DocumentInfo& doc) -> std::string {
        if (req.layoutTemplate.empty()) {
            // Default: preserve original path structure
            return (outputDir / doc.filePath).string();
        }

        // Simple template expansion: {hash}, {name}, {ext}
        std::string result = req.layoutTemplate;

        size_t pos = 0;
        while ((pos = result.find("{hash}", pos)) != std::string::npos) {
            result.replace(pos, 6, doc.sha256Hash);
            pos += doc.sha256Hash.length();
        }

        std::filesystem::path docPath(doc.filePath);
        std::string name = docPath.stem().string();
        std::string ext = docPath.extension().string();

        pos = 0;
        while ((pos = result.find("{name}", pos)) != std::string::npos) {
            result.replace(pos, 6, name);
            pos += name.length();
        }

        pos = 0;
        while ((pos = result.find("{ext}", pos)) != std::string::npos) {
            result.replace(pos, 5, ext);
            pos += ext.length();
        }

        return (outputDir / result).string();
    };

    // Process each document
    for (const auto& doc : documents) {
        if (!shouldInclude(doc)) {
            continue;
        }

        std::string targetPath = expandLayout(doc);

        // Check overwrite policy
        if (!req.overwrite && std::filesystem::exists(targetPath)) {
            RestoredFile skip;
            skip.path = targetPath;
            skip.hash = doc.sha256Hash;
            skip.size = doc.fileSize;
            skip.skipped = true;
            skip.skipReason = "File exists (overwrite=false)";
            resp.files.push_back(std::move(skip));
            continue;
        }

        // Restore file using content store
        if (!req.dryRun) {
            auto content = contentStore->retrieveBytes(doc.sha256Hash);
            if (!content) {
                RestoredFile fail;
                fail.path = targetPath;
                fail.hash = doc.sha256Hash;
                fail.size = doc.fileSize;
                fail.skipped = true;
                fail.skipReason = "Failed to retrieve content: " + content.error().message;
                resp.files.push_back(std::move(fail));
                continue;
            }

            // Create parent directories
            std::filesystem::path targetFilePath(targetPath);
            if (targetFilePath.has_parent_path()) {
                std::error_code ec;
                std::filesystem::create_directories(targetFilePath.parent_path(), ec);
                if (ec) {
                    RestoredFile fail;
                    fail.path = targetPath;
                    fail.hash = doc.sha256Hash;
                    fail.size = doc.fileSize;
                    fail.skipped = true;
                    fail.skipReason = "Failed to create parent directory: " + ec.message();
                    resp.files.push_back(std::move(fail));
                    continue;
                }
            }

            // Write file
            std::ofstream out(targetPath, std::ios::binary);
            if (!out) {
                RestoredFile fail;
                fail.path = targetPath;
                fail.hash = doc.sha256Hash;
                fail.size = doc.fileSize;
                fail.skipped = true;
                fail.skipReason = "Failed to open output file";
                resp.files.push_back(std::move(fail));
                continue;
            }

            out.write(reinterpret_cast<const char*>(content.value().data()),
                      static_cast<std::streamsize>(content.value().size()));
            out.close();

            if (!out.good()) {
                RestoredFile fail;
                fail.path = targetPath;
                fail.hash = doc.sha256Hash;
                fail.size = doc.fileSize;
                fail.skipped = true;
                fail.skipReason = "Failed to write file content";
                resp.files.push_back(std::move(fail));
                continue;
            }
        }

        RestoredFile success;
        success.path = targetPath;
        success.hash = doc.sha256Hash;
        success.size = doc.fileSize;
        success.skipped = false;
        resp.files.push_back(std::move(success));
        resp.filesRestored++;
    }

    spdlog::info("RestoreCollection: restored {} files (dry_run={})", resp.filesRestored,
                 resp.dryRun);
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleRestoreSnapshotRequest(const RestoreSnapshotRequest& req) {
    spdlog::info("RestoreSnapshot request: snapshot='{}' output='{}' dry_run={}", req.snapshotId,
                 req.outputDirectory, req.dryRun);

    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    auto contentStore = serviceManager_ ? serviceManager_->getContentStore() : nullptr;
    if (!contentStore) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Content store unavailable"};
    }

    if (req.snapshotId.empty()) {
        co_return ErrorResponse{.code = ErrorCode::InvalidArgument,
                                .message = "Snapshot ID is required"};
    }

    // Get documents from snapshot
    auto docsResult = metaRepo->findDocumentsBySnapshot(req.snapshotId);
    if (!docsResult) {
        co_return ErrorResponse{.code = docsResult.error().code,
                                .message = "Failed to find snapshot documents: " +
                                           docsResult.error().message};
    }

    // Reuse RestoreCollection logic by converting request
    RestoreCollectionRequest collectionReq;
    collectionReq.collection = req.snapshotId; // Not used, but kept for logging
    collectionReq.outputDirectory = req.outputDirectory;
    collectionReq.layoutTemplate = req.layoutTemplate;
    collectionReq.includePatterns = req.includePatterns;
    collectionReq.excludePatterns = req.excludePatterns;
    collectionReq.overwrite = req.overwrite;
    collectionReq.createDirs = req.createDirs;
    collectionReq.dryRun = req.dryRun;

    // Process documents (same logic as RestoreCollection)
    const auto& documents = docsResult.value();
    if (documents.empty()) {
        RestoreSnapshotResponse resp;
        resp.filesRestored = 0;
        resp.dryRun = req.dryRun;
        spdlog::info("RestoreSnapshot: no documents found in snapshot '{}'", req.snapshotId);
        co_return resp;
    }

    RestoreSnapshotResponse resp;
    resp.dryRun = req.dryRun;

    std::filesystem::path outputDir(req.outputDirectory);
    if (!req.dryRun && req.createDirs) {
        std::error_code ec;
        std::filesystem::create_directories(outputDir, ec);
        if (ec) {
            co_return ErrorResponse{.code = ErrorCode::IOError,
                                    .message =
                                        "Failed to create output directory: " + ec.message()};
        }
    }

    auto matchesPattern = [](const std::string& path, const std::string& pattern) -> bool {
        std::string regexPattern = pattern;
        size_t pos = 0;
        while ((pos = regexPattern.find('*', pos)) != std::string::npos) {
            regexPattern.replace(pos, 1, ".*");
            pos += 2;
        }
        try {
            std::regex re(regexPattern);
            return std::regex_search(path, re);
        } catch (...) {
            return false;
        }
    };

    auto shouldInclude = [&](const metadata::DocumentInfo& doc) -> bool {
        if (!req.includePatterns.empty()) {
            bool matches = false;
            for (const auto& pattern : req.includePatterns) {
                if (matchesPattern(doc.filePath, pattern)) {
                    matches = true;
                    break;
                }
            }
            if (!matches)
                return false;
        }

        for (const auto& pattern : req.excludePatterns) {
            if (matchesPattern(doc.filePath, pattern)) {
                return false;
            }
        }

        return true;
    };

    auto expandLayout = [&](const metadata::DocumentInfo& doc) -> std::string {
        if (req.layoutTemplate.empty()) {
            return (outputDir / doc.filePath).string();
        }

        std::string result = req.layoutTemplate;
        size_t pos = 0;
        while ((pos = result.find("{hash}", pos)) != std::string::npos) {
            result.replace(pos, 6, doc.sha256Hash);
            pos += doc.sha256Hash.length();
        }

        std::filesystem::path docPath(doc.filePath);
        std::string name = docPath.stem().string();
        std::string ext = docPath.extension().string();

        pos = 0;
        while ((pos = result.find("{name}", pos)) != std::string::npos) {
            result.replace(pos, 6, name);
            pos += name.length();
        }

        pos = 0;
        while ((pos = result.find("{ext}", pos)) != std::string::npos) {
            result.replace(pos, 5, ext);
            pos += ext.length();
        }

        return (outputDir / result).string();
    };

    for (const auto& doc : documents) {
        if (!shouldInclude(doc)) {
            continue;
        }

        std::string targetPath = expandLayout(doc);

        if (!req.overwrite && std::filesystem::exists(targetPath)) {
            RestoredFile skip;
            skip.path = targetPath;
            skip.hash = doc.sha256Hash;
            skip.size = doc.fileSize;
            skip.skipped = true;
            skip.skipReason = "File exists (overwrite=false)";
            resp.files.push_back(std::move(skip));
            continue;
        }

        if (!req.dryRun) {
            auto content = contentStore->retrieveBytes(doc.sha256Hash);
            if (!content) {
                RestoredFile fail;
                fail.path = targetPath;
                fail.hash = doc.sha256Hash;
                fail.size = doc.fileSize;
                fail.skipped = true;
                fail.skipReason = "Failed to retrieve content: " + content.error().message;
                resp.files.push_back(std::move(fail));
                continue;
            }

            std::filesystem::path targetFilePath(targetPath);
            if (targetFilePath.has_parent_path()) {
                std::error_code ec;
                std::filesystem::create_directories(targetFilePath.parent_path(), ec);
                if (ec) {
                    RestoredFile fail;
                    fail.path = targetPath;
                    fail.hash = doc.sha256Hash;
                    fail.size = doc.fileSize;
                    fail.skipped = true;
                    fail.skipReason = "Failed to create parent directory: " + ec.message();
                    resp.files.push_back(std::move(fail));
                    continue;
                }
            }

            std::ofstream out(targetPath, std::ios::binary);
            if (!out) {
                RestoredFile fail;
                fail.path = targetPath;
                fail.hash = doc.sha256Hash;
                fail.size = doc.fileSize;
                fail.skipped = true;
                fail.skipReason = "Failed to open output file";
                resp.files.push_back(std::move(fail));
                continue;
            }

            out.write(reinterpret_cast<const char*>(content.value().data()),
                      static_cast<std::streamsize>(content.value().size()));
            out.close();

            if (!out.good()) {
                RestoredFile fail;
                fail.path = targetPath;
                fail.hash = doc.sha256Hash;
                fail.size = doc.fileSize;
                fail.skipped = true;
                fail.skipReason = "Failed to write file content";
                resp.files.push_back(std::move(fail));
                continue;
            }
        }

        RestoredFile success;
        success.path = targetPath;
        success.hash = doc.sha256Hash;
        success.size = doc.fileSize;
        success.skipped = false;
        resp.files.push_back(std::move(success));
        resp.filesRestored++;
    }

    spdlog::info("RestoreSnapshot: restored {} files (dry_run={})", resp.filesRestored,
                 resp.dryRun);
    co_return resp;
}

} // namespace yams::daemon
