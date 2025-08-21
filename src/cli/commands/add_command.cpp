#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>
#include <sstream>
#include <yams/api/content_metadata.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/embedding_service.h>
// Daemon client API for daemon-first add
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::cli {

using json = nlohmann::json;

class AddCommand : public ICommand {
public:
    std::string getName() const override { return "add"; }

    std::string getDescription() const override {
        return "Add document(s) or directory to the content store";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("add", getDescription());
        // Accept multiple paths as positional arguments - use expected(-1) for unlimited
        cmd->add_option("paths", targetPaths_,
                        "Path(s) to file/directory to add (use '-' for stdin)")
            ->expected(-1);

        cmd->add_option("-n,--name", documentName_,
                        "Name for the document (especially useful for stdin)");
        cmd->add_option("-t,--tags", tags_, "Tags for the document (comma-separated)");
        cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs");
        cmd->add_option("--mime-type", mimeType_, "MIME type of the document");
        cmd->add_flag("--no-auto-mime", disableAutoMime_, "Disable automatic MIME type detection");
        cmd->add_flag("--no-embeddings", noEmbeddings_,
                      "Disable automatic embedding generation for added documents");

        // Collection and snapshot options
        cmd->add_option("-c,--collection", collection_, "Collection name for organizing documents");
        cmd->add_option("--snapshot-id", snapshotId_, "Unique snapshot identifier");
        cmd->add_option("--snapshot-label", snapshotLabel_, "User-friendly snapshot label");

        // Directory options
        cmd->add_flag("-r,--recursive", recursive_, "Recursively add files from directories");
        cmd->add_option("--include", includePatterns_,
                        "File patterns to include (e.g., '*.txt,*.md')");
        cmd->add_option("--exclude", excludePatterns_,
                        "File patterns to exclude (e.g., '*.tmp,*.log')");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            // Handle default stdin case
            if (targetPaths_.empty()) {
                targetPaths_.push_back("-");
            }

            // Process each path
            for (const auto& pathStr : targetPaths_) {
                auto result = processPath(pathStr);
                if (!result) {
                    return result; // Return error immediately
                }
            }

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

private:
    Result<void> processPath(const std::string& pathStr) {
        std::filesystem::path targetPath(pathStr);

        // Attempt daemon-first add; fall back to local on failure
        {
            // Skip daemon for stdin input (requires special handling)
            if (pathStr != "-") {
                yams::daemon::AddDocumentRequest dreq;
                // Convert to absolute path so daemon can find it regardless of its working
                // directory
                dreq.path = std::filesystem::absolute(targetPath).string();
                dreq.name = documentName_;
                dreq.tags = tags_;

                // Parse metadata key=value pairs
                for (const auto& kv : metadata_) {
                    auto pos = kv.find('=');
                    if (pos != std::string::npos) {
                        std::string key = kv.substr(0, pos);
                        std::string value = kv.substr(pos + 1);
                        dreq.metadata[key] = value;
                    }
                }

                dreq.recursive = recursive_;
                dreq.includePatterns = includePatterns_;
                dreq.excludePatterns = excludePatterns_;
                dreq.collection = collection_;
                dreq.snapshotId = snapshotId_;
                dreq.snapshotLabel = snapshotLabel_;
                dreq.mimeType = mimeType_;
                dreq.disableAutoMime = disableAutoMime_;
                dreq.noEmbeddings = noEmbeddings_;

                auto render = [&](const yams::daemon::AddDocumentResponse& resp) -> Result<void> {
                    // Display results
                    if (resp.documentsAdded == 1) {
                        std::cout << "Added document: " << resp.hash.substr(0, 16) << "..."
                                  << std::endl;
                    } else {
                        std::cout << "Added " << resp.documentsAdded << " documents" << std::endl;
                    }
                    return Result<void>();
                };

                auto fallback = [&, targetPath]() -> Result<void> {
                    // Fall back to local execution
                    return executeLocal(targetPath);
                };

                auto d = daemon_first(dreq, fallback, render);
                if (d) {
                    return Result<void>();
                }
                // If daemon returned an actual error (not network/not-implemented), return it
                if (d.error().code != ErrorCode::NetworkError &&
                    d.error().code != ErrorCode::NotImplemented) {
                    return d.error();
                }
            }
        }

        // Fall back to local execution for stdin or if daemon failed to connect
        return executeLocal(targetPath);
    }

private:
    std::string formatSize(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);
        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << size << " " << units[unitIndex];
        return oss.str();
    }

    Result<void> executeLocal(const std::filesystem::path& targetPath) {
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }
        auto store = cli_->getContentStore();
        if (!store) {
            return Error{ErrorCode::NotInitialized, "Content store not initialized"};
        }

        // Check if reading from stdin
        if (targetPath.string() == "-") {
            return storeFromStdin(store);
        }

        // Validate path exists
        if (!std::filesystem::exists(targetPath)) {
            return Error{ErrorCode::FileNotFound, "Path not found: " + targetPath.string()};
        }

        // Generate snapshot ID if not provided but snapshot label is given
        if (snapshotId_.empty() && !snapshotLabel_.empty()) {
            snapshotId_ = generateSnapshotId();
        }

        // Handle directory vs file
        if (std::filesystem::is_directory(targetPath)) {
            return storeDirectory(store, targetPath);
        } else {
            return storeFile(store, targetPath);
        }
    }

private:
    Result<void> storeFromStdin(std::shared_ptr<api::IContentStore> store) {
        // Read all content from stdin
        std::string content;
        std::string line;
        while (std::getline(std::cin, line)) {
            content += line + "\n";
        }

        if (content.empty()) {
            return Error{ErrorCode::InvalidArgument, "No content received from stdin"};
        }

        // Create a temporary file
        std::filesystem::path tempPath =
            std::filesystem::temp_directory_path() /
            ("yams_stdin_" +
             std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));

        std::ofstream tempFile(tempPath, std::ios::binary);
        if (!tempFile) {
            return Error{ErrorCode::FileNotFound, "Failed to create temporary file"};
        }
        tempFile.write(content.data(), static_cast<std::streamsize>(content.size()));
        tempFile.close();

        // Build metadata using helper (will auto-detect MIME from temp file if enabled)
        api::ContentMetadata metadata = buildMetadata("stdin", tempPath);

        // Set current file info for embedding worker
        currentFilePath_ = documentName_.empty() ? "stdin" : documentName_;
        currentMimeType_ = metadata.mimeType;

        // Store the file
        auto result = store->store(tempPath, metadata);

        // Clean up temp file
        std::filesystem::remove(tempPath);

        if (!result) {
            return Error{result.error().code, result.error().message};
        }

        // Store metadata in database
        auto metadataRepo = cli_->getMetadataRepository();
        if (metadataRepo) {
            int64_t docId = -1;
            bool isNewDocument = true;

            // Check if document already exists (100% dedup case)
            if (result.value().dedupRatio() >= 0.99) { // Near 100% dedup
                auto existingDoc = metadataRepo->getDocumentByHash(result.value().contentHash);
                if (existingDoc && existingDoc.value().has_value()) {
                    // Document already exists, update it
                    isNewDocument = false;
                    docId = existingDoc.value()->id;

                    // Update indexed time (make a copy since it's const)
                    auto doc = existingDoc.value().value();
                    doc.indexedTime = std::chrono::system_clock::now();
                    metadataRepo->updateDocument(doc);

                    if (cli_->getVerbose()) {
                        spdlog::debug("Document already exists with ID: {}, updating metadata",
                                      docId);
                    }
                }
            }

            // Insert new document if it doesn't exist
            if (isNewDocument) {
                metadata::DocumentInfo docInfo;
                docInfo.filePath = "stdin";
                docInfo.fileName = documentName_.empty() ? "stdin" : documentName_;
                docInfo.fileExtension = "";
                docInfo.fileSize = static_cast<int64_t>(content.size());
                docInfo.sha256Hash = result.value().contentHash;
                docInfo.mimeType = metadata.mimeType;

                auto now = std::chrono::system_clock::now();
                docInfo.createdTime = now;
                docInfo.modifiedTime = now;
                docInfo.indexedTime = now;

                auto insertResult = metadataRepo->insertDocument(docInfo);
                if (insertResult) {
                    docId = insertResult.value();
                    if (cli_->getVerbose()) {
                        spdlog::debug("Stored new stdin document with ID: {}", docId);
                    }
                } else {
                    spdlog::warn("Failed to store metadata for stdin: {}",
                                 insertResult.error().message);
                }
            }

            // Add tags and metadata for both new and existing documents
            if (docId > 0) {
                // Add tags as metadata - parse comma-separated values
                for (const auto& tagStr : tags_) {
                    // Each element in tags_ might contain comma-separated tags
                    std::stringstream ss(tagStr);
                    std::string tag;
                    while (std::getline(ss, tag, ',')) {
                        // Trim whitespace
                        tag.erase(0, tag.find_first_not_of(" \t"));
                        tag.erase(tag.find_last_not_of(" \t") + 1);
                        if (!tag.empty()) {
                            metadataRepo->setMetadata(docId, "tag", metadata::MetadataValue(tag));
                        }
                    }
                }

                // Add custom metadata
                for (const auto& kv : metadata_) {
                    auto pos = kv.find('=');
                    if (pos != std::string::npos) {
                        std::string key = kv.substr(0, pos);
                        std::string value = kv.substr(pos + 1);
                        metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
                    }
                }

                // Index document content for full-text search (only for new documents)
                if (isNewDocument) {
                    auto indexResult = metadataRepo->indexDocumentContent(
                        docId, documentName_.empty() ? "stdin" : documentName_,
                        content, // We already have the content from stdin
                        metadata.mimeType);
                    if (!indexResult) {
                        spdlog::warn("Failed to index stdin content: {}",
                                     indexResult.error().message);
                    }
                }

                // Update fuzzy index
                metadataRepo->updateFuzzyIndex(docId);
            }
        }

        outputResult(result.value());
        return Result<void>();
    }

    Result<void> storeFile(std::shared_ptr<api::IContentStore> store,
                           const std::filesystem::path& filePath) {
        // Build metadata
        api::ContentMetadata metadata = buildMetadata(filePath.filename().string(), filePath);

        // Set current file info for embedding worker
        currentFilePath_ = filePath;
        currentMimeType_ = metadata.mimeType;

        // Store the file
        auto result = store->store(filePath, metadata);
        if (!result) {
            return Error{result.error().code, result.error().message};
        }

        // Store metadata in database
        auto storeMetadataResult = storeFileMetadata(filePath, result.value());
        if (!storeMetadataResult) {
            spdlog::warn("Failed to store metadata: {}", storeMetadataResult.error().message);
        }

        outputResult(result.value());
        return Result<void>();
    }

    Result<void> storeDirectory(std::shared_ptr<api::IContentStore> store,
                                const std::filesystem::path& targetPath) {
        if (!recursive_) {
            return Error{ErrorCode::InvalidArgument, "Directory specified but --recursive not set"};
        }

        std::vector<std::filesystem::path> filesToAdd;

        // Collect files to add
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(targetPath)) {
                if (!entry.is_regular_file())
                    continue;

                // Check include/exclude patterns
                if (!shouldIncludeFile(entry.path()))
                    continue;

                filesToAdd.push_back(entry.path());
            }
        } catch (const std::filesystem::filesystem_error& e) {
            return Error{ErrorCode::FileNotFound,
                         "Failed to traverse directory: " + std::string(e.what())};
        }

        if (filesToAdd.empty()) {
            return Error{ErrorCode::InvalidArgument, "No files found to add in directory"};
        }

        // Process each file
        size_t successCount = 0;
        size_t failureCount = 0;

        for (const auto& filePath : filesToAdd) {
            if (cli_->getVerbose()) {
                spdlog::debug("Adding: {}", filePath.string());
            }

            // Build metadata with collection/snapshot info
            api::ContentMetadata metadata = buildMetadata(filePath.filename().string(), filePath);

            // Store the file
            auto result = store->store(filePath, metadata);
            if (!result) {
                spdlog::error("Failed to store {}: {}", filePath.string(), result.error().message);
                failureCount++;
                continue;
            }

            // Store metadata in database
            auto storeMetadataResult = storeFileMetadata(filePath, result.value(), targetPath);
            if (!storeMetadataResult) {
                spdlog::warn("Failed to store metadata for {}: {}", filePath.string(),
                             storeMetadataResult.error().message);
            }

            successCount++;
        }

        // Output summary
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["files_processed"] = filesToAdd.size();
            output["files_added"] = successCount;
            output["files_failed"] = failureCount;
            output["collection"] = collection_;
            output["snapshot_id"] = snapshotId_;
            output["snapshot_label"] = snapshotLabel_;
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "Directory processing complete!" << std::endl;
            std::cout << "Files added: " << successCount << std::endl;
            std::cout << "Files failed: " << failureCount << std::endl;
            if (!collection_.empty())
                std::cout << "Collection: " << collection_ << std::endl;
            if (!snapshotId_.empty())
                std::cout << "Snapshot ID: " << snapshotId_ << std::endl;
            if (!snapshotLabel_.empty())
                std::cout << "Snapshot Label: " << snapshotLabel_ << std::endl;
        }

        return Result<void>();
    }

    api::ContentMetadata buildMetadata(const std::string& defaultName,
                                       const std::filesystem::path& filePath = {}) {
        api::ContentMetadata metadata;
        metadata.name = documentName_.empty() ? defaultName : documentName_;

        // Set MIME type with auto-detection fallback
        if (!mimeType_.empty()) {
            // User explicitly provided MIME type
            metadata.mimeType = mimeType_;
        } else if (!disableAutoMime_) {
            // Auto-detect MIME type
            metadata.mimeType = detectMimeType(filePath, defaultName);
        }
        // If both user MIME type and auto-detection are disabled, leave empty

        // Convert vector tags to unordered_map - parse comma-separated values
        for (const auto& tagStr : tags_) {
            // Each element in tags_ might contain comma-separated tags
            std::stringstream ss(tagStr);
            std::string tag;
            while (std::getline(ss, tag, ',')) {
                // Trim whitespace
                tag.erase(0, tag.find_first_not_of(" \t"));
                tag.erase(tag.find_last_not_of(" \t") + 1);
                if (!tag.empty()) {
                    metadata.tags[tag] = "";
                }
            }
        }

        // Add custom metadata
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                metadata.tags[key] = value;
            }
        }

        // Add collection and snapshot metadata
        if (!collection_.empty()) {
            metadata.tags["collection"] = collection_;
        }
        if (!snapshotId_.empty()) {
            metadata.tags["snapshot_id"] = snapshotId_;
        }
        if (!snapshotLabel_.empty()) {
            metadata.tags["snapshot_label"] = snapshotLabel_;
        }

        return metadata;
    }

    std::string detectMimeType(const std::filesystem::path& filePath, const std::string& fileName) {
        // Try signature-based detection first (if we have a file path)
        if (!filePath.empty() && std::filesystem::exists(filePath)) {
            try {
                auto detection = detection::FileTypeDetector::instance().detectFromFile(filePath);
                if (detection && !detection.value().mimeType.empty()) {
                    if (cli_->getVerbose()) {
                        spdlog::debug("Detected MIME type '{}' for file '{}'",
                                      detection.value().mimeType, filePath.string());
                    }
                    return detection.value().mimeType;
                }
            } catch (const std::exception& e) {
                if (cli_->getVerbose()) {
                    spdlog::debug("Signature detection failed for '{}': {}", filePath.string(),
                                  e.what());
                }
                // Continue to extension-based detection
            }
        }

        // Fallback to extension-based detection
        std::string extension;
        if (!filePath.empty()) {
            extension = filePath.extension().string();
        } else if (!fileName.empty()) {
            // Extract extension from filename
            auto pos = fileName.rfind('.');
            if (pos != std::string::npos) {
                extension = fileName.substr(pos);
            }
        }

        if (!extension.empty()) {
            std::string mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(extension);
            if (cli_->getVerbose()) {
                spdlog::debug("Detected MIME type '{}' from extension '{}' for '{}'", mimeType,
                              extension, fileName);
            }
            return mimeType;
        }

        // Default fallback - use text/plain for stdin, application/octet-stream for files
        std::string defaultMime = (fileName == "stdin") ? "text/plain" : "application/octet-stream";
        if (cli_->getVerbose()) {
            spdlog::debug("No MIME type detected for '{}', using default '{}'", fileName,
                          defaultMime);
        }
        return defaultMime;
    }

    Result<void> storeFileMetadata(const std::filesystem::path& filePath,
                                   const api::StoreResult& storeResult,
                                   const std::filesystem::path& baseDirectory = {}) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        int64_t docId = -1;
        bool isNewDocument = true;

        // Check if document already exists (high dedup case)
        if (storeResult.dedupRatio() >= 0.99) { // Near 100% dedup
            auto existingDoc = metadataRepo->getDocumentByHash(storeResult.contentHash);
            if (existingDoc && existingDoc.value().has_value()) {
                // Document already exists, update it
                isNewDocument = false;
                docId = existingDoc.value()->id;

                // Update indexed time (make a copy since it's const)
                auto doc = existingDoc.value().value();
                doc.indexedTime = std::chrono::system_clock::now();
                metadataRepo->updateDocument(doc);

                if (cli_->getVerbose()) {
                    spdlog::debug("Document already exists with ID: {}, updating metadata", docId);
                }
            }
        }

        // Insert new document if it doesn't exist
        if (isNewDocument) {
            metadata::DocumentInfo docInfo;
            docInfo.filePath = filePath.string();
            docInfo.fileName = filePath.filename().string();
            docInfo.fileExtension = filePath.extension().string();
            docInfo.fileSize = static_cast<int64_t>(std::filesystem::file_size(filePath));
            docInfo.sha256Hash = storeResult.contentHash;
            docInfo.mimeType = mimeType_.empty() ? "application/octet-stream" : mimeType_;

            auto now = std::chrono::system_clock::now();
            docInfo.createdTime = now;

            // Convert file_time_type to system_clock::time_point
            auto ftime = std::filesystem::last_write_time(filePath);
            auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                ftime - std::filesystem::file_time_type::clock::now() +
                std::chrono::system_clock::now());
            docInfo.modifiedTime = sctp;
            docInfo.indexedTime = now;

            auto insertResult = metadataRepo->insertDocument(docInfo);
            if (!insertResult) {
                return insertResult.error();
            }

            docId = insertResult.value();
            if (cli_->getVerbose()) {
                spdlog::debug("Stored new document with ID: {}", docId);
            }
        }

        if (docId < 0) {
            return Error{ErrorCode::Unknown, "Failed to get document ID"};
        }

        // Add tags as metadata - parse comma-separated values
        for (const auto& tagStr : tags_) {
            // Each element in tags_ might contain comma-separated tags
            std::stringstream ss(tagStr);
            std::string tag;
            while (std::getline(ss, tag, ',')) {
                // Trim whitespace
                tag.erase(0, tag.find_first_not_of(" \t"));
                tag.erase(tag.find_last_not_of(" \t") + 1);
                if (!tag.empty()) {
                    metadataRepo->setMetadata(docId, "tag", metadata::MetadataValue(tag));
                }
            }
        }

        // Add custom metadata
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
            }
        }

        // Add collection and snapshot metadata
        if (!collection_.empty()) {
            metadataRepo->setMetadata(docId, "collection", metadata::MetadataValue(collection_));
        }
        if (!snapshotId_.empty()) {
            metadataRepo->setMetadata(docId, "snapshot_id", metadata::MetadataValue(snapshotId_));
        }
        if (!snapshotLabel_.empty()) {
            metadataRepo->setMetadata(docId, "snapshot_label",
                                      metadata::MetadataValue(snapshotLabel_));
        }

        // Add relative path metadata for directory operations
        if (!baseDirectory.empty() && std::filesystem::is_directory(baseDirectory)) {
            auto relativePath = std::filesystem::relative(filePath, baseDirectory);
            metadataRepo->setMetadata(docId, "path",
                                      metadata::MetadataValue(relativePath.string()));
        }

        // Read file content for indexing (only for new documents)
        if (isNewDocument) {
            std::string fileContent;
            try {
                std::ifstream file(filePath, std::ios::binary);
                if (file) {
                    std::stringstream buffer;
                    buffer << file.rdbuf();
                    fileContent = buffer.str();

                    // Index document content for full-text search
                    auto indexResult = metadataRepo->indexDocumentContent(
                        docId, filePath.filename().string(), fileContent,
                        mimeType_.empty() ? "application/octet-stream" : mimeType_);
                    if (!indexResult) {
                        spdlog::warn("Failed to index document content: {}",
                                     indexResult.error().message);
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("Failed to read file for indexing: {}", e.what());
            }
        }

        // Update fuzzy index
        metadataRepo->updateFuzzyIndex(docId);

        return Result<void>();
    }

    bool shouldIncludeFile(const std::filesystem::path& filePath) {
        std::string fileName = filePath.filename().string();

        // Split comma-separated patterns
        auto splitPatterns = [](const std::vector<std::string>& patterns) {
            std::vector<std::string> result;
            for (const auto& pattern : patterns) {
                std::stringstream ss(pattern);
                std::string item;
                while (std::getline(ss, item, ',')) {
                    // Trim whitespace
                    item.erase(0, item.find_first_not_of(" \t"));
                    item.erase(item.find_last_not_of(" \t") + 1);
                    if (!item.empty()) {
                        result.push_back(item);
                    }
                }
            }
            return result;
        };

        auto expandedExcludePatterns = splitPatterns(excludePatterns_);
        auto expandedIncludePatterns = splitPatterns(includePatterns_);

        // Check exclude patterns first
        for (const auto& pattern : expandedExcludePatterns) {
            if (matchesPattern(fileName, pattern)) {
                return false;
            }
        }

        // If no include patterns specified, include all files (that weren't excluded)
        if (expandedIncludePatterns.empty()) {
            return true;
        }

        // Check include patterns
        for (const auto& pattern : expandedIncludePatterns) {
            if (matchesPattern(fileName, pattern)) {
                return true;
            }
        }

        return false;
    }

    bool matchesPattern(const std::string& str, const std::string& pattern) {
        // Efficient iterative wildcard matching (* and ?)
        size_t strIdx = 0, patIdx = 0;
        size_t strLen = str.length(), patLen = pattern.length();
        size_t lastWildcard = std::string::npos, lastMatch = 0;

        while (strIdx < strLen) {
            if (patIdx < patLen && (pattern[patIdx] == '?' || pattern[patIdx] == str[strIdx])) {
                strIdx++;
                patIdx++;
            } else if (patIdx < patLen && pattern[patIdx] == '*') {
                lastWildcard = patIdx;
                lastMatch = strIdx;
                patIdx++;
            } else if (lastWildcard != std::string::npos) {
                patIdx = lastWildcard + 1;
                lastMatch++;
                strIdx = lastMatch;
            } else {
                return false;
            }
        }

        while (patIdx < patLen && pattern[patIdx] == '*') {
            patIdx++;
        }

        return patIdx == patLen;
    }

    std::string generateSnapshotId() {
        auto now = std::chrono::system_clock::now();
        auto timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        // Generate random component
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1000, 9999);

        std::stringstream ss;
        ss << "snapshot_" << timestamp << "_" << dis(gen);
        return ss.str();
    }

    void outputResult(const api::StoreResult& storeResult) {
        // Generate embedding asynchronously if not disabled
        // Note: Embedding repair is automatically triggered during storage initialization
        // No need to manually trigger here to avoid duplicate repair threads
        if (!noEmbeddings_ && cli_->getVerbose()) {
            spdlog::debug("Embedding generation handled by storage initialization");
        }

        // Output in JSON if --json flag is set, or if --verbose is set
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["hash"] = storeResult.contentHash;
            output["bytes_stored"] = storeResult.bytesStored;
            output["bytes_deduped"] = storeResult.bytesDeduped;
            output["dedup_ratio"] = storeResult.dedupRatio();
            output["duration_ms"] = storeResult.duration.count();

            std::cout << output.dump(2) << std::endl;
        } else {
            // Simple, concise output by default
            std::cout << "Document added successfully!" << std::endl;
            std::cout << "Hash: " << storeResult.contentHash << std::endl;
            std::cout << "Bytes stored: " << storeResult.bytesStored << std::endl;
            std::cout << "Bytes deduped: " << storeResult.bytesDeduped << std::endl;
            std::cout << "Dedup ratio: " << (storeResult.dedupRatio() * 100) << "%" << std::endl;
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::vector<std::string> targetPaths_; // Changed to vector for multiple paths
    std::string documentName_;
    std::vector<std::string> tags_;
    std::vector<std::string> metadata_;
    std::string mimeType_;
    bool disableAutoMime_ = false;
    bool noEmbeddings_ = false;

    // Current file being processed (for embedding worker)
    std::filesystem::path currentFilePath_;
    std::string currentMimeType_;

    // Collection and snapshot options
    std::string collection_;
    std::string snapshotId_;
    std::string snapshotLabel_;

    // Directory options
    bool recursive_ = false;
    std::vector<std::string> includePatterns_;
    std::vector<std::string> excludePatterns_;
};

// Factory function
std::unique_ptr<ICommand> createAddCommand() {
    return std::make_unique<AddCommand>();
}

} // namespace yams::cli
